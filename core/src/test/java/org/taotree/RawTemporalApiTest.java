package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.value.ValueCodec;
import org.taotree.internal.temporal.HistoryVisitor;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Directly exercises the raw low-level temporal API on byte[] entity keys and
 * int attribute IDs (bypasses the Value facade): {@code latest}, {@code at},
 * {@code stateAt}, {@code allFieldsAt}, {@code history}, {@code putTemporal}.
 */
class RawTemporalApiTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    private static byte[] entityKey(int id) {
        // Big-endian unsigned int32 matches KeyBuilder encoding for a uint32 field.
        return new byte[]{(byte) (id >>> 24), (byte) (id >>> 16), (byte) (id >>> 8), (byte) id};
    }

    private static long inlineValueRef(long inline) {
        // Legacy name: retained for backwards-compat inside this test. See vref().
        return inline;
    }

    /**
     * Encode a real {@link ValueCodec} slot holding {@code sentinel} as an int64 and
     * return its slot ref. Using real encoded refs is required now that the temporal
     * write path compares value-slot bytes (not raw pointers) to detect duplicates —
     * passing raw synthetic longs like {@code 100L} would alias into page-zero bytes.
     */
    private static long vref(TaoTree tree, long sentinel) {
        Map<Long, Long> cache = VREF_CACHE.computeIfAbsent(tree, t -> new HashMap<>());
        return cache.computeIfAbsent(sentinel, s ->
                ValueCodec.encodeStandalone(Value.ofLong(s), tree.bump()));
    }

    private static final Map<TaoTree, Map<Long, Long>> VREF_CACHE = new java.util.IdentityHashMap<>();

    @Test
    void latestAndAtViaByteKey() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            int aid = tree.internAttr("metric");
            byte[] k = entityKey(7);

            try (var w = tree.write()) {
                assertTrue(w.putTemporal(k, aid, vref(tree, 100L), 1_000L));
                w.putTemporal(k, aid, vref(tree, 200L), 2_000L);
                w.putTemporal(k, aid, vref(tree, 300L), 3_000L);
            }

            try (var r = tree.read()) {
                assertNotEquals(TaoTree.TEMPORAL_NOT_FOUND, r.latest(k, aid));
                assertEquals(vref(tree, 100L), r.at(k, aid, 1_500L));
                assertEquals(vref(tree, 200L), r.at(k, aid, 2_500L));
                assertEquals(vref(tree, 300L), r.at(k, aid, 99_999L));
                // Before first observation.
                assertEquals(TaoTree.TEMPORAL_NOT_FOUND, r.at(k, aid, 500L));
                // Missing entity.
                assertEquals(TaoTree.TEMPORAL_NOT_FOUND, r.latest(entityKey(999), aid));
                assertEquals(TaoTree.TEMPORAL_NOT_FOUND, r.at(entityKey(999), aid, 1L));
            }
        }
    }

    @Test
    void historyByteArrayOverloadDeliversAllRuns() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            int aid = tree.internAttr("x");
            byte[] k = entityKey(3);
            long v10 = vref(tree, 10L), v20 = vref(tree, 20L), v30 = vref(tree, 30L);

            try (var w = tree.write()) {
                w.putTemporal(k, aid, v10, 100L);
                w.putTemporal(k, aid, v20, 200L);
                w.putTemporal(k, aid, v30, 300L);
            }

            try (var r = tree.read()) {
                List<Long> seen = new ArrayList<>();
                boolean done = r.history(k, aid, (fs, ls, vt, ref) -> {
                    seen.add(ref);
                    return true;
                });
                assertTrue(done);
                assertEquals(List.of(v10, v20, v30), seen);

                // Early-stop returns false.
                AtomicInteger calls = new AtomicInteger();
                boolean stopped = r.history(k, aid, (fs, ls, vt, ref) -> {
                    calls.incrementAndGet();
                    return false;
                });
                assertFalse(stopped);
                assertEquals(1, calls.get());
            }

            // Missing entity returns true (0 callbacks).
            try (var r = tree.read()) {
                assertTrue(r.history(entityKey(123), aid, (fs, ls, vt, ref) -> true));
                assertTrue(r.historyRange(entityKey(123), aid, 0L, 1_000L,
                        (fs, ls, vt, ref) -> true));
            }
        }
    }

    @Test
    void stateAtAndAllFieldsAtWalkPastAndCurrentState() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            int attrA = tree.internAttr("a");
            int attrB = tree.internAttr("b");
            byte[] k = entityKey(42);
            long v100 = vref(tree, 100L), v200 = vref(tree, 200L), v101 = vref(tree, 101L);

            try (var w = tree.write()) {
                w.putTemporal(k, attrA, v100, 1_000L);
                w.putTemporal(k, attrB, v200, 1_000L);
                w.putTemporal(k, attrA, v101, 2_000L);
            }

            try (var r = tree.read()) {
                long stateRoot = r.stateAt(k, 1_500L);
                assertNotEquals(TaoTree.TEMPORAL_NOT_FOUND, stateRoot);

                // At t=1_500 we expect attrA=100, attrB=200.
                java.util.Map<Integer, Long> vals = new java.util.HashMap<>();
                boolean done = r.allFieldsAt(k, 1_500L, (aid, ref) -> {
                    vals.put(aid, ref);
                    return true;
                });
                assertTrue(done);
                assertEquals(v100, vals.get(attrA));
                assertEquals(v200, vals.get(attrB));

                // Missing entity: allFieldsAt returns true, 0 callbacks.
                AtomicInteger n = new AtomicInteger();
                assertTrue(r.allFieldsAt(entityKey(999), 1_500L, (aid, ref) -> {
                    n.incrementAndGet();
                    return true;
                }));
                assertEquals(0, n.get());

                // Early-stop returns false.
                assertFalse(r.allFieldsAt(k, 1_500L, (aid, ref) -> false));
            }
        }
    }

    @Test
    void putTemporalByteArrayValidatesKeyLength() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            int aid = tree.internAttr("x");
            try (var w = tree.write()) {
                // key length is 4 bytes (uint32); a 3-byte array must be rejected.
                byte[] bad = new byte[3];
                assertThrows(IllegalArgumentException.class,
                        () -> w.putTemporal(bad, aid, 1L, 100L));
            }
        }
    }

    @Test
    void putTemporalRejectsSyntheticOpaqueValueRef() throws IOException {
        // The raw API must fail fast when callers pass a long that is not a real
        // ValueCodec slot ref — otherwise duplicate detection via slotEquals would
        // silently read aliased bump-page bytes. See design-rationale.md §7.
        try (var tree = TaoTree.create(path(), layout())) {
            int aid = tree.internAttr("x");
            byte[] k = entityKey(1);
            // Seed at least one allocation so page 0 exists; the synthetic ref 100L
            // would otherwise be rejected for a different reason (no page).
            long ok = vref(tree, 42L);
            try (var w = tree.write()) {
                assertTrue(w.putTemporal(k, aid, ok, 1_000L));
                IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                        () -> w.putTemporal(k, aid, 100L, 2_000L));
                assertTrue(ex.getMessage().contains("valueRef"),
                        "expected valueRef in message: " + ex.getMessage());
                // KeyBuilder overload must validate identically.
                var kb = tree.newKeyBuilder(Arena.ofConfined());
                kb.setU32(0, 2);
                assertThrows(IllegalArgumentException.class,
                        () -> w.putTemporal(kb, aid, 200L, 3_000L));
                // Tombstone sentinel is allowed.
                assertDoesNotThrow(() ->
                        w.putTemporal(k, aid, org.taotree.internal.temporal.AttributeRun.TOMBSTONE_VALUE_REF, 4_000L));
            }
        }
    }

    @Test
    void createOpens() throws IOException {
        // Sanity: tree.create produces a usable temporal tree.
        try (var tree = TaoTree.create(path(), layout())) {
            assertNotNull(tree.attrDictionary());
        }
    }
}
