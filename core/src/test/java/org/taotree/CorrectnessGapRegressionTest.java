package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for three correctness gaps surfaced by code review:
 *
 * <ol>
 *   <li>{@code copyFrom} crashed on source histories containing tombstones
 *       because {@code ValueCodec.decodeStandalone(-1, ...)} was called for
 *       {@link org.taotree.internal.temporal.AttributeRun#TOMBSTONE_VALUE_REF}
 *       runs.</li>
 *   <li>{@code mergeAdjacentVersions} only rewrote the last merged group,
 *       leaving earlier groups with un-extended {@code validTo} after
 *       collapsing their successors.</li>
 *   <li>{@code delete(entity, attr)} always COW-created the entity leaf and
 *       bumped {@code size}, so deleting a missing entity leaked a phantom
 *       empty entity.</li>
 * </ol>
 */
class CorrectnessGapRegressionTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    // ----- Bug 3: delete(entity, attr) must NOT create a phantom entity -----

    @Test
    void deleteAttrOnMissingEntityDoesNotLeakPhantom() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            // Populate one real entity so the tree is non-empty.
            try (var w = tree.write()) {
                kb.set(ID, 1); w.put(kb, "n", Value.ofInt(1));
            }
            long sizeBefore;
            try (var r = tree.read()) { sizeBefore = r.size(); }
            assertEquals(1L, sizeBefore);

            // Delete an attribute on an entity that does NOT exist.
            try (var w = tree.write()) {
                kb.set(ID, 9999);
                assertFalse(w.delete(kb, "n"),
                        "delete on missing entity must return false");
            }

            try (var r = tree.read()) {
                assertEquals(sizeBefore, r.size(),
                        "size must not grow — no phantom empty entity allowed");
                kb.set(ID, 9999);
                // Entity must not be observable through getAll either.
                Map<String, Value> ga = r.getAll(kb);
                assertTrue(ga == null || ga.isEmpty(),
                        "missing entity must not appear in getAll");
            }
        }
    }

    @Test
    void deleteAttrOnMissingEntityWithMissingAttrStillReturnsFalse() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            try (var w = tree.write()) {
                kb.set(ID, 42);
                assertFalse(w.delete(kb, "never-interned-attr"),
                        "delete with unknown attr on unknown entity must return false");
            }
            try (var r = tree.read()) {
                assertEquals(0L, r.size());
            }
        }
    }

    // ----- Bug 1: copyFrom must handle tombstone runs -----

    @Test
    void copyFromReplaysTombstonesWithoutDecodingMinusOne() throws IOException {
        Path src = path();
        Path dst = path();
        var layout = keyLayout();

        try (var tree = TaoTree.create(src, layout);
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            try (var w = tree.write()) {
                kb.set(ID, 7);
                w.put(kb, "v", Value.ofInt(100), 1_000L);
                w.put(kb, "v", Value.ofInt(200), 2_000L);
                // Tombstone at t=3000 — this is what used to break copyFrom.
                w.delete(kb, "v", 3_000L);
                w.put(kb, "v", Value.ofInt(400), 4_000L);
            }
        }

        // Copy into a fresh tree. This must not throw from
        // ValueCodec.decodeStandalone on the tombstone ref (-1).
        try (var srcTree = TaoTree.open(src, layout);
             var dstTree = TaoTree.create(dst, layout);
             var arena = Arena.ofConfined()) {
            try (var w = dstTree.write();
                 var r = srcTree.read()) {
                w.copyFrom(r);
            }

            var ID = dstTree.keyUint32("id");
            var kb = dstTree.newKeyBuilder(arena);
            kb.set(ID, 7);

            try (var r = dstTree.read()) {
                // Before tombstone: values visible.
                assertEquals(100, r.getAt(kb, "v", 1_500L).asInt());
                assertEquals(200, r.getAt(kb, "v", 2_500L).asInt());
                // In the tombstone window [3000, 4000): must be null/absent.
                assertNull(r.getAt(kb, "v", 3_500L),
                        "tombstone window must replay as absent");
                // After the next put: value reappears.
                assertEquals(400, r.getAt(kb, "v", 4_500L).asInt());
            }
        }
    }

    // ----- Bug 2: mergeAdjacentVersions must rewrite every merged group -----

    @Test
    void mergeAdjacentVersionsHandlesMultipleGroups() throws IOException {
        // The bug lives in EntityVersions merge, so we test via getAllAt,
        // which uses the EntityVersions predecessor lookup — not via history,
        // which walks AttributeRuns.
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);

            // Seed four distinct versions at alternating states:
            //   V@1000: {x=1}   V@2000: {x=2}   V@3000: {x=1}   V@4000: {x=2}
            try (var w = tree.write()) {
                w.put(kb, "x", Value.ofInt(1), 1_000L);
                w.put(kb, "x", Value.ofInt(2), 2_000L);
                w.put(kb, "x", Value.ofInt(1), 3_000L);
                w.put(kb, "x", Value.ofInt(2), 4_000L);
            }

            // Now collapse V@2000 and V@3000 in a single call by overwriting
            // V@2000 to value 1 — after this write the versions walker sees
            // V@1000{x=1}, V@2000{x=1}, V@3000{x=1}, V@4000{x=2} and must
            // merge the whole [1000..3000] group. The old buggy loop left
            // V@1000's validTo at 1999 because it only rewrote the last
            // merged group.
            try (var w = tree.write()) {
                w.put(kb, "x", Value.ofInt(1), 2_000L);
            }

            try (var r = tree.read()) {
                // After the merge, [1000..3999] must be a single contiguous
                // window of state {x=1}. Queries anywhere inside must see it.
                for (long t : new long[] { 1_000L, 1_500L, 2_000L, 2_500L,
                                           3_000L, 3_500L, 3_999L }) {
                    Map<String, Value> m = r.getAllAt(kb, t);
                    assertNotNull(m, "getAllAt at t=" + t + " must not be null "
                            + "(bug symptom: V@1000 never had its validTo extended)");
                    assertNotNull(m.get("x"), "x must be present at t=" + t);
                    assertEquals(1, m.get("x").asInt(),
                            "x should be 1 at t=" + t);
                }
                // And at t=4000 the state flips to {x=2}.
                Map<String, Value> at4k = r.getAllAt(kb, 4_000L);
                assertNotNull(at4k);
                assertEquals(2, at4k.get("x").asInt());
            }
        }
    }
}
