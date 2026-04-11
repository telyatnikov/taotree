package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.ConflictResolver;
import org.taotree.TaoTree;
import org.taotree.layout.*;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the {@link ConflictResolver} merge path during concurrent rebases.
 *
 * <p>Two writers update the same key concurrently. One uses a
 * {@code ConflictResolver} that sums deltas (accumulator merge).
 * The loser's close triggers rebase → merge. The final value must
 * reflect contributions from both writers.
 */
@ExtendWith(FrayTestExtension.class)
class FrayConflictResolverTest extends FrayTestBase {

    private static final KeyLayout KEY_LAYOUT = KeyLayout.of(KeyField.uint32("id"));
    private static final LeafLayout LEAF_LAYOUT = LeafLayout.of(LeafField.int64("counter"));
    private static final LeafHandle.Int64 COUNTER = LEAF_LAYOUT.int64("counter");

    /**
     * Pre-populate key 1 with counter=100. Two writers each add 10.
     * With an additive resolver, final value must be 120 regardless of
     * commit order (both deltas applied).
     */
    @FrayTest(iterations = 10)
    void additiveResolverPreservesBothDeltas() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LAYOUT, LEAF_LAYOUT,
                CHUNK, false)) {
            var ID = tree.keyUint32("id");

            // Pre-populate: key 1 → counter=100
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                try (var w = tree.write()) {
                    kb.set(ID, 1);
                    w.getOrCreate(kb).set(COUNTER, 100L);
                }
            }

            // Additive resolver: delta = pending - snapshot, target += delta
            ConflictResolver additive = (target, pending, snapshot) -> {
                long tVal = target.get(COUNTER);
                long pVal = pending.get(COUNTER);
                long sVal = snapshot.get(COUNTER);
                target.set(COUNTER, tVal + (pVal - sVal));
            };

            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var a = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var kb = tree.newKeyBuilder(arena);
                    try (var w = tree.write(additive)) {
                        kb.set(ID, 1);
                        var acc = w.getOrCreate(kb);
                        long cur = acc.get(COUNTER);
                        acc.set(COUNTER, cur + 10);
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            var b = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var kb = tree.newKeyBuilder(arena);
                    try (var w = tree.write(additive)) {
                        kb.set(ID, 1);
                        var acc = w.getOrCreate(kb);
                        long cur = acc.get(COUNTER);
                        acc.set(COUNTER, cur + 10);
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var arena = Arena.ofConfined()) {
                var qb = tree.newQueryBuilder(arena);
                try (var r = tree.read()) {
                    qb.set(ID, 1);
                    var acc = r.lookup(qb.key(), LEAF_LAYOUT);
                    assertNotNull(acc, "Key 1 not found");
                    assertEquals(120L, acc.get(COUNTER),
                        "Both +10 deltas must be applied via resolver");
                }
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }

    /**
     * Without a resolver, last-writer-wins. Two writers set different values
     * on the same key — the final value must be one of the two (no corruption).
     */
    @FrayTest(iterations = 10)
    void lastWriterWinsWithoutResolver() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LAYOUT, LEAF_LAYOUT,
                CHUNK, false)) {
            var ID = tree.keyUint32("id");

            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                try (var w = tree.write()) {
                    kb.set(ID, 1);
                    w.getOrCreate(kb).set(COUNTER, 0L);
                }
            }

            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var a = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var kb = tree.newKeyBuilder(arena);
                    try (var w = tree.write()) {
                        kb.set(ID, 1);
                        w.getOrCreate(kb).set(COUNTER, 111L);
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            var b = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var kb = tree.newKeyBuilder(arena);
                    try (var w = tree.write()) {
                        kb.set(ID, 1);
                        w.getOrCreate(kb).set(COUNTER, 222L);
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var arena = Arena.ofConfined()) {
                var qb = tree.newQueryBuilder(arena);
                try (var r = tree.read()) {
                    qb.set(ID, 1);
                    var acc = r.lookup(qb.key(), LEAF_LAYOUT);
                    assertNotNull(acc, "Key 1 not found");
                    long val = acc.get(COUNTER);
                    assertTrue(val == 111L || val == 222L,
                        "Unexpected value " + val + " — expected 111 or 222");
                }
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
