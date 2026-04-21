package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Documents the current limitation of {@link org.taotree.internal.cow.Compactor}
 * for temporal trees: it walks the outer ART and copies each leaf (the 40-byte
 * {@code EntityNode}) verbatim, but does <b>not</b> recompact the nested roots
 * (CHAMP current state, AttributeRuns ART, EntityVersions ART) that those leaves
 * point into. Nor does it retire the old ART nodes via {@code EpochReclaimer}.
 *
 * <p><b>Correctness:</b> unaffected. The existing
 * {@link CompactionTest#compactPreservesTemporalHistory} proves reads
 * (including point-in-time) still return correct values after compact + reopen.
 * Pointers into the nested structures remain valid because those pages are
 * never explicitly retired and never shrunk on disk.
 *
 * <p><b>Gap (§18.12.3 of design.md):</b> Space is not fully reclaimed on
 * compact, and the "traversal-optimal locality" claim is only true for the
 * outer ART — the per-entity AttributeRuns/EntityVersions trees are not
 * repacked. This test asserts the empirical behavior so that any future
 * tightening of reclamation (which would cause data-loss were the compactor
 * left as-is) is detected immediately.
 *
 * <p>See plan.md Phase 8 / {@code p8-compactor-temporal}.
 */
class CompactorSpaceReclaimTest {

    @TempDir Path tmp;

    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    /**
     * Many entities with rich temporal history: compact + sync + reclaim then
     * read all history. Must succeed — proves pointers remain valid despite
     * the compactor not recompacting nested structures.
     */
    @Test
    void readsSucceedAfterCompactAndReclaimAdvance() throws IOException {
        Path p = tmp.resolve("reclaim.taotree");
        final int ENTITIES = 50;
        final int TIMES_PER_ATTR = 8;
        final long[] timestamps = new long[TIMES_PER_ATTR];
        for (int i = 0; i < TIMES_PER_ATTR; i++) timestamps[i] = 100L + i * 10L;

        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int e = 0; e < ENTITIES; e++) {
                    kb.set(ID, e);
                    for (int t = 0; t < TIMES_PER_ATTR; t++) {
                        w.put(kb, "a", Value.ofInt(e * 1000 + t), timestamps[t]);
                        w.put(kb, "b", Value.ofLong((long) e * 2_000_000L + t), timestamps[t]);
                    }
                }
            }
            tree.sync();
            tree.compact();
            // advanceDurableGeneration + reclaim() are already called inside compact().
            // A second sync/compact nudges durable pins forward again.
            tree.sync();
            tree.compact();

            try (var r = tree.read()) {
                for (int e = 0; e < ENTITIES; e++) {
                    kb.set(ID, e);
                    // Full history per attr is preserved.
                    int[] countA = {0};
                    r.history(kb, "a", (fs, ls, vt, v) -> { countA[0]++; return true; });
                    assertEquals(TIMES_PER_ATTR, countA[0],
                            "history for attr 'a' lost after compact for entity " + e);
                    // Mid-range point-in-time read returns the right observation.
                    Value mid = r.getAt(kb, "a", timestamps[3] + 5);
                    assertEquals(Value.ofInt(e * 1000 + 3), mid,
                            "getAt returned wrong value after compact+reclaim for entity " + e);
                }
            }
        }
    }

    /**
     * The space-reclamation gap: repeated compaction does <b>not</b> monotonically
     * shrink the store file, and may grow it because the compactor allocates new
     * slab nodes while leaving the old ones and the nested structures in place.
     *
     * <p>This test encodes the current behavior so we notice when a future fix
     * (recursive EntityNode compaction + retiring old slab nodes) actually
     * starts shrinking the file. It's intentionally permissive: we only assert
     * that file size does not explode beyond a conservative bound, and that
     * reads remain correct throughout.
     */
    @Test
    void fileSizeBoundedAcrossMultipleCompactCycles() throws IOException {
        Path p = tmp.resolve("bounded.taotree");
        final int ENTITIES = 30;
        final int OBSERVATIONS = 20;

        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int e = 0; e < ENTITIES; e++) {
                    kb.set(ID, e);
                    for (int i = 0; i < OBSERVATIONS; i++) {
                        w.put(kb, "v", Value.ofInt(e * 100 + i), 1000L + i);
                    }
                }
            }
            tree.sync();
            long baselineSize = Files.size(p);

            for (int cycle = 0; cycle < 3; cycle++) {
                tree.compact();
                tree.sync();
                long after = Files.size(p);
                // Conservative upper bound: compaction should not more than double
                // file size per cycle in steady state. Tighten when p8-compactor-temporal
                // lands a real implementation.
                assertTrue(after <= baselineSize * 4L,
                        "compact cycle " + cycle + " grew file " + baselineSize + "->" + after);
            }

            try (var r = tree.read()) {
                for (int e = 0; e < ENTITIES; e++) {
                    kb.set(ID, e);
                    assertEquals(Value.ofInt(e * 100 + (OBSERVATIONS - 1)), r.get(kb, "v"),
                            "latest value wrong after repeated compact for entity " + e);
                }
            }
        }
    }
}
