package org.taotree.internal.champ;

import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;

import static org.junit.jupiter.api.Assertions.*;

class ChampMapTest {

    private static BumpAllocator newBump(Arena arena) throws Exception {
        Path tmp = Files.createTempFile("champ-test-", ".dat");
        tmp.toFile().deleteOnExit();
        Files.delete(tmp);
        var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
        return new BumpAllocator(arena, cs, BumpAllocator.DEFAULT_PAGE_SIZE);
    }

    // ── Empty map ────────────────────────────────────────────────────────

    @Test
    void emptyMapGetReturnsNotFound() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, ChampMap.EMPTY_ROOT, 42));
        }
    }

    @Test
    void emptyMapSizeIsZero() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            assertEquals(0, ChampMap.size(bump, ChampMap.EMPTY_ROOT));
        }
    }

    @Test
    void emptyMapIterateCallsNothing() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            boolean completed = ChampMap.iterate(bump, ChampMap.EMPTY_ROOT,
                    (attrId, valueRef) -> { fail("should not be called"); return true; });
            assertTrue(completed);
        }
    }

    // ── Single entry ─────────────────────────────────────────────────────

    @Test
    void putAndGetSingleEntry() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 7, 0xBEEFL, result);
            assertTrue(result.modified);
            assertFalse(result.replaced);
            assertNotEquals(ChampMap.EMPTY_ROOT, root);

            assertEquals(0xBEEFL, ChampMap.get(bump, root, 7));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root, 8));
            assertEquals(1, ChampMap.size(bump, root));
        }
    }

    @Test
    void putSameValueIsNoOp() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 7, 0xBEEFL, result);

            result.reset();
            long root2 = ChampMap.put(bump, root, 7, 0xBEEFL, result);
            assertFalse(result.modified);
            assertEquals(root, root2); // pointer identity — canonical form
        }
    }

    @Test
    void putReplaceValue() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 7, 0xBEEFL, result);

            result.reset();
            long root2 = ChampMap.put(bump, root, 7, 0xCAFEL, result);
            assertTrue(result.modified);
            assertTrue(result.replaced);
            assertEquals(0xBEEFL, result.oldValueRef);

            assertEquals(0xCAFEL, ChampMap.get(bump, root2, 7));
            // old root still has old value (structural sharing / immutability)
            assertEquals(0xBEEFL, ChampMap.get(bump, root, 7));
        }
    }

    // ── Multiple entries (same hash prefix level 0 — different positions) ──

    @Test
    void multipleEntriesDifferentHashBits() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            long root = ChampMap.EMPTY_ROOT;
            for (int i = 0; i < 32; i++) {
                result.reset();
                root = ChampMap.put(bump, root, i, 1000L + i, result);
                assertTrue(result.modified);
            }

            assertEquals(32, ChampMap.size(bump, root));
            for (int i = 0; i < 32; i++) {
                assertEquals(1000L + i, ChampMap.get(bump, root, i));
            }
        }
    }

    // ── Entries that collide at level 0 (same low 5 bits) ────────────────

    @Test
    void hashCollisionLevel0() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // attrId 3 and 35 have the same low 5 bits (3 & 0x1F == 3, 35 & 0x1F == 3)
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 35, 200L, result);
            assertTrue(result.modified);

            assertEquals(2, ChampMap.size(bump, root));
            assertEquals(100L, ChampMap.get(bump, root, 3));
            assertEquals(200L, ChampMap.get(bump, root, 35));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root, 67)); // same low 5 bits
        }
    }

    @Test
    void deepCollisionMultipleLevels() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // Two attr_ids that collide at levels 0 and 1:
            // level 0: bits [4:0], level 1: bits [9:5]
            // attrId 3    = 0b...000_00000_00011  → lv0=3, lv1=0
            // attrId 1027 = 0b...010_00000_00011  → lv0=3, lv1=0, lv2=1 (same at lv0&1)
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 1027, 200L, result);
            assertTrue(result.modified);

            assertEquals(2, ChampMap.size(bump, root));
            assertEquals(100L, ChampMap.get(bump, root, 3));
            assertEquals(200L, ChampMap.get(bump, root, 1027));
        }
    }

    // ── Remove ───────────────────────────────────────────────────────────

    @Test
    void removeFromEmptyReturnsEmpty() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();
            result.reset();
            long root = ChampMap.remove(bump, ChampMap.EMPTY_ROOT, 42, result);
            assertEquals(ChampMap.EMPTY_ROOT, root);
            assertFalse(result.modified);
        }
    }

    @Test
    void removeNonExistentKey() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 7, 100L, result);

            result.reset();
            long root2 = ChampMap.remove(bump, root, 99, result);
            assertFalse(result.modified);
            assertEquals(root, root2);
        }
    }

    @Test
    void removeSingleEntry() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 7, 100L, result);

            result.reset();
            long root2 = ChampMap.remove(bump, root, 7, result);
            assertTrue(result.modified);
            assertTrue(result.replaced);
            assertEquals(100L, result.oldValueRef);
            assertEquals(ChampMap.EMPTY_ROOT, root2);
        }
    }

    @Test
    void removeOneOfTwo() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 1, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 2, 200L, result);

            result.reset();
            long root2 = ChampMap.remove(bump, root, 1, result);
            assertTrue(result.modified);
            assertEquals(1, ChampMap.size(bump, root2));
            assertEquals(200L, ChampMap.get(bump, root2, 2));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root2, 1));
        }
    }

    @Test
    void removeWithChampCompaction() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // Insert 3 and 35 (collide at level 0 → pushed to sub-node)
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 35, 200L, result);
            // Insert 10 (different position at level 0)
            result.reset();
            root = ChampMap.put(bump, root, 10, 300L, result);

            assertEquals(3, ChampMap.size(bump, root));

            // Remove 35 — sub-node for position 3 becomes singleton → should inline
            result.reset();
            long root2 = ChampMap.remove(bump, root, 35, result);
            assertTrue(result.modified);
            assertEquals(2, ChampMap.size(bump, root2));
            assertEquals(100L, ChampMap.get(bump, root2, 3));
            assertEquals(300L, ChampMap.get(bump, root2, 10));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root2, 35));
        }
    }

    // ── Iterate ──────────────────────────────────────────────────────────

    @Test
    void iterateCollectsAllEntries() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            long root = ChampMap.EMPTY_ROOT;
            Map<Integer, Long> expected = new HashMap<>();
            for (int i = 0; i < 50; i++) {
                result.reset();
                root = ChampMap.put(bump, root, i, 1000L + i, result);
                expected.put(i, 1000L + i);
            }

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(expected, collected);
        }
    }

    @Test
    void iterateEarlyTermination() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            long root = ChampMap.EMPTY_ROOT;
            for (int i = 0; i < 10; i++) {
                result.reset();
                root = ChampMap.put(bump, root, i, 1000L + i, result);
            }

            int[] count = {0};
            boolean completed = ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                count[0]++;
                return count[0] < 3; // stop after 3
            });
            assertFalse(completed);
            assertEquals(3, count[0]);
        }
    }

    // ── Structural sharing ───────────────────────────────────────────────

    @Test
    void oldRootPreservedAfterPut() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root1 = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 1, 100L, result);
            result.reset();
            root1 = ChampMap.put(bump, root1, 2, 200L, result);

            // Snapshot root1
            result.reset();
            long root2 = ChampMap.put(bump, root1, 3, 300L, result);

            // root1 still has 2 entries
            assertEquals(2, ChampMap.size(bump, root1));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root1, 3));

            // root2 has 3 entries
            assertEquals(3, ChampMap.size(bump, root2));
            assertEquals(300L, ChampMap.get(bump, root2, 3));
        }
    }

    @Test
    void oldRootPreservedAfterRemove() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root1 = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 1, 100L, result);
            result.reset();
            root1 = ChampMap.put(bump, root1, 2, 200L, result);

            // Remove from root1
            result.reset();
            long root2 = ChampMap.remove(bump, root1, 1, result);

            // root1 still has 2 entries
            assertEquals(2, ChampMap.size(bump, root1));
            assertEquals(100L, ChampMap.get(bump, root1, 1));

            // root2 has 1 entry
            assertEquals(1, ChampMap.size(bump, root2));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root2, 1));
        }
    }

    // ── Bulk operations (stress test) ────────────────────────────────────

    @Test
    void bulkInsertAndRemove() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            int N = 1000;
            long root = ChampMap.EMPTY_ROOT;
            Map<Integer, Long> reference = new HashMap<>();

            // Insert N entries
            for (int i = 0; i < N; i++) {
                result.reset();
                root = ChampMap.put(bump, root, i, (long) i * 7, result);
                assertTrue(result.modified);
                assertFalse(result.replaced);
                reference.put(i, (long) i * 7);
            }
            assertEquals(N, ChampMap.size(bump, root));

            // Verify all
            for (int i = 0; i < N; i++) {
                assertEquals((long) i * 7, ChampMap.get(bump, root, i),
                        "get(" + i + ") mismatch");
            }

            // Remove odd entries
            for (int i = 1; i < N; i += 2) {
                result.reset();
                root = ChampMap.remove(bump, root, i, result);
                assertTrue(result.modified, "remove(" + i + ") not modified");
                reference.remove(i);
            }
            assertEquals(N / 2, ChampMap.size(bump, root));

            // Verify remaining
            for (int i = 0; i < N; i++) {
                long expected = i % 2 == 0 ? (long) i * 7 : ChampMap.NOT_FOUND;
                assertEquals(expected, ChampMap.get(bump, root, i),
                        "get(" + i + ") after remove mismatch");
            }

            // Iterate and compare with reference
            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(reference, collected);
        }
    }

    @Test
    void bulkUpdateValues() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            int N = 500;
            long root = ChampMap.EMPTY_ROOT;

            // Insert
            for (int i = 0; i < N; i++) {
                result.reset();
                root = ChampMap.put(bump, root, i, (long) i, result);
            }

            // Update all values
            for (int i = 0; i < N; i++) {
                result.reset();
                root = ChampMap.put(bump, root, i, (long) i + N, result);
                assertTrue(result.modified);
                assertTrue(result.replaced);
                assertEquals((long) i, result.oldValueRef);
            }

            assertEquals(N, ChampMap.size(bump, root));
            for (int i = 0; i < N; i++) {
                assertEquals((long) i + N, ChampMap.get(bump, root, i));
            }
        }
    }

    // ── Edge cases: large attr_ids (high hash bits) ──────────────────────

    @Test
    void largeAttrIds() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            int[] ids = { 0, 1, 31, 32, 1023, 1024, 32767, 65535, Integer.MAX_VALUE };
            long root = ChampMap.EMPTY_ROOT;
            for (int id : ids) {
                result.reset();
                root = ChampMap.put(bump, root, id, (long) id * 11, result);
            }

            assertEquals(ids.length, ChampMap.size(bump, root));
            for (int id : ids) {
                assertEquals((long) id * 11, ChampMap.get(bump, root, id),
                        "get(" + id + ")");
            }
        }
    }

    // ── Put then remove all — should return to empty ─────────────────────

    @Test
    void insertAndRemoveAllReturnsToEmpty() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            long root = ChampMap.EMPTY_ROOT;
            for (int i = 0; i < 100; i++) {
                result.reset();
                root = ChampMap.put(bump, root, i, 1000L + i, result);
            }
            for (int i = 0; i < 100; i++) {
                result.reset();
                root = ChampMap.remove(bump, root, i, result);
                assertTrue(result.modified);
            }

            assertEquals(ChampMap.EMPTY_ROOT, root);
            assertEquals(0, ChampMap.size(bump, root));
        }
    }

    // ── Verify remove of non-existent key sharing hash prefix ────────────

    @Test
    void removeNonExistentKeyWithSamePrefix() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 3 and 35 share low 5 bits → they go to a sub-node
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 35, 200L, result);

            // Try removing 67 (same low 5 bits as 3 and 35, but different at level 1)
            result.reset();
            long root2 = ChampMap.remove(bump, root, 67, result);
            assertFalse(result.modified);
            assertEquals(root, root2);

            // Verify original entries intact
            assertEquals(100L, ChampMap.get(bump, root2, 3));
            assertEquals(200L, ChampMap.get(bump, root2, 35));
        }
    }

    // ── PIT mutation-killing tests ──────────────────────────────────────

    @Test
    void removeThenReinsert() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 5, 500L, result);
            result.reset();
            root = ChampMap.put(bump, root, 10, 1000L, result);

            result.reset();
            long rootAfterRemove = ChampMap.remove(bump, root, 5, result);
            assertTrue(result.modified);

            result.reset();
            long rootAfterReinsert = ChampMap.put(bump, rootAfterRemove, 5, 555L, result);
            assertTrue(result.modified);
            assertFalse(result.replaced);

            final long origRoot = root;
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, origRoot)),
                    () -> assertEquals(500L, ChampMap.get(bump, origRoot, 5)),
                    () -> assertEquals(1000L, ChampMap.get(bump, origRoot, 10))
            );
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, rootAfterReinsert)),
                    () -> assertEquals(555L, ChampMap.get(bump, rootAfterReinsert, 5)),
                    () -> assertEquals(1000L, ChampMap.get(bump, rootAfterReinsert, 10))
            );
            assertAll(
                    () -> assertEquals(1, ChampMap.size(bump, rootAfterRemove)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, rootAfterRemove, 5)),
                    () -> assertEquals(1000L, ChampMap.get(bump, rootAfterRemove, 10))
            );
        }
    }

    @Test
    void removeFromMixedNodeDataAndChildren() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 3 and 35 collide at L0 pos 3 → child pointer; 10 is inline at L0 pos 10
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 35, 200L, result);
            result.reset();
            root = ChampMap.put(bump, root, 10, 300L, result);

            assertEquals(3, ChampMap.size(bump, root));

            // Remove non-colliding inline entry (10) from node that has both data and child
            result.reset();
            long root2 = ChampMap.remove(bump, root, 10, result);
            assertTrue(result.modified);
            assertEquals(300L, result.oldValueRef);

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, r2)),
                    () -> assertEquals(100L, ChampMap.get(bump, r2, 3)),
                    () -> assertEquals(200L, ChampMap.get(bump, r2, 35)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 10))
            );

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root2, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(Map.of(3, 100L, 35, 200L), collected);
        }
    }

    @Test
    void removeCollidingEntryFromTwoLevelTree() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 3 and 35 collide at L0 → child node
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 35, 200L, result);
            assertEquals(2, ChampMap.size(bump, root));

            // Remove 3 → child singleton (only 35) → compacts inline
            result.reset();
            long root2 = ChampMap.remove(bump, root, 3, result);
            assertTrue(result.modified);
            assertEquals(100L, result.oldValueRef);

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(1, ChampMap.size(bump, r2)),
                    () -> assertEquals(200L, ChampMap.get(bump, r2, 35)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 3))
            );

            // Remove 35 from original → verify opposite compaction direction
            result.reset();
            long root3 = ChampMap.remove(bump, root, 35, result);
            assertTrue(result.modified);
            assertEquals(200L, result.oldValueRef);

            final long r3 = root3;
            assertAll(
                    () -> assertEquals(1, ChampMap.size(bump, r3)),
                    () -> assertEquals(100L, ChampMap.get(bump, r3, 3)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r3, 35))
            );
        }
    }

    @Test
    void threeWayCollisionL0() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 3, 35, 67 all have (attrId & 0x1F) == 3 → collide at L0
            // At L1: 3→0, 35→1, 67→2 — all differ → 3-entry child node
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 35, 200L, result);
            result.reset();
            root = ChampMap.put(bump, root, 67, 300L, result);

            final long r = root;
            assertAll(
                    () -> assertEquals(3, ChampMap.size(bump, r)),
                    () -> assertEquals(100L, ChampMap.get(bump, r, 3)),
                    () -> assertEquals(200L, ChampMap.get(bump, r, 35)),
                    () -> assertEquals(300L, ChampMap.get(bump, r, 67)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r, 99))
            );

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(Map.of(3, 100L, 35, 200L, 67, 300L), collected);

            // Remove 35 → child compacts from 3 data entries to 2
            result.reset();
            long root2 = ChampMap.remove(bump, root, 35, result);
            assertTrue(result.modified);

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, r2)),
                    () -> assertEquals(100L, ChampMap.get(bump, r2, 3)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 35)),
                    () -> assertEquals(300L, ChampMap.get(bump, r2, 67))
            );

            // Remove 67 from root2 → child becomes singleton → inlined
            result.reset();
            long root3 = ChampMap.remove(bump, root2, 67, result);
            assertTrue(result.modified);

            final long r3 = root3;
            assertAll(
                    () -> assertEquals(1, ChampMap.size(bump, r3)),
                    () -> assertEquals(100L, ChampMap.get(bump, r3, 3)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r3, 67))
            );
        }
    }

    @Test
    void removeMiddleEntryFromThreeEntryNode() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 0, 15, 31 at L0 positions 0, 15, 31 (no collisions)
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 0, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 15, 200L, result);
            result.reset();
            root = ChampMap.put(bump, root, 31, 300L, result);
            assertEquals(3, ChampMap.size(bump, root));

            // Remove middle (15) — both before and after data must be correctly copied
            result.reset();
            long root2 = ChampMap.remove(bump, root, 15, result);
            assertTrue(result.modified);
            assertEquals(200L, result.oldValueRef);

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, r2)),
                    () -> assertEquals(100L, ChampMap.get(bump, r2, 0)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 15)),
                    () -> assertEquals(300L, ChampMap.get(bump, r2, 31))
            );

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root2, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(Map.of(0, 100L, 31, 300L), collected);
        }
    }

    @Test
    void removeFirstEntryFromThreeEntryNode() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 0, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 15, 200L, result);
            result.reset();
            root = ChampMap.put(bump, root, 31, 300L, result);

            // Remove first (0) — no data before, two after
            result.reset();
            long root2 = ChampMap.remove(bump, root, 0, result);
            assertTrue(result.modified);
            assertEquals(100L, result.oldValueRef);

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, r2)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 0)),
                    () -> assertEquals(200L, ChampMap.get(bump, r2, 15)),
                    () -> assertEquals(300L, ChampMap.get(bump, r2, 31))
            );

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root2, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(Map.of(15, 200L, 31, 300L), collected);
        }
    }

    @Test
    void removeLastEntryFromThreeEntryNode() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 0, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 15, 200L, result);
            result.reset();
            root = ChampMap.put(bump, root, 31, 300L, result);

            // Remove last (31) — two data before, none after
            result.reset();
            long root2 = ChampMap.remove(bump, root, 31, result);
            assertTrue(result.modified);
            assertEquals(300L, result.oldValueRef);

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, r2)),
                    () -> assertEquals(100L, ChampMap.get(bump, r2, 0)),
                    () -> assertEquals(200L, ChampMap.get(bump, r2, 15)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 31))
            );

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root2, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(Map.of(0, 100L, 15, 200L), collected);
        }
    }

    @Test
    void deepTreeInsertAndRemoveMany() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            long root = ChampMap.EMPTY_ROOT;
            Map<Integer, Long> reference = new HashMap<>();

            for (int i = 0; i < 200; i++) {
                result.reset();
                root = ChampMap.put(bump, root, i, (long) i * 13 + 7, result);
                assertTrue(result.modified);
                reference.put(i, (long) i * 13 + 7);
            }
            assertEquals(200, ChampMap.size(bump, root));

            for (int i = 0; i < 200; i += 3) {
                result.reset();
                root = ChampMap.remove(bump, root, i, result);
                assertTrue(result.modified, "remove(" + i + ") not modified");
                assertEquals((long) i * 13 + 7, result.oldValueRef);
                reference.remove(i);
            }

            final long finalRoot = root;
            assertEquals(reference.size(), ChampMap.size(bump, finalRoot));

            for (var entry : reference.entrySet()) {
                assertEquals(entry.getValue(), ChampMap.get(bump, finalRoot, entry.getKey()),
                        "get(" + entry.getKey() + ")");
            }
            for (int i = 0; i < 200; i += 3) {
                assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, finalRoot, i),
                        "removed key " + i + " should be gone");
            }

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, finalRoot, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(reference, collected);
        }
    }

    @Test
    void sizeOfSingleEntryMapIsOne() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 42, 999L, result);

            assertEquals(1, ChampMap.size(bump, root));
            // Also verify size transitions: 0 → 1 → 2 → 1 → 0
            result.reset();
            long root2 = ChampMap.put(bump, root, 99, 888L, result);
            assertEquals(2, ChampMap.size(bump, root2));

            result.reset();
            long root3 = ChampMap.remove(bump, root2, 42, result);
            assertEquals(1, ChampMap.size(bump, root3));

            result.reset();
            long root4 = ChampMap.remove(bump, root3, 99, result);
            assertEquals(0, ChampMap.size(bump, root4));
            assertEquals(ChampMap.EMPTY_ROOT, root4);
        }
    }

    @Test
    void iterateStopsEarlyInChildNodes() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 3 and 35 collide at L0 → child node; 10 is inline at root
            // Root has: inline data at pos 10, child pointer at pos 3
            // Iteration visits inline data first, then descends into children
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 10, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 3, 200L, result);
            result.reset();
            root = ChampMap.put(bump, root, 35, 300L, result);
            assertEquals(3, ChampMap.size(bump, root));

            // Stop after first entry
            int[] count = {0};
            boolean completed = ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                count[0]++;
                return false;
            });
            assertFalse(completed);
            assertEquals(1, count[0]);

            // Stop after second entry (visits inline data then first child entry)
            count[0] = 0;
            completed = ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                count[0]++;
                return count[0] < 2;
            });
            assertFalse(completed);
            assertEquals(2, count[0]);

            // All three should be visited if not stopped
            count[0] = 0;
            completed = ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                count[0]++;
                return true;
            });
            assertTrue(completed);
            assertEquals(3, count[0]);
        }
    }

    @Test
    void putReturnsOldRootWhenChildSubtreeUnchanged() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 3 and 35 collide at L0 → child node
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 35, 200L, result);

            // Re-put same key/value → root pointer identity preserved (canonical)
            result.reset();
            long root2 = ChampMap.put(bump, root, 3, 100L, result);
            assertFalse(result.modified);
            assertEquals(root, root2);

            result.reset();
            long root3 = ChampMap.put(bump, root, 35, 200L, result);
            assertFalse(result.modified);
            assertEquals(root, root3);

            // Also test three-level: 3 and 1027 collide at L0 and L1
            result.reset();
            long deepRoot = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            deepRoot = ChampMap.put(bump, deepRoot, 1027, 200L, result);

            result.reset();
            long deepRoot2 = ChampMap.put(bump, deepRoot, 1027, 200L, result);
            assertFalse(result.modified);
            assertEquals(deepRoot, deepRoot2);
        }
    }

    @Test
    void removeNonExistentFromDeepTree() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 3 and 1027 collide at L0 (3) and L1 (0), differ at L2
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 1027, 200L, result);
            assertEquals(2, ChampMap.size(bump, root));

            final long deepRoot = root;

            // 2051 shares L0=3, L1=0 but differs at L2 → not found at leaf level
            result.reset();
            long root2 = ChampMap.remove(bump, deepRoot, 2051, result);
            assertFalse(result.modified);
            assertEquals(deepRoot, root2);

            // 35 shares L0=3 but differs at L1 → not found at child level
            result.reset();
            long root3 = ChampMap.remove(bump, deepRoot, 35, result);
            assertFalse(result.modified);
            assertEquals(deepRoot, root3);

            // 99 differs at L0 → not found at root level
            result.reset();
            long root4 = ChampMap.remove(bump, deepRoot, 99, result);
            assertFalse(result.modified);
            assertEquals(deepRoot, root4);

            assertAll(
                    () -> assertEquals(100L, ChampMap.get(bump, deepRoot, 3)),
                    () -> assertEquals(200L, ChampMap.get(bump, deepRoot, 1027)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, deepRoot, 2051)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, deepRoot, 35)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, deepRoot, 99))
            );
        }
    }

    // ── Additional structural tests for remaining mutation coverage ──────

    @Test
    void migrateInlineToNodeAtVariousPositions() throws Exception {
        // Build a root with data at multiple positions, then force inline→node
        // migrations at first, middle, and last data positions, each time creating
        // a new child between/around existing children.
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            long root = ChampMap.EMPTY_ROOT;
            // Non-colliding entries at L0 positions: 0, 5, 10, 20, 25
            result.reset(); root = ChampMap.put(bump, root, 0, 1L, result);
            result.reset(); root = ChampMap.put(bump, root, 5, 2L, result);
            result.reset(); root = ChampMap.put(bump, root, 10, 3L, result);
            result.reset(); root = ChampMap.put(bump, root, 20, 4L, result);
            result.reset(); root = ChampMap.put(bump, root, 25, 5L, result);

            // Collide at pos 5: 37 & 0x1F == 5 → migrate data[5] to child
            result.reset(); root = ChampMap.put(bump, root, 37, 6L, result);
            assertTrue(result.modified);
            // Root: data at {0,10,20,25}, child at {5}

            // Collide at pos 20: 52 & 0x1F == 20 → migrate data[20] to child
            result.reset(); root = ChampMap.put(bump, root, 52, 7L, result);
            assertTrue(result.modified);
            // Root: data at {0,10,25}, children at {5,20}

            // Collide at pos 0: 32 & 0x1F == 0 → migrate data[0] to child (first position)
            result.reset(); root = ChampMap.put(bump, root, 32, 8L, result);
            assertTrue(result.modified);
            // Root: data at {10,25}, children at {0,5,20}

            // Collide at pos 25: 57 & 0x1F == 25 → migrate data[25] to child (last data position)
            result.reset(); root = ChampMap.put(bump, root, 57, 9L, result);
            assertTrue(result.modified);
            // Root: data at {10}, children at {0,5,20,25}

            assertEquals(9, ChampMap.size(bump, root));
            final long r = root;
            assertAll(
                    () -> assertEquals(1L, ChampMap.get(bump, r, 0)),
                    () -> assertEquals(2L, ChampMap.get(bump, r, 5)),
                    () -> assertEquals(3L, ChampMap.get(bump, r, 10)),
                    () -> assertEquals(4L, ChampMap.get(bump, r, 20)),
                    () -> assertEquals(5L, ChampMap.get(bump, r, 25)),
                    () -> assertEquals(6L, ChampMap.get(bump, r, 37)),
                    () -> assertEquals(7L, ChampMap.get(bump, r, 52)),
                    () -> assertEquals(8L, ChampMap.get(bump, r, 32)),
                    () -> assertEquals(9L, ChampMap.get(bump, r, 57))
            );

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(9, collected.size());
            assertEquals(1L, collected.get(0));
            assertEquals(8L, collected.get(32));
            assertEquals(9L, collected.get(57));
        }
    }

    @Test
    void compactionAtVariousChildPositions() throws Exception {
        // Build a node with multiple children, then trigger node→inline compaction
        // at first, middle, and last child positions.
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // Create children at L0 positions {0, 5, 20} plus inline data at {10, 25}
            long root = ChampMap.EMPTY_ROOT;
            int[][] pairs = {{0, 32}, {5, 37}, {20, 52}};
            for (int[] pair : pairs) {
                result.reset(); root = ChampMap.put(bump, root, pair[0], (long) pair[0] * 10, result);
                result.reset(); root = ChampMap.put(bump, root, pair[1], (long) pair[1] * 10, result);
            }
            result.reset(); root = ChampMap.put(bump, root, 10, 100L, result);
            result.reset(); root = ChampMap.put(bump, root, 25, 250L, result);
            assertEquals(8, ChampMap.size(bump, root));

            // Remove 32 → child at pos 0 becomes singleton → inlined (first child)
            result.reset();
            long root2 = ChampMap.remove(bump, root, 32, result);
            assertTrue(result.modified);
            assertEquals(7, ChampMap.size(bump, root2));
            assertEquals(0L, ChampMap.get(bump, root2, 0));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root2, 32));

            // Remove 37 → child at pos 5 becomes singleton → inlined (middle child)
            result.reset();
            long root3 = ChampMap.remove(bump, root2, 37, result);
            assertTrue(result.modified);
            assertEquals(6, ChampMap.size(bump, root3));
            assertEquals(50L, ChampMap.get(bump, root3, 5));

            // Remove 52 → child at pos 20 becomes singleton → inlined (last child)
            result.reset();
            long root4 = ChampMap.remove(bump, root3, 52, result);
            assertTrue(result.modified);
            assertEquals(5, ChampMap.size(bump, root4));
            assertEquals(200L, ChampMap.get(bump, root4, 20));

            // All entries now inline at root
            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root4, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(Map.of(0, 0L, 5, 50L, 10, 100L, 20, 200L, 25, 250L), collected);
        }
    }

    @Test
    void removeFromNodeWithDataAndSubChildren() throws Exception {
        // Creates a child node with D=1, C>0 after removal.
        // Verifies isSingleton correctly returns false (nodeMap != 0).
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // All collide at L0 pos 3. At L1: 3→0, 35→1, 67→2, 1059→1
            // 35 and 1059 additionally collide at L1 → sub-child at L1 pos 1
            long root = ChampMap.EMPTY_ROOT;
            result.reset(); root = ChampMap.put(bump, root, 3, 100L, result);
            result.reset(); root = ChampMap.put(bump, root, 35, 200L, result);
            result.reset(); root = ChampMap.put(bump, root, 67, 300L, result);
            result.reset(); root = ChampMap.put(bump, root, 1059, 400L, result);
            assertEquals(4, ChampMap.size(bump, root));

            // Remove 3 → L1 child has D=1 (67 at pos 2), C=1 (child at pos 1 with {35,1059})
            // isSingleton must return false (has sub-child) → NOT inlined
            result.reset();
            long root2 = ChampMap.remove(bump, root, 3, result);
            assertTrue(result.modified);

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(3, ChampMap.size(bump, r2)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 3)),
                    () -> assertEquals(200L, ChampMap.get(bump, r2, 35)),
                    () -> assertEquals(300L, ChampMap.get(bump, r2, 67)),
                    () -> assertEquals(400L, ChampMap.get(bump, r2, 1059))
            );

            // Remove 67 → L1 child has D=0, C=1 → still not singleton
            result.reset();
            long root3 = ChampMap.remove(bump, root2, 67, result);
            assertTrue(result.modified);

            final long r3 = root3;
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, r3)),
                    () -> assertEquals(200L, ChampMap.get(bump, r3, 35)),
                    () -> assertEquals(400L, ChampMap.get(bump, r3, 1059))
            );

            // Remove 1059 → sub-child becomes singleton → compacted to L1 inline
            // Then L1 child is singleton (only 35) → compacted to L0 inline
            result.reset();
            long root4 = ChampMap.remove(bump, root3, 1059, result);
            assertTrue(result.modified);

            final long r4 = root4;
            assertAll(
                    () -> assertEquals(1, ChampMap.size(bump, r4)),
                    () -> assertEquals(200L, ChampMap.get(bump, r4, 35))
            );
        }
    }

    @Test
    void deepCollisionThreeLevels() throws Exception {
        // Tests mergeTwoEntries at deeper levels and deep removal compaction
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // 3: L0=3, L1=0, L2=0
            // 1027: L0=3, L1=0, L2=1 — collide at L0 AND L1, differ at L2
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 3, 100L, result);
            result.reset();
            root = ChampMap.put(bump, root, 1027, 200L, result);
            assertTrue(result.modified);

            // Add 35 at same L0, different L1 to create mixed child
            result.reset();
            root = ChampMap.put(bump, root, 35, 300L, result);
            assertEquals(3, ChampMap.size(bump, root));

            final long r = root;
            assertAll(
                    () -> assertEquals(100L, ChampMap.get(bump, r, 3)),
                    () -> assertEquals(200L, ChampMap.get(bump, r, 1027)),
                    () -> assertEquals(300L, ChampMap.get(bump, r, 35))
            );

            // Remove 1027 → deep compaction from L2 up
            result.reset();
            long root2 = ChampMap.remove(bump, root, 1027, result);
            assertTrue(result.modified);
            assertEquals(200L, result.oldValueRef);

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(2, ChampMap.size(bump, r2)),
                    () -> assertEquals(100L, ChampMap.get(bump, r2, 3)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 1027)),
                    () -> assertEquals(300L, ChampMap.get(bump, r2, 35))
            );

            // Remove 3 → now only 35 remains
            result.reset();
            long root3 = ChampMap.remove(bump, root2, 3, result);
            assertTrue(result.modified);

            final long r3 = root3;
            assertAll(
                    () -> assertEquals(1, ChampMap.size(bump, r3)),
                    () -> assertEquals(300L, ChampMap.get(bump, r3, 35)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r3, 3))
            );
        }
    }

    @Test
    void insertAtFirstMiddleLastPositions() throws Exception {
        // Exercises copyAndInsertEntry boundary conditions:
        // insertOffset == HEADER_SIZE (first), middle, and tailLen == 0 (last)
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            // Insert at pos 15 first
            result.reset();
            long root = ChampMap.put(bump, ChampMap.EMPTY_ROOT, 15, 150L, result);

            // Insert at pos 0 (before all existing) → insertOffset == HEADER_SIZE
            result.reset();
            root = ChampMap.put(bump, root, 0, 100L, result);
            assertTrue(result.modified);

            // Insert at pos 31 (after all existing) → tailLen == 0
            result.reset();
            root = ChampMap.put(bump, root, 31, 310L, result);
            assertTrue(result.modified);

            // Insert between existing (pos 8)
            result.reset();
            root = ChampMap.put(bump, root, 8, 80L, result);
            assertTrue(result.modified);

            // Insert between existing (pos 20)
            result.reset();
            root = ChampMap.put(bump, root, 20, 200L, result);
            assertTrue(result.modified);

            assertEquals(5, ChampMap.size(bump, root));
            final long r = root;
            assertAll(
                    () -> assertEquals(100L, ChampMap.get(bump, r, 0)),
                    () -> assertEquals(80L, ChampMap.get(bump, r, 8)),
                    () -> assertEquals(150L, ChampMap.get(bump, r, 15)),
                    () -> assertEquals(200L, ChampMap.get(bump, r, 20)),
                    () -> assertEquals(310L, ChampMap.get(bump, r, 31))
            );

            Map<Integer, Long> collected = new HashMap<>();
            ChampMap.iterate(bump, root, (attrId, valueRef) -> {
                collected.put(attrId, valueRef);
                return true;
            });
            assertEquals(Map.of(0, 100L, 8, 80L, 15, 150L, 20, 200L, 31, 310L), collected);
        }
    }

    @Test
    void removeFromNodeWithMultipleChildrenAndData() throws Exception {
        // Node with data entries interleaved between child pointers at various
        // bitmap positions. Removal exercises copyAndRemoveEntry with children
        // trailing in the memory layout.
        try (var arena = Arena.ofConfined()) {
            var bump = newBump(arena);
            var result = new ChampMap.Result();

            long root = ChampMap.EMPTY_ROOT;
            // Data at L0 positions: 1, 10, 15, 25
            // Children at L0 positions: 3 (keys 3,35), 20 (keys 20,52)
            result.reset(); root = ChampMap.put(bump, root, 1, 10L, result);
            result.reset(); root = ChampMap.put(bump, root, 10, 100L, result);
            result.reset(); root = ChampMap.put(bump, root, 15, 150L, result);
            result.reset(); root = ChampMap.put(bump, root, 25, 250L, result);
            result.reset(); root = ChampMap.put(bump, root, 3, 30L, result);
            result.reset(); root = ChampMap.put(bump, root, 35, 350L, result);
            result.reset(); root = ChampMap.put(bump, root, 20, 200L, result);
            result.reset(); root = ChampMap.put(bump, root, 52, 520L, result);
            assertEquals(8, ChampMap.size(bump, root));

            // Remove data between children (pos 10) — tests tail copy with child ptrs
            result.reset();
            long root2 = ChampMap.remove(bump, root, 10, result);
            assertTrue(result.modified);
            assertEquals(7, ChampMap.size(bump, root2));

            final long r2 = root2;
            assertAll(
                    () -> assertEquals(10L, ChampMap.get(bump, r2, 1)),
                    () -> assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, r2, 10)),
                    () -> assertEquals(150L, ChampMap.get(bump, r2, 15)),
                    () -> assertEquals(250L, ChampMap.get(bump, r2, 25)),
                    () -> assertEquals(30L, ChampMap.get(bump, r2, 3)),
                    () -> assertEquals(350L, ChampMap.get(bump, r2, 35)),
                    () -> assertEquals(200L, ChampMap.get(bump, r2, 20)),
                    () -> assertEquals(520L, ChampMap.get(bump, r2, 52))
            );

            // Remove first data entry (pos 1)
            result.reset();
            long root3 = ChampMap.remove(bump, root2, 1, result);
            assertTrue(result.modified);
            assertEquals(6, ChampMap.size(bump, root3));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root3, 1));
            assertEquals(150L, ChampMap.get(bump, root3, 15));
            assertEquals(350L, ChampMap.get(bump, root3, 35));

            // Remove last data entry (pos 25)
            result.reset();
            long root4 = ChampMap.remove(bump, root3, 25, result);
            assertTrue(result.modified);
            assertEquals(5, ChampMap.size(bump, root4));
            assertEquals(ChampMap.NOT_FOUND, ChampMap.get(bump, root4, 25));
            assertEquals(520L, ChampMap.get(bump, root4, 52));
        }
    }
}
