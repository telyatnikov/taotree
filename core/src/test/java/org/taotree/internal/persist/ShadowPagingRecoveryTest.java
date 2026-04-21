package org.taotree.internal.persist;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.persist.CommitRecord;
import org.taotree.internal.persist.ShadowPagingRecovery;

class ShadowPagingRecoveryTest {

    @TempDir Path tempDir;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private ChunkStore createStore(Arena arena) throws IOException {
        return ChunkStore.create(tempDir.resolve("test-" + System.nanoTime() + ".tao"), arena);
    }

    /**
     * Write a commit record at the given page with the specified fields.
     * Reserves pages up to arenaEndPage if needed.
     */
    private static void writeCommit(ChunkStore cs, int commitPage,
                                    long generation, long primaryRoot, long primarySize,
                                    int arenaStartPage, int arenaEndPage,
                                    int prevCommitPage) throws IOException {
        // Ensure enough pages are allocated in the ChunkStore
        while (cs.totalPages() <= commitPage) {
            cs.allocPages(1);
        }
        MemorySegment page = cs.resolvePage(commitPage);
        page.fill((byte) 0);

        var data = new CommitRecord.CommitData();
        data.generation = generation;
        data.primaryRoot = primaryRoot;
        data.primarySize = primarySize;
        data.dictionaryCount = 0;
        data.dictRoots = new long[0];
        data.dictNextCodes = new long[0];
        data.dictSizes = new long[0];
        data.arenaStartPage = arenaStartPage;
        data.arenaEndPage = arenaEndPage;
        data.prevCommitPage = prevCommitPage;
        CommitRecord.write(page, data);
    }

    /**
     * Write a commit record with dictionary entries.
     */
    private static void writeCommitWithDicts(ChunkStore cs, int commitPage,
                                             long generation, long primaryRoot,
                                             long primarySize,
                                             int arenaStartPage, int arenaEndPage,
                                             int dictCount, long[] dictRoots,
                                             long[] dictNextCodes, long[] dictSizes)
            throws IOException {
        while (cs.totalPages() <= commitPage) {
            cs.allocPages(1);
        }
        MemorySegment page = cs.resolvePage(commitPage);
        page.fill((byte) 0);

        var data = new CommitRecord.CommitData();
        data.generation = generation;
        data.primaryRoot = primaryRoot;
        data.primarySize = primarySize;
        data.dictionaryCount = dictCount;
        data.dictRoots = dictRoots;
        data.dictNextCodes = dictNextCodes;
        data.dictSizes = dictSizes;
        data.arenaStartPage = arenaStartPage;
        data.arenaEndPage = arenaEndPage;
        data.prevCommitPage = 0;
        CommitRecord.write(page, data);
    }

    /**
     * Write a commit record with dictionary and child tree entries.
     */
    private static void writeCommitWithChildTrees(ChunkStore cs, int commitPage,
                                                  long generation, long primaryRoot,
                                                  long primarySize,
                                                  int arenaStartPage, int arenaEndPage,
                                                  int dictCount, long[] dictRoots,
                                                  long[] dictNextCodes, long[] dictSizes,
                                                  int childTreeCount, long[] childTreeRoots,
                                                  long[] childTreeSizes)
            throws IOException {
        while (cs.totalPages() <= commitPage) {
            cs.allocPages(1);
        }
        MemorySegment page = cs.resolvePage(commitPage);
        page.fill((byte) 0);

        var data = new CommitRecord.CommitData();
        data.generation = generation;
        data.primaryRoot = primaryRoot;
        data.primarySize = primarySize;
        data.dictionaryCount = dictCount;
        data.dictRoots = dictRoots;
        data.dictNextCodes = dictNextCodes;
        data.dictSizes = dictSizes;
        data.arenaStartPage = arenaStartPage;
        data.arenaEndPage = arenaEndPage;
        data.prevCommitPage = 0;
        data.childTreeCount = childTreeCount;
        data.childTreeRoots = childTreeRoots;
        data.childTreeSizes = childTreeSizes;
        CommitRecord.write(page, data);
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    void recoverFromCheckpointOnly() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Ensure enough pages exist (pages 0-1 reserved for superblock)
            // No commit records written — recovery should return the base state
            var state = ShadowPagingRecovery.recover(
                    cs,
                    5L,          // baseGen
                    0x1234L,     // basePrimaryRoot
                    100L,        // basePrimarySize
                    0,           // baseDictCount
                    new long[0], // baseDictRoots
                    new long[0], // baseDictNextCodes
                    new long[0], // baseDictSizes
                    0,           // baseChildTreeCount
                    new long[0], // baseChildTreeRoots
                    new long[0], // baseChildTreeSizes
                    2,           // baseNextPage (right after superblock)
                    cs.totalPages());

            assertEquals(5L, state.generation);
            assertEquals(0x1234L, state.primaryRoot);
            assertEquals(100L, state.primarySize);
            assertEquals(0, state.dictionaryCount);
            assertEquals(2, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverOneCommit() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Write one commit record at page 2 (the baseNextPage)
            writeCommit(cs, 2,
                    /*generation*/ 1L,
                    /*primaryRoot*/ 0xABCDL,
                    /*primarySize*/ 50L,
                    /*arenaStartPage*/ 3,
                    /*arenaEndPage*/ 6,
                    /*prevCommitPage*/ 0);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L,          // baseGen (checkpoint has gen=0)
                    0L,          // basePrimaryRoot
                    0L,          // basePrimarySize
                    0,           // baseDictCount
                    new long[0],
                    new long[0],
                    new long[0],
                    0, new long[0], new long[0], // child trees
                    2,           // baseNextPage
                    cs.totalPages());

            assertEquals(1L, state.generation);
            assertEquals(0xABCDL, state.primaryRoot);
            assertEquals(50L, state.primarySize);
            assertEquals(6, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverMultipleCommits() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Write 3 commit records at consecutive positions.
            // Commit 1: page 2, arenaEnd=5
            writeCommit(cs, 2, 1L, 0x100L, 10L, 3, 5, 0);
            // Commit 2: page 5, arenaEnd=8
            writeCommit(cs, 5, 2L, 0x200L, 20L, 6, 8, 2);
            // Commit 3: page 8, arenaEnd=12
            writeCommit(cs, 8, 3L, 0x300L, 30L, 9, 12, 5);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0], // child trees
                    2,           // baseNextPage
                    cs.totalPages());

            // Should have recovered all 3 commits
            assertEquals(3L, state.generation);
            assertEquals(0x300L, state.primaryRoot);
            assertEquals(30L, state.primarySize);
            assertEquals(12, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverStopsAtCorruptPage() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Write 2 valid commits
            writeCommit(cs, 2, 1L, 0x100L, 10L, 3, 5, 0);
            writeCommit(cs, 5, 2L, 0x200L, 20L, 6, 8, 2);

            // Ensure page 8 exists but is zeroed (corrupt/invalid)
            while (cs.totalPages() <= 8) {
                cs.allocPages(1);
            }
            MemorySegment corruptPage = cs.resolvePage(8);
            corruptPage.fill((byte) 0);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0], // child trees
                    2,
                    cs.totalPages());

            // Recovery should have found commits 1 and 2, then stopped at corrupt page 8
            assertEquals(2L, state.generation);
            assertEquals(0x200L, state.primaryRoot);
            assertEquals(20L, state.primarySize);
            assertEquals(8, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverStopsAtStaleGeneration() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Write a valid commit at page 2 with generation 5
            writeCommit(cs, 2, 5L, 0x500L, 50L, 3, 6, 0);

            // Write a stale commit at page 6 with generation <= 5 (e.g., 3)
            writeCommit(cs, 6, 3L, 0x300L, 30L, 7, 10, 2);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0], // child trees
                    2,
                    cs.totalPages());

            // Should have recovered commit 1 (gen=5), then stopped at stale gen=3
            assertEquals(5L, state.generation);
            assertEquals(0x500L, state.primaryRoot);
            assertEquals(50L, state.primarySize);
            assertEquals(6, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverWithDictionaryEntries() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Write a commit with 2 dictionary entries
            writeCommitWithDicts(cs, 2, 1L, 0xABCL, 100L,
                    3, 6,
                    2,
                    new long[]{0x111L, 0x222L},
                    new long[]{10L, 20L},
                    new long[]{5L, 15L});

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0], // child trees
                    2,
                    cs.totalPages());

            assertEquals(1L, state.generation);
            assertEquals(0xABCL, state.primaryRoot);
            assertEquals(100L, state.primarySize);
            assertEquals(2, state.dictionaryCount);
            assertArrayEquals(new long[]{0x111L, 0x222L}, state.dictRoots);
            assertArrayEquals(new long[]{10L, 20L}, state.dictNextCodes);
            assertArrayEquals(new long[]{5L, 15L}, state.dictSizes);
            assertEquals(6, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverPreservesBaseStateWithNullArrays() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // No commits — pass null arrays for dict fields
            var state = ShadowPagingRecovery.recover(
                    cs,
                    10L, 0x999L, 500L, 0,
                    null, null, null,
                    0, null, null, // child trees
                    2,
                    cs.totalPages());

            assertEquals(10L, state.generation);
            assertEquals(0x999L, state.primaryRoot);
            assertEquals(500L, state.primarySize);
            assertEquals(0, state.dictionaryCount);
            assertNotNull(state.dictRoots);
            assertEquals(0, state.dictRoots.length);
            assertNotNull(state.dictNextCodes);
            assertEquals(0, state.dictNextCodes.length);
            assertNotNull(state.dictSizes);
            assertEquals(0, state.dictSizes.length);
            assertEquals(2, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverWhenBaseNextPageEqualsTotal() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            int total = cs.totalPages();

            // baseNextPage == totalPages → no pages to scan
            var state = ShadowPagingRecovery.recover(
                    cs,
                    7L, 0xFFL, 42L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0], // child trees
                    total,
                    total);

            assertEquals(7L, state.generation);
            assertEquals(0xFFL, state.primaryRoot);
            assertEquals(42L, state.primarySize);
            assertEquals(total, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverOverwritesDictStateFromCommit() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            long oldRoot = 0x01D_0000L;
            long newRoot1 = 0x0E01_0001L;
            long newRoot2 = 0x0E01_0002L;

            // Base has 1 dict
            long[] baseDictRoots = {oldRoot};
            long[] baseDictNext = {99L};
            long[] baseDictSizes = {50L};

            // Commit at page 2 has 2 dicts
            writeCommitWithDicts(cs, 2, 1L, 0xABC_DEF0L, 200L,
                    3, 6,
                    2,
                    new long[]{newRoot1, newRoot2},
                    new long[]{10L, 20L},
                    new long[]{5L, 15L});

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 1,
                    baseDictRoots, baseDictNext, baseDictSizes,
                    0, new long[0], new long[0], // child trees
                    2,
                    cs.totalPages());

            // Dict state should come from the commit, not the base
            assertEquals(2, state.dictionaryCount);
            assertEquals(newRoot1, state.dictRoots[0]);
            assertEquals(newRoot2, state.dictRoots[1]);
            assertArrayEquals(new long[]{10L, 20L}, state.dictNextCodes);

            cs.close();
        }
    }

    // -----------------------------------------------------------------------
    // Mutation-killing tests (child trees, boundary conditions, cloning)
    // -----------------------------------------------------------------------

    @Test
    void recoverWithChildTrees() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            writeCommitWithChildTrees(cs, 2,
                    1L, 0xABCL, 100L,
                    3, 6,
                    0, new long[0], new long[0], new long[0],
                    2, new long[]{0x111L, 0x222L}, new long[]{10L, 20L});

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0],
                    2, cs.totalPages());

            assertEquals(1L, state.generation);
            assertEquals(0xABCL, state.primaryRoot);
            assertEquals(100L, state.primarySize);
            assertEquals(2, state.childTreeCount);
            assertArrayEquals(new long[]{0x111L, 0x222L}, state.childTreeRoots);
            assertArrayEquals(new long[]{10L, 20L}, state.childTreeSizes);
            assertEquals(6, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverBaseChildTreesAreCloned() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            long[] baseChildRoots = {0x111L, 0x222L};
            long[] baseChildSizes = {10L, 20L};

            var state = ShadowPagingRecovery.recover(
                    cs,
                    5L, 0x999L, 500L, 0,
                    new long[0], new long[0], new long[0],
                    2, baseChildRoots, baseChildSizes,
                    2, cs.totalPages());

            // Modify originals — state should be independent
            baseChildRoots[0] = 0xDEADL;
            baseChildSizes[0] = 999L;

            assertEquals(0x111L, state.childTreeRoots[0]);
            assertEquals(0x222L, state.childTreeRoots[1]);
            assertEquals(10L, state.childTreeSizes[0]);
            assertEquals(20L, state.childTreeSizes[1]);

            cs.close();
        }
    }

    @Test
    void recoverCommitWithNullDictArrays() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Commit with dictionaryCount=0 — CommitRecord produces empty arrays.
            // Base had 2 dicts — commit should overwrite with 0.
            writeCommit(cs, 2, 1L, 0xABCL, 100L, 3, 6, 0);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 2,
                    new long[]{0x111L, 0x222L}, new long[]{10L, 20L}, new long[]{5L, 15L},
                    0, new long[0], new long[0],
                    2, cs.totalPages());

            assertEquals(0, state.dictionaryCount);
            assertNotNull(state.dictRoots);
            assertEquals(0, state.dictRoots.length);
            assertNotNull(state.dictNextCodes);
            assertEquals(0, state.dictNextCodes.length);
            assertNotNull(state.dictSizes);
            assertEquals(0, state.dictSizes.length);

            cs.close();
        }
    }

    @Test
    void recoverStopsAtArenaEndPageEqualsCursor() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Commit at page 2 with arenaEndPage == commitPage (cursor)
            writeCommit(cs, 2, 1L, 0xABCL, 100L, 3, 2, 0);

            // A second commit that should NOT be reached
            writeCommit(cs, 3, 2L, 0xDEFL, 200L, 4, 6, 2);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0],
                    2, cs.totalPages());

            // First commit is read and state partially updated, but
            // arenaEndPage(2) <= cursor(2) stops advancement
            assertEquals(1L, state.generation);
            assertEquals(0xABCL, state.primaryRoot);
            assertEquals(2, state.nextPage); // not advanced

            cs.close();
        }
    }

    @Test
    void recoverStopsAtArenaEndPageExceedsTotalPages() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            int total = cs.totalPages();

            // Commit at page 2 with arenaEndPage > totalPages
            writeCommit(cs, 2, 1L, 0xABCL, 100L, 3, total + 10, 0);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0],
                    2, total);

            // State updated from commit but nextPage not advanced
            assertEquals(1L, state.generation);
            assertEquals(0xABCL, state.primaryRoot);
            assertEquals(2, state.nextPage); // not advanced past corrupt arenaEndPage

            cs.close();
        }
    }

    @Test
    void recoverAcceptsArenaEndPageExactlyTotalPages() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            while (cs.totalPages() < 10) {
                cs.allocPages(1);
            }
            int total = cs.totalPages();

            // Commit at page 2 with arenaEndPage == totalPages (exactly at boundary)
            writeCommit(cs, 2, 1L, 0xABCL, 100L, 3, total, 0);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0],
                    2, total);

            // arenaEndPage == totalPages should be accepted (not > totalPages)
            assertEquals(1L, state.generation);
            assertEquals(0xABCL, state.primaryRoot);
            assertEquals(total, state.nextPage); // advanced to arenaEndPage

            cs.close();
        }
    }

    @Test
    void recoverStopsAtGenerationEqualToBase() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Commit with generation == baseGen (not strictly greater)
            writeCommit(cs, 2, 5L, 0xDEADL, 999L, 3, 6, 0);

            var state = ShadowPagingRecovery.recover(
                    cs,
                    5L, 0xBEEFL, 42L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0],
                    2, cs.totalPages());

            // generation == baseGen should NOT be accepted (must be strictly greater)
            assertEquals(5L, state.generation);
            assertEquals(0xBEEFL, state.primaryRoot); // base, not commit's 0xDEAD
            assertEquals(42L, state.primarySize);      // base, not commit's 999
            assertEquals(2, state.nextPage);           // not advanced

            cs.close();
        }
    }

    @Test
    void recoverMultipleCommitsWithChildTrees() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            // Commit 1: 2 child trees
            writeCommitWithChildTrees(cs, 2,
                    1L, 0x100L, 10L,
                    3, 5,
                    0, new long[0], new long[0], new long[0],
                    2, new long[]{0xA1L, 0xA2L}, new long[]{100L, 200L});

            // Commit 2: 2 child trees with different data
            writeCommitWithChildTrees(cs, 5,
                    2L, 0x200L, 20L,
                    6, 9,
                    0, new long[0], new long[0], new long[0],
                    2, new long[]{0xB1L, 0xB2L}, new long[]{300L, 400L});

            var state = ShadowPagingRecovery.recover(
                    cs,
                    0L, 0L, 0L, 0,
                    new long[0], new long[0], new long[0],
                    0, new long[0], new long[0],
                    2, cs.totalPages());

            // Should have recovered both commits — final state from commit 2
            assertEquals(2L, state.generation);
            assertEquals(0x200L, state.primaryRoot);
            assertEquals(20L, state.primarySize);
            assertEquals(2, state.childTreeCount);
            assertArrayEquals(new long[]{0xB1L, 0xB2L}, state.childTreeRoots);
            assertArrayEquals(new long[]{300L, 400L}, state.childTreeSizes);
            assertEquals(9, state.nextPage);

            cs.close();
        }
    }

    @Test
    void recoverBaseNonNullArraysPreservedWhenNoCommits() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);

            long[] baseDictRoots = {0x111L, 0x222L};
            long[] baseDictNextCodes = {10L, 20L};
            long[] baseDictSizes = {5L, 15L};
            long[] baseChildTreeRoots = {0x333L, 0x444L};
            long[] baseChildTreeSizes = {30L, 40L};

            // No commits written — all base arrays should be preserved
            var state = ShadowPagingRecovery.recover(
                    cs,
                    5L, 0x999L, 500L, 2,
                    baseDictRoots, baseDictNextCodes, baseDictSizes,
                    2, baseChildTreeRoots, baseChildTreeSizes,
                    2, cs.totalPages());

            assertEquals(5L, state.generation);
            assertEquals(0x999L, state.primaryRoot);
            assertEquals(500L, state.primarySize);
            assertEquals(2, state.dictionaryCount);
            assertArrayEquals(new long[]{0x111L, 0x222L}, state.dictRoots);
            assertArrayEquals(new long[]{10L, 20L}, state.dictNextCodes);
            assertArrayEquals(new long[]{5L, 15L}, state.dictSizes);
            assertEquals(2, state.childTreeCount);
            assertArrayEquals(new long[]{0x333L, 0x444L}, state.childTreeRoots);
            assertArrayEquals(new long[]{30L, 40L}, state.childTreeSizes);
            assertEquals(2, state.nextPage);

            cs.close();
        }
    }
}
