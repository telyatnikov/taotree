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
}
