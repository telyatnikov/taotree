package org.taotree.internal.persist;

import java.lang.foreign.MemorySegment;
import org.taotree.TaoTree;
import org.taotree.internal.alloc.ChunkStore;

/**
 * Recovery algorithm for log-structured shadow paging in TaoTree v2.
 *
 * <p>After opening a v2 file, recovery scans forward from the last checkpoint
 * to find valid commit records and reconstruct the latest state. Each valid
 * {@link CommitRecord} advances the recovered state; scanning stops at the
 * first invalid or stale page (garbage from a crashed scope).
 */
public final class ShadowPagingRecovery {

    private ShadowPagingRecovery() {}

    /**
     * Recovery result holding the reconstructed state after scanning commit records.
     */
    public static final class RecoveryState {
        public long generation;
        public long primaryRoot;
        public long primarySize;
        public int dictionaryCount;
        public long[] dictRoots;
        public long[] dictNextCodes;
        public long[] dictSizes;
        public int nextPage;  // end of last valid commit's arena (for resuming allocations)
    }

    /**
     * Scan forward from a base state (checkpoint) to find all valid commit records.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Set cursor = baseNextPage (end of checkpointed data)</li>
     *   <li>Read page at cursor</li>
     *   <li>Try to parse as {@link CommitRecord}</li>
     *   <li>If valid: update state from commit record, advance cursor past arenaEndPage</li>
     *   <li>If invalid: stop scanning (garbage from crashed scope)</li>
     *   <li>Return the final recovered state</li>
     * </ol>
     *
     * @param cs                the chunk store
     * @param baseGen           generation from the checkpoint
     * @param basePrimaryRoot   primary root from the checkpoint
     * @param basePrimarySize   primary size from the checkpoint
     * @param baseDictCount     number of dictionaries from the checkpoint
     * @param baseDictRoots     dict roots from the checkpoint
     * @param baseDictNextCodes dict next codes from the checkpoint
     * @param baseDictSizes     dict sizes from the checkpoint
     * @param baseNextPage      the next page after the checkpoint (where commit records start)
     * @param totalPages        total committed pages in the file
     * @return the recovered state
     */
    public static RecoveryState recover(ChunkStore cs,
                                         long baseGen,
                                         long basePrimaryRoot,
                                         long basePrimarySize,
                                         int baseDictCount,
                                         long[] baseDictRoots,
                                         long[] baseDictNextCodes,
                                         long[] baseDictSizes,
                                         int baseNextPage,
                                         int totalPages) {
        var state = new RecoveryState();
        state.generation = baseGen;
        state.primaryRoot = basePrimaryRoot;
        state.primarySize = basePrimarySize;
        state.dictionaryCount = baseDictCount;
        state.dictRoots = baseDictRoots != null ? baseDictRoots.clone() : new long[0];
        state.dictNextCodes = baseDictNextCodes != null ? baseDictNextCodes.clone() : new long[0];
        state.dictSizes = baseDictSizes != null ? baseDictSizes.clone() : new long[0];
        state.nextPage = baseNextPage;

        int cursor = baseNextPage;

        while (cursor < totalPages) {
            // Try to read a commit record at cursor
            MemorySegment page = cs.resolvePage(cursor);
            CommitRecord.CommitData commit = CommitRecord.read(page);

            if (commit == null) {
                break;  // Invalid page -- stop scanning
            }

            // Validate generation is strictly increasing
            if (commit.generation <= state.generation) {
                break;  // Stale or corrupt commit
            }

            // Update state from this commit record
            state.generation = commit.generation;
            state.primaryRoot = commit.primaryRoot;
            state.primarySize = commit.primarySize;
            state.dictionaryCount = commit.dictionaryCount;
            state.dictRoots = commit.dictRoots != null ? commit.dictRoots.clone() : new long[0];
            state.dictNextCodes = commit.dictNextCodes != null ? commit.dictNextCodes.clone() : new long[0];
            state.dictSizes = commit.dictSizes != null ? commit.dictSizes.clone() : new long[0];

            // Advance cursor past this commit's arena
            state.nextPage = commit.arenaEndPage;
            cursor = commit.arenaEndPage;
        }

        return state;
    }
}
