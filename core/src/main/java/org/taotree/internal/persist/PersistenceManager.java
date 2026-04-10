package org.taotree.internal.persist;

import org.taotree.internal.alloc.ChunkStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;

/**
 * Coordinates checkpoint and commit record writes for file-backed trees.
 *
 * <p>Manages the mirrored A/B checkpoint slots, monotonic generation counter,
 * shadow-paged commit records, and the commit-per-checkpoint policy.
 * {@link org.taotree.TaoTree} delegates all persistence I/O here.
 */
public final class PersistenceManager {

    private final ChunkStore chunkStore;

    // Checkpoint state
    private long generation;
    private int activeSlotPage;

    // Commit record state
    private int nextCommitPage = -1;
    private int prevCommitPage;
    private int commitsSinceCheckpoint;

    /** How many commit records between full checkpoints. */
    public static final int COMMITS_PER_CHECKPOINT = 64;

    /** Create for a new file-backed store (generation 0, slot A active). */
    public PersistenceManager(ChunkStore chunkStore) {
        this.chunkStore = chunkStore;
        this.activeSlotPage = CheckpointV2.SLOT_A_PAGE;
    }

    /** Restore from an existing store with known generation and active slot. */
    public PersistenceManager(ChunkStore chunkStore, long generation, int activeSlotPage) {
        this.chunkStore = chunkStore;
        this.generation = generation;
        this.activeSlotPage = activeSlotPage;
    }

    /** Current generation (monotonically increasing across checkpoints + commits). */
    public long generation() { return generation; }

    // -----------------------------------------------------------------------
    // Checkpoint
    // -----------------------------------------------------------------------

    /**
     * Write a v2 checkpoint to the inactive slot (mirrored A/B).
     * Each call toggles between slot A and slot B, so a torn write
     * to the new slot never corrupts the previous valid checkpoint.
     */
    public void writeCheckpoint(Superblock.SuperblockData metadata) {
        int targetSlotPage = CheckpointV2.inactiveSlotPage(activeSlotPage);
        long targetSlotId = (targetSlotPage == CheckpointV2.SLOT_A_PAGE) ? 0 : 1;
        generation++;

        var cpData = CheckpointIO.toCheckpoint(metadata, generation, targetSlotId);
        // Checkpoint slot pages may have been synced in a previous cycle — re-mark dirty
        chunkStore.markDirty(targetSlotPage);
        MemorySegment slot = chunkStore.resolve(targetSlotPage, CheckpointV2.SLOT_SIZE_PAGES);
        slot.fill((byte) 0);
        CheckpointV2.write(slot, cpData);

        activeSlotPage = targetSlotPage;
    }

    // -----------------------------------------------------------------------
    // Commit records (shadow paging)
    // -----------------------------------------------------------------------

    /**
     * Write a lightweight commit record to the pre-reserved page.
     *
     * <p>The caller fills in tree-specific fields ({@code primaryRoot},
     * {@code primarySize}, dictionary state). This method fills in the
     * persistence fields (generation, prevCommitPage, arena pages) and
     * writes the record.
     *
     * <p><b>Durability contract:</b> The caller MUST call
     * {@code chunkStore.syncDirty()} before returning to the user to
     * guarantee that the commit record and all modified data pages are
     * flushed to stable storage.
     */
    public void writeCommitRecord(CommitRecord.CommitData commitData) {
        if (nextCommitPage < 0) return;

        generation++;
        commitData.generation = generation;
        commitData.prevCommitPage = prevCommitPage;
        commitData.arenaStartPage = nextCommitPage;
        commitData.arenaEndPage = chunkStore.nextPage();

        // The commit page was allocated (and dirtied) by a previous reserveNextCommitPage(),
        // but its chunk may have been cleaned by syncDirty() since then — re-mark dirty.
        chunkStore.markDirty(nextCommitPage);
        MemorySegment page = chunkStore.resolvePage(nextCommitPage);
        CommitRecord.write(page, commitData);

        prevCommitPage = nextCommitPage;
        reserveNextCommitPage();
    }

    /**
     * Pre-reserve one ChunkStore page for the next commit record.
     * This ensures the commit record is at the exact page position
     * where {@link ShadowPagingRecovery} expects to find it.
     */
    public void reserveNextCommitPage() {
        try {
            nextCommitPage = chunkStore.allocPages(1);
            chunkStore.resolvePage(nextCommitPage).fill((byte) 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to reserve commit record page", e);
        }
    }

    // -----------------------------------------------------------------------
    // Checkpoint policy
    // -----------------------------------------------------------------------

    /**
     * Increment the commit counter and return true if a full checkpoint
     * is due (every {@value #COMMITS_PER_CHECKPOINT} commits).
     */
    public boolean shouldCheckpoint() {
        commitsSinceCheckpoint++;
        return commitsSinceCheckpoint >= COMMITS_PER_CHECKPOINT;
    }

    /** Reset the commit counter (called after a full checkpoint). */
    public void resetCommitCount() {
        commitsSinceCheckpoint = 0;
    }

    // -----------------------------------------------------------------------
    // Recovery helpers (pure functions on SuperblockData)
    // -----------------------------------------------------------------------

    /** Extract dict roots from superblock data for recovery. */
    public static long[] dictRootsFromData(Superblock.SuperblockData data) {
        long[] roots = new long[data.dicts.length];
        for (int i = 0; i < data.dicts.length; i++) {
            int treeIdx = data.dicts[i].treeIndex;
            roots[i] = treeIdx < data.trees.length ? data.trees[treeIdx].root : 0;
        }
        return roots;
    }

    /** Extract dict next codes from superblock data for recovery. */
    public static long[] dictNextCodesFromData(Superblock.SuperblockData data) {
        long[] codes = new long[data.dicts.length];
        for (int i = 0; i < data.dicts.length; i++) {
            codes[i] = data.dicts[i].nextCode;
        }
        return codes;
    }

    /** Extract dict sizes from superblock data for recovery. */
    public static long[] dictSizesFromData(Superblock.SuperblockData data) {
        long[] sizes = new long[data.dicts.length];
        for (int i = 0; i < data.dicts.length; i++) {
            int treeIdx = data.dicts[i].treeIndex;
            sizes[i] = treeIdx < data.trees.length ? data.trees[treeIdx].size : 0;
        }
        return sizes;
    }
}
