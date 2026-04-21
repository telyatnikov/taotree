package org.taotree;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.persist.CheckpointIO;
import org.taotree.internal.persist.Checkpoint;
import org.taotree.internal.persist.PersistenceManager;
import org.taotree.internal.persist.ShadowPagingRecovery;

/**
 * Static helpers for creating and reopening file-backed {@link TaoTree} stores.
 *
 * <p>Extracted from {@code TaoTree} to keep the core coordination class focused
 * on scopes and mutation. Package-private: callers go through the public
 * {@code TaoTree.create}/{@code TaoTree.open} factory methods.
 */
final class FileBackedStoreIO {

    private FileBackedStoreIO() {}

    /**
     * Creates a fresh file-backed TaoTree, writing the initial checkpoint
     * and reserving the first commit-record page.
     */
    static TaoTree createFileBacked(Path path, int slabSize, int keyLen,
                                    int[] leafValueSizes,
                                    long chunkSize, boolean preallocate) throws IOException {
        var arena = Arena.ofShared();
        var cs = ChunkStore.createCheckpointed(path, arena, chunkSize, preallocate);
        var slab = new SlabAllocator(arena, cs, slabSize);
        var bump = new BumpAllocator(arena, cs, BumpAllocator.DEFAULT_PAGE_SIZE);

        var tree = new TaoTree(arena, slab, bump, cs, keyLen, leafValueSizes);
        tree.persistence = new PersistenceManager(cs);
        tree.persistence.writeCheckpoint(tree.gatherMetadata());
        tree.persistence.reserveNextCommitPage();
        return tree;
    }

    /**
     * Reopens an existing file-backed TaoTree: reads the mirrored checkpoint,
     * replays any commit records newer than the checkpoint generation, restores
     * allocators and dictionaries.
     */
    static TaoTree openFileBacked(Path path) throws IOException {
        var arena = Arena.ofShared();

        long fileSize;
        try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            fileSize = channel.size();
        }
        if (fileSize < (long) ChunkStore.CHECKPOINT_RESERVED_PAGES * ChunkStore.PAGE_SIZE) {
            throw new IOException("File too small to contain checkpoint: " + path);
        }

        int totalPages = (int) (fileSize / ChunkStore.PAGE_SIZE);
        var cs = ChunkStore.open(path, arena, ChunkStore.DEFAULT_CHUNK_SIZE, totalPages, totalPages);

        MemorySegment slotA = cs.checkpointSlotA();
        MemorySegment slotB = cs.checkpointSlotB();
        var cp = Checkpoint.chooseBest(slotA, slotB);
        if (cp == null) {
            throw new IOException("No valid checkpoint found in " + path);
        }
        var data = CheckpointIO.fromCheckpoint(cp);
        long cpGeneration = cp.generation;
        int cpActiveSlot = (cp.slotId == 0)
            ? Checkpoint.SLOT_A_PAGE
            : Checkpoint.SLOT_B_PAGE;

        var recovered = ShadowPagingRecovery.recover(
            cs,
            cpGeneration,
            data.trees.length > 0 ? data.trees[0].root : NodePtr.EMPTY_PTR,
            data.trees.length > 0 ? data.trees[0].size : 0,
            data.dicts.length,
            PersistenceManager.dictRootsFromData(data),
            PersistenceManager.dictNextCodesFromData(data),
            PersistenceManager.dictSizesFromData(data),
            0,
            new long[0],
            new long[0],
            data.nextPage,
            totalPages);

        if (recovered.generation > cpGeneration) {
            if (data.trees.length > 0) {
                data.trees[0].root = recovered.primaryRoot;
                data.trees[0].size = recovered.primarySize;
            }
            for (int i = 0; i < Math.min(recovered.dictionaryCount, data.dicts.length); i++) {
                if (1 + i < data.trees.length) {
                    data.trees[1 + i].root = recovered.dictRoots[i];
                    data.trees[1 + i].size = recovered.dictSizes[i];
                }
            }
            cpGeneration = recovered.generation;
        }

        cs.close();
        cs = ChunkStore.open(path, arena, data.chunkSize,
            Math.max(data.totalPages, recovered.nextPage), recovered.nextPage);

        var slab = new SlabAllocator(arena, cs, data.slabSize);
        for (var clsDesc : data.classes) {
            slab.restoreClass(clsDesc);
        }

        var bump = new BumpAllocator(arena, cs, data.bumpPageSize);
        for (int i = 0; i < data.bumpPageLocations.length; i++) {
            int sizeInPages = (i < data.bumpPageSizes.length && data.bumpPageSizes[i] > 0)
                ? data.bumpPageSizes[i]
                : data.bumpPageSize / ChunkStore.PAGE_SIZE;
            bump.restorePage(data.bumpPageLocations[i], sizeInPages);
        }
        bump.restoreState(data.bumpCurrentPage, data.bumpOffset, data.bumpBytesAllocated);

        if (data.trees.length == 0) {
            throw new IOException("No tree descriptors found in checkpoint");
        }
        var treeDesc = data.trees[0];
        var tree = new TaoTree(arena, slab, bump, cs, treeDesc);
        tree.persistence = new PersistenceManager(cs, cpGeneration, cpActiveSlot);
        tree.loadedSchemaBinding = data.schemaBinding;

        for (var dictDesc : data.dicts) {
            var childTreeDesc = data.trees[dictDesc.treeIndex];
            var childTree = new TaoTree(tree, childTreeDesc);
            var dict = new TaoDictionary(tree, childTree, dictDesc.maxCode, dictDesc.nextCode);
            tree.registerDict(dict);
        }

        tree.persistence.reserveNextCommitPage();

        return tree;
    }
}
