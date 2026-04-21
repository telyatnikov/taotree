package org.taotree;

import org.taotree.internal.persist.CommitRecord;
import org.taotree.internal.persist.Superblock;

/**
 * Captures a persistence-ready snapshot of a {@link TaoTree}: either the full
 * {@link Superblock.SuperblockData} needed for a checkpoint, or the smaller
 * {@link CommitRecord.CommitData} summary used for a commit record.
 *
 * <p>Extracted from {@code TaoTree} to keep that class focused on scopes and
 * mutation. Caller-synchronised: both methods must be invoked while holding
 * whatever lock the caller uses to stabilise {@code root}, {@code size}, and
 * dictionary state (typically the commit lock).
 */
final class TaoTreeSnapshot {

    private TaoTreeSnapshot() {}

    /**
     * Gathers all metadata required to write a checkpoint: allocator state,
     * tree descriptors, dictionary descriptors, and the optional schema binding.
     */
    static Superblock.SuperblockData gatherMetadata(TaoTree t) {
        var dicts = t.dictsInternal();
        var data = new Superblock.SuperblockData();
        data.slabSize = t.slab().slabSize();
        data.bumpPageSize = t.bump().pageSize();
        data.chunkSize = t.chunkStoreInternal().chunkSize();
        data.totalPages = t.chunkStoreInternal().totalPages();
        data.nextPage = t.chunkStoreInternal().nextPage();

        data.classes = new Superblock.SlabClassDescriptor[t.slab().classCount()];
        for (int i = 0; i < t.slab().classCount(); i++) {
            data.classes[i] = t.slab().exportClass(i);
        }

        data.bumpPageCount = t.bump().pageCount();
        data.bumpCurrentPage = t.bump().currentPage();
        data.bumpOffset = t.bump().bumpOffset();
        data.bumpBytesAllocated = t.bump().bytesAllocated();
        data.bumpPageLocations = t.bump().exportPageLocations();
        data.bumpPageSizes = t.bump().exportPageSizes();

        int treeCount = 1 + dicts.size();
        data.trees = new Superblock.TreeDescriptor[treeCount];
        data.trees[0] = t.exportTreeDescriptor();
        for (int i = 0; i < dicts.size(); i++) {
            data.trees[1 + i] = dicts.get(i).childTree().exportTreeDescriptor();
        }

        data.dicts = new Superblock.DictDescriptor[dicts.size()];
        for (int i = 0; i < dicts.size(); i++) {
            data.dicts[i] = dicts.get(i).exportDescriptor(1 + i);
        }

        if (t.boundKeyLayoutInternal() != null) {
            data.schemaBinding =
                org.taotree.internal.persist.SchemaBinding.serialize(t.boundKeyLayoutInternal());
        } else if (t.loadedSchemaBinding != null) {
            data.schemaBinding = t.loadedSchemaBinding;
        }

        return data;
    }

    /**
     * Builds a compact commit-record payload. The {@link
     * org.taotree.internal.persist.PersistenceManager} fills in generation,
     * prev-page, and arena-page fields.
     */
    static CommitRecord.CommitData buildCommitData(TaoTree t) {
        var dicts = t.dictsInternal();
        var cd = new CommitRecord.CommitData();
        cd.primaryRoot = t.root;
        cd.primarySize = t.size;
        cd.dictionaryCount = dicts.size();
        cd.dictRoots = new long[dicts.size()];
        cd.dictNextCodes = new long[dicts.size()];
        cd.dictSizes = new long[dicts.size()];
        for (int i = 0; i < dicts.size(); i++) {
            var dict = dicts.get(i);
            var childTree = dict.childTree();
            cd.dictRoots[i] = childTree.root;
            cd.dictNextCodes[i] = dict.nextCode();
            cd.dictSizes[i] = childTree.size;
        }
        return cd;
    }
}
