package org.taotree.internal.cow;

import java.lang.foreign.MemorySegment;
import java.util.List;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.NodePtr;

/**
 * Copy-on-write engine for ART mutations with deferred publication.
 *
 * <p>Facade that delegates to operation-specific classes:
 * <ul>
 *   <li>{@link CowInsert} — insert with path-copy
 *   <li>{@link CowDelete} — delete with path-copy, shrink, collapse
 *   <li>{@link CowNodeOps} — node-level COW operations (replace, insert, remove)
 *   <li>{@link CowContext} — shared state, allocation, resolution
 * </ul>
 *
 * <p>The caller ({@link org.taotree.TaoTree.WriteScope}) accumulates the new root
 * and publishes once at close time.
 */
public final class CowEngine {

    private final CowContext ctx;

    public CowEngine(SlabAllocator slab, EpochReclaimer reclaimer,
                     int prefixClassId, int node4ClassId, int node16ClassId,
                     int node48ClassId, int node256ClassId,
                     int keyLen, int keySlotSize, int[] leafClassIds) {
        this(slab, reclaimer, null, null,
             prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
             keyLen, keySlotSize, leafClassIds);
    }

    public CowEngine(SlabAllocator slab, EpochReclaimer reclaimer,
                     WriterArena arena, ChunkStore chunkStore,
                     int prefixClassId, int node4ClassId, int node16ClassId,
                     int node48ClassId, int node256ClassId,
                     int keyLen, int keySlotSize, int[] leafClassIds) {
        this.ctx = new CowContext(slab, arena, chunkStore,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
    }

    // -----------------------------------------------------------------------
    // Result type
    // -----------------------------------------------------------------------

    /**
     * Result of a deferred COW mutation (no CAS — caller publishes).
     *
     * @param leafPtr   the leaf pointer (existing or new); 0 for delete
     * @param mutated   true if the tree was structurally changed
     * @param newRoot   the new root after path-copy (always set when mutated)
     * @param sizeDelta +1 for insert, -1 for delete, 0 for existing key
     * @param retirees  nodes from the published tree replaced by this mutation
     */
    public record DeferredResult(
        long leafPtr,
        boolean mutated,
        long newRoot,
        int sizeDelta,
        List<Long> retirees
    ) {
        static DeferredResult unchanged(long leafPtr) {
            return new DeferredResult(leafPtr, false, NodePtr.EMPTY_PTR, 0, List.of());
        }
    }

    // -----------------------------------------------------------------------
    // Public API — delegates to operation classes
    // -----------------------------------------------------------------------

    /**
     * Insert a key using COW path-copy, without publishing.
     */
    public DeferredResult deferredGetOrCreate(long currentRoot, MemorySegment key,
                                              int keyLen, int leafClass) {
        return CowInsert.deferredGetOrCreate(ctx, currentRoot, key, keyLen, leafClass);
    }

    /**
     * Delete a key using COW path-copy, without publishing.
     */
    public DeferredResult deferredDelete(long currentRoot, MemorySegment key, int keyLen) {
        return CowDelete.deferredDelete(ctx, currentRoot, key, keyLen);
    }

    // -----------------------------------------------------------------------
    // Path stack — lightweight traversal recorder (used by insert + delete)
    // -----------------------------------------------------------------------

    static final class PathStack {
        private static final int MAX_DEPTH = 128;

        final long[] nodePtrs = new long[MAX_DEPTH];
        final byte[] keyBytes = new byte[MAX_DEPTH];
        final int[] nodeTypes = new int[MAX_DEPTH];
        int depth;

        void push(long nodePtr, byte keyByte, int nodeType) {
            nodePtrs[depth] = nodePtr;
            keyBytes[depth] = keyByte;
            nodeTypes[depth] = nodeType;
            depth++;
        }
    }
}
