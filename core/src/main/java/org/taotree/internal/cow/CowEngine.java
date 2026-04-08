package org.taotree.internal.cow;

import java.lang.foreign.MemorySegment;
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
 *
 * <p>Operations accept an optional {@link WriterArena} parameter. When provided,
 * COW allocations go through the per-scope arena (for concurrent writers). When
 * {@code null}, allocations go through the slab allocator directly (child tree mode).
 */
public final class CowEngine {

    private final CowContext defaultCtx;

    // Immutable config — shared across all contexts
    final SlabAllocator slab;
    final ChunkStore chunkStore; // null for child trees
    final int prefixClassId;
    final int node4ClassId;
    final int node16ClassId;
    final int node48ClassId;
    final int node256ClassId;
    final int keyLen;
    final int keySlotSize;
    final int[] leafClassIds;

    public CowEngine(SlabAllocator slab, EpochReclaimer reclaimer,
                     WriterArena arena, ChunkStore chunkStore,
                     int prefixClassId, int node4ClassId, int node16ClassId,
                     int node48ClassId, int node256ClassId,
                     int keyLen, int keySlotSize, int[] leafClassIds) {
        this.slab = slab;
        this.chunkStore = chunkStore;
        this.prefixClassId = prefixClassId;
        this.node4ClassId = node4ClassId;
        this.node16ClassId = node16ClassId;
        this.node48ClassId = node48ClassId;
        this.node256ClassId = node256ClassId;
        this.keyLen = keyLen;
        this.keySlotSize = keySlotSize;
        this.leafClassIds = leafClassIds;
        this.defaultCtx = new CowContext(slab, arena, chunkStore,
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
        LongList retirees,
        long originalLeafPtr // original leaf ptr before COW-copy (0 if new key)
    ) {
        static DeferredResult unchanged(long leafPtr) {
            return new DeferredResult(leafPtr, false, NodePtr.EMPTY_PTR, 0, LongList.empty(), 0);
        }
    }

    // -----------------------------------------------------------------------
    // Public API — delegates to operation classes
    // -----------------------------------------------------------------------

    /**
     * Insert a key using COW path-copy, without publishing.
     * Uses the engine's default arena (child tree / single-writer mode).
     */
    public DeferredResult deferredGetOrCreate(long currentRoot, MemorySegment key,
                                              int keyLen, int leafClass) {
        return CowInsert.deferredGetOrCreate(defaultCtx, currentRoot, key, keyLen, leafClass);
    }

    /**
     * Insert a key using COW path-copy with a per-scope arena.
     * Used by concurrent writers to isolate arena allocations per WriteScope.
     */
    public DeferredResult deferredGetOrCreate(WriterArena arena, long currentRoot,
                                              MemorySegment key, int keyLen, int leafClass) {
        var ctx = contextWithArena(arena);
        return CowInsert.deferredGetOrCreate(ctx, currentRoot, key, keyLen, leafClass);
    }

    /**
     * Insert a key using COW path-copy with leaf-copy semantics.
     * When the key already exists, the leaf is COW-copied into the arena so the
     * caller gets a private mutable copy (invisible to readers until publication).
     */
    public DeferredResult deferredGetOrCreateCopy(WriterArena arena, long currentRoot,
                                                  MemorySegment key, int keyLen, int leafClass) {
        var ctx = contextWithArena(arena);
        return CowInsert.deferredGetOrCreateCopy(ctx, currentRoot, key, keyLen, leafClass);
    }

    /**
     * Like {@link #deferredGetOrCreateCopy}, but forces a COW-copy even for
     * arena-allocated leaves. Used during rebase where the published tree may
     * contain arena-allocated leaves from another writer's arena.
     */
    public DeferredResult deferredGetOrCreateForceCopy(WriterArena arena, long currentRoot,
                                                       MemorySegment key, int keyLen, int leafClass) {
        var ctx = contextWithArena(arena);
        return CowInsert.deferredGetOrCreateForceCopy(ctx, currentRoot, key, keyLen, leafClass);
    }

    /**
     * Delete a key using COW path-copy, without publishing.
     * Uses the engine's default arena.
     */
    public DeferredResult deferredDelete(long currentRoot, MemorySegment key, int keyLen) {
        return CowDelete.deferredDelete(defaultCtx, currentRoot, key, keyLen);
    }

    /**
     * Delete a key using COW path-copy with a per-scope arena.
     */
    public DeferredResult deferredDelete(WriterArena arena, long currentRoot,
                                         MemorySegment key, int keyLen) {
        var ctx = contextWithArena(arena);
        return CowDelete.deferredDelete(ctx, currentRoot, key, keyLen);
    }

    /** Create a CowContext with a per-scope arena (reuses immutable config). */
    private CowContext contextWithArena(WriterArena arena) {
        return new CowContext(slab, arena, chunkStore,
            prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
            keyLen, keySlotSize, leafClassIds);
    }

    // -----------------------------------------------------------------------
    // Path stack — lightweight traversal recorder (used by insert + delete)
    // -----------------------------------------------------------------------

    /**
     * Cached per-thread PathStack — allocated once per thread, reset before each use.
     * Eliminates the 3-array heap allocation that would otherwise happen on every
     * {@code getOrCreate} / {@code delete} call.
     */
    static final ThreadLocal<PathStack> PATH_STACK_CACHE =
            ThreadLocal.withInitial(PathStack::new);

    static final class PathStack {
        private static final int MAX_DEPTH = 128;

        final long[] nodePtrs = new long[MAX_DEPTH];
        final byte[] keyBytes = new byte[MAX_DEPTH];
        final int[] nodeTypes = new int[MAX_DEPTH];
        int depth;

        void reset() { depth = 0; }

        void push(long nodePtr, byte keyByte, int nodeType) {
            nodePtrs[depth] = nodePtr;
            keyBytes[depth] = keyByte;
            nodeTypes[depth] = nodeType;
            depth++;
        }
    }
}
