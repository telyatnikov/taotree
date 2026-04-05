package org.taotree.internal;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Records the traversal path during a COW write operation and provides
 * the machinery for path-copy publication.
 *
 * <p>During a write operation, the writer traverses from the root to the
 * modification point, pushing each visited node onto this path stack.
 * After the modification, the writer walks back up the stack creating
 * COW copies of each node with the updated child pointer.
 *
 * <p>For per-subtree CAS publication, the walk-up stops at a
 * <em>divergence point</em> — the deepest Node256 ancestor whose
 * {@code children[keyByte]} slot can be CASed independently from other
 * writers. Two writers targeting different first-level key bytes of the
 * same Node256 target different memory addresses and never conflict.
 *
 * <p><b>Thread safety:</b> not thread-safe. Each writer thread has its
 * own CowPath instance (typically held in a {@code WriterContext}).
 *
 * <p><b>Lifecycle:</b> call {@link #reset()} at the start of each
 * operation, push entries during traversal, then call
 * {@link #cowPublish} to perform path-copy and CAS publication.
 */
public final class CowPath {

    /** Maximum tree depth (more than enough for 16-byte keys with prefix compression). */
    private static final int MAX_DEPTH = 128;

    // VarHandle for CAS on MemorySegment (64-bit long at a given offset).
    // ValueLayout.varHandle() returns a VarHandle with access mode
    // (MemorySegment base, long offset) -> long, supporting CAS.
    private static final VarHandle LONG_HANDLE = ValueLayout.JAVA_LONG.varHandle();

    // Path stack
    private final long[] nodePtrs = new long[MAX_DEPTH];     // NodePtr at each level
    private final byte[] keyBytes = new byte[MAX_DEPTH];      // key byte used to reach next level
    private final int[] nodeTypes = new int[MAX_DEPTH];       // cached node type at each level
    private int depth;

    // -----------------------------------------------------------------------
    // Path recording
    // -----------------------------------------------------------------------

    /** Reset the path for a new operation. */
    public void reset() {
        depth = 0;
    }

    /**
     * Push a node onto the path stack.
     *
     * @param nodePtr  the NodePtr of the visited node
     * @param keyByte  the key byte used to descend to the next level
     * @param nodeType the cached node type tag
     */
    public void push(long nodePtr, byte keyByte, int nodeType) {
        nodePtrs[depth] = nodePtr;
        keyBytes[depth] = keyByte;
        nodeTypes[depth] = nodeType;
        depth++;
    }

    /** Push a prefix node (key byte is not meaningful for prefix traversal). */
    public void pushPrefix(long nodePtr) {
        nodePtrs[depth] = nodePtr;
        keyBytes[depth] = 0;
        nodeTypes[depth] = NodePtr.PREFIX;
        depth++;
    }

    /** Current path depth. */
    public int depth() { return depth; }

    /** NodePtr at the given level. */
    public long nodeAt(int level) { return nodePtrs[level]; }

    /** Key byte at the given level. */
    public byte keyByteAt(int level) { return keyBytes[level]; }

    /** Node type at the given level. */
    public int nodeTypeAt(int level) { return nodeTypes[level]; }

    // -----------------------------------------------------------------------
    // COW path-copy and publication
    // -----------------------------------------------------------------------

    /**
     * Perform COW path-copy from the modification point up to the divergence
     * point, then attempt CAS publication.
     *
     * <p>The divergence point is the deepest Node256 in the path (if any).
     * For Node256, the child pointer slot at {@code children[keyByte]} is a
     * unique memory address per key byte — two writers targeting different
     * key bytes never conflict.
     *
     * <p>If no Node256 ancestor exists, the CAS targets the
     * {@code PublicationState} reference (root-level CAS).
     *
     * @param newSubtree    the new subtree to publish (result of the bottom-up COW)
     * @param allocator     allocator for creating COW copies (slab or arena)
     * @param slab          slab allocator for resolving existing nodes
     * @param reclaimer     epoch reclaimer for retiring old nodes
     * @return the result of the publication attempt
     */
    public CasResult cowAndPublish(long newSubtree,
                                   NodeAllocator allocator,
                                   SlabAllocator slab,
                                   EpochReclaimer reclaimer) {
        // Walk back up from (depth-1) to find the divergence point
        int divergeLevel = -1;
        for (int i = 0; i < depth; i++) {
            if (nodeTypes[i] == NodePtr.NODE_256) {
                divergeLevel = i;
                break; // highest Node256 = best CAS target
            }
        }

        // COW from bottom of path up to (but not including) diverge level
        long current = newSubtree;
        int startLevel = depth - 1;
        int stopLevel = (divergeLevel >= 0) ? divergeLevel + 1 : 0;

        for (int i = startLevel; i >= stopLevel; i--) {
            long oldNode = nodePtrs[i];
            int type = nodeTypes[i];
            byte kb = keyBytes[i];

            current = cowNodeReplaceChild(allocator, slab, oldNode, type, kb, current);
            reclaimer.retire(oldNode);
        }

        if (divergeLevel >= 0) {
            // Per-subtree CAS on Node256's child slot
            long node256Ptr = nodePtrs[divergeLevel];
            MemorySegment seg = slab.resolve(node256Ptr);
            byte casKeyByte = keyBytes[divergeLevel];
            long expectedChild = nodePtrs[divergeLevel + 1];
            long childOffset = Node256.childOffset(casKeyByte);

            boolean success = LONG_HANDLE.compareAndSet(seg, childOffset,
                expectedChild, current);
            return new CasResult(success, divergeLevel, current,
                success ? node256Ptr : NodePtr.EMPTY_PTR);
        }

        // No Node256 ancestor — must CAS at root level
        return new CasResult(false, -1, current, NodePtr.EMPTY_PTR);
    }

    /**
     * Create a COW copy of a node with one child pointer replaced.
     */
    private long cowNodeReplaceChild(NodeAllocator allocator, SlabAllocator slab,
                                     long oldNodePtr, int type, byte keyByte,
                                     long newChild) {
        MemorySegment oldSeg = slab.resolve(oldNodePtr);

        return switch (type) {
            case NodePtr.PREFIX -> {
                long newPtr = allocator.allocNode(NodeConstants.PREFIX_SIZE, NodePtr.PREFIX);
                MemorySegment newSeg = allocator.resolveNew(newPtr, NodeConstants.PREFIX_SIZE);
                PrefixNode.cowWithChild(newSeg, oldSeg, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_4 -> {
                long newPtr = allocator.allocNode(NodeConstants.NODE4_SIZE, NodePtr.NODE_4);
                MemorySegment newSeg = allocator.resolveNew(newPtr, NodeConstants.NODE4_SIZE);
                Node4.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_16 -> {
                long newPtr = allocator.allocNode(NodeConstants.NODE16_SIZE, NodePtr.NODE_16);
                MemorySegment newSeg = allocator.resolveNew(newPtr, NodeConstants.NODE16_SIZE);
                Node16.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_48 -> {
                long newPtr = allocator.allocNode(NodeConstants.NODE48_SIZE, NodePtr.NODE_48);
                MemorySegment newSeg = allocator.resolveNew(newPtr, NodeConstants.NODE48_SIZE);
                Node48.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_256 -> {
                long newPtr = allocator.allocNode(NodeConstants.NODE256_SIZE, NodePtr.NODE_256);
                MemorySegment newSeg = allocator.resolveNew(newPtr, NodeConstants.NODE256_SIZE);
                Node256.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            default -> throw new IllegalStateException("Cannot COW node type: " + type);
        };
    }

    // -----------------------------------------------------------------------
    // Result types
    // -----------------------------------------------------------------------

    /**
     * Result of a CAS publication attempt.
     *
     * @param success      true if the CAS succeeded
     * @param casLevel     the path level where the CAS was attempted (-1 = root level)
     * @param newSubtree   the new subtree that was (or would have been) published
     * @param casTargetPtr the NodePtr of the node where the CAS was attempted
     */
    public record CasResult(boolean success, int casLevel, long newSubtree, long casTargetPtr) {}

    // -----------------------------------------------------------------------
    // Allocation interface — abstracts over SlabAllocator vs WriterArena
    // -----------------------------------------------------------------------

    /**
     * Allocation interface for COW node creation. Abstracts over
     * {@link SlabAllocator} (for single-writer mode) and WriterArena
     * (for concurrent-writer mode).
     */
    public interface NodeAllocator {
        /** Allocate a node of the given size and type. Returns a NodePtr. */
        long allocNode(int size, int nodeType);

        /** Resolve a newly allocated NodePtr to a writable MemorySegment. */
        MemorySegment resolveNew(long nodePtr, int size);
    }
}
