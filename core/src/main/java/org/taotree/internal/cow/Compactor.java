package org.taotree.internal.cow;


import java.lang.foreign.MemorySegment;
import org.taotree.TaoString;
import org.taotree.TaoTree;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.Node4;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.art.PrefixNode;

/**
 * Background compactor for TaoTree v2.
 *
 * <p>Walks the live tree in post-order (children before parents), copies live
 * nodes into packed slab layout, writes a fresh checkpoint, and advances
 * {@code durableGeneration} to enable epoch reclamation of old arena pages.
 *
 * <p>Post-order traversal produces traversal-optimal locality: a root-to-leaf
 * lookup accesses pages in roughly sequential order after compaction.
 *
 * <p>The old tree is NOT modified — the caller is responsible for publishing
 * the new root and retiring old nodes via the {@link EpochReclaimer}.
 */
public final class Compactor {

    private final SlabAllocator slab;
    private final BumpAllocator bump;
    private final EpochReclaimer reclaimer;
    private final ChunkStore chunkStore; // for resolving arena-allocated pointers

    // Node slab class IDs (same as the tree's)
    private final int prefixClassId;
    private final int node4ClassId;
    private final int node16ClassId;
    private final int node48ClassId;
    private final int node256ClassId;
    private final int keyLen;
    private final int keySlotSize;
    private final int[] leafClassIds;
    private final int[] leafValueSizes;

    public Compactor(SlabAllocator slab, BumpAllocator bump, EpochReclaimer reclaimer,
                     ChunkStore chunkStore,
                     int prefixClassId, int node4ClassId, int node16ClassId,
                     int node48ClassId, int node256ClassId,
                     int keyLen, int keySlotSize, int[] leafClassIds, int[] leafValueSizes) {
        this.slab = slab;
        this.bump = bump;
        this.reclaimer = reclaimer;
        this.chunkStore = chunkStore;
        this.prefixClassId = prefixClassId;
        this.node4ClassId = node4ClassId;
        this.node16ClassId = node16ClassId;
        this.node48ClassId = node48ClassId;
        this.node256ClassId = node256ClassId;
        this.keyLen = keyLen;
        this.keySlotSize = keySlotSize;
        this.leafClassIds = leafClassIds;
        this.leafValueSizes = leafValueSizes;
    }

    /**
     * Result of compaction.
     *
     * @param newRoot    the compacted root pointer
     * @param entryCount total number of leaf entries in the compacted tree
     */
    public record CompactResult(long newRoot, long entryCount) {}

    /**
     * Compact the tree rooted at the given pointer.
     *
     * <p>Walks the tree in post-order (children before parent), allocating
     * new copies of all nodes in the slab allocator. This produces
     * traversal-optimal locality: a root-to-leaf lookup accesses pages
     * in roughly sequential order.
     *
     * <p>The old tree is NOT modified — the caller is responsible for
     * publishing the new root and retiring old nodes.
     *
     * @param root the current root pointer
     * @return the new compacted root and total entry count
     */
    public CompactResult compact(long root) {
        if (NodePtr.isEmpty(root)) return new CompactResult(NodePtr.EMPTY_PTR, 0);
        long[] count = {0};
        long newRoot = compactNode(root, count);
        return new CompactResult(newRoot, count[0]);
    }

    // -----------------------------------------------------------------------
    // Recursive post-order traversal
    // -----------------------------------------------------------------------

    /** Resolve a pointer that may be slab-allocated or arena-allocated. */
    private MemorySegment resolveAny(long ptr) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            int classId = NodePtr.slabClassId(ptr);
            return WriterArena.resolve(chunkStore, ptr, slab.segmentSize(classId));
        }
        return slab.resolve(ptr);
    }

    private long compactNode(long nodePtr, long[] count) {
        if (NodePtr.isEmpty(nodePtr)) return NodePtr.EMPTY_PTR;
        int type = NodePtr.nodeType(nodePtr);

        // Leaf nodes: copy the segment verbatim (overflow data is immutable and shared)
        if (type == NodePtr.LEAF || type == NodePtr.LEAF_INLINE) {
            count[0]++;
            return copyLeaf(nodePtr);
        }

        // Prefix node: compact child first (post-order), then copy prefix
        if (type == NodePtr.PREFIX) {
            MemorySegment seg = resolveAny(nodePtr);
            long child = PrefixNode.child(seg);
            long newChild = compactNode(child, count);
            // Allocate new prefix with the compacted child
            long newPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment newSeg = slab.resolve(newPtr);
            PrefixNode.cowWithChild(newSeg, seg, newChild);
            return newPtr;
        }

        // Inner node: compact all children first (post-order), then copy this node
        MemorySegment seg = resolveAny(nodePtr);
        return switch (type) {
            case NodePtr.NODE_4   -> compactNode4(seg, count);
            case NodePtr.NODE_16  -> compactNode16(seg, count);
            case NodePtr.NODE_48  -> compactNode48(seg, count);
            case NodePtr.NODE_256 -> compactNode256(seg, count);
            default -> {
                System.getLogger(Compactor.class.getName())
                    .log(System.Logger.Level.ERROR, "Unknown node type during compaction: {0}", type);
                throw new IllegalStateException("Unknown node type: " + type);
            }
        };
    }

    // -----------------------------------------------------------------------
    // Leaf copy
    // -----------------------------------------------------------------------

    /**
     * Copy a leaf node. Allocates a new segment of the same slab class,
     * copies the entire segment (key + value) verbatim.
     *
     * <p>Overflow data in the {@link BumpAllocator} (e.g. TaoString overflow)
     * is immutable and shared — overflow pointers remain valid without copying.
     */
    private long copyLeaf(long oldPtr) {
        int classId = NodePtr.slabClassId(oldPtr);
        int nodeType = NodePtr.nodeType(oldPtr);
        int segSize = slab.segmentSize(classId);
        MemorySegment oldSeg = resolveAny(oldPtr);
        long newPtr = slab.allocate(classId, nodeType);
        MemorySegment newSeg = slab.resolve(newPtr);
        MemorySegment.copy(oldSeg, 0, newSeg, 0, segSize);
        return newPtr;
    }

    // -----------------------------------------------------------------------
    // Inner node compaction — one method per node type
    // -----------------------------------------------------------------------

    /**
     * Compact a Node4: read all children, recursively compact each,
     * allocate a new Node4, and populate it with compacted children.
     */
    private long compactNode4(MemorySegment oldSeg, long[] count) {
        int n = Node4.count(oldSeg);
        byte[] keys = new byte[n];
        long[] children = new long[n];
        for (int i = 0; i < n; i++) {
            keys[i] = Node4.keyAt(oldSeg, i);
            children[i] = compactNode(Node4.childAt(oldSeg, i), count);
        }
        long newPtr = slab.allocate(node4ClassId, NodePtr.NODE_4);
        MemorySegment newSeg = slab.resolve(newPtr);
        Node4.init(newSeg);
        for (int i = 0; i < n; i++) {
            Node4.insertChild(newSeg, keys[i], children[i]);
        }
        return newPtr;
    }

    /**
     * Compact a Node16: read all children, recursively compact each,
     * allocate a new Node16, and populate it with compacted children.
     */
    private long compactNode16(MemorySegment oldSeg, long[] count) {
        int n = Node16.count(oldSeg);
        byte[] keys = new byte[n];
        long[] children = new long[n];
        for (int i = 0; i < n; i++) {
            keys[i] = Node16.keyAt(oldSeg, i);
            children[i] = compactNode(Node16.childAt(oldSeg, i), count);
        }
        long newPtr = slab.allocate(node16ClassId, NodePtr.NODE_16);
        MemorySegment newSeg = slab.resolve(newPtr);
        Node16.init(newSeg);
        for (int i = 0; i < n; i++) {
            Node16.insertChild(newSeg, keys[i], children[i]);
        }
        return newPtr;
    }

    /**
     * Compact a Node48: iterate all key-child pairs via {@link Node48#forEach},
     * recursively compact each child, allocate a new Node48, and populate it.
     */
    private long compactNode48(MemorySegment oldSeg, long[] count) {
        // Collect all key-child pairs first — forEach doesn't guarantee
        // a stable order for interleaved allocation, so buffer them.
        int n = Node48.count(oldSeg);
        byte[] keys = new byte[n];
        long[] children = new long[n];
        int[] idx = {0};
        Node48.forEach(oldSeg, (key, child) -> {
            int i = idx[0]++;
            keys[i] = key;
            children[i] = child;
        });
        // Recursively compact each child (post-order)
        for (int i = 0; i < n; i++) {
            children[i] = compactNode(children[i], count);
        }
        long newPtr = slab.allocate(node48ClassId, NodePtr.NODE_48);
        MemorySegment newSeg = slab.resolve(newPtr);
        Node48.init(newSeg);
        for (int i = 0; i < n; i++) {
            Node48.insertChild(newSeg, keys[i], children[i]);
        }
        return newPtr;
    }

    /**
     * Compact a Node256: iterate all key-child pairs via {@link Node256#forEach},
     * recursively compact each child, allocate a new Node256, and populate it.
     */
    private long compactNode256(MemorySegment oldSeg, long[] count) {
        // Collect all key-child pairs first, then compact children.
        int n = Node256.count(oldSeg);
        byte[] keys = new byte[n];
        long[] children = new long[n];
        int[] idx = {0};
        Node256.forEach(oldSeg, (key, child) -> {
            int i = idx[0]++;
            keys[i] = key;
            children[i] = child;
        });
        // Recursively compact each child (post-order)
        for (int i = 0; i < n; i++) {
            children[i] = compactNode(children[i], count);
        }
        long newPtr = slab.allocate(node256ClassId, NodePtr.NODE_256);
        MemorySegment newSeg = slab.resolve(newPtr);
        Node256.init(newSeg);
        for (int i = 0; i < n; i++) {
            Node256.insertChild(newSeg, keys[i], children[i]);
        }
        return newPtr;
    }
}
