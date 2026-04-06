package org.taotree.internal.cow;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.Node4;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.NodeConstants;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.art.PrefixNode;

/**
 * Shared state and helper methods for all COW operations.
 *
 * <p>Holds allocator references, node class IDs, and tree parameters.
 * Provides allocation, resolution, and node-traversal helpers used by
 * {@link CowInsert}, {@link CowDelete}, and {@link CowNodeOps}.
 */
final class CowContext {

    final SlabAllocator slab;
    final WriterArena arena;       // null for in-memory mode
    final ChunkStore chunkStore;   // null for in-memory mode

    final int prefixClassId;
    final int node4ClassId;
    final int node16ClassId;
    final int node48ClassId;
    final int node256ClassId;

    final int keyLen;
    final int keySlotSize;
    final int[] leafClassIds;

    CowContext(SlabAllocator slab, WriterArena arena, ChunkStore chunkStore,
                      int prefixClassId, int node4ClassId, int node16ClassId,
                      int node48ClassId, int node256ClassId,
                      int keyLen, int keySlotSize, int[] leafClassIds) {
        this.slab = slab;
        this.arena = arena;
        this.chunkStore = chunkStore;
        this.prefixClassId = prefixClassId;
        this.node4ClassId = node4ClassId;
        this.node16ClassId = node16ClassId;
        this.node48ClassId = node48ClassId;
        this.node256ClassId = node256ClassId;
        this.keyLen = keyLen;
        this.keySlotSize = keySlotSize;
        this.leafClassIds = leafClassIds;
    }

    // -- Allocation --

    long cowAlloc(int classId, int nodeType) {
        if (arena != null) {
            return arena.alloc(slab.segmentSize(classId), nodeType, classId).nodePtr();
        }
        return slab.allocate(classId, nodeType);
    }

    long allocateLeaf(MemorySegment key, int keyLen, int leafClass) {
        int classId = leafClassIds[leafClass];
        long ptr;
        if (arena != null) {
            ptr = arena.alloc(slab.segmentSize(classId), NodePtr.LEAF, classId).nodePtr();
        } else {
            ptr = slab.allocate(classId);
        }
        MemorySegment seg = resolveAny(ptr);
        MemorySegment.copy(key, 0, seg, 0, this.keyLen);
        seg.asSlice(this.keyLen).fill((byte) 0);
        return ptr;
    }

    // -- Resolution --

    MemorySegment resolveAny(long ptr) {
        if (arena != null && WriterArena.isArenaAllocated(ptr)) {
            int classId = NodePtr.slabClassId(ptr);
            return WriterArena.resolve(chunkStore, ptr, slab.segmentSize(classId));
        }
        return slab.resolve(ptr);
    }

    MemorySegment resolveAny(long ptr, int length) {
        if (arena != null && WriterArena.isArenaAllocated(ptr)) {
            return WriterArena.resolve(chunkStore, ptr, length);
        }
        return slab.resolve(ptr, length);
    }

    // -- Traversal helpers --

    long findChild(long nodePtr, int type, byte keyByte) {
        MemorySegment seg = resolveAny(nodePtr);
        return switch (type) {
            case NodePtr.NODE_4 -> Node4.findChild(seg, keyByte);
            case NodePtr.NODE_16 -> Node16.findChild(seg, keyByte);
            case NodePtr.NODE_48 -> Node48.findChild(seg, keyByte);
            case NodePtr.NODE_256 -> Node256.findChild(seg, keyByte);
            default -> NodePtr.EMPTY_PTR;
        };
    }

    int nodeCount(MemorySegment seg, int type) {
        return switch (type) {
            case NodePtr.NODE_4 -> Node4.count(seg);
            case NodePtr.NODE_16 -> Node16.count(seg);
            case NodePtr.NODE_48 -> Node48.count(seg);
            case NodePtr.NODE_256 -> Node256.count(seg);
            default -> 0;
        };
    }

    boolean leafKeyMatches(long leafPtr, MemorySegment key, int keyLen) {
        if (keyLen != this.keyLen) return false;
        MemorySegment leafKey = resolveAny(leafPtr, this.keyLen);
        return leafKey.mismatch(key.asSlice(0, this.keyLen)) == -1;
    }

    // -- Prefix wrapping --

    long wrapInPrefix(MemorySegment key, int from, int to, long child) {
        if (from >= to) return child;
        int len = to - from;
        if (len <= NodeConstants.PREFIX_CAPACITY) {
            long prefPtr = cowAlloc(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = resolveAny(prefPtr);
            PrefixNode.init(prefSeg, key, from, len, child);
            return prefPtr;
        }
        long current = child;
        int pos = to;
        while (pos > from) {
            int chunkLen = Math.min(pos - from, NodeConstants.PREFIX_CAPACITY);
            int chunkStart = pos - chunkLen;
            long prefPtr = cowAlloc(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = resolveAny(prefPtr);
            PrefixNode.init(prefSeg, key, chunkStart, chunkLen, current);
            current = prefPtr;
            pos = chunkStart;
        }
        return current;
    }

    long wrapInPrefixBytes(byte[] key, int from, int to, long child) {
        if (from >= to) return child;
        int len = to - from;
        if (len <= NodeConstants.PREFIX_CAPACITY) {
            long prefPtr = cowAlloc(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = resolveAny(prefPtr);
            PrefixNode.init(prefSeg, key, from, len, child);
            return prefPtr;
        }
        long current = child;
        int pos = to;
        while (pos > from) {
            int chunkLen = Math.min(pos - from, NodeConstants.PREFIX_CAPACITY);
            int chunkStart = pos - chunkLen;
            long prefPtr = cowAlloc(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = resolveAny(prefPtr);
            PrefixNode.init(prefSeg, key, chunkStart, chunkLen, current);
            current = prefPtr;
            pos = chunkStart;
        }
        return current;
    }

    // -- Retire filtering --

    void addIfSlabAllocated(List<Long> retirees, long nodePtr) {
        if (!NodePtr.isEmpty(nodePtr) && (arena == null || !WriterArena.isArenaAllocated(nodePtr))) {
            retirees.add(nodePtr);
        }
    }
}
