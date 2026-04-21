package org.taotree;

import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.Node4;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.art.PrefixNode;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.LongPredicate;

/**
 * Package-private bundle of the ART read-path primitives: node resolution,
 * key descent ({@code lookupFrom}), child lookup, leaf-key comparison,
 * and tree/prefix walks. Extracted from {@code TaoTree} to shrink the
 * god-object footprint of the core class; all state is immutable after
 * construction so instances are safe for concurrent readers.
 *
 * <p>{@link BumpAllocator} is accepted for future overflow-payload resolution
 * but not currently read here — kept in the constructor signature so callers
 * that already hold one don't need to refactor later.
 */
final class ArtRead {
    @SuppressWarnings("unused") // reserved for overflow-payload resolution
    private final BumpAllocator bump;
    private final SlabAllocator slab;
    private final ChunkStore chunkStore; // null for child trees (dictionaries)
    private final int keyLen;
    private final int keySlotSize;

    ArtRead(BumpAllocator bump, SlabAllocator slab, ChunkStore chunkStore,
            int keyLen, int keySlotSize) {
        this.bump = bump;
        this.slab = slab;
        this.chunkStore = chunkStore;
        this.keyLen = keyLen;
        this.keySlotSize = keySlotSize;
    }

    // -------------------------------------------------------------------
    // Node resolution
    // -------------------------------------------------------------------

    /** Resolve a NodePtr to a MemorySegment, handling slab and arena pointers. */
    MemorySegment resolveNode(long ptr) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            int classId = NodePtr.slabClassId(ptr);
            return WriterArena.resolve(chunkStore, ptr, slab.segmentSize(classId));
        }
        return slab.resolve(ptr);
    }

    /** Resolve a NodePtr with an explicit length. */
    MemorySegment resolveNode(long ptr, int length) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            return WriterArena.resolve(chunkStore, ptr, length);
        }
        return slab.resolve(ptr, length);
    }

    MemorySegment nodeBaseSegment(long ptr) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            return chunkStore.chunkSegment(WriterArena.page(ptr));
        }
        return slab.backingSegment(ptr);
    }

    long nodeBaseOffset(long ptr) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            int page = WriterArena.page(ptr);
            return chunkStore.chunkByteOffset(page, WriterArena.offsetInPage(ptr));
        }
        return Integer.toUnsignedLong(NodePtr.offset(ptr));
    }

    /** Return the value slice of a leaf, i.e. everything after the key slot. */
    MemorySegment leafValue(long leafPtr) {
        MemorySegment full = resolveNode(leafPtr);
        return full.asSlice(keySlotSize);
    }

    // -------------------------------------------------------------------
    // Child lookup
    // -------------------------------------------------------------------

    long findChild(long nodePtr, int type, byte keyByte) {
        MemorySegment seg = nodeBaseSegment(nodePtr);
        long segOffset = nodeBaseOffset(nodePtr);
        return findChild(seg, segOffset, type, keyByte);
    }

    long findChild(MemorySegment seg, long segOffset, int type, byte keyByte) {
        return switch (type) {
            case NodePtr.NODE_4   -> Node4.findChild(seg, segOffset, keyByte);
            case NodePtr.NODE_16  -> Node16.findChild(seg, segOffset, keyByte);
            case NodePtr.NODE_48  -> Node48.findChild(seg, segOffset, keyByte);
            case NodePtr.NODE_256 -> Node256.findChild(seg, segOffset, keyByte);
            default -> NodePtr.EMPTY_PTR;
        };
    }

    // -------------------------------------------------------------------
    // Leaf key comparison
    // -------------------------------------------------------------------

    boolean leafKeyMatches(long leafPtr, MemorySegment key, int keyLen) {
        if (keyLen != this.keyLen) return false;
        MemorySegment seg = nodeBaseSegment(leafPtr);
        long segOffset = nodeBaseOffset(leafPtr);
        return bytesEqual(seg, segOffset, key, keyLen);
    }

    boolean leafKeyMatches(long leafPtr, byte[] key, int keyLen) {
        if (keyLen != this.keyLen) return false;
        MemorySegment seg = nodeBaseSegment(leafPtr);
        long segOffset = nodeBaseOffset(leafPtr);
        return bytesEqual(seg, segOffset, key, keyLen);
    }

    static boolean bytesEqual(MemorySegment seg, long segOffset, MemorySegment key, int keyLen) {
        int i = 0;
        for (; i + Long.BYTES <= keyLen; i += Long.BYTES) {
            if (seg.get(ValueLayout.JAVA_LONG_UNALIGNED, segOffset + i)
                    != key.get(ValueLayout.JAVA_LONG_UNALIGNED, i)) {
                return false;
            }
        }
        for (; i < keyLen; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, segOffset + i) != key.get(ValueLayout.JAVA_BYTE, i)) {
                return false;
            }
        }
        return true;
    }

    static boolean bytesEqual(MemorySegment seg, long segOffset, byte[] key, int keyLen) {
        for (int i = 0; i < keyLen; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, segOffset + i) != key[i]) {
                return false;
            }
        }
        return true;
    }

    // -------------------------------------------------------------------
    // ART descent
    // -------------------------------------------------------------------

    long lookupFrom(long startRoot, MemorySegment key, int keyLen) {
        long node = startRoot;
        int depth = 0;

        while (!NodePtr.isEmpty(node)) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                return leafKeyMatches(node, key, keyLen) ? node : NodePtr.EMPTY_PTR;
            }

            MemorySegment seg = nodeBaseSegment(node);
            long segOffset = nodeBaseOffset(node);

            if (type == NodePtr.PREFIX) {
                int prefLen = PrefixNode.count(seg, segOffset);
                int matched = PrefixNode.matchKey(seg, segOffset, key, keyLen, depth);
                if (matched < prefLen) {
                    return NodePtr.EMPTY_PTR;
                }
                depth += prefLen;
                node = PrefixNode.child(seg, segOffset);
                continue;
            }

            if (depth >= keyLen) return NodePtr.EMPTY_PTR;
            byte keyByte = key.get(ValueLayout.JAVA_BYTE, depth);
            node = findChild(seg, segOffset, type, keyByte);
            depth++;
        }
        return NodePtr.EMPTY_PTR;
    }

    long lookupFrom(long startRoot, byte[] key, int keyLen) {
        long node = startRoot;
        int depth = 0;

        while (!NodePtr.isEmpty(node)) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                return leafKeyMatches(node, key, keyLen) ? node : NodePtr.EMPTY_PTR;
            }

            MemorySegment seg = nodeBaseSegment(node);
            long segOffset = nodeBaseOffset(node);

            if (type == NodePtr.PREFIX) {
                int prefLen = PrefixNode.count(seg, segOffset);
                int matched = PrefixNode.matchKey(seg, segOffset, key, keyLen, depth);
                if (matched < prefLen) {
                    return NodePtr.EMPTY_PTR;
                }
                depth += prefLen;
                node = PrefixNode.child(seg, segOffset);
                continue;
            }

            if (depth >= keyLen) return NodePtr.EMPTY_PTR;
            node = findChild(seg, segOffset, type, key[depth]);
            depth++;
        }
        return NodePtr.EMPTY_PTR;
    }

    // -------------------------------------------------------------------
    // Walks
    // -------------------------------------------------------------------

    /** Walk every leaf under {@code nodePtr}, calling {@code visitor} with raw leaf pointers. */
    boolean walkLeaves(long nodePtr, LongPredicate visitor) {
        if (NodePtr.isEmpty(nodePtr)) return true;
        int type = NodePtr.nodeType(nodePtr);

        if (type == NodePtr.LEAF) {
            return visitor.test(nodePtr);
        }

        if (type == NodePtr.PREFIX) {
            MemorySegment seg = resolveNode(nodePtr);
            return walkLeaves(PrefixNode.child(seg), visitor);
        }

        MemorySegment seg = resolveNode(nodePtr);
        return switch (type) {
            case NodePtr.NODE_4 -> {
                int n = Node4.count(seg);
                boolean cont = true;
                for (int i = 0; i < n && cont; i++) {
                    cont = walkLeaves(Node4.childAt(seg, i), visitor);
                }
                yield cont;
            }
            case NodePtr.NODE_16 -> {
                int n = Node16.count(seg);
                boolean cont = true;
                for (int i = 0; i < n && cont; i++) {
                    cont = walkLeaves(Node16.childAt(seg, i), visitor);
                }
                yield cont;
            }
            case NodePtr.NODE_48 -> {
                boolean[] cont = {true};
                Node48.forEach(seg, (k, child) -> {
                    if (cont[0]) cont[0] = walkLeaves(child, visitor);
                });
                yield cont[0];
            }
            case NodePtr.NODE_256 -> {
                boolean[] cont = {true};
                Node256.forEach(seg, (k, child) -> {
                    if (cont[0]) cont[0] = walkLeaves(child, visitor);
                });
                yield cont[0];
            }
            default -> true;
        };
    }

    /**
     * Descend matching {@code prefix} then walk all leaves under the matching
     * subtree. Correctly handles prefix-compressed nodes that span or terminate
     * inside the compressed prefix.
     */
    boolean walkPrefixed(long startRoot, MemorySegment prefix, int prefixLen,
                         LongPredicate visitor) {
        if (NodePtr.isEmpty(startRoot)) return true;
        if (prefixLen == 0) return walkLeaves(startRoot, visitor);

        long node = startRoot;
        int depth = 0;

        while (depth < prefixLen) {
            if (NodePtr.isEmpty(node)) return true;
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                MemorySegment leafSeg = resolveNode(node);
                if (MemorySegment.mismatch(leafSeg, depth, prefixLen, prefix, depth, prefixLen) >= 0) {
                    return true;
                }
                return visitor.test(node);
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment seg = resolveNode(node);
                int prefLen = PrefixNode.count(seg);
                int toMatch = Math.min(prefLen, prefixLen - depth);
                if (toMatch > 0 && MemorySegment.mismatch(seg, PrefixNode.OFF_KEYS,
                        PrefixNode.OFF_KEYS + toMatch, prefix, depth, depth + toMatch) >= 0) {
                    return true;
                }
                depth += prefLen;
                node = PrefixNode.child(seg);
                continue;
            }

            byte keyByte = prefix.get(ValueLayout.JAVA_BYTE, depth);
            MemorySegment seg = resolveNode(node);
            long child = switch (type) {
                case NodePtr.NODE_4   -> Node4.findChild(seg, keyByte);
                case NodePtr.NODE_16  -> Node16.findChild(seg, keyByte);
                case NodePtr.NODE_48  -> Node48.findChild(seg, keyByte);
                case NodePtr.NODE_256 -> Node256.findChild(seg, keyByte);
                default -> NodePtr.EMPTY_PTR;
            };
            if (NodePtr.isEmpty(child)) return true;
            depth++;
            node = child;
        }

        return walkLeaves(node, visitor);
    }
}
