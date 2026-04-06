package org.taotree.internal.cow;

import java.lang.foreign.MemorySegment;
import org.taotree.internal.art.Node4;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.NodeConstants;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.art.PrefixNode;

/**
 * COW node-level operations: replace child, insert child (with growth),
 * remove child (with shrink/collapse), and prefix helpers.
 */
final class CowNodeOps {

    private CowNodeOps() {}

    /** COW-replace a child in an inner node, returning the new node pointer. */
    static long replaceChild(CowContext ctx, long oldNodePtr, int type, byte keyByte,
                             long newChild) {
        MemorySegment oldSeg = ctx.resolveAny(oldNodePtr);
        return switch (type) {
            case NodePtr.PREFIX -> {
                long newPtr = ctx.cowAlloc(ctx.prefixClassId, NodePtr.PREFIX);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                PrefixNode.cowWithChild(newSeg, oldSeg, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_4 -> {
                long newPtr = ctx.cowAlloc(ctx.node4ClassId, NodePtr.NODE_4);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node4.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_16 -> {
                long newPtr = ctx.cowAlloc(ctx.node16ClassId, NodePtr.NODE_16);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node16.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_48 -> {
                long newPtr = ctx.cowAlloc(ctx.node48ClassId, NodePtr.NODE_48);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node48.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_256 -> {
                long newPtr = ctx.cowAlloc(ctx.node256ClassId, NodePtr.NODE_256);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node256.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            default -> throw new IllegalStateException("Cannot COW node type: " + type);
        };
    }

    /** COW-insert a child into an inner node, handling growth. */
    static long insertChild(CowContext ctx, long oldNodePtr, int type, byte keyByte,
                            long childPtr) {
        MemorySegment oldSeg = ctx.resolveAny(oldNodePtr);
        return switch (type) {
            case NodePtr.NODE_4 -> {
                if (Node4.isFull(oldSeg)) {
                    long newPtr = ctx.cowAlloc(ctx.node16ClassId, NodePtr.NODE_16);
                    MemorySegment newSeg = ctx.resolveAny(newPtr);
                    Node16.init(newSeg);
                    Node4.growToNode16(newSeg, oldSeg, keyByte, childPtr);
                    yield newPtr;
                }
                long newPtr = ctx.cowAlloc(ctx.node4ClassId, NodePtr.NODE_4);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node4.cowInsertChild(newSeg, oldSeg, keyByte, childPtr);
                yield newPtr;
            }
            case NodePtr.NODE_16 -> {
                if (Node16.isFull(oldSeg)) {
                    long newPtr = ctx.cowAlloc(ctx.node48ClassId, NodePtr.NODE_48);
                    MemorySegment newSeg = ctx.resolveAny(newPtr);
                    Node48.init(newSeg);
                    Node16.growToNode48(newSeg, oldSeg, keyByte, childPtr);
                    yield newPtr;
                }
                long newPtr = ctx.cowAlloc(ctx.node16ClassId, NodePtr.NODE_16);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node16.cowInsertChild(newSeg, oldSeg, keyByte, childPtr);
                yield newPtr;
            }
            case NodePtr.NODE_48 -> {
                if (Node48.isFull(oldSeg)) {
                    long newPtr = ctx.cowAlloc(ctx.node256ClassId, NodePtr.NODE_256);
                    MemorySegment newSeg = ctx.resolveAny(newPtr);
                    Node256.init(newSeg);
                    Node48.growToNode256(newSeg, oldSeg, keyByte, childPtr);
                    yield newPtr;
                }
                long newPtr = ctx.cowAlloc(ctx.node48ClassId, NodePtr.NODE_48);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node48.cowInsertChild(newSeg, oldSeg, keyByte, childPtr);
                yield newPtr;
            }
            case NodePtr.NODE_256 -> {
                long newPtr = ctx.cowAlloc(ctx.node256ClassId, NodePtr.NODE_256);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node256.cowInsertChild(newSeg, oldSeg, keyByte, childPtr);
                yield newPtr;
            }
            default -> throw new IllegalStateException("Cannot insert into node type: " + type);
        };
    }

    /** COW-remove a child, handling shrink and collapse. */
    static long afterRemoval(CowContext ctx, long oldNodePtr, int type, byte keyByte) {
        MemorySegment oldSeg = ctx.resolveAny(oldNodePtr);
        int count = ctx.nodeCount(oldSeg, type);

        if (count <= 1 && type != NodePtr.NODE_256) {
            return NodePtr.EMPTY_PTR;
        }

        int newCount = count - 1;
        return switch (type) {
            case NodePtr.NODE_4 -> {
                if (newCount == 1) {
                    yield collapseSingleChild(ctx, oldSeg, type, keyByte);
                }
                long newPtr = ctx.cowAlloc(ctx.node4ClassId, NodePtr.NODE_4);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node4.cowRemoveChild(newSeg, oldSeg, keyByte);
                yield newPtr;
            }
            case NodePtr.NODE_16 -> {
                if (newCount <= NodeConstants.NODE16_SHRINK_THRESHOLD) {
                    long newPtr = ctx.cowAlloc(ctx.node4ClassId, NodePtr.NODE_4);
                    MemorySegment newSeg = ctx.resolveAny(newPtr);
                    Node4.init(newSeg);
                    int n = Node16.count(oldSeg);
                    for (int i = 0; i < n; i++) {
                        byte k = Node16.keyAt(oldSeg, i);
                        if (k != keyByte) Node4.insertChild(newSeg, k, Node16.childAt(oldSeg, i));
                    }
                    yield newPtr;
                }
                long newPtr = ctx.cowAlloc(ctx.node16ClassId, NodePtr.NODE_16);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node16.cowRemoveChild(newSeg, oldSeg, keyByte);
                yield newPtr;
            }
            case NodePtr.NODE_48 -> {
                if (newCount <= NodeConstants.NODE48_SHRINK_THRESHOLD) {
                    long newPtr = ctx.cowAlloc(ctx.node16ClassId, NodePtr.NODE_16);
                    MemorySegment newSeg = ctx.resolveAny(newPtr);
                    Node16.init(newSeg);
                    Node48.forEach(oldSeg, (k, c) -> {
                        if (k != keyByte) Node16.insertChild(newSeg, k, c);
                    });
                    yield newPtr;
                }
                long newPtr = ctx.cowAlloc(ctx.node48ClassId, NodePtr.NODE_48);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node48.cowRemoveChild(newSeg, oldSeg, keyByte);
                yield newPtr;
            }
            case NodePtr.NODE_256 -> {
                if (newCount <= NodeConstants.NODE256_SHRINK_THRESHOLD) {
                    long newPtr = ctx.cowAlloc(ctx.node48ClassId, NodePtr.NODE_48);
                    MemorySegment newSeg = ctx.resolveAny(newPtr);
                    Node48.init(newSeg);
                    Node256.forEach(oldSeg, (k, c) -> {
                        if (k != keyByte) Node48.insertChild(newSeg, k, c);
                    });
                    yield newPtr;
                }
                long newPtr = ctx.cowAlloc(ctx.node256ClassId, NodePtr.NODE_256);
                MemorySegment newSeg = ctx.resolveAny(newPtr);
                Node256.cowRemoveChild(newSeg, oldSeg, keyByte);
                yield newPtr;
            }
            default -> throw new IllegalStateException("Cannot remove from node type: " + type);
        };
    }

    /** Collapse a node with one remaining child into a prefix. */
    static long collapseSingleChild(CowContext ctx, MemorySegment seg, int type,
                                    byte removedKeyByte) {
        byte[] singleKey = {0};
        long[] singleChild = {0};
        if (type == NodePtr.NODE_4) {
            int n = Node4.count(seg);
            for (int i = 0; i < n; i++) {
                byte k = Node4.keyAt(seg, i);
                if (k != removedKeyByte) {
                    singleKey[0] = k;
                    singleChild[0] = Node4.childAt(seg, i);
                    break;
                }
            }
        }

        long child = singleChild[0];
        byte keyByte = singleKey[0];

        if (NodePtr.nodeType(child) == NodePtr.PREFIX) {
            MemorySegment childPrefSeg = ctx.resolveAny(child);
            int childPrefLen = PrefixNode.count(childPrefSeg);
            byte[] m = new byte[1 + childPrefLen];
            m[0] = keyByte;
            for (int i = 0; i < childPrefLen; i++) m[1 + i] = PrefixNode.keyAt(childPrefSeg, i);
            long grandChild = PrefixNode.child(childPrefSeg);
            return ctx.wrapInPrefixBytes(m, 0, m.length, grandChild);
        }

        long prefPtr = ctx.cowAlloc(ctx.prefixClassId, NodePtr.PREFIX);
        MemorySegment prefSeg = ctx.resolveAny(prefPtr);
        PrefixNode.init(prefSeg, new byte[]{keyByte}, 0, 1, child);
        return prefPtr;
    }

    /** COW-replace a prefix node's child. */
    static long prefixWithChild(CowContext ctx, long oldPrefixPtr, long newChild) {
        long newPtr = ctx.cowAlloc(ctx.prefixClassId, NodePtr.PREFIX);
        MemorySegment newSeg = ctx.resolveAny(newPtr);
        MemorySegment oldSeg = ctx.resolveAny(oldPrefixPtr);
        PrefixNode.cowWithChild(newSeg, oldSeg, newChild);
        return newPtr;
    }

    /** Merge two consecutive prefix nodes into one. */
    static long mergePrefixes(CowContext ctx, long outerPtr, long innerPtr) {
        MemorySegment outerSeg = ctx.resolveAny(outerPtr);
        MemorySegment innerSeg = ctx.resolveAny(innerPtr);
        int outerLen = PrefixNode.count(outerSeg);
        int innerLen = PrefixNode.count(innerSeg);
        long innerChild = PrefixNode.child(innerSeg);

        int totalLen = outerLen + innerLen;
        byte[] merged = new byte[totalLen];
        for (int i = 0; i < outerLen; i++) merged[i] = PrefixNode.keyAt(outerSeg, i);
        for (int i = 0; i < innerLen; i++) merged[outerLen + i] = PrefixNode.keyAt(innerSeg, i);

        return ctx.wrapInPrefixBytes(merged, 0, totalLen, innerChild);
    }
}
