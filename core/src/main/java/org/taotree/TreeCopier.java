package org.taotree;

import java.lang.foreign.MemorySegment;

import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.Node4;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.art.PrefixNode;

/**
 * Deep-copies all (key, value) leaves from {@code source} into {@code target}
 * by walking the source ART and re-inserting each leaf via
 * {@link TaoTree#getOrCreateWith}.
 *
 * <p>Used by {@link TaoTree.WriteScope#copyFrom} for trees whose layout is
 * compatible (identical {@code keyLen}, {@code leafClassCount}, and
 * per-class {@code leafValueSizes}).
 *
 * <p>Extracted from {@code TaoTree.Copier} as part of Phase 8
 * {@code p8-split-taotree}; behaviour is unchanged.
 */
final class TreeCopier {

    private final TaoTree source;
    private final TaoTree target;
    private final TaoTree.WriteState ws;

    TreeCopier(TaoTree source, TaoTree target, TaoTree.WriteState ws) {
        this.source = source;
        this.target = target;
        this.ws = ws;
    }

    void validate() {
        if (source.keyLen != target.keyLen)
            throw new IllegalArgumentException(
                "Key length mismatch: source=" + source.keyLen + " target=" + target.keyLen);
        if (source.leafClassCount != target.leafClassCount)
            throw new IllegalArgumentException(
                "Leaf class count mismatch: source=" + source.leafClassCount
                + " target=" + target.leafClassCount);
        for (int i = 0; i < source.leafClassCount; i++)
            if (source.leafValueSizes[i] != target.leafValueSizes[i])
                throw new IllegalArgumentException(
                    "Leaf value size mismatch at class " + i + ": source="
                    + source.leafValueSizes[i] + " target=" + target.leafValueSizes[i]);
    }

    void copy() {
        if (NodePtr.isEmpty(source.root)) return;
        copyNode(source.root);
    }

    private void copyNode(long nodePtr) {
        if (NodePtr.isEmpty(nodePtr)) return;
        int type = NodePtr.nodeType(nodePtr);

        if (type == NodePtr.LEAF) { copyLeaf(nodePtr); return; }

        if (type == NodePtr.PREFIX) {
            MemorySegment prefSeg = source.resolveNode(nodePtr);
            copyNode(PrefixNode.child(prefSeg));
            return;
        }

        MemorySegment seg = source.resolveNode(nodePtr);
        switch (type) {
            case NodePtr.NODE_4 -> {
                int n = Node4.count(seg);
                for (int i = 0; i < n; i++) copyNode(Node4.childAt(seg, i));
            }
            case NodePtr.NODE_16 -> {
                int n = Node16.count(seg);
                for (int i = 0; i < n; i++) copyNode(Node16.childAt(seg, i));
            }
            case NodePtr.NODE_48 ->
                Node48.forEach(seg, (k, child) -> copyNode(child));
            case NodePtr.NODE_256 ->
                Node256.forEach(seg, (k, child) -> copyNode(child));
        }
    }

    private void copyLeaf(long srcLeafPtr) {
        MemorySegment srcFull = source.resolveNode(srcLeafPtr);
        MemorySegment srcKey = srcFull.asSlice(0, source.keyLen);

        int srcSlabClassId = NodePtr.slabClassId(srcLeafPtr);
        int leafClassIdx = -1;
        for (int i = 0; i < source.leafClassCount; i++) {
            if (source.leafClassIds[i] == srcSlabClassId) { leafClassIdx = i; break; }
        }
        if (leafClassIdx < 0 || leafClassIdx >= target.leafClassCount)
            throw new IllegalStateException(
                "Source leaf class index " + leafClassIdx + " not compatible with target tree");

        long tgtLeafPtr = target.getOrCreateWith(ws, srcKey, source.keyLen, leafClassIdx);

        int tgtSlabClassId = NodePtr.slabClassId(tgtLeafPtr);
        if (tgtSlabClassId != target.leafClassIds[leafClassIdx])
            throw new IllegalStateException(
                "Leaf class conflict for existing key: target slab class " + tgtSlabClassId
                + " != expected " + target.leafClassIds[leafClassIdx]
                + " (leaf class index " + leafClassIdx + ").");

        MemorySegment srcValue = srcFull.asSlice(source.keySlotSize);
        MemorySegment tgtValue = target.leafValueImpl(tgtLeafPtr);

        MemorySegment.copy(srcValue, 0, tgtValue, 0, srcValue.byteSize());
    }
}
