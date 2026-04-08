package org.taotree.internal.cow;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.Node4;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.art.PrefixNode;

/**
 * COW insert operation: deferred path-copy without CAS.
 */
final class CowInsert {

    private CowInsert() {}

    static CowEngine.DeferredResult deferredGetOrCreate(
            CowContext ctx, long currentRoot, MemorySegment key,
            int keyLen, int leafClass) {
        return deferredGetOrCreateImpl(ctx, currentRoot, key, keyLen, leafClass, false, false);
    }

    /**
     * Like {@link #deferredGetOrCreate} but always copies existing leaves into the
     * writer's arena so the caller gets a private mutable copy. Used by the
     * deferred-commit WriteScope where leaf writes must be invisible to readers
     * until publication.
     */
    static CowEngine.DeferredResult deferredGetOrCreateCopy(
            CowContext ctx, long currentRoot, MemorySegment key,
            int keyLen, int leafClass) {
        return deferredGetOrCreateImpl(ctx, currentRoot, key, keyLen, leafClass, true, false);
    }

    /**
     * Like {@link #deferredGetOrCreateCopy}, but forces a COW-copy even for
     * arena-allocated leaves. Used during rebase, where arena-allocated leaves
     * may belong to a different thread's arena (the published tree) and must
     * not be modified in place.
     */
    static CowEngine.DeferredResult deferredGetOrCreateForceCopy(
            CowContext ctx, long currentRoot, MemorySegment key,
            int keyLen, int leafClass) {
        return deferredGetOrCreateImpl(ctx, currentRoot, key, keyLen, leafClass, true, true);
    }

    private static CowEngine.DeferredResult deferredGetOrCreateImpl(
            CowContext ctx, long currentRoot, MemorySegment key,
            int keyLen, int leafClass, boolean copyExisting, boolean forceCopy) {
        if (NodePtr.isEmpty(currentRoot)) {
            long leafPtr = ctx.allocateLeaf(key, keyLen, leafClass);
            long wrapped = ctx.wrapInPrefix(key, 0, keyLen, leafPtr);
            return new CowEngine.DeferredResult(leafPtr, true, wrapped, 1, LongList.empty(), 0);
        }

        var path = CowEngine.PATH_STACK_CACHE.get();
        path.reset();
        long node = currentRoot;
        int depth = 0;

        while (true) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                if (ctx.leafKeyMatches(node, key, keyLen)) {
                    if (!copyExisting) {
                        return CowEngine.DeferredResult.unchanged(node);
                    }
                    // If the leaf is already arena-allocated (private from a
                    // prior mutation in this scope), no need to copy again —
                    // unless forceCopy is set (rebase against a published tree
                    // where arena-allocated leaves belong to another writer).
                    if (!forceCopy && WriterArena.isArenaAllocated(node)) {
                        return CowEngine.DeferredResult.unchanged(node);
                    }
                    // COW-copy the existing leaf to give the writer a private copy
                    long originalLeaf = node;
                    long newLeaf = ctx.copyLeaf(node, leafClass);
                    return buildUp(ctx, path, newLeaf, newLeaf, node, 0, originalLeaf);
                }
                var result = expandLeaf(ctx, node, key, keyLen, depth, leafClass);
                return buildUp(ctx, path, result.subtree(), result.leafPtr(), node, 1, 0);
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = ctx.resolveAny(node);
                int prefLen = PrefixNode.count(prefSeg);
                int matched = PrefixNode.matchKey(prefSeg, key, keyLen, depth);

                if (matched < prefLen) {
                    var result = splitPrefix(ctx, node, prefSeg, key, keyLen,
                        depth, matched, leafClass);
                    return buildUp(ctx, path, result.subtree(), result.leafPtr(), node, 1, 0);
                }

                depth += prefLen;
                path.push(node, (byte) 0, NodePtr.PREFIX);
                node = PrefixNode.child(prefSeg);
                continue;
            }

            if (depth >= keyLen) {
                throw new IllegalStateException("Key exhausted at inner node (depth=" + depth + ")");
            }
            byte keyByte = key.get(ValueLayout.JAVA_BYTE, depth);
            long child = ctx.findChild(node, type, keyByte);

            if (NodePtr.isEmpty(child)) {
                long leafPtr = ctx.allocateLeaf(key, keyLen, leafClass);
                long wrapped = ctx.wrapInPrefix(key, depth + 1, keyLen, leafPtr);
                long newNode = CowNodeOps.insertChild(ctx, node, type, keyByte, wrapped);
                return buildUp(ctx, path, newNode, leafPtr, node, 1, 0);
            }

            path.push(node, keyByte, type);
            node = child;
            depth++;
        }
    }

    // -- Build-up: COW entire path to root --

    private static CowEngine.DeferredResult buildUp(
            CowContext ctx, CowEngine.PathStack path, long newSubtree,
            long leafPtr, long originalTarget, int sizeDelta, long originalLeafPtr) {
        long current = newSubtree;
        var retirees = new LongList();
        ctx.addIfSlabAllocated(retirees, originalTarget);

        for (int i = path.depth - 1; i >= 0; i--) {
            long oldNode = path.nodePtrs[i];
            int type = path.nodeTypes[i];
            byte kb = path.keyBytes[i];

            current = CowNodeOps.replaceChild(ctx, oldNode, type, kb, current);
            ctx.addIfSlabAllocated(retirees, oldNode);
        }

        return new CowEngine.DeferredResult(leafPtr, true, current, sizeDelta, retirees, originalLeafPtr);
    }

    // -- Leaf expansion and prefix split --

    record SubtreeResult(long subtree, long leafPtr) {}

    private static SubtreeResult expandLeaf(CowContext ctx, long existingLeafPtr,
                                            MemorySegment newKey, int newKeyLen,
                                            int depth, int leafClass) {
        MemorySegment existingKey = ctx.resolveAny(existingLeafPtr, ctx.keyLen);

        int mismatch = depth;
        while (mismatch < newKeyLen &&
               existingKey.get(ValueLayout.JAVA_BYTE, mismatch) ==
               newKey.get(ValueLayout.JAVA_BYTE, mismatch)) {
            mismatch++;
        }

        if (mismatch >= newKeyLen) {
            throw new IllegalStateException("Duplicate key in cowExpandLeaf");
        }

        long newLeafPtr = ctx.allocateLeaf(newKey, newKeyLen, leafClass);

        long n4Ptr = ctx.cowAlloc(ctx.node4ClassId, NodePtr.NODE_4);
        MemorySegment n4Seg = ctx.resolveAny(n4Ptr);
        Node4.init(n4Seg);

        byte existingByte = existingKey.get(ValueLayout.JAVA_BYTE, mismatch);
        byte newByte = newKey.get(ValueLayout.JAVA_BYTE, mismatch);

        long existingWrapped = ctx.wrapInPrefix(existingKey, mismatch + 1, ctx.keyLen, existingLeafPtr);
        long newWrapped = ctx.wrapInPrefix(newKey, mismatch + 1, newKeyLen, newLeafPtr);

        Node4.insertChild(n4Seg, existingByte, existingWrapped);
        Node4.insertChild(n4Seg, newByte, newWrapped);

        long subtree = ctx.wrapInPrefix(newKey, depth, mismatch, n4Ptr);
        return new SubtreeResult(subtree, newLeafPtr);
    }

    private static SubtreeResult splitPrefix(CowContext ctx, long prefixPtr,
                                             MemorySegment prefSeg, MemorySegment key,
                                             int keyLen, int depth, int matchedCount,
                                             int leafClass) {
        int prefLen = PrefixNode.count(prefSeg);
        long prefChild = PrefixNode.child(prefSeg);

        long newLeafPtr = ctx.allocateLeaf(key, keyLen, leafClass);

        long n4Ptr = ctx.cowAlloc(ctx.node4ClassId, NodePtr.NODE_4);
        MemorySegment n4Seg = ctx.resolveAny(n4Ptr);
        Node4.init(n4Seg);

        byte existingByte = PrefixNode.keyAt(prefSeg, matchedCount);
        byte newByte = key.get(ValueLayout.JAVA_BYTE, depth + matchedCount);

        long existingChild;
        int remainingPrefixLen = prefLen - matchedCount - 1;
        if (remainingPrefixLen > 0) {
            long newPrefPtr = ctx.cowAlloc(ctx.prefixClassId, NodePtr.PREFIX);
            MemorySegment newPrefSeg = ctx.resolveAny(newPrefPtr);
            byte[] remaining = new byte[remainingPrefixLen];
            for (int i = 0; i < remainingPrefixLen; i++) {
                remaining[i] = PrefixNode.keyAt(prefSeg, matchedCount + 1 + i);
            }
            PrefixNode.init(newPrefSeg, remaining, 0, remainingPrefixLen, prefChild);
            existingChild = newPrefPtr;
        } else {
            existingChild = prefChild;
        }

        long newWrapped = ctx.wrapInPrefix(key, depth + matchedCount + 1, keyLen, newLeafPtr);

        Node4.insertChild(n4Seg, existingByte, existingChild);
        Node4.insertChild(n4Seg, newByte, newWrapped);

        long subtree;
        if (matchedCount > 0) {
            long wrapPtr = ctx.cowAlloc(ctx.prefixClassId, NodePtr.PREFIX);
            MemorySegment wrapSeg = ctx.resolveAny(wrapPtr);
            byte[] shared = new byte[matchedCount];
            for (int i = 0; i < matchedCount; i++) {
                shared[i] = key.get(ValueLayout.JAVA_BYTE, depth + i);
            }
            PrefixNode.init(wrapSeg, shared, 0, matchedCount, n4Ptr);
            subtree = wrapPtr;
        } else {
            subtree = n4Ptr;
        }
        return new SubtreeResult(subtree, newLeafPtr);
    }
}
