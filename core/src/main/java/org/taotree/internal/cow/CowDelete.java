package org.taotree.internal.cow;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.art.PrefixNode;

/**
 * COW delete operation: deferred path-copy without CAS.
 */
final class CowDelete {

    private CowDelete() {}

    static CowEngine.DeferredResult deferredDelete(
            CowContext ctx, long currentRoot, MemorySegment key, int keyLen) {
        if (NodePtr.isEmpty(currentRoot)) {
            return CowEngine.DeferredResult.unchanged(0);
        }

        var path = CowEngine.PATH_STACK_CACHE.get();
        path.reset();
        long node = currentRoot;
        int depth = 0;

        while (true) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                if (!ctx.leafKeyMatches(node, key, keyLen)) {
                    return CowEngine.DeferredResult.unchanged(0);
                }
                return buildDeleteUp(ctx, path, NodePtr.EMPTY_PTR, node);
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = ctx.resolveAny(node);
                int prefLen = PrefixNode.count(prefSeg);
                int matched = PrefixNode.matchKey(prefSeg, key, keyLen, depth);
                if (matched < prefLen) {
                    return CowEngine.DeferredResult.unchanged(0);
                }
                depth += prefLen;
                path.push(node, (byte) 0, NodePtr.PREFIX);
                node = PrefixNode.child(prefSeg);
                continue;
            }

            if (depth >= keyLen) {
                return CowEngine.DeferredResult.unchanged(0);
            }
            byte keyByte = key.get(ValueLayout.JAVA_BYTE, depth);
            long child = ctx.findChild(node, type, keyByte);
            if (NodePtr.isEmpty(child)) {
                return CowEngine.DeferredResult.unchanged(0);
            }

            path.push(node, keyByte, type);
            node = child;
            depth++;
        }
    }

    // -- Build-up: COW entire path to root after deletion --

    private static CowEngine.DeferredResult buildDeleteUp(
            CowContext ctx, CowEngine.PathStack path, long replacement,
            long originalTarget) {
        long current = replacement;
        var retirees = new LongList();
        ctx.addIfSlabAllocated(retirees, originalTarget);

        for (int i = path.depth - 1; i >= 0; i--) {
            long oldNode = path.nodePtrs[i];
            int type = path.nodeTypes[i];
            byte kb = path.keyBytes[i];

            if (type == NodePtr.PREFIX) {
                if (NodePtr.isEmpty(current)) {
                    ctx.addIfSlabAllocated(retirees, oldNode);
                    continue;
                }
                if (NodePtr.nodeType(current) == NodePtr.PREFIX) {
                    long merged = CowNodeOps.mergePrefixes(ctx, oldNode, current);
                    ctx.addIfSlabAllocated(retirees, oldNode);
                    current = merged;
                } else {
                    current = CowNodeOps.prefixWithChild(ctx, oldNode, current);
                    ctx.addIfSlabAllocated(retirees, oldNode);
                }
                continue;
            }

            if (NodePtr.isEmpty(current)) {
                current = CowNodeOps.afterRemoval(ctx, oldNode, type, kb);
                ctx.addIfSlabAllocated(retirees, oldNode);
                continue;
            }

            current = CowNodeOps.replaceChild(ctx, oldNode, type, kb, current);
            ctx.addIfSlabAllocated(retirees, oldNode);
        }

        return new CowEngine.DeferredResult(0, true, current, -1, retirees, 0);
    }
}
