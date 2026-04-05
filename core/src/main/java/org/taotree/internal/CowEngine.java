package org.taotree.internal;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;

/**
 * Copy-on-write engine for ROWEX-hybrid ART mutations with per-subtree CAS publication.
 *
 * <p>Every structural mutation — insert, delete, node growth, prefix split — allocates
 * new nodes, fully initializes them with plain stores, then makes the result visible by
 * CAS-swapping a parent's child pointer. No published node is ever mutated in place.
 *
 * <p>For per-subtree CAS, the engine looks for the deepest Node256 ancestor in the
 * traversal path. Two writers targeting different key bytes of the same Node256 target
 * different memory addresses — both CAS operations succeed without conflict.
 *
 * <p><b>Thread safety:</b> individual operations are thread-safe via CAS. Multiple
 * threads may call {@link #cowGetOrCreate} and {@link #cowDelete} concurrently.
 * On CAS failure, the caller retries the entire operation.
 */
public final class CowEngine {

    // VarHandle for CAS on MemorySegment child pointer slots (64-bit long at offset)
    private static final VarHandle LONG_HANDLE = ValueLayout.JAVA_LONG.varHandle();

    private final SlabAllocator slab;
    private final EpochReclaimer reclaimer;

    // Node slab class IDs
    private final int prefixClassId;
    private final int node4ClassId;
    private final int node16ClassId;
    private final int node48ClassId;
    private final int node256ClassId;

    // Tree params
    private final int keyLen;
    private final int keySlotSize;
    private final int[] leafClassIds;

    public CowEngine(SlabAllocator slab, EpochReclaimer reclaimer,
                     int prefixClassId, int node4ClassId, int node16ClassId,
                     int node48ClassId, int node256ClassId,
                     int keyLen, int keySlotSize, int[] leafClassIds) {
        this.slab = slab;
        this.reclaimer = reclaimer;
        this.prefixClassId = prefixClassId;
        this.node4ClassId = node4ClassId;
        this.node16ClassId = node16ClassId;
        this.node48ClassId = node48ClassId;
        this.node256ClassId = node256ClassId;
        this.keyLen = keyLen;
        this.keySlotSize = keySlotSize;
        this.leafClassIds = leafClassIds;
    }

    // -----------------------------------------------------------------------
    // Result types
    // -----------------------------------------------------------------------

    /** Result of a COW insert operation. */
    public record InsertResult(
        long leafPtr,         // the leaf pointer (existing or new)
        boolean created,      // true if a new leaf was created
        long newRoot,         // new root if root changed, else EMPTY_PTR
        boolean published     // true if per-subtree CAS succeeded
    ) {}

    /** Result of a COW delete operation. */
    public record DeleteResult(
        boolean deleted,      // true if the key was found and removed
        long newRoot,         // new root if root changed, else EMPTY_PTR
        boolean published     // true if per-subtree CAS succeeded
    ) {}

    // -----------------------------------------------------------------------
    // COW Insert
    // -----------------------------------------------------------------------

    /**
     * Insert a key or return the existing leaf, using COW path-copy.
     *
     * <p>If no Node256 ancestor exists, {@code published} is false and
     * {@code newRoot} contains the new root for the caller to CAS.
     *
     * @param currentRoot the root pointer (from the caller's snapshot)
     * @param key         the binary-comparable key
     * @param keyLen      key length in bytes
     * @param leafClass   leaf class index
     * @return the insert result
     */
    public InsertResult cowGetOrCreate(long currentRoot, MemorySegment key,
                                       int keyLen, int leafClass) {
        if (NodePtr.isEmpty(currentRoot)) {
            // Empty tree — create leaf + prefix
            long leafPtr = allocateLeaf(key, keyLen, leafClass);
            long wrapped = wrapInPrefix(key, 0, keyLen, leafPtr);
            return new InsertResult(leafPtr, true, wrapped, false);
        }

        // Traverse tree, recording path
        var path = new PathStack();
        long node = currentRoot;
        int depth = 0;

        while (true) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                if (leafKeyMatches(node, key, keyLen)) {
                    return new InsertResult(node, false, NodePtr.EMPTY_PTR, true);
                }
                // Expand leaf → Node4 with two children
                var result = cowExpandLeaf(node, key, keyLen, depth, leafClass);
                return publishUp(path, result.subtree(), result.leafPtr(), currentRoot, node);
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = slab.resolve(node);
                int prefLen = PrefixNode.count(prefSeg);
                int matched = PrefixNode.matchKey(prefSeg, key, keyLen, depth);

                if (matched < prefLen) {
                    // Split prefix
                    var result = cowSplitPrefix(node, prefSeg, key, keyLen,
                        depth, matched, leafClass);
                    return publishUp(path, result.subtree(), result.leafPtr(), currentRoot, node);
                }

                depth += prefLen;
                path.push(node, (byte) 0, NodePtr.PREFIX);
                node = PrefixNode.child(prefSeg);
                continue;
            }

            // Inner node
            if (depth >= keyLen) {
                throw new IllegalStateException("Key exhausted at inner node (depth=" + depth + ")");
            }
            byte keyByte = key.get(ValueLayout.JAVA_BYTE, depth);
            long child = findChild(node, type, keyByte);

            if (NodePtr.isEmpty(child)) {
                // Insert new leaf into this node (COW)
                long leafPtr = allocateLeaf(key, keyLen, leafClass);
                long wrapped = wrapInPrefix(key, depth + 1, keyLen, leafPtr);
                long newNode = cowNodeInsertChild(node, type, keyByte, wrapped);
                return publishUp(path, newNode, leafPtr, currentRoot, node);
            }

            path.push(node, keyByte, type);
            node = child;
            depth++;
        }
    }

    // -----------------------------------------------------------------------
    // COW Delete
    // -----------------------------------------------------------------------

    /**
     * Delete a key using COW path-copy.
     *
     * <p>If no Node256 ancestor exists, {@code published} is false and
     * {@code newRoot} contains the new root for the caller to CAS.
     */
    public DeleteResult cowDelete(long currentRoot, MemorySegment key, int keyLen) {
        if (NodePtr.isEmpty(currentRoot)) {
            return new DeleteResult(false, NodePtr.EMPTY_PTR, true);
        }

        var path = new PathStack();
        long node = currentRoot;
        int depth = 0;

        // Traverse to find the key
        while (true) {
            int type = NodePtr.nodeType(node);

            if (type == NodePtr.LEAF) {
                if (!leafKeyMatches(node, key, keyLen)) {
                    return new DeleteResult(false, NodePtr.EMPTY_PTR, true);
                }
                // Found the leaf to delete — don't retire yet, publishDeleteUp handles it
                return publishDeleteUp(path, NodePtr.EMPTY_PTR, currentRoot, node);
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment prefSeg = slab.resolve(node);
                int prefLen = PrefixNode.count(prefSeg);
                int matched = PrefixNode.matchKey(prefSeg, key, keyLen, depth);
                if (matched < prefLen) {
                    return new DeleteResult(false, NodePtr.EMPTY_PTR, true);
                }
                depth += prefLen;
                path.push(node, (byte) 0, NodePtr.PREFIX);
                node = PrefixNode.child(prefSeg);
                continue;
            }

            if (depth >= keyLen) {
                return new DeleteResult(false, NodePtr.EMPTY_PTR, true);
            }
            byte keyByte = key.get(ValueLayout.JAVA_BYTE, depth);
            long child = findChild(node, type, keyByte);
            if (NodePtr.isEmpty(child)) {
                return new DeleteResult(false, NodePtr.EMPTY_PTR, true);
            }

            path.push(node, keyByte, type);
            node = child;
            depth++;
        }
    }

    // -----------------------------------------------------------------------
    // Speculative node cleanup (on CAS failure)
    // -----------------------------------------------------------------------

    /**
     * Retire all speculatively allocated nodes from a failed CAS attempt.
     * Called when a CAS fails and the COW copies are unreachable.
     */
    public void retireSpeculative(long nodePtr) {
        if (NodePtr.isEmpty(nodePtr)) return;
        int type = NodePtr.nodeType(nodePtr);

        if (type == NodePtr.LEAF) {
            reclaimer.retire(nodePtr);
            return;
        }

        if (type == NodePtr.PREFIX) {
            reclaimer.retire(nodePtr);
            // Don't recurse into children — they're existing nodes, not speculative
            return;
        }

        // For inner nodes, retire only the node itself (children are existing)
        reclaimer.retire(nodePtr);
    }

    // -----------------------------------------------------------------------
    // Publication: walk up path and CAS at Node256 or return new root
    // -----------------------------------------------------------------------

    private InsertResult publishUp(PathStack path, long newSubtree, long leafPtr,
                                   long currentRoot, long originalTarget) {
        long current = newSubtree;
        List<Long> oldNodes = new ArrayList<>();
        // The originalTarget is the node being replaced at the bottom of the path.
        // It needs to be retired on success, and is the expected CAS value.
        oldNodes.add(originalTarget);

        // Walk up from bottom, COWing each node
        // Look for the first Node256 to CAS on
        for (int i = path.depth - 1; i >= 0; i--) {
            long oldNode = path.nodePtrs[i];
            int type = path.nodeTypes[i];
            byte kb = path.keyBytes[i];

            if (type == NodePtr.NODE_256) {
                // Per-subtree CAS on this Node256's child slot
                MemorySegment seg = slab.resolve(oldNode);
                long childOffset = Node256.childOffset(kb);
                // Expected value: the original child at this slot
                long expected = (i + 1 < path.depth)
                    ? path.nodePtrs[i + 1]
                    : originalTarget;

                boolean success = (boolean) LONG_HANDLE.compareAndSet(
                    seg, childOffset, expected, current);

                if (success) {
                    // Retire the old COWed nodes (not the Node256 — it's still live)
                    for (long old : oldNodes) {
                        reclaimer.retire(old);
                    }
                    return new InsertResult(leafPtr, true, NodePtr.EMPTY_PTR, true);
                } else {
                    // CAS failed — speculative nodes are unreachable, retire them
                    // But DON'T retire oldNodes — they're still live in the tree
                    return new InsertResult(leafPtr, true, NodePtr.EMPTY_PTR, false);
                }
            }

            // Not Node256 — COW this node
            current = cowNodeReplaceChild(oldNode, type, kb, current);
            oldNodes.add(oldNode);
        }

        // No Node256 found — return new root for the caller to CAS
        // On success, caller should retire oldNodes. Store them for the caller.
        // For now, the caller handles retirement via retireSpeculative on failure.
        return new InsertResult(leafPtr, true, current, false);
    }

    private DeleteResult publishDeleteUp(PathStack path, long replacement,
                                         long currentRoot, long originalTarget) {
        long current = replacement;
        List<Long> oldNodes = new ArrayList<>();
        oldNodes.add(originalTarget); // retire the deleted leaf on success

        for (int i = path.depth - 1; i >= 0; i--) {
            long oldNode = path.nodePtrs[i];
            int type = path.nodeTypes[i];
            byte kb = path.keyBytes[i];

            if (type == NodePtr.PREFIX) {
                // Prefix: replace child with COW
                if (NodePtr.isEmpty(current)) {
                    // Child deleted → remove prefix too
                    oldNodes.add(oldNode);
                    continue;
                }
                // Merge prefixes if child is also a prefix
                if (NodePtr.nodeType(current) == NodePtr.PREFIX) {
                    long merged = cowMergePrefixes(oldNode, current);
                    oldNodes.add(oldNode);
                    current = merged;
                } else {
                    current = cowPrefixWithChild(oldNode, current);
                    oldNodes.add(oldNode);
                }
                continue;
            }

            if (NodePtr.isEmpty(current)) {
                // Child deleted — remove child from this node
                current = cowNodeAfterRemoval(oldNode, type, kb);
                oldNodes.add(oldNode);
                continue;
            }

            if (type == NodePtr.NODE_256) {
                // Per-subtree CAS
                MemorySegment seg = slab.resolve(oldNode);
                long childOffset = Node256.childOffset(kb);
                long expected = (i + 1 < path.depth)
                    ? path.nodePtrs[i + 1]
                    : originalTarget;

                boolean success = (boolean) LONG_HANDLE.compareAndSet(
                    seg, childOffset, expected, current);

                if (success) {
                    for (long old : oldNodes) {
                        reclaimer.retire(old);
                    }
                    return new DeleteResult(true, NodePtr.EMPTY_PTR, true);
                } else {
                    retireSpeculativeChain(current, oldNodes);
                    return new DeleteResult(true, NodePtr.EMPTY_PTR, false);
                }
            }

            // COW: replace child in this node
            current = cowNodeReplaceChild(oldNode, type, kb, current);
            oldNodes.add(oldNode);
        }

        // No Node256 — return new root
        return new DeleteResult(true, current, false);
    }

    private void retireSpeculativeChain(long newNode, List<Long> oldNodesNotToRetire) {
        // Retire only the speculative new nodes (the COW copies)
        // The old nodes are still live in the tree
        // For simplicity, we just retire the top-level new node
        // (its children are existing nodes, not speculative)
        retireSpeculative(newNode);
    }

    // -----------------------------------------------------------------------
    // COW node operations
    // -----------------------------------------------------------------------

    private long cowNodeReplaceChild(long oldNodePtr, int type, byte keyByte,
                                     long newChild) {
        MemorySegment oldSeg = slab.resolve(oldNodePtr);
        return switch (type) {
            case NodePtr.PREFIX -> {
                long newPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
                MemorySegment newSeg = slab.resolve(newPtr);
                PrefixNode.cowWithChild(newSeg, oldSeg, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_4 -> {
                long newPtr = slab.allocate(node4ClassId, NodePtr.NODE_4);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node4.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_16 -> {
                long newPtr = slab.allocate(node16ClassId, NodePtr.NODE_16);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node16.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_48 -> {
                long newPtr = slab.allocate(node48ClassId, NodePtr.NODE_48);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node48.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            case NodePtr.NODE_256 -> {
                long newPtr = slab.allocate(node256ClassId, NodePtr.NODE_256);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node256.cowReplaceChild(newSeg, oldSeg, keyByte, newChild);
                yield newPtr;
            }
            default -> throw new IllegalStateException("Cannot COW node type: " + type);
        };
    }

    /**
     * COW insert a child into an inner node. Handles growth (Node4→Node16, etc.).
     */
    private long cowNodeInsertChild(long oldNodePtr, int type, byte keyByte,
                                    long childPtr) {
        MemorySegment oldSeg = slab.resolve(oldNodePtr);
        return switch (type) {
            case NodePtr.NODE_4 -> {
                if (Node4.isFull(oldSeg)) {
                    // Grow to Node16
                    long newPtr = slab.allocate(node16ClassId, NodePtr.NODE_16);
                    MemorySegment newSeg = slab.resolve(newPtr);
                    Node16.init(newSeg);
                    Node4.growToNode16(newSeg, oldSeg, keyByte, childPtr);
                    yield newPtr;
                }
                long newPtr = slab.allocate(node4ClassId, NodePtr.NODE_4);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node4.cowInsertChild(newSeg, oldSeg, keyByte, childPtr);
                yield newPtr;
            }
            case NodePtr.NODE_16 -> {
                if (Node16.isFull(oldSeg)) {
                    long newPtr = slab.allocate(node48ClassId, NodePtr.NODE_48);
                    MemorySegment newSeg = slab.resolve(newPtr);
                    Node48.init(newSeg);
                    Node16.growToNode48(newSeg, oldSeg, keyByte, childPtr);
                    yield newPtr;
                }
                long newPtr = slab.allocate(node16ClassId, NodePtr.NODE_16);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node16.cowInsertChild(newSeg, oldSeg, keyByte, childPtr);
                yield newPtr;
            }
            case NodePtr.NODE_48 -> {
                if (Node48.isFull(oldSeg)) {
                    long newPtr = slab.allocate(node256ClassId, NodePtr.NODE_256);
                    MemorySegment newSeg = slab.resolve(newPtr);
                    Node256.init(newSeg);
                    Node48.growToNode256(newSeg, oldSeg, keyByte, childPtr);
                    yield newPtr;
                }
                long newPtr = slab.allocate(node48ClassId, NodePtr.NODE_48);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node48.cowInsertChild(newSeg, oldSeg, keyByte, childPtr);
                yield newPtr;
            }
            case NodePtr.NODE_256 -> {
                // Node256 never grows — just insert
                long newPtr = slab.allocate(node256ClassId, NodePtr.NODE_256);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node256.cowInsertChild(newSeg, oldSeg, keyByte, childPtr);
                yield newPtr;
            }
            default -> throw new IllegalStateException("Cannot insert into node type: " + type);
        };
    }

    /**
     * COW remove a child from an inner node. Handles shrink and collapse.
     */
    private long cowNodeAfterRemoval(long oldNodePtr, int type, byte keyByte) {
        MemorySegment oldSeg = slab.resolve(oldNodePtr);
        int count = nodeCount(oldSeg, type);

        // Remove child
        if (count <= 1 && type != NodePtr.NODE_256) {
            // Will become empty after removal — return EMPTY
            reclaimer.retire(oldNodePtr);
            return NodePtr.EMPTY_PTR;
        }

        // Check if we should shrink
        int newCount = count - 1;
        return switch (type) {
            case NodePtr.NODE_4 -> {
                if (newCount == 1) {
                    // Collapse to prefix + single child
                    long collapsed = cowCollapseSingleChild(oldSeg, type, keyByte);
                    yield collapsed;
                }
                long newPtr = slab.allocate(node4ClassId, NodePtr.NODE_4);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node4.cowRemoveChild(newSeg, oldSeg, keyByte);
                yield newPtr;
            }
            case NodePtr.NODE_16 -> {
                if (newCount <= NodeConstants.NODE16_SHRINK_THRESHOLD) {
                    long newPtr = slab.allocate(node4ClassId, NodePtr.NODE_4);
                    MemorySegment newSeg = slab.resolve(newPtr);
                    Node4.init(newSeg);
                    // Copy all children except the removed one
                    int n = Node16.count(oldSeg);
                    for (int i = 0; i < n; i++) {
                        byte k = Node16.keyAt(oldSeg, i);
                        if (k != keyByte) {
                            Node4.insertChild(newSeg, k, Node16.childAt(oldSeg, i));
                        }
                    }
                    yield newPtr;
                }
                long newPtr = slab.allocate(node16ClassId, NodePtr.NODE_16);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node16.cowRemoveChild(newSeg, oldSeg, keyByte);
                yield newPtr;
            }
            case NodePtr.NODE_48 -> {
                if (newCount <= NodeConstants.NODE48_SHRINK_THRESHOLD) {
                    long newPtr = slab.allocate(node16ClassId, NodePtr.NODE_16);
                    MemorySegment newSeg = slab.resolve(newPtr);
                    Node16.init(newSeg);
                    Node48.forEach(oldSeg, (k, c) -> {
                        if (k != keyByte) Node16.insertChild(newSeg, k, c);
                    });
                    yield newPtr;
                }
                long newPtr = slab.allocate(node48ClassId, NodePtr.NODE_48);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node48.cowRemoveChild(newSeg, oldSeg, keyByte);
                yield newPtr;
            }
            case NodePtr.NODE_256 -> {
                if (newCount <= NodeConstants.NODE256_SHRINK_THRESHOLD) {
                    long newPtr = slab.allocate(node48ClassId, NodePtr.NODE_48);
                    MemorySegment newSeg = slab.resolve(newPtr);
                    Node48.init(newSeg);
                    Node256.forEach(oldSeg, (k, c) -> {
                        if (k != keyByte) Node48.insertChild(newSeg, k, c);
                    });
                    yield newPtr;
                }
                long newPtr = slab.allocate(node256ClassId, NodePtr.NODE_256);
                MemorySegment newSeg = slab.resolve(newPtr);
                Node256.cowRemoveChild(newSeg, oldSeg, keyByte);
                yield newPtr;
            }
            default -> throw new IllegalStateException("Cannot remove from node type: " + type);
        };
    }

    private long cowCollapseSingleChild(MemorySegment seg, int type, byte removedKeyByte) {
        // Find the remaining single child
        byte[] singleKey = {0};
        long[] singleChild = {0};

        switch (type) {
            case NodePtr.NODE_4 -> {
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
        }

        long child = singleChild[0];
        byte keyByte = singleKey[0];

        // Wrap in prefix
        if (NodePtr.nodeType(child) == NodePtr.PREFIX) {
            MemorySegment childPrefSeg = slab.resolve(child);
            int childPrefLen = PrefixNode.count(childPrefSeg);
            byte[] m = new byte[1 + childPrefLen];
            m[0] = keyByte;
            for (int i = 0; i < childPrefLen; i++) {
                m[1 + i] = PrefixNode.keyAt(childPrefSeg, i);
            }
            long grandChild = PrefixNode.child(childPrefSeg);
            // Don't retire child prefix — it's still reachable from old tree
            return wrapInPrefixBytes(m, 0, m.length, grandChild);
        }

        long prefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
        MemorySegment prefSeg = slab.resolve(prefPtr);
        PrefixNode.init(prefSeg, new byte[]{keyByte}, 0, 1, child);
        return prefPtr;
    }

    // -----------------------------------------------------------------------
    // COW leaf expansion and prefix split
    // -----------------------------------------------------------------------

    private record SubtreeResult(long subtree, long leafPtr) {}

    private SubtreeResult cowExpandLeaf(long existingLeafPtr, MemorySegment newKey,
                                        int newKeyLen, int depth, int leafClass) {
        MemorySegment existingKey = slab.resolve(existingLeafPtr, keyLen);

        int mismatch = depth;
        while (mismatch < newKeyLen &&
               existingKey.get(ValueLayout.JAVA_BYTE, mismatch) ==
               newKey.get(ValueLayout.JAVA_BYTE, mismatch)) {
            mismatch++;
        }

        if (mismatch >= newKeyLen) {
            throw new IllegalStateException("Duplicate key in cowExpandLeaf");
        }

        long newLeafPtr = allocateLeaf(newKey, newKeyLen, leafClass);

        long n4Ptr = slab.allocate(node4ClassId, NodePtr.NODE_4);
        MemorySegment n4Seg = slab.resolve(n4Ptr);
        Node4.init(n4Seg);

        byte existingByte = existingKey.get(ValueLayout.JAVA_BYTE, mismatch);
        byte newByte = newKey.get(ValueLayout.JAVA_BYTE, mismatch);

        long existingWrapped = wrapInPrefix(existingKey, mismatch + 1, keyLen, existingLeafPtr);
        long newWrapped = wrapInPrefix(newKey, mismatch + 1, newKeyLen, newLeafPtr);

        Node4.insertChild(n4Seg, existingByte, existingWrapped);
        Node4.insertChild(n4Seg, newByte, newWrapped);

        long subtree = wrapInPrefix(newKey, depth, mismatch, n4Ptr);
        return new SubtreeResult(subtree, newLeafPtr);
    }

    private SubtreeResult cowSplitPrefix(long prefixPtr, MemorySegment prefSeg,
                                          MemorySegment key, int keyLen,
                                          int depth, int matchedCount, int leafClass) {
        int prefLen = PrefixNode.count(prefSeg);
        long prefChild = PrefixNode.child(prefSeg);

        long newLeafPtr = allocateLeaf(key, keyLen, leafClass);

        long n4Ptr = slab.allocate(node4ClassId, NodePtr.NODE_4);
        MemorySegment n4Seg = slab.resolve(n4Ptr);
        Node4.init(n4Seg);

        byte existingByte = PrefixNode.keyAt(prefSeg, matchedCount);
        byte newByte = key.get(ValueLayout.JAVA_BYTE, depth + matchedCount);

        long existingChild;
        int remainingPrefixLen = prefLen - matchedCount - 1;
        if (remainingPrefixLen > 0) {
            long newPrefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment newPrefSeg = slab.resolve(newPrefPtr);
            byte[] remaining = new byte[remainingPrefixLen];
            for (int i = 0; i < remainingPrefixLen; i++) {
                remaining[i] = PrefixNode.keyAt(prefSeg, matchedCount + 1 + i);
            }
            PrefixNode.init(newPrefSeg, remaining, 0, remainingPrefixLen, prefChild);
            existingChild = newPrefPtr;
        } else {
            existingChild = prefChild;
        }

        long newWrapped = wrapInPrefix(key, depth + matchedCount + 1, keyLen, newLeafPtr);

        Node4.insertChild(n4Seg, existingByte, existingChild);
        Node4.insertChild(n4Seg, newByte, newWrapped);

        // NOTE: Do NOT retire prefixPtr here. The old prefix is the
        // originalTarget — it will be retired by publishUp on CAS success.

        long subtree;
        if (matchedCount > 0) {
            long wrapPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment wrapSeg = slab.resolve(wrapPtr);
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

    private long cowPrefixWithChild(long oldPrefixPtr, long newChild) {
        long newPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
        MemorySegment newSeg = slab.resolve(newPtr);
        MemorySegment oldSeg = slab.resolve(oldPrefixPtr);
        PrefixNode.cowWithChild(newSeg, oldSeg, newChild);
        return newPtr;
    }

    private long cowMergePrefixes(long outerPtr, long innerPtr) {
        MemorySegment outerSeg = slab.resolve(outerPtr);
        MemorySegment innerSeg = slab.resolve(innerPtr);
        int outerLen = PrefixNode.count(outerSeg);
        int innerLen = PrefixNode.count(innerSeg);
        long innerChild = PrefixNode.child(innerSeg);

        int totalLen = outerLen + innerLen;
        byte[] merged = new byte[totalLen];
        for (int i = 0; i < outerLen; i++) merged[i] = PrefixNode.keyAt(outerSeg, i);
        for (int i = 0; i < innerLen; i++) merged[outerLen + i] = PrefixNode.keyAt(innerSeg, i);

        return wrapInPrefixBytes(merged, 0, totalLen, innerChild);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private long findChild(long nodePtr, int type, byte keyByte) {
        MemorySegment seg = slab.resolve(nodePtr);
        return switch (type) {
            case NodePtr.NODE_4 -> Node4.findChild(seg, keyByte);
            case NodePtr.NODE_16 -> Node16.findChild(seg, keyByte);
            case NodePtr.NODE_48 -> Node48.findChild(seg, keyByte);
            case NodePtr.NODE_256 -> Node256.findChild(seg, keyByte);
            default -> NodePtr.EMPTY_PTR;
        };
    }

    private int nodeCount(MemorySegment seg, int type) {
        return switch (type) {
            case NodePtr.NODE_4 -> Node4.count(seg);
            case NodePtr.NODE_16 -> Node16.count(seg);
            case NodePtr.NODE_48 -> Node48.count(seg);
            case NodePtr.NODE_256 -> Node256.count(seg);
            default -> 0;
        };
    }

    private boolean leafKeyMatches(long leafPtr, MemorySegment key, int keyLen) {
        if (keyLen != this.keyLen) return false;
        MemorySegment leafKey = slab.resolve(leafPtr, this.keyLen);
        return leafKey.mismatch(key.asSlice(0, this.keyLen)) == -1;
    }

    private long allocateLeaf(MemorySegment key, int keyLen, int leafClass) {
        long ptr = slab.allocate(leafClassIds[leafClass]);
        MemorySegment seg = slab.resolve(ptr);
        MemorySegment.copy(key, 0, seg, 0, this.keyLen);
        seg.asSlice(this.keyLen).fill((byte) 0);
        return ptr;
    }

    private long wrapInPrefix(MemorySegment key, int from, int to, long child) {
        if (from >= to) return child;
        int len = to - from;
        if (len <= NodeConstants.PREFIX_CAPACITY) {
            long prefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = slab.resolve(prefPtr);
            PrefixNode.init(prefSeg, key, from, len, child);
            return prefPtr;
        }
        // Chain multiple prefix nodes
        long current = child;
        int pos = to;
        while (pos > from) {
            int chunkLen = Math.min(pos - from, NodeConstants.PREFIX_CAPACITY);
            int chunkStart = pos - chunkLen;
            long prefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = slab.resolve(prefPtr);
            PrefixNode.init(prefSeg, key, chunkStart, chunkLen, current);
            current = prefPtr;
            pos = chunkStart;
        }
        return current;
    }

    private long wrapInPrefixBytes(byte[] key, int from, int to, long child) {
        if (from >= to) return child;
        int len = to - from;
        if (len <= NodeConstants.PREFIX_CAPACITY) {
            long prefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = slab.resolve(prefPtr);
            PrefixNode.init(prefSeg, key, from, len, child);
            return prefPtr;
        }
        long current = child;
        int pos = to;
        while (pos > from) {
            int chunkLen = Math.min(pos - from, NodeConstants.PREFIX_CAPACITY);
            int chunkStart = pos - chunkLen;
            long prefPtr = slab.allocate(prefixClassId, NodePtr.PREFIX);
            MemorySegment prefSeg = slab.resolve(prefPtr);
            PrefixNode.init(prefSeg, key, chunkStart, chunkLen, current);
            current = prefPtr;
            pos = chunkStart;
        }
        return current;
    }

    // -----------------------------------------------------------------------
    // Path stack — lightweight traversal recorder
    // -----------------------------------------------------------------------

    static final class PathStack {
        private static final int MAX_DEPTH = 128;

        final long[] nodePtrs = new long[MAX_DEPTH];
        final byte[] keyBytes = new byte[MAX_DEPTH];
        final int[] nodeTypes = new int[MAX_DEPTH];
        int depth;
        long originalChild; // the child at the modification point (for CAS expected value)

        void push(long nodePtr, byte keyByte, int nodeType) {
            nodePtrs[depth] = nodePtr;
            keyBytes[depth] = keyByte;
            nodeTypes[depth] = nodeType;
            depth++;
        }

        void reset() {
            depth = 0;
            originalChild = NodePtr.EMPTY_PTR;
        }
    }
}
