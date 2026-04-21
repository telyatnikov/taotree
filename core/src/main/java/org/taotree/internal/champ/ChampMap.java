package org.taotree.internal.champ;

import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.OverflowPtr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap CHAMP (Compressed Hash-Array Mapped Prefix-tree) persistent map.
 *
 * <p>Maps {@code attr_id} (uint32) → {@code value_ref} (long). All operations
 * are static and operate on pointers to off-heap nodes stored in a
 * {@link BumpAllocator}. Nodes are immutable once published. {@link #put} and
 * {@link #remove} return new root pointers with structural sharing — only the
 * path from root to the affected entry is copied.
 *
 * <p>Based on Steindorfer's CHAMP encoding (<i>Efficient Immutable
 * Collections</i>, 2017), adapted for off-heap FFM storage. Reference
 * implementation: <a href="https://github.com/usethesource/capsule">capsule</a>.
 *
 * <h3>Node layout (off-heap, variable-size, in BumpAllocator):</h3>
 * <pre>
 * Offset        Size          Field
 * ──────        ────          ─────
 * 0             4             dataMap  (uint32 bitmap: positions with inline data)
 * 4             4             nodeMap  (uint32 bitmap: positions with child pointers)
 * 8             12 × D        data entries  [D = popcount(dataMap)]
 *                               each: [attr_id:4B | value_ref:8B]
 * 8 + 12×D     8 × C         child pointers  [C = popcount(nodeMap)]
 *                               each: 8B OverflowPtr to child node
 * </pre>
 *
 * <h3>Hash function:</h3>
 * <p>Identity: {@code hash(attr_id) = attr_id}. Dictionary codes are sequential
 * uint32 values, consumed 5 bits per level (max 7 levels). Since attr_id IS the
 * hash, hash collisions between distinct keys are impossible.
 */
public final class ChampMap {

    private ChampMap() {} // static utility class

    // ── Sentinel constants ──────────────────────────────────────────────

    /** Empty map root pointer. */
    public static final long EMPTY_ROOT = OverflowPtr.EMPTY_PTR;

    /** Returned by {@link #get} when the key is not present. */
    public static final long NOT_FOUND = -1L;

    // ── Node layout constants ───────────────────────────────────────────

    private static final int DATAMAP_OFFSET = 0;
    private static final int NODEMAP_OFFSET = 4;
    static final int HEADER_SIZE = 8;

    private static final int ENTRY_KEY_SIZE = 4;   // uint32 attr_id
    private static final int ENTRY_VAL_SIZE = 8;   // long value_ref
    static final int ENTRY_SIZE = 12;               // attr_id + value_ref

    static final int CHILD_PTR_SIZE = 8;            // OverflowPtr

    // ── Trie parameters ─────────────────────────────────────────────────

    private static final int BIT_PARTITION_SIZE = 5;
    private static final int BIT_PARTITION_MASK = 0x1F;
    private static final int MAX_DEPTH = 7; // ceil(32 / 5)

    // ── Unaligned layouts (entries may not be 8-byte aligned) ────────────

    private static final ValueLayout.OfInt    LAYOUT_INT  = ValueLayout.JAVA_INT_UNALIGNED;
    private static final ValueLayout.OfLong   LAYOUT_LONG = ValueLayout.JAVA_LONG_UNALIGNED;

    // ── Mutable result holder for put / remove ──────────────────────────

    /**
     * Mutable result holder, reused across a single put/remove call.
     * <p>Callers inspect this after {@link #put} or {@link #remove} to determine
     * whether the map was modified and what the old value was.
     */
    public static final class Result {
        /** True if the map structure was modified. */
        public boolean modified;
        /** True if an existing entry's value was replaced (key already present). */
        public boolean replaced;
        /** The previous value_ref if {@code replaced} is true, else undefined. */
        public long oldValueRef;

        public void reset() {
            modified = false;
            replaced = false;
            oldValueRef = NOT_FOUND;
        }
    }

    // ====================================================================
    // get
    // ====================================================================

    /**
     * Look up an attribute's value reference in the map.
     *
     * @param alloc  the BumpAllocator containing CHAMP nodes
     * @param root   root pointer (or {@link #EMPTY_ROOT})
     * @param attrId the attribute ID to look up
     * @return the value_ref, or {@link #NOT_FOUND} if the key is absent
     */
    public static long get(BumpAllocator alloc, long root, int attrId) {
        if (root == EMPTY_ROOT) return NOT_FOUND;

        long nodePtr = root;
        int hash = attrId;

        for (int level = 0; level < MAX_DEPTH; level++) {
            MemorySegment node = resolveNode(alloc, nodePtr);

            int dataMap = node.get(LAYOUT_INT, DATAMAP_OFFSET);
            int nodeMap = node.get(LAYOUT_INT, NODEMAP_OFFSET);

            int idx = (hash >>> (level * BIT_PARTITION_SIZE)) & BIT_PARTITION_MASK;
            int bit = 1 << idx;

            if ((dataMap & bit) != 0) {
                // Inline data at this position
                int dataIndex = Integer.bitCount(dataMap & (bit - 1));
                int entryOffset = HEADER_SIZE + dataIndex * ENTRY_SIZE;
                int storedAttrId = node.get(LAYOUT_INT, entryOffset);
                if (storedAttrId == attrId) {
                    return node.get(LAYOUT_LONG, entryOffset + ENTRY_KEY_SIZE);
                }
                return NOT_FOUND; // hash prefix matches but key differs
            }

            if ((nodeMap & bit) != 0) {
                // Child node at this position — descend
                int D = Integer.bitCount(dataMap);
                int childIndex = Integer.bitCount(nodeMap & (bit - 1));
                int childPtrOffset = HEADER_SIZE + D * ENTRY_SIZE + childIndex * CHILD_PTR_SIZE;
                nodePtr = node.get(LAYOUT_LONG, childPtrOffset);
                continue;
            }

            return NOT_FOUND; // position empty
        }

        // Max depth reached — impossible with identity hash on unique uint32 keys
        return NOT_FOUND;
    }

    // ====================================================================
    // put
    // ====================================================================

    /**
     * Insert or update an entry, returning a new root with structural sharing.
     *
     * <p>If the key is already present with the same value, returns the
     * original root unchanged (canonical identity preservation).
     *
     * @param alloc    the BumpAllocator for node allocation and resolution
     * @param root     current root pointer (or {@link #EMPTY_ROOT})
     * @param attrId   the attribute ID to insert/update
     * @param valueRef the value reference to associate
     * @param result   mutable result holder (call {@link Result#reset()} first)
     * @return the new root pointer
     */
    public static long put(BumpAllocator alloc, long root, int attrId, long valueRef,
                           Result result) {
        if (root == EMPTY_ROOT) {
            // Create single-entry root node
            result.modified = true;
            return allocSingleEntryNode(alloc, attrId, valueRef,
                    bitpos(attrId & BIT_PARTITION_MASK));
        }
        return putRec(alloc, root, attrId, valueRef, attrId, 0, result);
    }

    private static long putRec(BumpAllocator alloc, long nodePtr, int attrId, long valueRef,
                                int hash, int level, Result result) {
        MemorySegment node = resolveNode(alloc, nodePtr);
        int dataMap = node.get(LAYOUT_INT, DATAMAP_OFFSET);
        int nodeMap = node.get(LAYOUT_INT, NODEMAP_OFFSET);

        int idx = (hash >>> (level * BIT_PARTITION_SIZE)) & BIT_PARTITION_MASK;
        int bit = 1 << idx;

        if ((dataMap & bit) != 0) {
            // Inline data at this position
            int dataIndex = Integer.bitCount(dataMap & (bit - 1));
            int entryOffset = HEADER_SIZE + dataIndex * ENTRY_SIZE;
            int existingAttrId = node.get(LAYOUT_INT, entryOffset);

            if (existingAttrId == attrId) {
                // Same key — check if value changed
                long existingValueRef = node.get(LAYOUT_LONG, entryOffset + ENTRY_KEY_SIZE);
                if (existingValueRef == valueRef) {
                    return nodePtr; // no change — canonical identity preservation
                }
                // Replace value: copy node with updated entry
                result.modified = true;
                result.replaced = true;
                result.oldValueRef = existingValueRef;
                return copyAndSetValue(alloc, node, dataMap, nodeMap, dataIndex, valueRef);
            }

            // Different key at same position — push both entries down one level
            long existingValueRef = node.get(LAYOUT_LONG, entryOffset + ENTRY_KEY_SIZE);
            long subNode = mergeTwoEntries(alloc, existingAttrId, existingValueRef,
                    attrId, valueRef, level + 1);
            result.modified = true;

            // Replace inline data with child pointer
            return copyAndMigrateFromInlineToNode(alloc, node, dataMap, nodeMap,
                    dataIndex, bit, subNode);
        }

        if ((nodeMap & bit) != 0) {
            // Child node at this position — recurse
            int D = Integer.bitCount(dataMap);
            int childIndex = Integer.bitCount(nodeMap & (bit - 1));
            int childPtrOffset = HEADER_SIZE + D * ENTRY_SIZE + childIndex * CHILD_PTR_SIZE;
            long oldChild = node.get(LAYOUT_LONG, childPtrOffset);

            long newChild = putRec(alloc, oldChild, attrId, valueRef, hash, level + 1, result);

            if (newChild == oldChild) {
                return nodePtr; // no change in subtree
            }

            return copyAndSetChild(alloc, node, dataMap, nodeMap, childIndex, newChild);
        }

        // Empty position — insert inline data
        result.modified = true;
        return copyAndInsertEntry(alloc, node, dataMap, nodeMap, bit, attrId, valueRef);
    }

    // ====================================================================
    // remove
    // ====================================================================

    /**
     * Remove an entry, returning a new root with structural sharing.
     *
     * <p>If the key is not present, returns the original root unchanged.
     *
     * @param alloc  the BumpAllocator for node allocation and resolution
     * @param root   current root pointer (or {@link #EMPTY_ROOT})
     * @param attrId the attribute ID to remove
     * @param result mutable result holder (call {@link Result#reset()} first)
     * @return the new root pointer
     */
    public static long remove(BumpAllocator alloc, long root, int attrId, Result result) {
        if (root == EMPTY_ROOT) return EMPTY_ROOT;
        return removeRec(alloc, root, attrId, attrId, 0, result, true);
    }

    private static long removeRec(BumpAllocator alloc, long nodePtr, int attrId,
                                   int hash, int level, Result result, boolean isRoot) {
        MemorySegment node = resolveNode(alloc, nodePtr);
        int dataMap = node.get(LAYOUT_INT, DATAMAP_OFFSET);
        int nodeMap = node.get(LAYOUT_INT, NODEMAP_OFFSET);

        int idx = (hash >>> (level * BIT_PARTITION_SIZE)) & BIT_PARTITION_MASK;
        int bit = 1 << idx;

        if ((dataMap & bit) != 0) {
            int dataIndex = Integer.bitCount(dataMap & (bit - 1));
            int entryOffset = HEADER_SIZE + dataIndex * ENTRY_SIZE;
            int storedAttrId = node.get(LAYOUT_INT, entryOffset);

            if (storedAttrId != attrId) {
                return nodePtr; // key not present
            }

            result.modified = true;
            result.replaced = true;
            result.oldValueRef = node.get(LAYOUT_LONG, entryOffset + ENTRY_KEY_SIZE);

            int D = Integer.bitCount(dataMap);
            int C = Integer.bitCount(nodeMap);

            if (D == 1 && C == 0) {
                // Removing the last entry — node becomes empty
                return EMPTY_ROOT;
            }

            if (D == 2 && C == 0 && !isRoot) {
                // Node has exactly 2 data entries, 0 children, and is not the root.
                // After removing one, it becomes a singleton.
                // Return the singleton node — the parent will inline it.
                int remainingIndex = (dataIndex == 0) ? 1 : 0;
                int remainingOffset = HEADER_SIZE + remainingIndex * ENTRY_SIZE;
                int remainingAttrId = node.get(LAYOUT_INT, remainingOffset);
                long remainingValueRef = node.get(LAYOUT_LONG, remainingOffset + ENTRY_KEY_SIZE);
                // Recompute the dataMap bit for the remaining entry at level 0
                // (the parent will place it at the correct position)
                int remainingBit = bitpos(remainingAttrId & BIT_PARTITION_MASK);
                return allocSingleEntryNode(alloc, remainingAttrId, remainingValueRef,
                        remainingBit);
            }

            return copyAndRemoveEntry(alloc, node, dataMap, nodeMap, dataIndex, bit);
        }

        if ((nodeMap & bit) != 0) {
            int D = Integer.bitCount(dataMap);
            int childIndex = Integer.bitCount(nodeMap & (bit - 1));
            int childPtrOffset = HEADER_SIZE + D * ENTRY_SIZE + childIndex * CHILD_PTR_SIZE;
            long oldChild = node.get(LAYOUT_LONG, childPtrOffset);

            long newChild = removeRec(alloc, oldChild, attrId, hash, level + 1, result, false);

            if (newChild == oldChild) {
                return nodePtr; // key not found in subtree
            }

            if (newChild == EMPTY_ROOT) {
                // Child became empty — remove child pointer
                return copyAndRemoveChild(alloc, node, dataMap, nodeMap, childIndex, bit);
            }

            // Check if child is now a singleton → inline it (CHAMP compaction rule)
            if (isSingleton(alloc, newChild)) {
                return copyAndMigrateFromNodeToInline(alloc, node, dataMap, nodeMap,
                        childIndex, bit, newChild);
            }

            return copyAndSetChild(alloc, node, dataMap, nodeMap, childIndex, newChild);
        }

        return nodePtr; // position empty — key not present
    }

    // ====================================================================
    // iterate
    // ====================================================================

    /**
     * Iterate all entries in the map, visiting each (attr_id, value_ref) pair.
     *
     * <p>Entries are visited in bitmap order (deterministic but not sorted by attr_id).
     *
     * @param alloc   the BumpAllocator containing CHAMP nodes
     * @param root    root pointer (or {@link #EMPTY_ROOT})
     * @param visitor the visitor to call for each entry
     * @return {@code true} if iteration completed, {@code false} if stopped early
     */
    public static boolean iterate(BumpAllocator alloc, long root, ChampVisitor visitor) {
        if (root == EMPTY_ROOT) return true;
        return iterateRec(alloc, root, visitor);
    }

    private static boolean iterateRec(BumpAllocator alloc, long nodePtr, ChampVisitor visitor) {
        // Fast path: read directly from the backing page without asSlice. This matters
        // for iterate throughput since every recursive step otherwise allocates a fresh
        // MemorySegment wrapper via resolveNode's two-pass resolve. See
        // docs/design-rationale.md §7 / p8-champ-iterate-perf.
        int pageId = OverflowPtr.pageId(nodePtr);
        int offset = OverflowPtr.offset(nodePtr);
        MemorySegment page = alloc.page(pageId);

        int dataMap = page.get(LAYOUT_INT, offset + DATAMAP_OFFSET);
        int nodeMap = page.get(LAYOUT_INT, offset + NODEMAP_OFFSET);

        int D = Integer.bitCount(dataMap);
        int C = Integer.bitCount(nodeMap);

        int entriesBase = offset + HEADER_SIZE;
        for (int i = 0; i < D; i++) {
            int entryOff = entriesBase + i * ENTRY_SIZE;
            int attrId = page.get(LAYOUT_INT, entryOff);
            long valueRef = page.get(LAYOUT_LONG, entryOff + ENTRY_KEY_SIZE);
            if (!visitor.visit(attrId, valueRef)) {
                return false; // early termination
            }
        }

        int childrenBase = entriesBase + D * ENTRY_SIZE;
        for (int i = 0; i < C; i++) {
            long childPtr = page.get(LAYOUT_LONG, childrenBase + i * CHILD_PTR_SIZE);
            if (!iterateRec(alloc, childPtr, visitor)) {
                return false;
            }
        }

        return true;
    }

    // ====================================================================
    // size
    // ====================================================================

    /**
     * Count the number of entries in the map (O(n) tree walk).
     *
     * <p>For frequently needed sizes, callers should cache the count externally
     * and update it based on {@link Result#modified} / {@link Result#replaced}.
     */
    public static int size(BumpAllocator alloc, long root) {
        if (root == EMPTY_ROOT) return 0;
        int[] count = {0};
        iterate(alloc, root, (attrId, valueRef) -> { count[0]++; return true; });
        return count[0];
    }

    // ====================================================================
    // Node resolution (two-pass: header then full)
    // ====================================================================

    /**
     * Resolve a CHAMP node from its pointer. Reads the 8-byte header first to
     * compute the full node size, then resolves the complete node.
     */
    static MemorySegment resolveNode(BumpAllocator alloc, long nodePtr) {
        MemorySegment header = alloc.resolve(nodePtr, HEADER_SIZE);
        int dataMap = header.get(LAYOUT_INT, DATAMAP_OFFSET);
        int nodeMap = header.get(LAYOUT_INT, NODEMAP_OFFSET);
        int fullSize = nodeSize(dataMap, nodeMap);
        return alloc.resolve(nodePtr, fullSize);
    }

    /** Compute the byte size of a CHAMP node from its bitmaps. */
    static int nodeSize(int dataMap, int nodeMap) {
        return HEADER_SIZE
                + Integer.bitCount(dataMap) * ENTRY_SIZE
                + Integer.bitCount(nodeMap) * CHILD_PTR_SIZE;
    }

    // ====================================================================
    // Node allocation helpers
    // ====================================================================

    /** Allocate a single-entry node (1 data entry, 0 children). */
    private static long allocSingleEntryNode(BumpAllocator alloc, int attrId, long valueRef,
                                              int dataMapBit) {
        int size = HEADER_SIZE + ENTRY_SIZE;
        long ptr = alloc.allocate(size);
        MemorySegment node = alloc.resolve(ptr, size);
        node.set(LAYOUT_INT, DATAMAP_OFFSET, dataMapBit);
        node.set(LAYOUT_INT, NODEMAP_OFFSET, 0);
        node.set(LAYOUT_INT, HEADER_SIZE, attrId);
        node.set(LAYOUT_LONG, HEADER_SIZE + ENTRY_KEY_SIZE, valueRef);
        return ptr;
    }

    /**
     * Create a sub-node containing two entries that collided at a higher level.
     * Recurses deeper if they collide again at this level.
     */
    private static long mergeTwoEntries(BumpAllocator alloc,
                                         int attrId0, long valueRef0,
                                         int attrId1, long valueRef1,
                                         int level) {
        int hash0 = attrId0;
        int hash1 = attrId1;
        int idx0 = (hash0 >>> (level * BIT_PARTITION_SIZE)) & BIT_PARTITION_MASK;
        int idx1 = (hash1 >>> (level * BIT_PARTITION_SIZE)) & BIT_PARTITION_MASK;

        if (idx0 != idx1) {
            // Both entries fit at this level
            int bit0 = 1 << idx0;
            int bit1 = 1 << idx1;
            int dataMap = bit0 | bit1;
            int size = HEADER_SIZE + 2 * ENTRY_SIZE;
            long ptr = alloc.allocate(size);
            MemorySegment node = alloc.resolve(ptr, size);
            node.set(LAYOUT_INT, DATAMAP_OFFSET, dataMap);
            node.set(LAYOUT_INT, NODEMAP_OFFSET, 0);

            // Data entries are stored in bitmap order (lower bit index first)
            int first, second;
            int firstAttrId, secondAttrId;
            long firstValueRef, secondValueRef;
            if (idx0 < idx1) {
                firstAttrId = attrId0; firstValueRef = valueRef0;
                secondAttrId = attrId1; secondValueRef = valueRef1;
            } else {
                firstAttrId = attrId1; firstValueRef = valueRef1;
                secondAttrId = attrId0; secondValueRef = valueRef0;
            }
            node.set(LAYOUT_INT, HEADER_SIZE, firstAttrId);
            node.set(LAYOUT_LONG, HEADER_SIZE + ENTRY_KEY_SIZE, firstValueRef);
            node.set(LAYOUT_INT, HEADER_SIZE + ENTRY_SIZE, secondAttrId);
            node.set(LAYOUT_LONG, HEADER_SIZE + ENTRY_SIZE + ENTRY_KEY_SIZE, secondValueRef);
            return ptr;
        }

        // Same index at this level — recurse deeper
        long childPtr = mergeTwoEntries(alloc, attrId0, valueRef0, attrId1, valueRef1, level + 1);
        int nodeMap = 1 << idx0;
        int size = HEADER_SIZE + CHILD_PTR_SIZE;
        long ptr = alloc.allocate(size);
        MemorySegment node = alloc.resolve(ptr, size);
        node.set(LAYOUT_INT, DATAMAP_OFFSET, 0);
        node.set(LAYOUT_INT, NODEMAP_OFFSET, nodeMap);
        node.set(LAYOUT_LONG, HEADER_SIZE, childPtr);
        return ptr;
    }

    // ====================================================================
    // Copy-and-mutate helpers (structural sharing)
    // ====================================================================

    /** Copy node with one data entry's value_ref replaced. */
    private static long copyAndSetValue(BumpAllocator alloc, MemorySegment oldNode,
                                         int dataMap, int nodeMap,
                                         int dataIndex, long newValueRef) {
        int size = nodeSize(dataMap, nodeMap);
        long ptr = alloc.allocate(size);
        MemorySegment newNode = alloc.resolve(ptr, size);
        MemorySegment.copy(oldNode, 0, newNode, 0, size);
        int entryOffset = HEADER_SIZE + dataIndex * ENTRY_SIZE + ENTRY_KEY_SIZE;
        newNode.set(LAYOUT_LONG, entryOffset, newValueRef);
        return ptr;
    }

    /** Copy node with one child pointer replaced. */
    private static long copyAndSetChild(BumpAllocator alloc, MemorySegment oldNode,
                                         int dataMap, int nodeMap,
                                         int childIndex, long newChildPtr) {
        int size = nodeSize(dataMap, nodeMap);
        long ptr = alloc.allocate(size);
        MemorySegment newNode = alloc.resolve(ptr, size);
        MemorySegment.copy(oldNode, 0, newNode, 0, size);
        int D = Integer.bitCount(dataMap);
        int childPtrOffset = HEADER_SIZE + D * ENTRY_SIZE + childIndex * CHILD_PTR_SIZE;
        newNode.set(LAYOUT_LONG, childPtrOffset, newChildPtr);
        return ptr;
    }

    /** Copy node with a new data entry inserted at the position indicated by bit. */
    private static long copyAndInsertEntry(BumpAllocator alloc, MemorySegment oldNode,
                                            int dataMap, int nodeMap,
                                            int bit, int attrId, long valueRef) {
        int D = Integer.bitCount(dataMap);
        int C = Integer.bitCount(nodeMap);
        int oldSize = HEADER_SIZE + D * ENTRY_SIZE + C * CHILD_PTR_SIZE;
        int newSize = oldSize + ENTRY_SIZE;

        int dataIndex = Integer.bitCount(dataMap & (bit - 1));
        int insertOffset = HEADER_SIZE + dataIndex * ENTRY_SIZE;

        long ptr = alloc.allocate(newSize);
        MemorySegment newNode = alloc.resolve(ptr, newSize);

        // Header with updated dataMap
        newNode.set(LAYOUT_INT, DATAMAP_OFFSET, dataMap | bit);
        newNode.set(LAYOUT_INT, NODEMAP_OFFSET, nodeMap);

        // Data entries before insertion point
        if (insertOffset > HEADER_SIZE) {
            MemorySegment.copy(oldNode, HEADER_SIZE, newNode, HEADER_SIZE,
                    insertOffset - HEADER_SIZE);
        }

        // New entry
        newNode.set(LAYOUT_INT, insertOffset, attrId);
        newNode.set(LAYOUT_LONG, insertOffset + ENTRY_KEY_SIZE, valueRef);

        // Data entries after insertion point + all child pointers
        int tailSrc = insertOffset;
        int tailDst = insertOffset + ENTRY_SIZE;
        int tailLen = oldSize - tailSrc;
        if (tailLen > 0) {
            MemorySegment.copy(oldNode, tailSrc, newNode, tailDst, tailLen);
        }

        return ptr;
    }

    /** Copy node with a data entry removed at the position indicated by bit. */
    private static long copyAndRemoveEntry(BumpAllocator alloc, MemorySegment oldNode,
                                            int dataMap, int nodeMap,
                                            int dataIndex, int bit) {
        int D = Integer.bitCount(dataMap);
        int C = Integer.bitCount(nodeMap);
        int oldSize = HEADER_SIZE + D * ENTRY_SIZE + C * CHILD_PTR_SIZE;
        int newSize = oldSize - ENTRY_SIZE;

        int removeOffset = HEADER_SIZE + dataIndex * ENTRY_SIZE;

        long ptr = alloc.allocate(newSize);
        MemorySegment newNode = alloc.resolve(ptr, newSize);

        // Header with updated dataMap
        newNode.set(LAYOUT_INT, DATAMAP_OFFSET, dataMap ^ bit);
        newNode.set(LAYOUT_INT, NODEMAP_OFFSET, nodeMap);

        // Data entries before removal point
        if (removeOffset > HEADER_SIZE) {
            MemorySegment.copy(oldNode, HEADER_SIZE, newNode, HEADER_SIZE,
                    removeOffset - HEADER_SIZE);
        }

        // Data entries after removal point + all child pointers
        int tailSrc = removeOffset + ENTRY_SIZE;
        int tailDst = removeOffset;
        int tailLen = oldSize - tailSrc;
        if (tailLen > 0) {
            MemorySegment.copy(oldNode, tailSrc, newNode, tailDst, tailLen);
        }

        return ptr;
    }

    /** Copy node with a child pointer removed at the position indicated by bit. */
    private static long copyAndRemoveChild(BumpAllocator alloc, MemorySegment oldNode,
                                            int dataMap, int nodeMap,
                                            int childIndex, int bit) {
        int D = Integer.bitCount(dataMap);
        int C = Integer.bitCount(nodeMap);
        int oldSize = HEADER_SIZE + D * ENTRY_SIZE + C * CHILD_PTR_SIZE;
        int newSize = oldSize - CHILD_PTR_SIZE;

        int childBase = HEADER_SIZE + D * ENTRY_SIZE;
        int removeOffset = childBase + childIndex * CHILD_PTR_SIZE;

        long ptr = alloc.allocate(newSize);
        MemorySegment newNode = alloc.resolve(ptr, newSize);

        // Header with updated nodeMap
        newNode.set(LAYOUT_INT, DATAMAP_OFFSET, dataMap);
        newNode.set(LAYOUT_INT, NODEMAP_OFFSET, nodeMap ^ bit);

        // All data entries
        if (D > 0) {
            MemorySegment.copy(oldNode, HEADER_SIZE, newNode, HEADER_SIZE, D * ENTRY_SIZE);
        }

        // Child pointers before removal
        if (childIndex > 0) {
            MemorySegment.copy(oldNode, childBase, newNode, childBase,
                    childIndex * CHILD_PTR_SIZE);
        }

        // Child pointers after removal
        int tailSrc = removeOffset + CHILD_PTR_SIZE;
        int tailDst = removeOffset;
        int tailLen = (C - childIndex - 1) * CHILD_PTR_SIZE;
        if (tailLen > 0) {
            MemorySegment.copy(oldNode, tailSrc, newNode, tailDst, tailLen);
        }

        return ptr;
    }

    /**
     * Migrate an inline data entry to a child node pointer.
     * Removes the data entry at {@code dataIndex} and adds a child pointer for {@code bit}.
     */
    private static long copyAndMigrateFromInlineToNode(BumpAllocator alloc, MemorySegment oldNode,
                                                        int dataMap, int nodeMap,
                                                        int dataIndex, int bit, long childPtr) {
        int D = Integer.bitCount(dataMap);
        int C = Integer.bitCount(nodeMap);
        // Remove 1 data entry (−12B), add 1 child pointer (+8B) = net −4B
        int oldSize = HEADER_SIZE + D * ENTRY_SIZE + C * CHILD_PTR_SIZE;
        int newDataMap = dataMap ^ bit;
        int newNodeMap = nodeMap | bit;
        int newD = D - 1;
        int newC = C + 1;
        int newSize = HEADER_SIZE + newD * ENTRY_SIZE + newC * CHILD_PTR_SIZE;

        long ptr = alloc.allocate(newSize);
        MemorySegment newNode = alloc.resolve(ptr, newSize);

        newNode.set(LAYOUT_INT, DATAMAP_OFFSET, newDataMap);
        newNode.set(LAYOUT_INT, NODEMAP_OFFSET, newNodeMap);

        // Copy data entries, skipping the removed one
        int removeEntryOffset = HEADER_SIZE + dataIndex * ENTRY_SIZE;
        // Before
        if (dataIndex > 0) {
            MemorySegment.copy(oldNode, HEADER_SIZE, newNode, HEADER_SIZE,
                    dataIndex * ENTRY_SIZE);
        }
        // After
        int entriesAfter = D - dataIndex - 1;
        if (entriesAfter > 0) {
            MemorySegment.copy(oldNode, removeEntryOffset + ENTRY_SIZE,
                    newNode, HEADER_SIZE + dataIndex * ENTRY_SIZE,
                    entriesAfter * ENTRY_SIZE);
        }

        // Copy old child pointers
        int oldChildBase = HEADER_SIZE + D * ENTRY_SIZE;
        int newChildBase = HEADER_SIZE + newD * ENTRY_SIZE;
        int newChildIndex = Integer.bitCount(newNodeMap & (bit - 1));

        // Children before new child
        if (newChildIndex > 0) {
            MemorySegment.copy(oldNode, oldChildBase, newNode, newChildBase,
                    newChildIndex * CHILD_PTR_SIZE);
        }
        // New child
        newNode.set(LAYOUT_LONG, newChildBase + newChildIndex * CHILD_PTR_SIZE, childPtr);
        // Children after new child
        int childrenAfter = C - newChildIndex;
        if (childrenAfter > 0) {
            MemorySegment.copy(oldNode, oldChildBase + newChildIndex * CHILD_PTR_SIZE,
                    newNode, newChildBase + (newChildIndex + 1) * CHILD_PTR_SIZE,
                    childrenAfter * CHILD_PTR_SIZE);
        }

        return ptr;
    }

    /**
     * Migrate a child node pointer to an inline data entry (CHAMP compaction).
     * Called when a child becomes a singleton after removal.
     * Removes the child pointer for {@code bit} and adds the singleton's data entry.
     */
    private static long copyAndMigrateFromNodeToInline(BumpAllocator alloc, MemorySegment oldNode,
                                                        int dataMap, int nodeMap,
                                                        int childIndex, int bit,
                                                        long singletonChildPtr) {
        // Read the singleton child's single data entry
        MemorySegment child = resolveNode(alloc, singletonChildPtr);
        int childAttrId = child.get(LAYOUT_INT, HEADER_SIZE);
        long childValueRef = child.get(LAYOUT_LONG, HEADER_SIZE + ENTRY_KEY_SIZE);

        int D = Integer.bitCount(dataMap);
        int C = Integer.bitCount(nodeMap);
        // Remove 1 child pointer (−8B), add 1 data entry (+12B) = net +4B
        int newDataMap = dataMap | bit;
        int newNodeMap = nodeMap ^ bit;
        int newD = D + 1;
        int newC = C - 1;
        int newSize = HEADER_SIZE + newD * ENTRY_SIZE + newC * CHILD_PTR_SIZE;

        int newDataIndex = Integer.bitCount(newDataMap & (bit - 1));

        long ptr = alloc.allocate(newSize);
        MemorySegment newNode = alloc.resolve(ptr, newSize);

        newNode.set(LAYOUT_INT, DATAMAP_OFFSET, newDataMap);
        newNode.set(LAYOUT_INT, NODEMAP_OFFSET, newNodeMap);

        // Copy data entries with insertion
        int insertOffset = HEADER_SIZE + newDataIndex * ENTRY_SIZE;
        // Before
        if (newDataIndex > 0) {
            MemorySegment.copy(oldNode, HEADER_SIZE, newNode, HEADER_SIZE,
                    newDataIndex * ENTRY_SIZE);
        }
        // New inline entry
        newNode.set(LAYOUT_INT, insertOffset, childAttrId);
        newNode.set(LAYOUT_LONG, insertOffset + ENTRY_KEY_SIZE, childValueRef);
        // After
        int entriesAfter = D - newDataIndex;
        if (entriesAfter > 0) {
            MemorySegment.copy(oldNode, HEADER_SIZE + newDataIndex * ENTRY_SIZE,
                    newNode, insertOffset + ENTRY_SIZE,
                    entriesAfter * ENTRY_SIZE);
        }

        // Copy child pointers, skipping the removed one
        int oldChildBase = HEADER_SIZE + D * ENTRY_SIZE;
        int newChildBase = HEADER_SIZE + newD * ENTRY_SIZE;
        // Before
        if (childIndex > 0) {
            MemorySegment.copy(oldNode, oldChildBase, newNode, newChildBase,
                    childIndex * CHILD_PTR_SIZE);
        }
        // After
        int childrenAfter = C - childIndex - 1;
        if (childrenAfter > 0) {
            MemorySegment.copy(oldNode, oldChildBase + (childIndex + 1) * CHILD_PTR_SIZE,
                    newNode, newChildBase + childIndex * CHILD_PTR_SIZE,
                    childrenAfter * CHILD_PTR_SIZE);
        }

        return ptr;
    }

    // ====================================================================
    // Utility helpers
    // ====================================================================

    /** Compute the bit position from a hash fragment index. */
    private static int bitpos(int mask) {
        return 1 << mask;
    }

    /**
     * Check if a node is a singleton (exactly 1 data entry, 0 children).
     * Used to decide whether to inline a child during removal.
     */
    private static boolean isSingleton(BumpAllocator alloc, long nodePtr) {
        if (nodePtr == EMPTY_ROOT) return false;
        MemorySegment header = alloc.resolve(nodePtr, HEADER_SIZE);
        int dataMap = header.get(LAYOUT_INT, DATAMAP_OFFSET);
        int nodeMap = header.get(LAYOUT_INT, NODEMAP_OFFSET);
        return Integer.bitCount(dataMap) == 1 && nodeMap == 0;
    }
}
