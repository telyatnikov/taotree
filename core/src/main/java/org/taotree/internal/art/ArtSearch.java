package org.taotree.internal.art;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.LongFunction;

/**
 * Read-only ART search operations: predecessor, successor, rightmost/leftmost leaf.
 *
 * <p>These operations traverse an existing ART without mutation. They accept a
 * {@code LongFunction<MemorySegment>} resolver that maps node pointers to memory
 * segments (typically {@code slab::resolve} or TaoTree's resolveNode).
 *
 * <p>Used by the temporal layer for predecessor searches in AttributeRuns and
 * EntityVersions ARTs.
 */
public final class ArtSearch {

    private ArtSearch() {}

    // Stack capacity: max depth in ART = keyLen + prefix overhead.
    // Keys ≤ 12 bytes, prefix nodes up to 15 bytes each — conservatively 64 frames.
    private static final int MAX_STACK = 64;

    // ====================================================================
    // predecessor: largest key ≤ searchKey
    // ====================================================================

    /**
     * Find the leaf with the largest key ≤ {@code searchKey}.
     *
     * @param resolver maps a node pointer to a MemorySegment
     * @param root     ART root pointer
     * @param key      the search key bytes
     * @param keyLen   the key length in bytes
     * @return leaf pointer, or {@link NodePtr#EMPTY_PTR} if no such key exists
     */
    public static long predecessor(LongFunction<MemorySegment> resolver,
                                   long root, MemorySegment key, int keyLen) {
        if (NodePtr.isEmpty(root)) return NodePtr.EMPTY_PTR;

        // Stack for backtracking: stores (nodePtr, depth, matchedByteUnsigned)
        // Only inner node entries where we took the exact match path go on the stack.
        long[] stackPtrs = new long[MAX_STACK];
        int[] stackDepths = new int[MAX_STACK];
        int[] stackBytes = new int[MAX_STACK]; // the byte we matched (unsigned)
        int stackSize = 0;

        long nodePtr = root;
        int depth = 0;

        // ── Phase 1: descend matching key bytes ──────────────────────────
        while (true) {
            if (NodePtr.isEmpty(nodePtr)) break; // dead end

            int type = NodePtr.nodeType(nodePtr);

            if (type == NodePtr.LEAF) {
                // Compare leaf key with search key
                MemorySegment leafSeg = resolver.apply(nodePtr);
                int cmp = compareKeys(leafSeg, 0, key, 0, keyLen);
                if (cmp <= 0) return nodePtr; // leaf key ≤ searchKey
                break; // leaf key > searchKey → backtrack
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment seg = resolver.apply(nodePtr);
                int prefLen = PrefixNode.count(seg);
                int toMatch = Math.min(prefLen, keyLen - depth);

                // Compare prefix bytes with search key
                boolean earlyBreak = false;
                for (int i = 0; i < toMatch; i++) {
                    int prefByte = Byte.toUnsignedInt(PrefixNode.keyAt(seg, i));
                    int searchByte = Byte.toUnsignedInt(key.get(ValueLayout.JAVA_BYTE, depth + i));

                    if (prefByte < searchByte) {
                        // All leaves under this prefix are < searchKey → rightmost leaf
                        return rightmostLeaf(resolver, PrefixNode.child(seg));
                    }
                    if (prefByte > searchByte) {
                        // All leaves under this prefix are > searchKey → backtrack
                        earlyBreak = true;
                        break;
                    }
                }

                if (earlyBreak) break; // backtrack to phase 2

                // Check if we ran out of search key bytes before matching all prefix bytes
                if (toMatch < prefLen) {
                    // searchKey is shorter → it's a prefix of keys under this node → backtrack
                    break;
                }

                // Full prefix match — continue descent
                depth += prefLen;
                nodePtr = PrefixNode.child(seg);
                continue;
            }

            // Inner node (Node4, Node16, Node48, Node256)
            if (depth >= keyLen) break; // search key exhausted at inner node

            MemorySegment seg = resolver.apply(nodePtr);
            byte targetByte = key.get(ValueLayout.JAVA_BYTE, depth);
            long child = findChild(seg, type, targetByte);

            if (!NodePtr.isEmpty(child)) {
                // Exact match — push frame for backtracking
                if (stackSize < MAX_STACK) {
                    stackPtrs[stackSize] = nodePtr;
                    stackDepths[stackSize] = depth;
                    stackBytes[stackSize] = Byte.toUnsignedInt(targetByte);
                    stackSize++;
                }
                nodePtr = child;
                depth++;
                continue;
            }

            // No exact match — find the largest child byte < targetByte
            long prevChild = largestChildBefore(seg, type, Byte.toUnsignedInt(targetByte));
            if (!NodePtr.isEmpty(prevChild)) {
                return rightmostLeaf(resolver, prevChild);
            }
            break; // no smaller child → backtrack
        }

        // ── Phase 2: backtrack ───────────────────────────────────────────
        while (stackSize > 0) {
            stackSize--;
            MemorySegment seg = resolver.apply(stackPtrs[stackSize]);
            int type = NodePtr.nodeType(stackPtrs[stackSize]);
            int matchedByte = stackBytes[stackSize];

            long prevChild = largestChildBefore(seg, type, matchedByte);
            if (!NodePtr.isEmpty(prevChild)) {
                return rightmostLeaf(resolver, prevChild);
            }
        }

        return NodePtr.EMPTY_PTR;
    }

    // ====================================================================
    // successor: smallest key ≥ searchKey
    // ====================================================================

    /**
     * Find the leaf with the smallest key ≥ {@code searchKey}.
     *
     * @param resolver maps a node pointer to a MemorySegment
     * @param root     ART root pointer
     * @param key      the search key bytes
     * @param keyLen   the key length in bytes
     * @return leaf pointer, or {@link NodePtr#EMPTY_PTR} if no such key exists
     */
    public static long successor(LongFunction<MemorySegment> resolver,
                                 long root, MemorySegment key, int keyLen) {
        if (NodePtr.isEmpty(root)) return NodePtr.EMPTY_PTR;

        long[] stackPtrs = new long[MAX_STACK];
        int[] stackDepths = new int[MAX_STACK];
        int[] stackBytes = new int[MAX_STACK];
        int stackSize = 0;

        long nodePtr = root;
        int depth = 0;

        // ── Phase 1: descend matching key bytes ──────────────────────────
        while (true) {
            if (NodePtr.isEmpty(nodePtr)) break;

            int type = NodePtr.nodeType(nodePtr);

            if (type == NodePtr.LEAF) {
                MemorySegment leafSeg = resolver.apply(nodePtr);
                int cmp = compareKeys(leafSeg, 0, key, 0, keyLen);
                if (cmp >= 0) return nodePtr; // leaf key ≥ searchKey
                break; // leaf key < searchKey → backtrack
            }

            if (type == NodePtr.PREFIX) {
                MemorySegment seg = resolver.apply(nodePtr);
                int prefLen = PrefixNode.count(seg);
                int toMatch = Math.min(prefLen, keyLen - depth);

                boolean earlyBreak = false;
                for (int i = 0; i < toMatch; i++) {
                    int prefByte = Byte.toUnsignedInt(PrefixNode.keyAt(seg, i));
                    int searchByte = Byte.toUnsignedInt(key.get(ValueLayout.JAVA_BYTE, depth + i));

                    if (prefByte > searchByte) {
                        return leftmostLeaf(resolver, PrefixNode.child(seg));
                    }
                    if (prefByte < searchByte) {
                        earlyBreak = true;
                        break; // all subtree keys are smaller → backtrack
                    }
                }

                if (earlyBreak) break; // backtrack to phase 2

                if (toMatch < prefLen) {
                    // searchKey is shorter → all keys under this prefix are > searchKey
                    return leftmostLeaf(resolver, PrefixNode.child(seg));
                }

                depth += prefLen;
                nodePtr = PrefixNode.child(seg);
                continue;
            }

            // Inner node
            if (depth >= keyLen) {
                // Search key exhausted at inner node — leftmost leaf is the successor
                return leftmostLeaf(resolver, nodePtr);
            }

            MemorySegment seg = resolver.apply(nodePtr);
            byte targetByte = key.get(ValueLayout.JAVA_BYTE, depth);
            long child = findChild(seg, type, targetByte);

            if (!NodePtr.isEmpty(child)) {
                if (stackSize < MAX_STACK) {
                    stackPtrs[stackSize] = nodePtr;
                    stackDepths[stackSize] = depth;
                    stackBytes[stackSize] = Byte.toUnsignedInt(targetByte);
                    stackSize++;
                }
                nodePtr = child;
                depth++;
                continue;
            }

            // No exact match — find the smallest child byte > targetByte
            long nextChild = smallestChildAfter(seg, type, Byte.toUnsignedInt(targetByte));
            if (!NodePtr.isEmpty(nextChild)) {
                return leftmostLeaf(resolver, nextChild);
            }
            break;
        }

        // ── Phase 2: backtrack ───────────────────────────────────────────
        while (stackSize > 0) {
            stackSize--;
            MemorySegment seg = resolver.apply(stackPtrs[stackSize]);
            int type = NodePtr.nodeType(stackPtrs[stackSize]);
            int matchedByte = stackBytes[stackSize];

            long nextChild = smallestChildAfter(seg, type, matchedByte);
            if (!NodePtr.isEmpty(nextChild)) {
                return leftmostLeaf(resolver, nextChild);
            }
        }

        return NodePtr.EMPTY_PTR;
    }

    // ====================================================================
    // rightmostLeaf / leftmostLeaf
    // ====================================================================

    /**
     * Descend to the rightmost (lexicographically largest) leaf under a subtree.
     */
    public static long rightmostLeaf(LongFunction<MemorySegment> resolver, long nodePtr) {
        while (true) {
            if (NodePtr.isEmpty(nodePtr)) return NodePtr.EMPTY_PTR;

            int type = NodePtr.nodeType(nodePtr);
            if (type == NodePtr.LEAF) return nodePtr;

            MemorySegment seg = resolver.apply(nodePtr);

            if (type == NodePtr.PREFIX) {
                nodePtr = PrefixNode.child(seg);
                continue;
            }

            nodePtr = switch (type) {
                case NodePtr.NODE_4 -> {
                    int n = Node4.count(seg);
                    yield n > 0 ? Node4.childAt(seg, n - 1) : NodePtr.EMPTY_PTR;
                }
                case NodePtr.NODE_16 -> {
                    int n = Node16.count(seg);
                    yield n > 0 ? Node16.childAt(seg, n - 1) : NodePtr.EMPTY_PTR;
                }
                case NodePtr.NODE_48 -> lastChildNode48(seg);
                case NodePtr.NODE_256 -> lastChildNode256(seg);
                default -> NodePtr.EMPTY_PTR;
            };
        }
    }

    /**
     * Descend to the leftmost (lexicographically smallest) leaf under a subtree.
     */
    public static long leftmostLeaf(LongFunction<MemorySegment> resolver, long nodePtr) {
        while (true) {
            if (NodePtr.isEmpty(nodePtr)) return NodePtr.EMPTY_PTR;

            int type = NodePtr.nodeType(nodePtr);
            if (type == NodePtr.LEAF) return nodePtr;

            MemorySegment seg = resolver.apply(nodePtr);

            if (type == NodePtr.PREFIX) {
                nodePtr = PrefixNode.child(seg);
                continue;
            }

            nodePtr = switch (type) {
                case NodePtr.NODE_4 -> Node4.count(seg) > 0 ? Node4.childAt(seg, 0) : NodePtr.EMPTY_PTR;
                case NodePtr.NODE_16 -> Node16.count(seg) > 0 ? Node16.childAt(seg, 0) : NodePtr.EMPTY_PTR;
                case NodePtr.NODE_48 -> firstChildNode48(seg);
                case NodePtr.NODE_256 -> firstChildNode256(seg);
                default -> NodePtr.EMPTY_PTR;
            };
        }
    }

    // ====================================================================
    // Child navigation helpers
    // ====================================================================

    /**
     * Find the child pointer for the largest key byte strictly less than {@code targetUnsigned}.
     */
    private static long largestChildBefore(MemorySegment seg, int nodeType, int targetUnsigned) {
        return switch (nodeType) {
            case NodePtr.NODE_4 -> {
                int n = Node4.count(seg);
                for (int i = n - 1; i >= 0; i--) {
                    if (Byte.toUnsignedInt(Node4.keyAt(seg, i)) < targetUnsigned) {
                        yield Node4.childAt(seg, i);
                    }
                }
                yield NodePtr.EMPTY_PTR;
            }
            case NodePtr.NODE_16 -> {
                int n = Node16.count(seg);
                for (int i = n - 1; i >= 0; i--) {
                    if (Byte.toUnsignedInt(Node16.keyAt(seg, i)) < targetUnsigned) {
                        yield Node16.childAt(seg, i);
                    }
                }
                yield NodePtr.EMPTY_PTR;
            }
            case NodePtr.NODE_48 -> {
                for (int k = targetUnsigned - 1; k >= 0; k--) {
                    int slot = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE,
                            Node48.OFF_CHILD_IDX + k));
                    if (slot != Byte.toUnsignedInt(Node48.EMPTY_MARKER)) {
                        yield Node48.childAtSlot(seg, slot);
                    }
                }
                yield NodePtr.EMPTY_PTR;
            }
            case NodePtr.NODE_256 -> {
                for (int k = targetUnsigned - 1; k >= 0; k--) {
                    long child = Node256.findChild(seg, (byte) k);
                    if (!NodePtr.isEmpty(child)) {
                        yield child;
                    }
                }
                yield NodePtr.EMPTY_PTR;
            }
            default -> NodePtr.EMPTY_PTR;
        };
    }

    /**
     * Find the child pointer for the smallest key byte strictly greater than {@code targetUnsigned}.
     */
    private static long smallestChildAfter(MemorySegment seg, int nodeType, int targetUnsigned) {
        return switch (nodeType) {
            case NodePtr.NODE_4 -> {
                int n = Node4.count(seg);
                for (int i = 0; i < n; i++) {
                    if (Byte.toUnsignedInt(Node4.keyAt(seg, i)) > targetUnsigned) {
                        yield Node4.childAt(seg, i);
                    }
                }
                yield NodePtr.EMPTY_PTR;
            }
            case NodePtr.NODE_16 -> {
                int n = Node16.count(seg);
                for (int i = 0; i < n; i++) {
                    if (Byte.toUnsignedInt(Node16.keyAt(seg, i)) > targetUnsigned) {
                        yield Node16.childAt(seg, i);
                    }
                }
                yield NodePtr.EMPTY_PTR;
            }
            case NodePtr.NODE_48 -> {
                for (int k = targetUnsigned + 1; k < 256; k++) {
                    int slot = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE,
                            Node48.OFF_CHILD_IDX + k));
                    if (slot != Byte.toUnsignedInt(Node48.EMPTY_MARKER)) {
                        yield Node48.childAtSlot(seg, slot);
                    }
                }
                yield NodePtr.EMPTY_PTR;
            }
            case NodePtr.NODE_256 -> {
                for (int k = targetUnsigned + 1; k < 256; k++) {
                    long child = Node256.findChild(seg, (byte) k);
                    if (!NodePtr.isEmpty(child)) {
                        yield child;
                    }
                }
                yield NodePtr.EMPTY_PTR;
            }
            default -> NodePtr.EMPTY_PTR;
        };
    }

    // ── Node48/Node256 first/last child ──────────────────────────────────

    private static long lastChildNode48(MemorySegment seg) {
        for (int k = 255; k >= 0; k--) {
            int slot = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE,
                    Node48.OFF_CHILD_IDX + k));
            if (slot != Byte.toUnsignedInt(Node48.EMPTY_MARKER)) {
                return Node48.childAtSlot(seg, slot);
            }
        }
        return NodePtr.EMPTY_PTR;
    }

    private static long firstChildNode48(MemorySegment seg) {
        for (int k = 0; k < 256; k++) {
            int slot = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE,
                    Node48.OFF_CHILD_IDX + k));
            if (slot != Byte.toUnsignedInt(Node48.EMPTY_MARKER)) {
                return Node48.childAtSlot(seg, slot);
            }
        }
        return NodePtr.EMPTY_PTR;
    }

    private static long lastChildNode256(MemorySegment seg) {
        for (int k = 255; k >= 0; k--) {
            long child = Node256.findChild(seg, (byte) k);
            if (!NodePtr.isEmpty(child)) return child;
        }
        return NodePtr.EMPTY_PTR;
    }

    private static long firstChildNode256(MemorySegment seg) {
        for (int k = 0; k < 256; k++) {
            long child = Node256.findChild(seg, (byte) k);
            if (!NodePtr.isEmpty(child)) return child;
        }
        return NodePtr.EMPTY_PTR;
    }

    // ── Key comparison ───────────────────────────────────────────────────

    static long findChild(MemorySegment seg, int nodeType, byte key) {
        return switch (nodeType) {
            case NodePtr.NODE_4 -> Node4.findChild(seg, key);
            case NodePtr.NODE_16 -> Node16.findChild(seg, key);
            case NodePtr.NODE_48 -> Node48.findChild(seg, key);
            case NodePtr.NODE_256 -> Node256.findChild(seg, key);
            default -> NodePtr.EMPTY_PTR;
        };
    }

    /**
     * Compare two key byte sequences lexicographically (unsigned byte comparison).
     * Returns negative if a < b, 0 if equal, positive if a > b.
     */
    public static int compareKeys(MemorySegment a, long aOff, MemorySegment b, long bOff, int len) {
        for (int i = 0; i < len; i++) {
            int ab = Byte.toUnsignedInt(a.get(ValueLayout.JAVA_BYTE, aOff + i));
            int bb = Byte.toUnsignedInt(b.get(ValueLayout.JAVA_BYTE, bOff + i));
            if (ab != bb) return ab - bb;
        }
        return 0;
    }
}
