package org.taotree.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompactorTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Key length used in all tests: 4 bytes. */
    private static final int KEY_LEN = 4;

    /** Key slot size: 8-byte aligned (same as TaoTree). */
    private static final int KEY_SLOT_SIZE = (KEY_LEN + 7) & ~7; // = 8

    /** Value size for leaves: 8 bytes (a single long). */
    private static final int VALUE_SIZE = 8;

    /** Total leaf segment size: key slot (aligned) + value. */
    private static final int LEAF_SEG_SIZE = KEY_SLOT_SIZE + VALUE_SIZE;

    /**
     * Allocate a leaf with the given 4-byte key and 8-byte value.
     * Returns the NodePtr for the leaf.
     */
    private static long allocLeaf(SlabAllocator slab, int leafClassId, byte[] key, long value) {
        long ptr = slab.allocate(leafClassId, NodePtr.LEAF);
        MemorySegment seg = slab.resolve(ptr);
        for (int i = 0; i < KEY_LEN; i++) {
            seg.set(ValueLayout.JAVA_BYTE, i, key[i]);
        }
        // Zero padding between key and value
        for (int i = KEY_LEN; i < KEY_SLOT_SIZE; i++) {
            seg.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
        }
        seg.set(ValueLayout.JAVA_LONG, KEY_SLOT_SIZE, value);
        return ptr;
    }

    /**
     * Wrap a child pointer in a prefix node with the given prefix bytes.
     */
    private static long wrapInPrefix(SlabAllocator slab, int prefixClassId,
                                     byte[] prefixBytes, int offset, int length,
                                     long child) {
        long ptr = slab.allocate(prefixClassId, NodePtr.PREFIX);
        MemorySegment seg = slab.resolve(ptr);
        PrefixNode.init(seg, prefixBytes, offset, length, child);
        return ptr;
    }

    /** Read the 8-byte value from a leaf segment. */
    private static long readLeafValue(SlabAllocator slab, long leafPtr) {
        MemorySegment seg = slab.resolve(leafPtr);
        return seg.get(ValueLayout.JAVA_LONG, KEY_SLOT_SIZE);
    }

    /** Read the i-th key byte from a leaf segment. */
    private static byte readLeafKeyByte(SlabAllocator slab, long leafPtr, int i) {
        MemorySegment seg = slab.resolve(leafPtr);
        return seg.get(ValueLayout.JAVA_BYTE, i);
    }

    /**
     * Walk a compacted tree to the leaf, given a root that is a PREFIX wrapping a leaf.
     * Returns the leaf NodePtr.
     */
    private static long walkToLeaf(SlabAllocator slab, long root) {
        long cur = root;
        while (!NodePtr.isEmpty(cur)) {
            int type = NodePtr.nodeType(cur);
            if (type == NodePtr.LEAF || type == NodePtr.LEAF_INLINE) {
                return cur;
            }
            if (type == NodePtr.PREFIX) {
                MemorySegment seg = slab.resolve(cur);
                cur = PrefixNode.child(seg);
            } else {
                break; // inner node — not expected for single-leaf tree
            }
        }
        return cur;
    }

    /**
     * Count all leaves reachable from root by recursive walk.
     */
    private static long countLeaves(SlabAllocator slab, long root) {
        if (NodePtr.isEmpty(root)) return 0;
        int type = NodePtr.nodeType(root);
        if (type == NodePtr.LEAF || type == NodePtr.LEAF_INLINE) return 1;
        if (type == NodePtr.PREFIX) {
            MemorySegment seg = slab.resolve(root);
            return countLeaves(slab, PrefixNode.child(seg));
        }
        // Inner nodes
        MemorySegment seg = slab.resolve(root);
        long count = 0;
        if (type == NodePtr.NODE_4) {
            int n = Node4.count(seg);
            for (int i = 0; i < n; i++) count += countLeaves(slab, Node4.childAt(seg, i));
        } else if (type == NodePtr.NODE_16) {
            int n = Node16.count(seg);
            for (int i = 0; i < n; i++) count += countLeaves(slab, Node16.childAt(seg, i));
        }
        return count;
    }

    /**
     * Create a Compactor wired to the given SlabAllocator and class IDs.
     */
    private static Compactor newCompactor(SlabAllocator slab, BumpAllocator bump,
                                          EpochReclaimer reclaimer,
                                          int prefixId, int n4Id, int n16Id,
                                          int n48Id, int n256Id,
                                          int leafId) {
        return new Compactor(slab, bump, reclaimer,
                prefixId, n4Id, n16Id, n48Id, n256Id,
                KEY_LEN, KEY_SLOT_SIZE,
                new int[]{leafId}, new int[]{VALUE_SIZE});
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    void compactEmptyTree() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 16);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(NodePtr.EMPTY_PTR);

            assertEquals(NodePtr.EMPTY_PTR, result.newRoot());
            assertEquals(0, result.entryCount());
        }
    }

    @Test
    void compactSingleLeaf() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 16);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            // Build: PREFIX([0,0,0,42]) -> LEAF(key=[0,0,0,42], value=0xCAFE)
            byte[] key = {0, 0, 0, 42};
            long leafPtr = allocLeaf(slab, leafId, key, 0xCAFEL);
            long root = wrapInPrefix(slab, prefixId, key, 0, KEY_LEN, leafPtr);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(root);

            assertNotEquals(NodePtr.EMPTY_PTR, result.newRoot());
            assertEquals(1, result.entryCount());
            // New root must be at a different slab location
            assertNotEquals(root, result.newRoot());

            // Walk the new tree and verify the leaf value
            long newLeaf = walkToLeaf(slab, result.newRoot());
            assertFalse(NodePtr.isEmpty(newLeaf));
            assertTrue(NodePtr.isLeaf(newLeaf));
            assertEquals(0xCAFEL, readLeafValue(slab, newLeaf));

            // Verify key bytes
            for (int i = 0; i < KEY_LEN; i++) {
                assertEquals(key[i], readLeafKeyByte(slab, newLeaf, i));
            }
        }
    }

    @Test
    void compactSmallTreeWithNode4() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 16);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            // Build a tree:
            //   PREFIX([0,0,0]) -> NODE4 with children at bytes 1, 2, 3
            //     each child is a leaf with key [0,0,0,X] and value X*100

            // Allocate 3 leaves
            long leaf1 = allocLeaf(slab, leafId, new byte[]{0, 0, 0, 1}, 100L);
            long leaf2 = allocLeaf(slab, leafId, new byte[]{0, 0, 0, 2}, 200L);
            long leaf3 = allocLeaf(slab, leafId, new byte[]{0, 0, 0, 3}, 300L);

            // Build Node4
            long n4Ptr = slab.allocate(n4Id, NodePtr.NODE_4);
            MemorySegment n4Seg = slab.resolve(n4Ptr);
            Node4.init(n4Seg);
            Node4.insertChild(n4Seg, (byte) 1, leaf1);
            Node4.insertChild(n4Seg, (byte) 2, leaf2);
            Node4.insertChild(n4Seg, (byte) 3, leaf3);

            // Wrap in prefix [0,0,0]
            long root = wrapInPrefix(slab, prefixId, new byte[]{0, 0, 0}, 0, 3, n4Ptr);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(root);

            assertNotEquals(NodePtr.EMPTY_PTR, result.newRoot());
            assertEquals(3, result.entryCount());
            assertNotEquals(root, result.newRoot());

            // Walk the new tree and verify structure
            long newRoot = result.newRoot();
            assertEquals(NodePtr.PREFIX, NodePtr.nodeType(newRoot));

            MemorySegment newPrefSeg = slab.resolve(newRoot);
            long newN4Ptr = PrefixNode.child(newPrefSeg);
            assertEquals(NodePtr.NODE_4, NodePtr.nodeType(newN4Ptr));

            MemorySegment newN4Seg = slab.resolve(newN4Ptr);
            assertEquals(3, Node4.count(newN4Seg));

            // Verify each child's value
            for (int i = 0; i < 3; i++) {
                long childPtr = Node4.childAt(newN4Seg, i);
                assertTrue(NodePtr.isLeaf(childPtr));
                long val = readLeafValue(slab, childPtr);
                byte keyByte = Node4.keyAt(newN4Seg, i);
                assertEquals((Byte.toUnsignedInt(keyByte)) * 100L, val);
            }
        }
    }

    @Test
    void compactPreservesLeafValues() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 16);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            // Insert 4 leaves with distinct values
            long[] values = {0xDEAD_BEEFL, Long.MAX_VALUE, Long.MIN_VALUE, 0L};
            long[] leafPtrs = new long[4];
            for (int i = 0; i < 4; i++) {
                leafPtrs[i] = allocLeaf(slab, leafId,
                        new byte[]{0, 0, 0, (byte) (i + 10)}, values[i]);
            }

            // Build Node4 with all 4 leaves
            long n4Ptr = slab.allocate(n4Id, NodePtr.NODE_4);
            MemorySegment n4Seg = slab.resolve(n4Ptr);
            Node4.init(n4Seg);
            for (int i = 0; i < 4; i++) {
                Node4.insertChild(n4Seg, (byte) (i + 10), leafPtrs[i]);
            }

            long root = wrapInPrefix(slab, prefixId, new byte[]{0, 0, 0}, 0, 3, n4Ptr);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(root);
            assertEquals(4, result.entryCount());

            // Walk the new tree and verify each leaf's value
            MemorySegment newPrefSeg = slab.resolve(result.newRoot());
            long newN4Ptr = PrefixNode.child(newPrefSeg);
            MemorySegment newN4Seg = slab.resolve(newN4Ptr);

            for (int i = 0; i < 4; i++) {
                long childPtr = Node4.childAt(newN4Seg, i);
                long val = readLeafValue(slab, childPtr);
                byte keyByte = Node4.keyAt(newN4Seg, i);
                int idx = Byte.toUnsignedInt(keyByte) - 10;
                assertEquals(values[idx], val,
                        "Leaf at key byte " + Byte.toUnsignedInt(keyByte));
            }
        }
    }

    @Test
    void compactPreservesEntryCount() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 16);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            // Build a deeper tree:
            //   PREFIX([0]) -> NODE4 with 2 children:
            //     byte 0: PREFIX([0]) -> NODE4 with 3 leaves at bytes 1,2,3
            //     byte 1: PREFIX([0]) -> NODE4 with 2 leaves at bytes 4,5
            // Total: 5 leaves

            // Left subtree: 3 leaves
            long l1 = allocLeaf(slab, leafId, new byte[]{0, 0, 0, 1}, 1L);
            long l2 = allocLeaf(slab, leafId, new byte[]{0, 0, 0, 2}, 2L);
            long l3 = allocLeaf(slab, leafId, new byte[]{0, 0, 0, 3}, 3L);
            long leftN4 = slab.allocate(n4Id, NodePtr.NODE_4);
            MemorySegment leftSeg = slab.resolve(leftN4);
            Node4.init(leftSeg);
            Node4.insertChild(leftSeg, (byte) 1, l1);
            Node4.insertChild(leftSeg, (byte) 2, l2);
            Node4.insertChild(leftSeg, (byte) 3, l3);
            long leftPrefix = wrapInPrefix(slab, prefixId, new byte[]{0}, 0, 1, leftN4);

            // Right subtree: 2 leaves
            long l4 = allocLeaf(slab, leafId, new byte[]{0, 1, 0, 4}, 4L);
            long l5 = allocLeaf(slab, leafId, new byte[]{0, 1, 0, 5}, 5L);
            long rightN4 = slab.allocate(n4Id, NodePtr.NODE_4);
            MemorySegment rightSeg = slab.resolve(rightN4);
            Node4.init(rightSeg);
            Node4.insertChild(rightSeg, (byte) 4, l4);
            Node4.insertChild(rightSeg, (byte) 5, l5);
            long rightPrefix = wrapInPrefix(slab, prefixId, new byte[]{0}, 0, 1, rightN4);

            // Root Node4
            long rootN4 = slab.allocate(n4Id, NodePtr.NODE_4);
            MemorySegment rootSeg = slab.resolve(rootN4);
            Node4.init(rootSeg);
            Node4.insertChild(rootSeg, (byte) 0, leftPrefix);
            Node4.insertChild(rootSeg, (byte) 1, rightPrefix);

            long root = wrapInPrefix(slab, prefixId, new byte[]{0}, 0, 1, rootN4);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(root);

            assertEquals(5, result.entryCount());
            assertNotEquals(root, result.newRoot());

            // Verify by walking the compacted tree
            assertEquals(5, countLeaves(slab, result.newRoot()));
        }
    }

    @Test
    void compactBareLeafWithoutPrefix() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 16);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            // A bare leaf as root (no prefix wrapping)
            byte[] key = {1, 2, 3, 4};
            long leafPtr = allocLeaf(slab, leafId, key, 0x42L);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(leafPtr);

            assertEquals(1, result.entryCount());
            assertNotEquals(leafPtr, result.newRoot());
            assertTrue(NodePtr.isLeaf(result.newRoot()));
            assertEquals(0x42L, readLeafValue(slab, result.newRoot()));
        }
    }

    @Test
    void compactNode16() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 16);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            // Build a Node16 with 8 leaves
            long n16Ptr = slab.allocate(n16Id, NodePtr.NODE_16);
            MemorySegment n16Seg = slab.resolve(n16Ptr);
            Node16.init(n16Seg);
            for (int i = 0; i < 8; i++) {
                long leaf = allocLeaf(slab, leafId,
                        new byte[]{0, 0, (byte) i, 0}, (long) i * 10);
                Node16.insertChild(n16Seg, (byte) i, leaf);
            }

            long root = wrapInPrefix(slab, prefixId, new byte[]{0, 0}, 0, 2, n16Ptr);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(root);

            assertEquals(8, result.entryCount());
            assertNotEquals(root, result.newRoot());

            // Verify the compacted tree has a Node16 child
            MemorySegment newPrefSeg = slab.resolve(result.newRoot());
            long newN16Ptr = PrefixNode.child(newPrefSeg);
            assertEquals(NodePtr.NODE_16, NodePtr.nodeType(newN16Ptr));
            assertEquals(8, Node16.count(slab.resolve(newN16Ptr)));
        }
    }

    @Test
    void compactNode48() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 20);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            // Build a Node48 with 20 leaves
            long n48Ptr = slab.allocate(n48Id, NodePtr.NODE_48);
            MemorySegment n48Seg = slab.resolve(n48Ptr);
            Node48.init(n48Seg);
            for (int i = 0; i < 20; i++) {
                long leaf = allocLeaf(slab, leafId,
                        new byte[]{0, (byte) i, 0, 0}, (long) i * 7);
                Node48.insertChild(n48Seg, (byte) (i + 10), leaf);
            }

            long root = wrapInPrefix(slab, prefixId, new byte[]{0}, 0, 1, n48Ptr);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(root);

            assertEquals(20, result.entryCount());
            assertNotEquals(root, result.newRoot());

            MemorySegment newPrefSeg = slab.resolve(result.newRoot());
            long newN48Ptr = PrefixNode.child(newPrefSeg);
            assertEquals(NodePtr.NODE_48, NodePtr.nodeType(newN48Ptr));
            assertEquals(20, Node48.count(slab.resolve(newN48Ptr)));
        }
    }

    @Test
    void compactNode256() {
        try (var arena = Arena.ofConfined()) {
            var slab = new SlabAllocator(arena, 1 << 20);
            int prefixId = slab.registerClass(NodeConstants.PREFIX_SIZE);
            int n4Id     = slab.registerClass(NodeConstants.NODE4_SIZE);
            int n16Id    = slab.registerClass(NodeConstants.NODE16_SIZE);
            int n48Id    = slab.registerClass(NodeConstants.NODE48_SIZE);
            int n256Id   = slab.registerClass(NodeConstants.NODE256_SIZE);
            int leafId   = slab.registerClass(LEAF_SEG_SIZE);

            var bump = new BumpAllocator(arena);
            var reclaimer = new EpochReclaimer(slab);

            // Build a Node256 with 64 leaves
            long n256Ptr = slab.allocate(n256Id, NodePtr.NODE_256);
            MemorySegment n256Seg = slab.resolve(n256Ptr);
            Node256.init(n256Seg);
            for (int i = 0; i < 64; i++) {
                long leaf = allocLeaf(slab, leafId,
                        new byte[]{(byte) i, 0, 0, 0}, (long) i * 3);
                Node256.insertChild(n256Seg, (byte) i, leaf);
            }

            long root = wrapInPrefix(slab, prefixId, new byte[]{}, 0, 0, n256Ptr);

            var compactor = newCompactor(slab, bump, reclaimer,
                    prefixId, n4Id, n16Id, n48Id, n256Id, leafId);

            var result = compactor.compact(root);

            assertEquals(64, result.entryCount());
            assertNotEquals(root, result.newRoot());

            // The compacted root should wrap a Node256
            MemorySegment newPrefSeg = slab.resolve(result.newRoot());
            long newN256Ptr = PrefixNode.child(newPrefSeg);
            assertEquals(NodePtr.NODE_256, NodePtr.nodeType(newN256Ptr));
            assertEquals(64, Node256.count(slab.resolve(newN256Ptr)));
        }
    }
}
