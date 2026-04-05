package org.taotree.internal;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class Node256Test {

    @Test
    void insertAndFindDirect() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node256ClassId);
            var seg = alloc.resolve(ptr);
            Node256.init(seg);

            Node256.insertChild(seg, (byte) 0x00, 1L);
            Node256.insertChild(seg, (byte) 0x7F, 2L);
            Node256.insertChild(seg, (byte) 0xFF, 3L);

            assertEquals(3, Node256.count(seg));
            assertEquals(1L, Node256.findChild(seg, (byte) 0x00));
            assertEquals(2L, Node256.findChild(seg, (byte) 0x7F));
            assertEquals(3L, Node256.findChild(seg, (byte) 0xFF));
            assertEquals(NodePtr.EMPTY_PTR, Node256.findChild(seg, (byte) 0x01));
        }
    }

    @Test
    void fillAll256() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node256ClassId);
            var seg = alloc.resolve(ptr);
            Node256.init(seg);

            for (int i = 0; i < 256; i++) {
                Node256.insertChild(seg, (byte) i, (long) (i + 1));
            }
            assertEquals(256, Node256.count(seg));

            for (int i = 0; i < 256; i++) {
                assertEquals((long) (i + 1), Node256.findChild(seg, (byte) i));
            }
        }
    }

    @Test
    void removeChild() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node256ClassId);
            var seg = alloc.resolve(ptr);
            Node256.init(seg);

            Node256.insertChild(seg, (byte) 0x10, 1L);
            Node256.insertChild(seg, (byte) 0x20, 2L);

            assertTrue(Node256.removeChild(seg, (byte) 0x10));
            assertEquals(1, Node256.count(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node256.findChild(seg, (byte) 0x10));
            assertEquals(2L, Node256.findChild(seg, (byte) 0x20));

            assertFalse(Node256.removeChild(seg, (byte) 0x99));
        }
    }

    @Test
    void copyFromNode48() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            

            long n48Ptr = alloc.allocate(node48ClassId);
            var n48Seg = alloc.resolve(n48Ptr);
            Node48.init(n48Seg);
            for (int i = 0; i < 48; i++) {
                Node48.insertChild(n48Seg, (byte) (i * 5), (long) (i + 1));
            }

            long n256Ptr = alloc.allocate(node256ClassId);
            var n256Seg = alloc.resolve(n256Ptr);
            Node256.init(n256Seg);
            Node256.copyFromNode48(n256Seg, n48Seg);

            assertEquals(48, Node256.count(n256Seg));
            for (int i = 0; i < 48; i++) {
                assertEquals((long) (i + 1), Node256.findChild(n256Seg, (byte) (i * 5)));
            }
        }
    }

    @Test
    void forEach() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node256ClassId);
            var seg = alloc.resolve(ptr);
            Node256.init(seg);

            Node256.insertChild(seg, (byte) 0x10, 1L);
            Node256.insertChild(seg, (byte) 0x20, 2L);

            Set<Byte> keys = new HashSet<>();
            Node256.forEach(seg, (key, child) -> keys.add(key));
            assertEquals(Set.of((byte) 0x10, (byte) 0x20), keys);
        }
    }

    @Test
    void updateChild() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node256ClassId);
            var seg = alloc.resolve(ptr);
            Node256.init(seg);

            Node256.insertChild(seg, (byte) 0x42, 1L);
            assertEquals(1L, Node256.findChild(seg, (byte) 0x42));

            Node256.updateChild(seg, (byte) 0x42, 999L);
            assertEquals(999L, Node256.findChild(seg, (byte) 0x42));
            assertEquals(1, Node256.count(seg)); // count unchanged
        }
    }

    // ---- Mutation-killing: init zeroes all children ----

    @Test
    void initAndEmpty() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            alloc.registerClass(NodeConstants.PREFIX_SIZE);
            alloc.registerClass(NodeConstants.NODE4_SIZE);
            alloc.registerClass(NodeConstants.NODE16_SIZE);
            alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);

            long ptr = alloc.allocate(node256ClassId);
            var seg = alloc.resolve(ptr);
            Node256.init(seg);

            assertEquals(0, Node256.count(seg));
            for (int i = 0; i < 256; i++) {
                assertEquals(NodePtr.EMPTY_PTR, Node256.findChild(seg, (byte) i));
            }
        }
    }

    // ---- COW operations ----

    @Test
    void cowReplaceChild() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            alloc.registerClass(NodeConstants.PREFIX_SIZE);
            alloc.registerClass(NodeConstants.NODE4_SIZE);
            alloc.registerClass(NodeConstants.NODE16_SIZE);
            alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);

            long srcPtr = alloc.allocate(node256ClassId);
            var srcSeg = alloc.resolve(srcPtr);
            Node256.init(srcSeg);
            Node256.insertChild(srcSeg, (byte) 0x10, 1L);
            Node256.insertChild(srcSeg, (byte) 0x20, 2L);
            Node256.insertChild(srcSeg, (byte) 0x30, 3L);

            long dstPtr = alloc.allocate(node256ClassId);
            var dstSeg = alloc.resolve(dstPtr);
            Node256.cowReplaceChild(dstSeg, srcSeg, (byte) 0x20, 999L);

            assertEquals(3, Node256.count(dstSeg));
            assertEquals(1L, Node256.findChild(dstSeg, (byte) 0x10));
            assertEquals(999L, Node256.findChild(dstSeg, (byte) 0x20));
            assertEquals(3L, Node256.findChild(dstSeg, (byte) 0x30));
            // Source unchanged
            assertEquals(2L, Node256.findChild(srcSeg, (byte) 0x20));
        }
    }

    @Test
    void cowInsertChild() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            alloc.registerClass(NodeConstants.PREFIX_SIZE);
            alloc.registerClass(NodeConstants.NODE4_SIZE);
            alloc.registerClass(NodeConstants.NODE16_SIZE);
            alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);

            long srcPtr = alloc.allocate(node256ClassId);
            var srcSeg = alloc.resolve(srcPtr);
            Node256.init(srcSeg);
            Node256.insertChild(srcSeg, (byte) 0x10, 1L);

            long dstPtr = alloc.allocate(node256ClassId);
            var dstSeg = alloc.resolve(dstPtr);
            Node256.cowInsertChild(dstSeg, srcSeg, (byte) 0x20, 2L);

            assertEquals(2, Node256.count(dstSeg));
            assertEquals(1L, Node256.findChild(dstSeg, (byte) 0x10));
            assertEquals(2L, Node256.findChild(dstSeg, (byte) 0x20));
            // Source unchanged
            assertEquals(1, Node256.count(srcSeg));
        }
    }

    @Test
    void cowRemoveChild() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            alloc.registerClass(NodeConstants.PREFIX_SIZE);
            alloc.registerClass(NodeConstants.NODE4_SIZE);
            alloc.registerClass(NodeConstants.NODE16_SIZE);
            alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);

            long srcPtr = alloc.allocate(node256ClassId);
            var srcSeg = alloc.resolve(srcPtr);
            Node256.init(srcSeg);
            Node256.insertChild(srcSeg, (byte) 0x10, 1L);
            Node256.insertChild(srcSeg, (byte) 0x20, 2L);
            Node256.insertChild(srcSeg, (byte) 0x30, 3L);

            long dstPtr = alloc.allocate(node256ClassId);
            var dstSeg = alloc.resolve(dstPtr);
            Node256.cowRemoveChild(dstSeg, srcSeg, (byte) 0x20);

            assertEquals(2, Node256.count(dstSeg));
            assertEquals(1L, Node256.findChild(dstSeg, (byte) 0x10));
            assertEquals(NodePtr.EMPTY_PTR, Node256.findChild(dstSeg, (byte) 0x20));
            assertEquals(3L, Node256.findChild(dstSeg, (byte) 0x30));
            // Source unchanged
            assertEquals(3, Node256.count(srcSeg));
        }
    }

    @Test
    void shrinkToNode48() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            alloc.registerClass(NodeConstants.PREFIX_SIZE);
            alloc.registerClass(NodeConstants.NODE4_SIZE);
            alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);

            long n256Ptr = alloc.allocate(node256ClassId);
            var n256Seg = alloc.resolve(n256Ptr);
            Node256.init(n256Seg);

            // Insert 36 entries (NODE256_SHRINK_THRESHOLD)
            for (int i = 0; i < 36; i++) {
                Node256.insertChild(n256Seg, (byte) (i * 7), (long) (i + 1));
            }

            long n48Ptr = alloc.allocate(node48ClassId);
            var n48Seg = alloc.resolve(n48Ptr);
            Node256.shrinkToNode48(n48Seg, n256Seg);

            assertEquals(36, Node48.count(n48Seg));
            for (int i = 0; i < 36; i++) {
                long child = Node48.findChild(n48Seg, (byte) (i * 7));
                assertEquals((long) (i + 1), child, "Missing entry at key " + (i * 7));
            }
        }
    }

    @Test
    void childOffset() {
        // Verify childOffset calculation for specific key bytes
        assertEquals(Node256.OFF_CHILDREN, Node256.childOffset((byte) 0));
        assertEquals(Node256.OFF_CHILDREN + 8, Node256.childOffset((byte) 1));
        assertEquals(Node256.OFF_CHILDREN + 255 * 8, Node256.childOffset((byte) 0xFF));
        assertEquals(Node256.OFF_CHILDREN + 128 * 8, Node256.childOffset((byte) 0x80));
    }
}
