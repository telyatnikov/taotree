package org.taotree.internal;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Node16Test {

    @Test
    void insertAndFindAll16() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node16ClassId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            for (int i = 0; i < 16; i++) {
                Node16.insertChild(seg, (byte) (i * 16), (long) (i + 1));
            }

            assertEquals(16, Node16.count(seg));
            assertTrue(Node16.isFull(seg));

            for (int i = 0; i < 16; i++) {
                assertEquals((long) (i + 1), Node16.findChild(seg, (byte) (i * 16)));
            }
            assertEquals(NodePtr.EMPTY_PTR, Node16.findChild(seg, (byte) 0xFF));
        }
    }

    @Test
    void sortedOrder() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node16ClassId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            // Insert in reverse
            byte[] keys = {(byte) 0xF0, (byte) 0xC0, (byte) 0x80, (byte) 0x40, (byte) 0x00};
            for (int i = 0; i < keys.length; i++) {
                Node16.insertChild(seg, keys[i], (long) (i + 1));
            }

            // Should be sorted by unsigned byte value
            assertEquals((byte) 0x00, Node16.keyAt(seg, 0));
            assertEquals((byte) 0x40, Node16.keyAt(seg, 1));
            assertEquals((byte) 0x80, Node16.keyAt(seg, 2));
            assertEquals((byte) 0xC0, Node16.keyAt(seg, 3));
            assertEquals((byte) 0xF0, Node16.keyAt(seg, 4));
        }
    }

    @Test
    void removeAndCompact() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node16ClassId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            for (int i = 0; i < 8; i++) {
                Node16.insertChild(seg, (byte) (i * 10), (long) (i + 100));
            }

            assertTrue(Node16.removeChild(seg, (byte) 30));
            assertEquals(7, Node16.count(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node16.findChild(seg, (byte) 30));
            // Others still present
            assertEquals(100L, Node16.findChild(seg, (byte) 0));
            assertEquals(104L, Node16.findChild(seg, (byte) 40));
        }
    }

    @Test
    void copyFromNode4() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            

            long n4Ptr = alloc.allocate(node4ClassId);
            var n4Seg = alloc.resolve(n4Ptr);
            Node4.init(n4Seg);
            Node4.insertChild(n4Seg, (byte) 0x10, 1L);
            Node4.insertChild(n4Seg, (byte) 0x20, 2L);
            Node4.insertChild(n4Seg, (byte) 0x30, 3L);
            Node4.insertChild(n4Seg, (byte) 0x40, 4L);

            long n16Ptr = alloc.allocate(node16ClassId);
            var n16Seg = alloc.resolve(n16Ptr);
            Node16.init(n16Seg);
            Node16.copyFromNode4(n16Seg, n4Seg);

            assertEquals(4, Node16.count(n16Seg));
            assertEquals(1L, Node16.findChild(n16Seg, (byte) 0x10));
            assertEquals(2L, Node16.findChild(n16Seg, (byte) 0x20));
            assertEquals(3L, Node16.findChild(n16Seg, (byte) 0x30));
            assertEquals(4L, Node16.findChild(n16Seg, (byte) 0x40));
        }
    }
}
