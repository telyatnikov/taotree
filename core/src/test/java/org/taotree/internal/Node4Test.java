package org.taotree.internal;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Node4Test {

    @Test
    void initAndEmpty() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node4ClassId);
            var seg = alloc.resolve(ptr);
            Node4.init(seg);

            assertEquals(0, Node4.count(seg));
            assertFalse(Node4.isFull(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node4.findChild(seg, (byte) 0x42));
        }
    }

    @Test
    void insertAndFind() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node4ClassId);
            var seg = alloc.resolve(ptr);
            Node4.init(seg);

            long child1 = 0xAAAA_AAAAL;
            long child2 = 0xBBBB_BBBBL;
            Node4.insertChild(seg, (byte) 0x10, child1);
            Node4.insertChild(seg, (byte) 0x20, child2);

            assertEquals(2, Node4.count(seg));
            assertEquals(child1, Node4.findChild(seg, (byte) 0x10));
            assertEquals(child2, Node4.findChild(seg, (byte) 0x20));
            assertEquals(NodePtr.EMPTY_PTR, Node4.findChild(seg, (byte) 0x30));
        }
    }

    @Test
    void insertMaintainsSortedOrder() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node4ClassId);
            var seg = alloc.resolve(ptr);
            Node4.init(seg);

            // Insert in reverse order
            Node4.insertChild(seg, (byte) 0xFF, 4L);
            Node4.insertChild(seg, (byte) 0x80, 3L);
            Node4.insertChild(seg, (byte) 0x40, 2L);
            Node4.insertChild(seg, (byte) 0x00, 1L);

            assertEquals(4, Node4.count(seg));
            assertTrue(Node4.isFull(seg));

            // Keys should be sorted by unsigned byte value
            assertEquals((byte) 0x00, Node4.keyAt(seg, 0));
            assertEquals((byte) 0x40, Node4.keyAt(seg, 1));
            assertEquals((byte) 0x80, Node4.keyAt(seg, 2));
            assertEquals((byte) 0xFF, Node4.keyAt(seg, 3));

            // Children should follow their keys
            assertEquals(1L, Node4.childAt(seg, 0));
            assertEquals(2L, Node4.childAt(seg, 1));
            assertEquals(3L, Node4.childAt(seg, 2));
            assertEquals(4L, Node4.childAt(seg, 3));
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
            
            long ptr = alloc.allocate(node4ClassId);
            var seg = alloc.resolve(ptr);
            Node4.init(seg);

            Node4.insertChild(seg, (byte) 0x10, 1L);
            Node4.insertChild(seg, (byte) 0x20, 2L);
            Node4.insertChild(seg, (byte) 0x30, 3L);

            assertTrue(Node4.removeChild(seg, (byte) 0x20));
            assertEquals(2, Node4.count(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node4.findChild(seg, (byte) 0x20));
            assertEquals(1L, Node4.findChild(seg, (byte) 0x10));
            assertEquals(3L, Node4.findChild(seg, (byte) 0x30));

            // Remove non-existent
            assertFalse(Node4.removeChild(seg, (byte) 0x99));
        }
    }

    @Test
    void findPos() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node4ClassId);
            var seg = alloc.resolve(ptr);
            Node4.init(seg);

            Node4.insertChild(seg, (byte) 0xAA, 1L);
            Node4.insertChild(seg, (byte) 0xBB, 2L);

            assertTrue(Node4.findPos(seg, (byte) 0xAA) >= 0);
            assertTrue(Node4.findPos(seg, (byte) 0xBB) >= 0);
            assertEquals(-1, Node4.findPos(seg, (byte) 0xCC));
        }
    }
}
