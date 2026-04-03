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
}
