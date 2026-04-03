package org.taotree.internal;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.*;

class PrefixNodeTest {

    @Test
    void initAndRead() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            byte[] key = {0x01, 0x02, 0x03, 0x04, 0x05};
            long childPtr = 0xDEAD_BEEFL;
            PrefixNode.init(seg, key, 0, key.length, childPtr);

            assertEquals(5, PrefixNode.count(seg));
            assertEquals(0x01, PrefixNode.keyAt(seg, 0));
            assertEquals(0x05, PrefixNode.keyAt(seg, 4));
            assertEquals(childPtr, PrefixNode.child(seg));
        }
    }

    @Test
    void initFromMiddleOfKey() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            byte[] key = {0x00, 0x00, (byte) 0xAA, (byte) 0xBB, (byte) 0xCC, 0x00};
            PrefixNode.init(seg, key, 2, 3, 42L);

            assertEquals(3, PrefixNode.count(seg));
            assertEquals((byte) 0xAA, PrefixNode.keyAt(seg, 0));
            assertEquals((byte) 0xBB, PrefixNode.keyAt(seg, 1));
            assertEquals((byte) 0xCC, PrefixNode.keyAt(seg, 2));
            assertEquals(42L, PrefixNode.child(seg));
        }
    }

    @Test
    void matchKeyFullMatch() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            byte[] prefixBytes = {0x10, 0x20, 0x30};
            PrefixNode.init(seg, prefixBytes, 0, 3, 0L);

            byte[] searchKey = {0x10, 0x20, 0x30, 0x40, 0x50};
            int matched = PrefixNode.matchKey(seg, searchKey, searchKey.length, 0);
            assertEquals(3, matched); // full prefix matched
        }
    }

    @Test
    void matchKeyPartialMatch() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            byte[] prefixBytes = {0x10, 0x20, 0x30};
            PrefixNode.init(seg, prefixBytes, 0, 3, 0L);

            byte[] searchKey = {0x10, 0x20, (byte) 0xFF, 0x40};
            int matched = PrefixNode.matchKey(seg, searchKey, searchKey.length, 0);
            assertEquals(2, matched); // mismatch at position 2
        }
    }

    @Test
    void matchKeyWithDepthOffset() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            byte[] prefixBytes = {(byte) 0xAA, (byte) 0xBB};
            PrefixNode.init(seg, prefixBytes, 0, 2, 0L);

            byte[] searchKey = {0x00, 0x00, (byte) 0xAA, (byte) 0xBB, (byte) 0xCC};
            int matched = PrefixNode.matchKey(seg, searchKey, searchKey.length, 2);
            assertEquals(2, matched); // starts comparing at depth=2
        }
    }

    @Test
    void matchKeyNoMatch() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            byte[] prefixBytes = {0x10, 0x20};
            PrefixNode.init(seg, prefixBytes, 0, 2, 0L);

            byte[] searchKey = {(byte) 0xFF, (byte) 0xFF};
            int matched = PrefixNode.matchKey(seg, searchKey, searchKey.length, 0);
            assertEquals(0, matched);
        }
    }

    @Test
    void matchKeyWithMemorySegment() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);

            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            byte[] prefixBytes = {0x10, 0x20, 0x30};
            PrefixNode.init(seg, prefixBytes, 0, 3, 0L);

            MemorySegment searchKey = arena.allocate(5);
            searchKey.set(ValueLayout.JAVA_BYTE, 0, (byte) 0x10);
            searchKey.set(ValueLayout.JAVA_BYTE, 1, (byte) 0x20);
            searchKey.set(ValueLayout.JAVA_BYTE, 2, (byte) 0x30);
            searchKey.set(ValueLayout.JAVA_BYTE, 3, (byte) 0x40);
            searchKey.set(ValueLayout.JAVA_BYTE, 4, (byte) 0x50);

            int matched = PrefixNode.matchKey(seg, searchKey, 5, 0);
            assertEquals(3, matched);
        }
    }

    @Test
    void initFromMemorySegment() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);

            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            MemorySegment keySeg = arena.allocate(4);
            keySeg.set(ValueLayout.JAVA_BYTE, 0, (byte) 0xAA);
            keySeg.set(ValueLayout.JAVA_BYTE, 1, (byte) 0xBB);
            keySeg.set(ValueLayout.JAVA_BYTE, 2, (byte) 0xCC);
            keySeg.set(ValueLayout.JAVA_BYTE, 3, (byte) 0xDD);

            PrefixNode.init(seg, keySeg, 1, 2, 99L);

            assertEquals(2, PrefixNode.count(seg));
            assertEquals((byte) 0xBB, PrefixNode.keyAt(seg, 0));
            assertEquals((byte) 0xCC, PrefixNode.keyAt(seg, 1));
            assertEquals(99L, PrefixNode.child(seg));
        }
    }

    @Test
    void setChild() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            PrefixNode.init(seg, new byte[]{0x01}, 0, 1, 1L);
            assertEquals(1L, PrefixNode.child(seg));

            PrefixNode.setChild(seg, 42L);
            assertEquals(42L, PrefixNode.child(seg));
        }
    }

    @Test
    void maxPrefixCapacity() {
        try (var arena = Arena.ofConfined()) {
            var alloc = new SlabAllocator(arena, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(prefixClassId);
            var seg = alloc.resolve(ptr);

            byte[] key = new byte[NodeConstants.PREFIX_CAPACITY];
            for (int i = 0; i < key.length; i++) key[i] = (byte) (i + 1);
            PrefixNode.init(seg, key, 0, key.length, 0L);

            assertEquals(NodeConstants.PREFIX_CAPACITY, PrefixNode.count(seg));
            for (int i = 0; i < key.length; i++) {
                assertEquals(key[i], PrefixNode.keyAt(seg, i));
            }
        }
    }
}
