package org.taotree.internal.art;

import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.NodeConstants;
import org.taotree.internal.art.NodePtr;

class Node48Test {

    @Test
    void insertAndFindO1() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node48ClassId);
            var seg = alloc.resolve(ptr);
            Node48.init(seg);

            Node48.insertChild(seg, (byte) 0x00, 1L);
            Node48.insertChild(seg, (byte) 0x7F, 2L);
            Node48.insertChild(seg, (byte) 0xFF, 3L);

            assertEquals(3, Node48.count(seg));
            assertEquals(1L, Node48.findChild(seg, (byte) 0x00));
            assertEquals(2L, Node48.findChild(seg, (byte) 0x7F));
            assertEquals(3L, Node48.findChild(seg, (byte) 0xFF));
            assertEquals(NodePtr.EMPTY_PTR, Node48.findChild(seg, (byte) 0x80));
        }
    }

    @Test
    void fillAll48Slots() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node48ClassId);
            var seg = alloc.resolve(ptr);
            Node48.init(seg);

            for (int i = 0; i < 48; i++) {
                Node48.insertChild(seg, (byte) (i * 5), (long) (i + 100));
            }
            assertEquals(48, Node48.count(seg));
            assertTrue(Node48.isFull(seg));

            for (int i = 0; i < 48; i++) {
                assertEquals((long) (i + 100), Node48.findChild(seg, (byte) (i * 5)));
            }
        }
    }

    @Test
    void removeChild() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node48ClassId);
            var seg = alloc.resolve(ptr);
            Node48.init(seg);

            Node48.insertChild(seg, (byte) 0x10, 1L);
            Node48.insertChild(seg, (byte) 0x20, 2L);
            Node48.insertChild(seg, (byte) 0x30, 3L);

            assertTrue(Node48.removeChild(seg, (byte) 0x20));
            assertEquals(2, Node48.count(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node48.findChild(seg, (byte) 0x20));
            assertEquals(1L, Node48.findChild(seg, (byte) 0x10));
            assertEquals(3L, Node48.findChild(seg, (byte) 0x30));

            assertFalse(Node48.removeChild(seg, (byte) 0x99));
        }
    }

    @Test
    void removeAndReinsert() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node48ClassId);
            var seg = alloc.resolve(ptr);
            Node48.init(seg);

            Node48.insertChild(seg, (byte) 0xAA, 1L);
            Node48.removeChild(seg, (byte) 0xAA);
            assertEquals(0, Node48.count(seg));

            // Reinsert — should reuse the freed slot
            Node48.insertChild(seg, (byte) 0xAA, 2L);
            assertEquals(1, Node48.count(seg));
            assertEquals(2L, Node48.findChild(seg, (byte) 0xAA));
        }
    }

    @Test
    void copyFromNode16() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            

            long n16Ptr = alloc.allocate(node16ClassId);
            var n16Seg = alloc.resolve(n16Ptr);
            Node16.init(n16Seg);
            for (int i = 0; i < 16; i++) {
                Node16.insertChild(n16Seg, (byte) (i * 10), (long) (i + 1));
            }

            long n48Ptr = alloc.allocate(node48ClassId);
            var n48Seg = alloc.resolve(n48Ptr);
            Node48.init(n48Seg);
            Node48.copyFromNode16(n48Seg, n16Seg);

            assertEquals(16, Node48.count(n48Seg));
            for (int i = 0; i < 16; i++) {
                assertEquals((long) (i + 1), Node48.findChild(n48Seg, (byte) (i * 10)));
            }
        }
    }

    @Test
    void forEach() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            
            long ptr = alloc.allocate(node48ClassId);
            var seg = alloc.resolve(ptr);
            Node48.init(seg);

            Node48.insertChild(seg, (byte) 0x10, 1L);
            Node48.insertChild(seg, (byte) 0x20, 2L);
            Node48.insertChild(seg, (byte) 0x30, 3L);

            Set<Byte> keys = new HashSet<>();
            long[] sum = {0};
            Node48.forEach(seg, (key, child) -> {
                keys.add(key);
                sum[0] += child;
            });

            assertEquals(Set.of((byte) 0x10, (byte) 0x20, (byte) 0x30), keys);
            assertEquals(6L, sum[0]);
        }
    }

    // ---- Mutation-killing: isFull, init, removeChild ----

    @Test
    void isFullReturnsFalseBeforeCapacity() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node48.init(seg);

            for (int i = 0; i < 47; i++) {
                assertFalse(Node48.isFull(seg));
                Node48.insertChild(seg, (byte) i, (long) (i + 1));
            }
            assertFalse(Node48.isFull(seg)); // 47 not full
            Node48.insertChild(seg, (byte) 47, 48L);
            assertTrue(Node48.isFull(seg)); // 48 = full
        }
    }

    @Test
    void removeChildClearsSlot() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node48.init(seg);

            Node48.insertChild(seg, (byte) 0x10, 1L);
            Node48.insertChild(seg, (byte) 0x20, 2L);
            Node48.insertChild(seg, (byte) 0x30, 3L);

            assertTrue(Node48.removeChild(seg, (byte) 0x20));
            assertEquals(2, Node48.count(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node48.findChild(seg, (byte) 0x20));
            assertEquals(1L, Node48.findChild(seg, (byte) 0x10));
            assertEquals(3L, Node48.findChild(seg, (byte) 0x30));

            // Reinsert at the freed slot
            Node48.insertChild(seg, (byte) 0x20, 22L);
            assertEquals(3, Node48.count(seg));
            assertEquals(22L, Node48.findChild(seg, (byte) 0x20));
        }
    }
}
