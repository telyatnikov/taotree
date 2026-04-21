package org.taotree.internal.art;

import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node4;
import org.taotree.internal.art.NodeConstants;
import org.taotree.internal.art.NodePtr;

class Node16Test {

    @Test
    void insertAndFindAll16() throws Exception {
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
    void sortedOrder() throws Exception {
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
    void removeAndCompact() throws Exception {
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
    void copyFromNode4() throws Exception {
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

    // ---- Mutation-killing: init, isFull, removeChild ----

    @Test
    void initSetsCountToZero() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            assertEquals(0, Node16.count(seg));
            assertFalse(Node16.isFull(seg));
        }
    }

    @Test
    void isFullAtCapacity() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            for (int i = 0; i < 16; i++) {
                assertFalse(Node16.isFull(seg));
                Node16.insertChild(seg, (byte) (i * 16), (long) (i + 1));
            }
            assertTrue(Node16.isFull(seg));
        }
    }

    @Test
    void removeReducesCountAndCompacts() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            for (int i = 0; i < 8; i++) {
                Node16.insertChild(seg, (byte) (i * 10), (long) (i + 1));
            }
            assertEquals(8, Node16.count(seg));

            // Remove middle element
            assertTrue(Node16.removeChild(seg, (byte) 40));
            assertEquals(7, Node16.count(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node16.findChild(seg, (byte) 40));

            // All others still present
            for (int i = 0; i < 8; i++) {
                if (i == 4) continue;
                assertNotEquals(NodePtr.EMPTY_PTR, Node16.findChild(seg, (byte) (i * 10)));
            }
        }
    }

    @Test
    void findChildNotFound() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            Node16.insertChild(seg, (byte) 0x42, 1L);
            assertEquals(NodePtr.EMPTY_PTR, Node16.findChild(seg, (byte) 0x43));
        }
    }

    // ---- STRONGER: remove first, last, and middle children ----

    @Test
    void removeFirstChild() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            // Insert 5 sorted keys
            byte[] keys = {0x10, 0x20, 0x30, 0x40, 0x50};
            for (int i = 0; i < keys.length; i++) {
                Node16.insertChild(seg, keys[i], (long) (i + 1));
            }

            // Remove first (requires shifting all remaining left)
            assertTrue(Node16.removeChild(seg, (byte) 0x10));
            assertEquals(4, Node16.count(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node16.findChild(seg, (byte) 0x10));
            for (int i = 1; i < keys.length; i++) {
                assertEquals((long) (i + 1), Node16.findChild(seg, keys[i]));
            }
        }
    }

    @Test
    void removeLastChild() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            byte[] keys = {0x10, 0x20, 0x30, 0x40, 0x50};
            for (int i = 0; i < keys.length; i++) {
                Node16.insertChild(seg, keys[i], (long) (i + 1));
            }

            // Remove last (no shifting needed)
            assertTrue(Node16.removeChild(seg, (byte) 0x50));
            assertEquals(4, Node16.count(seg));
            assertEquals(NodePtr.EMPTY_PTR, Node16.findChild(seg, (byte) 0x50));
            for (int i = 0; i < keys.length - 1; i++) {
                assertEquals((long) (i + 1), Node16.findChild(seg, keys[i]));
            }
        }
    }

    @Test
    void removeNonExistentChild() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, SlabAllocator.DEFAULT_SLAB_SIZE);
            int classId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            long ptr = alloc.allocate(classId);
            var seg = alloc.resolve(ptr);
            Node16.init(seg);

            Node16.insertChild(seg, (byte) 0x10, 1L);
            assertFalse(Node16.removeChild(seg, (byte) 0x20));
            assertEquals(1, Node16.count(seg));
        }
    }
}
