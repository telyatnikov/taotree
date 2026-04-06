package org.taotree.internal.art;

import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.Node4;
import org.taotree.internal.art.NodeConstants;

/**
 * Tests for node growth transitions: Node4 → Node16 → Node48 → Node256.
 * Validates that copy operations preserve all key-child mappings.
 */
class NodeGrowthTest {

    @Test
    void node4ToNode16() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createV2(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            

            var n4 = alloc.resolve(alloc.allocate(node4ClassId));
            Node4.init(n4);
            Node4.insertChild(n4, (byte) 0x10, 1L);
            Node4.insertChild(n4, (byte) 0x20, 2L);
            Node4.insertChild(n4, (byte) 0x30, 3L);
            Node4.insertChild(n4, (byte) 0x40, 4L);
            assertTrue(Node4.isFull(n4));

            var n16 = alloc.resolve(alloc.allocate(node16ClassId));
            Node16.init(n16);
            Node16.copyFromNode4(n16, n4);

            assertEquals(4, Node16.count(n16));
            assertEquals(1L, Node16.findChild(n16, (byte) 0x10));
            assertEquals(2L, Node16.findChild(n16, (byte) 0x20));
            assertEquals(3L, Node16.findChild(n16, (byte) 0x30));
            assertEquals(4L, Node16.findChild(n16, (byte) 0x40));

            // Can now insert a 5th child
            Node16.insertChild(n16, (byte) 0x50, 5L);
            assertEquals(5, Node16.count(n16));
            assertEquals(5L, Node16.findChild(n16, (byte) 0x50));
        }
    }

    @Test
    void node16ToNode48() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createV2(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            

            var n16 = alloc.resolve(alloc.allocate(node16ClassId));
            Node16.init(n16);
            for (int i = 0; i < 16; i++) {
                Node16.insertChild(n16, (byte) (i * 16), (long) (i + 1));
            }
            assertTrue(Node16.isFull(n16));

            var n48 = alloc.resolve(alloc.allocate(node48ClassId));
            Node48.init(n48);
            Node48.copyFromNode16(n48, n16);

            assertEquals(16, Node48.count(n48));
            for (int i = 0; i < 16; i++) {
                assertEquals((long) (i + 1), Node48.findChild(n48, (byte) (i * 16)));
            }

            // Can insert more
            Node48.insertChild(n48, (byte) 0x01, 100L);
            assertEquals(17, Node48.count(n48));
        }
    }

    @Test
    void node48ToNode256() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createV2(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            

            var n48 = alloc.resolve(alloc.allocate(node48ClassId));
            Node48.init(n48);
            for (int i = 0; i < 48; i++) {
                Node48.insertChild(n48, (byte) (i * 5), (long) (i + 1));
            }
            assertTrue(Node48.isFull(n48));

            var n256 = alloc.resolve(alloc.allocate(node256ClassId));
            Node256.init(n256);
            Node256.copyFromNode48(n256, n48);

            assertEquals(48, Node256.count(n256));
            for (int i = 0; i < 48; i++) {
                assertEquals((long) (i + 1), Node256.findChild(n256, (byte) (i * 5)));
            }

            // Can insert more up to 256
            Node256.insertChild(n256, (byte) 0x01, 200L);
            assertEquals(49, Node256.count(n256));
        }
    }

    @Test
    void fullGrowthChain() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createV2(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var alloc = new SlabAllocator(arena, cs, 64 * 1024);
            int prefixClassId = alloc.registerClass(NodeConstants.PREFIX_SIZE);
            int node4ClassId = alloc.registerClass(NodeConstants.NODE4_SIZE);
            int node16ClassId = alloc.registerClass(NodeConstants.NODE16_SIZE);
            int node48ClassId = alloc.registerClass(NodeConstants.NODE48_SIZE);
            int node256ClassId = alloc.registerClass(NodeConstants.NODE256_SIZE);
            

            // Start with Node4, fill it
            var n4 = alloc.resolve(alloc.allocate(node4ClassId));
            Node4.init(n4);
            for (int i = 0; i < 4; i++) {
                Node4.insertChild(n4, (byte) i, (long) (i + 1));
            }

            // Grow to Node16
            var n16 = alloc.resolve(alloc.allocate(node16ClassId));
            Node16.init(n16);
            Node16.copyFromNode4(n16, n4);
            for (int i = 4; i < 16; i++) {
                Node16.insertChild(n16, (byte) i, (long) (i + 1));
            }

            // Grow to Node48
            var n48 = alloc.resolve(alloc.allocate(node48ClassId));
            Node48.init(n48);
            Node48.copyFromNode16(n48, n16);
            for (int i = 16; i < 48; i++) {
                Node48.insertChild(n48, (byte) i, (long) (i + 1));
            }

            // Grow to Node256
            var n256 = alloc.resolve(alloc.allocate(node256ClassId));
            Node256.init(n256);
            Node256.copyFromNode48(n256, n48);

            // Verify all 48 entries survived the full chain
            assertEquals(48, Node256.count(n256));
            for (int i = 0; i < 48; i++) {
                assertEquals((long) (i + 1), Node256.findChild(n256, (byte) i),
                    "Missing child for key " + i);
            }
        }
    }
}
