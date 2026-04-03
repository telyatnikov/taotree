package org.taotree.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SuperblockTest {

    @Test
    void roundTrip() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(ChunkStore.PAGE_SIZE, 1);

            // Build test data
            var data = new Superblock.SuperblockData();
            data.slabSize = 1 << 20;
            data.bumpPageSize = 64 * 1024;
            data.chunkSize = 64L * 1024 * 1024;
            data.totalPages = 16384;
            data.nextPage = 300;

            data.bumpPageCount = 3;
            data.bumpCurrentPage = 2;
            data.bumpOffset = 1234;
            data.bumpBytesAllocated = 5678L;
            data.bumpPageLocations = new int[]{100, 200, 300};
            data.bumpPageSizes = new int[]{16, 16, 16};

            // 2 slab classes
            var cls0 = new Superblock.SlabClassDescriptor();
            cls0.segmentSize = 24;
            cls0.slabCount = 2;
            cls0.segmentsInUse = 100;
            cls0.dataStartPages = new int[]{10, 270};
            cls0.bitmaskStartPages = new int[]{266, 540};
            cls0.bitmaskPageCounts = new int[]{2, 2};

            var cls1 = new Superblock.SlabClassDescriptor();
            cls1.segmentSize = 40;
            cls1.slabCount = 1;
            cls1.segmentsInUse = 50;
            cls1.dataStartPages = new int[]{542};
            cls1.bitmaskStartPages = new int[]{798};
            cls1.bitmaskPageCounts = new int[]{1};

            data.classes = new Superblock.SlabClassDescriptor[]{cls0, cls1};

            // 2 trees
            var tree0 = new Superblock.TreeDescriptor();
            tree0.root = 0x0600_0001_0000_0018L; // LEAF pointer
            tree0.size = 42;
            tree0.keyLen = 16;
            tree0.prefixClassId = 0;
            tree0.node4ClassId = 1;
            tree0.node16ClassId = 2;
            tree0.node48ClassId = 3;
            tree0.node256ClassId = 4;
            tree0.leafValueSizes = new int[]{24};
            tree0.leafClassIds = new int[]{5};

            var tree1 = new Superblock.TreeDescriptor();
            tree1.root = 0x0600_0003_0000_0000L;
            tree1.size = 7;
            tree1.keyLen = 128;
            tree1.prefixClassId = 0;
            tree1.node4ClassId = 1;
            tree1.node16ClassId = 2;
            tree1.node48ClassId = 3;
            tree1.node256ClassId = 4;
            tree1.leafValueSizes = new int[]{4};
            tree1.leafClassIds = new int[]{6};

            data.trees = new Superblock.TreeDescriptor[]{tree0, tree1};

            // 1 dict
            var dict0 = new Superblock.DictDescriptor();
            dict0.maxCode = 0xFFFF;
            dict0.nextCode = 8;
            dict0.treeIndex = 1;
            data.dicts = new Superblock.DictDescriptor[]{dict0};

            // Write
            int bytesWritten = Superblock.write(page, data);
            assertTrue(bytesWritten > 0);
            assertTrue(bytesWritten < ChunkStore.PAGE_SIZE,
                "Superblock overflows page: " + bytesWritten);

            // Read back
            var restored = Superblock.read(page);

            // Verify fixed header
            assertEquals(data.slabSize, restored.slabSize);
            assertEquals(data.bumpPageSize, restored.bumpPageSize);
            assertEquals(data.chunkSize, restored.chunkSize);
            assertEquals(data.totalPages, restored.totalPages);
            assertEquals(data.nextPage, restored.nextPage);

            // Verify bump state
            assertEquals(data.bumpPageCount, restored.bumpPageCount);
            assertEquals(data.bumpCurrentPage, restored.bumpCurrentPage);
            assertEquals(data.bumpOffset, restored.bumpOffset);
            assertEquals(data.bumpBytesAllocated, restored.bumpBytesAllocated);
            assertArrayEquals(data.bumpPageLocations, restored.bumpPageLocations);
            assertArrayEquals(data.bumpPageSizes, restored.bumpPageSizes);

            // Verify slab classes
            assertEquals(2, restored.classes.length);
            assertEquals(cls0.segmentSize, restored.classes[0].segmentSize);
            assertEquals(cls0.slabCount, restored.classes[0].slabCount);
            assertEquals(cls0.segmentsInUse, restored.classes[0].segmentsInUse);
            assertArrayEquals(cls0.dataStartPages, restored.classes[0].dataStartPages);
            assertArrayEquals(cls0.bitmaskStartPages, restored.classes[0].bitmaskStartPages);
            assertArrayEquals(cls0.bitmaskPageCounts, restored.classes[0].bitmaskPageCounts);
            assertEquals(cls1.segmentSize, restored.classes[1].segmentSize);
            assertEquals(cls1.slabCount, restored.classes[1].slabCount);

            // Verify trees
            assertEquals(2, restored.trees.length);
            assertEquals(tree0.root, restored.trees[0].root);
            assertEquals(tree0.size, restored.trees[0].size);
            assertEquals(tree0.keyLen, restored.trees[0].keyLen);
            assertEquals(tree0.prefixClassId, restored.trees[0].prefixClassId);
            assertEquals(tree0.node256ClassId, restored.trees[0].node256ClassId);
            assertArrayEquals(tree0.leafValueSizes, restored.trees[0].leafValueSizes);
            assertArrayEquals(tree0.leafClassIds, restored.trees[0].leafClassIds);
            assertEquals(tree1.root, restored.trees[1].root);
            assertEquals(tree1.keyLen, restored.trees[1].keyLen);

            // Verify dicts
            assertEquals(1, restored.dicts.length);
            assertEquals(dict0.maxCode, restored.dicts[0].maxCode);
            assertEquals(dict0.nextCode, restored.dicts[0].nextCode);
            assertEquals(dict0.treeIndex, restored.dicts[0].treeIndex);
        }
    }

    @Test
    void invalidMagicThrows() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(ChunkStore.PAGE_SIZE, 1);
            page.fill((byte) 0);
            assertThrows(IllegalStateException.class, () -> Superblock.read(page));
        }
    }

    @Test
    void emptyStore() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(ChunkStore.PAGE_SIZE, 1);

            var data = new Superblock.SuperblockData();
            data.slabSize = 1 << 20;
            data.bumpPageSize = 64 * 1024;
            data.chunkSize = 64L * 1024 * 1024;
            data.totalPages = 16384;
            data.nextPage = 1;
            data.bumpCurrentPage = -1;

            Superblock.write(page, data);
            var restored = Superblock.read(page);

            assertEquals(0, restored.classes.length);
            assertEquals(0, restored.trees.length);
            assertEquals(0, restored.dicts.length);
            assertEquals(0, restored.bumpPageLocations.length);
        }
    }
}
