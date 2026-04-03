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

    @Test
    void roundTripMultipleClassesAndTrees() {
        try (var arena = Arena.ofConfined()) {
            var page = arena.allocate(4096);

            var data = new Superblock.SuperblockData();
            data.slabSize = 1048576;
            data.bumpPageSize = 65536;
            data.chunkSize = 67108864L;
            data.totalPages = 500;
            data.nextPage = 250;
            data.bumpPageCount = 5;
            data.bumpCurrentPage = 4;
            data.bumpOffset = 1234;
            data.bumpBytesAllocated = 99999L;

            // 4 slab classes with different counts
            data.classes = new Superblock.SlabClassDescriptor[4];
            for (int c = 0; c < 4; c++) {
                var cls = new Superblock.SlabClassDescriptor();
                cls.segmentSize = 64 * (c + 1);
                cls.slabCount = c + 1;
                cls.segmentsInUse = c * 10 + 5;
                cls.dataStartPages = new int[cls.slabCount];
                cls.bitmaskStartPages = new int[cls.slabCount];
                cls.bitmaskPageCounts = new int[cls.slabCount];
                for (int s = 0; s < cls.slabCount; s++) {
                    cls.dataStartPages[s] = 100 * c + 10 * s + 1;
                    cls.bitmaskStartPages[s] = 100 * c + 10 * s + 2;
                    cls.bitmaskPageCounts[s] = s + 1;
                }
                data.classes[c] = cls;
            }

            // 3 trees with varying leaf classes
            data.trees = new Superblock.TreeDescriptor[3];
            for (int t = 0; t < 3; t++) {
                var tree = new Superblock.TreeDescriptor();
                tree.root = (long) (t + 1) * 1000;
                tree.size = (t + 1) * 100;
                tree.keyLen = (t + 1) * 4;
                tree.prefixClassId = t * 10;
                tree.node4ClassId = t * 10 + 1;
                tree.node16ClassId = t * 10 + 2;
                tree.node48ClassId = t * 10 + 3;
                tree.node256ClassId = t * 10 + 4;
                tree.leafValueSizes = new int[t + 1];
                tree.leafClassIds = new int[t + 1];
                for (int i = 0; i <= t; i++) {
                    tree.leafValueSizes[i] = 8 * (i + 1);
                    tree.leafClassIds[i] = t * 10 + 5 + i;
                }
                data.trees[t] = tree;
            }

            // 2 dictionaries
            data.dicts = new Superblock.DictDescriptor[2];
            for (int d = 0; d < 2; d++) {
                var dict = new Superblock.DictDescriptor();
                dict.maxCode = (d == 0) ? 0xFFFF : Integer.MAX_VALUE;
                dict.nextCode = d * 50 + 1;
                dict.treeIndex = d + 1;
                data.dicts[d] = dict;
            }

            // 5 bump pages
            data.bumpPageLocations = new int[]{10, 20, 30, 40, 50};
            data.bumpPageSizes = new int[]{1, 2, 3, 4, 5};

            int written = Superblock.write(page, data);
            assertTrue(written > 0);

            var read = Superblock.read(page);

            // Verify header
            assertEquals(data.slabSize, read.slabSize);
            assertEquals(data.bumpPageSize, read.bumpPageSize);
            assertEquals(data.chunkSize, read.chunkSize);
            assertEquals(data.totalPages, read.totalPages);
            assertEquals(data.nextPage, read.nextPage);
            assertEquals(data.bumpPageCount, read.bumpPageCount);
            assertEquals(data.bumpCurrentPage, read.bumpCurrentPage);
            assertEquals(data.bumpOffset, read.bumpOffset);
            assertEquals(data.bumpBytesAllocated, read.bumpBytesAllocated);

            // Verify slab classes
            assertEquals(4, read.classes.length);
            for (int c = 0; c < 4; c++) {
                assertEquals(data.classes[c].segmentSize, read.classes[c].segmentSize, "class " + c + " segmentSize");
                assertEquals(data.classes[c].slabCount, read.classes[c].slabCount, "class " + c + " slabCount");
                assertEquals(data.classes[c].segmentsInUse, read.classes[c].segmentsInUse, "class " + c + " segmentsInUse");
                for (int s = 0; s < data.classes[c].slabCount; s++) {
                    assertEquals(data.classes[c].dataStartPages[s], read.classes[c].dataStartPages[s], "class " + c + " slab " + s + " dataStart");
                    assertEquals(data.classes[c].bitmaskStartPages[s], read.classes[c].bitmaskStartPages[s], "class " + c + " slab " + s + " bmStart");
                    assertEquals(data.classes[c].bitmaskPageCounts[s], read.classes[c].bitmaskPageCounts[s], "class " + c + " slab " + s + " bmCount");
                }
            }

            // Verify trees
            assertEquals(3, read.trees.length);
            for (int t = 0; t < 3; t++) {
                assertEquals(data.trees[t].root, read.trees[t].root, "tree " + t + " root");
                assertEquals(data.trees[t].size, read.trees[t].size, "tree " + t + " size");
                assertEquals(data.trees[t].keyLen, read.trees[t].keyLen, "tree " + t + " keyLen");
                assertEquals(data.trees[t].prefixClassId, read.trees[t].prefixClassId, "tree " + t + " prefixClassId");
                assertEquals(data.trees[t].node4ClassId, read.trees[t].node4ClassId, "tree " + t + " node4ClassId");
                assertEquals(data.trees[t].node16ClassId, read.trees[t].node16ClassId, "tree " + t + " node16ClassId");
                assertEquals(data.trees[t].node48ClassId, read.trees[t].node48ClassId, "tree " + t + " node48ClassId");
                assertEquals(data.trees[t].node256ClassId, read.trees[t].node256ClassId, "tree " + t + " node256ClassId");
                assertArrayEquals(data.trees[t].leafValueSizes, read.trees[t].leafValueSizes, "tree " + t + " leafValueSizes");
                assertArrayEquals(data.trees[t].leafClassIds, read.trees[t].leafClassIds, "tree " + t + " leafClassIds");
            }

            // Verify dicts
            assertEquals(2, read.dicts.length);
            for (int d = 0; d < 2; d++) {
                assertEquals(data.dicts[d].maxCode, read.dicts[d].maxCode, "dict " + d + " maxCode");
                assertEquals(data.dicts[d].nextCode, read.dicts[d].nextCode, "dict " + d + " nextCode");
                assertEquals(data.dicts[d].treeIndex, read.dicts[d].treeIndex, "dict " + d + " treeIndex");
            }

            // Verify bump pages
            assertArrayEquals(data.bumpPageLocations, read.bumpPageLocations);
            assertArrayEquals(data.bumpPageSizes, read.bumpPageSizes);
        }
    }
}
