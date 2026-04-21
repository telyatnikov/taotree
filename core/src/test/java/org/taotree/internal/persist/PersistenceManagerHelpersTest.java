package org.taotree.internal.persist;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PersistenceManager} helpers that don't need a
 * real ChunkStore — they run entirely off {@link Superblock.SuperblockData}.
 */
class PersistenceManagerHelpersTest {

    private static Superblock.TreeDescriptor tree(long root, long size) {
        var t = new Superblock.TreeDescriptor();
        t.root = root;
        t.size = size;
        return t;
    }

    private static Superblock.DictDescriptor dict(int treeIndex, int nextCode) {
        var d = new Superblock.DictDescriptor();
        d.treeIndex = treeIndex;
        d.nextCode = nextCode;
        return d;
    }

    @Test
    void dictRootsFromDataMapsDictsToTreeRoots() {
        var data = new Superblock.SuperblockData();
        data.trees = new Superblock.TreeDescriptor[]{
                tree(0xAAAA_BBBBL, 10), tree(0xCCCC_DDDDL, 20), tree(0xEEEE_FFFFL, 30)
        };
        data.dicts = new Superblock.DictDescriptor[]{
                dict(1, 0), dict(2, 0)
        };
        long[] roots = PersistenceManager.dictRootsFromData(data);
        assertEquals(2, roots.length);
        assertEquals(0xCCCC_DDDDL, roots[0]);
        assertEquals(0xEEEE_FFFFL, roots[1]);
    }

    @Test
    void dictRootsFromDataOutOfRangeTreeIndexYieldsZero() {
        // If dict's treeIndex ≥ trees.length, the helper must not crash and the
        // corresponding root is 0 (sentinel).
        var data = new Superblock.SuperblockData();
        data.trees = new Superblock.TreeDescriptor[]{ tree(0x1L, 1) };
        data.dicts = new Superblock.DictDescriptor[]{ dict(5, 0) };
        long[] roots = PersistenceManager.dictRootsFromData(data);
        assertEquals(1, roots.length);
        assertEquals(0L, roots[0]);
    }

    @Test
    void dictSizesFromDataMapsDictsToTreeSizes() {
        var data = new Superblock.SuperblockData();
        data.trees = new Superblock.TreeDescriptor[]{
                tree(1, 111), tree(2, 222), tree(3, 333)
        };
        data.dicts = new Superblock.DictDescriptor[]{
                dict(0, 0), dict(2, 0)
        };
        long[] sizes = PersistenceManager.dictSizesFromData(data);
        assertEquals(2, sizes.length);
        assertEquals(111, sizes[0]);
        assertEquals(333, sizes[1]);
    }

    @Test
    void dictSizesFromDataOutOfRangeTreeIndexYieldsZero() {
        var data = new Superblock.SuperblockData();
        data.trees = new Superblock.TreeDescriptor[]{ tree(1, 1) };
        data.dicts = new Superblock.DictDescriptor[]{ dict(99, 0) };
        long[] sizes = PersistenceManager.dictSizesFromData(data);
        assertEquals(1, sizes.length);
        assertEquals(0, sizes[0]);
    }

    @Test
    void dictNextCodesFromDataReturnsArrayOfCodes() {
        var data = new Superblock.SuperblockData();
        data.trees = new Superblock.TreeDescriptor[]{ tree(1, 1) };
        data.dicts = new Superblock.DictDescriptor[]{
                dict(0, 5), dict(0, 100), dict(0, 0)
        };
        long[] codes = PersistenceManager.dictNextCodesFromData(data);
        assertArrayEquals(new long[]{5, 100, 0}, codes);
    }

    @Test
    void dictNextCodesEmpty() {
        var data = new Superblock.SuperblockData();
        data.dicts = new Superblock.DictDescriptor[0];
        long[] codes = PersistenceManager.dictNextCodesFromData(data);
        assertEquals(0, codes.length);
    }
}
