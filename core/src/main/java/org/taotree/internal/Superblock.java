package org.taotree.internal;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Reads and writes the store header (page 0 of the {@link ChunkStore}).
 *
 * <p>The superblock contains all metadata needed to reconstruct the allocator and tree
 * state on reopen: slab class registry, bump allocator state, tree descriptors, and
 * dictionary descriptors.
 *
 * <p>Layout (all little-endian, packed):
 * <pre>
 * Offset  Size  Field
 * 0       8     magic ("TAOTREE\0")
 * 8       4     version
 * 12      4     slabSize
 * 16      4     bumpPageSize
 * 20      8     chunkSize
 * 28      4     totalPages (ChunkStore)
 * 32      4     nextPage (ChunkStore)
 * 36      4     slabClassCount
 * 40      4     treeCount
 * 44      4     dictCount
 * 48      4     bumpPageCount
 * 52      4     bumpCurrentPage
 * 56      4     bumpOffset
 * 60      8     bumpBytesAllocated
 * 68      ...   per-class descriptors (variable)
 * ...     ...   per-tree descriptors (variable)
 * ...     ...   per-dict descriptors (variable)
 * </pre>
 *
 * <p>Per slab class descriptor:
 * <pre>
 * 4B  segmentSize
 * 4B  slabCount
 * 4B  segmentsInUse
 * per slab: 4B dataStartPage, 4B bitmaskStartPage, 1B bitmaskPageCount
 * </pre>
 *
 * <p>Per tree descriptor:
 * <pre>
 * 8B  root pointer
 * 8B  size (entry count)
 * 4B  keyLen
 * 4B  prefixClassId
 * 4B  node4ClassId
 * 4B  node16ClassId
 * 4B  node48ClassId
 * 4B  node256ClassId
 * 4B  leafClassCount
 * per leaf class: 4B leafValueSize, 4B leafClassId
 * </pre>
 *
 * <p>Per dictionary descriptor:
 * <pre>
 * 4B  maxCode
 * 4B  nextCode
 * 4B  treeIndex (index into tree descriptor array)
 * </pre>
 */
public final class Superblock {

    private Superblock() {}

    public static final long MAGIC = 0x0045455254_4F4154L; // "TAOTREE\0" in little-endian
    public static final int VERSION = 1;

    private static final int OFF_MAGIC           = 0;
    private static final int OFF_VERSION         = 8;
    private static final int OFF_SLAB_SIZE       = 12;
    private static final int OFF_BUMP_PAGE_SIZE  = 16;
    private static final int OFF_CHUNK_SIZE      = 20;
    private static final int OFF_TOTAL_PAGES     = 28;
    private static final int OFF_NEXT_PAGE       = 32;
    private static final int OFF_CLASS_COUNT     = 36;
    private static final int OFF_TREE_COUNT      = 40;
    private static final int OFF_DICT_COUNT      = 44;
    private static final int OFF_BUMP_PAGE_COUNT = 48;
    private static final int OFF_BUMP_CUR_PAGE   = 52;
    private static final int OFF_BUMP_OFFSET     = 56;
    private static final int OFF_BUMP_BYTES      = 60;
    private static final int HEADER_SIZE         = 68; // fixed part

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Write the superblock to page 0.
     *
     * @return the number of bytes written
     */
    public static int write(MemorySegment page, SuperblockData data) {
        // Pre-flight: compute total size to catch overflow before writing
        int estimatedSize = HEADER_SIZE;
        for (var cls : data.classes) {
            estimatedSize += 12 + cls.slabCount * 9; // 4+4+4 + slabCount*(4+4+1)
        }
        for (var tree : data.trees) {
            estimatedSize += 36 + tree.leafValueSizes.length * 8; // fixed + per-leaf
        }
        estimatedSize += data.dicts.length * 12;
        estimatedSize += 4 + data.bumpPageLocations.length * 8; // count + per-page (loc+size)
        if (estimatedSize > page.byteSize()) {
            throw new IllegalStateException(
                "Superblock metadata (" + estimatedSize + " bytes) exceeds page size ("
                + page.byteSize() + " bytes). Too many slab classes or slabs for single-page superblock.");
        }

        int pos = 0;

        // Fixed header
        page.set(ValueLayout.JAVA_LONG_UNALIGNED, OFF_MAGIC, MAGIC);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_VERSION, VERSION);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_SLAB_SIZE, data.slabSize);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_BUMP_PAGE_SIZE, data.bumpPageSize);
        page.set(ValueLayout.JAVA_LONG_UNALIGNED, OFF_CHUNK_SIZE, data.chunkSize);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_TOTAL_PAGES, data.totalPages);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_NEXT_PAGE, data.nextPage);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_CLASS_COUNT, data.classes.length);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_TREE_COUNT, data.trees.length);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_DICT_COUNT, data.dicts.length);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_BUMP_PAGE_COUNT, data.bumpPageCount);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_BUMP_CUR_PAGE, data.bumpCurrentPage);
        page.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_BUMP_OFFSET, data.bumpOffset);
        page.set(ValueLayout.JAVA_LONG_UNALIGNED, OFF_BUMP_BYTES, data.bumpBytesAllocated);
        pos = HEADER_SIZE;

        // Slab class descriptors
        for (var cls : data.classes) {
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, cls.segmentSize); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, cls.slabCount); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, cls.segmentsInUse); pos += 4;
            for (int s = 0; s < cls.slabCount; s++) {
                page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, cls.dataStartPages[s]); pos += 4;
                page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, cls.bitmaskStartPages[s]); pos += 4;
                page.set(ValueLayout.JAVA_BYTE, pos, (byte) cls.bitmaskPageCounts[s]); pos += 1;
            }
        }

        // Tree descriptors
        for (var tree : data.trees) {
            page.set(ValueLayout.JAVA_LONG_UNALIGNED, pos, tree.root); pos += 8;
            page.set(ValueLayout.JAVA_LONG_UNALIGNED, pos, tree.size); pos += 8;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.keyLen); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.prefixClassId); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.node4ClassId); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.node16ClassId); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.node48ClassId); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.node256ClassId); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.leafValueSizes.length); pos += 4;
            for (int i = 0; i < tree.leafValueSizes.length; i++) {
                page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.leafValueSizes[i]); pos += 4;
                page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, tree.leafClassIds[i]); pos += 4;
            }
        }

        // Dictionary descriptors
        for (var dict : data.dicts) {
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, dict.maxCode); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, dict.nextCode); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, dict.treeIndex); pos += 4;
        }

        // Bump page locations and sizes
        page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, data.bumpPageLocations.length); pos += 4;
        for (int i = 0; i < data.bumpPageLocations.length; i++) {
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, data.bumpPageLocations[i]); pos += 4;
            page.set(ValueLayout.JAVA_INT_UNALIGNED, pos, data.bumpPageSizes[i]); pos += 4;
        }

        // Post-write guard: verify we didn't exceed the page
        if (pos > page.byteSize()) {
            throw new IllegalStateException(
                "Superblock overflow: wrote " + pos + " bytes into " + page.byteSize()
                + " byte page. Multi-page superblock not yet implemented.");
        }

        return pos;
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /**
     * Read the superblock from page 0.
     *
     * @throws IllegalStateException if the magic or version doesn't match
     */
    public static SuperblockData read(MemorySegment page) {
        long magic = page.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_MAGIC);
        if (magic != MAGIC) {
            throw new IllegalStateException(
                "Invalid superblock magic: expected 0x" + Long.toHexString(MAGIC)
                + " got 0x" + Long.toHexString(magic));
        }
        int version = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_VERSION);
        if (version != VERSION) {
            throw new IllegalStateException("Unsupported superblock version: " + version);
        }

        var data = new SuperblockData();
        data.slabSize = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_SLAB_SIZE);
        data.bumpPageSize = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_BUMP_PAGE_SIZE);
        data.chunkSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_CHUNK_SIZE);
        data.totalPages = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_TOTAL_PAGES);
        data.nextPage = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_NEXT_PAGE);
        int classCount = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_CLASS_COUNT);
        int treeCount = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_TREE_COUNT);
        int dictCount = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_DICT_COUNT);
        data.bumpPageCount = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_BUMP_PAGE_COUNT);
        data.bumpCurrentPage = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_BUMP_CUR_PAGE);
        data.bumpOffset = page.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_BUMP_OFFSET);
        data.bumpBytesAllocated = page.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_BUMP_BYTES);

        int pos = HEADER_SIZE;

        // Slab classes
        data.classes = new SlabClassDescriptor[classCount];
        for (int c = 0; c < classCount; c++) {
            var cls = new SlabClassDescriptor();
            cls.segmentSize = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            cls.slabCount = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            cls.segmentsInUse = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            cls.dataStartPages = new int[cls.slabCount];
            cls.bitmaskStartPages = new int[cls.slabCount];
            cls.bitmaskPageCounts = new int[cls.slabCount];
            for (int s = 0; s < cls.slabCount; s++) {
                cls.dataStartPages[s] = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
                cls.bitmaskStartPages[s] = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
                cls.bitmaskPageCounts[s] = Byte.toUnsignedInt(page.get(ValueLayout.JAVA_BYTE, pos)); pos += 1;
            }
            data.classes[c] = cls;
        }

        // Trees
        data.trees = new TreeDescriptor[treeCount];
        for (int t = 0; t < treeCount; t++) {
            var tree = new TreeDescriptor();
            tree.root = page.get(ValueLayout.JAVA_LONG_UNALIGNED, pos); pos += 8;
            tree.size = page.get(ValueLayout.JAVA_LONG_UNALIGNED, pos); pos += 8;
            tree.keyLen = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            tree.prefixClassId = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            tree.node4ClassId = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            tree.node16ClassId = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            tree.node48ClassId = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            tree.node256ClassId = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            int leafCount = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            tree.leafValueSizes = new int[leafCount];
            tree.leafClassIds = new int[leafCount];
            for (int i = 0; i < leafCount; i++) {
                tree.leafValueSizes[i] = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
                tree.leafClassIds[i] = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            }
            data.trees[t] = tree;
        }

        // Dicts
        data.dicts = new DictDescriptor[dictCount];
        for (int d = 0; d < dictCount; d++) {
            var dict = new DictDescriptor();
            dict.maxCode = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            dict.nextCode = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            dict.treeIndex = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            data.dicts[d] = dict;
        }

        // Bump page locations and sizes
        int bumpLocCount = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
        data.bumpPageLocations = new int[bumpLocCount];
        data.bumpPageSizes = new int[bumpLocCount];
        for (int i = 0; i < bumpLocCount; i++) {
            data.bumpPageLocations[i] = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
            data.bumpPageSizes[i] = page.get(ValueLayout.JAVA_INT_UNALIGNED, pos); pos += 4;
        }

        return data;
    }

    // -----------------------------------------------------------------------
    // Data classes
    // -----------------------------------------------------------------------

    /** All superblock data, used for read/write. */
    public static final class SuperblockData {
        // ChunkStore state
        public int slabSize;
        public int bumpPageSize;
        public long chunkSize;
        public int totalPages;
        public int nextPage;

        // BumpAllocator state
        public int bumpPageCount;
        public int bumpCurrentPage;
        public int bumpOffset;
        public long bumpBytesAllocated;
        public int[] bumpPageLocations = new int[0]; // startPage for each bump page
        public int[] bumpPageSizes = new int[0];     // size in ChunkStore pages for each bump page

        // SlabAllocator state
        public SlabClassDescriptor[] classes = new SlabClassDescriptor[0];

        // Tree and dict descriptors
        public TreeDescriptor[] trees = new TreeDescriptor[0];
        public DictDescriptor[] dicts = new DictDescriptor[0];
    }

    /** Per slab class descriptor. */
    public static final class SlabClassDescriptor {
        public int segmentSize;
        public int slabCount;
        public int segmentsInUse;
        public int[] dataStartPages;       // page where slab data starts (per slab)
        public int[] bitmaskStartPages;    // page where bitmask starts (per slab)
        public int[] bitmaskPageCounts;    // number of pages for bitmask (per slab)
    }

    /** Per tree descriptor. */
    public static final class TreeDescriptor {
        public long root;
        public long size;
        public int keyLen;
        public int prefixClassId;
        public int node4ClassId;
        public int node16ClassId;
        public int node48ClassId;
        public int node256ClassId;
        public int[] leafValueSizes = new int[0];
        public int[] leafClassIds = new int[0];
    }

    /** Per dictionary descriptor. */
    public static final class DictDescriptor {
        public int maxCode;
        public int nextCode;
        public int treeIndex;   // index into TreeDescriptor array
    }
}
