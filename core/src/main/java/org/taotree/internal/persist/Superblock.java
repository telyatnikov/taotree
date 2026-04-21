package org.taotree.internal.persist;

import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;

/**
 * Data-model container for checkpoint metadata: slab class registry, bump
 * allocator state, tree descriptors, dictionary descriptors, and the optional
 * schema-binding fingerprint.
 *
 * <p>Populated by {@link org.taotree.TaoTreeSnapshot} and consumed by
 * {@link CheckpointIO}; no wire-format helpers live here. An older direct
 * page-0 serializer was removed together with the v1 on-disk format.
 */
public final class Superblock {

    private Superblock() {}

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

        /**
         * Optional KeyLayout fingerprint (see {@link SchemaBinding}).
         * Null when the tree was created with a raw-key API (no layout).
         */
        public byte[] schemaBinding;
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
