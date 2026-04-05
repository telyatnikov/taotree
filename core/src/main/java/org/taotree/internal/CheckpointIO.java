package org.taotree.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Bridges TaoTree's internal metadata ({@link Superblock.SuperblockData}) and
 * the v2 checkpoint format ({@link CheckpointV2}).
 *
 * <p>All section payloads are packed into the checkpoint's inline data area.
 * No extent-backed sections are used — typical metadata easily fits within
 * the 8 KB checkpoint slot.
 *
 * <p>Section payloads use the same little-endian binary layout as the v1
 * superblock, but each is stored in its own typed section rather than one
 * monolithic blob.
 */
public final class CheckpointIO {

    private CheckpointIO() {}

    // -----------------------------------------------------------------------
    // SuperblockData → CheckpointV2.CheckpointData
    // -----------------------------------------------------------------------

    /**
     * Convert a {@link Superblock.SuperblockData} into a v2 checkpoint
     * ready for {@link CheckpointV2#write}.
     *
     * @param sb         the superblock data to convert
     * @param generation the checkpoint generation number
     * @param slotId     the target slot (0 = A, 1 = B)
     * @return a fully populated {@link CheckpointV2.CheckpointData}
     */
    public static CheckpointV2.CheckpointData toCheckpoint(Superblock.SuperblockData sb,
                                                            long generation, long slotId) {
        // Serialize each section payload into byte arrays
        byte[] coreState = serializeCoreState(sb);
        byte[] slabTable = serializeSlabClassTable(sb);
        byte[] bumpTable = serializeBumpPageTable(sb);
        byte[] treeTable = serializeTreeTable(sb);
        byte[] dictTable = serializeDictTable(sb);

        // Compute total inline size and offsets
        int sectionCount = 5;
        int inlineOffset = 0;

        CheckpointV2.SectionRef[] sections = new CheckpointV2.SectionRef[sectionCount];
        sections[0] = inlineSection(CheckpointV2.SECTION_CORE_STATE, coreState, inlineOffset);
        inlineOffset += coreState.length;
        sections[1] = inlineSection(CheckpointV2.SECTION_SLAB_CLASS_TABLE, slabTable, inlineOffset);
        inlineOffset += slabTable.length;
        sections[2] = inlineSection(CheckpointV2.SECTION_BUMP_PAGE_TABLE, bumpTable, inlineOffset);
        inlineOffset += bumpTable.length;
        sections[3] = inlineSection(CheckpointV2.SECTION_TREE_TABLE, treeTable, inlineOffset);
        inlineOffset += treeTable.length;
        sections[4] = inlineSection(CheckpointV2.SECTION_DICT_TABLE, dictTable, inlineOffset);
        inlineOffset += dictTable.length;

        // Concatenate inline data
        byte[] inlineData = new byte[inlineOffset];
        int pos = 0;
        System.arraycopy(coreState, 0, inlineData, pos, coreState.length); pos += coreState.length;
        System.arraycopy(slabTable, 0, inlineData, pos, slabTable.length); pos += slabTable.length;
        System.arraycopy(bumpTable, 0, inlineData, pos, bumpTable.length); pos += bumpTable.length;
        System.arraycopy(treeTable, 0, inlineData, pos, treeTable.length); pos += treeTable.length;
        System.arraycopy(dictTable, 0, inlineData, pos, dictTable.length);

        var data = new CheckpointV2.CheckpointData();
        data.generation = generation;
        data.slotId = slotId;
        data.incompatibleFeatures = 0;
        data.compatibleFeatures = 0;
        data.pageSize = ChunkStore.PAGE_SIZE;
        data.chunkSize = sb.chunkSize;
        data.totalPages = sb.totalPages;
        data.nextPage = sb.nextPage;
        data.sections = sections;
        data.inlineData = inlineData;
        return data;
    }

    // -----------------------------------------------------------------------
    // CheckpointV2.CheckpointData → SuperblockData
    // -----------------------------------------------------------------------

    /**
     * Reconstruct a {@link Superblock.SuperblockData} from a v2 checkpoint.
     */
    public static Superblock.SuperblockData fromCheckpoint(CheckpointV2.CheckpointData cp) {
        var sb = new Superblock.SuperblockData();
        sb.chunkSize = cp.chunkSize;
        sb.totalPages = cp.totalPages;
        sb.nextPage = cp.nextPage;

        byte[] inline = cp.inlineData;

        for (var ref : cp.sections) {
            if (ref.encoding() != CheckpointV2.ENCODING_INLINE) continue;
            byte[] payload = new byte[ref.inlineLength()];
            System.arraycopy(inline, ref.inlineOffset(), payload, 0, ref.inlineLength());

            switch (ref.sectionType()) {
                case CheckpointV2.SECTION_CORE_STATE -> deserializeCoreState(payload, sb);
                case CheckpointV2.SECTION_SLAB_CLASS_TABLE -> deserializeSlabClassTable(payload, sb);
                case CheckpointV2.SECTION_BUMP_PAGE_TABLE -> deserializeBumpPageTable(payload, sb);
                case CheckpointV2.SECTION_TREE_TABLE -> deserializeTreeTable(payload, sb);
                case CheckpointV2.SECTION_DICT_TABLE -> deserializeDictTable(payload, sb);
            }
        }
        return sb;
    }

    // -----------------------------------------------------------------------
    // Section serialization helpers
    // -----------------------------------------------------------------------

    private static CheckpointV2.SectionRef inlineSection(int sectionType, byte[] data,
                                                          int inlineOffset) {
        return new CheckpointV2.SectionRef(
            sectionType,
            CheckpointV2.ENCODING_INLINE,
            (short) 0,  // flags
            0,           // itemCount (not used for these sections)
            data.length, // payloadBytes
            inlineOffset,
            data.length,
            0, 0,        // extent fields unused
            0            // per-section CRC not used for inline
        );
    }

    // ---- CORE_STATE ----

    private static byte[] serializeCoreState(Superblock.SuperblockData sb) {
        var buf = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(sb.slabSize);
        buf.putInt(sb.bumpPageSize);
        buf.putInt(sb.bumpPageCount);
        buf.putInt(sb.bumpCurrentPage);
        buf.putInt(sb.bumpOffset);
        buf.putInt(0); // padding for alignment
        buf.putLong(sb.bumpBytesAllocated);
        return buf.array();
    }

    private static void deserializeCoreState(byte[] data, Superblock.SuperblockData sb) {
        var buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        sb.slabSize = buf.getInt();
        sb.bumpPageSize = buf.getInt();
        sb.bumpPageCount = buf.getInt();
        sb.bumpCurrentPage = buf.getInt();
        sb.bumpOffset = buf.getInt();
        buf.getInt(); // padding
        sb.bumpBytesAllocated = buf.getLong();
    }

    // ---- SLAB_CLASS_TABLE ----

    private static byte[] serializeSlabClassTable(Superblock.SuperblockData sb) {
        // Pre-compute size
        int size = 4; // classCount
        for (var cls : sb.classes) {
            size += 12 + cls.slabCount * 9; // segmentSize + slabCount + segmentsInUse + per-slab
        }
        var buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(sb.classes.length);
        for (var cls : sb.classes) {
            buf.putInt(cls.segmentSize);
            buf.putInt(cls.slabCount);
            buf.putInt(cls.segmentsInUse);
            for (int s = 0; s < cls.slabCount; s++) {
                buf.putInt(cls.dataStartPages[s]);
                buf.putInt(cls.bitmaskStartPages[s]);
                buf.put((byte) cls.bitmaskPageCounts[s]);
            }
        }
        return buf.array();
    }

    private static void deserializeSlabClassTable(byte[] data, Superblock.SuperblockData sb) {
        var buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int classCount = buf.getInt();
        sb.classes = new Superblock.SlabClassDescriptor[classCount];
        for (int c = 0; c < classCount; c++) {
            var cls = new Superblock.SlabClassDescriptor();
            cls.segmentSize = buf.getInt();
            cls.slabCount = buf.getInt();
            cls.segmentsInUse = buf.getInt();
            cls.dataStartPages = new int[cls.slabCount];
            cls.bitmaskStartPages = new int[cls.slabCount];
            cls.bitmaskPageCounts = new int[cls.slabCount];
            for (int s = 0; s < cls.slabCount; s++) {
                cls.dataStartPages[s] = buf.getInt();
                cls.bitmaskStartPages[s] = buf.getInt();
                cls.bitmaskPageCounts[s] = Byte.toUnsignedInt(buf.get());
            }
            sb.classes[c] = cls;
        }
    }

    // ---- BUMP_PAGE_TABLE ----

    private static byte[] serializeBumpPageTable(Superblock.SuperblockData sb) {
        int count = sb.bumpPageLocations.length;
        var buf = ByteBuffer.allocate(4 + count * 8).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(count);
        for (int i = 0; i < count; i++) {
            buf.putInt(sb.bumpPageLocations[i]);
            buf.putInt(sb.bumpPageSizes[i]);
        }
        return buf.array();
    }

    private static void deserializeBumpPageTable(byte[] data, Superblock.SuperblockData sb) {
        var buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int count = buf.getInt();
        sb.bumpPageLocations = new int[count];
        sb.bumpPageSizes = new int[count];
        for (int i = 0; i < count; i++) {
            sb.bumpPageLocations[i] = buf.getInt();
            sb.bumpPageSizes[i] = buf.getInt();
        }
    }

    // ---- TREE_TABLE ----

    private static byte[] serializeTreeTable(Superblock.SuperblockData sb) {
        int size = 4; // treeCount
        for (var tree : sb.trees) {
            // root(8) + size(8) + keyLen(4) + 5 classIds(20) + leafCount(4) = 44 fixed
            size += 44 + tree.leafValueSizes.length * 8;
        }
        var buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(sb.trees.length);
        for (var tree : sb.trees) {
            buf.putLong(tree.root);
            buf.putLong(tree.size);
            buf.putInt(tree.keyLen);
            buf.putInt(tree.prefixClassId);
            buf.putInt(tree.node4ClassId);
            buf.putInt(tree.node16ClassId);
            buf.putInt(tree.node48ClassId);
            buf.putInt(tree.node256ClassId);
            buf.putInt(tree.leafValueSizes.length);
            for (int i = 0; i < tree.leafValueSizes.length; i++) {
                buf.putInt(tree.leafValueSizes[i]);
                buf.putInt(tree.leafClassIds[i]);
            }
        }
        return buf.array();
    }

    private static void deserializeTreeTable(byte[] data, Superblock.SuperblockData sb) {
        var buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int treeCount = buf.getInt();
        sb.trees = new Superblock.TreeDescriptor[treeCount];
        for (int t = 0; t < treeCount; t++) {
            var tree = new Superblock.TreeDescriptor();
            tree.root = buf.getLong();
            tree.size = buf.getLong();
            tree.keyLen = buf.getInt();
            tree.prefixClassId = buf.getInt();
            tree.node4ClassId = buf.getInt();
            tree.node16ClassId = buf.getInt();
            tree.node48ClassId = buf.getInt();
            tree.node256ClassId = buf.getInt();
            int leafCount = buf.getInt();
            tree.leafValueSizes = new int[leafCount];
            tree.leafClassIds = new int[leafCount];
            for (int i = 0; i < leafCount; i++) {
                tree.leafValueSizes[i] = buf.getInt();
                tree.leafClassIds[i] = buf.getInt();
            }
            sb.trees[t] = tree;
        }
    }

    // ---- DICT_TABLE ----

    private static byte[] serializeDictTable(Superblock.SuperblockData sb) {
        var buf = ByteBuffer.allocate(4 + sb.dicts.length * 12).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(sb.dicts.length);
        for (var dict : sb.dicts) {
            buf.putInt(dict.maxCode);
            buf.putInt(dict.nextCode);
            buf.putInt(dict.treeIndex);
        }
        return buf.array();
    }

    private static void deserializeDictTable(byte[] data, Superblock.SuperblockData sb) {
        var buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int dictCount = buf.getInt();
        sb.dicts = new Superblock.DictDescriptor[dictCount];
        for (int d = 0; d < dictCount; d++) {
            var dict = new Superblock.DictDescriptor();
            dict.maxCode = buf.getInt();
            dict.nextCode = buf.getInt();
            dict.treeIndex = buf.getInt();
            sb.dicts[d] = dict;
        }
    }
}
