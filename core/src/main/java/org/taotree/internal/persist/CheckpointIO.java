package org.taotree.internal.persist;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.taotree.TaoTree;
import org.taotree.internal.alloc.ChunkStore;

/**
 * Bridges TaoTree's internal metadata ({@link Superblock.SuperblockData}) and
 * the checkpoint format ({@link Checkpoint}).
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
    // SuperblockData → Checkpoint.CheckpointData
    // -----------------------------------------------------------------------

    /**
     * Convert a {@link Superblock.SuperblockData} into a checkpoint
     * ready for {@link Checkpoint#write}.
     *
     * @param sb         the superblock data to convert
     * @param generation the checkpoint generation number
     * @param slotId     the target slot (0 = A, 1 = B)
     * @return a fully populated {@link Checkpoint.CheckpointData}
     */
    public static Checkpoint.CheckpointData toCheckpoint(Superblock.SuperblockData sb,
                                                            long generation, long slotId) {
        // Serialize each section payload into byte arrays
        byte[] coreState = serializeCoreState(sb);
        byte[] slabTable = serializeSlabClassTable(sb);
        byte[] bumpTable = serializeBumpPageTable(sb);
        byte[] treeTable = serializeTreeTable(sb);
        byte[] dictTable = serializeDictTable(sb);
        byte[] schemaBinding = (sb.schemaBinding != null) ? sb.schemaBinding : new byte[0];
        boolean hasSchema = schemaBinding.length > 0;

        // Compute total inline size and offsets
        int sectionCount = hasSchema ? 6 : 5;
        int inlineOffset = 0;

        Checkpoint.SectionRef[] sections = new Checkpoint.SectionRef[sectionCount];
        sections[0] = inlineSection(Checkpoint.SECTION_CORE_STATE, coreState, inlineOffset);
        inlineOffset += coreState.length;
        sections[1] = inlineSection(Checkpoint.SECTION_SLAB_CLASS_TABLE, slabTable, inlineOffset);
        inlineOffset += slabTable.length;
        sections[2] = inlineSection(Checkpoint.SECTION_BUMP_PAGE_TABLE, bumpTable, inlineOffset);
        inlineOffset += bumpTable.length;
        sections[3] = inlineSection(Checkpoint.SECTION_TREE_TABLE, treeTable, inlineOffset);
        inlineOffset += treeTable.length;
        sections[4] = inlineSection(Checkpoint.SECTION_DICT_TABLE, dictTable, inlineOffset);
        inlineOffset += dictTable.length;
        if (hasSchema) {
            sections[5] = inlineSection(Checkpoint.SECTION_SCHEMA_BINDING, schemaBinding, inlineOffset);
            inlineOffset += schemaBinding.length;
        }

        // Concatenate inline data
        byte[] inlineData = new byte[inlineOffset];
        int pos = 0;
        System.arraycopy(coreState, 0, inlineData, pos, coreState.length); pos += coreState.length;
        System.arraycopy(slabTable, 0, inlineData, pos, slabTable.length); pos += slabTable.length;
        System.arraycopy(bumpTable, 0, inlineData, pos, bumpTable.length); pos += bumpTable.length;
        System.arraycopy(treeTable, 0, inlineData, pos, treeTable.length); pos += treeTable.length;
        System.arraycopy(dictTable, 0, inlineData, pos, dictTable.length); pos += dictTable.length;
        if (hasSchema) {
            System.arraycopy(schemaBinding, 0, inlineData, pos, schemaBinding.length);
        }

        var data = new Checkpoint.CheckpointData();
        data.generation = generation;
        data.slotId = slotId;
        // v3 format marker — always set on write.
        data.incompatibleFeatures = Checkpoint.FEATURE_INCOMPAT_UNIFIED_TEMPORAL;
        data.compatibleFeatures = hasSchema ? Checkpoint.FEATURE_COMPAT_SCHEMA : 0;
        data.pageSize = ChunkStore.PAGE_SIZE;
        data.chunkSize = sb.chunkSize;
        data.totalPages = sb.totalPages;
        data.nextPage = sb.nextPage;
        data.sections = sections;
        data.inlineData = inlineData;
        return data;
    }

    // -----------------------------------------------------------------------
    // Checkpoint.CheckpointData → SuperblockData
    // -----------------------------------------------------------------------

    /**
     * Reconstruct a {@link Superblock.SuperblockData} from a checkpoint.
     */
    public static Superblock.SuperblockData fromCheckpoint(Checkpoint.CheckpointData cp) {
        // v3 format check: refuse legacy v2 files. The unified-temporal data
        // model is the only one supported, and v2 files lack the runtime
        // structures (per-entity ARTs, attribute dictionary) needed to read
        // them as unified-temporal trees.
        if ((cp.incompatibleFeatures & Checkpoint.FEATURE_INCOMPAT_UNIFIED_TEMPORAL) == 0) {
            throw new java.io.UncheckedIOException(new java.io.IOException(
                    "TaoTree file format v2 is no longer supported. "
                  + "This build only reads v3 files (created with the unified-temporal API). "
                  + "Recreate the store with TaoTree.create(Path, KeyLayout) and re-import the data."));
        }

        var sb = new Superblock.SuperblockData();
        sb.chunkSize = cp.chunkSize;
        sb.totalPages = cp.totalPages;
        sb.nextPage = cp.nextPage;

        byte[] inline = cp.inlineData;

        for (var ref : cp.sections) {
            if (ref.encoding() != Checkpoint.ENCODING_INLINE) continue;
            byte[] payload = new byte[ref.inlineLength()];
            System.arraycopy(inline, ref.inlineOffset(), payload, 0, ref.inlineLength());

            switch (ref.sectionType()) {
                case Checkpoint.SECTION_CORE_STATE -> deserializeCoreState(payload, sb);
                case Checkpoint.SECTION_SLAB_CLASS_TABLE -> deserializeSlabClassTable(payload, sb);
                case Checkpoint.SECTION_BUMP_PAGE_TABLE -> deserializeBumpPageTable(payload, sb);
                case Checkpoint.SECTION_TREE_TABLE -> deserializeTreeTable(payload, sb);
                case Checkpoint.SECTION_DICT_TABLE -> deserializeDictTable(payload, sb);
                case Checkpoint.SECTION_SCHEMA_BINDING -> sb.schemaBinding = payload;
                // Unknown section types are ignored for forward-compat.
            }
        }
        return sb;
    }

    // -----------------------------------------------------------------------
    // Section serialization helpers
    // -----------------------------------------------------------------------

    private static Checkpoint.SectionRef inlineSection(int sectionType, byte[] data,
                                                          int inlineOffset) {
        return new Checkpoint.SectionRef(
            sectionType,
            Checkpoint.ENCODING_INLINE,
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
