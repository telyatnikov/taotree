package org.taotree.internal.persist;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Reads and writes v2 checkpoint slots, replacing the v1 {@link Superblock} for the
 * mirrored-slot checkpoint format.
 *
 * <p>File layout:
 * <ul>
 *   <li>Pages 0-3: Checkpoint slot A (16 KB)</li>
 *   <li>Pages 4-7: Checkpoint slot B (16 KB)</li>
 * </ul>
 *
 * <p>Each checkpoint slot contains:
 * <ol>
 *   <li>A 64-byte {@link RecordHeader}</li>
 *   <li>A checkpoint payload (fixed header + section refs + inline data)</li>
 * </ol>
 *
 * <p>Checkpoint payload fixed header (44 bytes):
 * <pre>
 * Offset  Size  Field
 * 0       8     incompatibleFeatures (u64)
 * 8       8     compatibleFeatures (u64)
 * 16      4     pageSize (4096)
 * 20      8     chunkSize
 * 28      4     totalPages
 * 32      4     nextPage
 * 36      4     sectionCount
 * 40      4     reserved
 * </pre>
 *
 * <p>Followed by {@code sectionCount} {@link SectionRef} entries (44 bytes each),
 * then the inline data area (variable size, up to fill the 16 KB slot).
 */
public final class CheckpointV2 {

    private CheckpointV2() {}

    // -----------------------------------------------------------------------
    // Slot layout
    // -----------------------------------------------------------------------

    /** Page offset of checkpoint slot A. */
    public static final int SLOT_A_PAGE = 0;
    /** Page offset of checkpoint slot B (= SLOT_A_PAGE + SLOT_SIZE_PAGES). */
    public static final int SLOT_B_PAGE = 4;
    /** Number of pages per checkpoint slot (16 KB at 4 KB page size). */
    public static final int SLOT_SIZE_PAGES = 4;

    // -----------------------------------------------------------------------
    // Incompatible feature bits
    // -----------------------------------------------------------------------

    public static final long FEATURE_INCOMPAT_WAL           = 0x0001L;
    public static final long FEATURE_INCOMPAT_VERSION_WORDS = 0x0002L;

    // -----------------------------------------------------------------------
    // Compatible feature bits
    // -----------------------------------------------------------------------

    public static final long FEATURE_COMPAT_SCHEMA = 0x0001L;

    // -----------------------------------------------------------------------
    // Section types
    // -----------------------------------------------------------------------

    public static final int SECTION_CORE_STATE       = 0;
    public static final int SECTION_SLAB_CLASS_TABLE = 1;
    public static final int SECTION_BUMP_PAGE_TABLE  = 2;
    public static final int SECTION_TREE_TABLE       = 3;
    public static final int SECTION_DICT_TABLE       = 4;
    public static final int SECTION_SCHEMA_BINDING   = 5;

    // -----------------------------------------------------------------------
    // Section encoding
    // -----------------------------------------------------------------------

    public static final short ENCODING_INLINE  = 0;
    public static final short ENCODING_EXTENTS = 1;

    // -----------------------------------------------------------------------
    // Payload fixed-header offsets (relative to payload start)
    // -----------------------------------------------------------------------

    private static final int P_OFF_INCOMPAT_FEATURES = 0;
    private static final int P_OFF_COMPAT_FEATURES   = 8;
    private static final int P_OFF_PAGE_SIZE         = 16;
    private static final int P_OFF_CHUNK_SIZE        = 20;
    private static final int P_OFF_TOTAL_PAGES       = 28;
    private static final int P_OFF_NEXT_PAGE         = 32;
    private static final int P_OFF_SECTION_COUNT     = 36;
    private static final int P_OFF_RESERVED          = 40;
    private static final int PAYLOAD_HEADER_SIZE     = 44;

    /** Size of a single {@link SectionRef} on disk, in bytes. */
    public static final int SECTION_REF_SIZE = 44;

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Write a checkpoint to the given slot segment.
     *
     * <p>Writes the payload (fixed header + section refs + inline data), computes
     * the payload CRC-32C, then writes the {@link RecordHeader} with both CRCs.
     *
     * @param slot the memory segment for the slot (must be at least 8 KB)
     * @param data the checkpoint data to write
     */
    public static void write(MemorySegment slot, CheckpointData data) {
        long payloadStart = RecordHeader.HEADER_SIZE;

        // --- Payload fixed header ---
        slot.set(ValueLayout.JAVA_LONG_UNALIGNED, payloadStart + P_OFF_INCOMPAT_FEATURES,
                data.incompatibleFeatures);
        slot.set(ValueLayout.JAVA_LONG_UNALIGNED, payloadStart + P_OFF_COMPAT_FEATURES,
                data.compatibleFeatures);
        slot.set(ValueLayout.JAVA_INT_UNALIGNED, payloadStart + P_OFF_PAGE_SIZE,
                data.pageSize);
        slot.set(ValueLayout.JAVA_LONG_UNALIGNED, payloadStart + P_OFF_CHUNK_SIZE,
                data.chunkSize);
        slot.set(ValueLayout.JAVA_INT_UNALIGNED, payloadStart + P_OFF_TOTAL_PAGES,
                data.totalPages);
        slot.set(ValueLayout.JAVA_INT_UNALIGNED, payloadStart + P_OFF_NEXT_PAGE,
                data.nextPage);
        int sectionCount = data.sections != null ? data.sections.length : 0;
        slot.set(ValueLayout.JAVA_INT_UNALIGNED, payloadStart + P_OFF_SECTION_COUNT,
                sectionCount);
        slot.set(ValueLayout.JAVA_INT_UNALIGNED, payloadStart + P_OFF_RESERVED, 0);

        // --- Section refs ---
        long sectionStart = payloadStart + PAYLOAD_HEADER_SIZE;
        for (int i = 0; i < sectionCount; i++) {
            writeSectionRef(slot, sectionStart + (long) i * SECTION_REF_SIZE, data.sections[i]);
        }

        // --- Inline data ---
        long inlineStart = sectionStart + (long) sectionCount * SECTION_REF_SIZE;
        int inlineLength = data.inlineData != null ? data.inlineData.length : 0;
        long slotCapacity = (long) SLOT_SIZE_PAGES * 4096L; // PAGE_SIZE = 4096
        if (inlineStart + inlineLength > slotCapacity) {
            throw new IllegalStateException(
                    "Checkpoint inline data too large: " + inlineLength
                    + " bytes at offset " + inlineStart
                    + " exceeds slot capacity of " + slotCapacity + " bytes");
        }
        if (inlineLength > 0) {
            MemorySegment.copy(
                    MemorySegment.ofArray(data.inlineData), 0,
                    slot, inlineStart,
                    inlineLength);
        }

        int payloadLength = PAYLOAD_HEADER_SIZE + sectionCount * SECTION_REF_SIZE + inlineLength;

        // --- Compute payload CRC, then write record header ---
        int payloadCrc = RecordHeader.crc32c(slot, payloadStart, payloadLength);
        RecordHeader.write(slot, 0,
                RecordHeader.TYPE_CHECKPOINT, 0,
                payloadLength, data.generation,
                data.slotId, payloadCrc);
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /**
     * Validate and read a checkpoint from the given slot segment.
     *
     * <p>Returns {@code null} if the payloadLength field is out of the valid range
     * (indicating on-disk corruption), consistent with {@link CommitRecord#read}'s
     * defensive contract. Other structural errors still throw.
     *
     * @param slot the memory segment for the slot
     * @return the parsed checkpoint data, or {@code null} if payloadLength is corrupt
     * @throws IllegalStateException if a non-corruption structural error is detected
     */
    public static CheckpointData read(MemorySegment slot) {
        if (!RecordHeader.validate(slot, 0)) {
            throw new IllegalStateException("Invalid record header in checkpoint slot");
        }

        int recordType = RecordHeader.recordType(slot, 0);
        if (recordType != RecordHeader.TYPE_CHECKPOINT) {
            throw new IllegalStateException(
                    "Expected checkpoint record type " + RecordHeader.TYPE_CHECKPOINT
                    + ", got " + recordType);
        }

        int payloadLength = RecordHeader.payloadLength(slot, 0);
        long payloadStart = RecordHeader.HEADER_SIZE;

        // Validate payloadLength is within legal bounds before using it.
        // Return null on out-of-range (corruption) rather than throwing, consistent
        // with CommitRecord.read()'s contract. isValid() already checks this, so the
        // null path is only reachable if read() is called directly on a corrupt slot.
        int maxPayloadLength = (int) ((long) SLOT_SIZE_PAGES * 4096L - RecordHeader.HEADER_SIZE);
        if (payloadLength < PAYLOAD_HEADER_SIZE || payloadLength > maxPayloadLength) {
            return null;
        }

        // Verify payload CRC
        int storedPayloadCrc = RecordHeader.payloadCrc32c(slot, 0);
        int computedPayloadCrc = RecordHeader.crc32c(slot, payloadStart, payloadLength);
        if (storedPayloadCrc != computedPayloadCrc) {
            throw new IllegalStateException("Checkpoint payload CRC mismatch");
        }

        var data = new CheckpointData();
        data.generation = RecordHeader.generation(slot, 0);
        data.slotId = RecordHeader.recordId(slot, 0);

        // --- Payload fixed header ---
        data.incompatibleFeatures = slot.get(ValueLayout.JAVA_LONG_UNALIGNED,
                payloadStart + P_OFF_INCOMPAT_FEATURES);
        data.compatibleFeatures = slot.get(ValueLayout.JAVA_LONG_UNALIGNED,
                payloadStart + P_OFF_COMPAT_FEATURES);
        data.pageSize = slot.get(ValueLayout.JAVA_INT_UNALIGNED,
                payloadStart + P_OFF_PAGE_SIZE);
        data.chunkSize = slot.get(ValueLayout.JAVA_LONG_UNALIGNED,
                payloadStart + P_OFF_CHUNK_SIZE);
        data.totalPages = slot.get(ValueLayout.JAVA_INT_UNALIGNED,
                payloadStart + P_OFF_TOTAL_PAGES);
        data.nextPage = slot.get(ValueLayout.JAVA_INT_UNALIGNED,
                payloadStart + P_OFF_NEXT_PAGE);
        int sectionCount = slot.get(ValueLayout.JAVA_INT_UNALIGNED,
                payloadStart + P_OFF_SECTION_COUNT);

        // --- Section refs ---
        long sectionStart = payloadStart + PAYLOAD_HEADER_SIZE;
        data.sections = new SectionRef[sectionCount];
        for (int i = 0; i < sectionCount; i++) {
            data.sections[i] = readSectionRef(slot, sectionStart + (long) i * SECTION_REF_SIZE);
        }

        // --- Inline data ---
        long inlineStart = sectionStart + (long) sectionCount * SECTION_REF_SIZE;
        int inlineLength = payloadLength - PAYLOAD_HEADER_SIZE - sectionCount * SECTION_REF_SIZE;
        if (inlineLength > 0) {
            data.inlineData = slot.asSlice(inlineStart, inlineLength)
                    .toArray(ValueLayout.JAVA_BYTE);
        } else {
            data.inlineData = new byte[0];
        }

        return data;
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    /**
     * Check if the slot contains a valid checkpoint (magic + header CRC + payload CRC).
     *
     * @param slot the memory segment for the slot
     * @return {@code true} if the slot passes all integrity checks
     */
    public static boolean isValid(MemorySegment slot) {
        if (!RecordHeader.validate(slot, 0)) {
            return false;
        }
        int recordType = RecordHeader.recordType(slot, 0);
        if (recordType != RecordHeader.TYPE_CHECKPOINT) {
            return false;
        }
        int payloadLength = RecordHeader.payloadLength(slot, 0);
        int maxPayloadLength = (int) ((long) SLOT_SIZE_PAGES * 4096L - RecordHeader.HEADER_SIZE);
        if (payloadLength < PAYLOAD_HEADER_SIZE || payloadLength > maxPayloadLength) {
            return false;
        }
        long payloadStart = RecordHeader.HEADER_SIZE;
        int storedPayloadCrc = RecordHeader.payloadCrc32c(slot, 0);
        int computedPayloadCrc = RecordHeader.crc32c(slot, payloadStart, payloadLength);
        return storedPayloadCrc == computedPayloadCrc;
    }

    /**
     * Choose the valid checkpoint with the highest generation from two slots.
     *
     * @param slotA memory segment for slot A
     * @param slotB memory segment for slot B
     * @return the checkpoint with the highest generation, or {@code null} if neither is valid
     */
    public static CheckpointData chooseBest(MemorySegment slotA, MemorySegment slotB) {
        boolean validA = isValid(slotA);
        boolean validB = isValid(slotB);

        if (validA && validB) {
            long genA = RecordHeader.generation(slotA, 0);
            long genB = RecordHeader.generation(slotB, 0);
            // Tie-break: if generations are equal, slot A wins (written last on the previous
            // alternating-slot cycle, so it cannot be stale relative to slot B).
            return Long.compareUnsigned(genA, genB) >= 0 ? read(slotA) : read(slotB);
        } else if (validA) {
            return read(slotA);
        } else if (validB) {
            return read(slotB);
        } else {
            return null;
        }
    }

    /**
     * Return the page number of the inactive slot given the active slot's page.
     *
     * @param activeSlotPage page number of the currently active slot
     * @return page number of the other (inactive) slot
     */
    public static int inactiveSlotPage(int activeSlotPage) {
        return activeSlotPage == SLOT_A_PAGE ? SLOT_B_PAGE : SLOT_A_PAGE;
    }

    // -----------------------------------------------------------------------
    // SectionRef I/O
    // -----------------------------------------------------------------------

    /**
     * Write a single {@link SectionRef} at the given offset.
     *
     * <p>Per-section layout (44 bytes):
     * <pre>
     * Offset  Size  Field
     * 0       4     sectionType
     * 4       2     encoding
     * 6       2     flags
     * 8       4     itemCount
     * 12      8     payloadBytes
     * 20      4     inlineOffset
     * 24      4     inlineLength
     * 28      4     extentStartPage
     * 32      4     extentPageCount
     * 36      4     payloadCrc32c
     * 40      4     reserved (0)
     * </pre>
     */
    private static void writeSectionRef(MemorySegment seg, long offset, SectionRef ref) {
        seg.set(ValueLayout.JAVA_INT_UNALIGNED,   offset,      ref.sectionType());
        seg.set(ValueLayout.JAVA_SHORT_UNALIGNED,  offset + 4,  ref.encoding());
        seg.set(ValueLayout.JAVA_SHORT_UNALIGNED,  offset + 6,  ref.flags());
        seg.set(ValueLayout.JAVA_INT_UNALIGNED,   offset + 8,  ref.itemCount());
        seg.set(ValueLayout.JAVA_LONG_UNALIGNED,  offset + 12, ref.payloadBytes());
        seg.set(ValueLayout.JAVA_INT_UNALIGNED,   offset + 20, ref.inlineOffset());
        seg.set(ValueLayout.JAVA_INT_UNALIGNED,   offset + 24, ref.inlineLength());
        seg.set(ValueLayout.JAVA_INT_UNALIGNED,   offset + 28, ref.extentStartPage());
        seg.set(ValueLayout.JAVA_INT_UNALIGNED,   offset + 32, ref.extentPageCount());
        seg.set(ValueLayout.JAVA_INT_UNALIGNED,   offset + 36, ref.payloadCrc32c());
        seg.set(ValueLayout.JAVA_INT_UNALIGNED,   offset + 40, 0); // reserved
    }

    private static SectionRef readSectionRef(MemorySegment seg, long offset) {
        return new SectionRef(
                seg.get(ValueLayout.JAVA_INT_UNALIGNED,   offset),
                seg.get(ValueLayout.JAVA_SHORT_UNALIGNED,  offset + 4),
                seg.get(ValueLayout.JAVA_SHORT_UNALIGNED,  offset + 6),
                seg.get(ValueLayout.JAVA_INT_UNALIGNED,   offset + 8),
                seg.get(ValueLayout.JAVA_LONG_UNALIGNED,  offset + 12),
                seg.get(ValueLayout.JAVA_INT_UNALIGNED,   offset + 20),
                seg.get(ValueLayout.JAVA_INT_UNALIGNED,   offset + 24),
                seg.get(ValueLayout.JAVA_INT_UNALIGNED,   offset + 28),
                seg.get(ValueLayout.JAVA_INT_UNALIGNED,   offset + 32),
                seg.get(ValueLayout.JAVA_INT_UNALIGNED,   offset + 36)
        );
    }

    // -----------------------------------------------------------------------
    // Data classes
    // -----------------------------------------------------------------------

    /** All checkpoint fields (mutable data holder for read/write). */
    public static final class CheckpointData {
        /** Monotonically increasing generation counter. */
        public long generation;
        /** Slot identifier (0 for slot A, 1 for slot B). */
        public long slotId;
        /** Incompatible feature flags. */
        public long incompatibleFeatures;
        /** Compatible feature flags. */
        public long compatibleFeatures;
        /** Page size in bytes (typically 4096). */
        public int pageSize;
        /** Chunk size in bytes. */
        public long chunkSize;
        /** Total number of pages in the chunk store. */
        public int totalPages;
        /** Next free page number. */
        public int nextPage;
        /** Section references describing each metadata section. */
        public SectionRef[] sections = new SectionRef[0];
        /** Raw inline data area bytes. */
        public byte[] inlineData = new byte[0];
    }

    /**
     * A section reference within the checkpoint payload.
     *
     * @param sectionType    section type (e.g. {@link #SECTION_CORE_STATE})
     * @param encoding       encoding format ({@link #ENCODING_INLINE} or {@link #ENCODING_EXTENTS})
     * @param flags          section flags
     * @param itemCount      number of items in this section
     * @param payloadBytes   total payload size in bytes
     * @param inlineOffset   offset into the inline data area (relative to inline area start)
     * @param inlineLength   length of inline data in bytes
     * @param extentStartPage first page of extent data
     * @param extentPageCount number of pages in extent data
     * @param payloadCrc32c  CRC-32C of the section payload
     */
    public record SectionRef(
            int sectionType,
            short encoding,
            short flags,
            int itemCount,
            long payloadBytes,
            int inlineOffset,
            int inlineLength,
            int extentStartPage,
            int extentPageCount,
            int payloadCrc32c
    ) {}
}
