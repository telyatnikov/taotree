package org.taotree.internal;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.zip.CRC32C;

/**
 * Reads and writes the common 64-byte record header used by all v2 on-disk records.
 *
 * <p>Layout (all little-endian via {@code JAVA_*_UNALIGNED}):
 * <pre>
 * Offset  Size  Field
 * 0       8     magic (MAGIC_V2)
 * 8       4     majorVersion (2)
 * 12      4     minorVersion (0)
 * 16      4     recordType (0=checkpoint, 1=metadata_section, 2=wal_block, 3=commit_record)
 * 20      4     flags
 * 24      4     headerLength (always 64)
 * 28      4     payloadLength
 * 32      8     generation
 * 40      8     recordId (e.g. slot 0 or 1 for checkpoints)
 * 48      4     headerCrc32c
 * 52      4     payloadCrc32c
 * 56      8     reserved (0)
 * </pre>
 *
 * <p>The {@code headerCrc32c} is computed over bytes 0-47 (i.e. the first 48 bytes,
 * everything before the CRC fields). The {@code payloadCrc32c} is set by the caller.
 */
public final class RecordHeader {

    private RecordHeader() {}

    /** "TAOTREE2" in little-endian. */
    public static final long MAGIC_V2 = 0x3245455254_4F4154L;

    public static final int MAJOR_VERSION = 2;
    public static final int MINOR_VERSION = 0;

    // Record types
    public static final int TYPE_CHECKPOINT      = 0;
    public static final int TYPE_METADATA_SECTION = 1;
    public static final int TYPE_WAL_BLOCK        = 2;
    public static final int TYPE_COMMIT_RECORD    = 3;

    public static final int HEADER_SIZE = 64;

    // Field offsets
    private static final int OFF_MAGIC          = 0;
    private static final int OFF_MAJOR_VERSION  = 8;
    private static final int OFF_MINOR_VERSION  = 12;
    private static final int OFF_RECORD_TYPE    = 16;
    private static final int OFF_FLAGS          = 20;
    private static final int OFF_HEADER_LENGTH  = 24;
    private static final int OFF_PAYLOAD_LENGTH = 28;
    private static final int OFF_GENERATION     = 32;
    private static final int OFF_RECORD_ID      = 40;
    private static final int OFF_HEADER_CRC     = 48;
    private static final int OFF_PAYLOAD_CRC    = 52;
    private static final int OFF_RESERVED       = 56;

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Write all header fields. The {@code headerCrc32c} is computed over bytes 0-47
     * and written automatically at offset 48.
     *
     * @param seg           the memory segment to write into
     * @param offset        byte offset within {@code seg} where the header starts
     * @param recordType    record type (e.g. {@link #TYPE_CHECKPOINT})
     * @param flags         record flags
     * @param payloadLength length of the payload in bytes
     * @param generation    monotonically increasing generation counter
     * @param recordId      record identifier (e.g. slot index for checkpoints)
     * @param payloadCrc32c CRC-32C of the payload, computed by the caller
     */
    public static void write(MemorySegment seg, long offset,
                             int recordType, int flags,
                             int payloadLength, long generation,
                             long recordId, int payloadCrc32c) {
        seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + OFF_MAGIC, MAGIC_V2);
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_MAJOR_VERSION, MAJOR_VERSION);
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_MINOR_VERSION, MINOR_VERSION);
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_RECORD_TYPE, recordType);
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_FLAGS, flags);
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_HEADER_LENGTH, HEADER_SIZE);
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_PAYLOAD_LENGTH, payloadLength);
        seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + OFF_GENERATION, generation);
        seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + OFF_RECORD_ID, recordId);

        // Zero out header CRC slot before computing, set payload CRC and reserved
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_HEADER_CRC, 0);
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_PAYLOAD_CRC, payloadCrc32c);
        seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset + OFF_RESERVED, 0L);

        // Compute header CRC over bytes 0-47 and write at offset 48
        int headerCrc = crc32c(seg, offset, OFF_HEADER_CRC);
        seg.set(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_HEADER_CRC, headerCrc);
    }

    // -----------------------------------------------------------------------
    // Read / Validate
    // -----------------------------------------------------------------------

    /**
     * Validate the header by checking the magic and recomputing the header CRC.
     *
     * @param seg    the memory segment containing the header
     * @param offset byte offset within {@code seg} where the header starts
     * @return {@code true} if magic matches and CRC is correct
     */
    public static boolean validate(MemorySegment seg, long offset) {
        long magic = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + OFF_MAGIC);
        if (magic != MAGIC_V2) {
            return false;
        }
        int storedCrc = seg.get(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_HEADER_CRC);
        // CRC covers bytes 0-47 (everything before the CRC field at offset 48)
        int computedCrc = crc32c(seg, offset, OFF_HEADER_CRC);
        return storedCrc == computedCrc;
    }

    /** Read the generation field. */
    public static long generation(MemorySegment seg, long offset) {
        return seg.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + OFF_GENERATION);
    }

    /** Read the record type field. */
    public static int recordType(MemorySegment seg, long offset) {
        return seg.get(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_RECORD_TYPE);
    }

    /** Read the payload length field. */
    public static int payloadLength(MemorySegment seg, long offset) {
        return seg.get(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_PAYLOAD_LENGTH);
    }

    /** Read the record ID field. */
    public static long recordId(MemorySegment seg, long offset) {
        return seg.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + OFF_RECORD_ID);
    }

    /** Read the payload CRC-32C field. */
    public static int payloadCrc32c(MemorySegment seg, long offset) {
        return seg.get(ValueLayout.JAVA_INT_UNALIGNED, offset + OFF_PAYLOAD_CRC);
    }

    // -----------------------------------------------------------------------
    // CRC
    // -----------------------------------------------------------------------

    /**
     * Compute CRC-32C over {@code length} bytes starting at {@code offset} in {@code seg}.
     */
    static int crc32c(MemorySegment seg, long offset, int length) {
        var crc = new CRC32C();
        byte[] buf = seg.asSlice(offset, length).toArray(ValueLayout.JAVA_BYTE);
        crc.update(buf);
        return (int) crc.getValue();
    }
}
