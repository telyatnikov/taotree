package org.taotree.internal.persist;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.taotree.TaoTree;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.art.NodePtr;

/**
 * On-disk commit record for log-structured shadow paging in TaoTree v2.
 *
 * <p>Writers append COW nodes to thread-local arenas (contiguous {@link ChunkStore}
 * pages). On WriteScope close, a small commit record is appended at the end of the
 * arena's page range to mark a durable publication point. Recovery scans commit
 * records forward from the last checkpoint to reconstruct the latest valid state.
 *
 * <p>The commit record fits in one 4 KB page. It uses the common {@link RecordHeader}
 * (64 bytes) with {@code recordType = RecordHeader.TYPE_COMMIT_RECORD}, followed by
 * the commit payload.
 *
 * <p>Payload layout (all little-endian via {@code ValueLayout.JAVA_*_UNALIGNED}):
 * <pre>
 * Offset  Size  Field
 * 0       8     prevCommitPage (u32 zero-extended to long, page number of previous
 *               commit record, or 0 if first)
 * 8       8     primaryRoot (u64 NodePtr)
 * 16      8     primarySize (u64)
 * 24      4     dictionaryCount (u32)
 * 28      4     arenaStartPage (u32)
 * 32      4     arenaEndPage (u32, exclusive)
 * 36      4     reserved (0)
 * </pre>
 *
 * <p>Followed by {@code dictionaryCount} entries of:
 * <pre>
 * 8B  dictRoot (NodePtr)
 * 8B  dictNextCode
 * 8B  dictSize
 * </pre>
 */
public final class CommitRecord {

    private CommitRecord() {}

    /** Maximum payload size within a single page. */
    public static final int MAX_PAYLOAD_SIZE = ChunkStore.PAGE_SIZE - RecordHeader.HEADER_SIZE;

    // Fixed payload field offsets (relative to payload start)
    private static final int OFF_PREV_COMMIT_PAGE = 0;
    private static final int OFF_PRIMARY_ROOT     = 8;
    private static final int OFF_PRIMARY_SIZE     = 16;
    private static final int OFF_DICT_COUNT       = 24;
    private static final int OFF_ARENA_START_PAGE = 28;
    private static final int OFF_ARENA_END_PAGE   = 32;
    private static final int OFF_RESERVED         = 36;
    private static final int FIXED_PAYLOAD_SIZE   = 40;

    /** Size of each dictionary entry in bytes (dictRoot + dictNextCode + dictSize). */
    private static final int DICT_ENTRY_SIZE = 24;

    // -----------------------------------------------------------------------
    // Data class
    // -----------------------------------------------------------------------

    /**
     * Mutable data class holding all fields of a commit record.
     */
    public static final class CommitData {
        public long generation;
        public int prevCommitPage;
        public long primaryRoot;
        public long primarySize;
        public int dictionaryCount;
        public long[] dictRoots;
        public long[] dictNextCodes;
        public long[] dictSizes;
        public int arenaStartPage;
        public int arenaEndPage;
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Write a commit record to a page.
     *
     * <p>The payload bytes are written first, then the CRC-32C of the payload is
     * computed, and finally the {@link RecordHeader} is written at offset 0 with the
     * correct payload CRC and generation.
     *
     * @param page a {@link MemorySegment} of at least {@link ChunkStore#PAGE_SIZE} bytes
     * @param data the commit data to write
     */
    public static void write(MemorySegment page, CommitData data) {
        int dictCount = data.dictionaryCount;
        int payloadLen = payloadSize(dictCount);

        long pOff = RecordHeader.HEADER_SIZE; // payload start offset within the page

        // Write fixed payload fields
        page.set(ValueLayout.JAVA_LONG_UNALIGNED, pOff + OFF_PREV_COMMIT_PAGE,
                 Integer.toUnsignedLong(data.prevCommitPage));
        page.set(ValueLayout.JAVA_LONG_UNALIGNED, pOff + OFF_PRIMARY_ROOT, data.primaryRoot);
        page.set(ValueLayout.JAVA_LONG_UNALIGNED, pOff + OFF_PRIMARY_SIZE, data.primarySize);
        page.set(ValueLayout.JAVA_INT_UNALIGNED,  pOff + OFF_DICT_COUNT, dictCount);
        page.set(ValueLayout.JAVA_INT_UNALIGNED,  pOff + OFF_ARENA_START_PAGE, data.arenaStartPage);
        page.set(ValueLayout.JAVA_INT_UNALIGNED,  pOff + OFF_ARENA_END_PAGE, data.arenaEndPage);
        page.set(ValueLayout.JAVA_INT_UNALIGNED,  pOff + OFF_RESERVED, 0);

        // Write dictionary entries
        long dictOff = pOff + FIXED_PAYLOAD_SIZE;
        for (int i = 0; i < dictCount; i++) {
            page.set(ValueLayout.JAVA_LONG_UNALIGNED, dictOff,      data.dictRoots[i]);
            page.set(ValueLayout.JAVA_LONG_UNALIGNED, dictOff + 8,  data.dictNextCodes[i]);
            page.set(ValueLayout.JAVA_LONG_UNALIGNED, dictOff + 16, data.dictSizes[i]);
            dictOff += DICT_ENTRY_SIZE;
        }

        // Compute payload CRC-32C
        int payloadCrc = RecordHeader.crc32c(page, RecordHeader.HEADER_SIZE, payloadLen);

        // Write the header at offset 0
        RecordHeader.write(page, 0,
                RecordHeader.TYPE_COMMIT_RECORD,
                0,              // flags
                payloadLen,
                data.generation,
                0L,             // recordId (unused for commit records)
                payloadCrc);
    }

    // -----------------------------------------------------------------------
    // Read / Validate
    // -----------------------------------------------------------------------

    /**
     * Read and validate a commit record from a page.
     *
     * <p>Validates the {@link RecordHeader} (magic + header CRC), checks that the
     * record type is {@link RecordHeader#TYPE_COMMIT_RECORD}, and verifies the
     * payload CRC-32C before parsing the payload fields.
     *
     * @param page a {@link MemorySegment} of at least {@link ChunkStore#PAGE_SIZE} bytes
     * @return the parsed {@link CommitData}, or {@code null} if the page does not
     *         contain a valid commit record
     */
    public static CommitData read(MemorySegment page) {
        // Validate header (magic + header CRC)
        if (!RecordHeader.validate(page, 0)) {
            return null;
        }

        // Must be a commit record
        if (RecordHeader.recordType(page, 0) != RecordHeader.TYPE_COMMIT_RECORD) {
            return null;
        }

        // Verify payload CRC
        int payloadLen = RecordHeader.payloadLength(page, 0);
        // Validate payloadLength before use to catch corruption early.
        if (payloadLen < FIXED_PAYLOAD_SIZE || payloadLen > MAX_PAYLOAD_SIZE) {
            return null;
        }
        int storedPayloadCrc = RecordHeader.payloadCrc32c(page, 0);
        int computedPayloadCrc = RecordHeader.crc32c(page, RecordHeader.HEADER_SIZE, payloadLen);
        if (storedPayloadCrc != computedPayloadCrc) {
            return null;
        }

        // Parse payload
        long pOff = RecordHeader.HEADER_SIZE;

        CommitData data = new CommitData();
        data.generation = RecordHeader.generation(page, 0);
        data.prevCommitPage = (int) page.get(ValueLayout.JAVA_LONG_UNALIGNED,
                                              pOff + OFF_PREV_COMMIT_PAGE);
        data.primaryRoot = page.get(ValueLayout.JAVA_LONG_UNALIGNED, pOff + OFF_PRIMARY_ROOT);
        data.primarySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, pOff + OFF_PRIMARY_SIZE);
        data.dictionaryCount = page.get(ValueLayout.JAVA_INT_UNALIGNED, pOff + OFF_DICT_COUNT);
        data.arenaStartPage = page.get(ValueLayout.JAVA_INT_UNALIGNED, pOff + OFF_ARENA_START_PAGE);
        data.arenaEndPage = page.get(ValueLayout.JAVA_INT_UNALIGNED, pOff + OFF_ARENA_END_PAGE);

        int dictCount = data.dictionaryCount;
        // Validate dictionaryCount against the declared payloadLength to prevent
        // large array allocations or out-of-bounds reads on corrupted records.
        int expectedPayloadLen = FIXED_PAYLOAD_SIZE + dictCount * DICT_ENTRY_SIZE;
        if (dictCount < 0 || expectedPayloadLen > payloadLen) {
            return null;
        }
        data.dictRoots = new long[dictCount];
        data.dictNextCodes = new long[dictCount];
        data.dictSizes = new long[dictCount];

        long dictOff = pOff + FIXED_PAYLOAD_SIZE;
        for (int i = 0; i < dictCount; i++) {
            data.dictRoots[i] = page.get(ValueLayout.JAVA_LONG_UNALIGNED, dictOff);
            data.dictNextCodes[i] = page.get(ValueLayout.JAVA_LONG_UNALIGNED, dictOff + 8);
            data.dictSizes[i] = page.get(ValueLayout.JAVA_LONG_UNALIGNED, dictOff + 16);
            dictOff += DICT_ENTRY_SIZE;
        }

        return data;
    }

    /**
     * Check if a page contains a valid commit record.
     *
     * @param page a {@link MemorySegment} of at least {@link ChunkStore#PAGE_SIZE} bytes
     * @return {@code true} if the page contains a valid commit record
     */
    public static boolean isValid(MemorySegment page) {
        return read(page) != null;
    }

    // -----------------------------------------------------------------------
    // Sizing
    // -----------------------------------------------------------------------

    /**
     * Compute the required payload size for the given dictionary count.
     *
     * @param dictCount number of dictionary entries
     * @return payload size in bytes
     */
    public static int payloadSize(int dictCount) {
        return FIXED_PAYLOAD_SIZE + dictCount * DICT_ENTRY_SIZE;
    }
}
