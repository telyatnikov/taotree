package org.taotree.internal.cow;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.*;

class MutationLogTest {

    @Test
    void emptyLogHasSizeZero() {
        var log = new MutationLog();
        assertEquals(0, log.size());
    }

    @Test
    void recordStoresEntry() {
        var log = new MutationLog();
        byte[] key = {1, 2, 3};
        byte[] val = {10, 20};
        log.record(MemorySegment.ofArray(key), 3, 0, 100L,
                MemorySegment.ofArray(val), 2, 50L);

        assertEquals(1, log.size());
        assertArrayEquals(key, log.key(0));
        assertEquals(3, log.keyLen(0));
        assertEquals(0, log.leafClass(0));
        assertEquals(100L, log.leafPtr(0));
        assertArrayEquals(val, log.snapshotValue(0));
        assertEquals(50L, log.originalLeafPtr(0));
    }

    @Test
    void recordDefensivelyCopiesKey() {
        var log = new MutationLog();
        byte[] key = {1, 2, 3};
        log.record(MemorySegment.ofArray(key), 3, 0, 1L,
                MemorySegment.ofArray(new byte[1]), 1, 1L);
        key[0] = 99; // mutate original
        assertEquals(1, log.key(0)[0]); // log should have original
    }

    @Test
    void recordDefensivelyCopiesSnapshot() {
        var log = new MutationLog();
        byte[] val = {10, 20};
        log.record(MemorySegment.ofArray(new byte[1]), 1, 0, 1L,
                MemorySegment.ofArray(val), 2, 1L);
        val[0] = 99;
        assertEquals(10, log.snapshotValue(0)[0]);
    }

    @Test
    void totalValueBytesAccumulates() {
        var log = new MutationLog();
        log.record(MemorySegment.ofArray(new byte[1]), 1, 0, 1L,
                MemorySegment.ofArray(new byte[10]), 10, 1L);
        log.record(MemorySegment.ofArray(new byte[1]), 1, 0, 2L,
                MemorySegment.ofArray(new byte[20]), 20, 2L);
        assertEquals(30L, log.totalValueBytes());
    }

    @Test
    void allHaveOriginalsWhenAllNonZero() {
        var log = new MutationLog();
        log.record(MemorySegment.ofArray(new byte[1]), 1, 0, 1L,
                MemorySegment.ofArray(new byte[1]), 1, 10L);
        log.record(MemorySegment.ofArray(new byte[1]), 1, 0, 2L,
                MemorySegment.ofArray(new byte[1]), 1, 20L);
        assertTrue(log.allHaveOriginals());
    }

    @Test
    void allHaveOriginalsFalseWhenAnyZero() {
        var log = new MutationLog();
        log.record(MemorySegment.ofArray(new byte[1]), 1, 0, 1L,
                MemorySegment.ofArray(new byte[1]), 1, 10L);
        log.record(MemorySegment.ofArray(new byte[1]), 1, 0, 2L,
                MemorySegment.ofArray(new byte[1]), 1, 0L); // zero!
        assertFalse(log.allHaveOriginals());
    }

    @Test
    void allHaveOriginalsFalseWhenEmpty() {
        var log = new MutationLog();
        assertFalse(log.allHaveOriginals());
    }

    @Test
    void growHandlesMoreThanInitialCapacity() {
        var log = new MutationLog();
        for (int i = 0; i < 200; i++) {
            byte[] key = {(byte) (i & 0xFF)};
            log.record(MemorySegment.ofArray(key), 1, i % 3, i + 1L,
                    MemorySegment.ofArray(new byte[4]), 4, i + 100L);
        }
        assertEquals(200, log.size());
        // Verify first and last entries survived grow
        assertEquals(1L, log.leafPtr(0));
        assertEquals(200L, log.leafPtr(199));
        assertEquals(100L, log.originalLeafPtr(0));
        assertEquals(299L, log.originalLeafPtr(199));
        assertEquals(0, log.leafClass(0));
        assertEquals(1, log.leafClass(1));
        assertEquals(2, log.leafClass(2));
    }

    @Test
    void leafClassAccessor() {
        var log = new MutationLog();
        log.record(MemorySegment.ofArray(new byte[1]), 1, 7, 1L,
                MemorySegment.ofArray(new byte[1]), 1, 1L);
        assertEquals(7, log.leafClass(0));
    }

    @Test
    void multipleEntriesAccessible() {
        var log = new MutationLog();
        for (int i = 0; i < 5; i++) {
            byte[] key = {(byte) i};
            log.record(MemorySegment.ofArray(key), 1, 0, i * 10L,
                    MemorySegment.ofArray(new byte[2]), 2, 0L);
        }
        assertEquals(5, log.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(i * 10L, log.leafPtr(i));
            assertEquals((byte) i, log.key(i)[0]);
        }
        assertEquals(10L, log.totalValueBytes());
    }
}
