package org.taotree.internal.cow;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TemporalOpLogTest {

    @Test
    void kindConstantsAreZeroAndOne() {
        assertEquals(0, TemporalOpLog.KIND_PUT);
        assertEquals(1, TemporalOpLog.KIND_DELETE_ATTR);
    }

    @Test
    void newLogIsEmpty() {
        var log = new TemporalOpLog();
        assertEquals(0, log.size());
    }

    @Test
    void recordPutStoresAllFields() {
        var log = new TemporalOpLog();
        byte[] k = {1, 2, 3, 4};
        log.recordPut(k, 42, 0x12345678L, 9_999L);

        assertEquals(1, log.size());
        assertArrayEquals(k, log.entityKey(0));
        assertEquals(42, log.attrId(0));
        assertEquals(0x12345678L, log.valueRef(0));
        assertEquals(9_999L, log.timestamp(0));
        assertEquals(TemporalOpLog.KIND_PUT, log.kind(0));
    }

    @Test
    void recordDeleteAttrStoresKindAndZeros() {
        var log = new TemporalOpLog();
        byte[] k = {9, 8, 7};
        log.recordDeleteAttr(k, 17);

        assertEquals(1, log.size());
        assertArrayEquals(k, log.entityKey(0));
        assertEquals(17, log.attrId(0));
        assertEquals(0L, log.valueRef(0));
        assertEquals(0L, log.timestamp(0));
        assertEquals(TemporalOpLog.KIND_DELETE_ATTR, log.kind(0));
    }

    @Test
    void entityKeyIsDefensivelyCopied() {
        var log = new TemporalOpLog();
        byte[] k = {1, 2, 3};
        log.recordPut(k, 1, 100L, 200L);
        k[0] = 99;
        assertArrayEquals(new byte[]{1, 2, 3}, log.entityKey(0));
    }

    @Test
    void growsBeyondInitialCapacity() {
        var log = new TemporalOpLog();
        int n = 100; // > INITIAL_CAPACITY (32)
        for (int i = 0; i < n; i++) {
            byte[] k = {(byte) i, (byte) (i >>> 8)};
            if ((i & 1) == 0) {
                log.recordPut(k, i, (long) i * 10, (long) i * 100);
            } else {
                log.recordDeleteAttr(k, i);
            }
        }
        assertEquals(n, log.size());
        for (int i = 0; i < n; i++) {
            byte[] k = {(byte) i, (byte) (i >>> 8)};
            assertArrayEquals(k, log.entityKey(i));
            assertEquals(i, log.attrId(i));
            if ((i & 1) == 0) {
                assertEquals(TemporalOpLog.KIND_PUT, log.kind(i));
                assertEquals((long) i * 10, log.valueRef(i));
                assertEquals((long) i * 100, log.timestamp(i));
            } else {
                assertEquals(TemporalOpLog.KIND_DELETE_ATTR, log.kind(i));
                assertEquals(0L, log.valueRef(i));
                assertEquals(0L, log.timestamp(i));
            }
        }
    }

    @Test
    void multipleEntriesIndependentKeys() {
        var log = new TemporalOpLog();
        byte[] k1 = {1};
        byte[] k2 = {2};
        log.recordPut(k1, 1, 10L, 100L);
        log.recordPut(k2, 2, 20L, 200L);

        assertEquals(2, log.size());
        assertArrayEquals(k1, log.entityKey(0));
        assertArrayEquals(k2, log.entityKey(1));
        // Distinct arrays — mutating one doesn't touch the other.
        assertNotSame(log.entityKey(0), log.entityKey(1));
    }
}
