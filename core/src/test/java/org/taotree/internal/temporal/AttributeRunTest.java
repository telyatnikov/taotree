package org.taotree.internal.temporal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AttributeRunTest {

    // ── Key encoding ─────────────────────────────────────────────────────

    @Test
    void keyConstants() {
        assertEquals(12, AttributeRun.KEY_LEN);
        assertEquals(24, AttributeRun.VALUE_SIZE);
    }

    @Test
    void encodeAndDecodeKey() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment key = arena.allocate(AttributeRun.KEY_LEN, 4);

            AttributeRun.encodeKey(key, 42, 1_700_000_000_000L);

            assertEquals(42, AttributeRun.keyAttrId(key));
            assertEquals(1_700_000_000_000L, AttributeRun.keyFirstSeen(key));
        }
    }

    @Test
    void keyIsBigEndianForArtOrdering() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment key1 = arena.allocate(AttributeRun.KEY_LEN, 4);
            MemorySegment key2 = arena.allocate(AttributeRun.KEY_LEN, 4);

            // Same attr_id, different first_seen
            AttributeRun.encodeKey(key1, 1, 100L);
            AttributeRun.encodeKey(key2, 1, 200L);

            // Big-endian encoding means key1 < key2 in byte-lexicographic order
            assertTrue(compareBytes(key1, key2) < 0);

            // Different attr_id — attr_id=1 < attr_id=2 in big-endian
            AttributeRun.encodeKey(key1, 1, 100L);
            AttributeRun.encodeKey(key2, 2, 50L);
            assertTrue(compareBytes(key1, key2) < 0);
        }
    }

    @Test
    void encodeAttrPrefix() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment prefix = arena.allocate(4, 4);
            MemorySegment key = arena.allocate(AttributeRun.KEY_LEN, 4);

            AttributeRun.encodeAttrPrefix(prefix, 7);
            AttributeRun.encodeKey(key, 7, 12345L);

            // Prefix bytes should match first 4 bytes of key
            for (int i = 0; i < 4; i++) {
                assertEquals(prefix.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i),
                        key.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i),
                        "byte " + i + " mismatch");
            }
        }
    }

    // ── Leaf value read/write ────────────────────────────────────────────

    @Test
    void writeAndReadAllValueFields() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment value = arena.allocate(AttributeRun.VALUE_SIZE, 8);

            AttributeRun.writeValue(value,
                    1_700_000_000_000L,  // last_seen
                    Long.MAX_VALUE,       // valid_to
                    0xBEEF_CAFEL);        // value_ref

            assertEquals(1_700_000_000_000L, AttributeRun.lastSeen(value));
            assertEquals(Long.MAX_VALUE, AttributeRun.validTo(value));
            assertEquals(0xBEEF_CAFEL, AttributeRun.valueRef(value));
        }
    }

    @Test
    void individualSetters() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment value = arena.allocate(AttributeRun.VALUE_SIZE, 8);
            value.fill((byte) 0);

            AttributeRun.setLastSeen(value, 100L);
            assertEquals(100L, AttributeRun.lastSeen(value));

            AttributeRun.setValidTo(value, 200L);
            assertEquals(200L, AttributeRun.validTo(value));

            AttributeRun.setValueRef(value, 300L);
            assertEquals(300L, AttributeRun.valueRef(value));
        }
    }

    @Test
    void lastSeenUpdateDoesNotAffectOtherFields() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment value = arena.allocate(AttributeRun.VALUE_SIZE, 8);
            AttributeRun.writeValue(value, 100L, Long.MAX_VALUE, 0xABCDL);

            // Update only last_seen
            AttributeRun.setLastSeen(value, 200L);

            assertEquals(200L, AttributeRun.lastSeen(value));
            assertEquals(Long.MAX_VALUE, AttributeRun.validTo(value));
            assertEquals(0xABCDL, AttributeRun.valueRef(value));
        }
    }

    // ── Helper ───────────────────────────────────────────────────────────

    private static int compareBytes(MemorySegment a, MemorySegment b) {
        int len = (int) Math.min(a.byteSize(), b.byteSize());
        for (int i = 0; i < len; i++) {
            int cmp = Byte.compareUnsigned(
                    a.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i),
                    b.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i));
            if (cmp != 0) return cmp;
        }
        return Long.compare(a.byteSize(), b.byteSize());
    }
}
