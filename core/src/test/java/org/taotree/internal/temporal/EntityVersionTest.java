package org.taotree.internal.temporal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;
import org.taotree.internal.champ.ChampMap;

import static org.junit.jupiter.api.Assertions.*;

class EntityVersionTest {

    // ── Key encoding ─────────────────────────────────────────────────────

    @Test
    void keyConstants() {
        assertEquals(8, EntityVersion.KEY_LEN);
        assertEquals(16, EntityVersion.VALUE_SIZE);
    }

    @Test
    void encodeAndDecodeKey() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment key = arena.allocate(EntityVersion.KEY_LEN, 8);

            EntityVersion.encodeKey(key, 1_700_000_000_000L);

            assertEquals(1_700_000_000_000L, EntityVersion.keyFirstSeen(key));
        }
    }

    @Test
    void keyIsBigEndianForArtOrdering() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment key1 = arena.allocate(EntityVersion.KEY_LEN, 8);
            MemorySegment key2 = arena.allocate(EntityVersion.KEY_LEN, 8);

            EntityVersion.encodeKey(key1, 100L);
            EntityVersion.encodeKey(key2, 200L);

            // Big-endian: key1 < key2 in byte-lexicographic order
            assertTrue(compareBytes(key1, key2) < 0);
        }
    }

    // ── Leaf value read/write ────────────────────────────────────────────

    @Test
    void writeAndReadAllValueFields() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment value = arena.allocate(EntityVersion.VALUE_SIZE, 8);

            EntityVersion.writeValue(value, Long.MAX_VALUE, 0xCAFE_BABEL);

            assertEquals(Long.MAX_VALUE, EntityVersion.validTo(value));
            assertEquals(0xCAFE_BABEL, EntityVersion.stateRootRef(value));
        }
    }

    @Test
    void individualSetters() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment value = arena.allocate(EntityVersion.VALUE_SIZE, 8);
            value.fill((byte) 0);

            EntityVersion.setValidTo(value, 500L);
            assertEquals(500L, EntityVersion.validTo(value));
            assertEquals(0L, EntityVersion.stateRootRef(value));

            EntityVersion.setStateRootRef(value, 0xDEADL);
            assertEquals(0xDEADL, EntityVersion.stateRootRef(value));
            assertEquals(500L, EntityVersion.validTo(value));
        }
    }

    @Test
    void emptyStateRoot() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment value = arena.allocate(EntityVersion.VALUE_SIZE, 8);
            EntityVersion.writeValue(value, Long.MAX_VALUE, ChampMap.EMPTY_ROOT);

            assertEquals(ChampMap.EMPTY_ROOT, EntityVersion.stateRootRef(value));
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
