package org.taotree.internal.persist;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.persist.Checkpoint;
import org.taotree.internal.persist.RecordHeader;

class CheckpointTest {

    /** Two checkpoint slot pages = 8 KB. */
    private static final long SLOT_SIZE = 2L * ChunkStore.PAGE_SIZE;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Checkpoint.CheckpointData sampleCheckpoint(long generation, long slotId) {
        var data = new Checkpoint.CheckpointData();
        data.generation = generation;
        data.slotId = slotId;
        data.incompatibleFeatures = Checkpoint.FEATURE_INCOMPAT_WAL;
        data.compatibleFeatures = Checkpoint.FEATURE_COMPAT_SCHEMA;
        data.pageSize = ChunkStore.PAGE_SIZE;
        data.chunkSize = ChunkStore.DEFAULT_CHUNK_SIZE;
        data.totalPages = 16384;
        data.nextPage = 500;
        data.sections = new Checkpoint.SectionRef[]{
                new Checkpoint.SectionRef(
                        Checkpoint.SECTION_CORE_STATE,
                        Checkpoint.ENCODING_INLINE,
                        (short) 0,
                        1,
                        128L,
                        0,
                        128,
                        0,
                        0,
                        0xAABBCCDD
                )
        };
        data.inlineData = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        return data;
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    void writeAndReadRoundTrip() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment slot = arena.allocate(SLOT_SIZE, 8);
            slot.fill((byte) 0);

            var original = sampleCheckpoint(42, 0);
            Checkpoint.write(slot, original);

            assertTrue(Checkpoint.isValid(slot));

            var restored = Checkpoint.read(slot);

            assertEquals(original.generation, restored.generation);
            assertEquals(original.slotId, restored.slotId);
            assertEquals(original.incompatibleFeatures, restored.incompatibleFeatures);
            assertEquals(original.compatibleFeatures, restored.compatibleFeatures);
            assertEquals(original.pageSize, restored.pageSize);
            assertEquals(original.chunkSize, restored.chunkSize);
            assertEquals(original.totalPages, restored.totalPages);
            assertEquals(original.nextPage, restored.nextPage);

            assertEquals(1, restored.sections.length);
            var sec = restored.sections[0];
            assertEquals(Checkpoint.SECTION_CORE_STATE, sec.sectionType());
            assertEquals(Checkpoint.ENCODING_INLINE, sec.encoding());
            assertEquals(0, sec.flags());
            assertEquals(1, sec.itemCount());
            assertEquals(128L, sec.payloadBytes());
            assertEquals(0, sec.inlineOffset());
            assertEquals(128, sec.inlineLength());
            assertEquals(0xAABBCCDD, sec.payloadCrc32c());

            assertArrayEquals(original.inlineData, restored.inlineData);
        }
    }

    @Test
    void invalidMagicRejected() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment slot = arena.allocate(SLOT_SIZE, 8);
            slot.fill((byte) 0);

            Checkpoint.write(slot, sampleCheckpoint(1, 0));
            assertTrue(Checkpoint.isValid(slot));

            // Corrupt the magic (first 8 bytes)
            slot.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 0xDEADBEEFCAFEBABEL);

            assertFalse(Checkpoint.isValid(slot));
        }
    }

    @Test
    void invalidCrcRejected() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment slot = arena.allocate(SLOT_SIZE, 8);
            slot.fill((byte) 0);

            Checkpoint.write(slot, sampleCheckpoint(1, 0));
            assertTrue(Checkpoint.isValid(slot));

            // Corrupt a payload byte (payload starts after the 64-byte RecordHeader)
            long payloadOffset = RecordHeader.HEADER_SIZE + 10;
            byte old = slot.get(ValueLayout.JAVA_BYTE, payloadOffset);
            slot.set(ValueLayout.JAVA_BYTE, payloadOffset, (byte) (old ^ 0xFF));

            assertFalse(Checkpoint.isValid(slot));
        }
    }

    @Test
    void chooseBestPicksHigherGeneration() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment slotA = arena.allocate(SLOT_SIZE, 8);
            MemorySegment slotB = arena.allocate(SLOT_SIZE, 8);
            slotA.fill((byte) 0);
            slotB.fill((byte) 0);

            Checkpoint.write(slotA, sampleCheckpoint(5, 0));
            Checkpoint.write(slotB, sampleCheckpoint(10, 1));

            var best = Checkpoint.chooseBest(slotA, slotB);
            assertNotNull(best);
            assertEquals(10, best.generation);
            assertEquals(1, best.slotId);
        }
    }

    @Test
    void chooseBestSkipsCorruptSlot() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment slotA = arena.allocate(SLOT_SIZE, 8);
            MemorySegment slotB = arena.allocate(SLOT_SIZE, 8);
            slotA.fill((byte) 0);
            slotB.fill((byte) 0);

            Checkpoint.write(slotA, sampleCheckpoint(3, 0));
            Checkpoint.write(slotB, sampleCheckpoint(7, 1));

            // Corrupt slot B's magic
            slotB.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 0L);

            var best = Checkpoint.chooseBest(slotA, slotB);
            assertNotNull(best);
            assertEquals(3, best.generation);
            assertEquals(0, best.slotId);
        }
    }

    @Test
    void bothCorruptReturnsNull() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment slotA = arena.allocate(SLOT_SIZE, 8);
            MemorySegment slotB = arena.allocate(SLOT_SIZE, 8);
            // Both all-zero → no valid magic
            slotA.fill((byte) 0);
            slotB.fill((byte) 0);

            assertNull(Checkpoint.chooseBest(slotA, slotB));
        }
    }

    @Test
    void inactiveSlotPage() {
        assertEquals(Checkpoint.SLOT_B_PAGE,
                Checkpoint.inactiveSlotPage(Checkpoint.SLOT_A_PAGE));
        assertEquals(Checkpoint.SLOT_A_PAGE,
                Checkpoint.inactiveSlotPage(Checkpoint.SLOT_B_PAGE));
    }

    @Test
    void sectionRefsRoundTrip() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment slot = arena.allocate(SLOT_SIZE, 8);
            slot.fill((byte) 0);

            var data = new Checkpoint.CheckpointData();
            data.generation = 99;
            data.slotId = 1;
            data.incompatibleFeatures = 0;
            data.compatibleFeatures = 0;
            data.pageSize = ChunkStore.PAGE_SIZE;
            data.chunkSize = ChunkStore.DEFAULT_CHUNK_SIZE;
            data.totalPages = 1000;
            data.nextPage = 100;

            // Inline section
            var inlineSec = new Checkpoint.SectionRef(
                    Checkpoint.SECTION_CORE_STATE,
                    Checkpoint.ENCODING_INLINE,
                    (short) 0,
                    5,
                    64L,
                    0,
                    64,
                    0,
                    0,
                    0x11223344
            );

            // Extent section
            var extentSec = new Checkpoint.SectionRef(
                    Checkpoint.SECTION_SLAB_CLASS_TABLE,
                    Checkpoint.ENCODING_INLINE,
                    (short) 1,
                    10,
                    4096L,
                    0,
                    0,
                    50,
                    4,
                    0x55667788
            );

            // Another inline section
            var inlineSec2 = new Checkpoint.SectionRef(
                    Checkpoint.SECTION_TREE_TABLE,
                    Checkpoint.ENCODING_INLINE,
                    (short) 0,
                    3,
                    32L,
                    64,
                    32,
                    0,
                    0,
                    0x99AABBCC
            );

            data.sections = new Checkpoint.SectionRef[]{inlineSec, extentSec, inlineSec2};
            data.inlineData = new byte[96];
            for (int i = 0; i < 96; i++) {
                data.inlineData[i] = (byte) (i & 0xFF);
            }

            Checkpoint.write(slot, data);
            var restored = Checkpoint.read(slot);

            assertEquals(3, restored.sections.length);

            // Verify each section ref field-by-field
            for (int i = 0; i < 3; i++) {
                var orig = data.sections[i];
                var rest = restored.sections[i];
                assertEquals(orig.sectionType(), rest.sectionType(), "section " + i + " type");
                assertEquals(orig.encoding(), rest.encoding(), "section " + i + " encoding");
                assertEquals(orig.flags(), rest.flags(), "section " + i + " flags");
                assertEquals(orig.itemCount(), rest.itemCount(), "section " + i + " itemCount");
                assertEquals(orig.payloadBytes(), rest.payloadBytes(), "section " + i + " payloadBytes");
                assertEquals(orig.inlineOffset(), rest.inlineOffset(), "section " + i + " inlineOffset");
                assertEquals(orig.inlineLength(), rest.inlineLength(), "section " + i + " inlineLength");
                assertEquals(orig.extentStartPage(), rest.extentStartPage(), "section " + i + " extentStartPage");
                assertEquals(orig.extentPageCount(), rest.extentPageCount(), "section " + i + " extentPageCount");
                assertEquals(orig.payloadCrc32c(), rest.payloadCrc32c(), "section " + i + " payloadCrc32c");
            }

            assertArrayEquals(data.inlineData, restored.inlineData);
        }
    }
}
