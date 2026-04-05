package org.taotree.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CommitRecordTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static CommitRecord.CommitData sampleCommit(long generation, int dictCount) {
        var data = new CommitRecord.CommitData();
        data.generation = generation;
        data.prevCommitPage = 42;
        data.primaryRoot = 0x0600_0001_0000_0018L;
        data.primarySize = 1000;
        data.dictionaryCount = dictCount;
        data.dictRoots = new long[dictCount];
        data.dictNextCodes = new long[dictCount];
        data.dictSizes = new long[dictCount];
        for (int i = 0; i < dictCount; i++) {
            data.dictRoots[i] = 0x0600_0002_0000_0000L + i * 64;
            data.dictNextCodes[i] = (i + 1) * 100L;
            data.dictSizes[i] = (i + 1) * 50L;
        }
        data.arenaStartPage = 100;
        data.arenaEndPage = 200;
        return data;
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    void writeAndReadRoundTrip() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(ChunkStore.PAGE_SIZE, 8);
            page.fill((byte) 0);

            var original = sampleCommit(7, 2);
            CommitRecord.write(page, original);

            assertTrue(CommitRecord.isValid(page));

            var restored = CommitRecord.read(page);
            assertNotNull(restored);

            assertEquals(original.generation, restored.generation);
            assertEquals(original.prevCommitPage, restored.prevCommitPage);
            assertEquals(original.primaryRoot, restored.primaryRoot);
            assertEquals(original.primarySize, restored.primarySize);
            assertEquals(original.dictionaryCount, restored.dictionaryCount);
            assertEquals(original.arenaStartPage, restored.arenaStartPage);
            assertEquals(original.arenaEndPage, restored.arenaEndPage);

            for (int i = 0; i < original.dictionaryCount; i++) {
                assertEquals(original.dictRoots[i], restored.dictRoots[i], "dictRoot " + i);
                assertEquals(original.dictNextCodes[i], restored.dictNextCodes[i], "dictNextCode " + i);
                assertEquals(original.dictSizes[i], restored.dictSizes[i], "dictSize " + i);
            }
        }
    }

    @Test
    void invalidPageReturnsNull() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(ChunkStore.PAGE_SIZE, 8);
            page.fill((byte) 0);

            // All-zero page has no valid magic → read returns null
            assertNull(CommitRecord.read(page));
            assertFalse(CommitRecord.isValid(page));
        }
    }

    @Test
    void corruptedCrcReturnsNull() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(ChunkStore.PAGE_SIZE, 8);
            page.fill((byte) 0);

            CommitRecord.write(page, sampleCommit(1, 0));
            assertNotNull(CommitRecord.read(page));

            // Corrupt a payload byte (after the 64-byte header)
            long payloadOffset = RecordHeader.HEADER_SIZE + 5;
            byte old = page.get(ValueLayout.JAVA_BYTE, payloadOffset);
            page.set(ValueLayout.JAVA_BYTE, payloadOffset, (byte) (old ^ 0xFF));

            assertNull(CommitRecord.read(page));
        }
    }

    @Test
    void payloadSizeCalculation() {
        assertEquals(40, CommitRecord.payloadSize(0));
        assertEquals(40 + 24, CommitRecord.payloadSize(1));
        assertEquals(40 + 5 * 24, CommitRecord.payloadSize(5));
    }

    @Test
    void multipleDictionaries() {
        try (var arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(ChunkStore.PAGE_SIZE, 8);
            page.fill((byte) 0);

            var original = sampleCommit(15, 3);
            CommitRecord.write(page, original);

            var restored = CommitRecord.read(page);
            assertNotNull(restored);
            assertEquals(3, restored.dictionaryCount);

            for (int i = 0; i < 3; i++) {
                assertEquals(original.dictRoots[i], restored.dictRoots[i], "dictRoot " + i);
                assertEquals(original.dictNextCodes[i], restored.dictNextCodes[i], "dictNextCode " + i);
                assertEquals(original.dictSizes[i], restored.dictSizes[i], "dictSize " + i);
            }
        }
    }
}
