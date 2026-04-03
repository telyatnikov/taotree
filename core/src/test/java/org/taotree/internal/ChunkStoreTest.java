package org.taotree.internal;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class ChunkStoreTest {

    @TempDir Path tmp;

    @Test
    void createAndAllocPages() throws IOException {
        Path file = tmp.resolve("test.tao");
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena)) {

            // Page 0 is reserved for superblock
            assertEquals(1, store.nextPage());
            assertTrue(store.totalPages() > 0);

            // Allocate a run of 256 pages (1 MB slab)
            int start = store.allocPages(256);
            assertEquals(1, start);
            assertEquals(257, store.nextPage());

            // Resolve and write
            MemorySegment seg = store.resolve(start, 256);
            assertEquals(256 * ChunkStore.PAGE_SIZE, seg.byteSize());
            seg.set(ValueLayout.JAVA_LONG, 0, 0xDEADBEEFL);
            seg.set(ValueLayout.JAVA_LONG, seg.byteSize() - 8, 0xCAFEBABEL);

            // Read back
            assertEquals(0xDEADBEEFL, seg.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(0xCAFEBABEL, seg.get(ValueLayout.JAVA_LONG, seg.byteSize() - 8));
        }
    }

    @Test
    void allocMultipleSlabsAndBumpPages() throws IOException {
        Path file = tmp.resolve("test.tao");
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena)) {

            // Allocate 10 slabs (256 pages each) and 20 bump pages (16 pages each)
            int[] slabs = new int[10];
            int[] bumps = new int[20];

            for (int i = 0; i < 10; i++) {
                slabs[i] = store.allocPages(256);
                MemorySegment seg = store.resolve(slabs[i], 256);
                seg.set(ValueLayout.JAVA_LONG, 0, 1000L + i);

                // Two bump pages per slab
                bumps[i * 2] = store.allocPages(16);
                store.resolve(bumps[i * 2], 16).set(ValueLayout.JAVA_LONG, 0, 2000L + i * 2);
                bumps[i * 2 + 1] = store.allocPages(16);
                store.resolve(bumps[i * 2 + 1], 16).set(ValueLayout.JAVA_LONG, 0, 2000L + i * 2 + 1);
            }

            // Verify all slabs
            for (int i = 0; i < 10; i++) {
                MemorySegment seg = store.resolve(slabs[i], 256);
                assertEquals(1000L + i, seg.get(ValueLayout.JAVA_LONG, 0),
                    "slab " + i + " corrupted");
            }

            // Verify all bump pages
            for (int i = 0; i < 20; i++) {
                MemorySegment seg = store.resolve(bumps[i], 16);
                assertEquals(2000L + i, seg.get(ValueLayout.JAVA_LONG, 0),
                    "bump " + i + " corrupted");
            }
        }
    }

    @Test
    void chunkBoundarySkip() throws IOException {
        // Use a small chunk size to test boundary behavior
        Path file = tmp.resolve("test.tao");
        long smallChunk = 1024L * ChunkStore.PAGE_SIZE; // 1024 pages per chunk = 4 MB
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena, smallChunk, false)) {

            assertEquals(1024, store.pagesPerChunk());

            // Fill most of chunk 0 (page 0 is superblock, so 1023 pages available)
            // Allocate runs of 256 pages until we can't fit another in chunk 0
            int a = store.allocPages(256);  // pages 1-256
            assertEquals(1, a);
            int b = store.allocPages(256);  // pages 257-512
            assertEquals(257, b);
            int c = store.allocPages(256);  // pages 513-768
            assertEquals(513, c);
            // 769..1023 = 255 pages left, one more 256-page slab does NOT fit.
            // Should skip to chunk 1 (page 1024).
            int d = store.allocPages(256);
            assertEquals(1024, d);  // skipped to chunk 1
            assertEquals(1280, store.nextPage());
            assertTrue(store.chunkCount() >= 2, "should have 2 chunks now");

            // Next slab fits in chunk 1
            int e = store.allocPages(256);
            assertEquals(1280, e);
        }
    }

    @Test
    void chunkBoundaryStraddlePrevented() throws IOException {
        Path file = tmp.resolve("test.tao");
        long smallChunk = 512L * ChunkStore.PAGE_SIZE; // 512 pages per chunk = 2 MB
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena, smallChunk, false)) {

            // Chunk 0: pages 0-511. Page 0 is superblock. Available: 1-511 = 511 pages.
            // Allocate 256 pages: pages 1-256. nextPage = 257.
            int a = store.allocPages(256);
            assertEquals(1, a);
            assertEquals(257, store.nextPage());

            // 257 + 256 = 513 > 512 — would straddle chunk boundary.
            // Should skip to chunk 1 start (page 512).
            int b = store.allocPages(256);
            assertEquals(512, b);  // skipped to chunk 1
            assertEquals(768, store.nextPage());
        }
    }

    @Test
    void persistenceAcrossCloseReopen() throws IOException {
        Path file = tmp.resolve("test.tao");
        int slabStart;
        int bumpStart;

        // Phase 1: create, write, sync
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena)) {

            slabStart = store.allocPages(256);
            bumpStart = store.allocPages(16);

            MemorySegment slab = store.resolve(slabStart, 256);
            slab.set(ValueLayout.JAVA_LONG, 0, 0xFACE_CAFEL);
            slab.set(ValueLayout.JAVA_LONG, 1024, 0x1234_5678L);

            MemorySegment bump = store.resolve(bumpStart, 16);
            bump.set(ValueLayout.JAVA_LONG, 0, 0xAAAA_BBBBL);

            store.sync();
        }

        // Phase 2: reopen and verify
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.open(file, arena, ChunkStore.DEFAULT_CHUNK_SIZE,
                 // We know: 1 chunk was allocated = 16384 pages
                 16384, 256 + 16 + 1)) {

            MemorySegment slab = store.resolve(slabStart, 256);
            assertEquals(0xFACE_CAFEL, slab.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(0x1234_5678L, slab.get(ValueLayout.JAVA_LONG, 1024));

            MemorySegment bump = store.resolve(bumpStart, 16);
            assertEquals(0xAAAA_BBBBL, bump.get(ValueLayout.JAVA_LONG, 0));
        }
    }

    @Test
    void superblockAccessible() throws IOException {
        Path file = tmp.resolve("test.tao");
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena)) {

            MemorySegment sb = store.superblock();
            assertEquals(ChunkStore.PAGE_SIZE, sb.byteSize());

            // Write magic
            sb.set(ValueLayout.JAVA_LONG, 0, 0x54414F54524545L); // "TAOTREE"
            assertEquals(0x54414F54524545L, sb.get(ValueLayout.JAVA_LONG, 0));
        }
    }

    @Test
    void resolvePageAndResolveBytes() throws IOException {
        Path file = tmp.resolve("test.tao");
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena)) {

            int start = store.allocPages(4);
            MemorySegment full = store.resolve(start, 4);
            full.set(ValueLayout.JAVA_LONG, ChunkStore.PAGE_SIZE + 8, 42L);

            // resolvePage should give us page start+1
            MemorySegment page1 = store.resolvePage(start + 1);
            assertEquals(42L, page1.get(ValueLayout.JAVA_LONG, 8));

            // resolveBytes
            MemorySegment slice = store.resolveBytes(start + 1, 8, 8);
            assertEquals(42L, slice.get(ValueLayout.JAVA_LONG, 0));
        }
    }

    @Test
    void largeAllocation() throws IOException {
        Path file = tmp.resolve("test.tao");
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena)) {

            // Allocate 50 slabs = 50 MB. Should grow into multiple chunks if needed.
            // With 64MB chunks: all fits in 1 chunk (50 * 256 = 12800 pages < 16384).
            int[] starts = new int[50];
            for (int i = 0; i < 50; i++) {
                starts[i] = store.allocPages(256);
                store.resolve(starts[i], 256).set(ValueLayout.JAVA_LONG, 0, i);
            }

            // Verify all
            for (int i = 0; i < 50; i++) {
                assertEquals(i, store.resolve(starts[i], 256).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }
}
