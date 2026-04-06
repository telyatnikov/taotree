package org.taotree.internal.alloc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.ChunkStore;

class ChunkStoreTest {

    @TempDir Path tmp;

    @Test
    void createAndAllocPages() throws IOException {
        Path file = tmp.resolve("test.tao");
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena)) {

            // Pages 0-1 are reserved for superblock
            assertEquals(2, store.nextPage());
            assertTrue(store.totalPages() > 0);

            // Allocate a run of 256 pages (1 MB slab)
            int start = store.allocPages(256);
            assertEquals(2, start);
            assertEquals(258, store.nextPage());

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

            // Fill most of chunk 0 (pages 0-1 are superblock, so 1022 pages available)
            // Allocate runs of 256 pages until we can't fit another in chunk 0
            int a = store.allocPages(256);  // pages 2-257
            assertEquals(2, a);
            int b = store.allocPages(256);  // pages 258-513
            assertEquals(258, b);
            int c = store.allocPages(256);  // pages 514-769
            assertEquals(514, c);
            // 770..1023 = 254 pages left, one more 256-page slab does NOT fit.
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

            // Chunk 0: pages 0-511. Pages 0-1 are superblock. Available: 2-511 = 510 pages.
            // Allocate 256 pages: pages 2-257. nextPage = 258.
            int a = store.allocPages(256);
            assertEquals(2, a);
            assertEquals(258, store.nextPage());

            // 258 + 256 = 514 > 512 — would straddle chunk boundary.
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
                 16384, 256 + 16 + 2)) {

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
            assertEquals(2 * ChunkStore.PAGE_SIZE, sb.byteSize());

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

    // ---- STRONGER: input validation ----

    @Test
    void rejectInvalidChunkSize() throws Exception {
        Path file = tmp.resolve("bad.tao");
        try (var arena = Arena.ofConfined()) {
            // Not a multiple of page size
            assertThrows(IllegalArgumentException.class,
                () -> ChunkStore.create(file, arena, 5000, false));
            // Zero
            assertThrows(IllegalArgumentException.class,
                () -> ChunkStore.create(file, arena, 0, false));
            // Negative
            assertThrows(IllegalArgumentException.class,
                () -> ChunkStore.create(file, arena, -4096, false));
        }
    }

    // ---- File-backed: growFile across multiple chunks ----

    @Test
    void allocAcrossMultipleChunks() throws Exception {
        Path file = tmp.resolve("multi_chunk.tao");
        // Use tiny chunks (8 pages = 32KB per chunk) to exercise multi-chunk growth
        long tinyChunk = 8L * ChunkStore.PAGE_SIZE;
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena, tinyChunk, false)) {
            // Allocate pages spanning 3+ chunks
            int[] starts = new int[10];
            for (int i = 0; i < 10; i++) {
                starts[i] = store.allocPages(2);
            }
            // 10 allocations of 2 pages each = 20 pages (plus page 0 for superblock)
            // With 8 pages per chunk, we need at least 3 chunks

            // Verify we can write to all allocated pages
            for (int i = 0; i < 10; i++) {
                var seg = store.resolvePage(starts[i]);
                seg.set(ValueLayout.JAVA_LONG, 0, (long) i);
            }
            for (int i = 0; i < 10; i++) {
                var seg = store.resolvePage(starts[i]);
                assertEquals((long) i, seg.get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- File-backed: sync and reopen consistency ----

    @Test
    void syncAndReopenData() throws Exception {
        Path file = tmp.resolve("sync.tao");
        long chunk = 64L * ChunkStore.PAGE_SIZE;
        int startPage;
        int totalPages;
        int nextPage;

        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena, chunk, false)) {
            startPage = store.allocPages(4);
            var seg = store.resolvePage(startPage);
            seg.set(ValueLayout.JAVA_LONG, 0, 0xDEADBEEFL);
            seg.set(ValueLayout.JAVA_LONG, 8, 0xCAFEBABEL);
            totalPages = store.totalPages();
            nextPage = store.nextPage();
            store.sync();
        }

        try (var arena = Arena.ofConfined();
             var store = ChunkStore.open(file, arena, chunk, totalPages, nextPage)) {
            var seg = store.resolvePage(startPage);
            assertEquals(0xDEADBEEFL, seg.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(0xCAFEBABEL, seg.get(ValueLayout.JAVA_LONG, 8));
        }
    }

    // ---- Dirty chunk tracking ----

    @Test
    void allocPagesDirtiesContainingChunk() throws Exception {
        Path file = tmp.resolve("dirty_track.tao");
        long smallChunk = 512L * ChunkStore.PAGE_SIZE; // 2 MB chunks
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena, smallChunk, false)) {

            // After create, chunk 0 is mapped (for the superblock) and pages were
            // allocated internally — chunk 0 is dirty from the initial growFile.
            // But only allocPages() marks dirty, and create() calls growFile()
            // then sets nextPage directly (no allocPages()). So dirtyChunkCount
            // starts at 0.
            assertEquals(0, store.dirtyChunkCount());

            // Allocate pages in chunk 0
            store.allocPages(4);
            assertEquals(1, store.dirtyChunkCount());

            // Allocate more in chunk 0 — still 1 dirty chunk
            store.allocPages(4);
            assertEquals(1, store.dirtyChunkCount());

            // syncDirty clears the dirty set
            store.syncDirty();
            assertEquals(0, store.dirtyChunkCount());

            // Allocate again — re-dirties chunk 0
            store.allocPages(4);
            assertEquals(1, store.dirtyChunkCount());
        }
    }

    @Test
    void allocAcrossChunksDirtiesMultiple() throws Exception {
        Path file = tmp.resolve("dirty_multi.tao");
        long smallChunk = 8L * ChunkStore.PAGE_SIZE; // 8 pages per chunk (32 KB)
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena, smallChunk, false)) {

            // Chunk 0: pages 0-7. Pages 0-1 are superblock. Available: 2-7 = 6 pages.
            store.allocPages(2); // pages 2-3 in chunk 0
            assertEquals(1, store.dirtyChunkCount());

            store.allocPages(2); // pages 4-5 in chunk 0
            assertEquals(1, store.dirtyChunkCount()); // still chunk 0

            // Next alloc of 4 pages won't fit in chunk 0 (only 2 pages left) → chunk 1
            store.allocPages(4); // skips to chunk 1 (page 8)
            assertEquals(2, store.dirtyChunkCount()); // chunks 0 and 1

            // Another alloc in chunk 1
            store.allocPages(4); // pages 12-15 in chunk 1? No, 8+4=12, then 12+4=16 which is chunk 2
            // Actually: chunk 1 has pages 8-15, we allocated 8-11, now 12-15
            assertEquals(2, store.dirtyChunkCount()); // still chunks 0 and 1

            // Force into chunk 2
            store.allocPages(4); // pages 16-19 in chunk 2
            assertEquals(3, store.dirtyChunkCount());

            // syncDirty clears all
            store.syncDirty();
            assertEquals(0, store.dirtyChunkCount());
        }
    }

    @Test
    void markDirtyExplicitly() throws Exception {
        Path file = tmp.resolve("dirty_mark.tao");
        long smallChunk = 512L * ChunkStore.PAGE_SIZE;
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena, smallChunk, false)) {

            // No dirty chunks initially
            assertEquals(0, store.dirtyChunkCount());

            // Explicitly mark page 0 (checkpoint slot) dirty
            store.markDirty(0);
            assertEquals(1, store.dirtyChunkCount());

            // Mark same chunk again — no change
            store.markDirty(1);
            assertEquals(1, store.dirtyChunkCount());

            // syncDirty clears
            store.syncDirty();
            assertEquals(0, store.dirtyChunkCount());

            // Mark again after sync
            store.markDirty(0);
            assertEquals(1, store.dirtyChunkCount());
        }
    }

    @Test
    void syncDirtyAndReopenData() throws Exception {
        Path file = tmp.resolve("sync_dirty.tao");
        long chunk = 64L * ChunkStore.PAGE_SIZE;
        int startPage;
        int totalPages;
        int nextPage;

        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena, chunk, false)) {
            startPage = store.allocPages(4);
            var seg = store.resolvePage(startPage);
            seg.set(ValueLayout.JAVA_LONG, 0, 0xDEADBEEFL);
            seg.set(ValueLayout.JAVA_LONG, 8, 0xCAFEBABEL);
            totalPages = store.totalPages();
            nextPage = store.nextPage();
            // Use syncDirty instead of sync
            store.syncDirty();
        }

        try (var arena = Arena.ofConfined();
             var store = ChunkStore.open(file, arena, chunk, totalPages, nextPage)) {
            var seg = store.resolvePage(startPage);
            assertEquals(0xDEADBEEFL, seg.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(0xCAFEBABEL, seg.get(ValueLayout.JAVA_LONG, 8));
        }
    }

    @Test
    void fullSyncClearsDirtySet() throws Exception {
        Path file = tmp.resolve("full_sync.tao");
        try (var arena = Arena.ofConfined();
             var store = ChunkStore.create(file, arena)) {
            store.allocPages(4);
            assertEquals(1, store.dirtyChunkCount());

            // Full sync also clears the dirty set
            store.sync();
            assertEquals(0, store.dirtyChunkCount());
        }
    }
}
