package org.taotree.internal;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BumpAllocatorTest {

    @Test
    void allocateAndResolve() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 4096)) {

            byte[] data = "Hello, TaoTree!".getBytes(StandardCharsets.UTF_8);
            long ptr = bump.allocate(data.length);
            assertFalse(OverflowPtr.isEmpty(ptr));

            MemorySegment seg = bump.resolve(ptr, data.length);
            MemorySegment.copy(data, 0, seg, ValueLayout.JAVA_BYTE, 0, data.length);

            // Read back
            byte[] read = new byte[data.length];
            MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, 0, read, 0, data.length);
            assertArrayEquals(data, read);
        }
    }

    @Test
    void multipleAllocationsPackContiguously() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 4096)) {

            long ptr1 = bump.allocate(10);
            long ptr2 = bump.allocate(20);
            long ptr3 = bump.allocate(30);

            // All on the same page
            assertEquals(OverflowPtr.pageId(ptr1), OverflowPtr.pageId(ptr2));
            assertEquals(OverflowPtr.pageId(ptr2), OverflowPtr.pageId(ptr3));

            // Offsets are sequential (first page reserves byte 0 to avoid EMPTY_PTR collision)
            int base = OverflowPtr.offset(ptr1);
            assertTrue(base > 0, "First allocation should not be at offset 0 on page 0");
            assertEquals(base + 10, OverflowPtr.offset(ptr2));
            assertEquals(base + 30, OverflowPtr.offset(ptr3));

            assertEquals(60, bump.bytesAllocated());
        }
    }

    @Test
    void spillsToNextPage() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 64)) {

            // 64 byte pages. Allocate payloads that exceed one page.
            long ptr1 = bump.allocate(40);
            long ptr2 = bump.allocate(40); // won't fit in remaining 24 bytes

            assertEquals(0, OverflowPtr.pageId(ptr1));
            assertEquals(1, OverflowPtr.pageId(ptr2)); // spilled to page 1
            assertEquals(0, OverflowPtr.offset(ptr2));  // starts at beginning of new page

            assertEquals(2, bump.pageCount());
        }
    }

    @Test
    void writeAndReadAcrossPages() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 128)) {

            String[] strings = {
                "short",
                "a medium length string here",
                "Yellowstone Lake, near fishing bridge",
                "Haliaeetus leucocephalus (Linnaeus, 1766)",
                "x".repeat(100)
            };

            long[] ptrs = new long[strings.length];
            int[] lens = new int[strings.length];

            for (int i = 0; i < strings.length; i++) {
                byte[] utf8 = strings[i].getBytes(StandardCharsets.UTF_8);
                lens[i] = utf8.length;
                ptrs[i] = bump.allocate(lens[i]);
                MemorySegment seg = bump.resolve(ptrs[i], lens[i]);
                MemorySegment.copy(utf8, 0, seg, ValueLayout.JAVA_BYTE, 0, lens[i]);
            }

            // Read all back
            for (int i = 0; i < strings.length; i++) {
                MemorySegment seg = bump.resolve(ptrs[i], lens[i]);
                byte[] read = new byte[lens[i]];
                MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, 0, read, 0, lens[i]);
                assertEquals(strings[i], new String(read, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void oversizedPayload() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 64)) {

            // Payload larger than page size
            long ptr = bump.allocate(200);
            MemorySegment seg = bump.resolve(ptr, 200);
            assertEquals(200, seg.byteSize());

            // Write and read
            for (int i = 0; i < 200; i++) {
                seg.set(ValueLayout.JAVA_BYTE, i, (byte) i);
            }
            for (int i = 0; i < 200; i++) {
                assertEquals((byte) i, seg.get(ValueLayout.JAVA_BYTE, i));
            }
        }
    }

    @Test
    void oversizedDoesNotCorruptBumpPage() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 64)) {

            // Normal allocation first
            long ptr1 = bump.allocate(20);
            int baseOffset = OverflowPtr.offset(ptr1);
            // Oversized
            long ptrBig = bump.allocate(200);
            // Normal again — should continue on the original page, not the oversized one
            long ptr2 = bump.allocate(20);

            assertEquals(OverflowPtr.pageId(ptr1), OverflowPtr.pageId(ptr2));
            assertEquals(baseOffset + 20, OverflowPtr.offset(ptr2)); // right after ptr1

            // Oversized is on a different page
            assertNotEquals(OverflowPtr.pageId(ptr1), OverflowPtr.pageId(ptrBig));
        }
    }

    @Test
    void bytesAllocatedTracking() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 4096)) {

            assertEquals(0, bump.bytesAllocated());
            bump.allocate(100);
            assertEquals(100, bump.bytesAllocated());
            bump.allocate(200);
            assertEquals(300, bump.bytesAllocated());
        }
    }

    @Test
    void zeroLengthThrows() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena)) {
            assertThrows(IllegalArgumentException.class, () -> bump.allocate(0));
            assertThrows(IllegalArgumentException.class, () -> bump.allocate(-1));
        }
    }

    // ---- STRONGER: input validation ----

    @Test
    void rejectZeroPageSize() {
        try (var arena = Arena.ofConfined()) {
            assertThrows(IllegalArgumentException.class,
                () -> new BumpAllocator(arena, 0));
        }
    }

    @Test
    void rejectNegativePageSize() {
        try (var arena = Arena.ofConfined()) {
            assertThrows(IllegalArgumentException.class,
                () -> new BumpAllocator(arena, -1));
        }
    }

    @Test
    void negativeLengthAllocateThrows() {
        try (var arena = Arena.ofConfined()) {
            var bump = new BumpAllocator(arena, 4096);
            assertThrows(IllegalArgumentException.class,
                () -> bump.allocate(-1));
        }
    }

    // ---- isFileBacked ----

    @Test
    void inMemoryIsNotFileBacked() {
        try (var arena = Arena.ofConfined()) {
            var bump = new BumpAllocator(arena, 4096);
            assertFalse(bump.isFileBacked());
        }
    }

    // ---- totalCommittedBytes ----

    @Test
    void totalCommittedBytesTracksAllPages() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 128)) {

            assertEquals(0, bump.totalCommittedBytes());

            // First allocation creates a page
            bump.allocate(10);
            assertEquals(128, bump.totalCommittedBytes());

            // Fill current page and spill to next
            bump.allocate(120);
            assertEquals(256, bump.totalCommittedBytes());

            // Oversized allocation creates its own page
            bump.allocate(300);
            assertTrue(bump.totalCommittedBytes() >= 256 + 300);
        }
    }

    // ---- pageCount and currentPage tracking ----

    @Test
    void pageCountAndCurrentPage() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 64)) {

            assertEquals(0, bump.pageCount());
            assertEquals(-1, bump.currentPage());
            assertEquals(0, bump.bumpOffset());

            bump.allocate(10);
            assertEquals(1, bump.pageCount());
            assertEquals(0, bump.currentPage());

            // Spill to next page
            bump.allocate(60);
            assertEquals(2, bump.pageCount());
            assertEquals(1, bump.currentPage());
        }
    }

    // ---- default page size constructor ----

    @Test
    void defaultPageSizeConstructor() {
        try (var arena = Arena.ofConfined()) {
            var bump = new BumpAllocator(arena);
            assertEquals(BumpAllocator.DEFAULT_PAGE_SIZE, bump.pageSize());
            assertFalse(bump.isFileBacked());
        }
    }

    // ---- export returns empty arrays for in-memory mode ----

    @Test
    void exportReturnsEmptyForInMemory() {
        try (var arena = Arena.ofConfined()) {
            var bump = new BumpAllocator(arena, 4096);
            bump.allocate(100);

            int[] locations = bump.exportPageLocations();
            int[] sizes = bump.exportPageSizes();
            assertEquals(0, locations.length);
            assertEquals(0, sizes.length);
        }
    }

    // ---- file-backed mode ----

    @TempDir Path tempDir;

    @Test
    void fileBackedAllocation() throws IOException {
        try (var arena = Arena.ofShared()) {
            var cs = ChunkStore.create(tempDir.resolve("bump-test.tao"), arena);
            var bump = new BumpAllocator(arena, cs, ChunkStore.PAGE_SIZE * 4);

            assertTrue(bump.isFileBacked());

            long ptr = bump.allocate(100);
            assertFalse(OverflowPtr.isEmpty(ptr));

            MemorySegment seg = bump.resolve(ptr, 100);
            seg.set(ValueLayout.JAVA_INT_UNALIGNED, 0, 0xBEEF_CAFE);
            assertEquals(0xBEEF_CAFE, bump.resolve(ptr, 100).get(ValueLayout.JAVA_INT_UNALIGNED, 0));
            assertEquals(100, bump.bytesAllocated());
            assertEquals(1, bump.pageCount());

            cs.close();
        }
    }

    @Test
    void fileBackedExportAndRestore() throws IOException {
        try (var arena = Arena.ofShared()) {
            var cs = ChunkStore.create(tempDir.resolve("bump-export.tao"), arena);
            var bump = new BumpAllocator(arena, cs, ChunkStore.PAGE_SIZE * 2);

            // Allocate some data
            long ptr1 = bump.allocate(50);
            bump.resolve(ptr1, 50).set(ValueLayout.JAVA_INT_UNALIGNED, 0, 42);
            long ptr2 = bump.allocate(50);
            bump.resolve(ptr2, 50).set(ValueLayout.JAVA_INT_UNALIGNED, 0, 99);

            // Export
            int[] locations = bump.exportPageLocations();
            int[] sizes = bump.exportPageSizes();
            assertTrue(locations.length > 0);
            assertEquals(locations.length, sizes.length);

            int savedCurrentPage = bump.currentPage();
            int savedBumpOffset = bump.bumpOffset();
            long savedBytes = bump.bytesAllocated();

            // Create new allocator and restore
            var bump2 = new BumpAllocator(arena, cs, ChunkStore.PAGE_SIZE * 2);
            for (int i = 0; i < locations.length; i++) {
                bump2.restorePage(locations[i], sizes[i]);
            }
            bump2.restoreState(savedCurrentPage, savedBumpOffset, savedBytes);

            // Verify restored data
            assertEquals(savedBytes, bump2.bytesAllocated());
            assertEquals(savedCurrentPage, bump2.currentPage());
            assertEquals(savedBumpOffset, bump2.bumpOffset());
            assertEquals(42, bump2.resolve(ptr1, 50).get(ValueLayout.JAVA_INT_UNALIGNED, 0));
            assertEquals(99, bump2.resolve(ptr2, 50).get(ValueLayout.JAVA_INT_UNALIGNED, 0));

            cs.close();
        }
    }

    @Test
    void fileBackedOversized() throws IOException {
        try (var arena = Arena.ofShared()) {
            var cs = ChunkStore.create(tempDir.resolve("bump-oversized.tao"), arena);
            int pageSize = ChunkStore.PAGE_SIZE * 2;
            var bump = new BumpAllocator(arena, cs, pageSize);

            // Oversized allocation in file-backed mode
            long ptr = bump.allocate(pageSize + 100);
            MemorySegment seg = bump.resolve(ptr, pageSize + 100);
            seg.set(ValueLayout.JAVA_INT_UNALIGNED, 0, 0xDEAD_BEEF);
            assertEquals(0xDEAD_BEEF, bump.resolve(ptr, pageSize + 100).get(ValueLayout.JAVA_INT_UNALIGNED, 0));

            cs.close();
        }
    }

    @Test
    void fileBackedPageSizeValidation() {
        try (var arena = Arena.ofShared()) {
            assertThrows(IllegalArgumentException.class,
                () -> new BumpAllocator(arena, null, 0));
            assertThrows(IllegalArgumentException.class,
                () -> new BumpAllocator(arena, null, -1));
        }
    }

    @Test
    void restorePageRequiresFileBacked() {
        try (var arena = Arena.ofConfined()) {
            var bump = new BumpAllocator(arena, 4096);
            assertThrows(IllegalStateException.class,
                () -> bump.restorePage(0, 1));
        }
    }

    // ---- multiple page growth in-memory ----

    @Test
    void multiplePageGrowth() {
        try (var arena = Arena.ofConfined();
             var bump = new BumpAllocator(arena, 64)) {

            // Allocate many small payloads across multiple pages
            long[] ptrs = new long[100];
            for (int i = 0; i < 100; i++) {
                ptrs[i] = bump.allocate(10);
                bump.resolve(ptrs[i], 10).set(ValueLayout.JAVA_BYTE, 0, (byte) i);
            }

            assertTrue(bump.pageCount() > 1);
            assertEquals(1000, bump.bytesAllocated());

            // Verify all
            for (int i = 0; i < 100; i++) {
                assertEquals((byte) i, bump.resolve(ptrs[i], 10).get(ValueLayout.JAVA_BYTE, 0));
            }
        }
    }
}
