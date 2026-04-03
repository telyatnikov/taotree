package org.taotree.internal;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

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
}
