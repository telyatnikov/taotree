package org.taotree.internal.alloc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.HashSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.NodePtr;

class WriterArenaTest {

    @TempDir Path tempDir;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private ChunkStore createStore(Arena arena) throws IOException {
        return ChunkStore.create(tempDir.resolve("test-" + System.nanoTime() + ".tao"), arena);
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    void basicAllocation() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);
            wa.beginScope();

            long ptr = wa.alloc(64, NodePtr.NODE_4, 1);
            assertNotEquals(0, ptr);
            assertTrue(WriterArena.isArenaAllocated(ptr));
            assertEquals(1, wa.allocCount());

            wa.endScope();
            cs.close();
        }
    }

    @Test
    void multipleAllocationsHaveUniquePointers() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);
            wa.beginScope();

            var ptrs = new HashSet<Long>();
            int count = 100;
            for (int i = 0; i < count; i++) {
                long ptr = wa.alloc(40, NodePtr.LEAF, 2);
                assertTrue(ptrs.add(ptr), "Duplicate pointer at iteration " + i);
                assertTrue(WriterArena.isArenaAllocated(ptr));
            }

            assertEquals(count, wa.allocCount());
            assertEquals(count, ptrs.size());

            wa.endScope();
            cs.close();
        }
    }

    @Test
    void scopeTracking() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);

            // First scope
            wa.beginScope();
            wa.alloc(64, NodePtr.NODE_4, 0);
            wa.alloc(64, NodePtr.NODE_4, 0);
            wa.alloc(64, NodePtr.NODE_4, 0);
            assertEquals(3, wa.allocCount());
            int startPage1 = wa.scopeStartPage();
            int endPage1 = wa.scopeEndPage();
            assertTrue(startPage1 >= 0);
            assertTrue(endPage1 > startPage1);

            wa.endScope();
            // After endScope, allocCount resets
            assertEquals(0, wa.allocCount());

            // Second scope
            wa.beginScope();
            wa.alloc(128, NodePtr.PREFIX, 1);
            assertEquals(1, wa.allocCount());
            wa.endScope();
            assertEquals(0, wa.allocCount());

            cs.close();
        }
    }

    @Test
    void resolveArenaPtr() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);
            wa.beginScope();

            // Allocate and write data
            long ptr = wa.alloc(32, NodePtr.LEAF, 0);
            MemorySegment seg = wa.resolve(ptr, 32);
            seg.set(ValueLayout.JAVA_LONG, 0, 0xDEAD_BEEF_CAFE_BABEL);
            seg.set(ValueLayout.JAVA_INT, 8, 42);

            // Resolve via static method with the ChunkStore
            MemorySegment resolved = WriterArena.resolve(cs, ptr, 32);
            assertEquals(0xDEAD_BEEF_CAFE_BABEL, resolved.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(42, resolved.get(ValueLayout.JAVA_INT, 8));

            // Resolve via instance method
            MemorySegment resolved2 = wa.resolve(ptr, 32);
            assertEquals(0xDEAD_BEEF_CAFE_BABEL, resolved2.get(ValueLayout.JAVA_LONG, 0));

            wa.endScope();
            cs.close();
        }
    }

    @Test
    void arenaEncodingRoundTrip() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);
            wa.beginScope();

            long ptr = wa.alloc(64, NodePtr.NODE_16, 3);

            // Verify arena flag in metadata
            assertTrue(WriterArena.isArenaAllocated(ptr));
            assertTrue((NodePtr.metadata(ptr) & NodePtr.ARENA_FLAG) != 0);

            // Verify node type is preserved
            assertEquals(NodePtr.NODE_16, NodePtr.nodeType(ptr));

            // Verify slab class is preserved
            assertEquals(3, NodePtr.slabClassId(ptr));

            // Write and read back through resolve
            MemorySegment seg = wa.resolve(ptr, 64);
            seg.set(ValueLayout.JAVA_LONG, 0, 0xABCD_1234_5678_9ABCL);
            MemorySegment resolved = WriterArena.resolve(cs, ptr, 64);
            assertEquals(0xABCD_1234_5678_9ABCL, resolved.get(ValueLayout.JAVA_LONG, 0));

            wa.endScope();
            cs.close();
        }
    }

    @Test
    void largeBatchGrowth() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);
            wa.beginScope();

            // Allocate many nodes to trigger multiple batch growths.
            // Initial batch = 16 pages = 64 KB. With 512-byte allocations:
            // 4096/512 = 8 per page, 16 pages * 8 = 128 in first batch.
            // We'll go past that to trigger growth.
            int count = 500;
            long[] ptrs = new long[count];
            for (int i = 0; i < count; i++) {
                long ptr = wa.alloc(512, NodePtr.LEAF, 0);
                MemorySegment seg = wa.resolve(ptr, 512);
                seg.set(ValueLayout.JAVA_INT, 0, i);
                ptrs[i] = ptr;
            }

            assertEquals(count, wa.allocCount());

            // Verify all allocations resolve correctly
            for (int i = 0; i < count; i++) {
                MemorySegment seg = WriterArena.resolve(cs, ptrs[i], 512);
                assertEquals(i, seg.get(ValueLayout.JAVA_INT, 0),
                        "Mismatch at allocation " + i);
            }

            wa.endScope();
            cs.close();
        }
    }

    @Test
    void nonArenaPointerNotDetected() {
        // A regular slab pointer should not be detected as arena-allocated
        long regularPtr = NodePtr.pack(NodePtr.LEAF, 0, 0, 0);
        assertFalse(WriterArena.isArenaAllocated(regularPtr));

        long regularPtr2 = NodePtr.pack(NodePtr.NODE_4, 5, 3, 1024);
        assertFalse(WriterArena.isArenaAllocated(regularPtr2));
    }

    @Test
    void allocationExceedingPageSizeThrows() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);
            wa.beginScope();

            // 4096-byte page size — requesting more should fail
            assertThrows(IllegalArgumentException.class,
                    () -> wa.alloc(ChunkStore.PAGE_SIZE + 1, NodePtr.LEAF, 0));

            wa.endScope();
            cs.close();
        }
    }

    @Test
    void allocationAlignment() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);
            wa.beginScope();

            // Allocate an odd-sized node; should be 8-byte aligned internally
            long r1 = wa.alloc(13, NodePtr.LEAF, 0);
            long r2 = wa.alloc(13, NodePtr.LEAF, 0);

            // Both should be different pointers (different offsets)
            assertNotEquals(r1, r2);

            // Packed offsets should be 8-byte aligned
            int off1 = NodePtr.offset(r1) & 0xFFF;
            int off2 = NodePtr.offset(r2) & 0xFFF;
            assertEquals(0, off1 % 8, "First allocation offset not 8-byte aligned");
            assertEquals(0, off2 % 8, "Second allocation offset not 8-byte aligned");

            // The gap should be at least 16 (13 rounded up to 16)
            assertTrue(off2 - off1 >= 16,
                    "Expected 8-byte aligned gap, got: " + (off2 - off1));

            wa.endScope();
            cs.close();
        }
    }

    @Test
    void scopeStartPageUpdatedOnFirstAlloc() throws Exception {
        try (var arena = Arena.ofShared()) {
            var cs = createStore(arena);
            var wa = new WriterArena(cs);

            wa.beginScope();
            // Before any allocation, scopeStartPage may be -1 (initial state)
            wa.alloc(64, NodePtr.LEAF, 0);
            int start = wa.scopeStartPage();
            assertTrue(start >= 0, "scopeStartPage should be set after first alloc");

            // Additional allocs in the same scope don't change scopeStartPage
            wa.alloc(64, NodePtr.LEAF, 0);
            assertEquals(start, wa.scopeStartPage());

            wa.endScope();
            cs.close();
        }
    }

    @Test
    void pageEncodingLargePageNumber() {
        // Test with a page number that uses all 36 bits of the new encoding:
        // page = (0x7FF << 20) | 0xFFFFF = 0x7FFFFFFFL (max positive int, ~2B pages)
        int page = Integer.MAX_VALUE;
        int byteOff = ChunkStore.PAGE_SIZE - 8;
        long ptr = WriterArena.encodeArenaPtr(NodePtr.LEAF, 7, page, byteOff);

        assertTrue(WriterArena.isArenaAllocated(ptr));
        assertEquals(NodePtr.LEAF, NodePtr.nodeType(ptr));
        assertEquals(7, NodePtr.slabClassId(ptr));

        // Decode and verify the page number survives the round-trip
        int pageHigh = NodePtr.slabId(ptr);
        int offsetField = NodePtr.offset(ptr);
        int decodedPage = (pageHigh << 20) | ((offsetField >>> 12) & 0xFFFFF);
        int decodedByte = offsetField & 0xFFF;
        assertEquals(page, decodedPage);
        assertEquals(byteOff, decodedByte);
    }

    @Test
    void pageEncodingNegativePageThrows() {
        assertThrows(IllegalStateException.class,
            () -> WriterArena.encodeArenaPtr(NodePtr.LEAF, 0, -1, 0));
    }
}
