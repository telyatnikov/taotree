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

            var result = wa.alloc(64, NodePtr.NODE_4, 1);
            assertNotNull(result.segment());
            assertNotEquals(0, result.nodePtr());
            assertTrue(WriterArena.isArenaAllocated(result.nodePtr()));
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
                var result = wa.alloc(40, NodePtr.LEAF, 2);
                assertTrue(ptrs.add(result.nodePtr()), "Duplicate pointer at iteration " + i);
                assertTrue(WriterArena.isArenaAllocated(result.nodePtr()));
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
            var result = wa.alloc(32, NodePtr.LEAF, 0);
            result.segment().set(ValueLayout.JAVA_LONG, 0, 0xDEAD_BEEF_CAFE_BABEL);
            result.segment().set(ValueLayout.JAVA_INT, 8, 42);

            // Resolve via static method with the ChunkStore
            MemorySegment resolved = WriterArena.resolve(cs, result.nodePtr(), 32);
            assertEquals(0xDEAD_BEEF_CAFE_BABEL, resolved.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(42, resolved.get(ValueLayout.JAVA_INT, 8));

            // Resolve via instance method
            MemorySegment resolved2 = wa.resolve(result.nodePtr(), 32);
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

            var result = wa.alloc(64, NodePtr.NODE_16, 3);
            long ptr = result.nodePtr();

            // Verify sentinel slabId
            assertEquals(WriterArena.ARENA_SLAB_ID, NodePtr.slabId(ptr));
            assertTrue(WriterArena.isArenaAllocated(ptr));

            // Verify node type is preserved
            assertEquals(NodePtr.NODE_16, NodePtr.nodeType(ptr));

            // Verify we can decode the page+offset from the packed field
            int packed = NodePtr.offset(ptr);
            int page = packed >>> 12;
            int off = packed & 0xFFF;
            assertTrue(page >= 0);
            assertTrue(off >= 0);
            assertTrue(off < ChunkStore.PAGE_SIZE);

            // Write and read back through resolve
            result.segment().set(ValueLayout.JAVA_LONG, 0, 0xABCD_1234_5678_9ABCL);
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
                var result = wa.alloc(512, NodePtr.LEAF, 0);
                result.segment().set(ValueLayout.JAVA_INT, 0, i);
                ptrs[i] = result.nodePtr();
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
            var r1 = wa.alloc(13, NodePtr.LEAF, 0);
            var r2 = wa.alloc(13, NodePtr.LEAF, 0);

            // Both should be different pointers (different offsets)
            assertNotEquals(r1.nodePtr(), r2.nodePtr());

            // Packed offsets should be 8-byte aligned
            int off1 = NodePtr.offset(r1.nodePtr()) & 0xFFF;
            int off2 = NodePtr.offset(r2.nodePtr()) & 0xFFF;
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
    void pageEncodingLimitBoundary() {
        long ptr = WriterArena.encodeArenaPtr(
            NodePtr.LEAF, 7, WriterArena.MAX_ADDRESSABLE_PAGES - 1, ChunkStore.PAGE_SIZE - 8);

        assertEquals(WriterArena.ARENA_SLAB_ID, NodePtr.slabId(ptr));
        assertEquals(WriterArena.MAX_ADDRESSABLE_PAGES - 1, NodePtr.offset(ptr) >>> 12);
        assertEquals(ChunkStore.PAGE_SIZE - 8, NodePtr.offset(ptr) & 0xFFF);
    }

    @Test
    void pageEncodingOverflowThrows() {
        var ex = assertThrows(IllegalStateException.class,
            () -> WriterArena.encodeArenaPtr(NodePtr.LEAF, 0, WriterArena.MAX_ADDRESSABLE_PAGES, 0));
        assertTrue(ex.getMessage().contains("20-bit arena NodePtr limit"));
    }
}
