package org.taotree.internal;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.*;

class SlabAllocatorTest {

    @Test
    void registerClassAndAllocate() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 4096)) {

            int classId = alloc.registerClass(64);
            assertEquals(0, classId);
            assertEquals(64, alloc.segmentSize(classId));

            long ptr = alloc.allocate(classId);
            assertFalse(NodePtr.isEmpty(ptr));
            assertEquals(classId, NodePtr.slabClassId(ptr));

            MemorySegment seg = alloc.resolve(ptr);
            assertEquals(64, seg.byteSize());
        }
    }

    @Test
    void multipleClasses() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 4096)) {

            int small = alloc.registerClass(16);
            int large = alloc.registerClass(256);
            assertEquals(0, small);
            assertEquals(1, large);
            assertEquals(2, alloc.classCount());

            long p1 = alloc.allocate(small);
            long p2 = alloc.allocate(large);

            assertEquals(16, alloc.resolve(p1).byteSize());
            assertEquals(256, alloc.resolve(p2).byteSize());
        }
    }

    @Test
    void writeAndReadBack() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 4096)) {

            int classId = alloc.registerClass(32);
            long ptr = alloc.allocate(classId);

            MemorySegment seg = alloc.resolve(ptr);
            seg.set(ValueLayout.JAVA_LONG, 0, 0xDEAD_BEEF_CAFE_BABEL);
            seg.set(ValueLayout.JAVA_INT, 8, 42);

            // Re-resolve and verify
            MemorySegment seg2 = alloc.resolve(ptr);
            assertEquals(0xDEAD_BEEF_CAFE_BABEL, seg2.get(ValueLayout.JAVA_LONG, 0));
            assertEquals(42, seg2.get(ValueLayout.JAVA_INT, 8));
        }
    }

    @Test
    void allocateManySlotsSpansMultipleSlabs() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 1024)) {

            int classId = alloc.registerClass(64);
            // 1024 / 64 = 16 segments per slab. Allocate 50 → need 4 slabs.
            long[] ptrs = new long[50];
            for (int i = 0; i < 50; i++) {
                ptrs[i] = alloc.allocate(classId);
                // Write a unique value
                alloc.resolve(ptrs[i]).set(ValueLayout.JAVA_INT, 0, i);
            }

            assertEquals(50, alloc.totalSegmentsInUse());

            // Verify all values survived
            for (int i = 0; i < 50; i++) {
                int val = alloc.resolve(ptrs[i]).get(ValueLayout.JAVA_INT, 0);
                assertEquals(i, val, "Segment " + i);
            }
        }
    }

    @Test
    void freeAndReallocate() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 4096)) {

            int classId = alloc.registerClass(128);
            long ptr1 = alloc.allocate(classId);
            assertEquals(1, alloc.totalSegmentsInUse());

            alloc.free(ptr1);
            assertEquals(0, alloc.totalSegmentsInUse());

            // Re-allocate — should reuse the freed slot
            long ptr2 = alloc.allocate(classId);
            assertEquals(1, alloc.totalSegmentsInUse());
            // The freed slot should be reused (same slab, same or nearby offset)
            assertEquals(NodePtr.slabId(ptr1), NodePtr.slabId(ptr2));
        }
    }

    @Test
    void freeAllAndReallocate() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 1024)) {

            int classId = alloc.registerClass(64);
            long[] ptrs = new long[16]; // exactly one slab
            for (int i = 0; i < 16; i++) {
                ptrs[i] = alloc.allocate(classId);
            }
            assertEquals(16, alloc.totalSegmentsInUse());

            // Free all
            for (long ptr : ptrs) {
                alloc.free(ptr);
            }
            assertEquals(0, alloc.totalSegmentsInUse());

            // Reallocate — should reuse freed slots, not grow a new slab
            for (int i = 0; i < 16; i++) {
                alloc.allocate(classId);
            }
            assertEquals(16, alloc.totalSegmentsInUse());
            // Should still be 1 slab's worth of memory
            assertEquals(1024, alloc.totalAllocatedBytes());
        }
    }

    @Test
    void resolveWithExplicitLength() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 4096)) {

            int classId = alloc.registerClass(128);
            long ptr = alloc.allocate(classId);

            MemorySegment full = alloc.resolve(ptr);
            MemorySegment partial = alloc.resolve(ptr, 8);
            assertEquals(128, full.byteSize());
            assertEquals(8, partial.byteSize());
        }
    }

    @Test
    void segmentSizeExceedingSlabThrows() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 256)) {
            assertThrows(IllegalArgumentException.class, () -> alloc.registerClass(512));
        }
    }

    @Test
    void differentClassesIndependent() {
        try (var arena = Arena.ofConfined();
             var alloc = new SlabAllocator(arena, 4096)) {

            int c4   = alloc.registerClass(4);
            int c16  = alloc.registerClass(16);
            int c256 = alloc.registerClass(256);

            long p4   = alloc.allocate(c4);
            long p16  = alloc.allocate(c16);
            long p256 = alloc.allocate(c256);

            // Write different patterns
            alloc.resolve(p4).set(ValueLayout.JAVA_INT, 0, 0xAAAA);
            alloc.resolve(p16).set(ValueLayout.JAVA_LONG, 0, 0xBBBBL);
            alloc.resolve(p256).set(ValueLayout.JAVA_LONG, 0, 0xCCCCL);

            // Free one class doesn't affect others
            alloc.free(p16);
            assertEquals(0xAAAA, alloc.resolve(p4).get(ValueLayout.JAVA_INT, 0));
            assertEquals(0xCCCCL, alloc.resolve(p256).get(ValueLayout.JAVA_LONG, 0));
        }
    }
}
