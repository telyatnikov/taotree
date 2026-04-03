package org.taotree.internal;


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap slab allocator for ART nodes and leaves.
 *
 * <p>Organizes memory into <em>slab classes</em>, where each class allocates fixed-size
 * segments from large contiguous {@link MemorySegment} buffers (slabs). Each slab tracks
 * occupancy with a bitmask; allocation scans for a free bit, deallocation clears it.
 *
 * <p>Pointers returned by {@link #allocate(int)} are swizzled {@link NodePtr} values
 * encoding {@code (slabClassId, slabId, offset)} — position-independent and serializable.
 *
 * <p><b>Thread safety:</b> not thread-safe on its own. Must be accessed under the
 * {@link TaoTree}'s {@code ReentrantReadWriteLock}. All trees and dictionaries sharing
 * the same slab allocator share the same lock.
 */
public final class SlabAllocator implements AutoCloseable {

    /** Default slab size: 1 MB. */
    public static final int DEFAULT_SLAB_SIZE = 1 << 20;

    private static final int MAX_SLAB_CLASSES = 256;  // 8-bit slabClassId
    private static final int MAX_SLABS_PER_CLASS = 65536;  // 16-bit slabId
    private static final int INITIAL_SLABS_CAPACITY = 4;

    private final Arena arena;
    private final int slabSize;
    private final SlabClass[] classes;
    private int classCount;

    /**
     * Create a new allocator.
     *
     * @param arena    the arena used to allocate slab memory (controls lifetime)
     * @param slabSize the size of each slab in bytes (default: 1 MB)
     */
    public SlabAllocator(Arena arena, int slabSize) {
        if (slabSize <= 0) throw new IllegalArgumentException("slabSize must be positive");
        this.arena = arena;
        this.slabSize = slabSize;
        this.classes = new SlabClass[MAX_SLAB_CLASSES];
        this.classCount = 0;
    }

    public SlabAllocator(Arena arena) {
        this(arena, DEFAULT_SLAB_SIZE);
    }

    /**
     * Register a slab class with the given segment size.
     *
     * @param segmentSize the fixed size of each segment in this class (bytes)
     * @return the slab class ID (used in {@link #allocate} and encoded in {@link NodePtr})
     * @throws IllegalStateException if all 256 class IDs are exhausted
     */
    public int registerClass(int segmentSize) {
        if (classCount >= MAX_SLAB_CLASSES) {
            throw new IllegalStateException("Maximum slab classes (" + MAX_SLAB_CLASSES + ") exceeded");
        }
        if (segmentSize <= 0) {
            throw new IllegalArgumentException("segmentSize must be positive: " + segmentSize);
        }
        int id = classCount++;
        classes[id] = new SlabClass(id, segmentSize);
        return id;
    }

    /**
     * Allocate a segment from the given slab class, stamping it as a {@link NodePtr#LEAF}.
     *
     * @param slabClassId the slab class ID (from {@link #registerClass})
     * @return a {@link NodePtr}-encoded pointer to the allocated segment
     */
    public long allocate(int slabClassId) {
        return allocate(slabClassId, NodePtr.LEAF);
    }

    /**
     * Allocate a segment from the given slab class with a specific node type tag.
     *
     * @param slabClassId the slab class ID (from {@link #registerClass})
     * @param nodeType    the {@link NodePtr} node type tag to stamp in the pointer
     * @return a {@link NodePtr}-encoded pointer to the allocated segment
     */
    public long allocate(int slabClassId, int nodeType) {
        return classes[slabClassId].allocate(nodeType);
    }

    /**
     * Free a previously allocated segment.
     *
     * @param ptr the {@link NodePtr}-encoded pointer returned by {@link #allocate}
     */
    public void free(long ptr) {
        int classId = NodePtr.slabClassId(ptr);
        classes[classId].free(ptr);
    }

    /**
     * Resolve a pointer to a writable {@link MemorySegment} slice of the full segment size.
     */
    public MemorySegment resolve(long ptr) {
        int classId = NodePtr.slabClassId(ptr);
        return classes[classId].resolve(ptr);
    }

    /**
     * Resolve a pointer to a {@link MemorySegment} slice of the given length.
     * Useful when only a prefix of the segment is needed.
     */
    public MemorySegment resolve(long ptr, int length) {
        int classId = NodePtr.slabClassId(ptr);
        return classes[classId].resolve(ptr, length);
    }

    /** Returns the segment size for the given slab class. */
    public int segmentSize(int slabClassId) {
        return classes[slabClassId].segmentSize;
    }

    /** Returns the number of registered slab classes. */
    public int classCount() {
        return classCount;
    }

    /** Total bytes allocated across all slab classes (slab-level, not segment-level). */
    public long totalAllocatedBytes() {
        long total = 0;
        for (int i = 0; i < classCount; i++) {
            total += (long) classes[i].slabCount * slabSize;
        }
        return total;
    }

    /** Total segments currently in use across all slab classes. */
    public long totalSegmentsInUse() {
        long total = 0;
        for (int i = 0; i < classCount; i++) {
            total += classes[i].segmentsInUse;
        }
        return total;
    }

    @Override
    public void close() {
        // Memory is owned by the Arena — nothing to do here.
        // The Arena's close() will release all slab memory.
    }

    // ---- Inner: per-class state ----

    private final class SlabClass {
        final int id;
        final int segmentSize;
        final int segmentsPerSlab;   // how many segments fit in one slab
        final int bitmaskLongs;      // number of longs in the bitmask per slab

        MemorySegment[] slabs;       // the slab buffers
        long[][] bitmasks;           // occupancy: 1 = free, 0 = in-use
        int slabCount;
        int segmentsInUse;

        // Allocation hint: (slabIndex, bitmask word index) to start scanning from
        int hintSlab;
        int hintWord;

        SlabClass(int id, int segmentSize) {
            this.id = id;
            this.segmentSize = segmentSize;
            this.segmentsPerSlab = slabSize / segmentSize;
            if (segmentsPerSlab <= 0) {
                throw new IllegalArgumentException(
                    "Segment size " + segmentSize + " exceeds slab size " + slabSize);
            }
            this.bitmaskLongs = (segmentsPerSlab + 63) >>> 6;
            this.slabs = new MemorySegment[INITIAL_SLABS_CAPACITY];
            this.bitmasks = new long[INITIAL_SLABS_CAPACITY][];
            this.slabCount = 0;
            this.segmentsInUse = 0;
        }

        long allocate(int nodeType) {
            // Try to find a free segment starting from the hint
            for (int s = hintSlab; s < slabCount; s++) {
                int wordStart = (s == hintSlab) ? hintWord : 0;
                int idx = findFreeInSlab(s, wordStart);
                if (idx >= 0) {
                    return markAllocated(s, idx, nodeType);
                }
            }
            // Wrap around: check slabs before the hint
            for (int s = 0; s < hintSlab && s < slabCount; s++) {
                int idx = findFreeInSlab(s, 0);
                if (idx >= 0) {
                    return markAllocated(s, idx, nodeType);
                }
            }
            // No free segment found — grow
            int newSlabId = growSlab();
            int idx = findFreeInSlab(newSlabId, 0);
            return markAllocated(newSlabId, idx, nodeType);
        }

        void free(long ptr) {
            int slabId = NodePtr.slabId(ptr);
            int offset = NodePtr.offset(ptr);
            int segIdx = offset / segmentSize;
            int word = segIdx >>> 6;
            int bit = segIdx & 63;
            bitmasks[slabId][word] |= (1L << bit);  // mark free
            segmentsInUse--;
            // Move hint back to this position for locality
            hintSlab = slabId;
            hintWord = word;
        }

        MemorySegment resolve(long ptr) {
            int slabId = NodePtr.slabId(ptr);
            int offset = NodePtr.offset(ptr);
            return slabs[slabId].asSlice(offset, segmentSize);
        }

        MemorySegment resolve(long ptr, int length) {
            int slabId = NodePtr.slabId(ptr);
            int offset = NodePtr.offset(ptr);
            return slabs[slabId].asSlice(offset, length);
        }

        /**
         * Scan bitmask for a free bit in the given slab, starting from wordStart.
         * Returns the segment index, or -1 if none found.
         */
        private int findFreeInSlab(int slabId, int wordStart) {
            long[] bm = bitmasks[slabId];
            for (int w = wordStart; w < bitmaskLongs; w++) {
                if (bm[w] != 0L) {
                    int bit = Long.numberOfTrailingZeros(bm[w]);
                    int segIdx = (w << 6) | bit;
                    if (segIdx < segmentsPerSlab) {
                        return segIdx;
                    }
                }
            }
            return -1;
        }

        private long markAllocated(int slabId, int segIdx, int nodeType) {
            int word = segIdx >>> 6;
            int bit = segIdx & 63;
            bitmasks[slabId][word] &= ~(1L << bit);  // clear = in-use
            segmentsInUse++;
            // Advance hint
            hintSlab = slabId;
            hintWord = word;
            int offset = segIdx * segmentSize;
            return NodePtr.pack(nodeType, id, slabId, offset);
        }

        private int growSlab() {
            if (slabCount >= MAX_SLABS_PER_CLASS) {
                throw new OutOfMemoryError(
                    "Slab class " + id + " exhausted (" + MAX_SLABS_PER_CLASS + " slabs)");
            }
            // Grow arrays if needed
            if (slabCount >= slabs.length) {
                int newCap = Math.min(slabs.length * 2, MAX_SLABS_PER_CLASS);
                MemorySegment[] newSlabs = new MemorySegment[newCap];
                System.arraycopy(slabs, 0, newSlabs, 0, slabCount);
                slabs = newSlabs;
                long[][] newBm = new long[newCap][];
                System.arraycopy(bitmasks, 0, newBm, 0, slabCount);
                bitmasks = newBm;
            }
            int newId = slabCount;
            slabs[newId] = arena.allocate(slabSize, 8);  // 8-byte aligned
            // Initialize bitmask: all bits set = all free
            long[] bm = new long[bitmaskLongs];
            for (int w = 0; w < bitmaskLongs; w++) {
                bm[w] = -1L;  // all bits set
            }
            // Mask off bits beyond segmentsPerSlab in the last word
            int remainder = segmentsPerSlab & 63;
            if (remainder != 0) {
                bm[bitmaskLongs - 1] = (1L << remainder) - 1;
            }
            bitmasks[newId] = bm;
            slabCount++;
            return newId;
        }
    }
}
