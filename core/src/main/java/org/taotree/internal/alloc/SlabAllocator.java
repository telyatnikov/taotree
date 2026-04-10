package org.taotree.internal.alloc;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.persist.Superblock;

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
 * <p>Slabs and bitmasks are allocated as pages from a {@link ChunkStore}.
 * Writes are immediately visible in the mapped file.
 *
 * <p><b>Thread safety:</b> {@link #allocate} and {@link #free} are lock-free,
 * using {@link VarHandle#compareAndExchange} on off-heap bitmask words. Multiple
 * threads can allocate and free segments from the same slab class concurrently
 * without any lock acquisition. Only the rare slab-growth path serializes via a
 * per-class {@link ReentrantLock}. {@link #resolve} methods are lock-free.
 */
public final class SlabAllocator implements AutoCloseable {

    /** Default slab size: 1 MB. */
    public static final int DEFAULT_SLAB_SIZE = 1 << 20;

    private static final int MAX_SLAB_CLASSES = 256;  // 8-bit slabClassId
    private static final int MAX_SLABS_PER_CLASS = 65536;  // 16-bit slabId
    private static final int INITIAL_SLABS_CAPACITY = 4;

    /** VarHandle for atomic CAS on bitmask longs in off-heap MemorySegment. */
    private static final VarHandle VH_LONG = ValueLayout.JAVA_LONG.varHandle();

    private final ChunkStore chunkStore;
    private final int slabSize;
    private final SlabClass[] classes;
    private int classCount;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /**
     * Create a new file-backed allocator.
     *
     * @param arena      the arena controlling mapping lifetimes (retained for API compatibility)
     * @param chunkStore the chunk store providing mapped file pages
     * @param slabSize   the size of each slab in bytes
     */
    public SlabAllocator(Arena arena, ChunkStore chunkStore, int slabSize) {
        if (slabSize <= 0) throw new IllegalArgumentException("slabSize must be positive");
        this.chunkStore = chunkStore;
        this.slabSize = slabSize;
        this.classes = new SlabClass[MAX_SLAB_CLASSES];
        this.classCount = 0;
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
     * <p>Thread-safe: lock-free CAS on bitmask words.
     *
     * @param slabClassId the slab class ID (from {@link #registerClass})
     * @return a {@link NodePtr}-encoded pointer to the allocated segment
     */
    public long allocate(int slabClassId) {
        return classes[slabClassId].allocate(NodePtr.LEAF);
    }

    /**
     * Allocate a segment from the given slab class with a specific node type tag.
     *
     * <p>Thread-safe: lock-free CAS on bitmask words.
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
     * <p>Thread-safe: lock-free CAS on bitmask words.
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
     * Return the backing slab segment for a pointer without creating a slice.
     */
    public MemorySegment backingSegment(long ptr) {
        int classId = NodePtr.slabClassId(ptr);
        return classes[classId].backingSegment(ptr);
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

    /** Returns the slab size in bytes. */
    public int slabSize() {
        return slabSize;
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
            total += classes[i].segmentsInUse.get();
        }
        return total;
    }

    @Override
    public void close() {
        // Memory is owned by the Arena (in-memory) or ChunkStore (file-backed).
    }

    // -----------------------------------------------------------------------
    // Persistence support
    // -----------------------------------------------------------------------

    /**
     * Export the state of a slab class for persistence.
     * Only meaningful in file-backed mode.
     */
    public Superblock.SlabClassDescriptor exportClass(int classId) {
        var cls = classes[classId];
        var desc = new Superblock.SlabClassDescriptor();
        desc.segmentSize = cls.segmentSize;
        desc.slabCount = cls.slabCount;
        desc.segmentsInUse = cls.segmentsInUse.get();
        desc.dataStartPages = new int[cls.slabCount];
        desc.bitmaskStartPages = new int[cls.slabCount];
        desc.bitmaskPageCounts = new int[cls.slabCount];
        if (cls.dataStartPages != null) {
            System.arraycopy(cls.dataStartPages, 0, desc.dataStartPages, 0, cls.slabCount);
            System.arraycopy(cls.bitmaskStartPages, 0, desc.bitmaskStartPages, 0, cls.slabCount);
            System.arraycopy(cls.bitmaskPageCounts, 0, desc.bitmaskPageCounts, 0, cls.slabCount);
        }
        return desc;
    }

    /**
     * Restore a slab class from persisted state.
     * Must be called in file-backed mode. Registers the class and remaps its slabs.
     *
     * @param desc the persisted class descriptor
     * @return the slab class ID
     */
    public int restoreClass(Superblock.SlabClassDescriptor desc) {
        int id = registerClass(desc.segmentSize);
        var cls = classes[id];

        // Grow arrays to fit
        if (desc.slabCount > cls.slabs.length) {
            cls.slabs = new MemorySegment[desc.slabCount];
            cls.bitmaskSegs = new MemorySegment[desc.slabCount];
            cls.dataStartPages = new int[desc.slabCount];
            cls.bitmaskStartPages = new int[desc.slabCount];
            cls.bitmaskPageCounts = new int[desc.slabCount];
        }

        // Remap each slab
        int slabPages = slabSize / ChunkStore.PAGE_SIZE;
        for (int s = 0; s < desc.slabCount; s++) {
            cls.dataStartPages[s] = desc.dataStartPages[s];
            cls.bitmaskStartPages[s] = desc.bitmaskStartPages[s];
            cls.bitmaskPageCounts[s] = desc.bitmaskPageCounts[s];
            cls.slabs[s] = chunkStore.resolve(desc.dataStartPages[s], slabPages);
            cls.bitmaskSegs[s] = chunkStore.resolve(desc.bitmaskStartPages[s], desc.bitmaskPageCounts[s]);
        }
        cls.slabCount = desc.slabCount;
        cls.segmentsInUse.set(desc.segmentsInUse);

        return id;
    }

    // ---- Inner: per-class state (lock-free allocation via CAS on bitmask) ----

    private final class SlabClass {
        final int id;
        final int segmentSize;
        final int segmentsPerSlab;   // how many segments fit in one slab
        final int bitmaskLongs;      // number of longs in the bitmask per slab

        // Volatile for visibility: scanners read slabCount first (acquire),
        // which synchronizes with the growth path's volatile write (release),
        // ensuring populated array entries are visible.
        volatile MemorySegment[] slabs;
        volatile MemorySegment[] bitmaskSegs;
        volatile int slabCount;
        final AtomicInteger segmentsInUse = new AtomicInteger();

        // Slab growth serialization (only held during rare growth operations)
        private final ReentrantLock growLock = new ReentrantLock();

        // Persistence metadata (only accessed during sync/compact under write lock)
        int[] dataStartPages;
        int[] bitmaskStartPages;
        int[] bitmaskPageCounts;

        // Best-effort allocation hints (benign races — stale hint just means
        // a few extra words scanned, no correctness impact)
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
            this.bitmaskSegs = new MemorySegment[INITIAL_SLABS_CAPACITY];
            this.slabCount = 0;

            this.dataStartPages = new int[INITIAL_SLABS_CAPACITY];
            this.bitmaskStartPages = new int[INITIAL_SLABS_CAPACITY];
            this.bitmaskPageCounts = new int[INITIAL_SLABS_CAPACITY];
        }

        /**
         * Lock-free allocation. Scans bitmask words from the hint position,
         * atomically claiming a free bit via CAS. Falls back to growLock only
         * when all existing slabs are full.
         */
        long allocate(int nodeType) {
            // Phase 1: CAS-based scan of existing slabs
            long ptr = scanAndCas(nodeType);
            if (ptr != 0) return ptr;

            // Phase 2: grow under lock, then CAS from new slab
            growLock.lock();
            try {
                // Re-scan: another grower may have added capacity
                ptr = scanAndCas(nodeType);
                if (ptr != 0) return ptr;

                // Grow and allocate from the fresh slab
                int newSlabId = growSlabImpl();
                ptr = casInSlab(newSlabId, 0, nodeType);
                if (ptr != 0) return ptr;

                // Should not happen — fresh slab is completely free
                throw new IllegalStateException(
                    "Failed to allocate from fresh slab (class " + id + ")");
            } finally {
                growLock.unlock();
            }
        }

        /**
         * Lock-free free. Atomically sets the free bit in the bitmask via CAS.
         */
        void free(long ptr) {
            int slabId = NodePtr.slabId(ptr);
            int offset = NodePtr.offset(ptr);
            int segIdx = offset / segmentSize;
            int word = segIdx >>> 6;
            int bit = segIdx & 63;

            MemorySegment bm = bitmaskSegs[slabId];
            long byteOff = (long) word * 8;
            long mask = 1L << bit;

            // Atomic set-bit via CAS loop
            long prev;
            do {
                prev = (long) VH_LONG.getVolatile(bm, byteOff);
            } while ((long) VH_LONG.compareAndExchange(bm, byteOff, prev, prev | mask) != prev);

            segmentsInUse.decrementAndGet();
            hintSlab = slabId;
            hintWord = word;
        }

        MemorySegment resolve(long ptr) {
            int slabId = NodePtr.slabId(ptr);
            int offset = NodePtr.offset(ptr);
            return slabs[slabId].asSlice(offset, segmentSize);
        }

        MemorySegment backingSegment(long ptr) {
            return slabs[NodePtr.slabId(ptr)];
        }

        MemorySegment resolve(long ptr, int length) {
            int slabId = NodePtr.slabId(ptr);
            int offset = NodePtr.offset(ptr);
            return slabs[slabId].asSlice(offset, length);
        }

        /**
         * CAS-based scan across all slabs starting from the hint.
         * Returns 0 if no free segment found.
         */
        private long scanAndCas(int nodeType) {
            int sc = slabCount; // volatile read — acquire fence
            if (sc == 0) return 0;

            int hs = hintSlab;
            if (hs >= sc) hs = 0;
            int hw = hintWord;

            // Scan from hint forward
            for (int s = hs; s < sc; s++) {
                long ptr = casInSlab(s, (s == hs) ? hw : 0, nodeType);
                if (ptr != 0) return ptr;
            }
            // Wrap around: check slabs before the hint
            for (int s = 0; s < hs; s++) {
                long ptr = casInSlab(s, 0, nodeType);
                if (ptr != 0) return ptr;
            }
            return 0;
        }

        /**
         * CAS-based allocation within a single slab. Scans bitmask words for a
         * free bit (non-zero word) and atomically claims it via compareAndExchange.
         *
         * <p>On CAS failure (another thread claimed the same bit), retries with the
         * witness value — no re-read needed, and the loop naturally finds the next
         * free bit in the same word.
         *
         * @return a NodePtr-encoded pointer, or 0 if no free segment in this slab
         */
        private long casInSlab(int slabId, int wordStart, int nodeType) {
            MemorySegment bm = bitmaskSegs[slabId]; // volatile read of reference
            for (int w = wordStart; w < bitmaskLongs; w++) {
                long byteOff = (long) w * 8;
                long val = (long) VH_LONG.getVolatile(bm, byteOff);
                while (val != 0L) {
                    int bit = Long.numberOfTrailingZeros(val);
                    int segIdx = (w << 6) | bit;
                    if (segIdx >= segmentsPerSlab) break;

                    long newVal = val & ~(1L << bit);
                    long witness = (long) VH_LONG.compareAndExchange(bm, byteOff, val, newVal);
                    if (witness == val) {
                        // CAS succeeded — segment allocated
                        segmentsInUse.incrementAndGet();
                        hintSlab = slabId;
                        hintWord = w;
                        return NodePtr.pack(nodeType, id, slabId, segIdx * segmentSize);
                    }
                    // CAS failed — retry with witness as the new current value
                    val = witness;
                }
            }
            return 0;
        }

        /**
         * Grow this slab class by one slab. Called under {@link #growLock}.
         * Publishes new arrays (volatile write) BEFORE incrementing slabCount
         * (volatile write) to ensure scanners see populated entries.
         */
        private int growSlabImpl() {
            int newId = slabCount; // volatile read (we're under growLock, but be explicit)
            if (newId >= MAX_SLABS_PER_CLASS) {
                throw new OutOfMemoryError(
                    "Slab class " + id + " exhausted (" + MAX_SLABS_PER_CLASS + " slabs)");
            }

            int bitmaskBytes = bitmaskLongs * 8;
            int slabPages = slabSize / ChunkStore.PAGE_SIZE;
            int bmPages = (bitmaskBytes + ChunkStore.PAGE_SIZE - 1) / ChunkStore.PAGE_SIZE;

            // Allocate pages from ChunkStore
            int dataPage, bmPage;
            MemorySegment dataSeg, bmSeg;
            try {
                dataPage = chunkStore.allocPages(slabPages);
                bmPage = chunkStore.allocPages(bmPages);
                dataSeg = chunkStore.resolve(dataPage, slabPages);
                bmSeg = chunkStore.resolve(bmPage, bmPages);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to grow slab class " + id, e);
            }

            // Initialize bitmask: all bits set = all free
            for (int w = 0; w < bitmaskLongs; w++) {
                bmSeg.set(ValueLayout.JAVA_LONG, (long) w * 8, -1L);
            }
            // Mask off bits beyond segmentsPerSlab in the last word
            int remainder = segmentsPerSlab & 63;
            if (remainder != 0) {
                bmSeg.set(ValueLayout.JAVA_LONG, (long) (bitmaskLongs - 1) * 8, (1L << remainder) - 1);
            }

            // Grow arrays if needed, then populate and publish
            if (newId >= slabs.length) {
                int newCap = Math.min(slabs.length * 2, MAX_SLABS_PER_CLASS);

                MemorySegment[] newSlabs = new MemorySegment[newCap];
                System.arraycopy(slabs, 0, newSlabs, 0, newId);
                newSlabs[newId] = dataSeg;

                MemorySegment[] newBm = new MemorySegment[newCap];
                System.arraycopy(bitmaskSegs, 0, newBm, 0, newId);
                newBm[newId] = bmSeg;

                int[] newDsp = new int[newCap];
                System.arraycopy(dataStartPages, 0, newDsp, 0, newId);
                newDsp[newId] = dataPage;
                dataStartPages = newDsp;

                int[] newBsp = new int[newCap];
                System.arraycopy(bitmaskStartPages, 0, newBsp, 0, newId);
                newBsp[newId] = bmPage;
                bitmaskStartPages = newBsp;

                int[] newBpc = new int[newCap];
                System.arraycopy(bitmaskPageCounts, 0, newBpc, 0, newId);
                newBpc[newId] = bmPages;
                bitmaskPageCounts = newBpc;

                // Volatile publish arrays BEFORE slabCount
                slabs = newSlabs;
                bitmaskSegs = newBm;
            } else {
                // Arrays large enough — populate directly
                slabs[newId] = dataSeg;
                bitmaskSegs[newId] = bmSeg;
                dataStartPages[newId] = dataPage;
                bitmaskStartPages[newId] = bmPage;
                bitmaskPageCounts[newId] = bmPages;
            }

            // Volatile publish count LAST — scanners reading this see all prior stores
            slabCount = newId + 1;
            hintSlab = newId;
            hintWord = 0;
            return newId;
        }
    }
}
