package org.taotree.internal.alloc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import org.taotree.internal.art.NodePtr;

/**
 * Thread-local bump allocator for copy-on-write node allocation.
 *
 * <p>Each writer thread obtains a private {@code WriterArena} to avoid contending
 * on the shared {@link SlabAllocator}'s bitmap scans. The arena reserves pages in
 * bulk from a {@link ChunkStore} and bumps a pointer for each allocation. This
 * produces contiguous page ranges per writer per scope, which is critical for the
 * shadow-paging durability model (one contiguous {@code force()} call per scope).
 *
 * <p>Arena-allocated nodes are encoded as standard {@link NodePtr} values with a
 * sentinel {@code slabId} of {@code 0xFFFF}. The 32-bit offset field packs the
 * ChunkStore page number (bits 31-12) and byte offset within the page (bits 11-0):
 * <pre>
 *   slabId  = 0xFFFF                          (arena sentinel)
 *   offset  = (page &lt;&lt; 12) | (offsetInPage &amp; 0xFFF)
 * </pre>
 *
 * <p>Resolution uses {@link ChunkStore#resolveBytes} to map the packed offset back
 * to a {@link MemorySegment}. With 20 bits for the page number, the addressable
 * range is 2^20 pages = 4 GB of file space.
 *
 * <p><b>Thread safety:</b> a {@code WriterArena} is used by exactly one writer
 * thread at a time. No internal synchronization. The only external synchronization
 * point is {@link ChunkStore#allocPages}, which is serialized by ChunkStore
 * internally.
 */
public final class WriterArena {

    /** Sentinel slabId marking arena-allocated nodes. */
    public static final int ARENA_SLAB_ID = 0xFFFF;

    private static final int PAGE_SIZE = ChunkStore.PAGE_SIZE;
    private static final int INITIAL_BATCH_PAGES = 16;  // 64 KB
    private static final int MAX_BATCH_PAGES = 256;     // 1 MB

    private final ChunkStore chunkStore;

    // Current page batch from ChunkStore
    private int batchStart;    // first page of current batch
    private int batchEnd;      // exclusive end of current batch
    private int currentPage;   // page being allocated from
    private int offsetInPage;  // byte offset within currentPage
    private int batchPages = INITIAL_BATCH_PAGES;

    // Scope tracking
    private int scopeStartPage;
    private int allocCount;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    public WriterArena(ChunkStore chunkStore) {
        this.chunkStore = chunkStore;
        this.batchStart = -1;
        this.batchEnd = -1;
        this.currentPage = -1;
        this.offsetInPage = PAGE_SIZE; // force reservation on first alloc
        this.scopeStartPage = -1;
    }

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    /** Result of an arena allocation: a writable segment and the encoded {@link NodePtr}. */
    public record AllocResult(MemorySegment segment, long nodePtr) {}

    /**
     * Bump-allocate space for a node.
     *
     * <p>The size is 8-byte aligned. If the current page cannot fit the aligned
     * size, the allocator advances to the next page (reserving a new batch from
     * the {@link ChunkStore} if necessary).
     *
     * @param size        required size in bytes (will be 8-byte aligned)
     * @param nodeType    {@link NodePtr} node type tag
     * @param slabClassId slab class ID (preserved in the pointer for type resolution)
     * @return the allocation result containing the writable segment and encoded pointer
     * @throws IllegalArgumentException if the aligned size exceeds {@link ChunkStore#PAGE_SIZE}
     */
    public AllocResult alloc(int size, int nodeType, int slabClassId) {
        int aligned = (size + 7) & ~7;
        if (aligned > PAGE_SIZE) {
            throw new IllegalArgumentException(
                "Allocation size " + aligned + " exceeds page size " + PAGE_SIZE);
        }

        if (offsetInPage + aligned > PAGE_SIZE) {
            advancePage();
        }

        // Capture scope start on first allocation in the scope
        if (allocCount == 0) {
            scopeStartPage = currentPage;
        }

        int page = currentPage;
        int off = offsetInPage;
        offsetInPage += aligned;
        allocCount++;

        MemorySegment seg = chunkStore.resolveBytes(page, off, aligned);
        long ptr = encodeArenaPtr(nodeType, slabClassId, page, off);
        return new AllocResult(seg, ptr);
    }

    // -----------------------------------------------------------------------
    // Scope lifecycle
    // -----------------------------------------------------------------------

    /**
     * Mark the start of a new write scope.
     *
     * <p>Records the current page position. The actual {@link #scopeStartPage()}
     * is finalized on the first allocation (since a page reservation may be
     * needed).
     */
    public void beginScope() {
        scopeStartPage = currentPage;
        allocCount = 0;
    }

    /**
     * First page used since {@link #beginScope()}.
     * Only meaningful after at least one {@link #alloc} in the scope.
     */
    public int scopeStartPage() {
        return scopeStartPage;
    }

    /**
     * Current high-water page (exclusive).
     * Together with {@link #scopeStartPage()}, defines the contiguous page range
     * that must be {@code force()}'d for durability.
     */
    public int scopeEndPage() {
        return currentPage >= 0 ? currentPage + 1 : scopeStartPage;
    }

    /** Number of allocations since {@link #beginScope()}. */
    public int allocCount() {
        return allocCount;
    }

    /**
     * End the current scope and reset scope counters.
     *
     * <p>Pages are <b>not</b> freed — they contain live nodes now reachable from
     * the published tree. The compactor will reclaim them later. The arena retains
     * its current page position and can be reused for the next scope.
     */
    public void endScope() {
        scopeStartPage = currentPage;
        allocCount = 0;
    }

    // -----------------------------------------------------------------------
    // Arena NodePtr encoding / decoding
    // -----------------------------------------------------------------------

    /**
     * Encode an arena allocation as a {@link NodePtr}.
     *
     * <p>Uses {@link #ARENA_SLAB_ID} as the slabId sentinel. The 32-bit offset
     * field packs the page number (bits 31-12) and byte offset (bits 11-0).
     */
    static long encodeArenaPtr(int nodeType, int slabClassId, int page, int offsetInPage) {
        int packed = (page << 12) | (offsetInPage & 0xFFF);
        return NodePtr.pack(nodeType, slabClassId, ARENA_SLAB_ID, packed);
    }

    /** Test whether a {@link NodePtr} was allocated by an arena. */
    public static boolean isArenaAllocated(long ptr) {
        return NodePtr.slabId(ptr) == ARENA_SLAB_ID;
    }

    /**
     * Resolve an arena-allocated {@link NodePtr} to a {@link MemorySegment}.
     *
     * @param ptr  the arena-encoded pointer
     * @param size the segment size in bytes
     */
    public MemorySegment resolve(long ptr, int size) {
        return resolve(chunkStore, ptr, size);
    }

    /**
     * Resolve an arena-allocated {@link NodePtr} using any {@link ChunkStore}.
     * Static variant for use by readers outside the writer thread.
     *
     * @param cs   the chunk store backing this arena's pages
     * @param ptr  the arena-encoded pointer
     * @param size the segment size in bytes
     */
    public static MemorySegment resolve(ChunkStore cs, long ptr, int size) {
        int packed = NodePtr.offset(ptr);
        int page = packed >>> 12;
        int off = packed & 0xFFF;
        return cs.resolveBytes(page, off, size);
    }

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    private void advancePage() {
        if (currentPage >= 0) {
            currentPage++;
        }
        if (currentPage < 0 || currentPage >= batchEnd) {
            reserveMorePages();
        }
        offsetInPage = 0;
    }

    /**
     * Reserve a new batch of pages from the {@link ChunkStore}.
     * This is the only lock / IO point in the arena hot path.
     * Batch size doubles on each call (up to {@link #MAX_BATCH_PAGES}).
     */
    private void reserveMorePages() {
        try {
            batchStart = chunkStore.allocPages(batchPages);
            batchEnd = batchStart + batchPages;
            currentPage = batchStart;
            if (batchPages < MAX_BATCH_PAGES) {
                batchPages = Math.min(batchPages * 2, MAX_BATCH_PAGES);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to reserve arena pages", e);
        }
    }
}
