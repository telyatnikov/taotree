package org.taotree.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Append-only bump allocator for variable-length, immutable payloads.
 *
 * <p>Primary use case: overflow storage for {@code TaoString} leaves (strings longer
 * than 12 bytes). Also suitable for any immutable blob that doesn't fit a fixed slab class.
 *
 * <p>Payloads are packed contiguously within pages with no per-entry metadata. The length
 * of each payload is known by the caller (stored in the TaoString leaf's {@code len} field).
 *
 * <p>Supports two modes:
 * <ul>
 *   <li><b>In-memory:</b> pages are allocated from an {@link Arena}.
 *   <li><b>File-backed:</b> pages are allocated from a {@link ChunkStore}.
 * </ul>
 *
 * <p>Deallocation is deferred: deleted payloads become dead space, reclaimed only during
 * compaction (vacuum).
 *
 * <p><b>Thread safety:</b> not thread-safe on its own. Must be accessed under the
 * owning tree's {@code ReentrantReadWriteLock}. Write operations (allocate) require
 * the write lock. Read operations (resolve) require at least the read lock.
 */
public final class BumpAllocator implements AutoCloseable {

    /** Default page size: 64 KB. */
    public static final int DEFAULT_PAGE_SIZE = 64 * 1024;

    private static final int MAX_PAGES = 0x00FF_FFFF + 1; // 24-bit pageId → 16M pages
    private static final int INITIAL_PAGES_CAPACITY = 8;

    private final Arena arena;           // always set
    private final ChunkStore chunkStore; // null for in-memory mode
    private final int pageSize;

    private MemorySegment[] pages;
    private int[] pageStartPages;     // file-backed: ChunkStore start page for each bump page
    private int[] pageSizesInPages;   // file-backed: number of ChunkStore pages per bump page
    private int pageCount;            // number of allocated pages
    private int currentPage;          // index of the page currently being bumped into
    private int bumpOffset;           // next free byte in currentPage
    private long bytesAllocated;      // total payload bytes allocated

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    public BumpAllocator(Arena arena, int pageSize) {
        if (pageSize <= 0) throw new IllegalArgumentException("pageSize must be positive");
        this.arena = arena;
        this.chunkStore = null;
        this.pageSize = pageSize;
        this.pages = new MemorySegment[INITIAL_PAGES_CAPACITY];
        this.pageCount = 0;
        this.currentPage = -1;
        this.bumpOffset = 0;
        this.bytesAllocated = 0;
    }

    public BumpAllocator(Arena arena) {
        this(arena, DEFAULT_PAGE_SIZE);
    }

    /**
     * Create a file-backed bump allocator.
     *
     * @param arena      the arena controlling mapping lifetimes
     * @param chunkStore the chunk store providing mapped file pages
     * @param pageSize   the bump page size in bytes
     */
    public BumpAllocator(Arena arena, ChunkStore chunkStore, int pageSize) {
        if (pageSize <= 0) throw new IllegalArgumentException("pageSize must be positive");
        this.arena = arena;
        this.chunkStore = chunkStore;
        this.pageSize = pageSize;
        this.pages = new MemorySegment[INITIAL_PAGES_CAPACITY];
        this.pageStartPages = new int[INITIAL_PAGES_CAPACITY];
        this.pageSizesInPages = new int[INITIAL_PAGES_CAPACITY];
        this.pageCount = 0;
        this.currentPage = -1;
        this.bumpOffset = 0;
        this.bytesAllocated = 0;
    }

    /** Returns true if this allocator is file-backed. */
    public boolean isFileBacked() {
        return chunkStore != null;
    }

    /**
     * Allocate space for a payload of the given length.
     *
     * @param length payload size in bytes (must be positive)
     * @return an {@link OverflowPtr}-encoded pointer to the allocated space
     */
    public long allocate(int length) {
        if (length <= 0) throw new IllegalArgumentException("length must be positive: " + length);

        // Oversized payload: allocate a dedicated page
        if (length > pageSize) {
            return allocateOversized(length);
        }

        // Common path: bump within the current page
        if (currentPage < 0 || bumpOffset + length > pageSize) {
            growPage();
        }

        long ptr = OverflowPtr.pack(currentPage, bumpOffset);
        bumpOffset += length;
        bytesAllocated += length;
        return ptr;
    }

    /**
     * Resolve an overflow pointer to a {@link MemorySegment} slice of the given length.
     *
     * @param overflowPtr the pointer returned by {@link #allocate}
     * @param length      the payload length (same value passed to allocate)
     */
    public MemorySegment resolve(long overflowPtr, int length) {
        int pageId = OverflowPtr.pageId(overflowPtr);
        int offset = OverflowPtr.offset(overflowPtr);
        return pages[pageId].asSlice(offset, length);
    }

    /** Total payload bytes allocated (including dead space from deleted leaves). */
    public long bytesAllocated() {
        return bytesAllocated;
    }

    /** Total pages allocated. */
    public int pageCount() {
        return pageCount;
    }

    /** Current page index. */
    public int currentPage() {
        return currentPage;
    }

    /** Current bump offset within the current page. */
    public int bumpOffset() {
        return bumpOffset;
    }

    /** Page size in bytes. */
    public int pageSize() {
        return pageSize;
    }

    /** Total backing memory committed (pages × pageSize, plus oversized pages). */
    public long totalCommittedBytes() {
        long total = 0;
        for (int i = 0; i < pageCount; i++) {
            total += pages[i].byteSize();
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
     * Export page locations for the superblock.
     * Returns an array of ChunkStore start pages (one per bump page).
     * Only meaningful in file-backed mode.
     */
    public int[] exportPageLocations() {
        if (pageStartPages == null) return new int[0];
        int[] result = new int[pageCount];
        System.arraycopy(pageStartPages, 0, result, 0, pageCount);
        return result;
    }

    /**
     * Export page sizes for the superblock.
     * Returns an array of ChunkStore page counts (one per bump page).
     * Oversized pages will have sizes larger than the default pageSize/PAGE_SIZE.
     */
    public int[] exportPageSizes() {
        if (pageSizesInPages == null) return new int[0];
        int[] result = new int[pageCount];
        System.arraycopy(pageSizesInPages, 0, result, 0, pageCount);
        return result;
    }

    /**
     * Restore a bump page from persisted state.
     * Must be called in file-backed mode. Remaps the page from the ChunkStore.
     *
     * @param startPage the ChunkStore start page
     * @param sizeInPages the number of ChunkStore pages
     */
    public void restorePage(int startPage, int sizeInPages) {
        if (chunkStore == null) {
            throw new IllegalStateException("restorePage requires file-backed mode");
        }
        ensureCapacity();
        int id = pageCount;
        pages[id] = chunkStore.resolve(startPage, sizeInPages);
        pageStartPages[id] = startPage;
        pageSizesInPages[id] = sizeInPages;
        pageCount++;
    }

    /**
     * Restore the bump allocator state after remapping pages.
     *
     * @param currentPage   the current bump page index
     * @param bumpOffset    the bump offset within the current page
     * @param bytesAllocated total bytes allocated
     */
    public void restoreState(int currentPage, int bumpOffset, long bytesAllocated) {
        this.currentPage = currentPage;
        this.bumpOffset = bumpOffset;
        this.bytesAllocated = bytesAllocated;
    }

    // ---- Internals ----

    private void growPage() {
        if (pageCount >= MAX_PAGES) {
            throw new OutOfMemoryError("BumpAllocator exhausted (" + MAX_PAGES + " pages)");
        }
        ensureCapacity();

        if (chunkStore != null) {
            try {
                int chunkPages = pageSize / ChunkStore.PAGE_SIZE;
                int startPage = chunkStore.allocPages(chunkPages);
                pages[pageCount] = chunkStore.resolve(startPage, chunkPages);
                pageStartPages[pageCount] = startPage;
                pageSizesInPages[pageCount] = chunkPages;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to grow bump allocator", e);
            }
        } else {
            pages[pageCount] = arena.allocate(pageSize, 1);
        }

        currentPage = pageCount;
        pageCount++;
        // Reserve byte 0 on the very first page so that pack(0,0) is never returned
        // (pack(0,0) == 0 == EMPTY_PTR). Subsequent pages start at offset 0.
        bumpOffset = (currentPage == 0) ? 1 : 0;
    }

    private long allocateOversized(int length) {
        if (pageCount >= MAX_PAGES) {
            throw new OutOfMemoryError("BumpAllocator exhausted (" + MAX_PAGES + " pages)");
        }
        ensureCapacity();

        int oversizedPageId = pageCount;

        if (chunkStore != null) {
            try {
                int chunkPages = (length + ChunkStore.PAGE_SIZE - 1) / ChunkStore.PAGE_SIZE;
                int startPage = chunkStore.allocPages(chunkPages);
                pages[oversizedPageId] = chunkStore.resolve(startPage, chunkPages);
                pageStartPages[oversizedPageId] = startPage;
                pageSizesInPages[oversizedPageId] = chunkPages;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to allocate oversized bump page", e);
            }
        } else {
            pages[oversizedPageId] = arena.allocate(length, 1);
        }

        pageCount++;
        bytesAllocated += length;
        // Don't update currentPage/bumpOffset — the oversized page is a one-off
        return OverflowPtr.pack(oversizedPageId, 0);
    }

    private void ensureCapacity() {
        if (pageCount >= pages.length) {
            int newCap = Math.min(pages.length * 2, MAX_PAGES);
            MemorySegment[] newPages = new MemorySegment[newCap];
            System.arraycopy(pages, 0, newPages, 0, pageCount);
            pages = newPages;
            if (pageStartPages != null) {
                int[] newSp = new int[newCap];
                System.arraycopy(pageStartPages, 0, newSp, 0, pageCount);
                pageStartPages = newSp;
                int[] newSip = new int[newCap];
                System.arraycopy(pageSizesInPages, 0, newSip, 0, pageCount);
                pageSizesInPages = newSip;
            }
        }
    }
}
