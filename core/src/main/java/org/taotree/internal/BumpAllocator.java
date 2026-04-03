package org.taotree.internal;

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
 * <p>Deallocation is deferred: deleted payloads become dead space, reclaimed only during
 * compaction (vacuum).
 *
 * <p><b>Thread safety:</b> not thread-safe on its own. Must be accessed under the
 * {@code TaoTree}'s {@code ReentrantReadWriteLock}. Write operations (allocate) require
 * the write lock. Read operations (resolve) require at least the read lock.
 */
public final class BumpAllocator implements AutoCloseable {

    /** Default page size: 64 KB. */
    public static final int DEFAULT_PAGE_SIZE = 64 * 1024;

    private static final int MAX_PAGES = 0x00FF_FFFF + 1; // 24-bit pageId → 16M pages
    private static final int INITIAL_PAGES_CAPACITY = 8;

    private final Arena arena;
    private final int pageSize;

    private MemorySegment[] pages;
    private int pageCount;       // number of allocated pages
    private int currentPage;     // index of the page currently being bumped into
    private int bumpOffset;      // next free byte in currentPage
    private long bytesAllocated; // total payload bytes allocated (not including dead space tracking)

    public BumpAllocator(Arena arena, int pageSize) {
        if (pageSize <= 0) throw new IllegalArgumentException("pageSize must be positive");
        this.arena = arena;
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
        // Memory is owned by the Arena — nothing to do here.
    }

    // ---- Internals ----

    private void growPage() {
        if (pageCount >= MAX_PAGES) {
            throw new OutOfMemoryError("BumpAllocator exhausted (" + MAX_PAGES + " pages)");
        }
        if (pageCount >= pages.length) {
            int newCap = Math.min(pages.length * 2, MAX_PAGES);
            MemorySegment[] newPages = new MemorySegment[newCap];
            System.arraycopy(pages, 0, newPages, 0, pageCount);
            pages = newPages;
        }
        pages[pageCount] = arena.allocate(pageSize, 1);
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
        if (pageCount >= pages.length) {
            int newCap = Math.min(pages.length * 2, MAX_PAGES);
            MemorySegment[] newPages = new MemorySegment[newCap];
            System.arraycopy(pages, 0, newPages, 0, pageCount);
            pages = newPages;
        }
        // Oversized page: exactly the payload size, NOT the current bump page
        int oversizedPageId = pageCount;
        pages[oversizedPageId] = arena.allocate(length, 1);
        pageCount++;
        bytesAllocated += length;
        // Don't update currentPage/bumpOffset — the oversized page is a one-off
        return OverflowPtr.pack(oversizedPageId, 0);
    }
}
