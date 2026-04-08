package org.taotree.internal.alloc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.taotree.TaoString;

/**
 * Append-only bump allocator for variable-length, immutable payloads.
 *
 * <p>Primary use case: overflow storage for {@code TaoString} leaves (strings longer
 * than 12 bytes). Also suitable for any immutable blob that doesn't fit a fixed slab class.
 *
 * <p>Payloads are packed contiguously within pages with no per-entry metadata. The length
 * of each payload is known by the caller (stored in the TaoString leaf's {@code len} field).
 *
 * <p>Pages are allocated from a {@link ChunkStore} (file-backed).
 *
 * <p>Deallocation is deferred: deleted payloads become dead space, reclaimed only during
 * compaction (vacuum).
 *
 * <p><b>Thread safety:</b> regular allocations use a thread-local cursor, so
 * concurrent writers do not serialize on every {@link #allocate(int)} call.
 * Threads only synchronize when they need to reserve a new backing page or map
 * an oversized payload.
 */
public final class BumpAllocator implements AutoCloseable {

    /** Default page size: 64 KB. */
    public static final int DEFAULT_PAGE_SIZE = 64 * 1024;

    private static final int MAX_PAGES = 0x00FF_FFFF + 1; // 24-bit pageId → 16M pages
    private static final int INITIAL_PAGES_CAPACITY = 8;

    private final ChunkStore chunkStore;
    private final int pageSize;

    private volatile MemorySegment[] pages;
    private volatile int[] pageStartPages;     // ChunkStore start page for each bump page
    private volatile int[] pageSizesInPages;   // number of ChunkStore pages per bump page
    private volatile int pageCount;            // number of allocated pages
    private volatile int currentPage;          // persisted continuation page (best-effort)
    private volatile int bumpOffset;           // persisted continuation offset (best-effort)
    private final AtomicLong bytesAllocated;

    // After restore we allow exactly one thread to continue from the persisted tail.
    // Other threads start with a fresh page reservation to avoid sharing the tail page.
    private volatile int restoredTailPage = -1;
    private volatile int restoredTailOffset;
    private final AtomicBoolean restoredTailAvailable = new AtomicBoolean(false);
    private final ThreadLocal<Cursor> cursors = ThreadLocal.withInitial(Cursor::new);

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /**
     * Create a file-backed bump allocator.
     *
     * @param arena      the arena controlling mapping lifetimes (retained for API compatibility)
     * @param chunkStore the chunk store providing mapped file pages
     * @param pageSize   the bump page size in bytes
     */
    public BumpAllocator(Arena arena, ChunkStore chunkStore, int pageSize) {
        if (pageSize <= 0) throw new IllegalArgumentException("pageSize must be positive");
        this.chunkStore = chunkStore;
        this.pageSize = pageSize;
        this.pages = new MemorySegment[INITIAL_PAGES_CAPACITY];
        this.pageStartPages = new int[INITIAL_PAGES_CAPACITY];
        this.pageSizesInPages = new int[INITIAL_PAGES_CAPACITY];
        this.pageCount = 0;
        this.currentPage = -1;
        this.bumpOffset = 0;
        this.bytesAllocated = new AtomicLong();
    }

    /**
     * Allocate space for a payload of the given length.
     *
     * @param length payload size in bytes (must be positive)
     * @return an {@link OverflowPtr}-encoded pointer to the allocated space
     */
    public long allocate(int length) {
        if (length <= 0) throw new IllegalArgumentException("length must be positive: " + length);

        if (length > pageSize) {
            return allocateOversized(length);
        }

        Cursor cursor = attachCursor();
        if (cursor.pageId < 0 || cursor.offset + length > pageSize) {
            reserveRegularPage(cursor);
        }

        int pageId = cursor.pageId;
        int offset = cursor.offset;
        cursor.offset += length;
        bytesAllocated.addAndGet(length);
        currentPage = pageId;
        bumpOffset = cursor.offset;
        long ptr = OverflowPtr.pack(pageId, offset);
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
        return bytesAllocated.get();
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
        var localPages = pages;
        int count = pageCount;
        for (int i = 0; i < count; i++) {
            total += localPages[i].byteSize();
        }
        return total;
    }

    @Override
    public void close() {
        // Memory is owned by the ChunkStore.
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
        int count = pageCount;
        int[] result = new int[count];
        System.arraycopy(pageStartPages, 0, result, 0, count);
        return result;
    }

    /**
     * Export page sizes for the superblock.
     * Returns an array of ChunkStore page counts (one per bump page).
     * Oversized pages will have sizes larger than the default pageSize/PAGE_SIZE.
     */
    public int[] exportPageSizes() {
        if (pageSizesInPages == null) return new int[0];
        int count = pageCount;
        int[] result = new int[count];
        System.arraycopy(pageSizesInPages, 0, result, 0, count);
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
        ensureCapacityForAppend();
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
        this.bytesAllocated.set(bytesAllocated);
        this.restoredTailPage = currentPage;
        this.restoredTailOffset = bumpOffset;
        this.restoredTailAvailable.set(currentPage >= 0);
    }

    // ---- Internals ----

    private Cursor attachCursor() {
        Cursor cursor = cursors.get();
        if (cursor.pageId < 0 && restoredTailAvailable.compareAndSet(true, false)) {
            cursor.pageId = restoredTailPage;
            cursor.offset = restoredTailOffset;
        }
        return cursor;
    }

    private synchronized void reserveRegularPage(Cursor cursor) {
        if (pageCount >= MAX_PAGES) {
            throw new OutOfMemoryError("BumpAllocator exhausted (" + MAX_PAGES + " pages)");
        }
        ensureCapacityForAppend();

        try {
            int chunkPages = pageSize / ChunkStore.PAGE_SIZE;
            int pageId = pageCount;
            int startPage = chunkStore.allocPages(chunkPages);
            pages[pageId] = chunkStore.resolve(startPage, chunkPages);
            pageStartPages[pageId] = startPage;
            pageSizesInPages[pageId] = chunkPages;
            pageCount++;
            cursor.pageId = pageId;
            // Reserve byte 0 on the very first regular page so pack(0,0) is never returned.
            cursor.offset = (pageId == 0) ? 1 : 0;
            currentPage = pageId;
            bumpOffset = cursor.offset;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to grow bump allocator", e);
        }
    }

    private synchronized long allocateOversized(int length) {
        if (pageCount >= MAX_PAGES) {
            throw new OutOfMemoryError("BumpAllocator exhausted (" + MAX_PAGES + " pages)");
        }
        ensureCapacityForAppend();

        int oversizedPageId = pageCount;

        try {
            int chunkPages = (length + ChunkStore.PAGE_SIZE - 1) / ChunkStore.PAGE_SIZE;
            int startPage = chunkStore.allocPages(chunkPages);
            pages[oversizedPageId] = chunkStore.resolve(startPage, chunkPages);
            pageStartPages[oversizedPageId] = startPage;
            pageSizesInPages[oversizedPageId] = chunkPages;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to allocate oversized bump page", e);
        }

        pageCount++;
        bytesAllocated.addAndGet(length);
        // Don't update currentPage/bumpOffset — the oversized page is a one-off
        return OverflowPtr.pack(oversizedPageId, 0);
    }

    private void ensureCapacityForAppend() {
        if (pageCount >= pages.length) {
            int newCap = Math.min(pages.length * 2, MAX_PAGES);
            MemorySegment[] newPages = new MemorySegment[newCap];
            System.arraycopy(pages, 0, newPages, 0, pageCount);
            pages = newPages;
            int[] newSp = new int[newCap];
            System.arraycopy(pageStartPages, 0, newSp, 0, pageCount);
            pageStartPages = newSp;
            int[] newSip = new int[newCap];
            System.arraycopy(pageSizesInPages, 0, newSip, 0, pageCount);
            pageSizesInPages = newSip;
        }
    }

    private static final class Cursor {
        int pageId = -1;
        int offset;
    }
}
