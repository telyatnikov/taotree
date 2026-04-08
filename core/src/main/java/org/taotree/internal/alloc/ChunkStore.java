package org.taotree.internal.alloc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.taotree.internal.persist.Superblock;

/**
 * Manages a single file with chunked memory-mapped windows.
 *
 * <p>The file is divided into fixed-size 4 KB pages. Pages are allocated sequentially
 * within the file. The file grows in large "chunk" increments (default 64 MB), and each
 * chunk is mapped as a single {@link MemorySegment} via {@link FileChannel#map}.
 *
 * <p>This keeps the number of kernel VMAs small (one per chunk, ~tens even for large
 * databases) while allowing the file to grow incrementally.
 *
 * <p>Callers request runs of consecutive pages via {@link #allocPages(int)}. The returned
 * pages are guaranteed to be contiguous within a single chunk mapping, so the caller can
 * slice them as a single {@link MemorySegment}.
 *
 * <p><b>Thread safety:</b> {@link #allocPages} uses an atomic fast path for runs that
 * fit in the currently mapped file region and do not cross a chunk boundary. Only the
 * slow path (chunk-boundary skip or file growth) synchronizes. Resolution methods
 * ({@link #resolve}, {@link #resolvePage}, {@link #resolveBytes}) are safe for concurrent
 * reads once the chunk is mapped. {@link #sync()}, {@link #syncDirty()}, and
 * {@link #close()} must be called under external serialization (e.g., the tree's
 * write lock or commit lock).
 */
public final class ChunkStore implements AutoCloseable {

    /** Default page size: 4 KB (matches OS page size). */
    public static final int PAGE_SIZE = 4096;

    /** Default chunk size: 64 MB (16384 pages per chunk). */
    public static final long DEFAULT_CHUNK_SIZE = 64L * 1024 * 1024;

    /** Number of reserved pages for v2 checkpoint slots (pages 0-7, 32 KB: two 16 KB slots). */
    public static final int V2_RESERVED_PAGES = 8;

    private static final int MAX_CHUNKS = 16384; // 64 MB × 16384 = 1 TB max

    private final Path path;
    private final FileChannel channel;
    private final Arena arena;
    private final long chunkSize;
    private final int pagesPerChunk;
    private final boolean preallocate;

    private volatile MemorySegment[] chunks;   // mapped chunk windows
    private volatile int chunkCount;           // number of mapped chunks
    private volatile int totalPages;           // total committed pages (file size = totalPages × PAGE_SIZE)
    private final AtomicInteger nextPage;      // next page to allocate (bump pointer)

    // Dirty tracking: which chunks have been written to since the last syncDirty()
    private final Set<Integer> dirtyChunks = ConcurrentHashMap.newKeySet();

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    private ChunkStore(Path path, FileChannel channel, Arena arena,
                       long chunkSize, boolean preallocate) {
        this.path = path;
        this.channel = channel;
        this.arena = arena;
        this.chunkSize = chunkSize;
        this.pagesPerChunk = (int) (chunkSize / PAGE_SIZE);
        this.preallocate = preallocate;
        this.chunks = new MemorySegment[8];
        this.chunkCount = 0;
        this.totalPages = 0;
        this.nextPage = new AtomicInteger();
    }

    /**
     * Create a new store file. The file must not already exist.
     *
     * @param path        path to the store file
     * @param arena       arena controlling the lifetime of mapped segments
     * @param chunkSize   chunk size in bytes (must be a multiple of PAGE_SIZE)
     * @param preallocate if true, use OS-specific preallocation to reserve physical blocks
     */
    public static ChunkStore create(Path path, Arena arena, long chunkSize,
                                    boolean preallocate) throws IOException {
        validateChunkSize(chunkSize);
        var channel = FileChannel.open(path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        var store = new ChunkStore(path, channel, arena, chunkSize, preallocate);
        // Reserve pages 0-1 for the superblock (8 KB)
        store.growFile(2);
        store.nextPage.set(2);
        return store;
    }

    /** Create a new store file with the default chunk size (64 MB) and preallocation enabled. */
    public static ChunkStore create(Path path, Arena arena) throws IOException {
        return create(path, arena, DEFAULT_CHUNK_SIZE, Preallocator.isSupported());
    }

    /**
     * Create a new v2 store file with 4 reserved pages for two checkpoint slots.
     *
     * @param path        path to the store file
     * @param arena       arena controlling the lifetime of mapped segments
     * @param chunkSize   chunk size in bytes (must be a multiple of PAGE_SIZE)
     * @param preallocate if true, use OS-specific preallocation to reserve physical blocks
     */
    public static ChunkStore createV2(Path path, Arena arena, long chunkSize,
                                       boolean preallocate) throws IOException {
        validateChunkSize(chunkSize);
        var channel = FileChannel.open(path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        var store = new ChunkStore(path, channel, arena, chunkSize, preallocate);
        store.growFile(V2_RESERVED_PAGES);
        store.nextPage.set(V2_RESERVED_PAGES);
        return store;
    }

    /** Create a new v2 store file with the default chunk size (64 MB) and preallocation enabled. */
    public static ChunkStore createV2(Path path, Arena arena) throws IOException {
        return createV2(path, arena, DEFAULT_CHUNK_SIZE, Preallocator.isSupported());
    }

    /**
     * Open an existing store file for read-write access.
     *
     * @param path      path to the existing store file
     * @param arena     arena controlling the lifetime of mapped segments
     * @param chunkSize chunk size in bytes (must match the chunk size used at creation)
     * @param totalPages total committed pages (from persisted metadata)
     * @param nextPage  the next page to allocate (from persisted metadata)
     */
    public static ChunkStore open(Path path, Arena arena, long chunkSize,
                                  int totalPages, int nextPage) throws IOException {
        validateChunkSize(chunkSize);
        if (!Files.exists(path)) {
            throw new IOException("Store file does not exist: " + path);
        }
        var channel = FileChannel.open(path,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        var store = new ChunkStore(path, channel, arena, chunkSize, Preallocator.isSupported());
        store.totalPages = totalPages;
        store.nextPage.set(nextPage);
        // Map all existing chunks
        int chunksNeeded = (totalPages + store.pagesPerChunk - 1) / store.pagesPerChunk;
        for (int i = 0; i < chunksNeeded; i++) {
            store.mapChunk(i);
        }
        return store;
    }

    // -----------------------------------------------------------------------
    // Page allocation
    // -----------------------------------------------------------------------

    /**
     * Allocate a contiguous run of pages.
     *
     * <p>The returned pages are guaranteed to reside within a single chunk mapping,
     * so the caller can obtain a contiguous {@link MemorySegment} via {@link #resolve}.
     *
     * @param pageCount number of consecutive pages to allocate
     * @return the global page number of the first page in the run
     * @throws IOException if the file cannot be grown
     */
    public int allocPages(int pageCount) throws IOException {
        if (pageCount <= 0) throw new IllegalArgumentException("pageCount must be positive");
        if (pageCount > pagesPerChunk) {
            throw new IllegalArgumentException(
                "Cannot allocate " + pageCount + " pages; exceeds chunk capacity " + pagesPerChunk);
        }
        while (true) {
            int startPage = nextPage.get();
            int offsetInChunk = startPage % pagesPerChunk;
            int endPage = startPage + pageCount;
            if (offsetInChunk + pageCount > pagesPerChunk || endPage > totalPages) {
                return allocPagesSlow(pageCount);
            }
            if (nextPage.compareAndSet(startPage, endPage)) {
                dirtyChunks.add(startPage / pagesPerChunk);
                return startPage;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Page resolution
    // -----------------------------------------------------------------------

    /**
     * Resolve a run of pages to a {@link MemorySegment}.
     *
     * <p>The pages must reside within a single chunk (guaranteed if obtained via
     * {@link #allocPages}).
     *
     * @param startPage global page number of the first page
     * @param pageCount number of consecutive pages
     * @return a memory segment covering the requested pages
     */
    public MemorySegment resolve(int startPage, int pageCount) {
        int chunkIdx = startPage / pagesPerChunk;
        int offsetInChunk = startPage % pagesPerChunk;
        long byteOffset = (long) offsetInChunk * PAGE_SIZE;
        long byteLength = (long) pageCount * PAGE_SIZE;
        return chunks[chunkIdx].asSlice(byteOffset, byteLength);
    }

    /**
     * Resolve a single page to a {@link MemorySegment}.
     */
    public MemorySegment resolvePage(int page) {
        return resolve(page, 1);
    }

    /**
     * Return the backing chunk segment for the given page without creating a slice.
     */
    public MemorySegment chunkSegment(int page) {
        return chunks[page / pagesPerChunk];
    }

    /**
     * Compute the byte offset within a chunk for the given page-relative byte offset.
     */
    public long chunkByteOffset(int page, long byteOffset) {
        return (long) (page % pagesPerChunk) * PAGE_SIZE + byteOffset;
    }

    /**
     * Resolve a byte range within the file.
     *
     * @param startPage global page number
     * @param byteOffset byte offset within the starting page
     * @param byteLength number of bytes
     */
    public MemorySegment resolveBytes(int startPage, long byteOffset, long byteLength) {
        return chunkSegment(startPage).asSlice(chunkByteOffset(startPage, byteOffset), byteLength);
    }

    // -----------------------------------------------------------------------
    // Superblock access (page 0)
    // -----------------------------------------------------------------------

    /** Returns a writable MemorySegment for pages 0-1 (the superblock, 8 KB). */
    public MemorySegment superblock() {
        return resolve(0, 2);
    }

    // -----------------------------------------------------------------------
    // V2 checkpoint slot access
    // -----------------------------------------------------------------------

    /** Returns a writable MemorySegment for checkpoint slot A (pages 0-3, 16 KB). */
    public MemorySegment checkpointSlotA() {
        int slotSizePages = V2_RESERVED_PAGES / 2;
        return resolve(0, slotSizePages);
    }

    /** Returns a writable MemorySegment for checkpoint slot B (pages 4-7, 16 KB). */
    public MemorySegment checkpointSlotB() {
        int slotSizePages = V2_RESERVED_PAGES / 2;
        return resolve(slotSizePages, slotSizePages);
    }

    // -----------------------------------------------------------------------
    // Sync / lifecycle
    // -----------------------------------------------------------------------

    /**
     * Mark the chunk containing the given page as dirty.
     *
     * <p>Callers should use this when writing to pages that were allocated in a
     * previous sync cycle (e.g., checkpoint slot pages, pre-reserved commit pages).
     * Pages allocated via {@link #allocPages} are marked dirty automatically.
     *
     * @param page the global page number of any page within the chunk to mark
     */
    public void markDirty(int page) {
        dirtyChunks.add(page / pagesPerChunk);
    }

    /**
     * Force only the chunks that have been written to since the last
     * {@code syncDirty()} call, then flush the file channel and clear the
     * dirty set.
     *
     * <p>For a 10 GB file with 160 chunks, a typical write scope only dirties
     * 1-2 chunks, reducing the number of {@code force()} calls from 160 to 2.
     *
     * <p>Use {@link #sync()} when all chunks must be forced (e.g., after
     * compaction or before close).
     */
    public void syncDirty() throws IOException {
        for (int i : dirtyChunks) {
            if (i < chunkCount) {
                chunks[i].force();
            }
        }
        dirtyChunks.clear();
        channel.force(true);
        Preallocator.fullSync(path);
    }

    /**
     * Force all mapped chunks to disk.
     * On macOS, also uses {@code F_FULLFSYNC} for hardware cache flush.
     */
    public void sync() throws IOException {
        for (int i = 0; i < chunkCount; i++) {
            chunks[i].force();
        }
        dirtyChunks.clear();
        channel.force(true);
        // On macOS, channel.force() uses fsync which may not flush the drive's write cache.
        // F_FULLFSYNC ensures the drive actually writes to persistent media.
        Preallocator.fullSync(path);
    }

    @Override
    public void close() throws IOException {
        // Arena.close() will unmap all segments.
        // We only close the channel here.
        channel.close();
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /** Total committed pages in the file. */
    public int totalPages() { return totalPages; }

    /** Next page that will be allocated. */
    public int nextPage() { return nextPage.get(); }

    /** Chunk size in bytes. */
    public long chunkSize() { return chunkSize; }

    /** Pages per chunk. */
    public int pagesPerChunk() { return pagesPerChunk; }

    /** Number of mapped chunks. */
    public int chunkCount() { return chunkCount; }

    /** Number of chunks currently marked dirty. */
    public int dirtyChunkCount() { return dirtyChunks.size(); }

    /** The file path. */
    public Path path() { return path; }

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    private synchronized int allocPagesSlow(int pageCount) throws IOException {
        int startPage = nextPage.get();
        int currentChunkIdx = startPage / pagesPerChunk;
        int offsetInChunk = startPage % pagesPerChunk;
        if (offsetInChunk + pageCount > pagesPerChunk) {
            startPage = (currentChunkIdx + 1) * pagesPerChunk;
        }
        int endPage = startPage + pageCount;
        if (endPage > totalPages) {
            growFile(endPage);
        }
        nextPage.set(endPage);
        dirtyChunks.add(startPage / pagesPerChunk);
        return startPage;
    }

    /**
     * Grow the file so that it contains at least {@code minPages} pages.
     * Growth is rounded up to the chunk boundary.
     * Uses OS-specific preallocation if enabled.
     */
    private void growFile(int minPages) throws IOException {
        // Round up to chunk boundary
        int chunksNeeded = (minPages + pagesPerChunk - 1) / pagesPerChunk;
        int newTotalPages = chunksNeeded * pagesPerChunk;
        long newFileSize = (long) newTotalPages * PAGE_SIZE;
        long oldFileSize = (long) totalPages * PAGE_SIZE;

        if (preallocate && newFileSize > oldFileSize) {
            // Try platform-specific preallocation first
            boolean preallocated = Preallocator.preallocate(path, oldFileSize, newFileSize - oldFileSize);
            if (!preallocated) {
                // Fallback: plain truncate (sparse file)
                channel.truncate(newFileSize);
            }
        } else {
            // Extend the file (sparse)
            channel.truncate(newFileSize);
        }

        // Map any new chunks
        for (int i = chunkCount; i < chunksNeeded; i++) {
            mapChunk(i);
        }
        totalPages = newTotalPages;
    }

    private void mapChunk(int chunkIdx) throws IOException {
        if (chunkIdx >= chunks.length) {
            int newCap = Math.min(chunks.length * 2, MAX_CHUNKS);
            var newChunks = new MemorySegment[newCap];
            System.arraycopy(chunks, 0, newChunks, 0, chunkCount);
            chunks = newChunks;
        }
        long offset = (long) chunkIdx * chunkSize;
        chunks[chunkIdx] = channel.map(FileChannel.MapMode.READ_WRITE, offset, chunkSize, arena);
        chunkCount = Math.max(chunkCount, chunkIdx + 1);
    }

    private static void validateChunkSize(long chunkSize) {
        if (chunkSize <= 0 || chunkSize % PAGE_SIZE != 0) {
            throw new IllegalArgumentException(
                "chunkSize must be a positive multiple of PAGE_SIZE (" + PAGE_SIZE + "): " + chunkSize);
        }
    }
}
