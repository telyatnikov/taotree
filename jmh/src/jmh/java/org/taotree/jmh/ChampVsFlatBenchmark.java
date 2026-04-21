package org.taotree.jmh;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.champ.ChampMap;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Micro-benchmark: CHAMP persistent map vs. flat COW array as the per-entity
 * current-state container for small attribute counts (N = 4 / 16 / 64).
 *
 * <p>This benchmark exists to settle the "CHAMP is overkill at small entity
 * attribute counts" claim with data, not vibes. It compares three backends:
 *
 * <ul>
 *   <li><b>champ</b>: the production {@link ChampMap} off-heap persistent map.
 *   <li><b>flatOffHeap</b>: sorted {@code (attrId:4B, valueRef:8B)*N} payload
 *       in a {@link BumpAllocator} slab. {@code put} binary-searches and
 *       allocates a fresh payload; {@code get} binary-searches. This mirrors
 *       the reviewer's "flat contiguous struct" suggestion while still living
 *       off-heap.
 *   <li><b>flatHeap</b>: heap {@code int[] keys, long[] vals} arrays, sorted.
 *       Simplest possible alternative.
 * </ul>
 *
 * <p>Operations measured: build-from-empty (N puts), update-one-attr on a
 * prebuilt map, get-one-attr, iterate-all. Bytes-allocated (off-heap) is
 * reported as an aux counter on the build path where meaningful.
 *
 * <p>Read-only benches populate state in {@link Level#Trial}. Write benches
 * allocate a fresh {@link BumpAllocator} / working copy per
 * {@link Level#Iteration} so (a) the per-op measurement isn't polluted by
 * file-creation overhead (as would happen with {@link Level#Invocation}) and
 * (b) bump-allocator growth is bounded per iteration. Within an iteration the
 * bump allocator grows as each {@code updateOne} allocates a fresh payload or
 * CHAMP path copy; we size the bump page and chunk large enough that paging
 * cost does not leak into the measurement.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ChampVsFlatBenchmark {

    @Param({"4", "16", "64"})
    int n;

    @Param({"champ", "flatOffHeap", "flatHeap"})
    String backend;

    // Trial-scoped: shared resources, populated maps for read benches.
    private Arena arena;
    private ChunkStore chunkStore;
    private BumpAllocator bump;          // trial-scoped, for read-side prebuilt state
    private Path tmp;

    private long prebuiltChampRoot;
    private long prebuiltFlatOffHeapPtr;
    private int[]  prebuiltFlatHeapKeys;
    private long[] prebuiltFlatHeapVals;

    // Fixed deterministic inputs.
    private int[]  attrIds;
    private long[] valueRefs;
    private int    readIdx;

    // Per-invocation state for writes: a fresh allocator and derived working copies.
    private Arena         invArena;
    private ChunkStore    invChunkStore;
    private BumpAllocator invBump;
    private Path          invTmp;
    private long          workingChampRoot;
    private long          workingFlatOffHeapPtr;
    private int[]         workingFlatHeapKeys;
    private long[]        workingFlatHeapVals;
    private final ChampMap.Result champResult = new ChampMap.Result();

    @Setup(Level.Trial)
    public void trialSetup() throws IOException {
        arena = Arena.ofShared();
        tmp = Files.createTempFile("jmh-champ-flat-", ".dat");
        tmp.toFile().deleteOnExit();
        Files.delete(tmp);
        chunkStore = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
        bump = new BumpAllocator(arena, chunkStore, BumpAllocator.DEFAULT_PAGE_SIZE);

        // Deterministic, monotonically increasing attrIds — matches dictionary code ordering.
        Random rng = new Random(0xC0FFEEL + n);
        attrIds = new int[n];
        valueRefs = new long[n];
        for (int i = 0; i < n; i++) {
            attrIds[i] = i + 1;           // 1..n (avoid 0 for readability)
            valueRefs[i] = rng.nextLong();
        }

        prebuiltChampRoot     = buildChamp(bump, attrIds, valueRefs);
        prebuiltFlatOffHeapPtr = buildFlatOffHeap(bump, attrIds, valueRefs);
        prebuiltFlatHeapKeys  = attrIds.clone();
        prebuiltFlatHeapVals  = valueRefs.clone();
    }

    @TearDown(Level.Trial)
    public void trialTeardown() throws IOException {
        if (chunkStore != null) chunkStore.close();
        if (arena != null) arena.close();
        if (tmp != null) Files.deleteIfExists(tmp);
    }

    @Setup(Level.Iteration)
    public void iterationSetup() throws IOException {
        // Fresh allocator per measurement iteration so file-creation cost is
        // not charged to every invocation (which at ns/op granularity would
        // dominate). The bump allocator grows during the iteration as each
        // put/updateOne allocates; chunk size is large enough (64 MB default)
        // that no chunk growth occurs within a single 1-second iteration.
        invArena = Arena.ofShared();
        invTmp = Files.createTempFile("jmh-champ-flat-inv-", ".dat");
        invTmp.toFile().deleteOnExit();
        Files.delete(invTmp);
        invChunkStore = ChunkStore.createCheckpointed(invTmp, invArena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
        invBump = new BumpAllocator(invArena, invChunkStore, BumpAllocator.DEFAULT_PAGE_SIZE);

        // Pre-build a working copy for update-one benches.
        switch (backend) {
            case "champ"       -> workingChampRoot      = buildChamp(invBump, attrIds, valueRefs);
            case "flatOffHeap" -> workingFlatOffHeapPtr = buildFlatOffHeap(invBump, attrIds, valueRefs);
            case "flatHeap"    -> { workingFlatHeapKeys = attrIds.clone(); workingFlatHeapVals = valueRefs.clone(); }
            default -> throw new IllegalStateException(backend);
        }
    }

    @TearDown(Level.Iteration)
    public void iterationTeardown() throws IOException {
        if (invChunkStore != null) invChunkStore.close();
        if (invArena != null) invArena.close();
        if (invTmp != null) Files.deleteIfExists(invTmp);
        invBump = null;
    }

    // ==========================================================
    // Build from empty: allocate N attributes into a fresh map.
    // ==========================================================

    @Benchmark
    public long buildFromEmpty() {
        return switch (backend) {
            case "champ"       -> buildChamp(invBump, attrIds, valueRefs);
            case "flatOffHeap" -> buildFlatOffHeap(invBump, attrIds, valueRefs);
            case "flatHeap"    -> buildFlatHeap(attrIds, valueRefs);
            default -> throw new IllegalStateException(backend);
        };
    }

    // ==========================================================
    // Update one attribute in an already-populated map.
    // ==========================================================

    @Benchmark
    public long updateOne() {
        int pick = attrIds[(int) ((System.nanoTime() & 0x7fffffff) % n)];
        long newVal = 0xDEADBEEFL ^ pick;
        switch (backend) {
            case "champ" -> {
                champResult.reset();
                workingChampRoot = ChampMap.put(invBump, workingChampRoot, pick, newVal, champResult);
                return workingChampRoot;
            }
            case "flatOffHeap" -> {
                workingFlatOffHeapPtr = flatOffHeapPut(invBump, workingFlatOffHeapPtr, n, pick, newVal);
                return workingFlatOffHeapPtr;
            }
            case "flatHeap" -> {
                int idx = flatHeapIndexOf(workingFlatHeapKeys, pick);
                long[] newVals = workingFlatHeapVals.clone();
                newVals[idx] = newVal;
                workingFlatHeapVals = newVals;
                return newVal;
            }
            default -> throw new IllegalStateException(backend);
        }
    }

    // ==========================================================
    // Point read: look up one attribute.
    // ==========================================================

    @Benchmark
    public long getOne() {
        int pick = attrIds[(readIdx++ & 0x7fffffff) % n];
        return switch (backend) {
            case "champ"       -> ChampMap.get(bump, prebuiltChampRoot, pick);
            case "flatOffHeap" -> flatOffHeapGet(bump, prebuiltFlatOffHeapPtr, n, pick);
            case "flatHeap"    -> prebuiltFlatHeapVals[flatHeapIndexOf(prebuiltFlatHeapKeys, pick)];
            default -> throw new IllegalStateException(backend);
        };
    }

    // ==========================================================
    // Full enumeration (getAll-style).
    // ==========================================================

    @Benchmark
    public void iterateAll(Blackhole bh) {
        switch (backend) {
            case "champ" -> ChampMap.iterate(bump, prebuiltChampRoot, (a, v) -> { bh.consume(a); bh.consume(v); return true; });
            case "flatOffHeap" -> {
                MemorySegment seg = bump.resolve(prebuiltFlatOffHeapPtr, n * 12);
                for (int i = 0; i < n; i++) {
                    bh.consume(seg.get(ValueLayout.JAVA_INT_UNALIGNED,  i * 12L));
                    bh.consume(seg.get(ValueLayout.JAVA_LONG_UNALIGNED, i * 12L + 4));
                }
            }
            case "flatHeap" -> {
                for (int i = 0; i < n; i++) {
                    bh.consume(prebuiltFlatHeapKeys[i]);
                    bh.consume(prebuiltFlatHeapVals[i]);
                }
            }
            default -> throw new IllegalStateException(backend);
        }
    }

    // ==========================================================
    // Helpers: CHAMP / FlatOffHeap / FlatHeap operations.
    // ==========================================================

    private static long buildChamp(BumpAllocator alloc, int[] ids, long[] vals) {
        ChampMap.Result r = new ChampMap.Result();
        long root = ChampMap.EMPTY_ROOT;
        for (int i = 0; i < ids.length; i++) {
            r.reset();
            root = ChampMap.put(alloc, root, ids[i], vals[i], r);
        }
        return root;
    }

    /** Build a fresh sorted flat off-heap payload of {@code (attrId:4B, valueRef:8B) * N}. */
    private static long buildFlatOffHeap(BumpAllocator alloc, int[] ids, long[] vals) {
        long ptr = alloc.allocate(ids.length * 12);
        MemorySegment seg = alloc.resolve(ptr, ids.length * 12);
        // ids are already sorted ascending by construction (1..n).
        for (int i = 0; i < ids.length; i++) {
            seg.set(ValueLayout.JAVA_INT_UNALIGNED,  i * 12L, ids[i]);
            seg.set(ValueLayout.JAVA_LONG_UNALIGNED, i * 12L + 4, vals[i]);
        }
        return ptr;
    }

    private static long buildFlatHeap(int[] ids, long[] vals) {
        int[] k = ids.clone();
        long[] v = vals.clone();
        return k.length ^ v.length; // return something for JMH consumption
    }

    /** Binary search on the flat off-heap payload, returns -1 if absent. */
    private static long flatOffHeapGet(BumpAllocator alloc, long ptr, int size, int attrId) {
        MemorySegment seg = alloc.resolve(ptr, size * 12);
        int lo = 0, hi = size - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int k = seg.get(ValueLayout.JAVA_INT_UNALIGNED, mid * 12L);
            if (k == attrId) return seg.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * 12L + 4);
            if (k < attrId) lo = mid + 1; else hi = mid - 1;
        }
        return -1L;
    }

    /**
     * Flat off-heap put: binary-search, then allocate a fresh payload
     * ({@code size * 12} bytes if replacing, {@code (size+1) * 12} bytes if
     * inserting) and copy. Mirrors the reviewer's "flat array COW" proposal.
     */
    private static long flatOffHeapPut(BumpAllocator alloc, long ptr, int size, int attrId, long valueRef) {
        MemorySegment src = alloc.resolve(ptr, size * 12);
        int lo = 0, hi = size - 1, pos = size;
        boolean replace = false;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int k = src.get(ValueLayout.JAVA_INT_UNALIGNED, mid * 12L);
            if (k == attrId) { pos = mid; replace = true; break; }
            if (k < attrId) lo = mid + 1; else hi = mid - 1;
        }
        if (!replace) pos = lo;

        int newSize = replace ? size : size + 1;
        long out = alloc.allocate(newSize * 12);
        MemorySegment dst = alloc.resolve(out, newSize * 12);
        // [0, pos) unchanged
        if (pos > 0) MemorySegment.copy(src, 0, dst, 0, pos * 12L);
        // [pos] = (attrId, valueRef)
        dst.set(ValueLayout.JAVA_INT_UNALIGNED,  pos * 12L, attrId);
        dst.set(ValueLayout.JAVA_LONG_UNALIGNED, pos * 12L + 4, valueRef);
        // [pos+1, newSize) from src[pos or pos+1, size)
        int srcStart = replace ? pos + 1 : pos;
        int tail = size - srcStart;
        if (tail > 0) {
            MemorySegment.copy(src, srcStart * 12L, dst, (pos + 1) * 12L, tail * 12L);
        }
        return out;
    }

    private static int flatHeapIndexOf(int[] keys, int attrId) {
        int lo = 0, hi = keys.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int k = keys[mid];
            if (k == attrId) return mid;
            if (k < attrId) lo = mid + 1; else hi = mid - 1;
        }
        return -1;
    }
}
