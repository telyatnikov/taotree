package org.taotree.examples.gbif;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import org.taotree.*;
import org.taotree.layout.*;

import java.io.File;
import java.lang.foreign.Arena;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

/**
 * GBIF ingestion benchmark — compares single-threaded vs multi-threaded writes.
 *
 * <p>For each requested thread count, creates a fresh store, ingests all Parquet
 * files, verifies correctness with {@link GbifVerifier}, and records timing + size
 * metrics. Parquet files are partitioned across writer threads round-robin.
 *
 * <p>Usage:
 * <pre>
 *   GbifBenchmark &lt;parquet-dir&gt;                     # default: 1,2,4,8 threads
 *   GbifBenchmark &lt;parquet-dir&gt; 1 4 10              # explicit thread counts
 * </pre>
 */
public class GbifBenchmark {

    private static final int BATCH_ROWS = 100_000;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: GbifBenchmark <parquet-dir> [thread-counts...]");
            return;
        }

        File inputDir = new File(args[0]);
        File[] files = inputDir.listFiles((dir, name) -> name.endsWith(".parquet"));
        if (files == null || files.length == 0) {
            System.err.println("No .parquet files in " + inputDir);
            return;
        }
        Arrays.sort(files);

        int[] threadCounts;
        if (args.length > 1) {
            threadCounts = new int[args.length - 1];
            for (int i = 1; i < args.length; i++)
                threadCounts[i - 1] = Integer.parseInt(args[i]);
        } else {
            threadCounts = new int[]{1, 2, 4, 8};
        }

        System.out.println("TaoTree GBIF Benchmark");
        System.out.printf("Files:   %d parquet files%n", files.length);
        System.out.printf("Configs: %s threads%n", Arrays.toString(threadCounts));
        System.out.println();

        List<RunResult> results = new ArrayList<>();

        for (int nThreads : threadCounts) {
            System.out.printf("=== %d thread%s ===%n", nThreads, nThreads == 1 ? "" : "s");
            var result = runIngestion(inputDir.toPath(), files, nThreads);
            results.add(result);
            System.out.println();
        }

        printComparison(results);
    }

    // -----------------------------------------------------------------------
    // Result record
    // -----------------------------------------------------------------------

    record RunResult(
        int threads, long entries, double elapsedSec,
        long fileSizeBytes, long slabBytes, long segmentsInUse,
        long overflowBytes, int overflowPages,
        boolean verified
    ) {
        double rowsPerSec(long totalRows) { return totalRows / elapsedSec; }
        double fileSizeMB() { return fileSizeBytes / (1024.0 * 1024); }
        double slabMB()     { return slabBytes / (1024.0 * 1024); }
        double overflowMB() { return overflowBytes / (1024.0 * 1024); }
    }

    // -----------------------------------------------------------------------
    // Single run: ingest + verify
    // -----------------------------------------------------------------------

    private static RunResult runIngestion(Path dir, File[] files, int nThreads) throws Exception {
        Path storePath = dir.resolve("gbif-bench-" + nThreads + "t.taotree");

        try {
            var tree = GbifTracker.createFresh(storePath);
            long startNs = System.nanoTime();

            try {
                if (nThreads == 1) {
                    ingestSingleThread(tree, files);
                } else {
                    ingestMultiThread(tree, files, nThreads);
                }
                tree.sync();
            } finally {
                tree.close();
            }

            double elapsedSec = (System.nanoTime() - startNs) / 1e9;
            vacuumStore(storePath);

            // Reopen read-only for metrics
            tree = TaoTree.open(storePath, GbifTracker.KEY_LAYOUT, GbifTracker.LEAF_LAYOUT);
            long entries;
            long slabBytes, segsInUse, overflowBytes;
            int overflowPages;
            try {
                try (var r = tree.read()) { entries = r.size(); }
                slabBytes = tree.totalSlabBytes();
                segsInUse = tree.totalSegmentsInUse();
                overflowBytes = tree.totalOverflowBytes();
                overflowPages = tree.overflowPageCount();
            } finally {
                tree.close();
            }

            long fileSize = Files.size(storePath);

            System.out.printf("  Entries:    %,d%n", entries);
            System.out.printf("  Time:       %.2f sec%n", elapsedSec);
            System.out.printf("  File size:  %,.1f MB%n", fileSize / (1024.0 * 1024));

            // Verify with independent parser
            System.out.print("  Verifying...");
            var vr = GbifVerifier.verify(storePath, files);
            boolean ok = vr.passed();
            System.out.printf(" %s (checked %,d, %d errors)%n",
                ok ? "PASS" : "FAIL", vr.checked(), vr.fieldErrors());
            if (!ok) {
                vr.errors().stream().limit(10).forEach(e -> System.out.println("    " + e));
            }

            return new RunResult(nThreads, entries, elapsedSec,
                fileSize, slabBytes, segsInUse, overflowBytes, overflowPages, ok);
        } finally {
            Files.deleteIfExists(storePath);
        }
    }

    private static void vacuumStore(Path storePath) throws Exception {
        Path compactedPath = storePath.resolveSibling(storePath.getFileName() + ".vacuum");
        Files.deleteIfExists(compactedPath);
        try {
            try (var source = TaoTree.open(storePath, GbifTracker.KEY_LAYOUT, GbifTracker.LEAF_LAYOUT);
                 var compacted = GbifTracker.createFresh(compactedPath)) {
                copyDictionaries(source.keyLayout(), compacted.keyLayout());
                compacted.copyFrom(source);
                compacted.sync();
            }
            try {
                Files.move(compactedPath, storePath,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(compactedPath, storePath, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(compactedPath);
        }
    }

    private static void copyDictionaries(KeyLayout sourceLayout, KeyLayout targetLayout) {
        if (sourceLayout.fieldCount() != targetLayout.fieldCount()) {
            throw new IllegalArgumentException(
                "Key layout field count mismatch: source=" + sourceLayout.fieldCount()
                + " target=" + targetLayout.fieldCount());
        }
        for (int i = 0; i < sourceLayout.fieldCount(); i++) {
            KeyField sourceField = sourceLayout.field(i);
            KeyField targetField = targetLayout.field(i);
            if (sourceField instanceof KeyField.DictU16 sourceDict
                    && targetField instanceof KeyField.DictU16 targetDict) {
                targetDict.dict().copyFrom(sourceDict.dict());
            } else if (sourceField instanceof KeyField.DictU32 sourceDict
                    && targetField instanceof KeyField.DictU32 targetDict) {
                targetDict.dict().copyFrom(sourceDict.dict());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Single-threaded ingestion (uses GbifTracker shared methods)
    // -----------------------------------------------------------------------

    private static void ingestSingleThread(TaoTree tree, File[] files) throws Exception {
        var kh = GbifTracker.KeyHandles.bind(tree);
        try (var arena = Arena.ofConfined()) {
            var kb = tree.newKeyBuilder(arena);
            int batchCount = 0;

            for (File file : files) {
                try (var fileReader = ParquetFileReader.open(InputFile.of(file.toPath()));
                     var rows = fileReader.createRowReader()) {

                    var w = tree.write();
                    try {
                        while (rows.hasNext()) {
                            rows.next();
                            if (GbifTracker.encodeKey(kb, kh, rows) == null) continue;
                            GbifTracker.writeLeaf(w, kb, rows);

                            if (++batchCount % BATCH_ROWS == 0) {
                                w.close();
                                tree.sync();
                                w = tree.write();
                            }
                        }
                    } finally {
                        w.close();
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Multi-threaded ingestion (files partitioned round-robin)
    // -----------------------------------------------------------------------

    private static void ingestMultiThread(TaoTree tree, File[] files, int nThreads) throws Exception {
        @SuppressWarnings("unchecked")
        List<File>[] partitions = new List[nThreads];
        for (int i = 0; i < nThreads; i++) partitions[i] = new ArrayList<>();
        for (int i = 0; i < files.length; i++) partitions[i % nThreads].add(files[i]);

        var errors = new LongAdder();
        var latch = new CountDownLatch(nThreads);
        var kh = GbifTracker.KeyHandles.bind(tree);

        // Conflict resolver: during rebase, merge row-count deltas and keep the newer observation.
        ConflictResolver resolver = (target, pending, snapshot) -> {
            // Count is an accumulator: add the delta (rows processed by this scope)
            int countDelta = pending.get(GbifTracker.COUNT) - snapshot.get(GbifTracker.COUNT);
            target.set(GbifTracker.COUNT, target.get(GbifTracker.COUNT) + countDelta);
            // Keep the newer observation per the total-order comparison
            if (GbifTracker.isNewer(
                pending.get(GbifTracker.YEAR), pending.get(GbifTracker.MONTH), pending.get(GbifTracker.DAY),
                GbifTracker.getD(pending, GbifTracker.LAT), GbifTracker.getD(pending, GbifTracker.LON),
                pending.get(GbifTracker.TAXON_K), pending.get(GbifTracker.SPECIES_K),
                pending.get(GbifTracker.IND_CNT),
                pending.get(GbifTracker.LOCALITY), pending.get(GbifTracker.RECORDED),
                pending.get(GbifTracker.EXTRAS),
                target.get(GbifTracker.YEAR), target.get(GbifTracker.MONTH), target.get(GbifTracker.DAY),
                GbifTracker.getD(target, GbifTracker.LAT), GbifTracker.getD(target, GbifTracker.LON),
                target.get(GbifTracker.TAXON_K), target.get(GbifTracker.SPECIES_K),
                target.get(GbifTracker.IND_CNT),
                target.get(GbifTracker.LOCALITY), target.get(GbifTracker.RECORDED),
                target.get(GbifTracker.EXTRAS)
            )) {
                copyNullable(pending, target, GbifTracker.YEAR);
                copyNullable(pending, target, GbifTracker.MONTH);
                copyNullable(pending, target, GbifTracker.DAY);
                copyNullableD(pending, target, GbifTracker.LAT);
                copyNullableD(pending, target, GbifTracker.LON);
                copyNullableD(pending, target, GbifTracker.ELEV);
                copyNullable(pending, target, GbifTracker.IND_CNT);
                copyNullable(pending, target, GbifTracker.TAXON_K);
                copyNullable(pending, target, GbifTracker.SPECIES_K);
                copyNullableS(pending, target, GbifTracker.LOCALITY);
                copyNullableS(pending, target, GbifTracker.RECORDED);
                target.set(GbifTracker.EXTRAS, pending.get(GbifTracker.EXTRAS));
            }
        };

        for (int t = 0; t < nThreads; t++) {
            final List<File> myFiles = partitions[t];
            Thread.ofPlatform().name("writer-" + t).start(() -> {
                try (var arena = Arena.ofConfined()) {
                    var kb = tree.newKeyBuilder(arena);
                    int batchCount = 0;

                    for (File file : myFiles) {
                        try (var fileReader = ParquetFileReader.open(InputFile.of(file.toPath()));
                             var rows = fileReader.createRowReader()) {

                            var w = tree.write(resolver);
                            try {
                                while (rows.hasNext()) {
                                    rows.next();
                                    if (GbifTracker.encodeKey(kb, kh, rows) == null) continue;
                                    GbifTracker.writeLeaf(w, kb, rows);

                                    if (++batchCount % BATCH_ROWS == 0) {
                                        w.close();
                                        w = tree.write(resolver);
                                    }
                                }
                            } finally {
                                w.close();
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.printf("[writer-%d] %s%n", Thread.currentThread().threadId(), e);
                    e.printStackTrace();
                    errors.increment();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        if (errors.sum() > 0) throw new RuntimeException(errors.sum() + " writer(s) failed");
    }

    // -----------------------------------------------------------------------
    // Null-aware field copy helpers (preserve null bitmap during conflict resolution)
    // -----------------------------------------------------------------------

    private static void copyNullable(LeafAccessor src, LeafAccessor dst, LeafHandle.Int32 h) {
        if (src.isNull(h)) dst.setNull(h);
        else dst.set(h, src.get(h));
    }

    private static void copyNullableD(LeafAccessor src, LeafAccessor dst, LeafHandle.Float64 h) {
        if (src.isNull(h)) dst.setNull(h);
        else dst.set(h, src.get(h));
    }

    private static void copyNullableS(LeafAccessor src, LeafAccessor dst, LeafHandle.Str h) {
        if (src.isNull(h)) dst.setNull(h);
        else dst.set(h, src.get(h));
    }

    // -----------------------------------------------------------------------
    // Comparison table
    // -----------------------------------------------------------------------

    private static void printComparison(List<RunResult> results) {
        var b = results.getFirst(); // baseline
        long totalRows = b.entries; // approximate (entries, not rows — but consistent)

        System.out.println("╔══════════╦══════════════╦══════════════╦══════════════╦══════════════╦══════════╗");
        System.out.println("║ Threads  ║  Time (sec)  ║  File (MB)   ║  Slab (MB)   ║  Overflow MB ║ Verify   ║");
        System.out.println("╠══════════╬══════════════╬══════════════╬══════════════╬══════════════╬══════════╣");
        for (var r : results) {
            System.out.printf("║ %4d     ║ %10.2f   ║ %10.1f   ║ %10.1f   ║ %10.1f   ║ %-8s ║%n",
                r.threads, r.elapsedSec, r.fileSizeMB(), r.slabMB(), r.overflowMB(),
                r.verified ? "PASS" : "FAIL");
        }
        System.out.println("╠══════════╬══════════════╬══════════════╬══════════════╬══════════════╬══════════╣");
        for (var r : results) {
            double speedup = b.elapsedSec / r.elapsedSec;
            double sizeRatio = r.fileSizeBytes / (double) b.fileSizeBytes;
            System.out.printf("║ %4d     ║ %8.2fx     ║ %8.2fx     ║ %,10d   ║ %,10d   ║ %,8d ║%n",
                r.threads, speedup, sizeRatio, r.segmentsInUse, r.overflowPages, r.entries);
        }
        System.out.println("║          ║  speedup     ║  size ratio  ║  segments    ║  ovf pages   ║ entries  ║");
        System.out.println("╚══════════╩══════════════╩══════════════╩══════════════╩══════════════╩══════════╝");

        boolean allMatch = results.stream().allMatch(r -> r.entries == b.entries);
        boolean allVerified = results.stream().allMatch(r -> r.verified);
        System.out.printf("%nEntries: %,d %s%n", b.entries,
            allMatch ? "(consistent)" : "*** ENTRY COUNT MISMATCH ***");
        System.out.printf("Verify:  %s%n",
            allVerified ? "all passed" : "*** SOME RUNS FAILED VERIFICATION ***");
    }
}
