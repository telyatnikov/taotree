package org.taotree.jmh;

import org.openjdk.jmh.annotations.*;
import org.taotree.*;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for ART point lookup and insert throughput.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class TaoTreeBenchmark {

    private static final int KEY_LEN = 16;
    private static final int VALUE_SIZE = 24;

    @Param({"10000", "100000", "1000000"})
    int keyCount;

    private TaoTree tree;
    private byte[][] keys;
    private int lookupIndex;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Path tmp = Files.createTempFile("jmh-taotree-", ".dat");
        tmp.toFile().deleteOnExit();
        Files.delete(tmp);
        tree = TaoTree.create(tmp, KEY_LEN, VALUE_SIZE, 4L * 1024 * 1024, false);

        var rng = new Random(42);
        keys = new byte[keyCount][KEY_LEN];

        try (var w = tree.write()) {
            for (int i = 0; i < keyCount; i++) {
                rng.nextBytes(keys[i]);
                long leaf = w.getOrCreate(keys[i], 0);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG_UNALIGNED, 0, (long) i);
            }
        }
        lookupIndex = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        tree.close();
    }

    @Benchmark
    public long lookupExisting() {
        byte[] key = keys[lookupIndex++ % keyCount];
        try (var r = tree.read()) {
            return r.lookup(key);
        }
    }

    @Benchmark
    public long lookupMissing() {
        byte[] key = keys[lookupIndex++ % keyCount].clone();
        key[KEY_LEN - 1] ^= (byte) 0xFF;
        try (var r = tree.read()) {
            return r.lookup(key);
        }
    }

    @Benchmark
    public long getOrCreateExisting() {
        byte[] key = keys[lookupIndex++ % keyCount];
        try (var w = tree.write()) {
            return w.getOrCreate(key, 0);
        }
    }
}
