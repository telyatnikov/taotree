package org.taotree.jmh;

import org.openjdk.jmh.annotations.*;
import org.taotree.*;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Unified-temporal API benchmarks covering the p8-perf targets:
 * value-inline (≤12 B) vs overflow, timeless vs temporal writes, getAll
 * (CHAMP enumeration), and getAt (AttrRuns predecessor).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
public class TemporalApiBenchmark {

    private static final int KEY_COUNT = 100_000;
    private static final int MULTI_ATTR_COUNT = 8;

    private TaoTree tree;
    private Arena arena;
    private KeyBuilder[] kbs;
    private KeyHandle.UInt32 idHandle;
    private int idx;

    private final Value inlineInt = Value.ofInt(42);
    private final Value inlineStr = Value.ofString("abc");                 // 3 B payload (inline)
    private final Value overflowStr = Value.ofString(
            "this is a string well past the 12 byte inline threshold"); // > 12 B (overflow)

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Path tmp = Files.createTempFile("jmh-unified-", ".dat");
        tmp.toFile().deleteOnExit();
        Files.delete(tmp);

        KeyLayout layout = KeyLayout.of(KeyField.uint32("id"));
        tree = TaoTree.create(tmp, layout);
        idHandle = tree.keyUint32("id");
        arena = Arena.ofShared();

        var rng = new Random(42);
        kbs = new KeyBuilder[KEY_COUNT];
        for (int i = 0; i < KEY_COUNT; i++) {
            kbs[i] = tree.newKeyBuilder(arena);
            kbs[i].set(idHandle, rng.nextInt());
        }

        // Pre-populate: single attr "v" + MULTI_ATTR_COUNT attrs per entity, all TIMELESS.
        try (var w = tree.write()) {
            for (int i = 0; i < KEY_COUNT; i++) {
                w.put(kbs[i], "v", Value.ofInt(i));
                for (int a = 0; a < MULTI_ATTR_COUNT; a++) {
                    w.put(kbs[i], "a" + a, Value.ofInt(a));
                }
            }
        }

        // Pre-populate a temporal history on the first 1000 keys for getAt/history benches.
        try (var w = tree.write()) {
            for (int i = 0; i < 1000; i++) {
                w.put(kbs[i], "t", Value.ofInt(1), 1_000_000L);
                w.put(kbs[i], "t", Value.ofInt(2), 2_000_000L);
                w.put(kbs[i], "t", Value.ofInt(3), 3_000_000L);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
        tree.close();
    }

    // -------- Reads --------

    @Benchmark
    public Value getTimeless() {
        var kb = kbs[idx++ % KEY_COUNT];
        try (var r = tree.read()) {
            return r.get(kb, "v");
        }
    }

    @Benchmark
    public Value getAt() {
        var kb = kbs[idx++ % 1000];
        try (var r = tree.read()) {
            return r.getAt(kb, "t", 2_500_000L);
        }
    }

    @Benchmark
    public Map<String, Value> getAllBench() {
        var kb = kbs[idx++ % KEY_COUNT];
        try (var r = tree.read()) {
            return r.getAll(kb);
        }
    }

    // -------- Writes: inline vs overflow, timeless vs temporal --------

    @Benchmark
    public boolean putTimelessInt() {
        var kb = kbs[idx++ % KEY_COUNT];
        try (var w = tree.write()) {
            return w.put(kb, "v", inlineInt);
        }
    }

    @Benchmark
    public boolean putTimelessInlineStr() {
        var kb = kbs[idx++ % KEY_COUNT];
        try (var w = tree.write()) {
            return w.put(kb, "v", inlineStr);
        }
    }

    @Benchmark
    public boolean putTimelessOverflowStr() {
        var kb = kbs[idx++ % KEY_COUNT];
        try (var w = tree.write()) {
            return w.put(kb, "v", overflowStr);
        }
    }

    @Benchmark
    public boolean putTemporal() {
        var kb = kbs[idx++ % KEY_COUNT];
        long ts = 1_000_000L + (idx * 37L);
        try (var w = tree.write()) {
            return w.put(kb, "v", inlineInt, ts);
        }
    }

    @Benchmark
    public int putAllBench() {
        var kb = kbs[idx++ % KEY_COUNT];
        Map<String, Value> m = new HashMap<>();
        for (int a = 0; a < MULTI_ATTR_COUNT; a++) m.put("a" + a, Value.ofInt(idx + a));
        try (var w = tree.write()) {
            w.putAll(kb, m);
        }
        return m.size();
    }
}
