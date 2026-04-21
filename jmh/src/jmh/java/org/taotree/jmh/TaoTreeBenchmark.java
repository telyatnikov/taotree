package org.taotree.jmh;

import org.openjdk.jmh.annotations.*;
import org.taotree.*;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for ART point lookup and insert throughput using the temporal API.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class TaoTreeBenchmark {

    @Param({"10000", "100000", "1000000"})
    int keyCount;

    private TaoTree tree;
    private Arena arena;
    private org.taotree.layout.KeyBuilder[] keyBuilders;
    private KeyHandle.UInt32 idHandle;
    private int lookupIndex;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Path tmp = Files.createTempFile("jmh-taotree-", ".dat");
        tmp.toFile().deleteOnExit();
        Files.delete(tmp);

        KeyLayout layout = KeyLayout.of(KeyField.uint32("id"));
        tree = TaoTree.create(tmp, layout);
        idHandle = tree.keyUint32("id");
        arena = Arena.ofShared();

        var rng = new Random(42);
        keyBuilders = new org.taotree.layout.KeyBuilder[keyCount];
        for (int i = 0; i < keyCount; i++) {
            keyBuilders[i] = tree.newKeyBuilder(arena);
            keyBuilders[i].set(idHandle, rng.nextInt());
        }

        try (var w = tree.write()) {
            for (int i = 0; i < keyCount; i++) {
                w.put(keyBuilders[i], "v", Value.ofInt(i));
            }
        }
        lookupIndex = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
        tree.close();
    }

    @Benchmark
    public Value lookupExisting() {
        var kb = keyBuilders[lookupIndex++ % keyCount];
        try (var r = tree.read()) {
            return r.get(kb, "v");
        }
    }

    @Benchmark
    public Value lookupMissing() {
        try (var missArena = Arena.ofConfined();
             var r = tree.read()) {
            var kb = tree.newKeyBuilder(missArena);
            kb.set(idHandle, Integer.MIN_VALUE ^ (lookupIndex++ % keyCount));
            return r.get(kb, "v");
        }
    }

    @Benchmark
    public boolean insertOrUpdate() {
        var kb = keyBuilders[lookupIndex++ % keyCount];
        try (var w = tree.write()) {
            return w.put(kb, "v", Value.ofInt(lookupIndex));
        }
    }
}
