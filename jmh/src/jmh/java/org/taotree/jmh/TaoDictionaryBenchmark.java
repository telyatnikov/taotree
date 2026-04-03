package org.taotree.jmh;

import org.openjdk.jmh.annotations.*;
import org.taotree.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for TaoDictionary string-to-code resolution throughput.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class TaoDictionaryBenchmark {

    @Param({"100", "10000", "100000"})
    int dictSize;

    private TaoTree tree;
    private TaoDictionary dict;
    private String[] names;
    private int lookupIndex;

    @Setup(Level.Trial)
    public void setup() {
        tree = TaoTree.forDictionaries(4 * 1024 * 1024);
        dict = TaoDictionary.u32(tree);

        var rng = new Random(42);
        String[] genera = {"Haliaeetus", "Falco", "Aquila", "Buteo", "Accipiter",
            "Pandion", "Circus", "Elanus", "Milvus", "Pernis", "Gyps", "Aegypius",
            "Sagittarius", "Polyboroides", "Gypohierax", "Necrosyrtes", "Trigonoceps",
            "Torgos", "Sarcogyps", "Gypaetus", "Neophron", "Circaetus", "Terathopius",
            "Spilornis", "Pithecophaga", "Harpia", "Morphnus", "Spizaetus"};
        String[] epithets = {"leucocephalus", "peregrinus", "chrysaetos", "buteo",
            "gentilis", "haliaetus", "aeruginosus", "caeruleus", "milvus", "apivorus",
            "fulvus", "monachus", "serpentarius", "typus", "angolensis", "monachus",
            "occipitalis", "tracheliotos", "calvus", "barbatus", "percnopterus",
            "gallicus", "ecaudatus", "cheela", "jefferyi", "harpyja", "guianensis"};

        names = new String[dictSize];
        for (int i = 0; i < dictSize; i++) {
            names[i] = genera[rng.nextInt(genera.length)] + " "
                + epithets[rng.nextInt(epithets.length)] + " " + i;
            dict.intern(names[i]);
        }
        lookupIndex = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        tree.close();
    }

    @Benchmark
    public int resolveExisting() {
        return dict.intern(names[lookupIndex++ % dictSize]);
    }

    @Benchmark
    public int resolveOnly() {
        return dict.resolve(names[lookupIndex++ % dictSize]);
    }
}
