package org.taotree;

import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Lincheck linearizability test for the unified Value-typed API.
 *
 * <p>Models the tree as {@code Map<Integer, Map<String, Integer>>} (entity → attr → int).
 * Verifies that concurrent put/get/deleteEntity preserves the per-key map value.
 */
@Param(name = "key",  gen = IntGen.class, conf = "1:3")
@Param(name = "val",  gen = IntGen.class, conf = "100:103")
public class LincheckTaoTreePutGetTest {

    private static final List<Runnable> CLEANUPS = new CopyOnWriteArrayList<>();

    private final TaoTree tree;
    private final Path storePath;
    private final KeyHandle.UInt32 ID;

    public LincheckTaoTreePutGetTest() {
        try {
            storePath = Files.createTempFile("lincheck-tao-pg-", ".dat");
            Files.delete(storePath);
            tree = TaoTree.create(storePath, KeyLayout.of(KeyField.uint32("id")));
            ID = tree.keyUint32("id");
            CLEANUPS.add(() -> {
                tree.close();
                try { Files.deleteIfExists(storePath); } catch (IOException ignored) {}
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterEach
    void cleanup() {
        var toRun = List.copyOf(CLEANUPS);
        CLEANUPS.clear();
        toRun.forEach(Runnable::run);
    }

    private KeyBuilder kb(Arena a, int k) {
        var b = tree.newKeyBuilder(a);
        b.set(ID, k);
        return b;
    }

    @Operation
    public int put(@Param(name = "key") int k, @Param(name = "val") int v) {
        try (var a = Arena.ofConfined(); var w = tree.write()) {
            w.put(kb(a, k), "v", Value.ofInt(v));
            return v;
        }
    }

    @Operation
    public int get(@Param(name = "key") int k) {
        try (var a = Arena.ofConfined(); var r = tree.read()) {
            Value v = r.get(kb(a, k), "v");
            return (v == null) ? -1 : ((Value.Int32) v).value();
        }
    }

    @Operation
    public boolean deleteEntity(@Param(name = "key") int k) {
        try (var a = Arena.ofConfined(); var w = tree.write()) {
            return w.delete(kb(a, k));
        }
    }

    @Test
    void stressTest() {
        new StressOptions()
            .iterations(20)
            .invocationsPerIteration(50)
            .threads(2)
            .actorsPerThread(2)
            .sequentialSpecification(SequentialSpec.class)
            .check(this.getClass());
    }

    public static class SequentialSpec {
        private final Map<Integer, Map<String, Integer>> map = new HashMap<>();

        public int put(int k, int v) {
            map.computeIfAbsent(k, x -> new HashMap<>()).put("v", v);
            return v;
        }

        public int get(int k) {
            Map<String, Integer> m = map.get(k);
            if (m == null) return -1;
            Integer v = m.get("v");
            return v == null ? -1 : v;
        }

        public boolean deleteEntity(int k) {
            return map.remove(k) != null;
        }
    }
}
