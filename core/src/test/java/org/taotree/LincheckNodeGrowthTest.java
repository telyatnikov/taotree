package org.taotree;

import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Lincheck test targeting ART node growth and shrink under concurrency.
 *
 * <p>Uses a wider key range (1..50) so that concurrent insertions and deletions
 * force node transitions: Node4 -> Node16 -> Node48 -> Node256 and back.
 */
@Param(name = "key", gen = IntGen.class, conf = "1:50")
public class LincheckNodeGrowthTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;
    private static final long CHUNK_SIZE = 1024L * 1024;

    /** All trees and paths created by Lincheck invocations — drained by @AfterEach. */
    private static final List<Runnable> CLEANUPS = new CopyOnWriteArrayList<>();

    private final TaoTree tree;
    private final Path storePath;

    public LincheckNodeGrowthTest() {
        try {
            storePath = Files.createTempFile("lincheck-nodegrowth-", ".dat");
            storePath.toFile().deleteOnExit();
            Files.delete(storePath);
            tree = TaoTree.create(storePath, KEY_LEN, VALUE_SIZE, CHUNK_SIZE, false);
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
        List<Runnable> toRun = List.copyOf(CLEANUPS);
        CLEANUPS.clear();
        toRun.forEach(Runnable::run);
    }

    /**
     * Encodes key with a fixed first byte (0x42) and varying second byte,
     * so all keys land in the same inner node and force node type transitions.
     */
    private static byte[] encodeKey(int k) {
        return new byte[]{
            0x42,
            (byte) k,
            0x00,
            0x01
        };
    }

    @Operation
    public int put(@Param(name = "key") int k) {
        byte[] key = encodeKey(k);
        try (var w = tree.write()) {
            long leaf = w.getOrCreate(key);
            w.leafValue(leaf).set(ValueLayout.JAVA_INT, 0, k);
            return k;
        }
    }

    @Operation
    public int get(@Param(name = "key") int k) {
        byte[] key = encodeKey(k);
        try (var r = tree.read()) {
            long leaf = r.lookup(key);
            if (leaf == TaoTree.NOT_FOUND) return -1;
            return r.leafValue(leaf).get(ValueLayout.JAVA_INT, 0);
        }
    }

    @Operation
    public boolean delete(@Param(name = "key") int k) {
        byte[] key = encodeKey(k);
        try (var w = tree.write()) {
            return w.delete(key);
        }
    }

    @Operation
    public long size() {
        try (var r = tree.read()) {
            return r.size();
        }
    }

    @Test
    void stressTest() {
        new StressOptions()
            .iterations(100)
            .invocationsPerIteration(50)
            .threads(3)
            .actorsPerThread(4)
            .sequentialSpecification(SequentialSpec.class)
            .check(this.getClass());
    }

    /**
     * Sequential specification with the same key-encoding semantics.
     */
    public static class SequentialSpec {
        private final Map<Integer, Integer> map = new HashMap<>();

        public int put(int k) {
            map.put(k, k);
            return k;
        }

        public int get(int k) {
            Integer v = map.get(k);
            return v == null ? -1 : v;
        }

        public boolean delete(int k) {
            return map.remove(k) != null;
        }

        public long size() {
            return map.size();
        }
    }
}
