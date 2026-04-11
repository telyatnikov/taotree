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
 * Lincheck linearizability test for core TaoTree operations with raw byte keys.
 *
 * <p>Uses a small key space (1..5) and an independent value space (100..105)
 * so that conflicting updates on the same key are observable (different values
 * distinguish "which writer won").
 *
 * <p>These tests pass trivially with the current RWLock implementation
 * (every concurrent execution is serialised by the lock). They become the
 * safety net when the lock is replaced with a lock-free algorithm.
 */
@Param(name = "key", gen = IntGen.class, conf = "1:5")
@Param(name = "val", gen = IntGen.class, conf = "100:105")
public class LincheckTaoTreeTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;
    /** Chunk size must be >= slab size (1 MB default). */
    private static final long CHUNK_SIZE = 1024L * 1024;

    /** All trees and paths created by Lincheck invocations — drained by @AfterEach. */
    private static final List<Runnable> CLEANUPS = new CopyOnWriteArrayList<>();

    private final TaoTree tree;
    private final Path storePath;

    public LincheckTaoTreeTest() {
        try {
            storePath = Files.createTempFile("lincheck-taotree-", ".dat");
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

    private static byte[] encodeKey(int k) {
        return new byte[]{
            (byte) (k >>> 24), (byte) (k >>> 16),
            (byte) (k >>> 8), (byte) k
        };
    }

    @Operation
    public int put(@Param(name = "key") int k, @Param(name = "val") int v) {
        byte[] key = encodeKey(k);
        try (var w = tree.write()) {
            long leaf = w.getOrCreate(key);
            w.leafValue(leaf).set(ValueLayout.JAVA_INT, 0, v);
            return v;
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
            .actorsPerThread(3)
            .sequentialSpecification(SequentialSpec.class)
            .check(this.getClass());
    }

    /**
     * Sequential specification: a plain HashMap that defines the correct
     * sequential behaviour. Lincheck verifies that every concurrent
     * execution is equivalent to some sequential execution of this spec.
     */
    public static class SequentialSpec {
        private final Map<Integer, Integer> map = new HashMap<>();

        public int put(int k, int v) {
            map.put(k, v);
            return v;
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
