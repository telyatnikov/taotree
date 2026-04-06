package org.taotree;

import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;

/**
 * Lincheck linearizability test for file-backed TaoTree.
 *
 * <p>Same operations as {@link LincheckTaoTreeTest} but the tree is backed by
 * a memory-mapped file. This tests the ChunkStore / SlabAllocator / BumpAllocator
 * persistence layer under concurrent access.
 *
 * <p>Lincheck creates a fresh test instance per invocation, so we use a small
 * chunk size (64 KB) and disable OS preallocation to keep disk usage bounded.
 * Files are created in {@code /tmp} (real disk) to avoid filling a RAMDisk.
 */
@Param(name = "key", gen = IntGen.class, conf = "1:5")
public class LincheckMappedTreeTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;

    /** Chunk size must be ≥ slab size (1 MB default). No preallocation keeps disk usage low. */
    private static final long CHUNK_SIZE = 1024L * 1024;

    private final TaoTree tree;

    public LincheckMappedTreeTest() {
        try {
            Path tmp = Files.createTempFile("lincheck-taotree-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp); // TaoTree.create requires non-existent path
            tree = TaoTree.create(tmp, KEY_LEN, VALUE_SIZE, CHUNK_SIZE, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] encodeKey(int k) {
        return new byte[]{
            (byte) (k >>> 24), (byte) (k >>> 16),
            (byte) (k >>> 8), (byte) k
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
        // Conservative settings: Lincheck creates one instance per invocation.
        // Each instance opens a 1 MB sparse file that is never closed, so
        // total disk ≈ iterations × invocationsPerIteration × 1 MB.
        new StressOptions()
            .iterations(10)
            .invocationsPerIteration(50)
            .threads(3)
            .actorsPerThread(3)
            .sequentialSpecification(LincheckTaoTreeTest.SequentialSpec.class)
            .check(this.getClass());
    }
}
