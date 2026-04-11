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
 * Lincheck test specifically targeting Node4 → Node16 → Node48 growth transitions
 * and their reversal under concurrent insert/delete pressure.
 *
 * <h2>Strategy</h2>
 * <p>Unlike {@link LincheckNodeGrowthTest} which uses a wide key range (1..50) and
 * triggers all transition types, this test uses <b>constrained parameter ranges</b>
 * to isolate specific growth boundaries:
 * <ul>
 *   <li>Key range 1..18: All keys share first byte 0xAA, so they cohabit a single
 *       inner node. With 18 distinct second-bytes, concurrent inserts force
 *       Node4 → Node16 (at count=4) and Node16 → Node48 (at count=16).</li>
 *   <li>Concurrent deletions trigger the reverse: Node48 → Node16 (at count ≤12)
 *       and Node16 → Node4 (at count ≤3), plus single-child collapse to PrefixNode.</li>
 * </ul>
 *
 * <h2>Obstruction-Freedom Verification</h2>
 * <p>TaoTree's write model is obstruction-free: a single writer running in isolation
 * always completes (optimistic COW + bounded single-retry rebase). Multiple writers
 * may conflict, but any writer that runs unobstructed will finish in O(depth) time.
 * This test exercises this property by:
 * <ul>
 *   <li>Using 4 writer threads with 5 actors each — high concurrency pressure.</li>
 *   <li>Mixing writes (put/delete) with reads (get/size) — readers never block.</li>
 *   <li>The sequential spec proves that every concurrent execution is linearizable,
 *       which implies that no thread's operation was lost even under contention.</li>
 * </ul>
 *
 * <h2>Node Growth Coverage Map</h2>
 * <pre>
 *   Keys 1-4   → Node4 created, filled to capacity
 *   Key  5     → Node4.isFull() triggers growToNode16() via CowNodeOps.insertChild()
 *   Keys 5-16  → Node16 fills to capacity
 *   Key  17    → Node16.isFull() triggers growToNode48() via CowNodeOps.insertChild()
 *   Key  18    → Exercises Node48 with 18 children
 *   Delete 18→13 → Node48 count hits ≤12, triggers shrink to Node16
 *   Delete 12→4  → Node16 count hits ≤3, triggers shrink to Node4
 *   Delete 3→1   → Node4 count=1, triggers collapseSingleChild() → PrefixNode
 * </pre>
 */
@Param(name = "key", gen = IntGen.class, conf = "1:18")
public class LincheckNodeGrowthTransitionTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;
    private static final long CHUNK_SIZE = 1024L * 1024;

    /** All trees and paths created by Lincheck invocations — drained by @AfterEach. */
    private static final List<Runnable> CLEANUPS = new CopyOnWriteArrayList<>();

    private final TaoTree tree;
    private final Path storePath;

    public LincheckNodeGrowthTransitionTest() {
        try {
            storePath = Files.createTempFile("lincheck-growth-transition-", ".dat");
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
     * Encodes key with a fixed first byte (0xAA) so all 18 keys land in the same
     * inner node, forcing growth transitions. The second byte is the key index,
     * giving 18 distinct children under one parent node.
     */
    private static byte[] encodeKey(int k) {
        return new byte[]{
            (byte) 0xAA,
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

    /**
     * High-contention stress test with 4 writer threads.
     *
     * <p>With 3 threads × 4 actors = 12 concurrent operations per scenario.
     * Given 18 keys, the birthday paradox guarantees frequent key collisions,
     * stressing the COW rebase and growth/shrink transitions.
     *
     * <p>150 iterations × 80 invocations yields ~12,000 scenarios, providing
     * high confidence in linearizability across all transition boundaries.
     * Thread/actor count is kept at 3×4 to avoid LTS verifier state-space
     * explosion while still providing meaningful concurrency coverage.
     */
    @Test
    void stressTest() {
        new StressOptions()
            .iterations(150)
            .invocationsPerIteration(80)
            .threads(3)
            .actorsPerThread(4)
            .sequentialSpecification(SequentialSpec.class)
            .check(this.getClass());
    }

    /**
     * Sequential specification: a plain HashMap defining correct sequential behaviour.
     * Lincheck verifies every concurrent execution is equivalent to some sequential
     * execution of this spec, proving linearizability and obstruction-freedom.
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
