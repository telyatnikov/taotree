package org.taotree;

import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Test;

import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.Map;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.Node4;

/**
 * Lincheck test targeting ART node growth and shrink under concurrency.
 *
 * <p>Uses a wider key range (1..50) so that concurrent insertions and deletions
 * force node transitions: Node4 → Node16 → Node48 → Node256 and back.
 * Node growth/shrink is one of the hardest parts to get right in a lock-free
 * ART because it involves replacing an inner node pointer atomically while
 * concurrent readers may still hold references to the old node.
 *
 * <p>The key encodes a single byte in the critical position (varying the
 * second byte) so that all keys share the same first byte, forcing them
 * into the same inner node and triggering growth/shrink transitions.
 */
@Param(name = "key", gen = IntGen.class, conf = "1:50")
public class LincheckNodeGrowthTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;

    private final TaoTree tree = TaoTree.open(KEY_LEN, VALUE_SIZE);

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
            .invocationsPerIteration(1000)
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
