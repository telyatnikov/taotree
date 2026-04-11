package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the SlabAllocator's lock-free CAS alloc/free paths and
 * the compact-vs-writer interleaving that previously caused lost writes.
 *
 * <p>The {@code Thread.yield()} schedule points in {@code casInSlab()} and
 * {@code free()} let Fray control the interleaving of the off-heap
 * VarHandle CAS operations.
 */
@ExtendWith(FrayTestExtension.class)
class FraySlabAllocatorTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;

    /** Two concurrent writers both trigger slab allocation through COW. */
    @FrayTest(iterations = 10)
    void concurrentSlabAllocation() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var a = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(1))).set(ValueLayout.JAVA_INT, 0, 1);
                } catch (Throwable t) { errors.add(t); }
            });

            var b = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(2))).set(ValueLayout.JAVA_INT, 0, 2);
                } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var r = tree.read()) {
                assertEquals(2, r.size());
                assertEquals(1, r.leafValue(r.lookup(intKey(1))).get(ValueLayout.JAVA_INT, 0));
                assertEquals(2, r.leafValue(r.lookup(intKey(2))).get(ValueLayout.JAVA_INT, 0));
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }

    /**
     * One thread inserts while another deletes + compacts.
     * This previously caused lost writes because compact() used writeLock
     * while WriteScope used commitLock — separate locks allowed compact
     * to overwrite a concurrent writer's publication.
     */
    @FrayTest(iterations = 10)
    void insertWhileDeleteAndCompact() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            // Pre-populate keys 0..4 in the main thread
            for (int i = 0; i < 5; i++) {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(i))).set(ValueLayout.JAVA_INT, 0, i);
                }
            }

            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            // Thread A: insert key 10
            var a = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(10))).set(ValueLayout.JAVA_INT, 0, 10);
                } catch (Throwable t) { errors.add(t); }
            });

            // Thread B: delete key 0, then compact
            var b = new Thread(() -> {
                try {
                    try (var w = tree.write()) {
                        w.delete(intKey(0));
                    }
                    tree.compact();
                } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            // Key 10 must exist regardless of interleaving
            try (var r = tree.read()) {
                long ref = r.lookup(intKey(10));
                assertTrue(ref != TaoTree.NOT_FOUND, "Key 10 was lost — compact overwrote writer's publication");
                assertEquals(10, r.leafValue(ref).get(ValueLayout.JAVA_INT, 0));
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
