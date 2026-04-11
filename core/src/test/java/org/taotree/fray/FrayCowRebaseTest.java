package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Targets the COW rebase race: two writers build optimistic private trees
 * concurrently, and one must rebase against the other's publication.
 * Each writer does a single write-scope with one key — the minimum needed
 * to trigger rebase.
 */
@ExtendWith(FrayTestExtension.class)
class FrayCowRebaseTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    @FrayTest(iterations = 10)
    void twoWritersDisjointKeys() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var a = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(1))).set(ValueLayout.JAVA_LONG, 0, 10L);
                } catch (Throwable t) { errors.add(t); }
            });
            var b = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(2))).set(ValueLayout.JAVA_LONG, 0, 20L);
                } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var r = tree.read()) {
                assertEquals(2, r.size());
                assertEquals(10L, r.leafValue(r.lookup(intKey(1))).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(20L, r.leafValue(r.lookup(intKey(2))).get(ValueLayout.JAVA_LONG, 0));
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }

    @FrayTest(iterations = 10)
    void twoWritersSameKey() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var a = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(1))).set(ValueLayout.JAVA_LONG, 0, 100L);
                } catch (Throwable t) { errors.add(t); }
            });
            var b = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(1))).set(ValueLayout.JAVA_LONG, 0, 200L);
                } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var r = tree.read()) {
                assertEquals(1, r.size());
                long val = r.leafValue(r.lookup(intKey(1))).get(ValueLayout.JAVA_LONG, 0);
                assertTrue(val == 100L || val == 200L, "Unexpected value: " + val);
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
