package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies node shrink transitions (Node48→16, Node16→4) under concurrent
 * readers. Shrinking rewrites parent pointers; a reader traversing the tree
 * must still reach all live keys.
 *
 * <p>Pre-populates enough keys to build a Node48, then deletes keys to trigger
 * shrink while a reader looks up surviving keys.
 */
@ExtendWith(FrayTestExtension.class)
class FrayShrinkReadTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    /**
     * Populate 17 keys (forces Node16→Node48 growth), then delete 13 keys to
     * trigger Node48→Node16→Node4 shrink transitions. A concurrent reader
     * verifies keys 1–4 (survivors) are always reachable.
     */
    @FrayTest(iterations = 10)
    void shrinkWhileReading() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            // Pre-populate keys 1..17 (forces Node48)
            for (int i = 1; i <= 17; i++) {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(i)))
                        .set(ValueLayout.JAVA_LONG, 0, i * 10L);
                }
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();

            // Reader: look up surviving keys (1..4)
            var reader = new Thread(() -> {
                try (var r = tree.read()) {
                    for (int i = 1; i <= 4; i++) {
                        long leaf = r.lookup(intKey(i));
                        assertNotEquals(TaoTree.NOT_FOUND, leaf,
                            "Survivor key " + i + " not found during shrink");
                        long val = r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0);
                        assertEquals(i * 10L, val,
                            "Corrupted value for survivor key " + i);
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            // Writer: delete keys 5..17 → triggers shrink chain
            var writer = new Thread(() -> {
                try (var w = tree.write()) {
                    for (int i = 5; i <= 17; i++) {
                        w.delete(intKey(i));
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            reader.start(); writer.start();
            reader.join(); writer.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            // After both complete: verify survivors persisted
            try (var r = tree.read()) {
                assertEquals(4, r.size(), "Should have exactly 4 surviving keys");
                for (int i = 1; i <= 4; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf,
                        "Post-shrink key " + i + " not found");
                    assertEquals(i * 10L, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
