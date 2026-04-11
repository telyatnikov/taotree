package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that data survives sync + reopen under concurrent writers.
 *
 * <p>Writer inserts while another thread syncs. After both finish,
 * the tree is closed and reopened — all data written before sync must
 * be present, and data written concurrently with sync may or may not
 * be persisted (but must not cause corruption on reopen).
 */
@ExtendWith(FrayTestExtension.class)
class FrayReopenAfterSyncTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    @FrayTest(iterations = 10)
    void reopenAfterConcurrentSync() throws Exception {
        Path tmpDir = newTempDir();
        Path treePath = tmpDir.resolve("t.tao");
        try {
            // Phase 1: populate keys 1..3, then race writer(key 4) vs sync
            try (var tree = TaoTree.create(treePath, KEY_LEN, VALUE_SIZE, CHUNK, false)) {
                for (int i = 1; i <= 3; i++) {
                    try (var w = tree.write()) {
                        w.leafValue(w.getOrCreate(intKey(i)))
                            .set(ValueLayout.JAVA_LONG, 0, i * 100L);
                    }
                }

                var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

                var writer = new Thread(() -> {
                    try (var w = tree.write()) {
                        w.leafValue(w.getOrCreate(intKey(4)))
                            .set(ValueLayout.JAVA_LONG, 0, 400L);
                    } catch (Throwable t) { errors.add(t); }
                });

                var syncer = new Thread(() -> {
                    try { tree.sync(); } catch (Throwable t) { errors.add(t); }
                });

                writer.start(); syncer.start();
                writer.join(); syncer.join();
                assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

                // Final sync to ensure everything is on disk
                tree.sync();
            }

            // Phase 2: reopen and verify
            try (var tree = TaoTree.open(treePath)) {
                try (var r = tree.read()) {
                    // Keys 1..3 must always be present (written before the race)
                    for (int i = 1; i <= 3; i++) {
                        long leaf = r.lookup(intKey(i));
                        assertNotEquals(TaoTree.NOT_FOUND, leaf,
                            "Key " + i + " lost after reopen");
                        long val = r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0);
                        assertEquals(i * 100L, val,
                            "Key " + i + " value corrupted after reopen");
                    }
                    // Key 4 was written by the concurrent writer — should be present
                    // since we did a final sync() after joining both threads
                    long leaf4 = r.lookup(intKey(4));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf4,
                        "Key 4 lost after reopen");
                    assertEquals(400L, r.leafValue(leaf4).get(ValueLayout.JAVA_LONG, 0),
                        "Key 4 value corrupted after reopen");
                }
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
