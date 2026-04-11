package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies epoch-based reclamation safety: a reader that entered an epoch
 * before compaction must still see valid data after the compactor runs.
 *
 * <p>Pre-populates 3 keys (0→0, 1→100, 2→200), then races a reader against
 * a writer that overwrites key 0 (0→999) and compacts. The reader's epoch pin
 * must prevent the reclaimer from freeing its snapshot nodes.
 *
 * <p>Key distinction: keys 1 and 2 are <b>never modified by the writer</b>,
 * so the reader must always see their original values. Only key 0 may appear
 * as either its original value (0) or the writer's value (999) depending on
 * which snapshot the reader captured.
 */
@ExtendWith(FrayTestExtension.class)
class FrayEpochSafetyTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    @FrayTest(iterations = 10)
    void readerPinSurvivesCompaction() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            for (int i = 0; i < 3; i++) {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(i))).set(ValueLayout.JAVA_LONG, 0, i * 100L);
                }
            }

            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var reader = new Thread(() -> {
                try (var r = tree.read()) {
                    for (int i = 0; i < 3; i++) {
                        long leaf = r.lookup(intKey(i));
                        assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " not found");
                        long val = r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0);
                        if (i == 0) {
                            // Key 0 may reflect either the original or the writer's update
                            assertTrue(val == 0L || val == 999L,
                                "Corrupted value for key 0: " + val);
                        } else {
                            // Keys 1 and 2 are never modified — must be exact
                            assertEquals(i * 100L, val,
                                "Corrupted value for unmodified key " + i + ": " + val);
                        }
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            var writer = new Thread(() -> {
                try {
                    try (var w = tree.write()) {
                        w.leafValue(w.getOrCreate(intKey(0))).set(ValueLayout.JAVA_LONG, 0, 999L);
                    }
                    tree.compact();
                } catch (Throwable t) { errors.add(t); }
            });

            reader.start(); writer.start();
            reader.join(); writer.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
