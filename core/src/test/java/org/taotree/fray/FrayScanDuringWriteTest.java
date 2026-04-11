package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;
import org.taotree.layout.*;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@code forEach} / prefix {@code scan} on a read snapshot
 * are safe while a concurrent writer inserts or a compactor runs.
 *
 * <p>The reader's epoch pin must keep snapshot nodes alive, and the scan
 * must return a consistent set of entries from a single point-in-time
 * snapshot — no duplicates, no missing pre-populated keys, no corruption.
 */
@ExtendWith(FrayTestExtension.class)
class FrayScanDuringWriteTest extends FrayTestBase {

    private static final KeyLayout KEY_LAYOUT = KeyLayout.of(KeyField.uint32("id"));
    private static final LeafLayout LEAF_LAYOUT = LeafLayout.of(LeafField.int32("value"));
    private static final LeafHandle.Int32 VALUE = LEAF_LAYOUT.int32("value");

    /**
     * Pre-populate 5 keys. Race a full forEach scan against a writer that
     * inserts 2 new keys. The scan must return either 5, 6, or 7 entries
     * (depending on snapshot timing) — never duplicates or corruption.
     */
    @FrayTest(iterations = 10)
    void forEachDuringInsert() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LAYOUT, LEAF_LAYOUT,
                CHUNK, false)) {
            var ID = tree.keyUint32("id");

            // Pre-populate keys 1..5
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                try (var w = tree.write()) {
                    for (int i = 1; i <= 5; i++) {
                        kb.set(ID, i);
                        w.getOrCreate(kb).set(VALUE, i);
                    }
                }
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();

            var reader = new Thread(() -> {
                try (var r = tree.read()) {
                    var seen = new AtomicInteger();
                    r.forEach(leaf -> {
                        int val = leaf.get(VALUE);
                        assertTrue(val >= 1 && val <= 7,
                            "Unexpected scan value: " + val);
                        seen.incrementAndGet();
                        return true;
                    });
                    int count = seen.get();
                    // Snapshot is point-in-time: 5 pre-existing + 0..2 new
                    assertTrue(count >= 5 && count <= 7,
                        "forEach returned " + count + " entries (expected 5–7)");
                } catch (Throwable t) { errors.add(t); }
            });

            var writer = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var kb = tree.newKeyBuilder(arena);
                    try (var w = tree.write()) {
                        kb.set(ID, 6);
                        w.getOrCreate(kb).set(VALUE, 6);
                        kb.set(ID, 7);
                        w.getOrCreate(kb).set(VALUE, 7);
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            reader.start(); writer.start();
            reader.join(); writer.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);
        } finally {
            deleteRecursively(tmpDir);
        }
    }

    /**
     * Pre-populate 5 keys. Race a full forEach scan against compaction.
     * Compaction must not corrupt or reclaim nodes the reader is traversing.
     */
    @FrayTest(iterations = 10)
    void forEachDuringCompact() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LAYOUT, LEAF_LAYOUT,
                CHUNK, false)) {
            var ID = tree.keyUint32("id");

            // Pre-populate keys 1..5
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                try (var w = tree.write()) {
                    for (int i = 1; i <= 5; i++) {
                        kb.set(ID, i);
                        w.getOrCreate(kb).set(VALUE, i);
                    }
                }
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();

            var reader = new Thread(() -> {
                try (var r = tree.read()) {
                    var seen = new AtomicInteger();
                    r.forEach(leaf -> {
                        int val = leaf.get(VALUE);
                        assertTrue(val >= 1 && val <= 5,
                            "Unexpected value during compact: " + val);
                        seen.incrementAndGet();
                        return true;
                    });
                    assertEquals(5, seen.get(),
                        "Compaction must not remove entries visible to reader");
                } catch (Throwable t) { errors.add(t); }
            });

            var compactor = new Thread(() -> {
                try {
                    tree.compact();
                } catch (Throwable t) { errors.add(t); }
            });

            reader.start(); compactor.start();
            reader.join(); compactor.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
