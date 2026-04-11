package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that per-writer {@code WriterArena} instances allocate from
 * non-overlapping page ranges in the shared {@code ChunkStore}.
 *
 * <p>Two concurrent writers each insert one key (triggering arena bulk
 * page reservation from the ChunkStore). After both complete, both keys
 * must be present with correct values.
 */
@ExtendWith(FrayTestExtension.class)
class FrayWriterArenaTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    @FrayTest(iterations = 10)
    void twoWritersSeparateArenas() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var a = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(0x01_000001))).set(ValueLayout.JAVA_LONG, 0, 1L);
                } catch (Throwable t) { errors.add(t); }
            });

            var b = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(0x02_000001))).set(ValueLayout.JAVA_LONG, 0, 2L);
                } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var r = tree.read()) {
                assertEquals(2, r.size());
                assertEquals(1L, r.leafValue(r.lookup(intKey(0x01_000001))).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(2L, r.leafValue(r.lookup(intKey(0x02_000001))).get(ValueLayout.JAVA_LONG, 0));
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
