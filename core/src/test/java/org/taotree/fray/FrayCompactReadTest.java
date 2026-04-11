package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@code tree.compact()} does not corrupt data visible to a
 * concurrent reader holding an active ReadScope.
 *
 * <p>Pre-populates 3 keys, then races a reader against a compactor.
 * The reader's epoch pin prevents reclamation of its snapshot nodes.
 */
@ExtendWith(FrayTestExtension.class)
class FrayCompactReadTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    @FrayTest(iterations = 10)
    void readScopeAcrossCompact() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            for (int i = 0; i < 3; i++) {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(i))).set(ValueLayout.JAVA_LONG, 0, i * 7L);
                }
            }

            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var reader = new Thread(() -> {
                try (var r = tree.read()) {
                    assertEquals(3, r.size());
                    for (int i = 0; i < 3; i++) {
                        long leaf = r.lookup(intKey(i));
                        assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " lost");
                        assertEquals(i * 7L, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            var compactor = new Thread(() -> {
                try { tree.compact(); } catch (Throwable t) { errors.add(t); }
            });

            reader.start(); compactor.start();
            reader.join(); compactor.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
