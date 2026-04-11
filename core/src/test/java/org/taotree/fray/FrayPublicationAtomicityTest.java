package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@code PublicationState} (root + size) is always observed
 * atomically by readers.
 *
 * <p>Writer inserts a single key into an empty tree; reader takes one snapshot.
 * The reader must see either (size=0, key absent) or (size=1, key present) —
 * never a torn state like (size=1, key absent) or (size=0, key present).
 *
 * <p>Both directions are checked because {@code PublicationState} packs root
 * and size into a single object reference published via {@code VarHandle.setRelease}.
 */
@ExtendWith(FrayTestExtension.class)
class FrayPublicationAtomicityTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;

    @FrayTest(iterations = 10)
    void sizeAndLookupAreConsistent() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var writer = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(42))).set(ValueLayout.JAVA_INT, 0, 42);
                } catch (Throwable t) { errors.add(t); }
            });

            var reader = new Thread(() -> {
                try (var r = tree.read()) {
                    long size = r.size();
                    boolean found = r.lookup(intKey(42)) != TaoTree.NOT_FOUND;
                    // Exact atomicity: size and lookup must agree.
                    // ReadScope snapshots root+size from a single PublicationState,
                    // so the only valid states are (0, absent) or (1, present).
                    if (size == 0) {
                        assertFalse(found, "size=0 but key found — torn publication");
                    } else {
                        assertEquals(1, size, "size should be exactly 1, got " + size);
                        assertTrue(found, "size=1 but key absent — torn publication");
                    }
                } catch (Throwable t) { errors.add(t); }
            });

            writer.start(); reader.start();
            writer.join(); reader.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
