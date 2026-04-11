package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that node growth transitions (Node4 → Node16, Node16 → Node48)
 * are invisible to concurrent readers thanks to COW.
 *
 * <p>Pre-fills to just below the growth threshold, then races a writer
 * (which triggers the growth) against a reader (which looks up pre-existing
 * keys). The reader must see all pre-existing keys intact.
 */
@ExtendWith(FrayTestExtension.class)
class FrayNodeGrowthReadTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;

    /** Pre-fill 4 keys → Node4 full. Writer adds 5th → Node16 growth. Reader checks key 1. */
    @FrayTest(iterations = 10)
    void readDuringNode4ToNode16Growth() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            for (int i = 1; i <= 4; i++) {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(growthKey(i))).set(ValueLayout.JAVA_INT, 0, i);
                }
            }

            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var writer = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(growthKey(5))).set(ValueLayout.JAVA_INT, 0, 5);
                } catch (Throwable t) { errors.add(t); }
            });

            var reader = new Thread(() -> {
                try (var r = tree.read()) {
                    long leaf = r.lookup(growthKey(1));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key 1 lost during Node4→16 growth");
                    assertEquals(1, r.leafValue(leaf).get(ValueLayout.JAVA_INT, 0));
                } catch (Throwable t) { errors.add(t); }
            });

            writer.start(); reader.start();
            writer.join(); reader.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);
        } finally {
            deleteRecursively(tmpDir);
        }
    }

    /** Pre-fill 16 keys → Node16 full. Writer adds 17th → Node48 growth. Reader checks key 1. */
    @FrayTest(iterations = 10)
    void readDuringNode16ToNode48Growth() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            for (int i = 1; i <= 16; i++) {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(growthKey(i))).set(ValueLayout.JAVA_INT, 0, i);
                }
            }

            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var writer = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(growthKey(17))).set(ValueLayout.JAVA_INT, 0, 17);
                } catch (Throwable t) { errors.add(t); }
            });

            var reader = new Thread(() -> {
                try (var r = tree.read()) {
                    long leaf = r.lookup(growthKey(1));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key 1 lost during Node16→48 growth");
                    assertEquals(1, r.leafValue(leaf).get(ValueLayout.JAVA_INT, 0));
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
