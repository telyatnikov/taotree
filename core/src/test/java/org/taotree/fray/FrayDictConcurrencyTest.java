package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoDictionary;
import org.taotree.TaoTree;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies concurrent {@link TaoDictionary} intern (write) and resolve (read).
 *
 * <p>Dict uses its own lock for intern and is lock-free for resolve.
 * Each test uses exactly 2 threads with 1–2 operations each.
 */
@ExtendWith(FrayTestExtension.class)
class FrayDictConcurrencyTest extends FrayTestBase {

    /** Two threads intern distinct strings → both must get unique positive codes. */
    @FrayTest(iterations = 10)
    void concurrentInternDistinct() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.forDictionaries(tmpDir.resolve("t.tao"))) {
            var dict = TaoDictionary.u16(tree);
            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
            int[] codeA = new int[1], codeB = new int[1];

            var a = new Thread(() -> {
                try { codeA[0] = dict.intern("alpha"); } catch (Throwable t) { errors.add(t); }
            });
            var b = new Thread(() -> {
                try { codeB[0] = dict.intern("beta"); } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            assertTrue(codeA[0] > 0, "alpha code non-positive");
            assertTrue(codeB[0] > 0, "beta code non-positive");
            assertNotEquals(codeA[0], codeB[0], "Distinct strings got same code");
        } finally {
            deleteRecursively(tmpDir);
        }
    }

    /** Two threads intern the SAME string → both must get the same code (idempotent). */
    @FrayTest(iterations = 10)
    void concurrentInternSameString() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.forDictionaries(tmpDir.resolve("t.tao"))) {
            var dict = TaoDictionary.u16(tree);
            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
            int[] codeA = new int[1], codeB = new int[1];

            var a = new Thread(() -> {
                try { codeA[0] = dict.intern("shared"); } catch (Throwable t) { errors.add(t); }
            });
            var b = new Thread(() -> {
                try { codeB[0] = dict.intern("shared"); } catch (Throwable t) { errors.add(t); }
            });

            a.start(); b.start();
            a.join(); b.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            assertTrue(codeA[0] > 0);
            assertEquals(codeA[0], codeB[0], "Same string must produce same code");
        } finally {
            deleteRecursively(tmpDir);
        }
    }

    /**
     * One thread interns a string while another resolves it concurrently.
     * Resolve must return either -1 (not yet visible) or the correct positive code.
     * It must never return 0 (the uninitialized leaf value).
     */
    @FrayTest(iterations = 10)
    void internWhileResolve() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.forDictionaries(tmpDir.resolve("t.tao"))) {
            var dict = TaoDictionary.u16(tree);
            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
            int[] internCode = new int[1];
            int[] resolveCode = new int[1];

            var writer = new Thread(() -> {
                try { internCode[0] = dict.intern("gamma"); } catch (Throwable t) { errors.add(t); }
            });

            var reader = new Thread(() -> {
                try {
                    resolveCode[0] = dict.resolve("gamma");
                } catch (Throwable t) { errors.add(t); }
            });

            writer.start(); reader.start();
            writer.join(); reader.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            assertTrue(internCode[0] > 0, "intern returned non-positive code");
            // resolve must return either -1 (not yet published) or the interned code
            assertTrue(resolveCode[0] == -1 || resolveCode[0] == internCode[0],
                "resolve returned unexpected code " + resolveCode[0]
                    + " (expected -1 or " + internCode[0] + ")");
            // Must never return 0 (uninitialized leaf)
            assertNotEquals(0, resolveCode[0], "resolve returned 0 — saw uninitialized leaf");
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
