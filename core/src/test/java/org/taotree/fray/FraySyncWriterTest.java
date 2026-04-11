package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;

import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@code tree.sync()} and concurrent writer commits are
 * properly serialised via commitLock. Previously, sync() only held
 * writeLock (not commitLock), so buildCommitData/gatherMetadata could
 * read a torn (root, size) pair while a writer published concurrently.
 */
@ExtendWith(FrayTestExtension.class)
class FraySyncWriterTest extends FrayTestBase {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 4;

    /**
     * One thread inserts while another syncs.
     * After both complete, the inserted key must be present.
     */
    @FrayTest(iterations = 10)
    void insertWhileSync() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"), KEY_LEN, VALUE_SIZE, CHUNK, false)) {
            // Pre-populate a key so there's data to sync
            try (var w = tree.write()) {
                w.leafValue(w.getOrCreate(intKey(0))).set(ValueLayout.JAVA_INT, 0, 0);
            }

            var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

            var writer = new Thread(() -> {
                try (var w = tree.write()) {
                    w.leafValue(w.getOrCreate(intKey(5))).set(ValueLayout.JAVA_INT, 0, 5);
                } catch (Throwable t) { errors.add(t); }
            });

            var syncer = new Thread(() -> {
                try { tree.sync(); } catch (Throwable t) { errors.add(t); }
            });

            writer.start(); syncer.start();
            writer.join(); syncer.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var r = tree.read()) {
                long ref = r.lookup(intKey(5));
                assertTrue(ref != TaoTree.NOT_FOUND, "Key 5 was lost — sync interfered with writer");
                assertEquals(5, r.leafValue(ref).get(ValueLayout.JAVA_INT, 0));
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
