package org.taotree.fray;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.taotree.TaoTree;
import org.taotree.Value;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

/** Concurrent put while another thread calls {@code tree.sync()}. */
@ExtendWith(FrayTestExtension.class)
class FrayPutSyncRaceTest extends FrayTestBase {

    @FrayTest(iterations = 10)
    void putAndSync() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"),
                KeyLayout.of(KeyField.uint32("id")))) {

            KeyHandle.UInt32 ID = tree.keyUint32("id");
            // Pre-populate so the tree has at least one allocation before sync.
            try (var a = Arena.ofConfined(); var w = tree.write()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 1);
                w.put(kb, "seed", Value.ofInt(0));
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();

            Thread writer = new Thread(() -> {
                try (var a = Arena.ofConfined(); var w = tree.write()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 2);
                    w.put(kb, "x", Value.ofInt(42));
                } catch (Throwable t) { errors.add(t); }
            });
            Thread syncer = new Thread(() -> {
                try { tree.sync(); } catch (Throwable t) { errors.add(t); }
            });

            writer.start(); syncer.start();
            writer.join(); syncer.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var a = Arena.ofConfined(); var r = tree.read()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 2);
                assertEquals(Value.ofInt(42), r.get(kb, "x"),
                    "writer's put must be visible after both threads join");
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
