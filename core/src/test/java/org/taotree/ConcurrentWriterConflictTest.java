package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deterministic two-thread concurrent-writer test that forces
 * {@code WriteScope.deferredCommitImpl}'s conflict / rebase paths
 * ({@code rebaseCompute} and {@code rebaseComputeTemporal}) to execute.
 */
class ConcurrentWriterConflictTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void concurrentWritersOnDisjointEntitiesBothSurvive() throws Exception {
        try (var tree = TaoTree.create(path(), layout())) {
            int n = 64;

            // Pre-populate so snapshot captures a non-empty root.
            try (var arena = Arena.ofConfined()) {
                var ID = tree.keyUint32("id");
                var kb = tree.newKeyBuilder(arena);
                try (var w = tree.write()) {
                    for (int i = 0; i < n; i++) {
                        kb.set(ID, i);
                        w.put(kb, "seed", Value.ofInt(i));
                    }
                }
            }

            // Latches force an interleaving where T2 publishes while T1
            // has an open scope with pending mutations → T1 must rebase.
            var t2Published = new CountDownLatch(1);
            var t1DoneMutating = new CountDownLatch(1);
            var t2Error = new AtomicReference<Throwable>();
            var t1Error = new AtomicReference<Throwable>();

            Thread t2 = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var ID = tree.keyUint32("id");
                    var kb = tree.newKeyBuilder(arena);
                    t1DoneMutating.await();
                    try (var w = tree.write()) {
                        for (int i = 0; i < n; i++) {
                            kb.set(ID, 1000 + i);
                            w.put(kb, "t2", Value.ofInt(i));
                        }
                    }
                    t2Published.countDown();
                } catch (Throwable ex) {
                    t2Error.set(ex);
                    t2Published.countDown();
                }
            }, "t2");

            Thread t1 = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var ID = tree.keyUint32("id");
                    var kb = tree.newKeyBuilder(arena);
                    try (var w = tree.write()) {
                        for (int i = 0; i < n; i++) {
                            kb.set(ID, 2000 + i);
                            w.put(kb, "t1", Value.ofInt(i));
                        }
                        t1DoneMutating.countDown();
                        t2Published.await();
                    }
                } catch (Throwable ex) {
                    t1Error.set(ex);
                }
            }, "t1");

            t2.start();
            t1.start();
            t1.join(30_000);
            t2.join(30_000);
            assertNull(t1Error.get(), "t1 error: " + t1Error.get());
            assertNull(t2Error.get(), "t2 error: " + t2Error.get());

            // Both writers' data must survive after rebase.
            try (var r = tree.read();
                 var arena = Arena.ofConfined()) {
                var ID = tree.keyUint32("id");
                var kb = tree.newKeyBuilder(arena);
                for (int i = 0; i < n; i++) {
                    kb.set(ID, 1000 + i);
                    assertEquals(Value.ofInt(i), r.get(kb, "t2"));
                    kb.set(ID, 2000 + i);
                    assertEquals(Value.ofInt(i), r.get(kb, "t1"));
                    kb.set(ID, i);
                    assertEquals(Value.ofInt(i), r.get(kb, "seed"));
                }
            }
        }
    }

    @Test
    void concurrentWritersOverlappingEntitiesBothSurvive() throws Exception {
        try (var tree = TaoTree.create(path(), layout())) {
            // Seed entity 5 with attribute "seed".
            try (var arena = Arena.ofConfined()) {
                var ID = tree.keyUint32("id");
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 5);
                try (var w = tree.write()) {
                    w.put(kb, "seed", Value.ofInt(1));
                }
            }

            var t2Published = new CountDownLatch(1);
            var t1DoneMutating = new CountDownLatch(1);
            var t2Error = new AtomicReference<Throwable>();
            var t1Error = new AtomicReference<Throwable>();

            Thread t2 = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var ID = tree.keyUint32("id");
                    var kb = tree.newKeyBuilder(arena);
                    kb.set(ID, 5);
                    t1DoneMutating.await();
                    try (var w = tree.write()) {
                        w.put(kb, "t2", Value.ofString("t2-val"));
                    }
                    t2Published.countDown();
                } catch (Throwable ex) {
                    t2Error.set(ex);
                    t2Published.countDown();
                }
            }, "t2");

            Thread t1 = new Thread(() -> {
                try (var arena = Arena.ofConfined()) {
                    var ID = tree.keyUint32("id");
                    var kb = tree.newKeyBuilder(arena);
                    kb.set(ID, 5);
                    try (var w = tree.write()) {
                        w.put(kb, "t1", Value.ofString("t1-val"));
                        t1DoneMutating.countDown();
                        t2Published.await();
                    }
                } catch (Throwable ex) {
                    t1Error.set(ex);
                }
            }, "t1");

            t2.start();
            t1.start();
            t1.join(30_000);
            t2.join(30_000);
            assertNull(t1Error.get());
            assertNull(t2Error.get());

            try (var r = tree.read();
                 var arena = Arena.ofConfined()) {
                var ID = tree.keyUint32("id");
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 5);
                var all = r.getAll(kb);
                assertEquals(Value.ofInt(1), all.get("seed"));
                assertEquals(Value.ofString("t1-val"), all.get("t1"));
                assertEquals(Value.ofString("t2-val"), all.get("t2"));
            }
        }
    }
}
