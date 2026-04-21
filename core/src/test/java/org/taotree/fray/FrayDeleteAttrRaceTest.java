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

/** Concurrent {@code delete(entity, "a")} vs {@code put(entity, "a", v)}: final state must be coherent. */
@ExtendWith(FrayTestExtension.class)
class FrayDeleteAttrRaceTest extends FrayTestBase {

    @FrayTest(iterations = 10)
    void deleteAttrVsPut() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"),
                KeyLayout.of(KeyField.uint32("id")))) {

            KeyHandle.UInt32 ID = tree.keyUint32("id");
            try (var a = Arena.ofConfined(); var w = tree.write()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 1);
                w.put(kb, "a", Value.ofInt(1));
                w.put(kb, "b", Value.ofInt(2));
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();

            Thread t1 = new Thread(() -> {
                try (var a = Arena.ofConfined(); var w = tree.write()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 1);
                    w.delete(kb, "a");
                } catch (Throwable t) { errors.add(t); }
            });
            Thread t2 = new Thread(() -> {
                try (var a = Arena.ofConfined(); var w = tree.write()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 1);
                    w.put(kb, "a", Value.ofInt(99));
                } catch (Throwable t) { errors.add(t); }
            });

            t1.start(); t2.start();
            t1.join(); t2.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var a = Arena.ofConfined(); var r = tree.read()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 1);
                Value vA = r.get(kb, "a");
                // Either delete won (vA == null) or put won (vA == 99). Anything else is corruption.
                assertTrue(vA == null || vA.equals(Value.ofInt(99)),
                    "attr 'a' final state corrupt: " + vA);
                // Sibling attr 'b' must always survive.
                assertEquals(Value.ofInt(2), r.get(kb, "b"), "sibling attr 'b' lost");
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
