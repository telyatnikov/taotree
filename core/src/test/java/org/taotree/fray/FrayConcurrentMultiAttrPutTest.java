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
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrent puts to <em>different</em> attributes on the <em>same</em> entity
 * must preserve both writes (CHAMP merge invariant).
 */
@ExtendWith(FrayTestExtension.class)
class FrayConcurrentMultiAttrPutTest extends FrayTestBase {

    @FrayTest(iterations = 10)
    void concurrentMultiAttrPut() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"),
                KeyLayout.of(KeyField.uint32("id")))) {

            KeyHandle.UInt32 ID = tree.keyUint32("id");
            try (var a = Arena.ofConfined(); var w = tree.write()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 42);
                w.put(kb, "seed", Value.ofInt(0));
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();

            Thread t1 = new Thread(() -> {
                try (var a = Arena.ofConfined(); var w = tree.write()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 42);
                    w.put(kb, "a", Value.ofInt(1));
                } catch (Throwable t) { errors.add(t); }
            });
            Thread t2 = new Thread(() -> {
                try (var a = Arena.ofConfined(); var w = tree.write()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 42);
                    w.put(kb, "b", Value.ofInt(2));
                } catch (Throwable t) { errors.add(t); }
            });

            t1.start(); t2.start();
            t1.join(); t2.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var a = Arena.ofConfined(); var r = tree.read()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 42);
                Map<String, Value> all = r.getAll(kb);
                assertEquals(Value.ofInt(1), all.get("a"), "lost write to attr 'a': " + all);
                assertEquals(Value.ofInt(2), all.get("b"), "lost write to attr 'b': " + all);
                assertEquals(Value.ofInt(0), all.get("seed"), "seed gone: " + all);
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
