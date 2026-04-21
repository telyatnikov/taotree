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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Pre-populate one entity. T1 puts a new value, T2 reads. T2 must see one of the legal values. */
@ExtendWith(FrayTestExtension.class)
class FrayPutGetRaceTest extends FrayTestBase {

    @FrayTest(iterations = 10)
    void putGetRace() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"),
                KeyLayout.of(KeyField.uint32("id")))) {

            KeyHandle.UInt32 ID = tree.keyUint32("id");
            try (var a = Arena.ofConfined(); var w = tree.write()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 1);
                w.put(kb, "x", Value.ofInt(1));
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();
            AtomicReference<Value> readResult = new AtomicReference<>();

            Thread writer = new Thread(() -> {
                try (var a = Arena.ofConfined(); var w = tree.write()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 1);
                    w.put(kb, "x", Value.ofInt(2));
                } catch (Throwable t) { errors.add(t); }
            });
            Thread reader = new Thread(() -> {
                try (var a = Arena.ofConfined(); var r = tree.read()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 1);
                    readResult.set(r.get(kb, "x"));
                } catch (Throwable t) { errors.add(t); }
            });

            writer.start(); reader.start();
            writer.join(); reader.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            Value v = readResult.get();
            assertNotNull(v, "reader saw null — pre-populated entity must always be visible");
            assertTrue(v.equals(Value.ofInt(1)) || v.equals(Value.ofInt(2)),
                "reader saw unexpected value: " + v);
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
