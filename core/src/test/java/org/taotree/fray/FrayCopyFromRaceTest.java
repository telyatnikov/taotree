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

/**
 * Source pre-populated; T1 copies src→dst, T2 reads from src concurrently.
 * After join, dst must contain a snapshot of src and src must remain intact.
 */
@ExtendWith(FrayTestExtension.class)
class FrayCopyFromRaceTest extends FrayTestBase {

    @FrayTest(iterations = 10)
    void copyFromWhileReading() throws Exception {
        Path srcDir = newTempDir();
        Path dstDir = newTempDir();
        try (var src = TaoTree.create(srcDir.resolve("s.tao"),
                KeyLayout.of(KeyField.uint32("id")));
             var dst = TaoTree.create(dstDir.resolve("d.tao"),
                KeyLayout.of(KeyField.uint32("id")))) {

            KeyHandle.UInt32 SID = src.keyUint32("id");
            KeyHandle.UInt32 DID = dst.keyUint32("id");

            try (var a = Arena.ofConfined(); var w = src.write()) {
                var kb = src.newKeyBuilder(a);
                kb.set(SID, 7);
                w.put(kb, "v", Value.ofInt(123));
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();
            AtomicReference<Value> srcReadDuringCopy = new AtomicReference<>();

            Thread copier = new Thread(() -> {
                try (var w = dst.write(); var r = src.read()) {
                    w.copyFrom(r);
                } catch (Throwable t) { errors.add(t); }
            });
            Thread reader = new Thread(() -> {
                try (var a = Arena.ofConfined(); var r = src.read()) {
                    var kb = src.newKeyBuilder(a);
                    kb.set(SID, 7);
                    srcReadDuringCopy.set(r.get(kb, "v"));
                } catch (Throwable t) { errors.add(t); }
            });

            copier.start(); reader.start();
            copier.join(); reader.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            // Concurrent src reader must always see the pre-populated value.
            assertEquals(Value.ofInt(123), srcReadDuringCopy.get(),
                "src reader saw stale/null during copyFrom: " + srcReadDuringCopy.get());

            // dst must now contain the snapshot.
            try (var a = Arena.ofConfined(); var r = dst.read()) {
                var kb = dst.newKeyBuilder(a);
                kb.set(DID, 7);
                assertEquals(Value.ofInt(123), r.get(kb, "v"),
                    "dst missing copied value");
            }
        } finally {
            deleteRecursively(srcDir);
            deleteRecursively(dstDir);
        }
    }
}
