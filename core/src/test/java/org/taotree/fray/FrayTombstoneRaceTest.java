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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrent timestamped-tombstone vs timestamped-put on the same attribute.
 *
 * <p>Writer A retracts {@code "a"} at {@code ts=20}; Writer B puts
 * {@code "a"=99} at {@code ts=30}. Whoever commits last wins the current
 * CHAMP state — but both runs must be present in history, the sibling
 * attribute {@code "b"} must survive, and {@code getAt(15)} must still
 * see the original value 1 (history never rewrites the past).
 */
@ExtendWith(FrayTestExtension.class)
class FrayTombstoneRaceTest extends FrayTestBase {

    @FrayTest(iterations = 10)
    void tombstoneVsPut() throws Exception {
        Path tmpDir = newTempDir();
        try (var tree = TaoTree.create(tmpDir.resolve("t.tao"),
                KeyLayout.of(KeyField.uint32("id")))) {

            KeyHandle.UInt32 ID = tree.keyUint32("id");
            try (var a = Arena.ofConfined(); var w = tree.write()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 1);
                w.put(kb, "a", Value.ofInt(1), 10L);
                w.put(kb, "b", Value.ofInt(2), 10L);
            }

            var errors = new ConcurrentLinkedQueue<Throwable>();

            Thread retractor = new Thread(() -> {
                try (var a = Arena.ofConfined(); var w = tree.write()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 1);
                    w.delete(kb, "a", 20L);
                } catch (Throwable t) { errors.add(t); }
            });
            Thread putter = new Thread(() -> {
                try (var a = Arena.ofConfined(); var w = tree.write()) {
                    var kb = tree.newKeyBuilder(a);
                    kb.set(ID, 1);
                    w.put(kb, "a", Value.ofInt(99), 30L);
                } catch (Throwable t) { errors.add(t); }
            });

            retractor.start(); putter.start();
            retractor.join(); putter.join();
            assertTrue(errors.isEmpty(), () -> "Errors: " + errors);

            try (var a = Arena.ofConfined(); var r = tree.read()) {
                var kb = tree.newKeyBuilder(a);
                kb.set(ID, 1);

                // History before ts=20 is untouchable — always sees original.
                assertEquals(Value.ofInt(1), r.getAt(kb, "a", 15L),
                        "past history must never be rewritten");

                // Sibling attribute must always survive both scheduling orders.
                assertEquals(Value.ofInt(2), r.get(kb, "b"),
                        "sibling attribute 'b' lost");

                // Current (latest) state: 99 must always be visible — it
                // was written at ts=30, which is later than the tombstone
                // at ts=20 regardless of commit order.
                Value cur = r.get(kb, "a");
                assertEquals(Value.ofInt(99), cur,
                        "latest put (ts=30) must win current state");

                // At ts=25 (tombstone window): null — the retraction
                // covers [20, 30) regardless of commit order.
                assertEquals(null, r.getAt(kb, "a", 25L),
                        "tombstone window [20,30) must read null");
            }
        } finally {
            deleteRecursively(tmpDir);
        }
    }
}
