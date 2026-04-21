package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the internal {@code TaoTree.Copier} walk via {@link TaoTree#compact()}
 * on a tree constructed to stress all Copier branches at once:
 *  overflow values, dictionary fields in the key, temporal history,
 *  attribute-level delete tombstones, and NodePtr class variety (prefix/4/16/48).
 */
class CopierTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }

    @Test
    void compactRichTreePreservesEverything() throws IOException {
        Path p = path();
        var layout = KeyLayout.of(
                KeyField.dict16("kingdom"),
                KeyField.uint32("id"));

        try (var tree = TaoTree.create(p, layout);
             var arena = Arena.ofConfined()) {
            var K = tree.keyDict16("kingdom");
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            // Populate many entities across several dict values — enough to grow
            // ART to Node48 in some branches.
            String[] kingdoms = {"Animalia", "Plantae", "Fungi", "Bacteria"};
            for (int i = 0; i < 80; i++) {
                kb.set(K, kingdoms[i % kingdoms.length]).set(ID, i);
                try (var w = tree.write()) {
                    w.put(kb, "n",     Value.ofInt(i));
                    // Short + long string to exercise overflow allocator.
                    w.put(kb, "label", Value.ofString("k" + i));
                    w.put(kb, "bio",   Value.ofString("x".repeat(40 + (i % 7))));
                }
            }

            // Temporal history on a couple of entities.
            kb.set(K, "Animalia").set(ID, 0);
            try (var w = tree.write()) {
                w.put(kb, "temp", Value.ofFloat64(20.0), 1_000L);
                w.put(kb, "temp", Value.ofFloat64(21.0), 2_000L);
                w.put(kb, "temp", Value.ofFloat64(22.5), 3_000L);
            }

            // Delete-attr tombstone on one entity.
            kb.set(K, "Plantae").set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "doomed", Value.ofString("alive"));
            }
            try (var w = tree.write()) {
                assertTrue(w.delete(kb, "doomed"));
            }

            tree.sync();
            tree.compact();

            // Post-compact reads via the same tree instance.
            try (var r = tree.read()) {
                for (int i = 0; i < 80; i++) {
                    kb.set(K, kingdoms[i % kingdoms.length]).set(ID, i);
                    assertEquals(Value.ofInt(i), r.get(kb, "n"));
                    assertEquals(Value.ofString("k" + i), r.get(kb, "label"));
                    assertEquals(Value.ofString("x".repeat(40 + (i % 7))), r.get(kb, "bio"));
                }

                kb.set(K, "Animalia").set(ID, 0);
                List<Double> seen = new ArrayList<>();
                r.history(kb, "temp", (fs, ls, vt, v) -> {
                    seen.add(v.asFloat64());
                    return true;
                });
                assertEquals(List.of(20.0, 21.0, 22.5), seen);

                kb.set(K, "Plantae").set(ID, 1);
                assertNull(r.get(kb, "doomed"));
            }
        }

        // Reopen: the compacted store round-trips on-disk.
        try (var tree = TaoTree.open(p, layout);
             var arena = Arena.ofConfined()) {
            var K = tree.keyDict16("kingdom");
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            String[] kingdoms = {"Animalia", "Plantae", "Fungi", "Bacteria"};
            try (var r = tree.read()) {
                for (int i = 0; i < 80; i++) {
                    kb.set(K, kingdoms[i % kingdoms.length]).set(ID, i);
                    Map<String, Value> all = r.getAll(kb);
                    assertEquals(Value.ofInt(i), all.get("n"));
                    assertEquals(Value.ofString("k" + i), all.get("label"));
                }
                kb.set(K, "Animalia").set(ID, 0);
                assertEquals(Value.ofFloat64(20.0), r.getAt(kb, "temp", 1_500L));
                assertEquals(Value.ofFloat64(21.0), r.getAt(kb, "temp", 2_500L));
            }
        }
    }
}
