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
 * Extra compaction coverage: many-entity survival, temporal history preservation,
 * tombstone persistence across compact+reopen.
 */
class CompactionTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void compactManyEntitiesWithDeletesPreservesExpectedSet() throws IOException {
        Path p = path();
        int n = 1000;
        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            try (var w = tree.write()) {
                for (int i = 0; i < n; i++) {
                    kb.set(ID, i);
                    w.put(kb, "v", Value.ofInt(i));
                }
            }

            // Delete half the entities (odds).
            try (var w = tree.write()) {
                for (int i = 1; i < n; i += 2) {
                    kb.set(ID, i);
                    w.delete(kb);
                }
            }
            tree.sync();
            tree.compact();

            try (var r = tree.read()) {
                for (int i = 0; i < n; i++) {
                    kb.set(ID, i);
                    if (i % 2 == 0) {
                        assertEquals(Value.ofInt(i), r.get(kb, "v"), "survivor " + i);
                    } else {
                        assertNull(r.get(kb, "v"), "deleted " + i);
                    }
                }
            }
        }

        // Reopen to confirm compact survived.
        try (var tree = TaoTree.open(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var r = tree.read()) {
                for (int i = 0; i < n; i += 2) {
                    kb.set(ID, i);
                    assertEquals(Value.ofInt(i), r.get(kb, "v"));
                }
            }
        }
    }

    @Test
    void compactPreservesTemporalHistory() throws IOException {
        Path p = path();
        long[] times = {100, 200, 300, 400, 500};
        Value[] vals = {Value.ofInt(1), Value.ofInt(2), Value.ofInt(3),
                        Value.ofInt(4), Value.ofInt(5)};

        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 7);
            try (var w = tree.write()) {
                for (int i = 0; i < times.length; i++) {
                    w.put(kb, "metric", vals[i], times[i]);
                }
            }
            tree.sync();
            tree.compact();

            try (var r = tree.read()) {
                List<Value> history = new ArrayList<>();
                r.history(kb, "metric", (fs, ls, vt, v) -> {
                    history.add(v);
                    return true;
                });
                assertEquals(List.of(vals[0], vals[1], vals[2], vals[3], vals[4]), history);
            }
        }

        try (var tree = TaoTree.open(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 7);
            try (var r = tree.read()) {
                // Point-in-time queries after reopen.
                assertEquals(vals[0], r.getAt(kb, "metric", 150L));
                assertEquals(vals[2], r.getAt(kb, "metric", 350L));
                assertEquals(vals[4], r.get(kb, "metric"));
            }
        }
    }

    @Test
    void compactAfterAttrDeleteKeepsAttrDeleted() throws IOException {
        Path p = path();
        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "a", Value.ofInt(1));
                w.put(kb, "b", Value.ofInt(2));
                w.put(kb, "c", Value.ofInt(3));
            }
            try (var w = tree.write()) {
                w.delete(kb, "b");
            }
            tree.sync();
            tree.compact();

            try (var r = tree.read()) {
                Map<String, Value> all = r.getAll(kb);
                assertEquals(Value.ofInt(1), all.get("a"));
                assertNull(all.get("b"));
                assertEquals(Value.ofInt(3), all.get("c"));
            }
        }

        try (var tree = TaoTree.open(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var r = tree.read()) {
                assertNull(r.get(kb, "b"));
                assertEquals(Value.ofInt(1), r.get(kb, "a"));
                assertEquals(Value.ofInt(3), r.get(kb, "c"));
            }
        }
    }
}
