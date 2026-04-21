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
 * Phase 6b: covers {@link TaoTree.WriteScope#delete(org.taotree.layout.KeyBuilder)}
 * and {@link TaoTree.WriteScope#delete(org.taotree.layout.KeyBuilder, String)}.
 */
class DeleteSemanticsTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void deleteEntityRemovesFromForEachAndGet() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            try (var w = tree.write()) {
                kb.set(ID, 1); w.put(kb, "name", Value.ofString("a"));
                kb.set(ID, 2); w.put(kb, "name", Value.ofString("b"));
                kb.set(ID, 3); w.put(kb, "name", Value.ofString("c"));
            }
            try (var w = tree.write()) {
                kb.set(ID, 2);
                assertTrue(w.delete(kb), "delete must return true for existing entity");
            }
            try (var r = tree.read()) {
                List<Integer> seen = new ArrayList<>();
                r.forEach(k -> {
                    int v = ((k[0] & 0xFF) << 24) | ((k[1] & 0xFF) << 16)
                          | ((k[2] & 0xFF) << 8)  | (k[3] & 0xFF);
                    seen.add(v);
                    return true;
                });
                assertEquals(List.of(1, 3), seen);

                kb.set(ID, 2);
                assertNull(r.get(kb, "name"));
                assertTrue(r.getAll(kb).isEmpty(), "getAll on deleted entity must be empty");
            }
        }
    }

    @Test
    void deleteEntityReturnsFalseWhenMissing() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 999);
            try (var w = tree.write()) {
                assertFalse(w.delete(kb));
            }
        }
    }

    @Test
    void deleteAttrPreservesHistory() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);

            try (var w = tree.write()) {
                w.put(kb, "status", Value.ofString("a"), 100L);
                w.put(kb, "status", Value.ofString("b"), 200L);
            }
            try (var w = tree.write()) {
                assertTrue(w.delete(kb, "status"));
            }
            try (var r = tree.read()) {
                assertNull(r.get(kb, "status"), "current state must be gone");

                List<Value> seen = new ArrayList<>();
                r.history(kb, "status", (fs, ls, vt, v) -> { seen.add(v); return true; });
                assertEquals(2, seen.size(), "history must still see both runs");
                assertEquals(Value.ofString("a"), seen.get(0));
                assertEquals(Value.ofString("b"), seen.get(1));
            }
        }
    }

    @Test
    void deleteAttrReturnsFalseWhenMissing() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "x", Value.ofInt(1));
            }
            try (var w = tree.write()) {
                assertFalse(w.delete(kb, "missing"), "missing attr → false");
            }
            // Also: delete attr on non-existent entity must not crash.
            try (var w = tree.write()) {
                kb.set(ID, 4242);
                // no put on entity 4242 yet — delete(attr) on uncreated entity
                // currently creates the entity leaf (optimisticGetOrCreate); accept either.
                boolean r = w.delete(kb, "x");
                assertFalse(r);
            }
        }
    }

    @Test
    void deleteThenPutRecreates() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);

            try (var w = tree.write()) {
                w.put(kb, "x", Value.ofInt(1));
            }
            try (var w = tree.write()) {
                assertTrue(w.delete(kb));
            }
            try (var w = tree.write()) {
                w.put(kb, "x", Value.ofInt(2));
            }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(2), r.get(kb, "x"));
            }
        }
    }

    @Test
    void deleteAttrLeavesSiblingAttrs() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
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
                assertTrue(w.delete(kb, "b"));
            }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(1), r.get(kb, "a"));
                assertNull(r.get(kb, "b"));
                assertEquals(Value.ofInt(3), r.get(kb, "c"));
                Map<String, Value> all = r.getAll(kb);
                assertEquals(2, all.size());
                assertTrue(all.containsKey("a"));
                assertTrue(all.containsKey("c"));
                assertFalse(all.containsKey("b"));
            }
        }
    }
}
