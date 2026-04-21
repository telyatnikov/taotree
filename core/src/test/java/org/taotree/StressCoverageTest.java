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
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that exercise ART growth (Node4→16→48→256), ART scan order
 * (ArtSearch successor / predecessor), CHAMP inline↔node migrations via attribute
 * add/remove, and full forEach iteration order.
 */
class StressCoverageTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    // ── ART grows through all node sizes; forEach visits in sorted order ──

    @Test
    void forEachVisitsEntitiesInAscendingKeyOrder() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            // 300 entities forces ART growth through Node4 → Node16 → Node48 → Node256.
            Set<Integer> written = new TreeSet<>();
            try (var w = tree.write()) {
                for (int i = 0; i < 300; i++) {
                    int id = i * 7 + 1; // spread keys
                    kb.set(ID, id);
                    w.put(kb, "v", Value.ofInt(id));
                    written.add(id);
                }
            }

            try (var r = tree.read()) {
                assertEquals(written.size(), r.size());
                List<Integer> ordered = new ArrayList<>();
                boolean done = r.forEach(k -> {
                    // uint32 BE decode
                    int v = ((k[0] & 0xFF) << 24) | ((k[1] & 0xFF) << 16)
                            | ((k[2] & 0xFF) << 8) | (k[3] & 0xFF);
                    ordered.add(v);
                    return true;
                });
                assertTrue(done);
                assertEquals(new ArrayList<>(written), ordered,
                        "forEach must deliver keys in ascending order");
            }
        }
    }

    @Test
    void forEachEarlyStopReturnsFalse() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 0; i < 10; i++) {
                    kb.set(ID, i);
                    w.put(kb, "v", Value.ofInt(i));
                }
            }
            try (var r = tree.read()) {
                int[] seen = {0};
                boolean done = r.forEach(k -> {
                    seen[0]++;
                    return seen[0] < 3;
                });
                assertFalse(done);
                assertEquals(3, seen[0]);
            }
        }
    }

    // ── Prefix scan by KeyBuilder ──────────────────────────────────

    @Test
    void scanByKeyBuilderVisitsMatchingPrefix() throws IOException {
        var layout = KeyLayout.of(KeyField.uint16("cls"), KeyField.uint32("id"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var CLS = tree.keyUint16("cls");
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            try (var w = tree.write()) {
                for (int c = 0; c < 3; c++) {
                    for (int i = 0; i < 10; i++) {
                        kb.set(CLS, (short) c).set(ID, i);
                        w.put(kb, "v", Value.ofInt(c * 100 + i));
                    }
                }
            }

            try (var r = tree.read()) {
                var prefix = tree.newKeyBuilder(arena);
                prefix.set(CLS, (short) 1);
                int[] count = {0};
                boolean done = r.scan(prefix, CLS, k -> {
                    count[0]++;
                    return true;
                });
                assertTrue(done);
                assertEquals(10, count[0]);
            }
        }
    }

    // ── Scan via QueryBuilder — unsatisfiable short-circuits ──────

    @Test
    void scanQueryBuilderUnsatisfiableReturnsTrue() throws IOException {
        var layout = KeyLayout.of(KeyField.dict16("k"), KeyField.uint32("id"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var K = tree.keyDict16("k");
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                kb.set(K, "known").set(ID, 1);
                w.put(kb, "v", Value.ofInt(1));
            }

            try (var r = tree.read()) {
                var qb = tree.newQueryBuilder(arena);
                qb.set(K, "does-not-exist");
                int[] calls = {0};
                boolean done = r.scan(qb, K, key -> {
                    calls[0]++;
                    return true;
                });
                assertTrue(done);
                assertEquals(0, calls[0]);
            }
        }
    }

    @Test
    void scanQueryBuilderMatchesKnownPrefix() throws IOException {
        var layout = KeyLayout.of(KeyField.dict16("k"), KeyField.uint32("id"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            var K = tree.keyDict16("k");
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 0; i < 5; i++) {
                    kb.set(K, "alpha").set(ID, i);
                    w.put(kb, "v", Value.ofInt(i));
                }
                for (int i = 0; i < 3; i++) {
                    kb.set(K, "beta").set(ID, i);
                    w.put(kb, "v", Value.ofInt(i));
                }
            }
            try (var r = tree.read()) {
                var qb = tree.newQueryBuilder(arena);
                qb.set(K, "alpha");
                int[] count = {0};
                r.scan(qb, K, key -> { count[0]++; return true; });
                assertEquals(5, count[0]);
            }
        }
    }

    // ── CHAMP inline↔node migrations via add/remove attrs ────────

    @Test
    void attributeGrowAndShrinkExercisesChampMigrations() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);

            int n = 40; // enough to exceed CHAMP inline threshold and cause node migration
            try (var w = tree.write()) {
                for (int i = 0; i < n; i++) {
                    w.put(kb, "attr" + i, Value.ofInt(i));
                }
            }

            try (var r = tree.read()) {
                Map<String, Value> all = r.getAll(kb);
                assertEquals(n, all.size());
                for (int i = 0; i < n; i++) {
                    assertEquals(Value.ofInt(i), all.get("attr" + i));
                }
            }

            // Remove half the attributes in reverse order — triggers
            // copyAndRemoveEntry / copyAndMigrateFromNodeToInline paths.
            try (var w = tree.write()) {
                for (int i = n - 1; i >= n / 2; i--) {
                    assertTrue(w.delete(kb, "attr" + i),
                            "delete of attr" + i + " should succeed");
                }
            }

            try (var r = tree.read()) {
                Map<String, Value> all = r.getAll(kb);
                assertEquals(n / 2, all.size());
                for (int i = 0; i < n / 2; i++) {
                    assertEquals(Value.ofInt(i), all.get("attr" + i));
                }
                for (int i = n / 2; i < n; i++) {
                    assertNull(all.get("attr" + i));
                }
            }

            // Remove remaining — drops back to inline / empty.
            try (var w = tree.write()) {
                for (int i = 0; i < n / 2; i++) {
                    assertTrue(w.delete(kb, "attr" + i));
                }
            }
            try (var r = tree.read()) {
                assertTrue(r.getAll(kb).isEmpty());
            }
        }
    }

    // ── delete attr that doesn't exist returns false ─────────────

    @Test
    void deleteAttrOnEntityWithoutThatAttrReturnsFalse() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 5);
            try (var w = tree.write()) { w.put(kb, "has", Value.ofInt(1)); }
            try (var w = tree.write()) {
                assertFalse(w.delete(kb, "missing"));
                assertTrue(w.delete(kb, "has"));
                assertFalse(w.delete(kb, "has")); // idempotent
            }
        }
    }

    // ── Entity delete removes it from ART ────────────────────────

    @Test
    void deleteEntityReducesSize() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            try (var w = tree.write()) {
                for (int i = 0; i < 20; i++) {
                    kb.set(ID, i);
                    w.put(kb, "v", Value.ofInt(i));
                }
            }
            try (var r = tree.read()) { assertEquals(20, r.size()); }

            try (var w = tree.write()) {
                for (int i = 0; i < 20; i += 3) {
                    kb.set(ID, i);
                    assertTrue(w.delete(kb));
                }
            }
            try (var r = tree.read()) {
                int remaining = 0;
                for (int i = 0; i < 20; i++) {
                    kb.set(ID, i);
                    if (r.get(kb, "v") != null) remaining++;
                }
                assertEquals(20 - 7, remaining);
            }
        }
    }
}
