package org.taotree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

/**
 * Tombstone semantics for {@code delete(KeyBuilder, String, long)}.
 *
 * <p>Verifies Phase 8 {@code p8-tombstone-semantics}:
 * a timestamped delete must (a) truncate the predecessor run's validity so
 * {@code getAt(ts)} after the tombstone returns {@code null}, (b) remove the
 * attribute from the current CHAMP state, (c) survive reopen, and (d) be
 * restorable by a subsequent {@code put} at a later timestamp.
 */
class TombstoneSemanticsTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void getAtAfterTombstoneReturnsNull() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "age", Value.ofInt(30), 10L);
                assertTrue(w.delete(kb, "age", 20L));
            }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(30), r.getAt(kb, "age", 15L));
                assertNull(r.getAt(kb, "age", 20L));
                assertNull(r.getAt(kb, "age", 1_000L));
                assertNull(r.get(kb, "age"));
            }
        }
    }

    @Test
    void tombstoneRemovesFromCurrentStateChamp() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "name", Value.ofString("Ada"), 10L);
                w.put(kb, "age",  Value.ofInt(30),       10L);
                w.delete(kb, "age", 20L);
            }
            try (var r = tree.read()) {
                var all = r.getAll(kb);
                assertTrue(all.containsKey("name"));
                assertFalse(all.containsKey("age"),
                        "age should have been removed from CHAMP by tombstone");
                assertFalse(r.getAllAt(kb, 25L).containsKey("age"));
                // But historical snapshot before tombstone still sees "age".
                assertTrue(r.getAllAt(kb, 15L).containsKey("age"));
            }
        }
    }

    @Test
    void putAfterTombstoneRestores() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "age", Value.ofInt(30), 10L);
                w.delete(kb, "age", 20L);
                w.put(kb, "age", Value.ofInt(40), 30L);
            }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(30), r.getAt(kb, "age", 15L));
                assertNull(r.getAt(kb, "age", 25L));       // still in tombstone window
                assertEquals(Value.ofInt(40), r.getAt(kb, "age", 35L));
                assertEquals(Value.ofInt(40), r.get(kb, "age"));
            }
        }
    }

    @Test
    void historySurfacesTombstoneAsNullValue() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "age", Value.ofInt(30), 10L);
                w.delete(kb, "age", 20L);
                w.put(kb, "age", Value.ofInt(40), 30L);
            }
            try (var r = tree.read()) {
                List<long[]> stamps = new ArrayList<>();
                List<Value> values = new ArrayList<>();
                r.history(kb, "age", (firstSeen, lastSeen, validTo, value) -> {
                    stamps.add(new long[]{firstSeen, lastSeen, validTo});
                    values.add(value);
                    return true;
                });
                assertEquals(3, stamps.size(), "expected 3 runs (put, tombstone, put)");
                assertEquals(10L, stamps.get(0)[0]);
                assertEquals(20L, stamps.get(1)[0]);
                assertEquals(30L, stamps.get(2)[0]);
                assertEquals(Value.ofInt(30), values.get(0));
                assertEquals(Value.ofNull(),  values.get(1), "tombstone run → ofNull()");
                assertEquals(Value.ofInt(40), values.get(2));
            }
        }
    }

    @Test
    void tombstoneSurvivesReopen() throws IOException {
        Path file = path();
        try (var tree = TaoTree.create(file, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 42);
            try (var w = tree.write()) {
                w.put(kb, "age", Value.ofInt(30), 10L);
                w.delete(kb, "age", 20L);
            }
            tree.sync();
        }
        try (var tree = TaoTree.open(file, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 42);
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(30), r.getAt(kb, "age", 15L));
                assertNull(r.getAt(kb, "age", 25L));
                assertNull(r.get(kb, "age"));
            }
        }
    }

    @Test
    void doubleTombstoneIsNoop() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena).set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "age", Value.ofInt(30), 10L);
                w.delete(kb, "age", 20L);
                // Second identical tombstone extends last_seen but does not
                // change state. Must not throw, must not reveal the attr.
                w.delete(kb, "age", 21L);
            }
            try (var r = tree.read()) {
                assertNull(r.get(kb, "age"));
                assertNull(r.getAt(kb, "age", 25L));
            }
        }
    }
}
