package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 2 integration tests for the new unified put/get API.
 * Exercises the Value-typed facade on top of the existing temporal backend.
 */
class UnifiedTemporalApiTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void putAndGetTimeless() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 42);

            try (var w = tree.write()) {
                w.put(kb, "name",   Value.ofString("alice"));
                w.put(kb, "age",    Value.ofInt(30));
                w.put(kb, "height", Value.ofFloat64(1.72));
                w.put(kb, "active", Value.ofBool(true));
            }

            try (var r = tree.read()) {
                assertEquals(Value.ofString("alice"),  r.get(kb, "name"));
                assertEquals(Value.ofInt(30),          r.get(kb, "age"));
                assertEquals(Value.ofFloat64(1.72),    r.get(kb, "height"));
                assertEquals(Value.ofBool(true),       r.get(kb, "active"));
                assertNull(r.get(kb, "missing_attr"));
            }
        }
    }

    @Test
    void putAllTimeless() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 7);

            Map<String, Value> attrs = new LinkedHashMap<>();
            attrs.put("first", Value.ofString("Ada"));
            attrs.put("last",  Value.ofString("Lovelace"));
            attrs.put("born",  Value.ofInt(1815));

            try (var w = tree.write()) {
                w.putAll(kb, attrs);
            }

            try (var r = tree.read()) {
                Map<String, Value> all = r.getAll(kb);
                assertEquals(3, all.size());
                assertEquals(Value.ofString("Ada"),       all.get("first"));
                assertEquals(Value.ofString("Lovelace"),  all.get("last"));
                assertEquals(Value.ofInt(1815),           all.get("born"));
            }
        }
    }

    @Test
    void putTemporalThenReadAt() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);

            try (var w = tree.write()) {
                w.put(kb, "temp", Value.ofFloat64(20.0), 1_000L);
                w.put(kb, "temp", Value.ofFloat64(21.5), 2_000L);
                w.put(kb, "temp", Value.ofFloat64(22.0), 3_000L);
            }

            try (var r = tree.read()) {
                assertEquals(Value.ofFloat64(22.0), r.get(kb, "temp"));
                assertEquals(Value.ofFloat64(20.0), r.getAt(kb, "temp", 1_500L));
                assertEquals(Value.ofFloat64(21.5), r.getAt(kb, "temp", 2_500L));
                assertEquals(Value.ofFloat64(22.0), r.getAt(kb, "temp", 99_999L));
                assertNull(r.getAt(kb, "temp", 500L));
            }
        }
    }

    @Test
    void historyCallbackReceivesDecodedValues() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);

            try (var w = tree.write()) {
                w.put(kb, "status", Value.ofString("pending"),  100L);
                w.put(kb, "status", Value.ofString("running"),  200L);
                w.put(kb, "status", Value.ofString("complete"), 300L);
            }

            try (var r = tree.read()) {
                var seen = new java.util.ArrayList<Value>();
                r.history(kb, "status", (firstSeen, lastSeen, validTo, v) -> {
                    seen.add(v);
                    return true;
                });
                assertEquals(3, seen.size());
                assertEquals(Value.ofString("pending"),  seen.get(0));
                assertEquals(Value.ofString("running"),  seen.get(1));
                assertEquals(Value.ofString("complete"), seen.get(2));
            }
        }
    }

    @Test
    void getAllWithOverflowValues() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 99);

            String longBio = "x".repeat(500);
            try (var w = tree.write()) {
                w.put(kb, "short", Value.ofString("hi"));
                w.put(kb, "bio",   Value.ofString(longBio));
                w.put(kb, "blob",  Value.ofBytes(new byte[200]));
            }

            try (var r = tree.read()) {
                assertEquals(Value.ofString("hi"),       r.get(kb, "short"));
                assertEquals(Value.ofString(longBio),    r.get(kb, "bio"));
                assertEquals(200, r.get(kb, "blob").asBytes().length);
            }
        }
    }

    @Test
    void getOnMissingEntityReturnsNull() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 12345);
            try (var r = tree.read()) {
                assertNull(r.get(kb, "anything"));
                assertTrue(r.getAll(kb).isEmpty());
            }
        }
    }

}
