package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 6b: error / negative-path coverage for the unified API.
 */
class NegativeApiTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void putOnClosedWriteScopeThrows() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            var w = tree.write();
            w.close();
            assertThrows(IllegalStateException.class,
                () -> w.put(kb, "x", Value.ofInt(1)));
        }
    }

    @Test
    void getOnClosedReadScopeThrows() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            var r = tree.read();
            r.close();
            assertThrows(IllegalStateException.class, () -> r.get(kb, "x"));
        }
    }

    @Test
    void doubleCloseIsIdempotent() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout())) {
            var w = tree.write();
            w.close();
            assertDoesNotThrow(w::close);
            var r = tree.read();
            r.close();
            assertDoesNotThrow(r::close);
        }
    }

    @Test
    void putWithNullAttrThrows() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                assertThrows(NullPointerException.class,
                    () -> w.put(kb, null, Value.ofInt(1)));
            }
        }
    }

    @Test
    void putWithNullValueStoresNull() throws IOException {
        // Per current implementation, put coerces a null Value into Value.ofNull().
        // Lock that behavior in so future regressions are caught explicitly.
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                assertDoesNotThrow(() -> w.put(kb, "x", null));
            }
            try (var r = tree.read()) {
                Value v = r.get(kb, "x");
                assertNotNull(v, "stored null must round-trip as Value.ofNull (not Java null)");
                assertTrue(v.isNull());
            }
        }
    }

    @Test
    void putAllWithNullMapIsNoop() throws IOException {
        // putAll(kb, null) is documented as a no-op (early return on null/empty).
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                assertDoesNotThrow(() -> w.putAll(kb, (Map<String, Value>) null));
            }
            try (var r = tree.read()) {
                assertTrue(r.getAll(kb).isEmpty());
            }
        }
    }

    @Test
    void putAllRejectsNullKeyInMap() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            Map<String, Value> attrs = new HashMap<>();
            attrs.put(null, Value.ofInt(1));
            try (var w = tree.write()) {
                assertThrows(NullPointerException.class, () -> w.putAll(kb, attrs));
            }
        }
    }

    @Test
    void oversizedValuePayloadThrows() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            String big = "x".repeat(70_000);
            try (var w = tree.write()) {
                IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> w.put(kb, "big", Value.ofString(big)));
                assertTrue(ex.getMessage().toLowerCase().contains("payload"),
                    "exception message should mention payload size: " + ex.getMessage());
            }
        }
    }

    @Test
    void valueOfNullRoundTrip() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "n", Value.ofNull());
            }
            try (var r = tree.read()) {
                Value v = r.get(kb, "n");
                assertNotNull(v, "stored Value.ofNull must NOT be reported as missing");
                assertEquals(Value.ofNull(), v);
                assertTrue(v.isNull());
                assertNull(r.get(kb, "missing"), "absent attr must still report null");
            }
        }
    }
}
