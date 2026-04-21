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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lifecycle / error-path tests for {@link TaoTree.WriteScope}: closed, cross-thread,
 * empty batches, null handling, idempotent batching.
 */
class WriteScopeErrorTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    // ── Closed scope → IllegalStateException on every mutation API ────

    @Test
    void closedScopeRejectsAllMutations() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);

            TaoTree.WriteScope w = tree.write();
            w.close();

            assertThrows(IllegalStateException.class,
                    () -> w.put(kb, "a", Value.ofInt(1)));
            assertThrows(IllegalStateException.class,
                    () -> w.put(kb, "a", Value.ofInt(1), 100L));
            assertThrows(IllegalStateException.class,
                    () -> w.putAll(kb, Map.of("x", Value.ofInt(1))));
            assertThrows(IllegalStateException.class,
                    () -> w.delete(kb));
            assertThrows(IllegalStateException.class,
                    () -> w.delete(kb, "a"));
            assertThrows(IllegalStateException.class,
                    () -> w.getOrCreate(kb.key()));
            assertThrows(IllegalStateException.class,
                    () -> w.lookup(kb.key()));
            assertThrows(IllegalStateException.class,
                    w::size);
            assertThrows(IllegalStateException.class,
                    w::isEmpty);
            // copyFrom must also reject.
            try (var r = tree.read()) {
                assertThrows(IllegalStateException.class, () -> w.copyFrom(r));
            }
        }
    }

    // ── Cross-thread use is rejected via checkThread() ────────────────

    @Test
    void crossThreadUseRejected() throws Exception {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 7);

            try (var w = tree.write()) {
                var error = new AtomicReference<Throwable>();
                Thread t = new Thread(() -> {
                    try {
                        w.put(kb, "a", Value.ofInt(1));
                    } catch (Throwable ex) {
                        error.set(ex);
                    }
                });
                t.start();
                t.join();
                assertInstanceOf(IllegalStateException.class, error.get());
                assertTrue(error.get().getMessage().contains("owner="));
            }
        }
    }

    // ── putAll null/empty semantics ──────────────────────────────────

    @Test
    void putAllEmptyMapIsNoOp() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.putAll(kb, Map.of());
                w.putAll(kb, null);
            }
            try (var r = tree.read()) {
                assertTrue(r.getAll(kb).isEmpty(),
                        "putAll with empty/null map produces no observations");
            }
        }
    }

    @Test
    void putAllWithNullValueEntryStoresNull() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 2);

            Map<String, Value> m = new LinkedHashMap<>();
            m.put("x", null);
            m.put("y", Value.ofInt(9));
            try (var w = tree.write()) { w.putAll(kb, m); }

            try (var r = tree.read()) {
                assertEquals(Value.ofNull(), r.get(kb, "x"));
                assertEquals(Value.ofInt(9), r.get(kb, "y"));
            }
        }
    }

    @Test
    void putWithNullValueStoresNull() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 3);
            try (var w = tree.write()) { w.put(kb, "n", null); }
            try (var r = tree.read()) {
                assertEquals(Value.ofNull(), r.get(kb, "n"));
            }
        }
    }

    // ── Empty attr name — lock in current behaviour (interned like any other) ──

    @Test
    void emptyAttrNameIsJustAnotherAttr() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 4);
            try (var w = tree.write()) { w.put(kb, "", Value.ofInt(42)); }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(42), r.get(kb, ""));
            }
        }
    }

    // ── delete returns false when entity/attr doesn't exist ─────────

    @Test
    void deleteMissingEntityReturnsFalse() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 99);
            try (var w = tree.write()) {
                assertFalse(w.delete(kb));
                assertFalse(w.delete(kb, "absent"));
            }
        }
    }

    // ── Double close is a silent no-op ──────────────────────────────

    @Test
    void doubleCloseIsNoOp() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            TaoTree.WriteScope w = tree.write();
            w.close();
            assertDoesNotThrow(w::close);
        }
    }
}
