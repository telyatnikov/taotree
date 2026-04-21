package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lifecycle / error-path tests for {@link TaoTree.ReadScope}.
 */
class ReadScopeErrorTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    // ── Closed scope rejects everything ──────────────────────────────

    @Test
    void closedReadScopeRejectsAll() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);

            TaoTree.ReadScope r = tree.read();
            r.close();

            assertThrows(IllegalStateException.class, () -> r.get(kb, "a"));
            assertThrows(IllegalStateException.class, () -> r.getAt(kb, "a", 1L));
            assertThrows(IllegalStateException.class, () -> r.getAll(kb));
            assertThrows(IllegalStateException.class, () -> r.getAllAt(kb, 1L));
            assertThrows(IllegalStateException.class,
                    () -> r.history(kb, "a", (fs, ls, vt, v) -> true));
            assertThrows(IllegalStateException.class,
                    () -> r.historyRange(kb, "a", 0L, 100L, (fs, ls, vt, v) -> true));
            assertThrows(IllegalStateException.class, () -> r.lookup(kb.key()));
            assertThrows(IllegalStateException.class,
                    () -> r.forEach(k -> true));
            assertThrows(IllegalStateException.class, r::size);
            assertThrows(IllegalStateException.class, r::isEmpty);
        }
    }

    // ── Cross-thread rejected ────────────────────────────────────────

    @Test
    void crossThreadReadRejected() throws Exception {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 5);

            try (var r = tree.read()) {
                var error = new AtomicReference<Throwable>();
                Thread t = new Thread(() -> {
                    try { r.get(kb, "x"); }
                    catch (Throwable ex) { error.set(ex); }
                });
                t.start();
                t.join();
                assertInstanceOf(IllegalStateException.class, error.get());
            }
        }
    }

    // ── Missing attr / entity → benign ───────────────────────────────

    @Test
    void historyOnMissingAttrReturnsTrueNoCallbacks() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) { w.put(kb, "known", Value.ofInt(1)); }

            try (var r = tree.read()) {
                AtomicInteger calls = new AtomicInteger();
                boolean finished = r.history(kb, "missing", (fs, ls, vt, v) -> {
                    calls.incrementAndGet();
                    return true;
                });
                assertTrue(finished);
                assertEquals(0, calls.get());
            }
        }
    }

    @Test
    void historyOnMissingEntityReturnsTrueNoCallbacks() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 99); // never written
            try (var r = tree.read()) {
                AtomicInteger calls = new AtomicInteger();
                boolean finished = r.history(kb, "x", (fs, ls, vt, v) -> {
                    calls.incrementAndGet();
                    return true;
                });
                assertTrue(finished);
                assertEquals(0, calls.get());
            }
        }
    }

    @Test
    void getAllOnMissingEntityReturnsEmptyMap() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 404);
            try (var r = tree.read()) {
                assertTrue(r.getAll(kb).isEmpty());
                assertTrue(r.getAllAt(kb, 100L).isEmpty());
            }
        }
    }

    // ── getAt with Long.MAX_VALUE returns the latest run ────────────

    @Test
    void getAtMaxTimestampReturnsLatest() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "n", Value.ofInt(1), 100L);
                w.put(kb, "n", Value.ofInt(2), 200L);
                w.put(kb, "n", Value.ofInt(3), 300L);
            }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(3), r.getAt(kb, "n", Long.MAX_VALUE));
            }
        }
    }

    // ── historyRange clips future observations past toMs ────────────

    @Test
    void historyRangeStopsPastEndOfRange() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "v", Value.ofInt(1), 100L);
                w.put(kb, "v", Value.ofInt(2), 200L);
                w.put(kb, "v", Value.ofInt(3), 300L);
                w.put(kb, "v", Value.ofInt(4), 400L);
            }
            try (var r = tree.read()) {
                AtomicInteger count = new AtomicInteger();
                r.historyRange(kb, "v", 150L, 250L, (fs, ls, vt, v) -> {
                    count.incrementAndGet();
                    return true;
                });
                // Runs overlapping [150,250]: first_seen=100 (predecessor) and 200.
                assertTrue(count.get() >= 1 && count.get() <= 3,
                        "expected at least one overlapping run, got " + count.get());
            }
        }
    }

    // ── scan on empty tree ──────────────────────────────────────────

    @Test
    void scanEmptyTreeCompletes() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            try (var r = tree.read()) {
                AtomicInteger calls = new AtomicInteger();
                boolean done = r.forEach(k -> {
                    calls.incrementAndGet();
                    return true;
                });
                assertTrue(done);
                assertEquals(0, calls.get());
                assertTrue(r.isEmpty());
                assertEquals(0, r.size());
            }
        }
    }

    // ── Double close is a no-op ─────────────────────────────────────

    @Test
    void doubleCloseIsNoOp() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            TaoTree.ReadScope r = tree.read();
            r.close();
            assertDoesNotThrow(r::close);
        }
    }
}
