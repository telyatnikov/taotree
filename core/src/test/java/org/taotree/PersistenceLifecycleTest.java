package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end lifecycle tests: sync, close, reopen, compact, recovery on damaged files.
 */
class PersistenceLifecycleTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    // ── sync → reopen ────────────────────────────────────────────────

    @Test
    void syncThenReopenRetainsData() throws IOException {
        Path p = path();
        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 0; i < 25; i++) {
                    kb.set(ID, i);
                    w.put(kb, "v", Value.ofInt(i));
                }
            }
            tree.sync();
        }
        try (var tree = TaoTree.open(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var r = tree.read()) {
                for (int i = 0; i < 25; i++) {
                    kb.set(ID, i);
                    assertEquals(Value.ofInt(i), r.get(kb, "v"));
                }
            }
        }
    }

    // ── close without explicit sync still persists ───────────────────

    @Test
    void closeImpliesSync() throws IOException {
        Path p = path();
        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                kb.set(ID, 1);
                w.put(kb, "x", Value.ofString("persisted"));
            }
            // No explicit sync() — rely on close() writing the final checkpoint.
        }
        try (var tree = TaoTree.open(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var r = tree.read()) {
                assertEquals(Value.ofString("persisted"), r.get(kb, "x"));
            }
        }
    }

    // ── compact then reopen ──────────────────────────────────────────

    @Test
    void compactThenReopenPreservesData() throws IOException {
        Path p = path();
        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 0; i < 100; i++) {
                    kb.set(ID, i);
                    w.put(kb, "v", Value.ofInt(i));
                }
            }
            tree.sync();
            tree.compact();
        }
        try (var tree = TaoTree.open(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var r = tree.read()) {
                for (int i = 0; i < 100; i++) {
                    kb.set(ID, i);
                    assertEquals(Value.ofInt(i), r.get(kb, "v"));
                }
            }
        }
    }

    // ── compact on empty tree is a no-op ─────────────────────────────

    @Test
    void compactEmptyTreeIsNoOp() throws IOException {
        Path p = path();
        try (var tree = TaoTree.create(p, layout())) {
            tree.compact();
            assertEquals(0, tree.totalSegmentsInUse(), "still empty after compact");
        }
    }

    // ── compact after many deletes ──────────────────────────────────

    @Test
    void compactAfterDeletesPreservesSurvivors() throws IOException {
        Path p = path();
        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    kb.set(ID, i);
                    w.put(kb, "v", Value.ofInt(i));
                }
            }
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i += 2) {
                    kb.set(ID, i);
                    w.delete(kb);
                }
            }
            tree.sync();
            tree.compact();

            try (var r = tree.read()) {
                for (int i = 0; i < 50; i++) {
                    kb.set(ID, i);
                    if (i % 2 == 0) {
                        assertNull(r.get(kb, "v"));
                    } else {
                        assertEquals(Value.ofInt(i), r.get(kb, "v"));
                    }
                }
            }
        }
    }

    // ── multiple sync cycles, then reopen ───────────────────────────

    @Test
    void multipleSyncCyclesReopenReadsAll() throws IOException {
        Path p = path();
        try (var tree = TaoTree.create(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            for (int batch = 0; batch < 5; batch++) {
                try (var w = tree.write()) {
                    for (int i = 0; i < 10; i++) {
                        kb.set(ID, batch * 10 + i);
                        w.put(kb, "v", Value.ofInt(batch * 10 + i));
                    }
                }
                tree.sync();
            }
        }
        try (var tree = TaoTree.open(p, layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var r = tree.read()) {
                for (int i = 0; i < 50; i++) {
                    kb.set(ID, i);
                    assertEquals(Value.ofInt(i), r.get(kb, "v"));
                }
            }
        }
    }

    // ── idempotent sync ─────────────────────────────────────────────

    @Test
    void syncIsIdempotent() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            tree.sync();
            tree.sync();
            tree.sync();
        }
    }

    // ── open non-existent path fails ────────────────────────────────

    @Test
    void openNonExistentPathThrows() {
        Path missing = tmp.resolve("does-not-exist.taotree");
        assertThrows(IOException.class, () -> TaoTree.open(missing, layout()));
    }

    // ── open a file with wrong bytes fails ───────────────────────────

    @Test
    void openGarbageFileThrows() throws IOException {
        Path junk = tmp.resolve("junk.taotree");
        Files.write(junk, new byte[4096]);
        // Not a valid TaoTree store — must fail.
        assertThrows(IOException.class, () -> TaoTree.open(junk, layout()));
    }
}
