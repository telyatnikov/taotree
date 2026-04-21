package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end file-backed persistence tests for the unified temporal API:
 * sync, close, reopen, compact, many entities.
 *
 * Intended to replace coverage previously provided by MappedTaoTreeTest.
 */
class FileBackedPersistenceTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void syncThenReopenRetainsData() throws IOException {
        var storePath = path();
        int n = 200;

        try (var tree = TaoTree.create(storePath, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 0; i < n; i++) {
                    kb.set(ID, i);
                    w.put(kb, "n",   Value.ofInt(i));
                    w.put(kb, "sq",  Value.ofLong((long) i * i));
                    w.put(kb, "str", Value.ofString("item-" + i));
                }
            }
            tree.sync();
        }

        try (var tree = TaoTree.open(storePath, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var r = tree.read()) {
                for (int i = 0; i < n; i++) {
                    kb.set(ID, i);
                    assertEquals(Value.ofInt(i),             r.get(kb, "n"));
                    assertEquals(Value.ofLong((long) i * i), r.get(kb, "sq"));
                    assertEquals(Value.ofString("item-" + i), r.get(kb, "str"));
                }
            }
        }
    }

    @Test
    void compactSucceedsAndPreservesData() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    kb.set(ID, i);
                    w.put(kb, "v", Value.ofInt(i * 10));
                }
            }
            tree.sync();
            tree.compact();

            try (var r = tree.read()) {
                for (int i = 0; i < 50; i++) {
                    kb.set(ID, i);
                    assertEquals(Value.ofInt(i * 10), r.get(kb, "v"));
                }
            }
        }
    }

    @Test
    void multipleWriteScopesAccumulate() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) { w.put(kb, "a", Value.ofInt(1)); }
            try (var w = tree.write()) { w.put(kb, "b", Value.ofInt(2)); }
            try (var w = tree.write()) { w.put(kb, "c", Value.ofInt(3)); }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(1), r.get(kb, "a"));
                assertEquals(Value.ofInt(2), r.get(kb, "b"));
                assertEquals(Value.ofInt(3), r.get(kb, "c"));
            }
        }
    }
}
