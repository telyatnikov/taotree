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
 * Phase 3 integration: {@code TaoTree.create(Path, KeyLayout)} always produces a
 * temporal tree and is bit-compatible with the legacy {@code createTemporal}.
 */
class AlwaysTemporalTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void createProducesTemporalTree() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout())) {
            assertNotNull(tree.attrDictionary());
        }
    }

    @Test
    void createPutReopenGet() throws IOException {
        var storePath = path();

        try (var tree = TaoTree.create(storePath, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 42);
            try (var w = tree.write()) {
                w.put(kb, "name", Value.ofString("Ada"));
                w.put(kb, "age",  Value.ofInt(30));
                w.put(kb, "bio",  Value.ofString("x".repeat(500)));
            }
            tree.sync();
        }

        try (var tree = TaoTree.open(storePath, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 42);
            try (var r = tree.read()) {
                assertEquals(Value.ofString("Ada"),           r.get(kb, "name"));
                assertEquals(Value.ofInt(30),                 r.get(kb, "age"));
                assertEquals(Value.ofString("x".repeat(500)), r.get(kb, "bio"));
            }
        }
    }

    @Test
    void reopenWithTemporalTimelineIntact() throws IOException {
        var storePath = path();
        try (var tree = TaoTree.create(storePath, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "temp", Value.ofFloat64(20.0), 1_000L);
                w.put(kb, "temp", Value.ofFloat64(21.0), 2_000L);
            }
            tree.sync();
        }

        try (var tree = TaoTree.open(storePath, keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var r = tree.read()) {
                assertEquals(Value.ofFloat64(21.0), r.get(kb, "temp"));
                assertEquals(Value.ofFloat64(20.0), r.getAt(kb, "temp", 1_500L));
                assertEquals(Value.ofFloat64(21.0), r.getAt(kb, "temp", 2_500L));
            }
        }
    }
}
