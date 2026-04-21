package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GetAllAtTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void getAllAtPicksRunValidAtTimestamp() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "v", Value.ofInt(10), 100L);
                w.put(kb, "v", Value.ofInt(20), 200L);
                w.put(kb, "v", Value.ofInt(30), 300L);
            }
            try (var r = tree.read()) {
                Map<String, Value> at150 = r.getAllAt(kb, 150L);
                assertEquals(Value.ofInt(10), at150.get("v"));
                Map<String, Value> at350 = r.getAllAt(kb, 350L);
                assertEquals(Value.ofInt(30), at350.get("v"));
                Map<String, Value> at50 = r.getAllAt(kb, 50L);
                assertTrue(at50.isEmpty(), "before any run, must be empty");
            }
        }
    }

    @Test
    void getAllAtMixesTimelessAndTemporal() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "label", Value.ofString("alpha"));            // TIMELESS
                w.put(kb, "score", Value.ofInt(10), 100L);
                w.put(kb, "score", Value.ofInt(20), 200L);
            }
            try (var r = tree.read()) {
                Map<String, Value> at150 = r.getAllAt(kb, 150L);
                assertEquals(Value.ofString("alpha"), at150.get("label"));
                assertEquals(Value.ofInt(10), at150.get("score"));

                Map<String, Value> at250 = r.getAllAt(kb, 250L);
                assertEquals(Value.ofString("alpha"), at250.get("label"));
                assertEquals(Value.ofInt(20), at250.get("score"));
            }
        }
    }

    @Test
    void getAllAtReturnsEmptyForMissingEntity() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 9999);
            try (var r = tree.read()) {
                assertTrue(r.getAllAt(kb, 1234L).isEmpty());
            }
        }
    }
}
