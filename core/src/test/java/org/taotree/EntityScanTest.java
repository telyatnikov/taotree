package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 6b: covers {@link TaoTree.ReadScope#forEach} and
 * {@link TaoTree.ReadScope#scan(org.taotree.layout.KeyBuilder, KeyHandle, EntityVisitor)}.
 */
class EntityScanTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    private static int decodeUint32(byte[] k, int off) {
        return ((k[off] & 0xFF) << 24) | ((k[off+1] & 0xFF) << 16)
             | ((k[off+2] & 0xFF) << 8) | (k[off+3] & 0xFF);
    }

    @Test
    void forEachVisitsAllEntitiesInKeyOrder() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i : new int[]{5, 1, 9, 3, 7}) {
                    kb.set(ID, i);
                    w.put(kb, "x", Value.ofInt(i));
                }
            }
            try (var r = tree.read()) {
                List<Integer> seen = new ArrayList<>();
                boolean done = r.forEach(k -> { seen.add(decodeUint32(k, 0)); return true; });
                assertTrue(done, "forEach should return true when not stopped early");
                assertEquals(List.of(1, 3, 5, 7, 9), seen);
            }
        }
    }

    @Test
    void forEachOnEmptyTreeVisitsNothing() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout())) {
            try (var r = tree.read()) {
                List<byte[]> seen = new ArrayList<>();
                boolean done = r.forEach(k -> { seen.add(k); return true; });
                assertTrue(done);
                assertTrue(seen.isEmpty());
            }
        }
    }

    @Test
    void forEachEarlyStopReturnsFalse() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 1; i <= 5; i++) { kb.set(ID, i); w.put(kb, "x", Value.ofInt(i)); }
            }
            try (var r = tree.read()) {
                List<Integer> seen = new ArrayList<>();
                boolean done = r.forEach(k -> {
                    seen.add(decodeUint32(k, 0));
                    return seen.size() < 2;
                });
                assertFalse(done, "early-stop forEach must return false");
                assertEquals(2, seen.size());
            }
        }
    }

    @Test
    void scanByFullKeyVisitsExactlyOne() throws IOException {
        // For a single-field key the whole key IS the prefix, so scan(kb, ID, ...)
        // narrows to exactly that one entity.
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                for (int i = 1; i <= 5; i++) { kb.set(ID, i); w.put(kb, "x", Value.ofInt(i)); }
            }
            try (var r = tree.read()) {
                kb.set(ID, 3);
                List<Integer> seen = new ArrayList<>();
                boolean done = r.scan(kb, ID, k -> { seen.add(decodeUint32(k, 0)); return true; });
                assertTrue(done);
                assertEquals(List.of(3), seen);
            }
        }
    }

    @Test
    void scanForMissingPrefixVisitsNothing() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            try (var w = tree.write()) {
                kb.set(ID, 1); w.put(kb, "x", Value.ofInt(1));
            }
            try (var r = tree.read()) {
                kb.set(ID, 7777);
                List<byte[]> seen = new ArrayList<>();
                boolean done = r.scan(kb, ID, k -> { seen.add(k); return true; });
                assertTrue(done);
                assertTrue(seen.isEmpty());
            }
        }
    }

    @Test
    void scanByCompositeNamespacePrefixOnlyVisitsThatNamespace() throws IOException {
        var layout = KeyLayout.of(KeyField.uint16("ns"), KeyField.uint32("id"));
        try (var tree = TaoTree.create(path(), layout);
             var arena = Arena.ofConfined()) {
            KeyHandle.UInt16 NS = tree.keyUint16("ns");
            KeyHandle.UInt32 ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            try (var w = tree.write()) {
                for (short ns : new short[]{10, 20, 30}) {
                    for (int i = 1; i <= 3; i++) {
                        kb.set(NS, ns); kb.set(ID, i);
                        w.put(kb, "x", Value.ofInt(ns * 100 + i));
                    }
                }
            }
            try (var r = tree.read()) {
                kb.set(NS, (short) 20);
                List<Integer> seen = new ArrayList<>();
                // Prefix is the ns field only; pass NS as the upTo handle.
                boolean done = r.scan(kb, NS, k -> {
                    int ns = ((k[0] & 0xFF) << 8) | (k[1] & 0xFF);
                    int id = decodeUint32(k, 2);
                    assertEquals(20, ns, "scan must only visit ns=20");
                    seen.add(id);
                    return true;
                });
                assertTrue(done);
                assertEquals(List.of(1, 2, 3), seen);
            }
        }
    }
}
