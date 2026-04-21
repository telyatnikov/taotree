package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises ART delete paths: collapseSingleChild, afterRemoval, mergePrefixes
 * (in CowNodeOps) and copyAndRemoveChild (in ChampMap), plus ART leaf expansion
 * on insertion with shared prefixes.
 *
 * <p>Each scenario is designed so that deleting one or more keys forces a
 * structural change (leaf removal, node collapse, prefix merge).
 */
class DeleteShapeTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout layout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void deleteLastSiblingCollapsesParentPrefix() throws IOException {
        // Insert two keys whose high bits share a long prefix and then remove
        // one — forcing the parent Node4 to merge back into its grandparent.
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            // Two near-identical keys (diverge at last byte)
            kb.set(ID, 0x01020301); try (var w = tree.write()) { w.put(kb, "a", Value.ofInt(1)); }
            kb.set(ID, 0x01020302); try (var w = tree.write()) { w.put(kb, "a", Value.ofInt(2)); }
            kb.set(ID, 0x01020303); try (var w = tree.write()) { w.put(kb, "a", Value.ofInt(3)); }
            // A key that diverges earlier
            kb.set(ID, 0xFF000000); try (var w = tree.write()) { w.put(kb, "a", Value.ofInt(99)); }

            try (var r = tree.read()) { assertEquals(4, r.size()); }

            try (var w = tree.write()) {
                kb.set(ID, 0x01020301);
                assertTrue(w.delete(kb));
                kb.set(ID, 0x01020302);
                assertTrue(w.delete(kb));
            }

            try (var r = tree.read()) {
                kb.set(ID, 0x01020303);
                assertEquals(Value.ofInt(3), r.get(kb, "a"));
                kb.set(ID, 0xFF000000);
                assertEquals(Value.ofInt(99), r.get(kb, "a"));
                assertEquals(2, r.size());
            }

            // Delete the remaining prefix sibling — triggers final collapse.
            try (var w = tree.write()) {
                kb.set(ID, 0x01020303);
                assertTrue(w.delete(kb));
            }
            try (var r = tree.read()) {
                assertEquals(1, r.size());
                kb.set(ID, 0xFF000000);
                assertEquals(Value.ofInt(99), r.get(kb, "a"));
            }
        }
    }

    @Test
    void manyRandomInsertDeletesMatchReferenceSet() throws IOException {
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            var random = new Random(42);
            var present = new TreeSet<Integer>();

            List<Integer> keys = new ArrayList<>();
            for (int i = 0; i < 200; i++) keys.add(random.nextInt(1_000_000));
            Collections.shuffle(keys, random);

            try (var w = tree.write()) {
                for (int k : keys) {
                    kb.set(ID, k);
                    w.put(kb, "v", Value.ofInt(k));
                    present.add(k);
                }
            }

            // Delete a random subset in random order.
            List<Integer> toDelete = new ArrayList<>(present);
            Collections.shuffle(toDelete, random);
            toDelete = toDelete.subList(0, present.size() / 2);

            try (var w = tree.write()) {
                for (int k : toDelete) {
                    kb.set(ID, k);
                    assertTrue(w.delete(kb));
                    present.remove(k);
                }
            }

            try (var r = tree.read()) {
                assertEquals(present.size(), r.size());
                // Confirm each surviving key reads correctly.
                for (int k : present) {
                    kb.set(ID, k);
                    assertEquals(Value.ofInt(k), r.get(kb, "v"));
                }
                // Forward scan returns keys in ascending order and matches set.
                List<Integer> scanned = new ArrayList<>();
                r.forEach(key -> {
                    int v = ((key[0] & 0xFF) << 24) | ((key[1] & 0xFF) << 16)
                            | ((key[2] & 0xFF) << 8) | (key[3] & 0xFF);
                    scanned.add(v);
                    return true;
                });
                assertEquals(new ArrayList<>(present), scanned);
            }

            // Finally delete everything, empty tree.
            try (var w = tree.write()) {
                for (int k : present) {
                    kb.set(ID, k);
                    assertTrue(w.delete(kb));
                }
            }
            try (var r = tree.read()) {
                assertEquals(0, r.size());
                assertTrue(r.isEmpty());
            }
        }
    }

    @Test
    void insertTwoKeysSharingLongPrefixExpandsLeafAndRoundtrips() throws IOException {
        // Force CowInsert.expandLeaf: a single key lives in a collapsed leaf,
        // a second key sharing almost all bytes must expand the leaf path.
        try (var tree = TaoTree.create(path(), layout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);

            kb.set(ID, 0x0AAAAAAA);
            try (var w = tree.write()) { w.put(kb, "v", Value.ofInt(1)); }

            kb.set(ID, 0x0AAAAAAB);
            try (var w = tree.write()) { w.put(kb, "v", Value.ofInt(2)); }

            try (var r = tree.read()) {
                kb.set(ID, 0x0AAAAAAA);
                assertEquals(Value.ofInt(1), r.get(kb, "v"));
                kb.set(ID, 0x0AAAAAAB);
                assertEquals(Value.ofInt(2), r.get(kb, "v"));
            }
        }
    }

    @Test
    void deleteByteArrayKey() throws IOException {
        try (var tree = TaoTree.create(path(), layout())) {
            int aid = tree.internAttr("v");
            byte[] k = {0x10, 0x20, 0x30, 0x40};
            try (var w = tree.write()) {
                // Use the Value API to obtain a real encoded ref (raw putTemporal
                // now validates refs — synthetic longs are rejected fail-fast).
                var kb = tree.newKeyBuilder(java.lang.foreign.Arena.ofConfined());
                kb.setU32(0, 0x10203040);
                w.put(kb, "v", org.taotree.Value.ofInt(1));
            }
            try (var w = tree.write()) {
                // MemorySegment/byte[] delete overload.
                assertTrue(w.delete(k));
                assertFalse(w.delete(k)); // already gone
            }
        }
    }
}
