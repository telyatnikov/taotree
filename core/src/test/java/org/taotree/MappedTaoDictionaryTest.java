package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for file-backed TaoDictionary persistence.
 */
class MappedTaoDictionaryTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    @TempDir Path tmp;

    private byte[] intKey(int value) {
        byte[] key = new byte[KEY_LEN];
        key[0] = (byte) (value >>> 24);
        key[1] = (byte) (value >>> 16);
        key[2] = (byte) (value >>> 8);
        key[3] = (byte) value;
        return key;
    }

    @Test
    void singleDictRoundTrip() throws IOException {
        Path file = tmp.resolve("test.tao");

        // Phase 1: create tree + dict, intern strings, close
        try (var tree = TaoTree.create(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            var dict = TaoDictionary.u16(tree);
            assertEquals(2, tree.dictionaryCount());

            int animalia = dict.intern("Animalia");
            int plantae = dict.intern("Plantae");
            int fungi = dict.intern("Fungi");

            assertEquals(1, animalia);
            assertEquals(2, plantae);
            assertEquals(3, fungi);
            assertEquals(3, dict.size());
        }

        // Phase 2: reopen and verify dict is restored
        try (var tree = TaoTree.open(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            assertEquals(2, tree.dictionaryCount());
            var dict = tree.dictionary(1);
            assertNotNull(dict);

            // Resolve should find all interned strings with same codes
            assertEquals(1, dict.resolve("Animalia"));
            assertEquals(2, dict.resolve("Plantae"));
            assertEquals(3, dict.resolve("Fungi"));
            assertEquals(-1, dict.resolve("Unknown"));
            assertEquals(3, dict.size());

            // Intern new string should get code 4
            int bacteria = dict.intern("Bacteria");
            assertEquals(4, bacteria);
            assertEquals(4, dict.size());
        }

        // Phase 3: reopen again, verify new entry persisted
        try (var tree = TaoTree.open(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            var dict = tree.dictionary(1);
            assertEquals(4, dict.size());
            assertEquals(4, dict.resolve("Bacteria"));
        }
    }

    @Test
    void multipleDictsRoundTrip() throws IOException {
        Path file = tmp.resolve("test.tao");

        // Phase 1: create tree + 3 dicts
        try (var tree = TaoTree.create(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            var kingdomDict = TaoDictionary.u16(tree);
            var familyDict = TaoDictionary.u16(tree);
            var countryDict = TaoDictionary.u32(tree);

            assertEquals(4, tree.dictionaryCount());

            kingdomDict.intern("Animalia");
            kingdomDict.intern("Plantae");

            familyDict.intern("Accipitridae");
            familyDict.intern("Felidae");
            familyDict.intern("Canidae");

            countryDict.intern("US");
            countryDict.intern("DE");

            assertEquals(2, kingdomDict.size());
            assertEquals(3, familyDict.size());
            assertEquals(2, countryDict.size());
        }

        // Phase 2: reopen and verify all dicts restored
        try (var tree = TaoTree.open(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            assertEquals(4, tree.dictionaryCount());

            var kingdomDict = tree.dictionary(1);
            var familyDict = tree.dictionary(2);
            var countryDict = tree.dictionary(3);

            assertEquals(2, kingdomDict.size());
            assertEquals(1, kingdomDict.resolve("Animalia"));
            assertEquals(2, kingdomDict.resolve("Plantae"));

            assertEquals(3, familyDict.size());
            assertEquals(1, familyDict.resolve("Accipitridae"));
            assertEquals(2, familyDict.resolve("Felidae"));
            assertEquals(3, familyDict.resolve("Canidae"));

            assertEquals(2, countryDict.size());
            assertEquals(1, countryDict.resolve("US"));
            assertEquals(2, countryDict.resolve("DE"));
        }
    }

    @Test
    void dictWithDataTree() throws IOException {
        Path file = tmp.resolve("test.tao");

        // Phase 1: dict + data tree together
        try (var tree = TaoTree.create(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            var dict = TaoDictionary.u16(tree);

            // Intern some strings
            int code1 = dict.intern("Alpha");
            int code2 = dict.intern("Beta");

            // Insert into data tree
            try (var w = tree.write()) {
                for (int i = 1; i <= 50; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i * 10);
                }
            }

            assertEquals(50, tree.read().size());
        }

        // Phase 2: verify both data tree and dict
        try (var tree = TaoTree.open(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            // Data tree
            try (var r = tree.read()) {
                assertEquals(50, r.size());
                for (int i = 1; i <= 50; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i);
                    assertEquals((long) i * 10,
                        r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }

            // Dict
            assertEquals(2, tree.dictionaryCount());
            var dict = tree.dictionary(1);
            assertEquals(2, dict.size());
            assertEquals(1, dict.resolve("Alpha"));
            assertEquals(2, dict.resolve("Beta"));
        }
    }

    @Test
    void largeDictRoundTrip() throws IOException {
        Path file = tmp.resolve("test.tao");

        // Phase 1: intern 1000 unique strings
        try (var tree = TaoTree.create(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            var dict = TaoDictionary.u16(tree);
            for (int i = 0; i < 1000; i++) {
                int code = dict.intern("string_" + i);
                assertEquals(i + 1, code); // codes start at 1
            }
            assertEquals(1000, dict.size());
        }

        // Phase 2: verify all 1000 strings
        try (var tree = TaoTree.open(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            var dict = tree.dictionary(1);
            assertEquals(1000, dict.size());

            for (int i = 0; i < 1000; i++) {
                assertEquals(i + 1, dict.resolve("string_" + i),
                    "Mismatch for string_" + i);
            }
            assertEquals(-1, dict.resolve("nonexistent"));

            // Next code should be 1001
            assertEquals(1001, dict.intern("new_string"));
        }
    }
}
