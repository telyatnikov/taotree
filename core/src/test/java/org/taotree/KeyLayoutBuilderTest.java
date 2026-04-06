package org.taotree;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

class KeyLayoutBuilderTest {

    @TempDir Path tmp;
    private int fc;

    @Test
    void layoutBasic() throws IOException {
        var layout = KeyLayout.of(
            KeyField.uint16("a"),
            KeyField.uint32("b"),
            KeyField.uint64("c")
        );
        assertEquals(14, layout.totalWidth());
        assertEquals(3, layout.fieldCount());
        assertEquals(0, layout.offset(0));
        assertEquals(2, layout.offset(1));
        assertEquals(6, layout.offset(2));
    }

    @Test
    void encodeU16AndU32() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(
                KeyField.uint16("first"),
                KeyField.uint32("second")
            );
            var enc = new KeyBuilder(layout, arena);
            enc.setU16(0, (short) 42);
            enc.setU32(1, 100);

            assertEquals(6, enc.keyLen());
            assertEquals((short) 42, TaoKey.decodeU16(enc.key(), 0));
            assertEquals(100, TaoKey.decodeU32(enc.key(), 2));
        }
    }

    @Test
    void encodeDictFields() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
            var kingdomDict = TaoDictionary.u16(tree);
            var speciesDict = TaoDictionary.u32(tree);

            var layout = KeyLayout.of(
                KeyField.dict16("kingdom", kingdomDict),
                KeyField.dict32("species", speciesDict)
            );
            assertEquals(6, layout.totalWidth());

            try (var arena = Arena.ofConfined()) {
                var enc = new KeyBuilder(layout, arena);
                enc.setDict(0, "Animalia");
                enc.setDict(1, "Haliaeetus leucocephalus");

                int kingdomCode = kingdomDict.resolve("Animalia");
                int speciesCode = speciesDict.resolve("Haliaeetus leucocephalus");
                assertTrue(kingdomCode > 0);
                assertTrue(speciesCode > 0);

                assertEquals((short) kingdomCode, TaoKey.decodeU16(enc.key(), 0));
                assertEquals(speciesCode, TaoKey.decodeU32(enc.key(), 2));
            }
        }
    }

    @Test
    void encodeDictNull() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
            var dict = TaoDictionary.u16(tree);
            var layout = KeyLayout.of(KeyField.dict16("field", dict));

            try (var arena = Arena.ofConfined()) {
                var enc = new KeyBuilder(layout, arena);
                enc.setDict(0, null);
                assertEquals((short) 0, TaoKey.decodeU16(enc.key(), 0));
            }
        }
    }

    @Test
    void fullGbifKey() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"), 512 * 1024)) {
            var kingdomDict  = TaoDictionary.u16(tree);
            var phylumDict   = TaoDictionary.u16(tree);
            var familyDict   = TaoDictionary.u16(tree);
            var speciesDict  = TaoDictionary.u32(tree);
            var countryDict  = TaoDictionary.u16(tree);
            var stateDict    = TaoDictionary.u16(tree);
            var fieldDict    = TaoDictionary.u16(tree);

            var layout = KeyLayout.of(
                KeyField.dict16("kingdom", kingdomDict),
                KeyField.dict16("phylum",  phylumDict),
                KeyField.dict16("family",  familyDict),
                KeyField.dict32("species", speciesDict),
                KeyField.dict16("country", countryDict),
                KeyField.dict16("state",   stateDict),
                KeyField.dict16("field",   fieldDict)
            );
            assertEquals(16, layout.totalWidth());

            try (var arena = Arena.ofConfined()) {
                var enc = new KeyBuilder(layout, arena);
                enc.setDict(0, "Animalia").setDict(1, "Chordata")
                   .setDict(2, "Accipitridae")
                   .setDict(3, "Haliaeetus leucocephalus")
                   .setDict(4, "US").setDict(5, "Wyoming").setDict(6, "header");

                assertEquals(16, enc.keyLen());

                var enc2 = new KeyBuilder(layout, arena);
                enc2.setDict(0, "Animalia").setDict(1, "Chordata")
                    .setDict(2, "Accipitridae")
                    .setDict(3, "Haliaeetus leucocephalus")
                    .setDict(4, "US").setDict(5, "Wyoming").setDict(6, "header");

                assertEquals(-1, enc.key().mismatch(enc2.key()));

                var enc3 = new KeyBuilder(layout, arena);
                enc3.setDict(0, "Animalia").setDict(1, "Chordata")
                    .setDict(2, "Accipitridae")
                    .setDict(3, "Falco peregrinus")
                    .setDict(4, "US").setDict(5, "Wyoming").setDict(6, "header");

                assertNotEquals(-1, enc.key().mismatch(enc3.key()));
            }
        }
    }

    @Test
    void keySchemaWithArt() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"), 512 * 1024)) {
            var dict = TaoDictionary.u16(tree);
            var layout = KeyLayout.of(
                KeyField.dict16("a", dict),
                KeyField.dict16("b", dict),
                KeyField.uint32("c")
            );

            var art = new TaoTree(tree, layout.totalWidth(), new int[]{8});

            try (var arena = Arena.ofConfined()) {
                var enc = new KeyBuilder(layout, arena);

                try (var w = art.write()) {
                    enc.setDict(0, "x").setDict(1, "y").setU32(2, 1);
                    long l1 = w.getOrCreate(enc.key(), enc.keyLen(), 0);
                    w.leafValue(l1).set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 100L);

                    enc.setDict(0, "x").setDict(1, "y").setU32(2, 2);
                    long l2 = w.getOrCreate(enc.key(), enc.keyLen(), 0);
                    w.leafValue(l2).set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 200L);

                    enc.setDict(0, "x").setDict(1, "z").setU32(2, 1);
                    long l3 = w.getOrCreate(enc.key(), enc.keyLen(), 0);
                    w.leafValue(l3).set(ValueLayout.JAVA_LONG_UNALIGNED, 0, 300L);

                    assertEquals(3, w.size());
                }

                // Encode key BEFORE entering read scope (setDict calls intern
                // which needs write lock — can't do that under a read scope)
                enc.setDict(0, "x").setDict(1, "y").setU32(2, 2);

                try (var r = art.read()) {
                    long found = r.lookup(enc.key(), enc.keyLen());
                    assertEquals(200L, r.leafValue(found).get(ValueLayout.JAVA_LONG_UNALIGNED, 0));
                }
            }
        }
    }

    // ---- Mutation-killing: U8, U64, I64 encoding ----

    @Test
    void encodeU8() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(KeyField.uint8("byte"));
            var enc = new KeyBuilder(layout, arena);
            enc.setU8(0, (byte) 0xAB);
            assertEquals(1, enc.keyLen());
            assertEquals((byte) 0xAB, TaoKey.decodeU8(enc.key(), 0));
        }
    }

    @Test
    void encodeU64() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(KeyField.uint64("big"));
            var enc = new KeyBuilder(layout, arena);
            enc.setU64(0, 0x0102030405060708L);
            assertEquals(8, enc.keyLen());
            assertEquals(0x0102030405060708L, TaoKey.decodeU64(enc.key(), 0));
        }
    }

    @Test
    void encodeI64() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(KeyField.int64("signed"));
            var enc = new KeyBuilder(layout, arena);
            enc.setI64(0, -42L);
            assertEquals(8, enc.keyLen());
            assertEquals(-42L, TaoKey.decodeI64(enc.key(), 0));
        }
    }

    @Test
    void encodeI64OrderPreserving() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(KeyField.int64("signed"));
            var enc1 = new KeyBuilder(layout, arena);
            var enc2 = new KeyBuilder(layout, arena);

            enc1.setI64(0, -100L);
            enc2.setI64(0, 100L);

            // Negative should sort before positive in binary comparison
            assertTrue(enc1.key().mismatch(enc2.key()) >= 0);  // not equal
            // First differing byte of -100 should be less than first differing byte of +100
            byte b1 = enc1.key().get(ValueLayout.JAVA_BYTE, 0);
            byte b2 = enc2.key().get(ValueLayout.JAVA_BYTE, 0);
            assertTrue(Byte.toUnsignedInt(b1) < Byte.toUnsignedInt(b2),
                "Encoded -100 should sort before +100");
        }
    }

    // ---- Mutation-killing: name-based setters ----

    @Test
    void nameBasedSetters() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(
                KeyField.uint8("a"),
                KeyField.uint16("b"),
                KeyField.uint32("c"),
                KeyField.uint64("d"),
                KeyField.int64("e")
            );
            var enc = new KeyBuilder(layout, arena);
            enc.setU8("a", (byte) 1)
               .setU16("b", (short) 2)
               .setU32("c", 3)
               .setU64("d", 4L)
               .setI64("e", -5L);

            assertEquals((byte) 1, TaoKey.decodeU8(enc.key(), 0));
            assertEquals((short) 2, TaoKey.decodeU16(enc.key(), 1));
            assertEquals(3, TaoKey.decodeU32(enc.key(), 3));
            assertEquals(4L, TaoKey.decodeU64(enc.key(), 7));
            assertEquals(-5L, TaoKey.decodeI64(enc.key(), 15));
        }
    }

    // ---- Mutation-killing: chaining returns this ----

    @Test
    void setterChaining() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(KeyField.uint16("a"), KeyField.uint32("b"));
            var enc = new KeyBuilder(layout, arena);
            KeyBuilder result = enc.setU16(0, (short) 1).setU32(1, 2);
            assertSame(enc, result, "Chained setters should return 'this'");
        }
    }

    // ---- Mutation-killing: fieldIndex lookup ----

    @Test
    void fieldIndexLookup() throws IOException {
        var layout = KeyLayout.of(
            KeyField.uint16("first"),
            KeyField.uint32("second"),
            KeyField.uint64("third")
        );
        assertEquals(0, layout.fieldIndex("first"));
        assertEquals(1, layout.fieldIndex("second"));
        assertEquals(2, layout.fieldIndex("third"));
        assertThrows(IllegalArgumentException.class, () -> layout.fieldIndex("nonexistent"));
    }

    // ---- Mutation-killing: KeyField factories ----

    @Test
    void keyFieldFactoryWidths() throws IOException {
        assertEquals(1, KeyField.uint8("x").width());
        assertEquals(2, KeyField.uint16("x").width());
        assertEquals(4, KeyField.uint32("x").width());
        assertEquals(8, KeyField.uint64("x").width());
        assertEquals(8, KeyField.int64("x").width());
    }

    // ---- Round 2: I64 setter chaining ----

    @Test
    void setI64Chaining() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(KeyField.int64("a"), KeyField.int64("b"));
            var enc = new KeyBuilder(layout, arena);
            KeyBuilder result = enc.setI64(0, 100L).setI64(1, -200L);
            assertSame(enc, result);
            assertEquals(100L, TaoKey.decodeI64(enc.key(), 0));
            assertEquals(-200L, TaoKey.decodeI64(enc.key(), 8));
        }
    }

    @Test
    void setI64ByNameChaining() throws IOException {
        try (var arena = Arena.ofConfined()) {
            var layout = KeyLayout.of(KeyField.int64("x"), KeyField.uint32("y"));
            var enc = new KeyBuilder(layout, arena);
            KeyBuilder result = enc.setI64("x", 42L).setU32("y", 7);
            assertSame(enc, result);
            assertEquals(42L, TaoKey.decodeI64(enc.key(), 0));
            assertEquals(7, TaoKey.decodeU32(enc.key(), 8));
        }
    }

    @Test
    void setDictByNameChaining() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
            var dict = TaoDictionary.u16(tree);
            var layout = KeyLayout.of(KeyField.dict16("d", dict), KeyField.uint32("n"));
            try (var arena = Arena.ofConfined()) {
                var enc = new KeyBuilder(layout, arena);
                KeyBuilder result = enc.setDict("d", "hello").setU32("n", 99);
                assertSame(enc, result);
            }
        }
    }

    // ---- STRONGER: KeyLayout.of validation ----

    @Test
    void keyLayoutRejectsNoFields() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> KeyLayout.of());
    }

    // ---- STRONGER: setDict null vs non-null ----

    @Test
    void setDictNullEncodesZero() throws IOException {
        try (var tree = TaoTree.forDictionaries(tmp.resolve(fc++ + ".tao"))) {
            var dict = TaoDictionary.u16(tree);
            var layout = KeyLayout.of(KeyField.dict16("d", dict));

            try (var arena = Arena.ofConfined()) {
                var enc = new KeyBuilder(layout, arena);
                enc.setDict(0, null);
                assertEquals((short) 0, TaoKey.decodeU16(enc.key(), 0));

                // Non-null should encode to > 0
                enc.setDict(0, "hello");
                assertTrue(TaoKey.decodeU16(enc.key(), 0) > 0);
            }
        }
    }
}
