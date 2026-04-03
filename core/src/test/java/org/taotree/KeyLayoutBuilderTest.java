package org.taotree;

import org.junit.jupiter.api.Test;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.*;

class KeyLayoutBuilderTest {

    @Test
    void layoutBasic() {
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
    void encodeU16AndU32() {
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
    void encodeDictFields() {
        try (var tree = TaoTree.forDictionaries()) {
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
    void encodeDictNull() {
        try (var tree = TaoTree.forDictionaries()) {
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
    void fullGbifKey() {
        try (var tree = TaoTree.forDictionaries(512 * 1024)) {
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
    void keySchemaWithArt() {
        try (var tree = TaoTree.forDictionaries(512 * 1024)) {
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
}
