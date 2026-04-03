package org.taotree;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TaoDictionaryTest {

    @Test
    void internBasic() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = TaoDictionary.u16(tree);

            int code1 = dict.intern("Animalia");
            int code2 = dict.intern("Plantae");
            int code3 = dict.intern("Animalia");

            assertTrue(code1 > 0);
            assertTrue(code2 > 0);
            assertNotEquals(code1, code2);
            assertEquals(code1, code3);
            assertEquals(2, dict.size());
        }
    }

    @Test
    void resolveOnly() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = TaoDictionary.u16(tree);
            dict.intern("Chordata");
            assertEquals(-1, dict.resolve("Unknown"));
            assertTrue(dict.resolve("Chordata") > 0);
        }
    }

    @Test
    void monotonicallyIncreasing() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = TaoDictionary.u16(tree);
            int c1 = dict.intern("first");
            int c2 = dict.intern("second");
            int c3 = dict.intern("third");
            assertEquals(c1 + 1, c2);
            assertEquals(c2 + 1, c3);
        }
    }

    @Test
    void nullSentinel() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = TaoDictionary.u16(tree);
            int code = dict.intern("test");
            assertEquals(1, code);
        }
    }

    @Test
    void manyEntries() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = TaoDictionary.u32(tree);
            String[] names = {
                "Animalia", "Plantae", "Fungi", "Protista", "Archaea", "Bacteria", "Chromista",
                "Chordata", "Arthropoda", "Mollusca", "Cnidaria", "Echinodermata",
                "Accipitridae", "Falconidae", "Strigidae", "Columbidae", "Corvidae",
                "Haliaeetus leucocephalus", "Falco peregrinus", "Aquila chrysaetos",
                "US", "DE", "BR", "IN", "AU", "JP", "FR", "GB", "CA", "MX"
            };

            int[] codes = new int[names.length];
            for (int i = 0; i < names.length; i++) codes[i] = dict.intern(names[i]);
            assertEquals(names.length, dict.size());

            for (int i = 0; i < codes.length; i++) {
                for (int j = i + 1; j < codes.length; j++) {
                    assertNotEquals(codes[i], codes[j],
                        names[i] + " and " + names[j] + " have the same code");
                }
            }

            for (int i = 0; i < names.length; i++) {
                assertEquals(codes[i], dict.intern(names[i]));
                assertEquals(codes[i], dict.resolve(names[i]));
            }
        }
    }

    @Test
    void u16TaoDictionaryCapacity() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = new TaoDictionary(tree, 3, 1);
            dict.intern("a");
            dict.intern("b");
            dict.intern("c");
            assertThrows(IllegalStateException.class, () -> dict.intern("d"));
        }
    }

    @Test
    void newDictEntryCodeIsNeverZero() {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = TaoDictionary.u16(tree);
            // All assigned codes must be > 0 (0 is sentinel)
            for (int i = 0; i < 50; i++) {
                int code = dict.intern("name_" + i);
                assertTrue(code > 0, "Code should never be 0, got " + code + " for name_" + i);
            }
        }
    }
}
