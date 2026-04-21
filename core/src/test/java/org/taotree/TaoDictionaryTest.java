package org.taotree;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class TaoDictionaryTest {

    @TempDir Path tmp;
    private int fc;

    @Test
    void internBasic() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
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
    void resolveOnly() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            dict.intern("Chordata");
            assertEquals(-1, dict.resolve("Unknown"));
            assertTrue(dict.resolve("Chordata") > 0);
        }
    }

    @Test
    void monotonicallyIncreasing() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            int c1 = dict.intern("first");
            int c2 = dict.intern("second");
            int c3 = dict.intern("third");
            assertEquals(c1 + 1, c2);
            assertEquals(c2 + 1, c3);
        }
    }

    @Test
    void nullSentinel() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            int code = dict.intern("test");
            assertEquals(1, code);
        }
    }

    @Test
    void manyEntries() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
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
    void u16TaoDictionaryCapacity() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = new TaoDictionary(tree, 3, 1);
            dict.intern("a");
            dict.intern("b");
            dict.intern("c");
            assertThrows(IllegalStateException.class, () -> dict.intern("d"));
        }
    }

    @Test
    void newDictEntryCodeIsNeverZero() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            // All assigned codes must be > 0 (0 is sentinel)
            for (int i = 0; i < 50; i++) {
                int code = dict.intern("name_" + i);
                assertTrue(code > 0, "Code should never be 0, got " + code + " for name_" + i);
            }
        }
    }

    // ---- Mutation-killing: owner() ----

    @Test
    void ownerReturnsTree() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            assertSame(tree, dict.owner());
        }
    }

    // ---- Mutation-killing: nextCode() ----

    @Test
    void nextCodeTracksState() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            assertEquals(1, dict.nextCode()); // starts at 1
            dict.intern("first");
            assertEquals(2, dict.nextCode());
            dict.intern("second");
            assertEquals(3, dict.nextCode());
            dict.intern("first"); // duplicate → no increment
            assertEquals(3, dict.nextCode());
        }
    }

    // ---- Mutation-killing: scoped access ----

    @Test
    void scopedReadAccess() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            dict.intern("test");

            try (var r = dict.read()) {
                assertEquals(1, r.size());
                assertTrue(r.resolve("test") > 0);
                assertEquals(-1, r.resolve("unknown"));
            }
        }
    }

    @Test
    void scopedWriteAccess() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);

            try (var w = dict.write()) {
                int code = w.intern("hello");
                assertTrue(code > 0);
                assertEquals(1, w.size());
                assertEquals(code, w.resolve("hello"));
                assertEquals(-1, w.resolve("world"));
            }
        }
    }

    // ---- Mutation-killing: size() ----

    @Test
    void sizeTracksEntries() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            assertEquals(0, dict.size());
            dict.intern("a");
            assertEquals(1, dict.size());
            dict.intern("b");
            assertEquals(2, dict.size());
            dict.intern("a"); // duplicate
            assertEquals(2, dict.size());
        }
    }

    // ---- Mutation-killing: encodeAndPad boundary ----

    @Test
    void longStringNearMaxKeyLen() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u32(tree);
            // Create a string that, after encoding, fits within 128 bytes
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 120; i++) sb.append('a');
            int code = dict.intern(sb.toString());
            assertTrue(code > 0);
            assertEquals(code, dict.resolve(sb.toString()));
        }
    }

    // ---- Round 2: scope close releases lock ----

    @Test
    void readScopeCloseReleasesLock() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);
            dict.intern("test");

            // Open and close a read scope
            var r = dict.read();
            assertEquals(1, r.size());
            r.close();

            // If the lock wasn't released, this write would deadlock
            try (var w = dict.write()) {
                int code = w.intern("another");
                assertTrue(code > 0);
            }
        }
    }

    @Test
    void writeScopeCloseReleasesLock() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);

            // Open and close a write scope
            var w = dict.write();
            w.intern("test");
            w.close();

            // If the lock wasn't released, this read would deadlock
            try (var r = dict.read()) {
                assertTrue(r.resolve("test") > 0);
            }
        }
    }

    // ---- Round 2: encodeAndPad boundary ----

    @Test
    void stringExceedingMaxKeyLenThrows() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u32(tree);
            // Create a string whose encoded form exceeds 128 bytes
            // Each char is 1 byte in UTF-8 + null terminator, so 128 chars → 129 bytes encoded
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 128; i++) sb.append('x');
            assertThrows(IllegalArgumentException.class, () -> dict.intern(sb.toString()));
        }
    }

    // ---- Mutation-killing: MAX_KEY_LEN exact boundary ----

    @Test
    void stringExactlyAtMaxKeyLenSucceeds() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u32(tree);
            // Encoded form = raw bytes + null terminator.
            // 127 chars → 127 bytes + 1 null = 128 bytes = MAX_KEY_LEN → should succeed
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 127; i++) sb.append('A');
            int code = dict.intern(sb.toString());
            assertTrue(code > 0, "String at exactly MAX_KEY_LEN should be accepted");
            assertEquals(code, dict.resolve(sb.toString()));
        }
    }

    // ---- Mutation-killing: WriteScope.close() releases write lock ----

    @Test
    void writeScopeCloseReleasesLockFromAnotherThread() throws Exception {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var dict = TaoDictionary.u16(tree);

            // Open write scope and close it
            var w = dict.write();
            w.intern("test");
            w.close();

            // Verify lock is released: another thread must be able to acquire it
            var result = new java.util.concurrent.atomic.AtomicBoolean(false);
            var thread = Thread.ofVirtual().start(() -> {
                int code = dict.resolve("test");
                result.set(code > 0);
            });
            thread.join(5000);
            assertTrue(result.get(), "Lock should be released after WriteScope.close()");
        }
    }

    // ---- Mutation-killing: copyFrom requires write lock ----

    @Test
    void copyFromSelfLocking() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint8("k")))) {
            var source = TaoDictionary.u16(tree);
            source.intern("hello");

            var target = TaoDictionary.u16(tree);

            // copyFrom is self-locking — no external write scope needed
            target.copyFrom(source);
            assertEquals(1, target.size());
        }
    }
}
