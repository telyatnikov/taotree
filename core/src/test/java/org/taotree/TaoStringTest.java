package org.taotree;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class TaoStringTest {

    @TempDir Path tmp;
    private int fc;

    @Test
    void shortStringInline() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
                TaoString.write(slot, data, tree);

                assertTrue(TaoString.isShort(slot));
                assertEquals(5, TaoString.length(slot));
                assertArrayEquals(data, TaoString.readBytes(slot, tree));
                assertEquals("hello", TaoString.read(slot, tree));
            }
        }
    }

    @Test
    void shortStringExactly12Bytes() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                byte[] data = "Elops smithi".getBytes(StandardCharsets.UTF_8);
                assertEquals(12, data.length);
                TaoString.write(slot, data, tree);

                assertTrue(TaoString.isShort(slot));
                assertArrayEquals(data, TaoString.readBytes(slot, tree));
            }
        }
    }

    @Test
    void longStringOverflow() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                String species = "Haliaeetus leucocephalus";
                byte[] data = species.getBytes(StandardCharsets.UTF_8);
                assertTrue(data.length > 12);
                TaoString.write(slot, species, tree);

                assertFalse(TaoString.isShort(slot));
                assertEquals(data.length, TaoString.length(slot));
                assertArrayEquals(data, TaoString.readBytes(slot, tree));
                assertEquals(species, TaoString.read(slot, tree));
            }
        }
    }

    @Test
    void longStringLocality() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                String s = "Yellowstone Lake, near fishing bridge";
                TaoString.write(slot, s, tree);

                assertEquals(s, TaoString.read(slot, tree));
            }
        }
    }

    @Test
    void emptyString() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                TaoString.write(slot, new byte[0], tree);
                assertTrue(TaoString.isShort(slot));
                assertEquals(0, TaoString.length(slot));
                assertArrayEquals(new byte[0], TaoString.readBytes(slot, tree));
            }
        }
    }

    @Test
    void equalsShortStrings() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            byte[] keyA = new byte[TaoString.SIZE];
            byte[] keyB = new byte[TaoString.SIZE];
            keyA[0] = 1;
            keyB[0] = 2;

            try (var w = tree.write()) {
                long leafA = w.getOrCreate(keyA, 0);
                long leafB = w.getOrCreate(keyB, 0);
                var a = w.leafValue(leafA);
                var b = w.leafValue(leafB);

                byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
                TaoString.write(a, data, tree);
                TaoString.write(b, data, tree);

                assertTrue(TaoString.equals(a, b, tree));
            }
        }
    }

    @Test
    void equalsLongStrings() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            byte[] keyA = new byte[TaoString.SIZE];
            byte[] keyB = new byte[TaoString.SIZE];
            keyA[0] = 1;
            keyB[0] = 2;

            try (var w = tree.write()) {
                long leafA = w.getOrCreate(keyA, 0);
                long leafB = w.getOrCreate(keyB, 0);
                var a = w.leafValue(leafA);
                var b = w.leafValue(leafB);

                byte[] data = "Centropomus undecimalis".getBytes(StandardCharsets.UTF_8);
                TaoString.write(a, data, tree);
                TaoString.write(b, data, tree);

                assertTrue(TaoString.equals(a, b, tree));
            }
        }
    }

    @Test
    void notEqualsDifferentLength() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            byte[] keyA = new byte[TaoString.SIZE];
            byte[] keyB = new byte[TaoString.SIZE];
            keyA[0] = 1;
            keyB[0] = 2;

            try (var w = tree.write()) {
                long leafA = w.getOrCreate(keyA, 0);
                long leafB = w.getOrCreate(keyB, 0);
                var a = w.leafValue(leafA);
                var b = w.leafValue(leafB);

                TaoString.write(a, "abc".getBytes(StandardCharsets.UTF_8), tree);
                TaoString.write(b, "abcd".getBytes(StandardCharsets.UTF_8), tree);

                assertFalse(TaoString.equals(a, b, tree));
            }
        }
    }

    @Test
    void notEqualsDifferentContent() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            byte[] keyA = new byte[TaoString.SIZE];
            byte[] keyB = new byte[TaoString.SIZE];
            keyA[0] = 1;
            keyB[0] = 2;

            try (var w = tree.write()) {
                long leafA = w.getOrCreate(keyA, 0);
                long leafB = w.getOrCreate(keyB, 0);
                var a = w.leafValue(leafA);
                var b = w.leafValue(leafB);

                TaoString.write(a, "Haliaeetus leucocephalus".getBytes(StandardCharsets.UTF_8), tree);
                TaoString.write(b, "Haliaeetus pelagicus".getBytes(StandardCharsets.UTF_8), tree);

                assertFalse(TaoString.equals(a, b, tree));
            }
        }
    }

    @Test
    void equalsBytesShort() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                byte[] data = "US".getBytes(StandardCharsets.UTF_8);
                TaoString.write(slot, data, tree);

                assertTrue(TaoString.equalsBytes(slot, data, tree));
                assertFalse(TaoString.equalsBytes(slot, "DE".getBytes(StandardCharsets.UTF_8), tree));
            }
        }
    }

    @Test
    void equalsBytesLong() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                byte[] data = "HUMAN_OBSERVATION".getBytes(StandardCharsets.UTF_8);
                TaoString.write(slot, data, tree);

                assertTrue(TaoString.equalsBytes(slot, data, tree));
                assertFalse(TaoString.equalsBytes(slot, "MACHINE_OBSERVATION".getBytes(StandardCharsets.UTF_8), tree));
            }
        }
    }

    @Test
    void writeStringOverload() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                TaoString.write(slot, "Panthera tigris", tree);
                assertEquals("Panthera tigris", TaoString.read(slot, tree));
            }
        }
    }

    // ---- Mutation-killing: write length field ----

    @Test
    void writeLengthFieldIsSet() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                // Write then verify length can be read back correctly
                byte[] data = "test".getBytes(StandardCharsets.UTF_8);
                TaoString.write(slot, data, tree);

                // Overwrite with a different-length string
                byte[] data2 = "longer data!".getBytes(StandardCharsets.UTF_8);
                TaoString.write(slot, data2, tree);
                assertEquals(data2.length, TaoString.length(slot));
                assertArrayEquals(data2, TaoString.readBytes(slot, tree));
            }
        }
    }

    @Test
    void writeLongStringLengthFieldIsSet() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                String s = "Haliaeetus leucocephalus";
                TaoString.write(slot, s, tree);
                assertEquals(s.length(), TaoString.length(slot));
                assertEquals(s, TaoString.read(slot, tree));
            }
        }
    }

    // ---- Mutation-killing: equals fast path ----

    @Test
    void equalsReturnsFalseForDifferentShortInlineData() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            byte[] keyA = new byte[TaoString.SIZE];
            byte[] keyB = new byte[TaoString.SIZE];
            keyA[0] = 1; keyB[0] = 2;

            try (var w = tree.write()) {
                long leafA = w.getOrCreate(keyA, 0);
                long leafB = w.getOrCreate(keyB, 0);
                var a = w.leafValue(leafA);
                var b = w.leafValue(leafB);

                // Same length, different content in data bytes 4-7 (slot offset 8-11)
                TaoString.write(a, "AAAA1111".getBytes(StandardCharsets.UTF_8), tree); // 8 bytes
                TaoString.write(b, "AAAA2222".getBytes(StandardCharsets.UTF_8), tree); // 8 bytes

                assertFalse(TaoString.equals(a, b, tree));
            }
        }
    }

    @Test
    void equalsLongStringSamePrefix() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            byte[] keyA = new byte[TaoString.SIZE];
            byte[] keyB = new byte[TaoString.SIZE];
            keyA[0] = 1; keyB[0] = 2;

            try (var w = tree.write()) {
                long leafA = w.getOrCreate(keyA, 0);
                long leafB = w.getOrCreate(keyB, 0);
                var a = w.leafValue(leafA);
                var b = w.leafValue(leafB);

                // Same 4-byte prefix, same length, different suffix → tests overflow comparison
                TaoString.write(a, "ABCD_different_tail_1".getBytes(StandardCharsets.UTF_8), tree);
                TaoString.write(b, "ABCD_different_tail_2".getBytes(StandardCharsets.UTF_8), tree);

                assertFalse(TaoString.equals(a, b, tree));
            }
        }
    }

    // ---- Mutation-killing: equalsBytes boundary conditions ----

    @Test
    void equalsBytesBoundaryAt13Bytes() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                byte[] data = "1234567890123".getBytes(StandardCharsets.UTF_8);  // 13 bytes = overflow
                TaoString.write(slot, data, tree);

                assertTrue(TaoString.equalsBytes(slot, data, tree));
                // Same length, different content
                assertFalse(TaoString.equalsBytes(slot, "1234567890124".getBytes(StandardCharsets.UTF_8), tree));
                // Same prefix, different length
                assertFalse(TaoString.equalsBytes(slot, "123456789012".getBytes(StandardCharsets.UTF_8), tree));
            }
        }
    }

    @Test
    void equalsBytesShortLastByteDiffers() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                byte[] data = "abcdefghijkl".getBytes(StandardCharsets.UTF_8);  // 12 bytes inline
                TaoString.write(slot, data, tree);

                assertTrue(TaoString.equalsBytes(slot, data, tree));
                // Differ only in last byte
                byte[] diff = "abcdefghijkm".getBytes(StandardCharsets.UTF_8);
                assertFalse(TaoString.equalsBytes(slot, diff, tree));
            }
        }
    }

    @Test
    void equalsBytesLongPrefixDiffers() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                byte[] data = "ABCDEFGHIJKLMNOP".getBytes(StandardCharsets.UTF_8);  // 16 bytes overflow
                TaoString.write(slot, data, tree);

                assertTrue(TaoString.equalsBytes(slot, data, tree));
                // Differ in prefix (byte 2)
                byte[] diffPrefix = "ABxDEFGHIJKLMNOP".getBytes(StandardCharsets.UTF_8);
                assertFalse(TaoString.equalsBytes(slot, diffPrefix, tree));
            }
        }
    }

    // ---- Round 2: zero-padding mutation ----

    @Test
    void shortStringZeroPadded() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                // Write a long string first to fill the slot with non-zero data
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);
                TaoString.write(slot, "AAAAAAAAAAAA".getBytes(StandardCharsets.UTF_8), tree); // 12 bytes, fills payload
                // Now overwrite with a shorter string
                TaoString.write(slot, "BB".getBytes(StandardCharsets.UTF_8), tree); // 2 bytes
                // Read back and verify exact content (no leftover from previous write)
                assertArrayEquals("BB".getBytes(StandardCharsets.UTF_8), TaoString.readBytes(slot, tree));
                // Verify by checking equality with a fresh write of "BB"
                byte[] key2 = new byte[TaoString.SIZE];
                key2[0] = 2;
                long leaf2 = w.getOrCreate(key2, 0);
                var slot2 = w.leafValue(leaf2);
                TaoString.write(slot2, "BB".getBytes(StandardCharsets.UTF_8), tree);
                assertTrue(TaoString.equals(slot, slot2, tree), "Zero-padding mismatch after overwrite");
            }
        }
    }

    // ---- Round 2: equals boundary at exactly 12 bytes ----

    @Test
    void equalsExactly12ByteStrings() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            byte[] keyA = new byte[TaoString.SIZE];
            byte[] keyB = new byte[TaoString.SIZE];
            keyA[0] = 1; keyB[0] = 2;

            try (var w = tree.write()) {
                long leafA = w.getOrCreate(keyA, 0);
                long leafB = w.getOrCreate(keyB, 0);
                var a = w.leafValue(leafA);
                var b = w.leafValue(leafB);

                // Exactly 12 bytes — boundary case for SHORT_THRESHOLD
                byte[] data = "AbcDef012345".getBytes(StandardCharsets.UTF_8);
                assertEquals(12, data.length);
                TaoString.write(a, data, tree);
                TaoString.write(b, data, tree);

                assertTrue(TaoString.equals(a, b, tree));
                assertTrue(TaoString.isShort(a));
            }
        }
    }

    // ---- Round 2: equalsBytes long string with matching prefix ----

    @Test
    void equalsBytesLongStringMatchingPrefixDifferentTail() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                // 20-byte string: same 4-byte prefix, different after that
                byte[] data = "ABCDxxxxxxxxxxxx1234".getBytes(StandardCharsets.UTF_8);
                TaoString.write(slot, data, tree);

                assertTrue(TaoString.equalsBytes(slot, data, tree));
                // Same prefix "ABCD" but different tail
                byte[] diff = "ABCDxxxxxxxxxxxx5678".getBytes(StandardCharsets.UTF_8);
                assertFalse(TaoString.equalsBytes(slot, diff, tree));
            }
        }
    }

    // ---- STRONGER: equalsBytes length mismatch kills ----

    @Test
    void equalsBytesRejectsDifferentLengths() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                TaoString.write(slot, "abc".getBytes(StandardCharsets.UTF_8), tree);

                // Different lengths must return false
                assertFalse(TaoString.equalsBytes(slot, "ab".getBytes(StandardCharsets.UTF_8), tree));
                assertFalse(TaoString.equalsBytes(slot, "abcd".getBytes(StandardCharsets.UTF_8), tree));
                assertFalse(TaoString.equalsBytes(slot, "".getBytes(StandardCharsets.UTF_8), tree));
            }
        }
    }

    @Test
    void equalsBytesLongDifferentInFirstPrefixByte() throws IOException {
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                // 16 bytes → long string
                byte[] data = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
                TaoString.write(slot, data, tree);

                // Differ in first byte of prefix (byte 0)
                byte[] diff = "X123456789abcdef".getBytes(StandardCharsets.UTF_8);
                assertFalse(TaoString.equalsBytes(slot, diff, tree));
            }
        }
    }
}
