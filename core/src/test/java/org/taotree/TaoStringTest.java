package org.taotree;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class TaoStringTest {

    @Test
    void shortStringInline() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void shortStringExactly12Bytes() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void longStringOverflow() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void longStringLocality() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void emptyString() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void equalsShortStrings() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void equalsLongStrings() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void notEqualsDifferentLength() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void notEqualsDifferentContent() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void equalsBytesShort() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void equalsBytesLong() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
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
    void writeStringOverload() {
        try (var tree = TaoTree.open(TaoString.SIZE, TaoString.SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(new byte[TaoString.SIZE], 0);
                var slot = w.leafValue(leaf);

                TaoString.write(slot, "Panthera tigris", tree);
                assertEquals("Panthera tigris", TaoString.read(slot, tree));
            }
        }
    }
}
