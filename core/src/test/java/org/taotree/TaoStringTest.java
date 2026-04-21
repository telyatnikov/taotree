package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct tests for the package-private {@link TaoString} 16-byte German-string
 * codec. Placed in {@code org.taotree} so we can reach both the codec and the
 * package-private {@link TaoTree#bump()} accessor.
 */
class TaoStringTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }

    private TaoTree tree() throws IOException {
        return TaoTree.create(path(), KeyLayout.of(KeyField.uint32("id")));
    }

    private static MemorySegment slot(Arena arena) {
        return arena.allocate(TaoString.SIZE, 1);
    }

    // -----------------------------------------------------------------
    // Short string round-trip (≤ 12 bytes → fully inline)
    // -----------------------------------------------------------------

    @Test
    void shortStringRoundTripsInline() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            TaoString.write(s, "hello", tree);

            assertTrue(TaoString.isShort(s));
            assertEquals(5, TaoString.length(s));
            assertEquals("hello", TaoString.read(s, tree));
            assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8),
                    TaoString.readBytes(s, tree));
        }
    }

    @Test
    void shortStringAtBoundaryIsStillInline() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            String twelve = "abcdefghijkl"; // exactly 12 bytes
            TaoString.write(s, twelve, tree);

            assertTrue(TaoString.isShort(s));
            assertEquals(12, TaoString.length(s));
            assertEquals(twelve, TaoString.read(s, tree));
        }
    }

    // -----------------------------------------------------------------
    // Long string round-trip (> 12 bytes → overflow)
    // -----------------------------------------------------------------

    @Test
    void longStringRoundTripsViaOverflow() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            String big = "abcdefghijklm"; // 13 bytes → long
            TaoString.write(s, big, tree);

            assertFalse(TaoString.isShort(s));
            assertEquals(13, TaoString.length(s));
            assertEquals(big, TaoString.read(s, tree));
            assertArrayEquals(big.getBytes(StandardCharsets.UTF_8),
                    TaoString.readBytes(s, tree));
        }
    }

    @Test
    void veryLongStringRoundTrips() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            String big = "x".repeat(8192);
            TaoString.write(s, big, tree);

            assertFalse(TaoString.isShort(s));
            assertEquals(8192, TaoString.length(s));
            assertEquals(big, TaoString.read(s, tree));
        }
    }

    // -----------------------------------------------------------------
    // Empty string
    // -----------------------------------------------------------------

    @Test
    void emptyStringIsShort() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            TaoString.write(s, "", tree);

            assertTrue(TaoString.isShort(s));
            assertEquals(0, TaoString.length(s));
            assertEquals("", TaoString.read(s, tree));
            assertArrayEquals(new byte[0], TaoString.readBytes(s, tree));
        }
    }

    // -----------------------------------------------------------------
    // UTF-8 multibyte
    // -----------------------------------------------------------------

    @Test
    void utf8MultibyteShortRoundTrip() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            String emoji = "🦅"; // 4-byte UTF-8
            TaoString.write(s, emoji, tree);
            assertTrue(TaoString.isShort(s));
            assertEquals(4, TaoString.length(s));
            assertEquals(emoji, TaoString.read(s, tree));
        }
    }

    @Test
    void utf8MultibyteLongRoundTrip() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            String big = "éèàü".repeat(20); // many 2-byte code points
            TaoString.write(s, big, tree);
            assertFalse(TaoString.isShort(s));
            assertEquals(big, TaoString.read(s, tree));
        }
    }

    // -----------------------------------------------------------------
    // equals() — short/short
    // -----------------------------------------------------------------

    @Test
    void equalsShortShort() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var a = slot(arena);
            var b = slot(arena);
            var c = slot(arena);
            TaoString.write(a, "hello", tree);
            TaoString.write(b, "hello", tree);
            TaoString.write(c, "world", tree);

            assertTrue(TaoString.equals(a, b, tree));
            assertTrue(TaoString.equals(b, a, tree));
            assertFalse(TaoString.equals(a, c, tree));
        }
    }

    // -----------------------------------------------------------------
    // equals() — different length buckets never equal
    // -----------------------------------------------------------------

    @Test
    void equalsShortLongNotEqual() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var shrt = slot(arena);
            var lng = slot(arena);
            TaoString.write(shrt, "short", tree);
            TaoString.write(lng, "this-is-a-longer-string", tree);
            assertFalse(TaoString.equals(shrt, lng, tree));
            assertFalse(TaoString.equals(lng, shrt, tree));
        }
    }

    // -----------------------------------------------------------------
    // equals() — long/long same + different
    // -----------------------------------------------------------------

    @Test
    void equalsLongLong() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var a = slot(arena);
            var b = slot(arena);
            String s = "Haliaeetus leucocephalus";
            TaoString.write(a, s, tree);
            TaoString.write(b, s, tree);
            assertTrue(TaoString.equals(a, b, tree));
        }
    }

    @Test
    void equalsLongLongSamePrefixDifferentTails() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var a = slot(arena);
            var b = slot(arena);
            // Same 4-byte prefix "Halt" but differ after; same length so the
            // header word collides and the code must compare the overflow.
            TaoString.write(a, "Halteridium trailing1xx", tree);
            TaoString.write(b, "Halteridium trailing2xx", tree);
            assertFalse(TaoString.equals(a, b, tree));
        }
    }

    // -----------------------------------------------------------------
    // equalsBytes()
    // -----------------------------------------------------------------

    @Test
    void equalsBytesShortMatch() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            TaoString.write(s, "abc", tree);
            assertTrue(TaoString.equalsBytes(s, "abc".getBytes(StandardCharsets.UTF_8), tree));
            assertFalse(TaoString.equalsBytes(s, "abd".getBytes(StandardCharsets.UTF_8), tree));
            // Different length → fast-reject
            assertFalse(TaoString.equalsBytes(s, "abcd".getBytes(StandardCharsets.UTF_8), tree));
        }
    }

    @Test
    void equalsBytesLongMatch() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            String big = "this-is-a-longer-string";
            TaoString.write(s, big, tree);
            assertTrue(TaoString.equalsBytes(s, big.getBytes(StandardCharsets.UTF_8), tree));

            byte[] diffPrefix = big.getBytes(StandardCharsets.UTF_8).clone();
            diffPrefix[0] = 'T';
            assertFalse(TaoString.equalsBytes(s, diffPrefix, tree));

            byte[] diffTail = big.getBytes(StandardCharsets.UTF_8).clone();
            diffTail[diffTail.length - 1] = '!';
            assertFalse(TaoString.equalsBytes(s, diffTail, tree));
        }
    }

    @Test
    void equalsShortShortSameHeaderDifferentInlineInt() throws IOException {
        // Same length (6) with identical first 4 data bytes ("abcd") ⇒ header
        // longs at offset 0 are equal. The codec must compare the second inline
        // int (offset 8) to detect the mismatch in the remaining payload.
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var a = slot(arena);
            var b = slot(arena);
            TaoString.write(a, "abcdXY", tree);
            TaoString.write(b, "abcdZW", tree);
            assertFalse(TaoString.equals(a, b, tree),
                    "short strings differing only in payload bytes 8..11 must compare unequal");
        }
    }

    @Test
    void equalsBytesAtShortThresholdBoundary() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            String twelve = "abcdefghijkl"; // exactly 12 bytes → short path
            TaoString.write(s, twelve, tree);
            assertTrue(TaoString.equalsBytes(s,
                    twelve.getBytes(StandardCharsets.UTF_8), tree));
            // Differ in the last byte — boundary mutator would miss this
            // if the `<=` were flipped to `<`.
            byte[] diffLast = twelve.getBytes(StandardCharsets.UTF_8).clone();
            diffLast[11] = 'Z';
            assertFalse(TaoString.equalsBytes(s, diffLast, tree));
        }
    }

    @Test
    void equalsBytesLongPrefixDifferAtEveryIndex() throws IOException {
        // Covers each of the 4 prefix-byte comparisons by writing a long
        // string and then testing a differing byte at each prefix index.
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            byte[] raw = "abcd-long-string-longer-than-12b".getBytes(StandardCharsets.UTF_8);
            TaoString.write(s, raw, tree);
            for (int i = 0; i < 4; i++) {
                byte[] mod = raw.clone();
                mod[i] = (byte) (mod[i] ^ 0x40);
                assertFalse(TaoString.equalsBytes(s, mod, tree),
                        "prefix-byte diff at index " + i + " must reject");
            }
            // And a matching byte array must still match.
            assertTrue(TaoString.equalsBytes(s, raw, tree));
        }
    }

    @Test
    void equalsBytesViaByteArrayOverload() throws IOException {
        try (var tree = tree(); var arena = Arena.ofConfined()) {
            var s = slot(arena);
            byte[] raw = "bytes-ok".getBytes(StandardCharsets.UTF_8);
            TaoString.write(s, raw, tree);
            assertEquals("bytes-ok", TaoString.read(s, tree));
            assertTrue(TaoString.equalsBytes(s, raw, tree));
        }
    }
}
