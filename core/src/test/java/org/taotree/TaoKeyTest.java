package org.taotree;

import org.junit.jupiter.api.Test;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.*;

class TaoKeyTest {

    @Test
    void encodeDecodeU16() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(2);
            TaoKey.encodeU16(seg, 0, (short) 0x1234);
            // Big-endian: 0x12 first
            assertEquals(0x12, seg.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0));
            assertEquals(0x34, seg.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 1));
            assertEquals((short) 0x1234, TaoKey.decodeU16(seg, 0));
        }
    }

    @Test
    void encodeDecodeU32() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(4);
            TaoKey.encodeU32(seg, 0, 0x12345678);
            assertEquals(0x12345678, TaoKey.decodeU32(seg, 0));
        }
    }

    @Test
    void encodeDecodeU64() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(8);
            TaoKey.encodeU64(seg, 0, 0x123456789ABCDEF0L);
            assertEquals(0x123456789ABCDEF0L, TaoKey.decodeU64(seg, 0));
        }
    }

    @Test
    void signedI64Ordering() {
        try (var arena = Arena.ofConfined()) {
            var negSeg = arena.allocate(8);
            var zeroSeg = arena.allocate(8);
            var posSeg = arena.allocate(8);

            TaoKey.encodeI64(negSeg, 0, -100L);
            TaoKey.encodeI64(zeroSeg, 0, 0L);
            TaoKey.encodeI64(posSeg, 0, 100L);

            // Negative should sort before zero, zero before positive
            assertTrue(compareBytes(negSeg, zeroSeg, 8) < 0);
            assertTrue(compareBytes(zeroSeg, posSeg, 8) < 0);
            assertTrue(compareBytes(negSeg, posSeg, 8) < 0);

            // Decode round-trip
            assertEquals(-100L, TaoKey.decodeI64(negSeg, 0));
            assertEquals(0L, TaoKey.decodeI64(zeroSeg, 0));
            assertEquals(100L, TaoKey.decodeI64(posSeg, 0));
        }
    }

    @Test
    void signedI32Ordering() {
        try (var arena = Arena.ofConfined()) {
            var neg = arena.allocate(4);
            var pos = arena.allocate(4);

            TaoKey.encodeI32(neg, 0, Integer.MIN_VALUE);
            TaoKey.encodeI32(pos, 0, Integer.MAX_VALUE);

            assertTrue(compareBytes(neg, pos, 4) < 0);
            assertEquals(Integer.MIN_VALUE, TaoKey.decodeI32(neg, 0));
            assertEquals(Integer.MAX_VALUE, TaoKey.decodeI32(pos, 0));
        }
    }

    @Test
    void u16Ordering() {
        try (var arena = Arena.ofConfined()) {
            var s1 = arena.allocate(2);
            var s2 = arena.allocate(2);

            TaoKey.encodeU16(s1, 0, (short) 1);
            TaoKey.encodeU16(s2, 0, (short) 256);

            assertTrue(compareBytes(s1, s2, 2) < 0);
        }
    }

    @Test
    void encodeStringBasic() {
        byte[] encoded = TaoKey.encodeString("hello");
        assertEquals('h', encoded[0]);
        assertEquals('e', encoded[1]);
        assertEquals('l', encoded[2]);
        assertEquals('l', encoded[3]);
        assertEquals('o', encoded[4]);
        assertEquals(0x00, encoded[5]); // null terminator
        assertEquals(6, encoded.length);
    }

    @Test
    void encodeStringEmpty() {
        byte[] encoded = TaoKey.encodeString("");
        assertEquals(1, encoded.length);
        assertEquals(0x00, encoded[0]); // just null terminator
    }

    @Test
    void encodeStringEscapes() {
        // String containing 0x00 and 0x01 bytes
        byte[] raw = {0x41, 0x00, 0x42, 0x01, 0x43};
        byte[] encoded = TaoKey.encodeStringBytes(raw);
        // Expected: 0x41, 0x01, 0x00, 0x42, 0x01, 0x01, 0x43, 0x00
        assertEquals(8, encoded.length);
        assertEquals(0x41, encoded[0]);
        assertEquals(0x01, encoded[1]); // escape
        assertEquals(0x00, encoded[2]); // escaped 0x00
        assertEquals(0x42, encoded[3]);
        assertEquals(0x01, encoded[4]); // escape
        assertEquals(0x01, encoded[5]); // escaped 0x01
        assertEquals(0x43, encoded[6]);
        assertEquals(0x00, encoded[7]); // null terminator
    }

    @Test
    void encodeStringIntoWritesEscapedBytesIntoProvidedBuffer() {
        byte[] buffer = new byte[8];
        int len = TaoKey.encodeStringInto("\u0000\u0001A", buffer);

        assertEquals(6, len);
        assertArrayEquals(new byte[]{0x01, 0x00, 0x01, 0x01, 0x41, 0x00, 0x00, 0x00}, buffer);
    }

    @Test
    void encodeStringIntoReportsOverflow() {
        assertEquals(-1, TaoKey.encodeStringInto("abc", new byte[3]));
    }

    @Test
    void encodeStringOrdering() {
        byte[] abc = TaoKey.encodeString("abc");
        byte[] abd = TaoKey.encodeString("abd");
        byte[] abcd = TaoKey.encodeString("abcd");

        assertTrue(compareBytesArr(abc, abd) < 0);
        assertTrue(compareBytesArr(abc, abcd) < 0);
        assertTrue(compareBytesArr(abd, abcd) > 0); // 'd' > 'c' + terminator
    }

    @Test
    void decodeStringRoundTripEmpty() {
        byte[] encoded = TaoKey.encodeString("");
        assertEquals("", TaoKey.decodeString(encoded));
    }

    @Test
    void decodeStringRoundTripBasic() {
        byte[] encoded = TaoKey.encodeString("hello");
        assertEquals("hello", TaoKey.decodeString(encoded));
    }

    @Test
    void decodeStringRoundTripNullByte() {
        // A string containing an actual NUL byte
        String s = "\u0000";
        byte[] encoded = TaoKey.encodeStringBytes(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(s, TaoKey.decodeString(encoded));
    }

    @Test
    void decodeStringRoundTripEscapedBytes() {
        // String containing 0x00 and 0x01 bytes interleaved with regular chars
        byte[] raw = {0x41, 0x00, 0x42, 0x01, 0x43};
        byte[] encoded = TaoKey.encodeStringBytes(raw);
        String decoded = TaoKey.decodeString(encoded);
        byte[] decodedBytes = decoded.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
        assertArrayEquals(raw, decodedBytes);
    }

    @Test
    void decodeStringRoundTripAllAscii() {
        String s = "abc\u0000def\u0001ghi";
        byte[] raw = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] encoded = TaoKey.encodeStringBytes(raw);
        assertEquals(s, TaoKey.decodeString(encoded));
    }

    // -- helpers --

    private static int compareBytes(MemorySegment a, MemorySegment b, int len) {
        for (int i = 0; i < len; i++) {
            int cmp = Byte.toUnsignedInt(a.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i))
                    - Byte.toUnsignedInt(b.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i));
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    private static int compareBytesArr(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    // ---- Round 2: I8 encoding ----

    @Test
    void encodeDecodeI8() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(1);
            TaoKey.encodeI8(seg, 0, (byte) -42);
            // Verify sign-bit flip: -42 ^ 0x80 = 86
            assertEquals((byte) 86, seg.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0));
        }
    }

    @Test
    void encodeDecodeI16() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(2);
            TaoKey.encodeI16(seg, 0, (short) -100);
            // XOR with Short.MIN_VALUE flips the sign bit: -100 ^ 0x8000 = 0x7F9C = 32668
            short raw = seg.get(java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(
                java.nio.ByteOrder.BIG_ENDIAN), 0);
            assertEquals((short) (-100 ^ Short.MIN_VALUE), raw,
                "encodeI16 must XOR with Short.MIN_VALUE");
        }
    }

    @Test
    void encodeI16OrderPreserving() {
        try (var arena = Arena.ofConfined()) {
            var neg = arena.allocate(2);
            var pos = arena.allocate(2);
            TaoKey.encodeI16(neg, 0, (short) -1);
            TaoKey.encodeI16(pos, 0, (short) 1);
            // Negative must sort before positive in unsigned byte comparison
            int b0neg = Byte.toUnsignedInt(neg.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0));
            int b0pos = Byte.toUnsignedInt(pos.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0));
            assertTrue(b0neg < b0pos,
                "Encoded -1 (first byte=" + b0neg + ") must sort before +1 (first byte=" + b0pos + ")");
        }
    }

    @Test
    void decodeU8RoundTrip() {
        try (var arena = Arena.ofConfined()) {
            var seg = arena.allocate(1);
            TaoKey.encodeU8(seg, 0, (byte) 0xAB);
            assertEquals((byte) 0xAB, TaoKey.decodeU8(seg, 0));
        }
    }
}
