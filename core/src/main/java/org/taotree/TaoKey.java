package org.taotree;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Binary-comparable key encoding utilities.
 *
 * <p>All transformations produce byte sequences where lexicographic comparison of the
 * encoded bytes yields the same ordering as the logical comparison of the original values.
 * This is required for correct ART traversal.
 *
 * <p>Follows the ART paper (Section IV) and DuckDB's {@code Radix} class.
 */
public final class TaoKey {

    private TaoKey() {}

    private static final ValueLayout.OfShort BE_SHORT =
        ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfInt BE_INT =
        ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfLong BE_LONG =
        ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    // -- Unsigned integers: big-endian --

    public static void encodeU8(MemorySegment seg, long offset, byte value) {
        seg.set(ValueLayout.JAVA_BYTE, offset, value);
    }

    public static void encodeU16(MemorySegment seg, long offset, short value) {
        seg.set(BE_SHORT, offset, value);
    }

    public static void encodeU32(MemorySegment seg, long offset, int value) {
        seg.set(BE_INT, offset, value);
    }

    public static void encodeU64(MemorySegment seg, long offset, long value) {
        seg.set(BE_LONG, offset, value);
    }

    // -- Signed integers: flip sign bit + big-endian --

    public static void encodeI8(MemorySegment seg, long offset, byte value) {
        seg.set(ValueLayout.JAVA_BYTE, offset, (byte) (value ^ 0x80));
    }

    public static void encodeI16(MemorySegment seg, long offset, short value) {
        seg.set(BE_SHORT, offset, (short) (value ^ Short.MIN_VALUE));
    }

    public static void encodeI32(MemorySegment seg, long offset, int value) {
        seg.set(BE_INT, offset, value ^ Integer.MIN_VALUE);
    }

    public static void encodeI64(MemorySegment seg, long offset, long value) {
        seg.set(BE_LONG, offset, value ^ Long.MIN_VALUE);
    }

    // -- Decode (reverse transformations) --

    public static byte decodeU8(MemorySegment seg, long offset) {
        return seg.get(ValueLayout.JAVA_BYTE, offset);
    }

    public static short decodeU16(MemorySegment seg, long offset) {
        return seg.get(BE_SHORT, offset);
    }

    public static int decodeU32(MemorySegment seg, long offset) {
        return seg.get(BE_INT, offset);
    }

    public static long decodeU64(MemorySegment seg, long offset) {
        return seg.get(BE_LONG, offset);
    }

    public static long decodeI64(MemorySegment seg, long offset) {
        return seg.get(BE_LONG, offset) ^ Long.MIN_VALUE;
    }

    public static int decodeI32(MemorySegment seg, long offset) {
        return seg.get(BE_INT, offset) ^ Integer.MIN_VALUE;
    }

    // -- String encoding: escape + null-terminate --

    /**
     * Encode a string as a binary-comparable key for use in dictionary ARTs.
     *
     * <p>Bytes ≤ 0x01 are escaped with a 0x01 prefix. A 0x00 null terminator is appended.
     * This ensures no encoded string is a prefix of another (required for ART lazy expansion).
     *
     * @param value the string to encode (UTF-8)
     * @return the encoded key bytes
     */
    public static byte[] encodeString(String value) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        return encodeStringBytes(raw);
    }

    /**
     * Encode raw UTF-8 bytes as a binary-comparable key.
     */
    public static byte[] encodeStringBytes(byte[] raw) {
        int escapeCount = 0;
        for (byte b : raw) {
            if ((b & 0xFF) <= 1) escapeCount++;
        }
        byte[] key = new byte[raw.length + escapeCount + 1];
        int pos = 0;
        for (byte b : raw) {
            if ((b & 0xFF) <= 1) key[pos++] = 0x01;
            key[pos++] = b;
        }
        key[pos] = 0x00; // null terminator
        return key;
    }

    static int encodeStringInto(String value, byte[] dst) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        int maxPayload = dst.length - 1;
        int pos = 0;
        for (byte b : raw) {
            int needed = ((b & 0xFF) <= 1) ? 2 : 1;
            if (pos + needed > maxPayload) {
                return -1;
            }
            if ((b & 0xFF) <= 1) {
                dst[pos++] = 0x01;
            }
            dst[pos++] = b;
        }
        if (pos >= dst.length) {
            return -1;
        }
        dst[pos++] = 0x00;
        return pos;
    }
}
