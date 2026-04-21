package org.taotree;

import org.taotree.internal.alloc.BumpAllocator;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * 16-byte off-heap string representation inspired by CedarDB/Umbra "German Strings".
 *
 * <p>Dual representation:
 * <ul>
 *   <li>Short (≤ 12 bytes): {@code [len:4B][data:12B]} — fully inline, zero-padded</li>
 *   <li>Long (&gt; 12 bytes): {@code [len:4B][prefix:4B][overflowPtr:8B]} — prefix +
 *       pointer to bump allocator overflow storage</li>
 * </ul>
 *
 * <p>All methods are static. The caller provides a 16-byte {@link MemorySegment} slice
 * (typically the leaf value portion from a {@link TaoTree}) and the owning tree
 * (which manages the overflow allocator internally).
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * try (var w = tree.write()) {
 *     long leaf = w.getOrCreate(key, 0);
 *     MemorySegment value = w.leafValue(leaf);
 *     TaoString.write(value, "Haliaeetus leucocephalus", tree);
 * }
 * try (var r = tree.read()) {
 *     long leaf = r.lookup(key);
 *     String name = TaoString.read(r.leafValue(leaf), tree);
 * }
 * }</pre>
 */
final class TaoString {

    private TaoString() {}

    /** Size of a TaoString slot in bytes. Use this as the leaf value size. */
    public static final int SIZE = 16;

    /** Maximum length for inline (short) strings. */
    public static final int SHORT_THRESHOLD = 12;

    private static final long OFF_LEN     = 0;
    private static final long OFF_PAYLOAD = 4;
    private static final long OFF_PREFIX  = 4;
    private static final long OFF_OVFL    = 8;

    // -- Write --

    /**
     * Write a string into a TaoString leaf slot.
     *
     * @param leaf 16-byte leaf segment
     * @param utf8 the string bytes (UTF-8)
     * @param tree the owning tree (provides overflow storage for strings &gt; 12 bytes)
     */
    public static void write(MemorySegment leaf, byte[] utf8, TaoTree tree) {
        BumpAllocator bump = tree.bump();
        leaf.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_LEN, utf8.length);

        if (utf8.length <= SHORT_THRESHOLD) {
            MemorySegment.copy(utf8, 0, leaf, ValueLayout.JAVA_BYTE, OFF_PAYLOAD, utf8.length);
            for (int i = utf8.length; i < 12; i++) {
                leaf.set(ValueLayout.JAVA_BYTE, OFF_PAYLOAD + i, (byte) 0);
            }
        } else {
            MemorySegment.copy(utf8, 0, leaf, ValueLayout.JAVA_BYTE, OFF_PREFIX, 4);
            long overflowRef = bump.allocate(utf8.length);
            MemorySegment dest = bump.resolve(overflowRef, utf8.length);
            MemorySegment.copy(utf8, 0, dest, ValueLayout.JAVA_BYTE, 0, utf8.length);
            leaf.set(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL, overflowRef);
        }
    }

    /**
     * Write a string into a TaoString leaf slot.
     *
     * @param leaf  16-byte leaf segment
     * @param value the string value
     * @param tree  the owning tree
     */
    public static void write(MemorySegment leaf, String value, TaoTree tree) {
        write(leaf, value.getBytes(StandardCharsets.UTF_8), tree);
    }

    // -- Read --

    /**
     * Read the raw bytes from a TaoString leaf slot.
     */
    public static byte[] readBytes(MemorySegment leaf, TaoTree tree) {
        BumpAllocator bump = tree.bump();
        int len = leaf.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_LEN);
        byte[] result = new byte[len];

        if (len <= SHORT_THRESHOLD) {
            MemorySegment.copy(leaf, ValueLayout.JAVA_BYTE, OFF_PAYLOAD, result, 0, len);
        } else {
            long overflowRef = leaf.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL);
            MemorySegment payload = bump.resolve(overflowRef, len);
            MemorySegment.copy(payload, ValueLayout.JAVA_BYTE, 0, result, 0, len);
        }
        return result;
    }

    /**
     * Read a TaoString leaf slot as a UTF-8 {@link String}.
     */
    public static String read(MemorySegment leaf, TaoTree tree) {
        return new String(readBytes(leaf, tree), StandardCharsets.UTF_8);
    }

    // -- Equality --

    /**
     * Fast equality comparison between two TaoString leaf slots.
     * Rejects most mismatches by comparing (len + prefix/inline) as a single long.
     */
    public static boolean equals(MemorySegment a, MemorySegment b, TaoTree tree) {
        if (a.get(ValueLayout.JAVA_LONG_UNALIGNED, 0) != b.get(ValueLayout.JAVA_LONG_UNALIGNED, 0)) {
            return false;
        }

        BumpAllocator bump = tree.bump();
        int len = a.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_LEN);
        if (len <= SHORT_THRESHOLD) {
            return a.get(ValueLayout.JAVA_INT_UNALIGNED, 8) == b.get(ValueLayout.JAVA_INT_UNALIGNED, 8);
        }

        MemorySegment pa = bump.resolve(a.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL), len);
        MemorySegment pb = bump.resolve(b.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL), len);
        return pa.asSlice(0, len).mismatch(pb.asSlice(0, len)) == -1;
    }

    /**
     * Compare a TaoString leaf slot against a raw byte array.
     */
    public static boolean equalsBytes(MemorySegment leaf, byte[] utf8, TaoTree tree) {
        int len = leaf.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_LEN);
        if (len != utf8.length) return false;

        if (len <= SHORT_THRESHOLD) {
            for (int i = 0; i < len; i++) {
                if (leaf.get(ValueLayout.JAVA_BYTE, OFF_PAYLOAD + i) != utf8[i]) return false;
            }
            return true;
        }

        for (int i = 0; i < 4; i++) {
            if (leaf.get(ValueLayout.JAVA_BYTE, OFF_PREFIX + i) != utf8[i]) return false;
        }

        BumpAllocator bump = tree.bump();
        long overflowRef = leaf.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_OVFL);
        MemorySegment payload = bump.resolve(overflowRef, len);
        return MemorySegment.ofArray(utf8).mismatch(payload.asSlice(0, len)) == -1;
    }

    // -- Helpers --

    /** Returns true if the string is stored inline (≤ 12 bytes). */
    public static boolean isShort(MemorySegment leaf) {
        return leaf.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_LEN) <= SHORT_THRESHOLD;
    }

    /** Returns the byte length of the stored string. */
    public static int length(MemorySegment leaf) {
        return leaf.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_LEN);
    }
}
