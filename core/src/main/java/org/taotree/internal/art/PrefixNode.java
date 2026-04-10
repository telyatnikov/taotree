package org.taotree.internal.art;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.taotree.TaoTree;

/**
 * Operations on Prefix nodes — path compression for the TaoTree.
 *
 * <p>Layout (24 bytes):
 * <pre>
 *  Offset  Size  Field
 *  0       1     count (u8) — number of key bytes stored (0..15)
 *  1       15    keys[15] — compressed path bytes
 *  16      8     child (NodePtr) — single child pointer
 * </pre>
 *
 * <p>A prefix node stores up to {@link NodeConstants#PREFIX_CAPACITY} (15) bytes of compressed
 * path. If the path is longer, multiple prefix nodes are chained.
 *
 * <p>During lookup, each byte of the prefix is compared against the search key.
 * On mismatch, the lookup fails. On full match, traversal continues to the child.
 */
public final class PrefixNode {

    private PrefixNode() {}

    public static final long OFF_COUNT = 0;
    public static final long OFF_KEYS  = 1;
    public static final long OFF_CHILD = 16;

    /**
     * Initialize a prefix node with the given key bytes and child pointer.
     *
     * @param seg    the prefix node's memory segment
     * @param key    the source key bytes
     * @param offset starting offset in the key
     * @param length number of prefix bytes to store (must be ≤ PREFIX_CAPACITY)
     * @param child  the child pointer
     */
    public static void init(MemorySegment seg, byte[] key, int offset, int length, long child) {
        if (length > NodeConstants.PREFIX_CAPACITY) throwPrefixTooLong(length);
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) length);
        for (int i = 0; i < length; i++) {
            seg.set(ValueLayout.JAVA_BYTE, OFF_KEYS + i, key[offset + i]);
        }
        seg.set(ValueLayout.JAVA_LONG, OFF_CHILD, child);
    }

    /**
     * Initialize from a MemorySegment key source.
     */
    public static void init(MemorySegment seg, MemorySegment key, int offset, int length, long child) {
        if (length > NodeConstants.PREFIX_CAPACITY) throwPrefixTooLong(length);
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) length);
        for (int i = 0; i < length; i++) {
            seg.set(ValueLayout.JAVA_BYTE, OFF_KEYS + i, key.get(ValueLayout.JAVA_BYTE, offset + i));
        }
        seg.set(ValueLayout.JAVA_LONG, OFF_CHILD, child);
    }

    /** Cold-path helper — extracted so both {@code init} overloads stay small for JIT inlining. */
    private static void throwPrefixTooLong(int length) {
        throw new IllegalArgumentException(
            "Prefix length " + length + " > " + NodeConstants.PREFIX_CAPACITY);
    }

    /** Get the number of prefix bytes. */
    public static int count(MemorySegment seg) {
        return count(seg, 0);
    }

    public static int count(MemorySegment seg, long baseOffset) {
        return Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, baseOffset + OFF_COUNT));
    }

    /** Get the prefix byte at the given position. */
    public static byte keyAt(MemorySegment seg, int pos) {
        return keyAt(seg, 0, pos);
    }

    public static byte keyAt(MemorySegment seg, long baseOffset, int pos) {
        return seg.get(ValueLayout.JAVA_BYTE, baseOffset + OFF_KEYS + pos);
    }

    /** Get the child pointer. */
    public static long child(MemorySegment seg) {
        return child(seg, 0);
    }

    public static long child(MemorySegment seg, long baseOffset) {
        return seg.get(ValueLayout.JAVA_LONG, baseOffset + OFF_CHILD);
    }

    /** Set the child pointer. */
    public static void setChild(MemorySegment seg, long child) {
        setChild(seg, 0, child);
    }

    public static void setChild(MemorySegment seg, long baseOffset, long child) {
        seg.set(ValueLayout.JAVA_LONG, baseOffset + OFF_CHILD, child);
    }

    /**
     * Check how many prefix bytes match the given key starting at {@code depth}.
     *
     * @param seg      the prefix node
     * @param key      the search key
     * @param keyLen   total length of the search key
     * @param depth    current depth in the key
     * @return the number of matching bytes (0 to count). If equal to count, it's a full match.
     */
    public static int matchKey(MemorySegment seg, MemorySegment key, int keyLen, int depth) {
        return matchKey(seg, 0, key, keyLen, depth);
    }

    public static int matchKey(MemorySegment seg, long baseOffset, MemorySegment key, int keyLen, int depth) {
        int prefixLen = count(seg, baseOffset);
        int maxMatch = Math.min(prefixLen, keyLen - depth);
        if (maxMatch <= 0) return 0;
        for (int i = 0; i < maxMatch; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, baseOffset + OFF_KEYS + i)
                    != key.get(ValueLayout.JAVA_BYTE, depth + i)) {
                return i;
            }
        }
        return maxMatch;
    }

    /**
     * Same as {@link #matchKey} but with a byte array key.
     */
    public static int matchKey(MemorySegment seg, byte[] key, int keyLen, int depth) {
        return matchKey(seg, 0, key, keyLen, depth);
    }

    public static int matchKey(MemorySegment seg, long baseOffset, byte[] key, int keyLen, int depth) {
        int prefixLen = count(seg, baseOffset);
        int maxMatch = Math.min(prefixLen, keyLen - depth);
        if (maxMatch <= 0) return 0;
        for (int i = 0; i < maxMatch; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, baseOffset + OFF_KEYS + i) != key[depth + i]) {
                return i;
            }
        }
        return maxMatch;
    }

    // -----------------------------------------------------------------------
    // COW (copy-on-write) operations
    // -----------------------------------------------------------------------

    /** Copy the source prefix node to dst with a different child pointer. */
    public static void cowWithChild(MemorySegment dst, MemorySegment src, long newChild) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.PREFIX_SIZE);
        dst.set(ValueLayout.JAVA_LONG, OFF_CHILD, newChild);
    }
}
