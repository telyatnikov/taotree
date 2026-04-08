package org.taotree.internal.art;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Operations on Node16 — an inner ART node with up to 16 children.
 *
 * <p>Layout (152 bytes):
 * <pre>
 *  Offset  Size  Field
 *  0       1     count (u8)
 *  1       7     padding (alignment)
 *  8       16    keys[16] (sorted byte values)
 *  24      128   children[16] (16 × 8B NodePtr)
 * </pre>
 *
 * <p>Keys are maintained in sorted order. Lookup is a linear scan over
 * 16 bytes (fits one cache line; JIT may auto-vectorize).
 */
public final class Node16 {

    private Node16() {}

    public static final long OFF_COUNT    = 0;
    public static final long OFF_KEYS     = 8;
    public static final long OFF_CHILDREN = 24;

    public static final int CAPACITY = NodeConstants.NODE16_CAPACITY; // 16

    public static void init(MemorySegment seg) {
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) 0);
        for (int i = 0; i < CAPACITY; i++) {
            seg.set(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) i * 8, NodePtr.EMPTY_PTR);
        }
    }

    public static int count(MemorySegment seg) {
        return count(seg, 0);
    }

    public static int count(MemorySegment seg, long baseOffset) {
        return Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, baseOffset + OFF_COUNT));
    }

    public static byte keyAt(MemorySegment seg, int pos) {
        return keyAt(seg, 0, pos);
    }

    public static byte keyAt(MemorySegment seg, long baseOffset, int pos) {
        return seg.get(ValueLayout.JAVA_BYTE, baseOffset + OFF_KEYS + pos);
    }

    public static long childAt(MemorySegment seg, int pos) {
        return childAt(seg, 0, pos);
    }

    public static long childAt(MemorySegment seg, long baseOffset, int pos) {
        return seg.get(ValueLayout.JAVA_LONG, baseOffset + OFF_CHILDREN + (long) pos * 8);
    }

    public static void setChildAt(MemorySegment seg, int pos, long childPtr) {
        setChildAt(seg, 0, pos, childPtr);
    }

    public static void setChildAt(MemorySegment seg, long baseOffset, int pos, long childPtr) {
        seg.set(ValueLayout.JAVA_LONG, baseOffset + OFF_CHILDREN + (long) pos * 8, childPtr);
    }

    public static long findChild(MemorySegment seg, byte key) {
        return findChild(seg, 0, key);
    }

    public static long findChild(MemorySegment seg, long baseOffset, byte key) {
        int n = count(seg, baseOffset);
        long keysOffset = baseOffset + OFF_KEYS;
        long childrenOffset = baseOffset + OFF_CHILDREN;
        for (int i = 0; i < n; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, keysOffset + i) == key) {
                return seg.get(ValueLayout.JAVA_LONG, childrenOffset + (long) i * 8);
            }
        }
        return NodePtr.EMPTY_PTR;
    }

    public static int findPos(MemorySegment seg, byte key) {
        return findPos(seg, 0, key);
    }

    public static int findPos(MemorySegment seg, long baseOffset, byte key) {
        int n = count(seg, baseOffset);
        long keysOffset = baseOffset + OFF_KEYS;
        for (int i = 0; i < n; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, keysOffset + i) == key) {
                return i;
            }
        }
        return -1;
    }

    public static void insertChild(MemorySegment seg, byte key, long childPtr) {
        int n = count(seg);
        int keyUnsigned = Byte.toUnsignedInt(key);
        int pos = 0;
        while (pos < n && Byte.toUnsignedInt(keyAt(seg, pos)) < keyUnsigned) {
            pos++;
        }
        for (int i = n; i > pos; i--) {
            seg.set(ValueLayout.JAVA_BYTE, OFF_KEYS + i, keyAt(seg, i - 1));
            setChildAt(seg, i, childAt(seg, i - 1));
        }
        seg.set(ValueLayout.JAVA_BYTE, OFF_KEYS + pos, key);
        setChildAt(seg, pos, childPtr);
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) (n + 1));
    }

    public static boolean removeChild(MemorySegment seg, byte key) {
        int pos = findPos(seg, key);
        if (pos < 0) return false;
        int n = count(seg);
        for (int i = pos; i < n - 1; i++) {
            seg.set(ValueLayout.JAVA_BYTE, OFF_KEYS + i, keyAt(seg, i + 1));
            setChildAt(seg, i, childAt(seg, i + 1));
        }
        setChildAt(seg, n - 1, NodePtr.EMPTY_PTR);
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) (n - 1));
        return true;
    }

    public static boolean isFull(MemorySegment seg) {
        return count(seg) >= CAPACITY;
    }

    /**
     * Copy all entries from a Node4 into this Node16. The Node16 must be freshly
     * initialized (count == 0). Used during Node4 → Node16 growth.
     */
    public static void copyFromNode4(MemorySegment dst, MemorySegment src) {
        int n = Node4.count(src);
        for (int i = 0; i < n; i++) {
            dst.set(ValueLayout.JAVA_BYTE, OFF_KEYS + i, Node4.keyAt(src, i));
            setChildAt(dst, i, Node4.childAt(src, i));
        }
        dst.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) n);
    }

    // -----------------------------------------------------------------------
    // COW (copy-on-write) operations
    // -----------------------------------------------------------------------

    /** Copy the source Node16 to dst, replacing one child pointer. */
    public static void cowReplaceChild(MemorySegment dst, MemorySegment src,
                                       byte keyByte, long newChild) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE16_SIZE);
        int pos = findPos(dst, keyByte);
        if (pos >= 0) setChildAt(dst, pos, newChild);
    }

    /** Copy the source Node16 to dst with an additional child inserted (source must not be full). */
    public static void cowInsertChild(MemorySegment dst, MemorySegment src,
                                      byte keyByte, long childPtr) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE16_SIZE);
        insertChild(dst, keyByte, childPtr);
    }

    /** Copy the source Node16 to dst with one child removed. */
    public static void cowRemoveChild(MemorySegment dst, MemorySegment src, byte keyByte) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE16_SIZE);
        removeChild(dst, keyByte);
    }

    /**
     * Grow a Node16 to a Node48, inserting a new child in the process.
     * The destination (Node48) must be freshly initialized.
     */
    public static void growToNode48(MemorySegment dst48, MemorySegment src16,
                                    byte newKey, long newChild) {
        Node48.copyFromNode16(dst48, src16);
        Node48.insertChild(dst48, newKey, newChild);
    }
}
