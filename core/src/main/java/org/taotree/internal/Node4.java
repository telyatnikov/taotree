package org.taotree.internal;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Operations on Node4 — an inner ART node with up to 4 children.
 *
 * <p>Layout (40 bytes):
 * <pre>
 *  Offset  Size  Field
 *  0       1     count (u8)
 *  1       4     keys[4] (sorted byte values)
 *  5       3     padding (alignment)
 *  8       32    children[4] (4 × 8B NodePtr)
 * </pre>
 *
 * <p>Keys are maintained in sorted order for binary-comparable key correctness.
 * All methods are static and operate directly on a {@link MemorySegment} slice
 * obtained from the {@link SlabAllocator}.
 */
public final class Node4 {

    private Node4() {}

    // Layout offsets
    public static final long OFF_COUNT    = 0;
    public static final long OFF_KEYS     = 1;
    public static final long OFF_CHILDREN = 8;

    public static final int CAPACITY = NodeConstants.NODE4_CAPACITY; // 4

    /** Initialize a freshly allocated Node4 segment (zero count, empty children). */
    public static void init(MemorySegment seg) {
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) 0);
        for (int i = 0; i < CAPACITY; i++) {
            seg.set(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) i * 8, NodePtr.EMPTY_PTR);
        }
    }

    /** Get the number of children. */
    public static int count(MemorySegment seg) {
        return Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, OFF_COUNT));
    }

    /** Get the key byte at position {@code pos} (0-based, within count). */
    public static byte keyAt(MemorySegment seg, int pos) {
        return seg.get(ValueLayout.JAVA_BYTE, OFF_KEYS + pos);
    }

    /** Get the child pointer at position {@code pos}. */
    public static long childAt(MemorySegment seg, int pos) {
        return seg.get(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) pos * 8);
    }

    /** Set the child pointer at position {@code pos}. */
    public static void setChildAt(MemorySegment seg, int pos, long childPtr) {
        seg.set(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) pos * 8, childPtr);
    }

    /**
     * Find the child for the given key byte.
     *
     * @return the child {@link NodePtr}, or {@link NodePtr#EMPTY_PTR} if not found
     */
    public static long findChild(MemorySegment seg, byte key) {
        int n = count(seg);
        for (int i = 0; i < n; i++) {
            if (keyAt(seg, i) == key) {
                return childAt(seg, i);
            }
        }
        return NodePtr.EMPTY_PTR;
    }

    /**
     * Find the position of the given key byte.
     *
     * @return the position (0..count-1), or -1 if not found
     */
    public static int findPos(MemorySegment seg, byte key) {
        int n = count(seg);
        for (int i = 0; i < n; i++) {
            if (keyAt(seg, i) == key) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Insert a child at the correct sorted position. Does NOT check for duplicates
     * or capacity — caller must verify {@code count < 4} and key is absent.
     */
    public static void insertChild(MemorySegment seg, byte key, long childPtr) {
        int n = count(seg);
        // Find insertion point (maintain sorted order by unsigned byte value)
        int keyUnsigned = Byte.toUnsignedInt(key);
        int pos = 0;
        while (pos < n && Byte.toUnsignedInt(keyAt(seg, pos)) < keyUnsigned) {
            pos++;
        }
        // Shift right
        for (int i = n; i > pos; i--) {
            seg.set(ValueLayout.JAVA_BYTE, OFF_KEYS + i, keyAt(seg, i - 1));
            setChildAt(seg, i, childAt(seg, i - 1));
        }
        // Insert
        seg.set(ValueLayout.JAVA_BYTE, OFF_KEYS + pos, key);
        setChildAt(seg, pos, childPtr);
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) (n + 1));
    }

    /**
     * Remove the child at the given key byte. Compacts the arrays.
     *
     * @return true if the key was found and removed
     */
    public static boolean removeChild(MemorySegment seg, byte key) {
        int pos = findPos(seg, key);
        if (pos < 0) return false;
        int n = count(seg);
        // Shift left
        for (int i = pos; i < n - 1; i++) {
            seg.set(ValueLayout.JAVA_BYTE, OFF_KEYS + i, keyAt(seg, i + 1));
            setChildAt(seg, i, childAt(seg, i + 1));
        }
        // Clear last slot
        setChildAt(seg, n - 1, NodePtr.EMPTY_PTR);
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) (n - 1));
        return true;
    }

    /** Returns true if the node is full (count == 4). */
    public static boolean isFull(MemorySegment seg) {
        return count(seg) >= CAPACITY;
    }
}
