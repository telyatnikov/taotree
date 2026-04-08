package org.taotree.internal.art;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.taotree.internal.alloc.SlabAllocator;

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
        return count(seg, 0);
    }

    public static int count(MemorySegment seg, long baseOffset) {
        return Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, baseOffset + OFF_COUNT));
    }

    /** Get the key byte at position {@code pos} (0-based, within count). */
    public static byte keyAt(MemorySegment seg, int pos) {
        return keyAt(seg, 0, pos);
    }

    public static byte keyAt(MemorySegment seg, long baseOffset, int pos) {
        return seg.get(ValueLayout.JAVA_BYTE, baseOffset + OFF_KEYS + pos);
    }

    /** Get the child pointer at position {@code pos}. */
    public static long childAt(MemorySegment seg, int pos) {
        return childAt(seg, 0, pos);
    }

    public static long childAt(MemorySegment seg, long baseOffset, int pos) {
        return seg.get(ValueLayout.JAVA_LONG, baseOffset + OFF_CHILDREN + (long) pos * 8);
    }

    /** Set the child pointer at position {@code pos}. */
    public static void setChildAt(MemorySegment seg, int pos, long childPtr) {
        setChildAt(seg, 0, pos, childPtr);
    }

    public static void setChildAt(MemorySegment seg, long baseOffset, int pos, long childPtr) {
        seg.set(ValueLayout.JAVA_LONG, baseOffset + OFF_CHILDREN + (long) pos * 8, childPtr);
    }

    /**
     * Find the child for the given key byte.
     *
     * @return the child {@link NodePtr}, or {@link NodePtr#EMPTY_PTR} if not found
     */
    public static long findChild(MemorySegment seg, byte key) {
        return findChild(seg, 0, key);
    }

    public static long findChild(MemorySegment seg, long baseOffset, byte key) {
        int n = count(seg, baseOffset);
        long keysOffset = baseOffset + OFF_KEYS;
        long childrenOffset = baseOffset + OFF_CHILDREN;
        return switch (n) {
            case 4 -> seg.get(ValueLayout.JAVA_BYTE, keysOffset + 3) == key
                    ? seg.get(ValueLayout.JAVA_LONG, childrenOffset + 24)
                    : findChildUpTo(seg, keysOffset, childrenOffset, key, 3);
            case 3 -> findChildUpTo(seg, keysOffset, childrenOffset, key, 3);
            case 2 -> findChildUpTo(seg, keysOffset, childrenOffset, key, 2);
            case 1 -> seg.get(ValueLayout.JAVA_BYTE, keysOffset) == key
                    ? seg.get(ValueLayout.JAVA_LONG, childrenOffset)
                    : NodePtr.EMPTY_PTR;
            default -> NodePtr.EMPTY_PTR;
        };
    }

    /**
     * Find the position of the given key byte.
     *
     * @return the position (0..count-1), or -1 if not found
     */
    public static int findPos(MemorySegment seg, byte key) {
        return findPos(seg, 0, key);
    }

    public static int findPos(MemorySegment seg, long baseOffset, byte key) {
        int n = count(seg, baseOffset);
        long keysOffset = baseOffset + OFF_KEYS;
        return switch (n) {
            case 4 -> seg.get(ValueLayout.JAVA_BYTE, keysOffset + 3) == key ? 3 : findPosUpTo(seg, keysOffset, key, 3);
            case 3 -> findPosUpTo(seg, keysOffset, key, 3);
            case 2 -> findPosUpTo(seg, keysOffset, key, 2);
            case 1 -> seg.get(ValueLayout.JAVA_BYTE, keysOffset) == key ? 0 : -1;
            default -> -1;
        };
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

    // -----------------------------------------------------------------------
    // COW (copy-on-write) operations — used by the deferred-COW engine.
    // These methods create a modified copy of a node in a destination segment
    // without mutating the source. The caller is responsible for allocation.
    // -----------------------------------------------------------------------

    /**
     * Copy the source Node4 to dst, replacing one child pointer.
     * The destination must be pre-allocated with NODE4_SIZE bytes.
     */
    public static void cowReplaceChild(MemorySegment dst, MemorySegment src,
                                       byte keyByte, long newChild) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE4_SIZE);
        int pos = findPos(dst, keyByte);
        if (pos >= 0) setChildAt(dst, pos, newChild);
    }

    /**
     * Copy the source Node4 to dst with an additional child inserted.
     * The source must NOT be full (count < 4). The destination must be
     * pre-allocated with NODE4_SIZE bytes and freshly zeroed.
     */
    public static void cowInsertChild(MemorySegment dst, MemorySegment src,
                                      byte keyByte, long childPtr) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE4_SIZE);
        insertChild(dst, keyByte, childPtr);
    }

    /**
     * Copy the source Node4 to dst with one child removed.
     * The destination must be pre-allocated with NODE4_SIZE bytes.
     */
    public static void cowRemoveChild(MemorySegment dst, MemorySegment src, byte keyByte) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE4_SIZE);
        removeChild(dst, keyByte);
    }

    /**
     * Grow a Node4 to a Node16, inserting a new child in the process.
     * The destination (Node16) must be freshly initialized.
     */
    public static void growToNode16(MemorySegment dst16, MemorySegment src4,
                                    byte newKey, long newChild) {
        Node16.copyFromNode4(dst16, src4);
        Node16.insertChild(dst16, newKey, newChild);
    }

    private static long findChildUpTo(MemorySegment seg, long keysOffset, long childrenOffset, byte key, int limit) {
        for (int i = 0; i < limit; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, keysOffset + i) == key) {
                return seg.get(ValueLayout.JAVA_LONG, childrenOffset + (long) i * 8);
            }
        }
        return NodePtr.EMPTY_PTR;
    }

    private static int findPosUpTo(MemorySegment seg, long keysOffset, byte key, int limit) {
        for (int i = 0; i < limit; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, keysOffset + i) == key) {
                return i;
            }
        }
        return -1;
    }
}
