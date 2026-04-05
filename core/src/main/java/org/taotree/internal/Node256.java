package org.taotree.internal;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Operations on Node256 — an inner ART node with up to 256 children.
 *
 * <p>Layout (2056 bytes):
 * <pre>
 *  Offset  Size  Field
 *  0       2     count (u16)
 *  2       6     padding (alignment)
 *  8       2048  children[256] (256 × 8B NodePtr; indexed directly by key byte)
 * </pre>
 *
 * <p>Lookup is O(1): single array access {@code children[key]}.
 * Presence is checked with {@link NodePtr#isEmpty(long)}.
 */
public final class Node256 {

    private Node256() {}

    public static final long OFF_COUNT    = 0;
    public static final long OFF_CHILDREN = 8;

    public static final int CAPACITY = NodeConstants.NODE256_CAPACITY; // 256

    public static void init(MemorySegment seg) {
        seg.set(ValueLayout.JAVA_SHORT, OFF_COUNT, (short) 0);
        for (int i = 0; i < CAPACITY; i++) {
            seg.set(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) i * 8, NodePtr.EMPTY_PTR);
        }
    }

    public static int count(MemorySegment seg) {
        return Short.toUnsignedInt(seg.get(ValueLayout.JAVA_SHORT, OFF_COUNT));
    }

    /** Get the child pointer for the given key byte, or EMPTY_PTR. */
    public static long findChild(MemorySegment seg, byte key) {
        return seg.get(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) Byte.toUnsignedInt(key) * 8);
    }

    /** Set the child pointer for the given key byte. */
    public static void setChild(MemorySegment seg, byte key, long childPtr) {
        seg.set(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) Byte.toUnsignedInt(key) * 8, childPtr);
    }

    /**
     * Insert a child. Caller must verify key is currently absent (EMPTY_PTR).
     */
    public static void insertChild(MemorySegment seg, byte key, long childPtr) {
        setChild(seg, key, childPtr);
        int n = count(seg);
        seg.set(ValueLayout.JAVA_SHORT, OFF_COUNT, (short) (n + 1));
    }

    /**
     * Remove the child at the given key byte.
     *
     * @return true if the key was present and removed
     */
    public static boolean removeChild(MemorySegment seg, byte key) {
        long existing = findChild(seg, key);
        if (NodePtr.isEmpty(existing)) return false;
        setChild(seg, key, NodePtr.EMPTY_PTR);
        int n = count(seg);
        seg.set(ValueLayout.JAVA_SHORT, OFF_COUNT, (short) (n - 1));
        return true;
    }

    /** Update the child pointer for an existing key. */
    public static void updateChild(MemorySegment seg, byte key, long newChildPtr) {
        setChild(seg, key, newChildPtr);
    }

    /**
     * Copy all entries from a Node48 into this Node256. The Node256 must be freshly
     * initialized (count == 0). Used during Node48 → Node256 growth.
     */
    public static void copyFromNode48(MemorySegment dst, MemorySegment src) {
        int[] countHolder = {0};
        Node48.forEach(src, (key, child) -> {
            setChild(dst, key, child);
            countHolder[0]++;
        });
        dst.set(ValueLayout.JAVA_SHORT, OFF_COUNT, (short) countHolder[0]);
    }

    /**
     * Iterate over all key-child pairs.
     */
    public static void forEach(MemorySegment seg, Node48.KeyChildConsumer consumer) {
        for (int k = 0; k < 256; k++) {
            long child = seg.get(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) k * 8);
            if (!NodePtr.isEmpty(child)) {
                consumer.accept((byte) k, child);
            }
        }
    }

    // -----------------------------------------------------------------------
    // COW (copy-on-write) operations
    // -----------------------------------------------------------------------

    /** Copy the source Node256 to dst, replacing one child pointer. */
    public static void cowReplaceChild(MemorySegment dst, MemorySegment src,
                                       byte keyByte, long newChild) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE256_SIZE);
        setChild(dst, keyByte, newChild);
    }

    /** Copy the source Node256 to dst with an additional child inserted. */
    public static void cowInsertChild(MemorySegment dst, MemorySegment src,
                                      byte keyByte, long childPtr) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE256_SIZE);
        insertChild(dst, keyByte, childPtr);
    }

    /** Copy the source Node256 to dst with one child removed. */
    public static void cowRemoveChild(MemorySegment dst, MemorySegment src, byte keyByte) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE256_SIZE);
        removeChild(dst, keyByte);
    }

    /**
     * Shrink a Node256 to a Node48 (copy entries into a freshly initialized Node48).
     * Source must have count <= Node48.CAPACITY.
     */
    public static void shrinkToNode48(MemorySegment dst48, MemorySegment src256) {
        Node48.init(dst48);
        forEach(src256, (k, c) -> Node48.insertChild(dst48, k, c));
    }

    /**
     * Return the byte offset of the child pointer slot for the given key byte.
     * Used for per-subtree CAS: two writers targeting different key bytes
     * target different memory addresses.
     */
    public static long childOffset(byte keyByte) {
        return OFF_CHILDREN + (long) Byte.toUnsignedInt(keyByte) * 8;
    }
}
