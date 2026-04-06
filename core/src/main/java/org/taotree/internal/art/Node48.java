package org.taotree.internal.art;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Operations on Node48 — an inner ART node with up to 48 children.
 *
 * <p>Layout (648 bytes):
 * <pre>
 *  Offset  Size  Field
 *  0       1     count (u8)
 *  1       7     padding (alignment)
 *  8       256   childIndex[256] (key byte → position in children[], or EMPTY_MARKER)
 *  264     384   children[48] (48 × 8B NodePtr)
 * </pre>
 *
 * <p>Lookup is O(1): one read of {@code childIndex[key]} to get the slot, one read of
 * {@code children[slot]} to get the pointer.
 */
public final class Node48 {

    private Node48() {}

    public static final long OFF_COUNT      = 0;
    public static final long OFF_CHILD_IDX  = 8;
    public static final long OFF_CHILDREN   = 264;

    public static final int CAPACITY = NodeConstants.NODE48_CAPACITY; // 48
    public static final byte EMPTY_MARKER = 48; // impossible slot index (valid: 0-47)

    public static void init(MemorySegment seg) {
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) 0);
        // Fill childIndex with EMPTY_MARKER
        for (int i = 0; i < 256; i++) {
            seg.set(ValueLayout.JAVA_BYTE, OFF_CHILD_IDX + i, EMPTY_MARKER);
        }
        for (int i = 0; i < CAPACITY; i++) {
            seg.set(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) i * 8, NodePtr.EMPTY_PTR);
        }
    }

    public static int count(MemorySegment seg) {
        return Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, OFF_COUNT));
    }

    /** Get the child pointer for the given key byte, or EMPTY_PTR. */
    public static long findChild(MemorySegment seg, byte key) {
        int idx = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, OFF_CHILD_IDX + Byte.toUnsignedInt(key)));
        if (idx == Byte.toUnsignedInt(EMPTY_MARKER)) {
            return NodePtr.EMPTY_PTR;
        }
        return seg.get(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) idx * 8);
    }

    /** Get the child pointer at the given slot index (0-47). */
    public static long childAtSlot(MemorySegment seg, int slot) {
        return seg.get(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) slot * 8);
    }

    /** Set the child pointer at the given slot index (0-47). */
    public static void setChildAtSlot(MemorySegment seg, int slot, long childPtr) {
        seg.set(ValueLayout.JAVA_LONG, OFF_CHILDREN + (long) slot * 8, childPtr);
    }

    /**
     * Insert a child. Caller must verify count < 48 and key is absent.
     */
    public static void insertChild(MemorySegment seg, byte key, long childPtr) {
        int n = count(seg);
        // Find a free slot in the children array
        int freeSlot = findFreeSlot(seg);
        seg.set(ValueLayout.JAVA_BYTE, OFF_CHILD_IDX + Byte.toUnsignedInt(key), (byte) freeSlot);
        setChildAtSlot(seg, freeSlot, childPtr);
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) (n + 1));
    }

    /**
     * Remove the child at the given key byte.
     *
     * @return true if the key was found and removed
     */
    public static boolean removeChild(MemorySegment seg, byte key) {
        int keyIdx = Byte.toUnsignedInt(key);
        int slot = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, OFF_CHILD_IDX + keyIdx));
        if (slot == Byte.toUnsignedInt(EMPTY_MARKER)) return false;

        seg.set(ValueLayout.JAVA_BYTE, OFF_CHILD_IDX + keyIdx, EMPTY_MARKER);
        setChildAtSlot(seg, slot, NodePtr.EMPTY_PTR);
        int n = count(seg);
        seg.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) (n - 1));
        return true;
    }

    /** Update the child pointer for an existing key. */
    public static void updateChild(MemorySegment seg, byte key, long newChildPtr) {
        int slot = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, OFF_CHILD_IDX + Byte.toUnsignedInt(key)));
        setChildAtSlot(seg, slot, newChildPtr);
    }

    public static boolean isFull(MemorySegment seg) {
        return count(seg) >= CAPACITY;
    }

    /**
     * Copy all entries from a Node16 into this Node48. The Node48 must be freshly
     * initialized (count == 0). Used during Node16 → Node48 growth.
     */
    public static void copyFromNode16(MemorySegment dst, MemorySegment src) {
        int n = Node16.count(src);
        for (int i = 0; i < n; i++) {
            byte key = Node16.keyAt(src, i);
            long child = Node16.childAt(src, i);
            dst.set(ValueLayout.JAVA_BYTE, OFF_CHILD_IDX + Byte.toUnsignedInt(key), (byte) i);
            setChildAtSlot(dst, i, child);
        }
        dst.set(ValueLayout.JAVA_BYTE, OFF_COUNT, (byte) n);
    }

    /** Find the first free (EMPTY_PTR) slot in the children array. */
    private static int findFreeSlot(MemorySegment seg) {
        for (int i = 0; i < CAPACITY; i++) {
            if (NodePtr.isEmpty(childAtSlot(seg, i))) {
                return i;
            }
        }
        throw new IllegalStateException("Node48 has no free slot but count < 48");
    }

    /**
     * Iterate over all key-child pairs. Used by shrink and copy operations.
     * Callback receives (key byte, child NodePtr) for each present entry.
     */
    public static void forEach(MemorySegment seg, KeyChildConsumer consumer) {
        for (int k = 0; k < 256; k++) {
            int slot = Byte.toUnsignedInt(seg.get(ValueLayout.JAVA_BYTE, OFF_CHILD_IDX + k));
            if (slot != Byte.toUnsignedInt(EMPTY_MARKER)) {
                long child = childAtSlot(seg, slot);
                consumer.accept((byte) k, child);
            }
        }
    }

    @FunctionalInterface
    public interface KeyChildConsumer {
        void accept(byte key, long childPtr);
    }

    // -----------------------------------------------------------------------
    // COW (copy-on-write) operations
    // -----------------------------------------------------------------------

    /** Copy the source Node48 to dst, replacing one child pointer. */
    public static void cowReplaceChild(MemorySegment dst, MemorySegment src,
                                       byte keyByte, long newChild) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE48_SIZE);
        updateChild(dst, keyByte, newChild);
    }

    /** Copy the source Node48 to dst with an additional child inserted (source must not be full). */
    public static void cowInsertChild(MemorySegment dst, MemorySegment src,
                                      byte keyByte, long childPtr) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE48_SIZE);
        insertChild(dst, keyByte, childPtr);
    }

    /** Copy the source Node48 to dst with one child removed. */
    public static void cowRemoveChild(MemorySegment dst, MemorySegment src, byte keyByte) {
        MemorySegment.copy(src, 0, dst, 0, NodeConstants.NODE48_SIZE);
        removeChild(dst, keyByte);
    }

    /**
     * Grow a Node48 to a Node256, inserting a new child in the process.
     * The destination (Node256) must be freshly initialized.
     */
    public static void growToNode256(MemorySegment dst256, MemorySegment src48,
                                     byte newKey, long newChild) {
        Node256.copyFromNode48(dst256, src48);
        Node256.insertChild(dst256, newKey, newChild);
    }
}
