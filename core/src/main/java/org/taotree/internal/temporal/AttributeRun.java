package org.taotree.internal.temporal;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap accessor for the AttributeRun leaf value — a 24-byte struct recording
 * a contiguous time interval during which an attribute reported a specific value.
 *
 * <p>AttributeRuns are the <b>canonical write layer</b> (source of truth).
 * They are stored as ART leaf values keyed by {@code [attr_id:4B | first_seen:8B]}.
 *
 * <h3>ART key layout (12 bytes):</h3>
 * <pre>
 * Offset  Size  Field
 * ──────  ────  ─────────────────
 * 0       4     attr_id   (uint32, big-endian)
 * 4       8     first_seen (int64, big-endian, epoch ms)
 * </pre>
 *
 * <h3>Leaf value layout (24 bytes):</h3>
 * <pre>
 * Offset  Size  Field
 * ──────  ────  ──────────────────
 * 0       8     last_seen  (int64, epoch ms — mutable via COW)
 * 8       8     valid_to   (int64, epoch ms; Long.MAX_VALUE = open-ended)
 * 16      8     value_ref  (long, BumpAllocator pointer to value payload)
 * </pre>
 *
 * @see EntityNode
 */
public final class AttributeRun {

    private AttributeRun() {}

    // ── Key constants ────────────────────────────────────────────────────

    /** Total ART key length for AttributeRuns. */
    public static final int KEY_LEN = 12;

    /** Offset of attr_id within the key. */
    public static final int KEY_ATTR_ID_OFFSET = 0;

    /** Offset of first_seen within the key. */
    public static final int KEY_FIRST_SEEN_OFFSET = 4;

    // ── Leaf value constants ─────────────────────────────────────────────

    /** Total leaf value size. */
    public static final int VALUE_SIZE = 24;

    /**
     * Sentinel {@code value_ref} marking a tombstone run — a history entry
     * representing the deletion (retraction) of an attribute at its
     * {@code first_seen} timestamp. Readers must treat this as "no value at
     * this time"; CHAMP state updates must remove the attribute rather than
     * map it to this sentinel.
     *
     * <p>Not a valid BumpAllocator offset (negative), so it cannot collide
     * with a real {@code value_ref}.
     */
    public static final long TOMBSTONE_VALUE_REF = -1L;

    private static final int LAST_SEEN_OFFSET = 0;
    private static final int VALID_TO_OFFSET = 8;
    private static final int VALUE_REF_OFFSET = 16;

    private static final ValueLayout.OfLong LAYOUT = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt LAYOUT_INT_BE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(java.nio.ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfLong LAYOUT_LONG_BE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.BIG_ENDIAN);

    // ── Key encoding ─────────────────────────────────────────────────────

    /**
     * Encode an AttributeRuns ART key into the given segment.
     *
     * @param key       target segment (at least 12 bytes)
     * @param attrId    attribute dictionary code
     * @param firstSeen epoch milliseconds
     */
    public static void encodeKey(MemorySegment key, int attrId, long firstSeen) {
        key.set(LAYOUT_INT_BE, KEY_ATTR_ID_OFFSET, attrId);
        key.set(LAYOUT_LONG_BE, KEY_FIRST_SEEN_OFFSET, firstSeen);
    }

    /**
     * Encode a prefix key for scanning all runs of a given attribute.
     *
     * @param prefix target segment (at least 4 bytes)
     * @param attrId attribute dictionary code
     */
    public static void encodeAttrPrefix(MemorySegment prefix, int attrId) {
        prefix.set(LAYOUT_INT_BE, 0, attrId);
    }

    /** Extract attr_id from an encoded key. */
    public static int keyAttrId(MemorySegment key) {
        return key.get(LAYOUT_INT_BE, KEY_ATTR_ID_OFFSET);
    }

    /** Extract first_seen from an encoded key. */
    public static long keyFirstSeen(MemorySegment key) {
        return key.get(LAYOUT_LONG_BE, KEY_FIRST_SEEN_OFFSET);
    }

    // ── Leaf value read accessors ────────────────────────────────────────

    /** Read last_seen from a leaf value segment. */
    public static long lastSeen(MemorySegment value) {
        return value.get(LAYOUT, LAST_SEEN_OFFSET);
    }

    /** Read valid_to from a leaf value segment. */
    public static long validTo(MemorySegment value) {
        return value.get(LAYOUT, VALID_TO_OFFSET);
    }

    /** Read value_ref from a leaf value segment. */
    public static long valueRef(MemorySegment value) {
        return value.get(LAYOUT, VALUE_REF_OFFSET);
    }

    // ── Leaf value write accessors ───────────────────────────────────────

    public static void setLastSeen(MemorySegment value, long lastSeen) {
        value.set(LAYOUT, LAST_SEEN_OFFSET, lastSeen);
    }

    public static void setValidTo(MemorySegment value, long validTo) {
        value.set(LAYOUT, VALID_TO_OFFSET, validTo);
    }

    public static void setValueRef(MemorySegment value, long valueRef) {
        value.set(LAYOUT, VALUE_REF_OFFSET, valueRef);
    }

    /**
     * Write all leaf value fields at once.
     */
    public static void writeValue(MemorySegment value, long lastSeen, long validTo, long valueRef) {
        value.set(LAYOUT, LAST_SEEN_OFFSET, lastSeen);
        value.set(LAYOUT, VALID_TO_OFFSET, validTo);
        value.set(LAYOUT, VALUE_REF_OFFSET, valueRef);
    }
}
