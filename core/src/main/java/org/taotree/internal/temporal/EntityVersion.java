package org.taotree.internal.temporal;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap accessor for the EntityVersion leaf value — a 16-byte struct recording
 * a point in time at which the aggregate entity state changed, together with a
 * CHAMP snapshot of the full state at that point.
 *
 * <p>EntityVersions are a <b>materialized read index</b> derived from AttributeRuns.
 * They are stored as ART leaf values keyed by {@code [first_seen:8B]}.
 *
 * <h3>ART key layout (8 bytes):</h3>
 * <pre>
 * Offset  Size  Field
 * ──────  ────  ─────────────────
 * 0       8     first_seen (int64, big-endian, epoch ms)
 * </pre>
 *
 * <h3>Leaf value layout (16 bytes):</h3>
 * <pre>
 * Offset  Size  Field
 * ──────  ────  ──────────────────
 * 0       8     valid_to        (int64, epoch ms; Long.MAX_VALUE = open-ended)
 * 8       8     state_root_ref  (long, CHAMP root pointer to full state snapshot)
 * </pre>
 *
 * @see EntityNode
 * @see org.taotree.internal.champ.ChampMap
 */
public final class EntityVersion {

    private EntityVersion() {}

    // ── Key constants ────────────────────────────────────────────────────

    /** Total ART key length for EntityVersions. */
    public static final int KEY_LEN = 8;

    // ── Leaf value constants ─────────────────────────────────────────────

    /** Total leaf value size. */
    public static final int VALUE_SIZE = 16;

    private static final int VALID_TO_OFFSET = 0;
    private static final int STATE_ROOT_OFFSET = 8;

    private static final ValueLayout.OfLong LAYOUT = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfLong LAYOUT_LONG_BE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.BIG_ENDIAN);

    // ── Key encoding ─────────────────────────────────────────────────────

    /**
     * Encode an EntityVersions ART key into the given segment.
     *
     * @param key       target segment (at least 8 bytes)
     * @param firstSeen epoch milliseconds
     */
    public static void encodeKey(MemorySegment key, long firstSeen) {
        key.set(LAYOUT_LONG_BE, 0, firstSeen);
    }

    /** Extract first_seen from an encoded key. */
    public static long keyFirstSeen(MemorySegment key) {
        return key.get(LAYOUT_LONG_BE, 0);
    }

    // ── Leaf value read accessors ────────────────────────────────────────

    /** Read valid_to from a leaf value segment. */
    public static long validTo(MemorySegment value) {
        return value.get(LAYOUT, VALID_TO_OFFSET);
    }

    /**
     * Read the CHAMP state root pointer from a leaf value segment.
     * <p>Returns {@link org.taotree.internal.champ.ChampMap#EMPTY_ROOT} (0) if
     * the version snapshot has been evicted to cold storage.
     */
    public static long stateRootRef(MemorySegment value) {
        return value.get(LAYOUT, STATE_ROOT_OFFSET);
    }

    // ── Leaf value write accessors ───────────────────────────────────────

    public static void setValidTo(MemorySegment value, long validTo) {
        value.set(LAYOUT, VALID_TO_OFFSET, validTo);
    }

    public static void setStateRootRef(MemorySegment value, long stateRoot) {
        value.set(LAYOUT, STATE_ROOT_OFFSET, stateRoot);
    }

    /**
     * Write all leaf value fields at once.
     */
    public static void writeValue(MemorySegment value, long validTo, long stateRootRef) {
        value.set(LAYOUT, VALID_TO_OFFSET, validTo);
        value.set(LAYOUT, STATE_ROOT_OFFSET, stateRootRef);
    }
}
