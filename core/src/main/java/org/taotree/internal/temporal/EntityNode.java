package org.taotree.internal.temporal;

import org.taotree.internal.art.NodePtr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap accessor for the 24-byte EntityNode struct — the aggregate root
 * for each tracked entity in the temporal store.
 *
 * <p>An EntityNode is stored inline as the leaf value in the global ART.
 * It contains root pointers for all per-entity data structures.
 *
 * <h3>Layout (24 bytes, fixed-size slab slot):</h3>
 * <pre>
 * Offset  Size  Field
 * ──────  ────  ─────────────────────────
 * 0       8     current_state_root_ref   (CHAMP root: latest full entity state)
 * 8       8     attr_art_root_ref        (AttributeRuns ART root pointer)
 * 16      8     versions_art_root_ref    (EntityVersions ART root pointer)
 * </pre>
 *
 * <p>All methods are static, operating on a {@link MemorySegment} resolved from
 * a slab pointer. Fields use {@link ValueLayout#JAVA_LONG} (8-byte aligned within
 * the 24-byte slab slot).
 *
 * @see org.taotree.internal.champ.ChampMap
 */
public final class EntityNode {

    private EntityNode() {} // static utility class

    // ── Layout constants ─────────────────────────────────────────────────

    /** Total size of an EntityNode struct in bytes. */
    public static final int SIZE = 24;

    /** Offset of {@code current_state_root_ref} (CHAMP root pointer). */
    public static final int CURRENT_STATE_ROOT_OFFSET = 0;

    /** Offset of {@code attr_art_root_ref} (AttributeRuns ART root). */
    public static final int ATTR_ART_ROOT_OFFSET = 8;

    /** Offset of {@code versions_art_root_ref} (EntityVersions ART root). */
    public static final int VERSIONS_ART_ROOT_OFFSET = 16;

    private static final ValueLayout.OfLong LAYOUT = ValueLayout.JAVA_LONG;

    // ── Read accessors ───────────────────────────────────────────────────

    /**
     * Read the CHAMP root pointer for the current (latest) entity state.
     * <p>Returns {@link org.taotree.internal.champ.ChampMap#EMPTY_ROOT} (0) if
     * no attributes have been recorded yet.
     */
    public static long currentStateRoot(MemorySegment node) {
        return node.get(LAYOUT, CURRENT_STATE_ROOT_OFFSET);
    }

    /**
     * Read the AttributeRuns ART root pointer.
     * <p>Returns {@link NodePtr#EMPTY_PTR} (0) if no attribute data exists.
     */
    public static long attrArtRoot(MemorySegment node) {
        return node.get(LAYOUT, ATTR_ART_ROOT_OFFSET);
    }

    /**
     * Read the EntityVersions ART root pointer.
     * <p>Returns {@link NodePtr#EMPTY_PTR} (0) if no materialized versions exist.
     */
    public static long versionsArtRoot(MemorySegment node) {
        return node.get(LAYOUT, VERSIONS_ART_ROOT_OFFSET);
    }

    // ── Write accessors ──────────────────────────────────────────────────

    public static void setCurrentStateRoot(MemorySegment node, long champRoot) {
        node.set(LAYOUT, CURRENT_STATE_ROOT_OFFSET, champRoot);
    }

    public static void setAttrArtRoot(MemorySegment node, long artRoot) {
        node.set(LAYOUT, ATTR_ART_ROOT_OFFSET, artRoot);
    }

    public static void setVersionsArtRoot(MemorySegment node, long artRoot) {
        node.set(LAYOUT, VERSIONS_ART_ROOT_OFFSET, artRoot);
    }

    // ── Bulk operations ──────────────────────────────────────────────────

    /** Initialize all fields to zero (empty entity). */
    public static void clear(MemorySegment node) {
        node.fill((byte) 0);
    }

    /** Copy all fields from {@code src} to {@code dst}. */
    public static void copyTo(MemorySegment src, MemorySegment dst) {
        MemorySegment.copy(src, 0, dst, 0, SIZE);
    }

    /**
     * Write new CHAMP root and both ART roots into {@code dst}. Since the
     * 24-byte struct has no other fields, {@code src} is ignored but kept in
     * the signature so callers don't have to special-case the first write.
     */
    public static void copyWithRoots(MemorySegment src, MemorySegment dst,
                                     long newCurrentStateRoot,
                                     long newAttrArtRoot,
                                     long newVersionsArtRoot) {
        dst.set(LAYOUT, CURRENT_STATE_ROOT_OFFSET, newCurrentStateRoot);
        dst.set(LAYOUT, ATTR_ART_ROOT_OFFSET, newAttrArtRoot);
        dst.set(LAYOUT, VERSIONS_ART_ROOT_OFFSET, newVersionsArtRoot);
    }
}
