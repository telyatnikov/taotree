package org.taotree.internal.temporal;

import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.ArtSearch;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.champ.ChampMap;
import org.taotree.internal.champ.ChampVisitor;

import java.lang.foreign.MemorySegment;
import java.util.function.LongFunction;

/**
 * Temporal read path for per-entity queries.
 *
 * <p>Implements the read logic from §18.9 of the design:
 * <ul>
 *   <li>{@link #latest} — current attribute value via CHAMP state (§18.9.1)</li>
 *   <li>{@link #at} — attribute value at a specific time via AttributeRuns (§18.9.2)</li>
 *   <li>{@link #stateAt} — full CHAMP state root at a specific time via EntityVersions</li>
 *   <li>{@link #allFieldsAt} — all attribute values at a specific time (§18.9.3)</li>
 *   <li>{@link #history} — full attribute history in chronological order (§18.9.4)</li>
 *   <li>{@link #historyRange} — bounded attribute history (§18.9.5)</li>
 * </ul>
 *
 * <p>All operations are read-only and lock-free. The caller must be within a
 * ReadScope (or equivalent epoch guard) to prevent reclamation of accessed nodes.
 *
 * <p><b>Thread safety:</b> instances are thread-safe for concurrent reads.
 * No mutable state is accessed or modified.
 */
public final class TemporalReader {

    /** Sentinel returned when a value is not found. */
    public static final long NOT_FOUND = ChampMap.NOT_FOUND;

    // Leaf layouts: [key_slot | value] — key_slot is keyLen padded to 8-byte alignment
    static final int ATTR_KEY_SLOT = (AttributeRun.KEY_LEN + 7) & ~7; // 16
    static final int VERS_KEY_SLOT = (EntityVersion.KEY_LEN + 7) & ~7; // 8

    private final SlabAllocator slab;
    private final ChunkStore chunkStore;
    private final BumpAllocator champBump;
    private final LongFunction<MemorySegment> resolver;

    /**
     * Create a TemporalReader sharing infrastructure with the owning TaoTree.
     *
     * @param slab       shared slab allocator (for resolving ART node pointers)
     * @param chunkStore shared chunk store (for resolving arena pointers; may be null)
     * @param champBump  bump allocator for CHAMP node resolution
     */
    public TemporalReader(SlabAllocator slab, ChunkStore chunkStore,
                          BumpAllocator champBump) {
        this.slab = slab;
        this.chunkStore = chunkStore;
        this.champBump = champBump;
        this.resolver = this::resolveNode;
    }

    // ====================================================================
    // latest — current value (§18.9.1)
    // ====================================================================

    /**
     * Return the current value of an attribute.
     *
     * <p>O(log₃₂ S) where S = number of attributes. Uses the CHAMP state
     * directly — the fastest read path.
     *
     * @param entityNode resolved EntityNode segment (40 bytes)
     * @param attrId     attribute dictionary ID
     * @return value_ref, or {@link #NOT_FOUND} if the attribute is absent
     */
    public long latest(MemorySegment entityNode, int attrId) {
        long stateRoot = EntityNode.currentStateRoot(entityNode);
        if (stateRoot == ChampMap.EMPTY_ROOT) return NOT_FOUND;
        long ref = ChampMap.get(champBump, stateRoot, attrId);
        // Defensive: CHAMP should never contain a tombstone sentinel (the
        // writer calls ChampMap.remove for tombstones), but guard anyway.
        if (ref == AttributeRun.TOMBSTONE_VALUE_REF) return NOT_FOUND;
        return ref;
    }

    // ====================================================================
    // at — value at specific time (§18.9.2)
    // ====================================================================

    /**
     * Return the value of an attribute at a specific point in time.
     *
     * <p>Uses a predecessor search in the AttributeRuns ART — the most
     * efficient path for single-attribute point-in-time queries. O(12) in ART
     * depth, no CHAMP lookup needed.
     *
     * @param entityNode resolved EntityNode segment (40 bytes)
     * @param attrId     attribute dictionary ID
     * @param T          query timestamp (epoch ms, must be ≥ 0)
     * @return value_ref, or {@link #NOT_FOUND} if no value exists at time T
     */
    public long at(MemorySegment entityNode, int attrId, long T) {
        long attrRoot = EntityNode.attrArtRoot(entityNode);
        if (NodePtr.isEmpty(attrRoot)) return NOT_FOUND;

        byte[] keyBytes = new byte[AttributeRun.KEY_LEN];
        MemorySegment searchKey = MemorySegment.ofArray(keyBytes);
        AttributeRun.encodeKey(searchKey, attrId, T);

        long predLeaf = ArtSearch.predecessor(resolver, attrRoot,
                searchKey, AttributeRun.KEY_LEN);
        if (NodePtr.isEmpty(predLeaf)) return NOT_FOUND;

        MemorySegment full = resolver.apply(predLeaf);
        int predAttrId = AttributeRun.keyAttrId(full);
        if (predAttrId != attrId) return NOT_FOUND;

        MemorySegment value = full.asSlice(ATTR_KEY_SLOT);
        if (AttributeRun.validTo(value) < T) return NOT_FOUND; // gap

        long ref = AttributeRun.valueRef(value);
        // Tombstone run — the attribute was retracted at predFirstSeen and
        // remains absent until the run's valid_to (next event).
        if (ref == AttributeRun.TOMBSTONE_VALUE_REF) return NOT_FOUND;
        return ref;
    }

    // ====================================================================
    // stateAt — CHAMP root at specific time (§18.9.3)
    // ====================================================================

    /**
     * Return the CHAMP state root pointer at a specific point in time.
     *
     * <p>Uses a predecessor search in the EntityVersions ART. The returned
     * root can be queried with {@link ChampMap#get} or {@link ChampMap#iterate}.
     *
     * @param entityNode resolved EntityNode segment (40 bytes)
     * @param T          query timestamp (epoch ms, must be ≥ 0)
     * @return CHAMP root pointer, or {@link ChampMap#EMPTY_ROOT} if no state exists at T
     */
    public long stateAt(MemorySegment entityNode, long T) {
        long versRoot = EntityNode.versionsArtRoot(entityNode);
        if (NodePtr.isEmpty(versRoot)) return ChampMap.EMPTY_ROOT;

        byte[] keyBytes = new byte[EntityVersion.KEY_LEN];
        MemorySegment searchKey = MemorySegment.ofArray(keyBytes);
        EntityVersion.encodeKey(searchKey, T);

        long predLeaf = ArtSearch.predecessor(resolver, versRoot,
                searchKey, EntityVersion.KEY_LEN);
        if (NodePtr.isEmpty(predLeaf)) return ChampMap.EMPTY_ROOT;

        MemorySegment full = resolver.apply(predLeaf);
        MemorySegment value = full.asSlice(VERS_KEY_SLOT);
        if (EntityVersion.validTo(value) < T) return ChampMap.EMPTY_ROOT; // gap

        return EntityVersion.stateRootRef(value);
    }

    // ====================================================================
    // allFieldsAt — all attributes at specific time (§18.9.3)
    // ====================================================================

    /**
     * Visit all attribute values at a specific point in time.
     *
     * <p>Finds the EntityVersion at time T, then iterates its CHAMP state
     * snapshot. O(P + 8 + S) where S = number of attributes.
     *
     * @param entityNode resolved EntityNode segment (40 bytes)
     * @param T          query timestamp (epoch ms, must be ≥ 0)
     * @param visitor    called for each (attrId, valueRef) pair
     * @return {@code true} if iteration completed, {@code false} if stopped early
     */
    public boolean allFieldsAt(MemorySegment entityNode, long T,
                               ChampVisitor visitor) {
        long stateRoot = stateAt(entityNode, T);
        if (stateRoot == ChampMap.EMPTY_ROOT) return true;
        return ChampMap.iterate(champBump, stateRoot, visitor);
    }

    // ====================================================================
    // history — full attribute history (§18.9.4)
    // ====================================================================

    /**
     * Visit the full history of an attribute as a sequence of runs in
     * chronological order (ascending first_seen).
     *
     * <p>Performs a forward scan in the AttributeRuns ART over all entries
     * matching the given attribute ID. O(12 × R) where R = number of runs.
     *
     * @param entityNode resolved EntityNode segment (40 bytes)
     * @param attrId     attribute dictionary ID
     * @param visitor    called for each run; return {@code false} to stop early
     * @return {@code true} if iteration completed, {@code false} if stopped early
     */
    public boolean history(MemorySegment entityNode, int attrId,
                           HistoryVisitor visitor) {
        long attrRoot = EntityNode.attrArtRoot(entityNode);
        if (NodePtr.isEmpty(attrRoot)) return true;

        // Reusable search key buffer for successor calls.
        // ArtSearch.successor returns key ≥ searchKey (inclusive), so after
        // visiting a leaf at first_seen=X, we search for (attrId, X+1).
        byte[] searchBytes = new byte[AttributeRun.KEY_LEN];
        MemorySegment searchKey = MemorySegment.ofArray(searchBytes);

        // Start from (attrId, 0) — TIMELESS uses ts=0 and must be visible.
        AttributeRun.encodeKey(searchKey, attrId, 0);

        long leafPtr = ArtSearch.successor(resolver, attrRoot,
                searchKey, AttributeRun.KEY_LEN);

        while (!NodePtr.isEmpty(leafPtr)) {
            MemorySegment full = resolver.apply(leafPtr);
            int leafAttrId = AttributeRun.keyAttrId(full);
            if (leafAttrId != attrId) break; // past our attribute

            long firstSeen = AttributeRun.keyFirstSeen(full);
            MemorySegment value = full.asSlice(ATTR_KEY_SLOT);
            if (!visitor.visit(
                    firstSeen,
                    AttributeRun.lastSeen(value),
                    AttributeRun.validTo(value),
                    AttributeRun.valueRef(value))) {
                return false;
            }

            // Advance: search for the next key strictly after this one.
            // successor returns ≥, so we encode (attrId, firstSeen + 1).
            AttributeRun.encodeKey(searchKey, attrId, firstSeen + 1);
            leafPtr = ArtSearch.successor(resolver, attrRoot,
                    searchKey, AttributeRun.KEY_LEN);
        }

        return true;
    }

    // ====================================================================
    // historyRange — bounded attribute history (§18.9.5)
    // ====================================================================

    /**
     * Visit attribute history within a time range, in chronological order.
     *
     * <p>Includes any run whose validity window overlaps {@code [fromMs, toMs]}:
     * <ul>
     *   <li>Predecessor at fromMs — catches runs starting before the range
     *       but whose valid_to ≥ fromMs</li>
     *   <li>Forward scan from fromMs — catches runs starting within the range
     *       (first_seen ≤ toMs)</li>
     * </ul>
     *
     * @param entityNode resolved EntityNode segment (40 bytes)
     * @param attrId     attribute dictionary ID
     * @param fromMs     range start (inclusive, epoch ms)
     * @param toMs       range end (inclusive, epoch ms)
     * @param visitor    called for each run; return {@code false} to stop early
     * @return {@code true} if iteration completed, {@code false} if stopped early
     */
    public boolean historyRange(MemorySegment entityNode, int attrId,
                                long fromMs, long toMs, HistoryVisitor visitor) {
        long attrRoot = EntityNode.attrArtRoot(entityNode);
        if (NodePtr.isEmpty(attrRoot)) return true;

        // Reusable search key buffer
        byte[] searchBytes = new byte[AttributeRun.KEY_LEN];
        MemorySegment searchKey = MemorySegment.ofArray(searchBytes);
        AttributeRun.encodeKey(searchKey, attrId, fromMs);

        // Predecessor check: catch runs starting before fromMs that span into range
        long scanFrom = fromMs;
        long predLeaf = ArtSearch.predecessor(resolver, attrRoot,
                searchKey, AttributeRun.KEY_LEN);
        if (!NodePtr.isEmpty(predLeaf)) {
            MemorySegment predFull = resolver.apply(predLeaf);
            int predAttrId = AttributeRun.keyAttrId(predFull);
            if (predAttrId == attrId) {
                MemorySegment predValue = predFull.asSlice(ATTR_KEY_SLOT);
                if (AttributeRun.validTo(predValue) >= fromMs) {
                    long predFirstSeen = AttributeRun.keyFirstSeen(predFull);
                    if (!visitor.visit(
                            predFirstSeen,
                            AttributeRun.lastSeen(predValue),
                            AttributeRun.validTo(predValue),
                            AttributeRun.valueRef(predValue))) {
                        return false;
                    }
                    // Skip this run in the forward scan (avoids duplicate when
                    // predecessor first_seen == fromMs, since successor is ≥)
                    scanFrom = predFirstSeen + 1;
                }
            }
        }

        // Forward scan: successor returns key ≥ searchKey, so encode scanFrom
        AttributeRun.encodeKey(searchKey, attrId, scanFrom);
        long leafPtr = ArtSearch.successor(resolver, attrRoot,
                searchKey, AttributeRun.KEY_LEN);
        while (!NodePtr.isEmpty(leafPtr)) {
            MemorySegment full = resolver.apply(leafPtr);
            int leafAttrId = AttributeRun.keyAttrId(full);
            if (leafAttrId != attrId) break;

            long firstSeen = AttributeRun.keyFirstSeen(full);
            if (firstSeen > toMs) break; // past end of range

            MemorySegment value = full.asSlice(ATTR_KEY_SLOT);
            if (!visitor.visit(firstSeen,
                    AttributeRun.lastSeen(value),
                    AttributeRun.validTo(value),
                    AttributeRun.valueRef(value))) {
                return false;
            }

            // Advance: successor is ≥, so search for (attrId, firstSeen + 1)
            AttributeRun.encodeKey(searchKey, attrId, firstSeen + 1);
            leafPtr = ArtSearch.successor(resolver, attrRoot,
                    searchKey, AttributeRun.KEY_LEN);
        }

        return true;
    }

    // ====================================================================
    // Internal: node resolution
    // ====================================================================

    private MemorySegment resolveNode(long ptr) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            int classId = NodePtr.slabClassId(ptr);
            return WriterArena.resolve(chunkStore, ptr, slab.segmentSize(classId));
        }
        return slab.resolve(ptr);
    }

    /** Resolver function used by ART searches over temporal sub-trees. */
    public LongFunction<MemorySegment> makeResolver() {
        return resolver;
    }
}
