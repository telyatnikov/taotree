package org.taotree.internal.temporal;

import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.alloc.WriterArena;
import org.taotree.internal.art.ArtSearch;
import org.taotree.internal.art.NodePtr;
import org.taotree.internal.champ.ChampMap;
import org.taotree.internal.cow.CowEngine;
import org.taotree.internal.cow.EpochReclaimer;
import org.taotree.internal.cow.LongList;
import org.taotree.internal.value.ValueCodec;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

/**
 * Temporal write path for per-entity ART mutations.
 *
 * <p>Implements the write logic from §18.8 of the design:
 * <ol>
 *   <li>Upsert into AttributeRuns ART (find/split/merge run)</li>
 *   <li>If state changed: compute affected window, rewrite EntityVersions</li>
 *   <li>Return updated EntityNode fields</li>
 * </ol>
 *
 * <p>All operations are copy-on-write: old roots remain valid for concurrent readers.
 * The caller (WriteScope) publishes the new global root atomically.
 *
 * <p><b>Thread safety:</b> not thread-safe. One instance per TaoTree, called
 * from within a WriteScope (which serializes writers).
 */
public final class TemporalWriter {

    // Key slot sizes (keyLen rounded up to 8-byte alignment)
    static final int ATTR_KEY_SLOT = (AttributeRun.KEY_LEN + 7) & ~7; // 16
    static final int VERS_KEY_SLOT = (EntityVersion.KEY_LEN + 7) & ~7; // 8

    private final CowEngine attrCow;
    private final CowEngine versionsCow;
    private final BumpAllocator champBump;
    private final SlabAllocator slab;
    private final ChunkStore chunkStore;

    // Thread-local CHAMP result to avoid allocating per call
    private final ChampMap.Result champResult = new ChampMap.Result();

    /**
     * Create a TemporalWriter sharing infrastructure with the owning TaoTree.
     *
     * @param slab           shared slab allocator
     * @param reclaimer      shared epoch reclaimer
     * @param chunkStore     shared chunk store (null for in-memory)
     * @param champBump      bump allocator for CHAMP nodes
     * @param prefixClassId  slab class ID for ART prefix nodes
     * @param node4ClassId   slab class ID for ART Node4
     * @param node16ClassId  slab class ID for ART Node16
     * @param node48ClassId  slab class ID for ART Node48
     * @param node256ClassId slab class ID for ART Node256
     */
    public TemporalWriter(SlabAllocator slab, EpochReclaimer reclaimer,
                          ChunkStore chunkStore, BumpAllocator champBump,
                          int prefixClassId, int node4ClassId, int node16ClassId,
                          int node48ClassId, int node256ClassId) {
        this.slab = slab;
        this.chunkStore = chunkStore;
        this.champBump = champBump;

        int attrLeafClassId = slab.registerClass(ATTR_KEY_SLOT + AttributeRun.VALUE_SIZE);
        this.attrCow = new CowEngine(slab, reclaimer, null, chunkStore,
                prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
                AttributeRun.KEY_LEN, ATTR_KEY_SLOT, new int[]{attrLeafClassId});

        int versLeafClassId = slab.registerClass(VERS_KEY_SLOT + EntityVersion.VALUE_SIZE);
        this.versionsCow = new CowEngine(slab, reclaimer, null, chunkStore,
                prefixClassId, node4ClassId, node16ClassId, node48ClassId, node256ClassId,
                EntityVersion.KEY_LEN, VERS_KEY_SLOT, new int[]{versLeafClassId});
    }

    // ====================================================================
    // Result types
    // ====================================================================

    /** Result of a full temporal write operation. */
    public record WriteResult(
            long newAttrArtRoot,
            long newVersionsArtRoot,
            long newCurrentStateRoot,
            boolean stateChanged,
            LongList retirees
    ) {}

    /** Internal result of an AttributeRuns upsert. */
    record AttrUpsertResult(
            long newRoot,
            boolean stateChanged,
            AffectedWindow affectedWindow,
            AffectedWindow revertWindow,   // non-null for splits: suffix reverts to old value
            long revertValueRef,           // value_ref for the revert window
            long effectiveValueRef,        // canonical value_ref to use downstream (EntityVersions/CHAMP)
            LongList retirees
    ) {
        /** Simple constructor for non-split cases. */
        AttrUpsertResult(long newRoot, boolean stateChanged,
                         AffectedWindow affectedWindow, LongList retirees) {
            this(newRoot, stateChanged, affectedWindow, null, 0, 0, retirees);
        }
        /** Non-split with an explicit canonical ref. */
        AttrUpsertResult(long newRoot, boolean stateChanged,
                         AffectedWindow affectedWindow, long effectiveValueRef, LongList retirees) {
            this(newRoot, stateChanged, affectedWindow, null, 0, effectiveValueRef, retirees);
        }
    }

    /** Internal result of an EntityVersions update. */
    record VersionsUpdateResult(
            long newRoot,
            long newCurrentStateRoot,
            LongList retirees
    ) {}

    /** Affected time window for EntityVersions rewriting. */
    record AffectedWindow(long from, long to) {}

    // ====================================================================
    // Main write entry point (§18.8.1)
    // ====================================================================

    /**
     * Perform a temporal write: upsert an attribute value at a given timestamp.
     *
     * <p>Per-entity ART nodes are allocated directly in the slab (not the writer arena)
     * because they are only reachable after the global ART root is published.
     *
     * @param entityNodeSeg  resolved EntityNode segment (40 bytes)
     * @param attrId         attribute dictionary ID
     * @param valueRef       pointer to the interned value
     * @param timestamp      observation timestamp (epoch ms)
     * @return updated roots and state-change flag
     */
    public WriteResult write(MemorySegment entityNodeSeg,
                             int attrId, long valueRef, long timestamp) {
        long attrArtRoot = EntityNode.attrArtRoot(entityNodeSeg);
        long versionsArtRoot = EntityNode.versionsArtRoot(entityNodeSeg);
        long currentStateRoot = EntityNode.currentStateRoot(entityNodeSeg);

        LongFunction<MemorySegment> resolver = makeResolver();

        // Phase 3: AttributeRuns upsert
        var attrResult = upsertAttributeRun(resolver, attrArtRoot,
                attrId, timestamp, valueRef);

        // Phase 4: Conditional EntityVersions update
        if (attrResult.stateChanged && attrResult.affectedWindow != null) {
            // Use the canonical value_ref from the AttributeRuns upsert — this
            // is the ref actually stored in the run, which may differ from the
            // caller-provided ref when the upsert collapsed a same-value
            // observation into an existing run (see ValueCodec.slotEquals).
            long canonicalValueRef = attrResult.effectiveValueRef;
            var versResult = updateEntityVersions(resolver,
                    versionsArtRoot, currentStateRoot,
                    attrId, canonicalValueRef, attrResult.affectedWindow);

            // Phase 4b: Handle split revert (suffix run restores old value)
            if (attrResult.revertWindow != null) {
                var revertResult = updateEntityVersions(resolver,
                        versResult.newRoot, versResult.newCurrentStateRoot,
                        attrId, attrResult.revertValueRef, attrResult.revertWindow);
                versResult = new VersionsUpdateResult(
                        revertResult.newRoot,
                        revertResult.newCurrentStateRoot,
                        revertResult.retirees);
            }

            LongList allRetirees = new LongList();
            allRetirees.addAll(attrResult.retirees);
            allRetirees.addAll(versResult.retirees);

            return new WriteResult(
                    attrResult.newRoot,
                    versResult.newRoot,
                    versResult.newCurrentStateRoot,
                    true,
                    allRetirees);
        }

        return new WriteResult(
                attrResult.newRoot,
                versionsArtRoot,
                currentStateRoot,
                false,
                attrResult.retirees);
    }

    // ====================================================================
    // AttributeRuns upsert (§18.8.2)
    // ====================================================================

    AttrUpsertResult upsertAttributeRun(
                                        LongFunction<MemorySegment> resolver,
                                        long attrArtRoot, int attrId,
                                        long T, long valueRef) {
        LongList retirees = new LongList();

        // Step 1: Find predecessor run for this attribute at time T
        byte[] searchKeyBytes = new byte[AttributeRun.KEY_LEN];
        MemorySegment searchKey = MemorySegment.ofArray(searchKeyBytes);
        AttributeRun.encodeKey(searchKey, attrId, T);

        long predLeafPtr = ArtSearch.predecessor(resolver, attrArtRoot,
                searchKey, AttributeRun.KEY_LEN);

        // Step 2: Check if T falls within an existing run of this attribute
        if (!NodePtr.isEmpty(predLeafPtr)) {
            MemorySegment predFull = resolver.apply(predLeafPtr);
            MemorySegment predKeyPart = predFull.asSlice(0, ATTR_KEY_SLOT);
            int predAttrId = AttributeRun.keyAttrId(predKeyPart);

            if (predAttrId == attrId) {
                MemorySegment predValue = predFull.asSlice(ATTR_KEY_SLOT);
                long predFirstSeen = AttributeRun.keyFirstSeen(predKeyPart);
                long predLastSeen = AttributeRun.lastSeen(predValue);
                long predValidTo = AttributeRun.validTo(predValue);
                long predValueRef = AttributeRun.valueRef(predValue);

                if (ValueCodec.slotEquals(predValueRef, valueRef, champBump)) {
                    // Same logical value — extend last_seen only, no state change.
                    // Reuse predValueRef as the canonical ref (the fresh valueRef
                    // the caller allocated becomes dead bump space; reclaimed
                    // by compaction). See docs/design-rationale.md §7.3.
                    long newLastSeen = Math.max(predLastSeen, T);
                    if (newLastSeen == predLastSeen) {
                        // No change at all
                        return new AttrUpsertResult(attrArtRoot, false, null, predValueRef, retirees);
                    }
                    // COW-update the run's last_seen
                    byte[] predKeyBytes = new byte[AttributeRun.KEY_LEN];
                    MemorySegment predKey = MemorySegment.ofArray(predKeyBytes);
                    AttributeRun.encodeKey(predKey, attrId, predFirstSeen);

                    long newRoot = cowUpdateAttrValue(attrArtRoot, predKey,
                            newLastSeen, predValidTo, predValueRef, retirees);
                    return new AttrUpsertResult(newRoot, false, null, predValueRef, retirees);
                }

                if (T >= predFirstSeen && T <= predLastSeen) {
                    // Different value, T within observed range → split run
                    return splitAndInsert(resolver, attrArtRoot,
                            attrId, predFirstSeen, predLastSeen, predValidTo, predValueRef,
                            T, valueRef);
                }
                // T > predLastSeen: falls after last observation.
                // Don't split — fall through to step 6 (shrink pred, insert new run).
            }
        }

        // Step 3: Find successor run of this attribute
        byte[] succSearchBytes = new byte[AttributeRun.KEY_LEN];
        MemorySegment succSearchKey = MemorySegment.ofArray(succSearchBytes);
        AttributeRun.encodeKey(succSearchKey, attrId, T);

        long succLeafPtr = ArtSearch.successor(resolver, attrArtRoot,
                succSearchKey, AttributeRun.KEY_LEN);

        // Find the actual successor for THIS attribute (skip runs of other attrs)
        long succFirstSeen = 0;
        long succValueRef = 0;
        boolean hasSucc = false;

        if (!NodePtr.isEmpty(succLeafPtr)) {
            MemorySegment succFull = resolver.apply(succLeafPtr);
            MemorySegment succKeyPart = succFull.asSlice(0, ATTR_KEY_SLOT);
            int succAttrId = AttributeRun.keyAttrId(succKeyPart);

            if (succAttrId == attrId) {
                MemorySegment succValue = succFull.asSlice(ATTR_KEY_SLOT);
                succFirstSeen = AttributeRun.keyFirstSeen(succKeyPart);
                succValueRef = AttributeRun.valueRef(succValue);
                hasSucc = true;
            }
        }

        // Step 4: Check if merge with successor is possible (logical equality)
        if (hasSucc && ValueCodec.slotEquals(succValueRef, valueRef, champBump)) {
            // Same logical value as successor — extend successor backwards to T.
            // Use succValueRef as the canonical ref so downstream CHAMP/EntityVersion
            // updates see a stable pointer (see docs/design-rationale.md §7.3).
            byte[] oldSuccKeyBytes = new byte[AttributeRun.KEY_LEN];
            MemorySegment oldSuccKey = MemorySegment.ofArray(oldSuccKeyBytes);
            AttributeRun.encodeKey(oldSuccKey, attrId, succFirstSeen);

            // Delete old successor and reinsert with first_seen = T
            long root = cowDeleteAttr(attrArtRoot, oldSuccKey, retirees);

            MemorySegment succFullAfterDelete = resolver.apply(succLeafPtr);
            MemorySegment succValAfterDelete = succFullAfterDelete.asSlice(ATTR_KEY_SLOT);
            long succLastSeen = AttributeRun.lastSeen(succValAfterDelete);
            long succValidTo = AttributeRun.validTo(succValAfterDelete);

            root = cowInsertAttr(root, attrId, T,
                    Math.max(succLastSeen, T), succValidTo, succValueRef, retirees);

            AffectedWindow window = new AffectedWindow(T, succFirstSeen - 1);
            return new AttrUpsertResult(root, true, window, succValueRef, retirees);
        }

        // Step 6: Insert new run
        long newValidTo = hasSucc ? succFirstSeen - 1 : Long.MAX_VALUE;

        long root = attrArtRoot;

        // Update predecessor's valid_to if it exists for this attribute
        if (!NodePtr.isEmpty(predLeafPtr)) {
            MemorySegment predFull = resolver.apply(predLeafPtr);
            MemorySegment predKeyPart = predFull.asSlice(0, ATTR_KEY_SLOT);
            int predAttrId = AttributeRun.keyAttrId(predKeyPart);

            if (predAttrId == attrId) {
                MemorySegment predValue = predFull.asSlice(ATTR_KEY_SLOT);
                long predFirstSeen = AttributeRun.keyFirstSeen(predKeyPart);
                long predLastSeen = AttributeRun.lastSeen(predValue);
                long predValueRef = AttributeRun.valueRef(predValue);

                byte[] predKeyBytes = new byte[AttributeRun.KEY_LEN];
                MemorySegment predKey = MemorySegment.ofArray(predKeyBytes);
                AttributeRun.encodeKey(predKey, attrId, predFirstSeen);

                root = cowUpdateAttrValue(root, predKey,
                        predLastSeen, T - 1, predValueRef, retirees);
            }
        }

        // Insert the new run
        root = cowInsertAttr(root, attrId, T, T, newValidTo, valueRef, retirees);

        AffectedWindow window = new AffectedWindow(T, newValidTo);
        return new AttrUpsertResult(root, true, window, valueRef, retirees);
    }

    // ====================================================================
    // Split-and-insert (§18.8.3)
    // ====================================================================

    private AttrUpsertResult splitAndInsert(
                                            LongFunction<MemorySegment> resolver,
                                            long attrArtRoot,
                                            int attrId, long existFirstSeen,
                                            long existLastSeen, long existValidTo,
                                            long existValueRef,
                                            long T, long valueRef) {
        LongList retirees = new LongList();
        long root = attrArtRoot;

        // Existing run key
        byte[] existKeyBytes = new byte[AttributeRun.KEY_LEN];
        MemorySegment existKey = MemorySegment.ofArray(existKeyBytes);
        AttributeRun.encodeKey(existKey, attrId, existFirstSeen);

        if (T > existFirstSeen) {
            // Shrink existing run to prefix portion: [existFirstSeen, T-1]
            root = cowUpdateAttrValue(root, existKey,
                    existLastSeen, T - 1, existValueRef, retirees);
        } else {
            // T == existFirstSeen — no prefix portion, delete existing
            root = cowDeleteAttr(root, existKey, retirees);
        }

        // Decide whether a suffix run exists: only when there are actual
        // observations of the old value strictly past T. Without that,
        // the new run inherits the old run's validTo (the old "assumed valid"
        // window collapses into the new value). This prevents a degenerate
        // suffix run [T+1, existLastSeen] when existLastSeen <= T (notably
        // the TIMELESS overwrite case where existFirstSeen==existLastSeen==T==0
        // but existValidTo==Long.MAX_VALUE).
        boolean hasSuffix = (T < existValidTo) && (existLastSeen > T);

        // Insert new run at T: [T, T]. If no suffix, the new run inherits
        // existValidTo (so the overwrite fully replaces the prior run's window).
        long insertValidTo = hasSuffix ? T : existValidTo;
        root = cowInsertAttr(root, attrId, T,
                T, insertValidTo, valueRef, retirees);

        if (hasSuffix) {
            root = cowInsertAttr(root, attrId, T + 1,
                    existLastSeen, existValidTo, existValueRef, retirees);
            AffectedWindow window = new AffectedWindow(T, T);
            AffectedWindow revertWindow = new AffectedWindow(T + 1, existValidTo);
            return new AttrUpsertResult(root, true, window,
                    revertWindow, existValueRef, valueRef, retirees);
        }

        AffectedWindow window = new AffectedWindow(T, existValidTo);
        return new AttrUpsertResult(root, true, window, valueRef, retirees);
    }

    // ====================================================================
    // EntityVersions update (§18.10.2)
    // ====================================================================

    VersionsUpdateResult updateEntityVersions(
                                              LongFunction<MemorySegment> resolver,
                                              long versionsArtRoot,
                                              long currentStateRoot,
                                              int attrId, long newValueRef,
                                              AffectedWindow window) {
        LongList retirees = new LongList();
        long root = versionsArtRoot;

        // Step 1: Find all EntityVersions in the affected window
        List<VersionEntry> affectedVersions = new ArrayList<>();
        collectVersionsInRange(resolver, root, window.from, window.to, affectedVersions);

        // Step 2: Find predecessor version (active just before the window)
        byte[] predKeyBytes = new byte[EntityVersion.KEY_LEN];
        MemorySegment predKey = MemorySegment.ofArray(predKeyBytes);
        EntityVersion.encodeKey(predKey, window.from);

        long predLeafPtr = ArtSearch.predecessor(resolver, root,
                predKey, EntityVersion.KEY_LEN);

        long baseStateRoot = ChampMap.EMPTY_ROOT;
        long predFirstSeen = 0;
        boolean hasPred = false;
        if (!NodePtr.isEmpty(predLeafPtr)) {
            MemorySegment predFull = resolver.apply(predLeafPtr);
            MemorySegment predKeyPart = predFull.asSlice(0, VERS_KEY_SLOT);
            MemorySegment predValue = predFull.asSlice(VERS_KEY_SLOT);
            baseStateRoot = EntityVersion.stateRootRef(predValue);
            predFirstSeen = EntityVersion.keyFirstSeen(predKeyPart);
            hasPred = true;
        }

        // Step 4: Compute new state at window.from
        champResult.reset();
        long newStateRoot = (newValueRef == AttributeRun.TOMBSTONE_VALUE_REF)
                ? ChampMap.remove(champBump, baseStateRoot, attrId, champResult)
                : ChampMap.put(champBump, baseStateRoot, attrId, newValueRef, champResult);

        // Step 5: Check if we need a new EntityVersion at window.from
        boolean needNewVersion = true;
        if (hasPred && baseStateRoot == newStateRoot) {
            needNewVersion = false; // state didn't actually change (CHAMP canonical form)
        }
        if (!affectedVersions.isEmpty()
                && affectedVersions.getFirst().firstSeen == window.from) {
            needNewVersion = false; // there's already a version at this timestamp
        }

        // Step 6: Insert or update version at window.from
        if (needNewVersion) {
            long validTo = affectedVersions.isEmpty()
                    ? window.to
                    : affectedVersions.getFirst().firstSeen - 1;

            root = cowInsertVersion(root, window.from,
                    validTo, newStateRoot, retirees);

            // Update predecessor's valid_to
            if (hasPred) {
                byte[] predOrigKey = new byte[EntityVersion.KEY_LEN];
                MemorySegment pKey = MemorySegment.ofArray(predOrigKey);
                EntityVersion.encodeKey(pKey, predFirstSeen);
                root = cowUpdateVersionValue(root, pKey,
                        window.from - 1, baseStateRoot, retirees);
            }
        }

        // Step 7: Rewrite each affected version's CHAMP root
        for (var entry : affectedVersions) {
            champResult.reset();
            long updatedStateRoot = (newValueRef == AttributeRun.TOMBSTONE_VALUE_REF)
                    ? ChampMap.remove(champBump, entry.stateRootRef, attrId, champResult)
                    : ChampMap.put(champBump, entry.stateRootRef, attrId, newValueRef, champResult);
            byte[] vKeyBytes = new byte[EntityVersion.KEY_LEN];
            MemorySegment vKey = MemorySegment.ofArray(vKeyBytes);
            EntityVersion.encodeKey(vKey, entry.firstSeen);
            root = cowUpdateVersionValue(root, vKey,
                    entry.validTo, updatedStateRoot, retirees);
        }

        // Step 8: Merge adjacent versions with equal state_root_ref
        root = mergeAdjacentVersions(resolver, root, window, retirees);

        // Step 9: Update current state if the window touches the present
        long newCurrentStateRoot;
        if (window.to == Long.MAX_VALUE) {
            newCurrentStateRoot = newStateRoot;
        } else {
            // Find the latest version to get its state root
            long latestLeaf = ArtSearch.rightmostLeaf(resolver, root);
            if (!NodePtr.isEmpty(latestLeaf)) {
                MemorySegment latestFull = resolver.apply(latestLeaf);
                MemorySegment latestValue = latestFull.asSlice(VERS_KEY_SLOT);
                newCurrentStateRoot = EntityVersion.stateRootRef(latestValue);
            } else {
                newCurrentStateRoot = currentStateRoot;
            }
        }

        return new VersionsUpdateResult(root, newCurrentStateRoot, retirees);
    }

    // ====================================================================
    // Merge adjacent versions (§18.10.3)
    // ====================================================================

    private long mergeAdjacentVersions(
                                       LongFunction<MemorySegment> resolver,
                                       long root, AffectedWindow window,
                                       LongList retirees) {
        // Collect versions in window + one beyond
        List<VersionEntry> versions = new ArrayList<>();
        collectVersionsInRange(resolver, root, window.from, Long.MAX_VALUE, versions);

        if (versions.size() < 2) return root;

        // Walk the list and collect every adjacent equal-state group.
        // Previously this loop only kept state for the *last* group, so earlier
        // groups had their successors deleted but the surviving version's
        // validTo was never extended — producing duplicate/gap artefacts.
        List<Long> toDelete = new ArrayList<>();
        List<VersionEntry> updates = new ArrayList<>();  // survivors needing validTo rewrite
        VersionEntry prev = versions.getFirst();
        long extendedTo = prev.validTo;
        boolean merged = false;

        for (int i = 1; i < versions.size(); i++) {
            VersionEntry curr = versions.get(i);

            if (prev.stateRootRef == curr.stateRootRef) {
                // Merge: delete current, extend the surviving prev's valid_to.
                toDelete.add(curr.firstSeen);
                extendedTo = curr.validTo;
                merged = true;
            } else {
                // Close off the previous group before moving on.
                if (merged) {
                    updates.add(new VersionEntry(prev.firstSeen, extendedTo, prev.stateRootRef));
                }
                prev = curr;
                extendedTo = curr.validTo;
                merged = false;
            }

            // Stop scanning once we're beyond the window + 1
            if (curr.firstSeen > window.to) break;
        }
        if (merged) {
            updates.add(new VersionEntry(prev.firstSeen, extendedTo, prev.stateRootRef));
        }

        // Delete merged versions
        for (long ts : toDelete) {
            byte[] keyBytes = new byte[EntityVersion.KEY_LEN];
            MemorySegment key = MemorySegment.ofArray(keyBytes);
            EntityVersion.encodeKey(key, ts);
            root = cowDeleteVersion(root, key, retirees);
        }

        // Update every surviving version's valid_to.
        for (VersionEntry u : updates) {
            byte[] keyBytes = new byte[EntityVersion.KEY_LEN];
            MemorySegment key = MemorySegment.ofArray(keyBytes);
            EntityVersion.encodeKey(key, u.firstSeen);
            root = cowUpdateVersionValue(root, key,
                    u.validTo, u.stateRootRef, retirees);
        }

        return root;
    }

    // ====================================================================
    // Version scan helper
    // ====================================================================

    private record VersionEntry(long firstSeen, long validTo, long stateRootRef) {}

    /**
     * Collect EntityVersion entries whose first_seen is in [from, to].
     * Uses ArtSearch.successor to walk forward from 'from'.
     */
    private void collectVersionsInRange(LongFunction<MemorySegment> resolver,
                                        long root, long from, long to,
                                        List<VersionEntry> out) {
        byte[] keyBytes = new byte[EntityVersion.KEY_LEN];
        MemorySegment searchKey = MemorySegment.ofArray(keyBytes);
        EntityVersion.encodeKey(searchKey, from);

        long leafPtr = ArtSearch.successor(resolver, root,
                searchKey, EntityVersion.KEY_LEN);

        while (!NodePtr.isEmpty(leafPtr)) {
            MemorySegment full = resolver.apply(leafPtr);
            MemorySegment keyPart = full.asSlice(0, VERS_KEY_SLOT);
            MemorySegment valuePart = full.asSlice(VERS_KEY_SLOT);

            long firstSeen = EntityVersion.keyFirstSeen(keyPart);
            if (firstSeen > to) break;

            out.add(new VersionEntry(firstSeen,
                    EntityVersion.validTo(valuePart),
                    EntityVersion.stateRootRef(valuePart)));

            // Find next version after this one
            EntityVersion.encodeKey(searchKey, firstSeen + 1);
            leafPtr = ArtSearch.successor(resolver, root,
                    searchKey, EntityVersion.KEY_LEN);
        }
    }

    // ====================================================================
    // CowEngine wrappers for AttributeRuns ART
    // ====================================================================

    /**
     * Insert a new AttributeRun leaf into the ART.
     * Returns the new root.
     */
    private long cowInsertAttr(long root,
                               int attrId, long firstSeen,
                               long lastSeen, long validTo, long valueRef,
                               LongList retirees) {
        byte[] keyBytes = new byte[AttributeRun.KEY_LEN];
        MemorySegment key = MemorySegment.ofArray(keyBytes);
        AttributeRun.encodeKey(key, attrId, firstSeen);

        var result = attrCow.deferredGetOrCreate(root,
                key, AttributeRun.KEY_LEN, 0);
        collectRetirees(result, retirees);

        if (result.mutated()) root = result.newRoot();

        // Write leaf value (starts after keySlot)
        MemorySegment leafFull = resolveLeaf(result.leafPtr());
        MemorySegment leafValue = leafFull.asSlice(ATTR_KEY_SLOT);
        AttributeRun.writeValue(leafValue, lastSeen, validTo, valueRef);

        return root;
    }

    /**
     * COW-update an existing AttributeRun's leaf value.
     * Returns the new root.
     */
    private long cowUpdateAttrValue(long root,
                                    MemorySegment key,
                                    long lastSeen, long validTo, long valueRef,
                                    LongList retirees) {
        var result = attrCow.deferredGetOrCreateCopy(root,
                key, AttributeRun.KEY_LEN, 0);
        collectRetirees(result, retirees);

        if (result.mutated()) root = result.newRoot();

        MemorySegment leafFull = resolveLeaf(result.leafPtr());
        MemorySegment leafValue = leafFull.asSlice(ATTR_KEY_SLOT);
        AttributeRun.writeValue(leafValue, lastSeen, validTo, valueRef);

        return root;
    }

    /**
     * Delete an AttributeRun from the ART.
     * Returns the new root.
     */
    private long cowDeleteAttr(long root,
                               MemorySegment key, LongList retirees) {
        var result = attrCow.deferredDelete(root,
                key, AttributeRun.KEY_LEN);
        collectRetirees(result, retirees);
        return result.mutated() ? result.newRoot() : root;
    }

    // ====================================================================
    // CowEngine wrappers for EntityVersions ART
    // ====================================================================

    private long cowInsertVersion(long root,
                                  long firstSeen, long validTo, long stateRootRef,
                                  LongList retirees) {
        byte[] keyBytes = new byte[EntityVersion.KEY_LEN];
        MemorySegment key = MemorySegment.ofArray(keyBytes);
        EntityVersion.encodeKey(key, firstSeen);

        var result = versionsCow.deferredGetOrCreate(root,
                key, EntityVersion.KEY_LEN, 0);
        collectRetirees(result, retirees);

        if (result.mutated()) root = result.newRoot();

        MemorySegment leafFull = resolveLeaf(result.leafPtr());
        MemorySegment leafValue = leafFull.asSlice(VERS_KEY_SLOT);
        EntityVersion.writeValue(leafValue, validTo, stateRootRef);

        return root;
    }

    private long cowUpdateVersionValue(long root,
                                       MemorySegment key,
                                       long validTo, long stateRootRef,
                                       LongList retirees) {
        var result = versionsCow.deferredGetOrCreateCopy(root,
                key, EntityVersion.KEY_LEN, 0);
        collectRetirees(result, retirees);

        if (result.mutated()) root = result.newRoot();

        MemorySegment leafFull = resolveLeaf(result.leafPtr());
        MemorySegment leafValue = leafFull.asSlice(VERS_KEY_SLOT);
        EntityVersion.writeValue(leafValue, validTo, stateRootRef);

        return root;
    }

    private long cowDeleteVersion(long root,
                                  MemorySegment key, LongList retirees) {
        var result = versionsCow.deferredDelete(root,
                key, EntityVersion.KEY_LEN);
        collectRetirees(result, retirees);
        return result.mutated() ? result.newRoot() : root;
    }

    // ====================================================================
    // Infrastructure helpers
    // ====================================================================

    /** Resolve a node pointer handling both slab and arena-allocated pointers. */
    private MemorySegment resolveLeaf(long ptr) {
        if (chunkStore != null && WriterArena.isArenaAllocated(ptr)) {
            int classId = NodePtr.slabClassId(ptr);
            return WriterArena.resolve(chunkStore, ptr, slab.segmentSize(classId));
        }
        return slab.resolve(ptr);
    }

    /** Create a resolver function for ArtSearch. */
    LongFunction<MemorySegment> makeResolver() {
        return this::resolveLeaf;
    }

    /** Collect retirees from a CowEngine result. */
    private static void collectRetirees(CowEngine.DeferredResult result, LongList target) {
        var retirees = result.retirees();
        for (int i = 0, n = retirees.size(); i < n; i++) {
            target.add(retirees.get(i));
        }
    }
}
