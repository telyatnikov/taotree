package org.taotree;

import org.taotree.internal.champ.ChampMap;
import org.taotree.internal.cow.CowEngine;
import org.taotree.internal.cow.LongList;
import org.taotree.internal.cow.LongOpenHashSet;
import org.taotree.internal.cow.MutationLog;
import org.taotree.internal.cow.TemporalOpLog;
import org.taotree.internal.temporal.EntityNode;

import java.lang.foreign.MemorySegment;

/**
 * Package-private bundle of {@link TaoTree.WriteScope}'s heavyweight internals:
 * optimistic/locked COW, deferred commit with optimistic rebase, write-back
 * fast path, and temporal op-log replay. Extracted from the nested WriteScope
 * class so the public API surface stays small and the god-object footprint of
 * {@code TaoTree.java} shrinks.
 *
 * <p>Holds a reference to its owning {@link TaoTree.WriteScope}; mutates the
 * scope's package-private state fields and calls into the enclosing
 * {@link TaoTree} for shared infrastructure (commit lock, reclaimer, slab,
 * bump, cow engine, temporal writer, publication state).
 *
 * <p>Not thread-safe: each WriteScope is used by exactly one thread; the Ops
 * instance inherits that confinement.
 */
final class WriteScopeOps {
    private final TaoTree.WriteScope scope;
    private final TaoTree tree;

    WriteScopeOps(TaoTree.WriteScope scope) {
        this.scope = scope;
        this.tree = scope.tree();
    }

    /** Result of a rebase computation: new root, size, and retirees. */
    private record RebaseResult(long root, long size, LongList retirees) {}

    // -----------------------------------------------------------------------
    // Per-mutation deferred COW (file-backed) + child-tree locked fallback
    // -----------------------------------------------------------------------

    long optimisticGetOrCreate(MemorySegment key, int keyLen, int leafClass) {
        if (scope.scopeSnapshot != null && !scope.lockedMode) {
            // File-backed deferred-commit mode: COW against private root
            scope.scopeMutated = true;
            if (scope.seenLeafPtrs == null) scope.seenLeafPtrs = new LongOpenHashSet();

            var result = tree.cowEngine.deferredGetOrCreateCopy(
                    scope.scopeArena, scope.scopeRoot, key, keyLen, leafClass);
            long ptr = result.leafPtr();
            long originalPtr = result.originalLeafPtr();

            // If the leaf was returned unchanged (arena-allocated), check whether
            // it's from THIS scope or from the published tree (stale arena
            // allocation from another writer's rebase). Force-copy stale leaves
            // to get a truly private copy.
            if (!result.mutated() && !scope.seenLeafPtrs.contains(ptr)) {
                originalPtr = ptr;
                result = tree.cowEngine.deferredGetOrCreateForceCopy(
                        scope.scopeArena, scope.scopeRoot, key, keyLen, leafClass);
                ptr = result.leafPtr();
            }

            if (result.mutated()) {
                scope.scopeRoot = result.newRoot();
                scope.scopeSize += result.sizeDelta();
                collectRetirees(result);
            }
            // Track seen leaf ptrs for force-copy dedup.
            // Always record mutations for rebase capability.
            boolean firstSeen = scope.seenLeafPtrs.add(ptr);
            if (firstSeen) {
                if (scope.mutationLog == null) {
                    scope.mutationLog = new MutationLog();
                }
                // No resolver — skip snapshot (saves a copy per key)
                scope.mutationLog.record(key, keyLen, leafClass, ptr,
                    MemorySegment.ofArray(new byte[0]), 0, originalPtr);
            }
            return ptr;
        }

        // Child-tree mode: old model (commit lock on first mutation)
        if (scope.commitLockHeld) {
            tree.publishRoot();
            return lockedGetOrCreate(key, keyLen, leafClass);
        }
        var snapshot = tree.publishedState();
        CowEngine.DeferredResult result = doCow(key, keyLen, leafClass, snapshot.root());
        tree.commitLock.lock();
        scope.commitLockHeld = true;
        var current = tree.publishedState();
        if (current != snapshot && result.mutated()) {
            result = doCow(key, keyLen, leafClass, current.root());
        }
        if (result.mutated()) {
            applyResult(result);
        }
        return result.leafPtr();
    }

    boolean optimisticDelete(MemorySegment key, int keyLen) {
        if (scope.scopeSnapshot != null && !scope.lockedMode) {
            // Transition from deferred to locked mode: commit any pending
            // deferred mutations first, then delete under the commit lock.
            transitionToLocked();
            return lockedDelete(key, keyLen);
        }

        // Child-tree mode
        if (scope.commitLockHeld) {
            tree.publishRoot();
            return lockedDelete(key, keyLen);
        }
        var snapshot = tree.publishedState();
        CowEngine.DeferredResult result = doDeleteCow(key, keyLen, snapshot.root());
        tree.commitLock.lock();
        scope.commitLockHeld = true;
        var current = tree.publishedState();
        if (current != snapshot) {
            result = doDeleteCow(key, keyLen, current.root());
        }
        if (result.mutated()) {
            applyResult(result);
        }
        return result.mutated();
    }

    /** COW under commit lock (child-tree mode, second+ mutation in scope). */
    long lockedGetOrCreate(MemorySegment key, int keyLen, int leafClass) {
        var result = doCow(key, keyLen, leafClass, tree.root);
        if (result.mutated()) {
            applyResult(result);
        }
        return result.leafPtr();
    }

    boolean lockedDelete(MemorySegment key, int keyLen) {
        var result = doDeleteCow(key, keyLen, tree.root);
        if (result.mutated()) {
            applyResult(result);
        }
        return result.mutated();
    }

    private CowEngine.DeferredResult doCow(MemorySegment key, int keyLen,
                                           int leafClass, long currentRoot) {
        if (scope.scopeArena != null) {
            return tree.cowEngine.deferredGetOrCreate(scope.scopeArena, currentRoot,
                key, keyLen, leafClass);
        }
        return tree.cowEngine.deferredGetOrCreate(currentRoot, key, keyLen, leafClass);
    }

    private CowEngine.DeferredResult doDeleteCow(MemorySegment key, int keyLen,
                                                 long currentRoot) {
        if (scope.scopeArena != null) {
            return tree.cowEngine.deferredDelete(scope.scopeArena, currentRoot, key, keyLen);
        }
        return tree.cowEngine.deferredDelete(currentRoot, key, keyLen);
    }

    private void collectRetirees(CowEngine.DeferredResult result) {
        var retirees = result.retirees();
        for (int i = 0, n = retirees.size(); i < n; i++) {
            scope.scopeRetirees.add(retirees.get(i));
        }
    }

    /** Apply a mutation result to the outer tree's root/size (child-tree mode). */
    private void applyResult(CowEngine.DeferredResult result) {
        tree.root = result.newRoot();
        tree.size += result.sizeDelta();
        var reclaimer = tree.reclaimer();
        if (reclaimer != null) {
            var retirees = result.retirees();
            for (int i = 0, n = retirees.size(); i < n; i++) {
                reclaimer.retire(retirees.get(i));
            }
            reclaimer.advanceGeneration();
        }
    }

    /**
     * Transition from deferred-commit mode to locked mode.
     * Commits any pending deferred mutations under the commit lock, then
     * operates in locked mode for subsequent mutations (deletes).
     */
    private void transitionToLocked() {
        if (scope.scopeMutated) {
            deferredCommitImpl(true); // commit + keep lock held
        } else {
            ensureCommitLock();
        }
        scope.lockedMode = true;
        scope.scopeMutated = false; // already committed
    }

    private void ensureCommitLock() {
        if (!scope.commitLockHeld) {
            tree.commitLock.lock();
            scope.commitLockHeld = true;
            // Sync with latest published state
            var pub = tree.publishedState();
            tree.root = pub.root();
            tree.size = pub.size();
        }
    }

    // -----------------------------------------------------------------------
    // Temporal writes (public API entry point lives on WriteScope)
    // -----------------------------------------------------------------------

    boolean putTemporalImpl(MemorySegment entityKey, int attrId,
                            long valueRef, long timestamp) {
        var tw = tree.ensureTemporalWriter();

        // getOrCreate the entity → COW-copies the 40-byte EntityNode leaf
        long leafPtr = optimisticGetOrCreate(entityKey, tree.keyLen, 0);
        MemorySegment entityNodeSeg = tree.leafValueImpl(leafPtr);

        // Perform the temporal write (modifies per-entity ARTs and CHAMP)
        var result = tw.write(entityNodeSeg, attrId, valueRef, timestamp);

        EntityNode.setCurrentStateRoot(entityNodeSeg, result.newCurrentStateRoot());
        EntityNode.setAttrArtRoot(entityNodeSeg, result.newAttrArtRoot());
        EntityNode.setVersionsArtRoot(entityNodeSeg, result.newVersionsArtRoot());

        // Record the op so rebaseCompute can replay it (instead of copying the
        // full EntityNode segment and clobbering concurrent CHAMP/ART updates).
        if (scope.scopeArena != null) {
            recordTempOp(TemporalOpLog.KIND_PUT, entityKey, attrId, valueRef, timestamp);
        }

        // Collect per-entity ART retirees for epoch reclamation
        var retirees = result.retirees();
        if (retirees != null && retirees.size() > 0) {
            if (scope.scopeRetirees != null) {
                scope.scopeRetirees.addAll(retirees);
            } else {
                var reclaimer = tree.reclaimer();
                if (reclaimer != null) {
                    for (int i = 0, n = retirees.size(); i < n; i++) {
                        reclaimer.retire(retirees.get(i));
                    }
                }
            }
        }

        return result.stateChanged();
    }

    void recordTempOp(int kind, MemorySegment entityKey,
                      int attrId, long valueRef, long ts) {
        if (scope.tempOpLog == null) scope.tempOpLog = new TemporalOpLog();
        byte[] k = new byte[tree.keyLen];
        MemorySegment.copy(entityKey, 0, MemorySegment.ofArray(k), 0, tree.keyLen);
        if (kind == TemporalOpLog.KIND_PUT) {
            scope.tempOpLog.recordPut(k, attrId, valueRef, ts);
        } else {
            scope.tempOpLog.recordDeleteAttr(k, attrId);
        }
    }

    // -----------------------------------------------------------------------
    // Deferred commit + rebase + write-back
    // -----------------------------------------------------------------------

    void deferredCommit() {
        deferredCommitImpl(false);
    }

    void deferredCommitImpl(boolean keepLock) {
        LongList retireAfterPublish = null;
        LongList retireStaleAfterPublish = null;
        boolean resetArenaAfterPublish = false;

        // Optimistic rebase: if we detect a conflict before acquiring the lock,
        // perform the expensive rebase computation outside the lock so concurrent
        // writers are not serialized during the replay.
        if (!scope.commitLockHeld && scope.mutationLog != null && scope.mutationLog.size() > 0) {
            var pre = tree.publishedState();
            if (pre != scope.scopeSnapshot) {
                RebaseResult optimistic = rebaseCompute(pre);

                tree.commitLock.lock();
                scope.commitLockHeld = true;
                try {
                    var current = tree.publishedState();
                    if (current == pre) {
                        tree.root = optimistic.root;
                        tree.size = optimistic.size;
                        tree.publishRoot();
                        retireAfterPublish = optimistic.retirees;
                    } else {
                        // Another writer published during optimistic rebase — redo under lock
                        var locked = rebaseCompute(current);
                        tree.root = locked.root;
                        tree.size = locked.size;
                        tree.publishRoot();
                        retireAfterPublish = locked.retirees;
                    }
                    retireStaleAfterPublish = scope.scopeRetirees;
                } finally {
                    if (!keepLock) {
                        tree.commitLock.unlock();
                        scope.commitLockHeld = false;
                    }
                }
                tree.retireNodes(retireAfterPublish);
                tree.retireNodes(retireStaleAfterPublish);
                return;
            }
        }

        // Fast path: no conflict detected pre-lock, or commitLock already held
        if (!scope.commitLockHeld) {
            tree.commitLock.lock();
            scope.commitLockHeld = true;
        }
        try {
            var current = tree.publishedState();
            if (current == scope.scopeSnapshot) {
                if (shouldWriteBack()) {
                    // Write-back is only worthwhile for small pure-update scopes.
                    // Large copies monopolize the commit lock longer than simply
                    // publishing the scope root and reclaiming arena pages later.
                    writeBackMutations();
                    if (scope.scopeSize == scope.scopeSnapshot.size()) {
                        tree.root = scope.scopeSnapshot.root();
                        tree.size = scope.scopeSize;
                        tree.publishRoot();
                        resetArenaAfterPublish = true;
                    } else {
                        tree.root = scope.scopeRoot;
                        tree.size = scope.scopeSize;
                        tree.publishRoot();
                        retireAfterPublish = scope.scopeRetirees;
                    }
                } else {
                    tree.root = scope.scopeRoot;
                    tree.size = scope.scopeSize;
                    tree.publishRoot();
                    retireAfterPublish = scope.scopeRetirees;
                }
            } else {
                // Conflict under lock (commitLock was already held, or TOCTOU race)
                var result = rebaseCompute(current);
                tree.root = result.root;
                tree.size = result.size;
                tree.publishRoot();
                retireAfterPublish = result.retirees;
                retireStaleAfterPublish = scope.scopeRetirees;
            }
        } finally {
            if (!keepLock) {
                tree.commitLock.unlock();
                scope.commitLockHeld = false;
            }
        }
        if (resetArenaAfterPublish) {
            scope.scopeArena.resetToScopeStart();
        }
        tree.retireNodes(retireAfterPublish);
        tree.retireNodes(retireStaleAfterPublish);
    }

    /**
     * Replay the mutation log against the given published state, copying leaf
     * values from our private COW copies to the rebased copies.
     *
     * <p>Only <em>computes</em> the rebase result; does not modify TaoTree
     * fields or publish. Caller publishes under the commit lock after
     * verifying the target state is still current.
     *
     * <p>For keys that already existed in the published tree (sizeDelta == 0),
     * the pending value overwrites (last-writer-wins).
     */
    private RebaseResult rebaseCompute(TaoTree.PublicationState against) {
        // Temporal trees: replay logical operations so concurrent writers'
        // CHAMP/ART updates to the same EntityNode are preserved. The
        // segment-copy fallback path (below) would clobber those updates.
        if (scope.tempOpLog != null && scope.tempOpLog.size() > 0) {
            return rebaseComputeTemporal(against);
        }

        long rebaseRoot = against.root();
        long rebaseSize = against.size();
        var rebaseRetirees = new LongList();

        // Mutation log is already de-duplicated by leafPtr at recording time
        for (int i = 0, n = scope.mutationLog.size(); i < n; i++) {
            byte[] keyBytes = scope.mutationLog.key(i);
            int kl = scope.mutationLog.keyLen(i);
            int lc = scope.mutationLog.leafClass(i);
            long oldLeafPtr = scope.mutationLog.leafPtr(i);

            var keySeg = MemorySegment.ofArray(keyBytes);
            // Force-copy during rebase: the published tree may contain
            // arena-allocated leaves from another writer's arena.
            var result = tree.cowEngine.deferredGetOrCreateForceCopy(
                    scope.scopeArena, rebaseRoot, keySeg, kl, lc);

            if (result.mutated()) {
                rebaseRoot = result.newRoot();
                rebaseSize += result.sizeDelta();
                var rets = result.retirees();
                for (int j = 0, rn = rets.size(); j < rn; j++) {
                    rebaseRetirees.add(rets.get(j));
                }
            }

            // Merge leaf values from the old private leaf into the rebased leaf
            MemorySegment oldValue = tree.leafValueImpl(oldLeafPtr);
            MemorySegment newValue = tree.leafValueImpl(result.leafPtr());
            int classId = tree.leafClassIds[lc];
            int valueSize = tree.slab().segmentSize(classId) - tree.keySlotSize;

            // Last-writer-wins: always copy our value
            MemorySegment.copy(oldValue, 0, newValue, 0, valueSize);
        }

        return new RebaseResult(rebaseRoot, rebaseSize, rebaseRetirees);
    }

    /**
     * Rebase for temporal trees: replay each logical op (put/delete-attr)
     * against the newly published root, merging with whatever concurrent
     * writers did to the same entity's EntityNode (CHAMP state root and
     * per-entity ART roots).
     */
    private RebaseResult rebaseComputeTemporal(TaoTree.PublicationState against) {
        long rebaseRoot = against.root();
        long rebaseSize = against.size();
        var rebaseRetirees = new LongList();
        var tw = tree.ensureTemporalWriter();

        for (int i = 0, n = scope.tempOpLog.size(); i < n; i++) {
            byte[] entityKey = scope.tempOpLog.entityKey(i);
            int attrId = scope.tempOpLog.attrId(i);
            int kind = scope.tempOpLog.kind(i);
            var keySeg = MemorySegment.ofArray(entityKey);

            // Force-copy the entity leaf under the rebased root so we start
            // from the *currently published* EntityNode (which may reflect
            // another writer's concurrent changes).
            var result = tree.cowEngine.deferredGetOrCreateForceCopy(
                    scope.scopeArena, rebaseRoot, keySeg, entityKey.length, 0);

            if (result.mutated()) {
                rebaseRoot = result.newRoot();
                rebaseSize += result.sizeDelta();
                var rets = result.retirees();
                for (int j = 0, rn = rets.size(); j < rn; j++) {
                    rebaseRetirees.add(rets.get(j));
                }
            }

            MemorySegment entityNodeSeg = tree.leafValueImpl(result.leafPtr());

            if (kind == TemporalOpLog.KIND_PUT) {
                long valueRef = scope.tempOpLog.valueRef(i);
                long ts = scope.tempOpLog.timestamp(i);
                var twr = tw.write(entityNodeSeg, attrId, valueRef, ts);
                EntityNode.setCurrentStateRoot(entityNodeSeg, twr.newCurrentStateRoot());
                EntityNode.setAttrArtRoot(entityNodeSeg, twr.newAttrArtRoot());
                EntityNode.setVersionsArtRoot(entityNodeSeg, twr.newVersionsArtRoot());
                var twRets = twr.retirees();
                if (twRets != null) {
                    for (int j = 0, rn = twRets.size(); j < rn; j++) {
                        rebaseRetirees.add(twRets.get(j));
                    }
                }
            } else { // KIND_DELETE_ATTR
                long currentStateRoot = EntityNode.currentStateRoot(entityNodeSeg);
                if (currentStateRoot != ChampMap.EMPTY_ROOT) {
                    var r = new ChampMap.Result();
                    long newStateRoot = ChampMap.remove(
                            tree.bump(), currentStateRoot, attrId, r);
                    if (r.modified) {
                        EntityNode.setCurrentStateRoot(entityNodeSeg, newStateRoot);
                    }
                }
            }
        }

        return new RebaseResult(rebaseRoot, rebaseSize, rebaseRetirees);
    }

    private boolean shouldWriteBack() {
        return scope.mutationLog != null
                && scope.mutationLog.allHaveOriginals()
                && scope.mutationLog.size() <= TaoTree.MAX_WRITE_BACK_ENTRIES
                && scope.mutationLog.totalValueBytes() <= TaoTree.MAX_WRITE_BACK_BYTES;
    }

    private void writeBackMutations() {
        for (int i = 0, n = scope.mutationLog.size(); i < n; i++) {
            long origPtr = scope.mutationLog.originalLeafPtr(i);
            if (origPtr == 0) continue;
            long arenaPtr = scope.mutationLog.leafPtr(i);
            int classId = tree.leafClassIds[scope.mutationLog.leafClass(i)];
            int valueSize = tree.slab().segmentSize(classId) - tree.keySlotSize;
            MemorySegment arenaVal = tree.leafValueImpl(arenaPtr);
            MemorySegment origVal = tree.leafValueImpl(origPtr);
            MemorySegment.copy(arenaVal, 0, origVal, 0, valueSize);
        }
    }
}
