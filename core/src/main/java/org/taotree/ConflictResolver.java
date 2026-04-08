package org.taotree;

/**
 * Strategy for merging leaf values during deferred-commit rebase.
 *
 * <p>When a {@link TaoTree.WriteScope} operates in deferred-commit mode (the
 * default for file-backed trees), mutations are accumulated privately and
 * published atomically at close time. If another writer published between
 * the scope's snapshot and commit, a <em>rebase</em> replays mutations
 * against the new root.
 *
 * <p>For keys that exist in both the published tree and this scope's private
 * tree, the resolver merges the two leaf values. Without a resolver, the
 * scope's value always overwrites (last-writer-wins). With a resolver, the
 * application can implement domain-specific merge logic — e.g. "keep the
 * newer observation but sum the count deltas".
 *
 * @see TaoTree#write(ConflictResolver)
 */
@FunctionalInterface
public interface ConflictResolver {

    /**
     * Merge the pending (scope-private) leaf value into the target
     * (published) leaf.
     *
     * <p>The {@code target} accessor is writable and initially contains the
     * published value. The {@code pending} accessor is read-only and contains
     * this scope's final value. The {@code snapshot} accessor is read-only and
     * contains the value the key had when this scope first accessed it (i.e.
     * the base value before any of this scope's modifications).
     *
     * <p>For accumulator fields (e.g. counts), compute the delta as
     * {@code pending - snapshot} and add it to {@code target}.
     *
     * <p>Do <em>not</em> retain references to any accessor beyond this call.
     *
     * @param target   writable accessor for the rebased leaf (starts with published value)
     * @param pending  read-only accessor for this scope's final leaf value
     * @param snapshot read-only accessor for the snapshot's original leaf value
     */
    void merge(LeafAccessor target, LeafAccessor pending, LeafAccessor snapshot);
}
