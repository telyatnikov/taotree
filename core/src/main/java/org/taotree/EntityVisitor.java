package org.taotree;

/**
 * Callback for entity key scans over a {@link TaoTree}.
 *
 * <p>Returned by {@link TaoTree.ReadScope#forEach(EntityVisitor)} and
 * {@link TaoTree.ReadScope#scan(org.taotree.layout.QueryBuilder,
 * org.taotree.layout.KeyHandle, EntityVisitor)} variants.
 *
 * <p>The {@code entityKey} byte array is valid only during the callback.
 * Do not retain references to it across calls.
 */
@FunctionalInterface
public interface EntityVisitor {
    /**
     * Visit one entity key during a scan.
     *
     * @param entityKey the raw key bytes for this entity
     * @return {@code true} to continue scanning, {@code false} to stop early
     */
    boolean visit(byte[] entityKey);
}
