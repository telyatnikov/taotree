package org.taotree;

/**
 * Visitor for scanning leaf entries in a {@link TaoTree}.
 *
 * <p>The {@link LeafAccessor} passed to {@link #visit} is reusable — it is
 * re-pointed to each leaf during the scan. It is valid only for the duration
 * of the callback. Do not retain references to it.
 *
 * @return {@code true} to continue scanning, {@code false} to stop early
 */
@FunctionalInterface
public interface LeafVisitor {
    boolean visit(LeafAccessor leaf);
}
