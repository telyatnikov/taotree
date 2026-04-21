package org.taotree;

/**
 * Visitor for iterating the history of a single attribute as decoded {@link Value}s.
 *
 * <p>Mirrors {@link org.taotree.internal.temporal.HistoryVisitor} but delivers a
 * fully-decoded {@code Value} instead of an opaque {@code long valueRef}, so
 * callers don't need to reach into {@code ValueCodec} themselves.
 */
@FunctionalInterface
public interface ValueHistoryVisitor {
    /**
     * Visit one attribute run.
     *
     * @param firstSeen earliest observation timestamp (epoch ms; may be
     *                  {@link TaoTree#TIMELESS})
     * @param lastSeen  latest observation timestamp (epoch ms)
     * @param validTo   validity end timestamp (epoch ms; {@code Long.MAX_VALUE} = open-ended)
     * @param value     the decoded value for this run
     * @return {@code true} to continue iteration, {@code false} to stop early
     */
    boolean visit(long firstSeen, long lastSeen, long validTo, Value value);
}
