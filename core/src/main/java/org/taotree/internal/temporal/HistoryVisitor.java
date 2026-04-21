package org.taotree.internal.temporal;

/**
 * Visitor for iterating attribute history ({@link AttributeRun} entries).
 *
 * @see TemporalReader#history
 * @see TemporalReader#historyRange
 */
@FunctionalInterface
public interface HistoryVisitor {
    /**
     * Visit a single AttributeRun in the history.
     *
     * @param firstSeen earliest observation timestamp (epoch ms)
     * @param lastSeen  latest observation timestamp (epoch ms)
     * @param validTo   validity end timestamp (epoch ms; {@code Long.MAX_VALUE} = open-ended)
     * @param valueRef  pointer to the value payload
     * @return {@code true} to continue iteration, {@code false} to stop early
     */
    boolean visit(long firstSeen, long lastSeen, long validTo, long valueRef);
}
