package org.taotree.internal;

/**
 * Immutable snapshot of the tree's visible state.
 *
 * <p>A {@code PublicationState} captures everything a reader needs to traverse
 * the tree: the primary root pointer, dictionary roots and counters, and a
 * monotonically increasing generation number.
 *
 * <p><b>Concurrency contract:</b>
 * <ul>
 *   <li>Published by the writer via {@code VarHandle.setRelease} on the tree's
 *       publication handle — this is the single linearization point for each
 *       {@code WriteScope}.
 *   <li>Read by readers via {@code VarHandle.getAcquire} — this is the single
 *       acquire per traversal. JMM transitivity guarantees that all plain stores
 *       performed before the release are visible to all plain loads after the
 *       matching acquire.
 *   <li>The record itself is immutable after construction — no fields are ever
 *       modified. A new {@code PublicationState} is created for each generation.
 * </ul>
 *
 * <p><b>Generation semantics:</b>
 * <ul>
 *   <li>{@code generation} increases monotonically. Each successful writer
 *       publication increments it.
 *   <li>The epoch reclaimer uses generations to determine when retired nodes
 *       can be freed: a node retired at generation G is safe to free when
 *       {@code safeReclaimGeneration >= G}.
 * </ul>
 *
 * @param generation       monotonically increasing publication number
 * @param primaryRoot      root {@link NodePtr} of the primary data tree
 * @param primarySize      number of entries in the primary data tree
 * @param dictCount        number of dictionaries
 * @param dictRoots        root {@link NodePtr} per dictionary child tree
 * @param dictNextCodes    next assignable code per dictionary
 * @param dictMaxCodes     maximum code per dictionary
 * @param dictSizes        number of entries per dictionary child tree
 * @param arenaHighWaterPage highest ChunkStore page allocated in this generation
 *                           (used for recovery: everything beyond this is garbage)
 */
public record PublicationState(
    long generation,
    long primaryRoot,
    long primarySize,
    int dictCount,
    long[] dictRoots,
    long[] dictNextCodes,
    long[] dictMaxCodes,
    long[] dictSizes,
    int arenaHighWaterPage
) {

    /** Initial empty state at generation 0. */
    public static final PublicationState EMPTY = new PublicationState(
        0L,
        NodePtr.EMPTY_PTR,
        0L,
        0,
        new long[0],
        new long[0],
        new long[0],
        new long[0],
        0
    );

    /**
     * Create a successor state with an updated primary root and size.
     * All other fields are copied. Generation is incremented.
     */
    public PublicationState withPrimary(long newRoot, long newSize, int newHighWaterPage) {
        return new PublicationState(
            generation + 1,
            newRoot,
            newSize,
            dictCount,
            dictRoots,
            dictNextCodes,
            dictMaxCodes,
            dictSizes,
            newHighWaterPage
        );
    }

    /**
     * Create a successor state with updated primary root, size, and dictionary state.
     * Generation is incremented.
     */
    public PublicationState withAll(long newRoot, long newSize,
                                   long[] newDictRoots, long[] newDictNextCodes,
                                   long[] newDictSizes, int newHighWaterPage) {
        return new PublicationState(
            generation + 1,
            newRoot,
            newSize,
            dictCount,
            newDictRoots,
            newDictNextCodes,
            dictMaxCodes,
            newDictSizes,
            newHighWaterPage
        );
    }

    /**
     * Create an initial state for a tree with the given number of dictionaries.
     */
    public static PublicationState initial(int dictCount) {
        return new PublicationState(
            0L,
            NodePtr.EMPTY_PTR,
            0L,
            dictCount,
            new long[dictCount],
            new long[dictCount],
            new long[dictCount],
            new long[dictCount],
            0
        );
    }
}
