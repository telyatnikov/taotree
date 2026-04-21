package org.taotree.internal.champ;

/**
 * Visitor for iterating CHAMP map entries.
 *
 * @see ChampMap#iterate(org.taotree.internal.alloc.BumpAllocator, long, ChampVisitor)
 */
@FunctionalInterface
public interface ChampVisitor {
    /**
     * Visit a single entry in the CHAMP map.
     *
     * @param attrId   the attribute ID (uint32 dictionary code)
     * @param valueRef the value reference (pointer into BumpAllocator)
     * @return {@code true} to continue iteration, {@code false} to stop early
     */
    boolean visit(int attrId, long valueRef);
}
