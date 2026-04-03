/**
 * TaoTree — Adaptive Radix Tree on Java Foreign Function & Memory API.
 *
 * <p>All data structures live off-heap in {@link java.lang.foreign.MemorySegment}-backed
 * slab allocators. Zero GC pressure on the hot path.
 *
 * <p>Public API is in {@code org.taotree}. Implementation internals are in
 * {@code org.taotree.internal} (not exported).
 */
module org.taotree {
    requires java.base;

    exports org.taotree;
    // org.taotree.internal is intentionally NOT exported
}
