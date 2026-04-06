/**
 * TaoTree — Adaptive Radix Tree on Java Foreign Function & Memory API.
 *
 * <p>All data structures live off-heap in {@link java.lang.foreign.MemorySegment}-backed
 * slab allocators. Zero GC pressure on the hot path.
 *
 * <p>Public API:
 * <ul>
 *   <li>{@code org.taotree} — core tree, dictionary, string, accessor types
 *   <li>{@code org.taotree.layout} — typed key/leaf layouts, builders, handles
 * </ul>
 *
 * <p>Internal packages ({@code org.taotree.internal.*}) are not exported.
 */
module org.taotree {
    requires java.base;

    exports org.taotree;
    exports org.taotree.layout;
    // org.taotree.internal.{art,alloc,persist,cow} are intentionally NOT exported
}
