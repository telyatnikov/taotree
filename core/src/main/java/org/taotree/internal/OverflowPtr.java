package org.taotree.internal;

/**
 * Encodes and decodes 64-bit swizzled pointers into the {@link BumpAllocator}.
 *
 * <p>Layout:
 * <pre>
 *  63      56 55                  32 31                       0
 * ┌──────────┬─────────────────────┬──────────────────────────┐
 * │ reserved │  pageId (24 bits)   │  offset (32 bits)        │
 * │  8 bits  │                     │                          │
 * └──────────┴─────────────────────┴──────────────────────────┘
 * </pre>
 *
 * <p>All methods are static, pure bitwise arithmetic, zero allocation.
 */
public final class OverflowPtr {

    private OverflowPtr() {}

    public static final long EMPTY_PTR = 0L;

    public static long pack(int pageId, int offset) {
        return ((long) (pageId & 0x00FF_FFFF) << 32)
             | (offset & 0xFFFF_FFFFL);
    }

    public static int pageId(long ptr) {
        return (int) (ptr >>> 32) & 0x00FF_FFFF;
    }

    public static int offset(long ptr) {
        return (int) (ptr & 0xFFFF_FFFFL);
    }

    public static boolean isEmpty(long ptr) {
        return ptr == 0L;
    }

    public static String toString(long ptr) {
        if (isEmpty(ptr)) return "EMPTY";
        return "OverflowPtr[page=" + pageId(ptr) + ",off=" + offset(ptr) + "]";
    }
}
