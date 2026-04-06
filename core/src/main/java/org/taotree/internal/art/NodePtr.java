package org.taotree.internal.art;

import org.taotree.TaoTree;
/**
 * Encodes and decodes 64-bit tagged, swizzled pointers used throughout TaoTree.
 *
 * <p>Layout:
 * <pre>
 *  63      56 55      48 47      32 31                       0
 * ┌──────────┬──────────┬──────────┬──────────────────────────┐
 * │ metadata │ slabClass│  slabId  │        offset            │
 * │  8 bits  │  8 bits  │ 16 bits  │       32 bits            │
 * └──────────┴──────────┴──────────┴──────────────────────────┘
 * </pre>
 *
 * <p>Metadata byte:
 * <pre>
 *  bit 7:    gate flag (reserved)
 *  bits 6-4: reserved
 *  bits 3-0: node type tag
 * </pre>
 *
 * <p>All methods are static, pure bitwise arithmetic, zero allocation.
 */
public final class NodePtr {

    private NodePtr() {}

    // -- Node type tags --

    public static final int EMPTY       = 0;
    public static final int PREFIX      = 1;
    public static final int NODE_4      = 2;
    public static final int NODE_16     = 3;
    public static final int NODE_48     = 4;
    public static final int NODE_256    = 5;
    public static final int LEAF        = 6;
    public static final int LEAF_INLINE = 7;

    /** The null / absent pointer. Metadata and payload are all zeros. */
    public static final long EMPTY_PTR = 0L;

    // -- Pack --

    /**
     * Pack a full pointer from its components.
     *
     * @param nodeType    node type tag (0-7), stored in the low 4 bits of metadata
     * @param slabClassId slab class index (0-255)
     * @param slabId      slab index within the class (0-65535)
     * @param offset      segment offset within the slab (0 - 2^32-1)
     */
    public static long pack(int nodeType, int slabClassId, int slabId, int offset) {
        return ((long) (nodeType & 0x0F) << 56)
             | ((long) (slabClassId & 0xFF) << 48)
             | ((long) (slabId & 0xFFFF) << 32)
             | (offset & 0xFFFF_FFFFL);
    }

    /**
     * Pack a pointer with full metadata byte (including gate flag and reserved bits).
     */
    public static long packWithMetadata(int metadata, int slabClassId, int slabId, int offset) {
        return ((long) (metadata & 0xFF) << 56)
             | ((long) (slabClassId & 0xFF) << 48)
             | ((long) (slabId & 0xFFFF) << 32)
             | (offset & 0xFFFF_FFFFL);
    }

    // -- Unpack --

    /** Full metadata byte (bits 63-56). */
    public static int metadata(long ptr) {
        return (int) (ptr >>> 56) & 0xFF;
    }

    /** Node type tag (bits 59-56, the low 4 bits of metadata). */
    public static int nodeType(long ptr) {
        return (int) (ptr >>> 56) & 0x0F;
    }

    /** Slab class ID (bits 55-48). */
    public static int slabClassId(long ptr) {
        return (int) (ptr >>> 48) & 0xFF;
    }

    /** Slab ID within the class (bits 47-32). */
    public static int slabId(long ptr) {
        return (int) (ptr >>> 32) & 0xFFFF;
    }

    /** Segment offset within the slab (bits 31-0). */
    public static int offset(long ptr) {
        return (int) (ptr & 0xFFFF_FFFFL);
    }

    // -- Predicates --

    public static boolean isEmpty(long ptr) {
        return ptr == 0L;
    }

    public static boolean isLeaf(long ptr) {
        int type = nodeType(ptr);
        return type == LEAF || type == LEAF_INLINE;
    }

    public static boolean isInnerNode(long ptr) {
        int type = nodeType(ptr);
        return type >= NODE_4 && type <= NODE_256;
    }

    public static boolean isPrefix(long ptr) {
        return nodeType(ptr) == PREFIX;
    }

    // -- Mutate metadata --

    /** Return a new pointer with a different node type, keeping the payload unchanged. */
    public static long withNodeType(long ptr, int newNodeType) {
        long cleared = ptr & ~(0x0FL << 56);
        return cleared | ((long) (newNodeType & 0x0F) << 56);
    }

    // -- Debug --

    public static String toString(long ptr) {
        if (isEmpty(ptr)) return "EMPTY";
        return switch (nodeType(ptr)) {
            case EMPTY       -> "EMPTY";
            case PREFIX      -> "PREFIX";
            case NODE_4      -> "NODE_4";
            case NODE_16     -> "NODE_16";
            case NODE_48     -> "NODE_48";
            case NODE_256    -> "NODE_256";
            case LEAF        -> "LEAF";
            case LEAF_INLINE -> "LEAF_INLINE";
            default          -> "UNKNOWN(" + nodeType(ptr) + ")";
        } + "[class=" + slabClassId(ptr)
          + ",slab=" + slabId(ptr)
          + ",off=" + Integer.toUnsignedString(offset(ptr))
          + "]";
    }
}
