package org.taotree.internal.art;

/**
 * Constants for ART node types — segment sizes, capacities, grow/shrink thresholds.
 *
 * <p>Instance-specific slab class IDs are held by the owning tree, not here.
 * This class contains only immutable constants.
 */
public final class NodeConstants {

    private NodeConstants() {}

    // -- Segment sizes (bytes) --

    public static final int PREFIX_SIZE = 24;   // 1B count + 15B keys + 8B child
    public static final int PREFIX_CAPACITY = 15; // max key bytes per prefix node
    public static final int NODE4_SIZE = 40;    // 1B count + 4B keys + 3B pad + 32B children
    public static final int NODE16_SIZE = 152;  // 1B count + 7B pad + 16B keys + 128B children
    public static final int NODE48_SIZE = 648;  // 1B count + 7B pad + 256B index + 48×8B children
    public static final int NODE256_SIZE = 2056; // 2B count + 6B pad + 256×8B children

    // -- Capacities --

    public static final int NODE4_CAPACITY   = 4;
    public static final int NODE16_CAPACITY  = 16;
    public static final int NODE48_CAPACITY  = 48;
    public static final int NODE256_CAPACITY = 256;

    // -- Shrink thresholds --

    public static final int NODE256_SHRINK_THRESHOLD = 36;
    public static final int NODE48_SHRINK_THRESHOLD  = 12;
    public static final int NODE16_SHRINK_THRESHOLD  = 4;
}
