package org.taotree.internal.art;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.art.NodePtr;

class NodePtrTest {

    @Test
    void emptyPtr() {
        assertEquals(0L, NodePtr.EMPTY_PTR);
        assertTrue(NodePtr.isEmpty(NodePtr.EMPTY_PTR));
        assertEquals(NodePtr.EMPTY, NodePtr.nodeType(NodePtr.EMPTY_PTR));
    }

    @Test
    void packAndUnpack() {
        long ptr = NodePtr.pack(NodePtr.LEAF, 5, 42, 1024);
        assertEquals(NodePtr.LEAF, NodePtr.nodeType(ptr));
        assertEquals(5, NodePtr.slabClassId(ptr));
        assertEquals(42, NodePtr.slabId(ptr));
        assertEquals(1024, NodePtr.offset(ptr));
        assertFalse(NodePtr.isEmpty(ptr));
        assertTrue(NodePtr.isLeaf(ptr));
        assertFalse(NodePtr.isInnerNode(ptr));
    }

    @Test
    void packAllNodeTypes() {
        for (int type = 0; type <= 7; type++) {
            long ptr = NodePtr.pack(type, 0, 0, 0);
            assertEquals(type, NodePtr.nodeType(ptr));
        }
    }

    @Test
    void innerNodePredicates() {
        assertTrue(NodePtr.isInnerNode(NodePtr.pack(NodePtr.NODE_4, 0, 0, 0)));
        assertTrue(NodePtr.isInnerNode(NodePtr.pack(NodePtr.NODE_16, 0, 0, 0)));
        assertTrue(NodePtr.isInnerNode(NodePtr.pack(NodePtr.NODE_48, 0, 0, 0)));
        assertTrue(NodePtr.isInnerNode(NodePtr.pack(NodePtr.NODE_256, 0, 0, 0)));
        assertFalse(NodePtr.isInnerNode(NodePtr.pack(NodePtr.LEAF, 0, 0, 0)));
        assertFalse(NodePtr.isInnerNode(NodePtr.pack(NodePtr.PREFIX, 0, 0, 0)));
    }

    @Test
    void prefixPredicate() {
        assertTrue(NodePtr.isPrefix(NodePtr.pack(NodePtr.PREFIX, 0, 0, 0)));
        assertFalse(NodePtr.isPrefix(NodePtr.pack(NodePtr.NODE_4, 0, 0, 0)));
    }

    @Test
    void leafInline() {
        long ptr = NodePtr.pack(NodePtr.LEAF_INLINE, 0, 0, 0);
        assertTrue(NodePtr.isLeaf(ptr));
        assertFalse(NodePtr.isInnerNode(ptr));
        assertEquals(NodePtr.LEAF_INLINE, NodePtr.nodeType(ptr));
    }

    @Test
    void maxValues() {
        long ptr = NodePtr.pack(0x0F, 0xFF, 0xFFFF, 0xFFFFFFFF);
        assertEquals(0x0F, NodePtr.nodeType(ptr));
        assertEquals(0xFF, NodePtr.slabClassId(ptr));
        assertEquals(0xFFFF, NodePtr.slabId(ptr));
        assertEquals(0xFFFFFFFF, NodePtr.offset(ptr));
    }

    @Test
    void withNodeTypePreservesPayload() {
        long ptr = NodePtr.pack(NodePtr.NODE_4, 7, 100, 5000);
        long changed = NodePtr.withNodeType(ptr, NodePtr.NODE_16);
        assertEquals(NodePtr.NODE_16, NodePtr.nodeType(changed));
        assertEquals(7, NodePtr.slabClassId(changed));
        assertEquals(100, NodePtr.slabId(changed));
        assertEquals(5000, NodePtr.offset(changed));
    }

    @Test
    void packWithMetadata() {
        // Set gate flag (bit 7) + node type 6
        int meta = 0x86; // 1000_0110
        long ptr = NodePtr.packWithMetadata(meta, 3, 10, 200);
        assertEquals(meta, NodePtr.metadata(ptr));
        assertEquals(NodePtr.LEAF, NodePtr.nodeType(ptr)); // low 4 bits = 6
        assertEquals(3, NodePtr.slabClassId(ptr));
        assertEquals(10, NodePtr.slabId(ptr));
        assertEquals(200, NodePtr.offset(ptr));
    }

    @Test
    void toStringReadable() {
        assertEquals("EMPTY", NodePtr.toString(NodePtr.EMPTY_PTR));
        String s = NodePtr.toString(NodePtr.pack(NodePtr.LEAF, 5, 42, 1024));
        assertTrue(s.startsWith("LEAF["));
        assertTrue(s.contains("class=5"));
        assertTrue(s.contains("slab=42"));
    }

    // ---- STRONGER: isLeaf must differentiate node types ----

    @Test
    void isLeafReturnsFalseForNonLeafTypes() {
        long node4Ptr = NodePtr.pack(NodePtr.NODE_4, 0, 0, 0);
        long node16Ptr = NodePtr.pack(NodePtr.NODE_16, 0, 0, 0);
        long node48Ptr = NodePtr.pack(NodePtr.NODE_48, 0, 0, 0);
        long node256Ptr = NodePtr.pack(NodePtr.NODE_256, 0, 0, 0);
        long prefixPtr = NodePtr.pack(NodePtr.PREFIX, 0, 0, 0);

        assertFalse(NodePtr.isLeaf(node4Ptr));
        assertFalse(NodePtr.isLeaf(node16Ptr));
        assertFalse(NodePtr.isLeaf(node48Ptr));
        assertFalse(NodePtr.isLeaf(node256Ptr));
        assertFalse(NodePtr.isLeaf(prefixPtr));
    }

    @Test
    void isLeafReturnsTrueForLeafTypes() {
        long leafPtr = NodePtr.pack(NodePtr.LEAF, 0, 0, 0);
        long leafInlinePtr = NodePtr.pack(NodePtr.LEAF_INLINE, 0, 0, 0);

        assertTrue(NodePtr.isLeaf(leafPtr));
        assertTrue(NodePtr.isLeaf(leafInlinePtr));
    }

    // ---- toString coverage for all node types ----

    @Test
    void toStringAllNodeTypes() {
        // Covers every branch of the toString switch statement
        String prefix = NodePtr.toString(NodePtr.pack(NodePtr.PREFIX, 1, 2, 3));
        assertTrue(prefix.startsWith("PREFIX["));

        String n4 = NodePtr.toString(NodePtr.pack(NodePtr.NODE_4, 1, 2, 3));
        assertTrue(n4.startsWith("NODE_4["));

        String n16 = NodePtr.toString(NodePtr.pack(NodePtr.NODE_16, 1, 2, 3));
        assertTrue(n16.startsWith("NODE_16["));

        String n48 = NodePtr.toString(NodePtr.pack(NodePtr.NODE_48, 1, 2, 3));
        assertTrue(n48.startsWith("NODE_48["));

        String n256 = NodePtr.toString(NodePtr.pack(NodePtr.NODE_256, 1, 2, 3));
        assertTrue(n256.startsWith("NODE_256["));

        String leaf = NodePtr.toString(NodePtr.pack(NodePtr.LEAF, 1, 2, 3));
        assertTrue(leaf.startsWith("LEAF["));

        String leafInline = NodePtr.toString(NodePtr.pack(NodePtr.LEAF_INLINE, 1, 2, 3));
        assertTrue(leafInline.startsWith("LEAF_INLINE["));

        // EMPTY type but non-zero payload (theoretical edge case)
        long emptyType = NodePtr.pack(NodePtr.EMPTY, 1, 2, 3);
        String emptyNonZero = NodePtr.toString(emptyType);
        assertTrue(emptyNonZero.startsWith("EMPTY["));
    }

    @Test
    void toStringUnknownType() {
        // Craft a pointer with a type > 7 using packWithMetadata
        // Metadata byte = 0x08 → nodeType = 8 (UNKNOWN)
        long ptr = NodePtr.packWithMetadata(0x08, 1, 2, 3);
        String s = NodePtr.toString(ptr);
        assertTrue(s.startsWith("UNKNOWN(8)"));
    }

    // ---- withNodeType preserves high metadata bits ----

    @Test
    void withNodeTypePreservesGateFlag() {
        // Set gate flag (bit 7) in metadata using packWithMetadata
        int meta = 0x86; // gate=1, reserved=000, type=6 (LEAF)
        long ptr = NodePtr.packWithMetadata(meta, 3, 10, 200);
        assertEquals(NodePtr.LEAF, NodePtr.nodeType(ptr));

        // Change type to NODE_4
        long changed = NodePtr.withNodeType(ptr, NodePtr.NODE_4);
        assertEquals(NodePtr.NODE_4, NodePtr.nodeType(changed));
        // Gate flag (bit 7 of metadata) should be preserved
        int newMeta = NodePtr.metadata(changed);
        assertTrue((newMeta & 0x80) != 0, "Gate flag should be preserved");
        // Payload should be preserved
        assertEquals(3, NodePtr.slabClassId(changed));
        assertEquals(10, NodePtr.slabId(changed));
        assertEquals(200, NodePtr.offset(changed));
    }

    // ---- isInnerNode boundary cases ----

    @Test
    void isInnerNodeBoundaries() {
        // EMPTY (0) is not inner
        assertFalse(NodePtr.isInnerNode(NodePtr.pack(NodePtr.EMPTY, 0, 0, 0)));
        // PREFIX (1) is not inner
        assertFalse(NodePtr.isInnerNode(NodePtr.pack(NodePtr.PREFIX, 0, 0, 0)));
        // NODE_4 (2) is inner — lower bound
        assertTrue(NodePtr.isInnerNode(NodePtr.pack(NodePtr.NODE_4, 0, 0, 0)));
        // NODE_256 (5) is inner — upper bound
        assertTrue(NodePtr.isInnerNode(NodePtr.pack(NodePtr.NODE_256, 0, 0, 0)));
        // LEAF (6) is not inner
        assertFalse(NodePtr.isInnerNode(NodePtr.pack(NodePtr.LEAF, 0, 0, 0)));
        // LEAF_INLINE (7) is not inner
        assertFalse(NodePtr.isInnerNode(NodePtr.pack(NodePtr.LEAF_INLINE, 0, 0, 0)));
    }

    // ---- isPrefix negative cases ----

    @Test
    void isPrefixNegativeCases() {
        assertFalse(NodePtr.isPrefix(NodePtr.EMPTY_PTR));
        assertFalse(NodePtr.isPrefix(NodePtr.pack(NodePtr.NODE_4, 0, 0, 0)));
        assertFalse(NodePtr.isPrefix(NodePtr.pack(NodePtr.LEAF, 0, 0, 0)));
    }

    // ---- Pack round-trip with extreme values ----

    @Test
    void packRoundTripLargeOffset() {
        // Large unsigned offset
        long ptr = NodePtr.pack(NodePtr.LEAF, 255, 65535, -1);
        assertEquals(NodePtr.LEAF, NodePtr.nodeType(ptr));
        assertEquals(255, NodePtr.slabClassId(ptr));
        assertEquals(65535, NodePtr.slabId(ptr));
        assertEquals(-1, NodePtr.offset(ptr)); // -1 as int = 0xFFFFFFFF
    }
}
