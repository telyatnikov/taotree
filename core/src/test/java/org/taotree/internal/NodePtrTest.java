package org.taotree.internal;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

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
}
