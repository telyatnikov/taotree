package org.taotree.internal;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OverflowPtrTest {

    @Test
    void emptyPtr() {
        assertTrue(OverflowPtr.isEmpty(OverflowPtr.EMPTY_PTR));
        assertEquals(0, OverflowPtr.pageId(OverflowPtr.EMPTY_PTR));
        assertEquals(0, OverflowPtr.offset(OverflowPtr.EMPTY_PTR));
    }

    @Test
    void packAndUnpack() {
        long ptr = OverflowPtr.pack(42, 1024);
        assertEquals(42, OverflowPtr.pageId(ptr));
        assertEquals(1024, OverflowPtr.offset(ptr));
        assertFalse(OverflowPtr.isEmpty(ptr));
    }

    @Test
    void maxValues() {
        long ptr = OverflowPtr.pack(0x00FF_FFFF, 0xFFFF_FFFF);
        assertEquals(0x00FF_FFFF, OverflowPtr.pageId(ptr));
        assertEquals(0xFFFF_FFFF, OverflowPtr.offset(ptr));
    }

    @Test
    void zeroPageNonZeroOffset() {
        long ptr = OverflowPtr.pack(0, 500);
        assertFalse(OverflowPtr.isEmpty(ptr));
        assertEquals(0, OverflowPtr.pageId(ptr));
        assertEquals(500, OverflowPtr.offset(ptr));
    }

    @Test
    void toStringReadable() {
        assertEquals("EMPTY", OverflowPtr.toString(OverflowPtr.EMPTY_PTR));
        String s = OverflowPtr.toString(OverflowPtr.pack(3, 100));
        assertTrue(s.contains("page=3"));
        assertTrue(s.contains("off=100"));
    }
}
