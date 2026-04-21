package org.taotree.internal.cow;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LongListTest {

    @Test
    void newListIsEmpty() {
        var list = new LongList();
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
    }

    @Test
    void addAndGet() {
        var list = new LongList();
        list.add(42L);
        assertEquals(1, list.size());
        assertFalse(list.isEmpty());
        assertEquals(42L, list.get(0));
    }

    @Test
    void addMultiple() {
        var list = new LongList();
        for (long i = 0; i < 5; i++) list.add(i * 10);
        assertEquals(5, list.size());
        assertEquals(0L, list.get(0));
        assertEquals(40L, list.get(4));
    }

    @Test
    void growBeyondInitialCapacity() {
        var list = new LongList(2);
        for (int i = 0; i < 100; i++) list.add(i);
        assertEquals(100, list.size());
        for (int i = 0; i < 100; i++) {
            assertEquals(i, list.get(i));
        }
    }

    @Test
    void addAllFromOtherList() {
        var a = new LongList();
        a.add(1);
        a.add(2);
        var b = new LongList();
        b.add(3);
        b.add(4);
        a.addAll(b);
        assertEquals(4, a.size());
        assertEquals(1L, a.get(0));
        assertEquals(4L, a.get(3));
    }

    @Test
    void addAllFromEmpty() {
        var a = new LongList();
        a.add(1);
        a.addAll(new LongList());
        assertEquals(1, a.size());
    }

    @Test
    void addAllTriggersGrow() {
        var a = new LongList(2);
        a.add(1);
        var b = new LongList();
        for (int i = 0; i < 50; i++) b.add(i + 100);
        a.addAll(b);
        assertEquals(51, a.size());
        assertEquals(1L, a.get(0));
        assertEquals(149L, a.get(50));
    }

    @Test
    void clearResetsSizeButAllowsReuse() {
        var list = new LongList();
        list.add(1);
        list.add(2);
        list.clear();
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
        list.add(3);
        assertEquals(1, list.size());
        assertEquals(3L, list.get(0));
    }

    @Test
    void emptySingleton() {
        var e = LongList.empty();
        assertTrue(e.isEmpty());
        assertEquals(0, e.size());
    }

    @Test
    void defaultCapacity() {
        var list = new LongList();
        // Should handle 8+ elements without issue
        for (int i = 0; i < 20; i++) list.add(i);
        assertEquals(20, list.size());
    }
}
