package org.taotree.internal.cow;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LongOpenHashSetTest {

    @Test
    void addAndContains() {
        var set = new LongOpenHashSet();
        assertTrue(set.add(42));
        assertTrue(set.contains(42));
        assertFalse(set.contains(43));
    }

    @Test
    void addReturnsFalseForDuplicate() {
        var set = new LongOpenHashSet();
        assertTrue(set.add(10));
        assertFalse(set.add(10));
        assertEquals(1, set.size());
    }

    @Test
    void containsReturnsFalseForEmpty() {
        var set = new LongOpenHashSet();
        assertFalse(set.contains(99));
    }

    @Test
    void containsReturnsFalseForSentinel() {
        var set = new LongOpenHashSet();
        assertFalse(set.contains(0L));
    }

    @Test
    void addThrowsForSentinel() {
        var set = new LongOpenHashSet();
        assertThrows(IllegalArgumentException.class, () -> set.add(0L));
    }

    @Test
    void sizeTracksAdds() {
        var set = new LongOpenHashSet();
        assertEquals(0, set.size());
        set.add(1);
        assertEquals(1, set.size());
        set.add(2);
        assertEquals(2, set.size());
        set.add(1); // duplicate
        assertEquals(2, set.size());
    }

    @Test
    void clearResetsSet() {
        var set = new LongOpenHashSet();
        set.add(1);
        set.add(2);
        set.clear();
        assertEquals(0, set.size());
        assertFalse(set.contains(1));
        assertFalse(set.contains(2));
    }

    @Test
    void growHandlesCollisionsCorrectly() {
        var set = new LongOpenHashSet(4);
        // Insert enough elements to force grow (threshold = 4 * 0.75 = 3)
        for (int i = 1; i <= 20; i++) {
            assertTrue(set.add(i));
        }
        assertEquals(20, set.size());
        for (int i = 1; i <= 20; i++) {
            assertTrue(set.contains(i), "should contain " + i);
        }
        assertFalse(set.contains(21));
    }

    @Test
    void handlesLargeValues() {
        var set = new LongOpenHashSet();
        set.add(Long.MAX_VALUE);
        set.add(Long.MIN_VALUE + 1); // not 0
        set.add(-1L);
        assertTrue(set.contains(Long.MAX_VALUE));
        assertTrue(set.contains(Long.MIN_VALUE + 1));
        assertTrue(set.contains(-1L));
        assertFalse(set.contains(1L));
    }

    @Test
    void customInitialCapacityRoundsUp() {
        var set = new LongOpenHashSet(3);
        // Should round to power of two >= 16 (MIN_CAPACITY)
        for (int i = 1; i <= 100; i++) {
            set.add(i);
        }
        assertEquals(100, set.size());
    }

    @Test
    void addAfterClear() {
        var set = new LongOpenHashSet();
        set.add(42);
        set.clear();
        assertTrue(set.add(42));
        assertEquals(1, set.size());
    }

    @Test
    void manyCollisions() {
        // Force values that hash to same bucket by using multiples of table size
        var set = new LongOpenHashSet(16);
        for (long i = 1; i <= 50; i++) {
            set.add(i * 17); // arbitrary multiplier
        }
        assertEquals(50, set.size());
        for (long i = 1; i <= 50; i++) {
            assertTrue(set.contains(i * 17));
        }
    }
}
