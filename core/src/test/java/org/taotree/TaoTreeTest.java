package org.taotree;

import org.taotree.internal.NodePtr;

import org.junit.jupiter.api.Test;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TaoTreeTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    private byte[] intKey(int value) {
        byte[] key = new byte[KEY_LEN];
        key[0] = (byte) (value >>> 24);
        key[1] = (byte) (value >>> 16);
        key[2] = (byte) (value >>> 8);
        key[3] = (byte) value;
        return key;
    }

    // ---- Basic operations ----

    @Test
    void emptyTree() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var r = tree.read()) {
                assertTrue(r.isEmpty());
                assertEquals(0, r.size());
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(42)));
            }
        }
    }

    @Test
    void insertSingleKey() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(42), 0);
                assertNotEquals(TaoTree.NOT_FOUND, leaf);
                assertEquals(1, w.size());
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 100L);
            }
            try (var r = tree.read()) {
                long found = r.lookup(intKey(42));
                assertNotEquals(TaoTree.NOT_FOUND, found);
                assertEquals(100L, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void insertDuplicateReturnsSameLeaf() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                long leaf1 = w.getOrCreate(intKey(42), 0);
                long leaf2 = w.getOrCreate(intKey(42), 0);
                assertEquals(leaf1, leaf2);
                assertEquals(1, w.size());
            }
        }
    }

    @Test
    void lookupMissingKey() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                w.getOrCreate(intKey(42), 0);
            }
            try (var r = tree.read()) {
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(99)));
            }
        }
    }

    @Test
    void insertTwoKeys() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                long leaf1 = w.getOrCreate(intKey(10), 0);
                long leaf2 = w.getOrCreate(intKey(20), 0);
                assertNotEquals(leaf1, leaf2);
                assertEquals(2, w.size());
                w.leafValue(leaf1).set(ValueLayout.JAVA_LONG, 0, 100L);
                w.leafValue(leaf2).set(ValueLayout.JAVA_LONG, 0, 200L);
            }
            try (var r = tree.read()) {
                assertEquals(100L, r.leafValue(r.lookup(intKey(10))).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(200L, r.leafValue(r.lookup(intKey(20))).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void insertKeysWithSharedPrefix() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            byte[] k1 = {0x0A, 0x00, 0x00, 0x01};
            byte[] k2 = {0x0A, 0x00, 0x00, 0x02};

            try (var w = tree.write()) {
                long leaf1 = w.getOrCreate(k1, 0);
                long leaf2 = w.getOrCreate(k2, 0);
                assertNotEquals(leaf1, leaf2);
                assertEquals(2, w.size());
            }
            try (var r = tree.read()) {
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(k1));
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(k2));
            }
        }
    }

    // ---- Scale: force node growth ----

    @Test
    void insertFiveKeysGrowsToNode16() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 5; i++) {
                    byte[] key = {(byte) i, 0, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, i * 10L);
                }
                assertEquals(5, w.size());
            }
            try (var r = tree.read()) {
                for (int i = 0; i < 5; i++) {
                    byte[] key = {(byte) i, 0, 0, 0};
                    long leaf = r.lookup(key);
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " not found");
                    assertEquals(i * 10L, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void insert17KeysGrowsToNode48() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 17; i++) {
                    byte[] key = {(byte) (i * 15), 0, 0, 0};
                    w.getOrCreate(key, 0);
                }
                assertEquals(17, w.size());
            }
            try (var r = tree.read()) {
                for (int i = 0; i < 17; i++) {
                    byte[] key = {(byte) (i * 15), 0, 0, 0};
                    assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key), "Key " + i + " not found");
                }
            }
        }
    }

    @Test
    void insert49KeysGrowsToNode256() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 49; i++) {
                    byte[] key = {(byte) (i * 5), 0, 0, 0};
                    w.getOrCreate(key, 0);
                }
                assertEquals(49, w.size());
            }
            try (var r = tree.read()) {
                for (int i = 0; i < 49; i++) {
                    byte[] key = {(byte) (i * 5), 0, 0, 0};
                    assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key), "Key " + i + " not found");
                }
            }
        }
    }

    // ---- Many keys ----

    @Test
    void insertManySequentialKeys() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            int N = 1000;
            try (var w = tree.write()) {
                for (int i = 0; i < N; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(N, w.size());
            }
            try (var r = tree.read()) {
                for (int i = 0; i < N; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " not found");
                    assertEquals(i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(N)));
            }
        }
    }

    @Test
    void insertManyRandomKeys() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            var rng = new Random(42);
            int N = 5000;
            Set<Integer> inserted = new HashSet<>();
            try (var w = tree.write()) {
                for (int i = 0; i < N; i++) {
                    int v = rng.nextInt(100_000);
                    inserted.add(v);
                    w.getOrCreate(intKey(v), 0);
                }
                assertEquals(inserted.size(), w.size());
            }
            try (var r = tree.read()) {
                for (int v : inserted) {
                    assertNotEquals(TaoTree.NOT_FOUND, r.lookup(intKey(v)), "Key " + v + " not found");
                }
            }
        }
    }

    // ---- Delete ----

    @Test
    void deleteSingleKey() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                w.getOrCreate(intKey(42), 0);
                assertTrue(w.delete(intKey(42)));
                assertEquals(0, w.size());
                assertTrue(w.isEmpty());
                assertEquals(TaoTree.NOT_FOUND, w.lookup(intKey(42)));
            }
        }
    }

    @Test
    void deleteNonExistent() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                w.getOrCreate(intKey(42), 0);
                assertFalse(w.delete(intKey(99)));
                assertEquals(1, w.size());
            }
        }
    }

    @Test
    void deleteFromEmpty() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                assertFalse(w.delete(intKey(42)));
            }
        }
    }

    @Test
    void insertDeleteInsert() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                long leaf1 = w.getOrCreate(intKey(42), 0);
                w.leafValue(leaf1).set(ValueLayout.JAVA_LONG, 0, 100L);
                assertTrue(w.delete(intKey(42)));
                assertEquals(0, w.size());
                long leaf2 = w.getOrCreate(intKey(42), 0);
                w.leafValue(leaf2).set(ValueLayout.JAVA_LONG, 0, 200L);
                assertEquals(1, w.size());
                assertEquals(200L, w.leafValue(w.lookup(intKey(42))).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void deleteOneOfTwo() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                w.getOrCreate(intKey(10), 0);
                long leaf2 = w.getOrCreate(intKey(20), 0);
                w.leafValue(leaf2).set(ValueLayout.JAVA_LONG, 0, 200L);
                assertTrue(w.delete(intKey(10)));
                assertEquals(1, w.size());
                assertEquals(TaoTree.NOT_FOUND, w.lookup(intKey(10)));
                assertEquals(200L, w.leafValue(w.lookup(intKey(20))).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void deleteAllKeys() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            int N = 100;
            try (var w = tree.write()) {
                for (int i = 0; i < N; i++) w.getOrCreate(intKey(i), 0);
                assertEquals(N, w.size());
                for (int i = 0; i < N; i++) assertTrue(w.delete(intKey(i)));
                assertEquals(0, w.size());
                assertTrue(w.isEmpty());
                for (int i = 0; i < N; i++) assertEquals(TaoTree.NOT_FOUND, w.lookup(intKey(i)));
            }
        }
    }

    @Test
    void deleteRandomSubset() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            int N = 500;
            var rng = new Random(123);
            try (var w = tree.write()) {
                for (int i = 0; i < N; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }

                Set<Integer> deleted = new HashSet<>();
                for (int i = 0; i < N / 2; i++) {
                    int v = rng.nextInt(N);
                    if (deleted.add(v)) assertTrue(w.delete(intKey(v)));
                }
                assertEquals(N - deleted.size(), w.size());

                for (int i = 0; i < N; i++) {
                    long leaf = w.lookup(intKey(i));
                    if (deleted.contains(i)) {
                        assertEquals(TaoTree.NOT_FOUND, leaf);
                    } else {
                        assertNotEquals(TaoTree.NOT_FOUND, leaf);
                        assertEquals(i, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                    }
                }
            }
        }
    }

    // ---- 16-byte keys (GBIF-like) ----

    @Test
    void sixteenByteKeys() {
        try (var tree = TaoTree.open(16, new int[]{24})) {

            byte[] k1 = new byte[16]; byte[] k2 = new byte[16]; byte[] k3 = new byte[16];
            k1[1] = 1; k1[3] = 2; k1[5] = 10; k1[9] = 1; k1[11] = 1; k1[13] = 1; k1[15] = 1;
            System.arraycopy(k1, 0, k2, 0, 16); k2[9] = 2;
            System.arraycopy(k1, 0, k3, 0, 16); k3[11] = 2;

            try (var w = tree.write()) {
                w.getOrCreate(k1, 0); w.getOrCreate(k2, 0); w.getOrCreate(k3, 0);
                assertEquals(3, w.size());
                assertTrue(w.delete(k2));
                assertEquals(2, w.size());
                assertEquals(TaoTree.NOT_FOUND, w.lookup(k2));
                assertNotEquals(TaoTree.NOT_FOUND, w.lookup(k1));
                assertNotEquals(TaoTree.NOT_FOUND, w.lookup(k3));
            }
        }
    }

    // ---- Multiple leaf classes ----

    @Test
    void multipleLeafClasses() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{8, 24})) {
            try (var w = tree.write()) {
                long leaf1 = w.getOrCreate(intKey(1), 0);
                long leaf2 = w.getOrCreate(intKey(2), 1);
                w.leafValue(leaf1).set(ValueLayout.JAVA_LONG, 0, 111L);
                w.leafValue(leaf2).set(ValueLayout.JAVA_LONG, 0, 222L);
                w.leafValue(leaf2).set(ValueLayout.JAVA_LONG, 8, 333L);
            }
            try (var r = tree.read()) {
                assertEquals(111L, r.leafValue(r.lookup(intKey(1))).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(222L, r.leafValue(r.lookup(intKey(2))).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(333L, r.leafValue(r.lookup(intKey(2))).get(ValueLayout.JAVA_LONG, 8));
            }
        }
    }

    // ---- Validation ----

    @Test
    void rejectWrongKeyLength() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                assertThrows(IllegalArgumentException.class,
                    () -> w.getOrCreate(new byte[]{1, 2, 3}, 0));
                assertThrows(IllegalArgumentException.class,
                    () -> w.getOrCreate(new byte[]{1, 2, 3, 4, 5}, 0));
            }
            try (var r = tree.read()) {
                assertThrows(IllegalArgumentException.class,
                    () -> r.lookup(new byte[]{1, 2}));
            }
        }
    }

    @Test
    void rejectInvalidLeafClass() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                assertThrows(IllegalArgumentException.class,
                    () -> w.getOrCreate(intKey(1), -1));
                assertThrows(IllegalArgumentException.class,
                    () -> w.getOrCreate(intKey(1), 1)); // only class 0 exists
            }
        }
    }

    @Test
    void newLeafValueIsZeroed() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1), 0);
                MemorySegment val = w.leafValue(leaf);
                // All value bytes should be zero on a freshly created leaf
                assertEquals(0L, val.get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void reusedLeafValueIsZeroed() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                // Insert and set a value
                long leaf = w.getOrCreate(intKey(1), 0);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 0xDEAD_BEEFL);
                // Delete
                w.delete(intKey(1));
                // Re-insert — slab may reuse the same slot
                long leaf2 = w.getOrCreate(intKey(1), 0);
                // Value must be zeroed regardless of reuse
                assertEquals(0L, w.leafValue(leaf2).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void closedScopeThrows() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            var read = tree.read();
            read.close();
            assertThrows(IllegalStateException.class, () -> read.lookup(intKey(1)));

            var write = tree.write();
            write.close();
            assertThrows(IllegalStateException.class, () -> write.getOrCreate(intKey(1), 0));
        }
    }

    @Test
    void readScopeLeafValueIsReadOnly() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1), 0);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
            }
            try (var r = tree.read()) {
                long leaf = r.lookup(intKey(1));
                MemorySegment seg = r.leafValue(leaf);
                // Reading works
                assertEquals(42L, seg.get(ValueLayout.JAVA_LONG, 0));
                // Writing throws (FFM read-only segments throw IllegalArgumentException)
                assertThrows(IllegalArgumentException.class,
                    () -> seg.set(ValueLayout.JAVA_LONG, 0, 99L));
            }
        }
    }

    @Test
    void leafValueRejectsEmptyPtr() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var r = tree.read()) {
                assertThrows(IllegalArgumentException.class,
                    () -> r.leafValue(TaoTree.NOT_FOUND));
            }
        }
    }

    @Test
    void leafValueRejectsNonLeafPtr() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            // Fabricate a non-leaf pointer
            long fakePtr = NodePtr.pack(NodePtr.NODE_4, 0, 0, 0);
            try (var r = tree.read()) {
                assertThrows(IllegalArgumentException.class,
                    () -> r.leafValue(fakePtr));
            }
        }
    }
}
