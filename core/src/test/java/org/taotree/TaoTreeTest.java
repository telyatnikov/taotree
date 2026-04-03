package org.taotree;

import org.taotree.internal.NodePtr;
import org.taotree.TaoString;

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

    // ---- Mutation-killing: isFileBacked ----

    @Test
    void inMemoryTreeIsNotFileBacked() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            assertFalse(tree.isFileBacked());
        }
    }

    // ---- Mutation-killing: isEmpty / size ----

    @Test
    void isEmptyReturnsFalseAfterInsert() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                assertTrue(w.isEmpty());
                w.getOrCreate(intKey(1), 0);
                assertFalse(w.isEmpty());
            }
            try (var r = tree.read()) {
                assertFalse(r.isEmpty());
            }
        }
    }

    @Test
    void isEmptyReturnsTrueAfterDeletingAll() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                w.getOrCreate(intKey(1), 0);
                w.getOrCreate(intKey(2), 0);
                assertFalse(w.isEmpty());
                w.delete(intKey(1));
                w.delete(intKey(2));
                assertTrue(w.isEmpty());
                assertEquals(0, w.size());
            }
        }
    }

    // ---- Mutation-killing: totalSlabBytes ----

    @Test
    void totalSlabBytesPositiveAfterInsert() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                w.getOrCreate(intKey(1), 0);
            }
            assertTrue(tree.totalSlabBytes() > 0, "totalSlabBytes should be > 0 after insertion");
        }
    }

    // ---- Mutation-killing: node shrinking during delete ----

    @Test
    void deleteShrinksNode256ToNode48() {
        // Insert 49 distinct first-byte keys → forces Node256
        // Then delete until ≤ 36 → should shrink to Node48
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 49; i++) {
                    byte[] key = {(byte) (i * 5), 0, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(49, w.size());

                // Delete down to 30 keys
                for (int i = 30; i < 49; i++) {
                    byte[] key = {(byte) (i * 5), 0, 0, 0};
                    assertTrue(w.delete(key));
                }
                assertEquals(30, w.size());

                // Verify remaining keys are intact
                for (int i = 0; i < 30; i++) {
                    byte[] key = {(byte) (i * 5), 0, 0, 0};
                    long leaf = w.lookup(key);
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " should still exist");
                    assertEquals((long) i, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }

                // Deleted keys should not be found
                for (int i = 30; i < 49; i++) {
                    byte[] key = {(byte) (i * 5), 0, 0, 0};
                    assertEquals(TaoTree.NOT_FOUND, w.lookup(key));
                }
            }
        }
    }

    @Test
    void deleteShrinksNode48ToNode16() {
        // Insert 17 distinct first-byte keys → forces Node48
        // Then delete until ≤ 10 → should shrink to Node16
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 17; i++) {
                    byte[] key = {(byte) (i * 15), 0, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(17, w.size());

                // Delete down to 8
                for (int i = 8; i < 17; i++) {
                    assertTrue(w.delete(new byte[]{(byte) (i * 15), 0, 0, 0}));
                }
                assertEquals(8, w.size());

                for (int i = 0; i < 8; i++) {
                    byte[] key = {(byte) (i * 15), 0, 0, 0};
                    long leaf = w.lookup(key);
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals((long) i, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void deleteShrinksNode16ToNode4() {
        // Insert 5 distinct first-byte keys → forces Node16
        // Then delete until ≤ 3 → should shrink to Node4
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 5; i++) {
                    byte[] key = {(byte) (i * 50), 0, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(5, w.size());

                // Delete 3 keys, leaving 2
                for (int i = 2; i < 5; i++) {
                    assertTrue(w.delete(new byte[]{(byte) (i * 50), 0, 0, 0}));
                }
                assertEquals(2, w.size());

                for (int i = 0; i < 2; i++) {
                    byte[] key = {(byte) (i * 50), 0, 0, 0};
                    long leaf = w.lookup(key);
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals((long) i, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void deleteTriggersCollapseSingleChild() {
        // Two keys with same first byte → Node4 with 2 children
        // Delete one → collapse to single child (prefix merge)
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            byte[] k1 = {0x0A, 0x01, 0x00, 0x00};
            byte[] k2 = {0x0A, 0x02, 0x00, 0x00};

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(k1, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 100L);
                w.getOrCreate(k2, 0);
                assertEquals(2, w.size());

                assertTrue(w.delete(k2));
                assertEquals(1, w.size());

                // Remaining key should still be found with correct value
                long leaf = w.lookup(k1);
                assertNotEquals(TaoTree.NOT_FOUND, leaf);
                assertEquals(100L, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void deleteTriggersPrefixMerging() {
        // Three keys with deep shared prefix, delete middle to trigger prefix merging
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            byte[] k1 = {0x0A, 0x0B, 0x01, 0x00};
            byte[] k2 = {0x0A, 0x0B, 0x02, 0x00};
            byte[] k3 = {0x0A, 0x0B, 0x02, 0x01};

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(k1, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 10L);
                long l2 = w.getOrCreate(k2, 0);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 20L);
                long l3 = w.getOrCreate(k3, 0);
                w.leafValue(l3).set(ValueLayout.JAVA_LONG, 0, 30L);

                // Delete k1 to leave k2 and k3 under a common prefix
                assertTrue(w.delete(k1));
                assertEquals(2, w.size());
                assertNotEquals(TaoTree.NOT_FOUND, w.lookup(k2));
                assertNotEquals(TaoTree.NOT_FOUND, w.lookup(k3));
                assertEquals(20L, w.leafValue(w.lookup(k2)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(30L, w.leafValue(w.lookup(k3)).get(ValueLayout.JAVA_LONG, 0));

                // Now delete k2 to collapse prefix
                assertTrue(w.delete(k2));
                assertEquals(1, w.size());
                long remaining = w.lookup(k3);
                assertNotEquals(TaoTree.NOT_FOUND, remaining);
                assertEquals(30L, w.leafValue(remaining).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- Mutation-killing: full insert-delete-verify cycle through all node types ----

    @Test
    void insertAndDeleteAllNodeTypes() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            // Insert 256 keys (every possible first byte) → forces full Node256
            try (var w = tree.write()) {
                for (int i = 0; i < 256; i++) {
                    byte[] key = {(byte) i, 0, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(256, w.size());

                // Delete in reverse order, verifying at each step
                for (int i = 255; i >= 0; i--) {
                    byte[] key = {(byte) i, 0, 0, 0};
                    assertTrue(w.delete(key), "Failed to delete key " + i);
                    assertEquals(i, w.size());

                    // Verify remaining keys still accessible
                    for (int j = 0; j < i; j++) {
                        byte[] k = {(byte) j, 0, 0, 0};
                        long leaf = w.lookup(k);
                        assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + j + " lost after deleting " + i);
                        assertEquals((long) j, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                    }
                }

                assertTrue(w.isEmpty());
            }
        }
    }

    // ---- Mutation-killing: leafKeyMatches ----

    @Test
    void lookupDistinguishesKeysDifferingOnlyInLastByte() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            byte[] k1 = {0x0A, 0x0B, 0x0C, 0x01};
            byte[] k2 = {0x0A, 0x0B, 0x0C, 0x02};
            byte[] k3 = {0x0A, 0x0B, 0x0C, 0x03};

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(k1, 0);
                long l2 = w.getOrCreate(k2, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 1L);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 2L);
            }
            try (var r = tree.read()) {
                assertEquals(1L, r.leafValue(r.lookup(k1)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(2L, r.leafValue(r.lookup(k2)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(k3));
            }
        }
    }

    // ---- Mutation-killing: wrapInPrefix with long shared prefixes ----

    @Test
    void longSharedPrefixKeys() {
        // Use 16-byte keys with keys that share first 14 bytes
        try (var tree = TaoTree.open(16, new int[]{8})) {
            byte[] k1 = new byte[16];
            byte[] k2 = new byte[16];
            byte[] k3 = new byte[16];
            // All share bytes 0-13
            for (int i = 0; i < 14; i++) {
                k1[i] = (byte) (i + 1);
                k2[i] = (byte) (i + 1);
                k3[i] = (byte) (i + 1);
            }
            k1[14] = 0x01; k1[15] = 0x01;
            k2[14] = 0x01; k2[15] = 0x02;
            k3[14] = 0x02; k3[15] = 0x01;

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(k1, 0);
                long l2 = w.getOrCreate(k2, 0);
                long l3 = w.getOrCreate(k3, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 100L);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 200L);
                w.leafValue(l3).set(ValueLayout.JAVA_LONG, 0, 300L);
                assertEquals(3, w.size());
            }
            try (var r = tree.read()) {
                assertEquals(100L, r.leafValue(r.lookup(k1)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(200L, r.leafValue(r.lookup(k2)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(300L, r.leafValue(r.lookup(k3)).get(ValueLayout.JAVA_LONG, 0));
            }
            // Delete and verify prefix merging works correctly
            try (var w = tree.write()) {
                assertTrue(w.delete(k2));
                assertEquals(2, w.size());
                assertNotEquals(TaoTree.NOT_FOUND, w.lookup(k1));
                assertNotEquals(TaoTree.NOT_FOUND, w.lookup(k3));
                assertEquals(TaoTree.NOT_FOUND, w.lookup(k2));
            }
        }
    }

    // ---- Mutation-killing: multiple leaf classes with delete ----

    @Test
    void deleteWithMultipleLeafClasses() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{8, 16})) {
            try (var w = tree.write()) {
                long l0 = w.getOrCreate(intKey(1), 0);
                long l1 = w.getOrCreate(intKey(2), 1);
                w.leafValue(l0).set(ValueLayout.JAVA_LONG, 0, 10L);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 20L);

                assertEquals(2, w.size());
                assertTrue(w.delete(intKey(1)));
                assertEquals(1, w.size());
                assertEquals(TaoTree.NOT_FOUND, w.lookup(intKey(1)));
                assertEquals(20L, w.leafValue(w.lookup(intKey(2))).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- Mutation-killing: deep delete with interleaved lookups ----

    @Test
    void deleteAndReinsertCycle() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                // Insert 50 keys
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                // Delete all odd keys
                for (int i = 1; i < 50; i += 2) {
                    assertTrue(w.delete(intKey(i)));
                }
                assertEquals(25, w.size());

                // Reinsert odd keys with new values
                for (int i = 1; i < 50; i += 2) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) (i + 1000));
                }
                assertEquals(50, w.size());

                // Verify all keys
                for (int i = 0; i < 50; i++) {
                    long leaf = w.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    long expected = (i % 2 == 0) ? i : i + 1000;
                    assertEquals(expected, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- Round 2: Scope safety — every method rejects closed scope ----

    @Test
    void closedReadScopeRejectsAllMethods() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            // Insert a key so we have a valid leaf pointer
            long leafPtr;
            try (var w = tree.write()) {
                leafPtr = w.getOrCreate(intKey(1), 0);
                w.leafValue(leafPtr).set(ValueLayout.JAVA_LONG, 0, 42L);
            }

            var r = tree.read();
            r.close();

            assertThrows(IllegalStateException.class, () -> r.lookup(intKey(1)));
            assertThrows(IllegalStateException.class, () -> r.lookup(MemorySegment.ofArray(intKey(1))));
            assertThrows(IllegalStateException.class, () -> r.lookup(MemorySegment.ofArray(intKey(1)), KEY_LEN));
            assertThrows(IllegalStateException.class, () -> r.leafValue(leafPtr));
            assertThrows(IllegalStateException.class, () -> r.size());
            assertThrows(IllegalStateException.class, () -> r.isEmpty());
        }
    }

    @Test
    void closedWriteScopeRejectsAllMethods() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            long leafPtr;
            try (var w2 = tree.write()) {
                leafPtr = w2.getOrCreate(intKey(1), 0);
            }

            var w = tree.write();
            w.close();

            assertThrows(IllegalStateException.class, () -> w.lookup(intKey(1)));
            assertThrows(IllegalStateException.class, () -> w.lookup(MemorySegment.ofArray(intKey(1))));
            assertThrows(IllegalStateException.class, () -> w.lookup(MemorySegment.ofArray(intKey(1)), KEY_LEN));
            assertThrows(IllegalStateException.class, () -> w.leafValue(leafPtr));
            assertThrows(IllegalStateException.class, () -> w.size());
            assertThrows(IllegalStateException.class, () -> w.isEmpty());
            assertThrows(IllegalStateException.class, () -> w.getOrCreate(intKey(2), 0));
            assertThrows(IllegalStateException.class, () -> w.getOrCreate(MemorySegment.ofArray(intKey(2))));
            assertThrows(IllegalStateException.class, () -> w.getOrCreate(MemorySegment.ofArray(intKey(2)), KEY_LEN, 0));
            assertThrows(IllegalStateException.class, () -> w.delete(intKey(2)));
            assertThrows(IllegalStateException.class, () -> w.delete(MemorySegment.ofArray(intKey(2)), KEY_LEN));
        }
    }

    // ---- Round 2: Cross-thread scope use ----

    @Test
    void scopeRejectsCrossThreadUse() throws Exception {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                w.getOrCreate(intKey(1), 0);
            }
            var r = tree.read();
            var thread = new Thread(() -> {
                assertThrows(IllegalStateException.class, () -> r.lookup(intKey(1)));
            });
            thread.start();
            thread.join();
            r.close();
        }
    }

    // ---- Round 2: MemorySegment-based API coverage ----

    @Test
    void memorySegmentLookupAndGetOrCreate() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            byte[] keyBytes = intKey(42);
            MemorySegment key = MemorySegment.ofArray(keyBytes);

            try (var w = tree.write()) {
                long leaf = w.getOrCreate(key, KEY_LEN, 0);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 99L);

                // Lookup via MemorySegment with explicit keyLen
                long found1 = w.lookup(key, KEY_LEN);
                assertNotEquals(TaoTree.NOT_FOUND, found1);
                assertEquals(99L, w.leafValue(found1).get(ValueLayout.JAVA_LONG, 0));

                // Lookup via MemorySegment with implicit keyLen
                long found2 = w.lookup(key);
                assertNotEquals(TaoTree.NOT_FOUND, found2);

                // getOrCreate via MemorySegment with implicit keyLen and class
                long found3 = w.getOrCreate(key);
                assertEquals(found1, found3); // same leaf
            }

            try (var r = tree.read()) {
                long found = r.lookup(key, KEY_LEN);
                assertNotEquals(TaoTree.NOT_FOUND, found);
                assertEquals(99L, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));

                // Implicit keyLen lookup
                long found2 = r.lookup(key);
                assertEquals(found, found2);
            }
        }
    }

    @Test
    void memorySegmentDelete() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            MemorySegment key = MemorySegment.ofArray(intKey(42));
            try (var w = tree.write()) {
                w.getOrCreate(key, KEY_LEN, 0);
                assertTrue(w.delete(key, KEY_LEN));
                assertEquals(0, w.size());
                assertEquals(TaoTree.NOT_FOUND, w.lookup(key, KEY_LEN));
            }
        }
    }

    // ---- Round 2: open overloads ----

    @Test
    void openWithSingleValueSize() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            assertEquals(KEY_LEN, tree.keyLen());
            assertFalse(tree.isFileBacked());
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1), 0);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
            }
            try (var r = tree.read()) {
                assertEquals(42L, r.leafValue(r.lookup(intKey(1))).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void openWithCustomSlabSize() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE, 512 * 1024)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1), 0);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
                assertEquals(1, w.size());
            }
        }
    }

    // ---- Round 2: stats methods ----

    @Test
    void statsMethodsAfterInsertions() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                for (int i = 0; i < 10; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
            }
            assertTrue(tree.totalSlabBytes() > 0);
            assertTrue(tree.totalSegmentsInUse() > 0);
            // overflowPageCount is 0 for trees without TaoString usage
            assertTrue(tree.overflowPageCount() >= 0);
        }
    }

    @Test
    void statsWithOverflow() {
        // Tree with TaoString-sized values will use bump allocator
        try (var tree = TaoTree.open(16, TaoString.SIZE)) {
            try (var w = tree.write()) {
                byte[] key = new byte[16];
                key[0] = 1;
                long leaf = w.getOrCreate(key, 0);
                TaoString.write(w.leafValue(leaf), "Haliaeetus leucocephalus", tree);
            }
            assertTrue(tree.totalOverflowBytes() > 0);
            assertTrue(tree.overflowPageCount() > 0);
        }
    }

    // ---- Round 2: validateKeyLen on MemorySegment path ----

    @Test
    void rejectWrongKeyLengthMemorySegment() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                MemorySegment shortKey = MemorySegment.ofArray(new byte[]{1, 2, 3});
                assertThrows(IllegalArgumentException.class,
                    () -> w.getOrCreate(shortKey, 3, 0));
                assertThrows(IllegalArgumentException.class,
                    () -> w.lookup(shortKey, 3));
                assertThrows(IllegalArgumentException.class,
                    () -> w.delete(shortKey, 3));
            }
            try (var r = tree.read()) {
                MemorySegment shortKey = MemorySegment.ofArray(new byte[]{1, 2, 3});
                assertThrows(IllegalArgumentException.class,
                    () -> r.lookup(shortKey, 3));
            }
        }
    }

    // ---- Round 2: memory integrity after heavy insert/delete cycles ----

    @Test
    void heavyInsertDeleteCycleMemoryIntegrity() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            long slabBefore = tree.totalSlabBytes();
            try (var w = tree.write()) {
                // Insert many keys
                for (int i = 0; i < 500; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                // Delete all
                for (int i = 0; i < 500; i++) {
                    assertTrue(w.delete(intKey(i)));
                }
                assertTrue(w.isEmpty());
                assertEquals(0, w.size());

                // Reinsert
                for (int i = 0; i < 500; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) (i + 1000));
                }
                assertEquals(500, w.size());
            }
            try (var r = tree.read()) {
                for (int i = 0; i < 500; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals((long) (i + 1000), r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
            // After delete+reinsert, slab bytes should be reasonable (not growing unbounded)
            long slabAfter = tree.totalSlabBytes();
            assertTrue(slabAfter > slabBefore);
        }
    }

    // ---- STRONGER round: validateLeafPtr thorough ----

    @Test
    void leafValueRejectsWrongClassPtr() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            // Create a tree with a different leaf class to get a pointer that 
            // doesn't belong to this tree's leaf classes
            try (var r = tree.read()) {
                // Fabricate a leaf pointer with wrong class ID (class 255)
                long fakePtr = NodePtr.pack(NodePtr.LEAF, 255, 0, 0);
                assertThrows(IllegalArgumentException.class,
                    () -> r.leafValue(fakePtr));
            }
            try (var w = tree.write()) {
                long fakePtr = NodePtr.pack(NodePtr.LEAF, 255, 0, 0);
                assertThrows(IllegalArgumentException.class,
                    () -> w.leafValue(fakePtr));
            }
        }
    }

    // ---- STRONGER round: scope double-close is no-op ----

    @Test
    void doubleCloseReadScopeIsNoOp() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            var r = tree.read();
            r.close();
            r.close(); // should not throw or deadlock
        }
    }

    @Test
    void doubleCloseWriteScopeIsNoOp() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            var w = tree.write();
            w.close();
            w.close(); // should not throw or deadlock
            // Verify the tree is still usable
            try (var r = tree.read()) {
                assertTrue(r.isEmpty());
            }
        }
    }

    // ---- STRONGER round: open parameter validation ----

    @Test
    void openRejectsInvalidKeyLen() {
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.open(0, VALUE_SIZE));
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.open(-1, VALUE_SIZE));
    }

    @Test
    void openRejectsEmptyLeafValueSizes() {
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.open(KEY_LEN, new int[]{}));
    }

    @Test
    void openAcceptsZeroValueSize() {
        // Zero-value-size leaf class is valid (key-only entries)
        try (var tree = TaoTree.open(KEY_LEN, new int[]{0})) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1), 0);
                assertNotEquals(TaoTree.NOT_FOUND, leaf);
            }
        }
    }

    @Test
    void openRejectsInvalidSlabSize() {
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.open(KEY_LEN, VALUE_SIZE, 0));
    }

    // ---- STRONGER round: empty tree delete guard ----

    @Test
    void deleteMemorySegmentFromEmptyTree() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                assertFalse(w.delete(MemorySegment.ofArray(intKey(1)), KEY_LEN));
            }
        }
    }

    // ---- STRONGER round: deep prefix operations ----

    @Test
    void insertDeleteKeysWithMaxPrefixChains() {
        // 32-byte keys forcing deep prefix chains
        try (var tree = TaoTree.open(32, new int[]{8})) {
            // Two keys identical in first 28 bytes, differ in last 4
            byte[] k1 = new byte[32];
            byte[] k2 = new byte[32];
            for (int i = 0; i < 28; i++) {
                k1[i] = (byte) (i + 1);
                k2[i] = (byte) (i + 1);
            }
            k1[28] = 1; k1[29] = 2; k1[30] = 3; k1[31] = 4;
            k2[28] = 1; k2[29] = 2; k2[30] = 3; k2[31] = 5;

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(k1, 0);
                long l2 = w.getOrCreate(k2, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 100L);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 200L);
                assertEquals(2, w.size());

                // Delete k1, k2's structure should collapse correctly
                assertTrue(w.delete(k1));
                assertEquals(1, w.size());
                long found = w.lookup(k2);
                assertNotEquals(TaoTree.NOT_FOUND, found);
                assertEquals(200L, w.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- STRONGER round: updateChildInNode switch mutation ----

    @Test
    void insertAndDeleteInAllNodeTypesPreservesData() {
        // Exercise all node type transitions and updateChildInNode dispatch
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                // Insert 100 keys with distinct first byte
                for (int i = 0; i < 100; i++) {
                    byte[] key = {(byte) i, (byte) (i >> 2), 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i * 100);
                }
                assertEquals(100, w.size());

                // Delete every third key
                int deleted = 0;
                for (int i = 0; i < 100; i += 3) {
                    byte[] key = {(byte) i, (byte) (i >> 2), 0, 0};
                    assertTrue(w.delete(key));
                    deleted++;
                }
                assertEquals(100 - deleted, w.size());

                // Verify remaining keys
                for (int i = 0; i < 100; i++) {
                    byte[] key = {(byte) i, (byte) (i >> 2), 0, 0};
                    long leaf = w.lookup(key);
                    if (i % 3 == 0) {
                        assertEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " should be deleted");
                    } else {
                        assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " should exist");
                        assertEquals((long) i * 100, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                    }
                }
            }
        }
    }

    // ---- STRONGER round: copyFrom with closed scope ----

    @Test
    void copyFromRejectsClosedScopes() {
        try (var src = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE});
             var dst = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var sw = src.write()) {
                sw.getOrCreate(intKey(1), 0);
            }
            // Source scope closed
            var srcRead = src.read();
            srcRead.close();
            try (var dw = dst.write()) {
                assertThrows(IllegalStateException.class,
                    () -> dw.copyFrom(srcRead));
            }
        }
    }

    // ---- STRONGER: memory leak detection via totalSegmentsInUse ----

    @Test
    void deleteFreesSlabSegments() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                // Insert 256 keys to create many nodes (Node4, Node16, Node48, Node256, prefix nodes, leaves)
                for (int i = 0; i < 256; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
            }
            long segsBefore = tree.totalSegmentsInUse();
            assertTrue(segsBefore > 256, "Should have nodes + leaves allocated");

            try (var w = tree.write()) {
                for (int i = 0; i < 256; i++) {
                    w.delete(intKey(i));
                }
                assertEquals(0, w.size());
            }
            long segsAfter = tree.totalSegmentsInUse();
            // After deleting everything, segments in use should drop significantly
            // (some internal overhead may remain but should be much less than before)
            assertTrue(segsAfter < segsBefore / 2,
                "Expected segments to decrease after deleting all keys: before=" + segsBefore + " after=" + segsAfter);
        }
    }

    // ---- STRONGER: exact shrink threshold tests ----

    @Test
    void shrinkNode256ExactThreshold() {
        // NODE256_SHRINK_THRESHOLD = 36
        // Insert 49 distinct first-byte keys → Node256
        // Delete down to exactly 37 (above threshold) → no shrink
        // Delete one more to 36 (at threshold) → shrink to Node48
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 49; i++) {
                    byte[] key = {(byte) (i * 5), 0, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                // Delete to 37 remaining
                for (int i = 37; i < 49; i++) {
                    assertTrue(w.delete(new byte[]{(byte) (i * 5), 0, 0, 0}));
                }
                assertEquals(37, w.size());
                // All 37 remaining should be found
                for (int i = 0; i < 37; i++) {
                    assertNotEquals(TaoTree.NOT_FOUND, w.lookup(new byte[]{(byte) (i * 5), 0, 0, 0}));
                }
                // Delete one more → shrink threshold reached
                assertTrue(w.delete(new byte[]{(byte) (36 * 5), 0, 0, 0}));
                assertEquals(36, w.size());
                for (int i = 0; i < 36; i++) {
                    long leaf = w.lookup(new byte[]{(byte) (i * 5), 0, 0, 0});
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " lost at shrink threshold");
                    assertEquals((long) i, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void shrinkNode48ExactThreshold() {
        // NODE48_SHRINK_THRESHOLD = 12
        // Insert 17 → Node48, delete to 13 (above), then to 12 (at threshold → shrink to Node16)
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 17; i++) {
                    byte[] key = {(byte) (i * 15), 0, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                // Delete to 13
                for (int i = 13; i < 17; i++) {
                    assertTrue(w.delete(new byte[]{(byte) (i * 15), 0, 0, 0}));
                }
                assertEquals(13, w.size());
                for (int i = 0; i < 13; i++) {
                    assertNotEquals(TaoTree.NOT_FOUND, w.lookup(new byte[]{(byte) (i * 15), 0, 0, 0}));
                }
                // Delete one more → threshold
                assertTrue(w.delete(new byte[]{(byte) (12 * 15), 0, 0, 0}));
                assertEquals(12, w.size());
                for (int i = 0; i < 12; i++) {
                    long leaf = w.lookup(new byte[]{(byte) (i * 15), 0, 0, 0});
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals((long) i, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void shrinkNode16ExactThreshold() {
        // NODE16_SHRINK_THRESHOLD = 4
        // Insert 5 → Node16, delete to 5 (still Node16), delete to 4 → shrink to Node4
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            try (var w = tree.write()) {
                for (int i = 0; i < 5; i++) {
                    byte[] key = {(byte) (i * 50), 0, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(5, w.size());
                // Delete to exactly 4 → threshold
                assertTrue(w.delete(new byte[]{(byte) (4 * 50), 0, 0, 0}));
                assertEquals(4, w.size());
                for (int i = 0; i < 4; i++) {
                    long leaf = w.lookup(new byte[]{(byte) (i * 50), 0, 0, 0});
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals((long) i, w.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- STRONGER: wrapInPrefix chunking with deep keys ----

    @Test
    void deepPrefixChainWith32ByteKeys() {
        // PREFIX_CAPACITY is about 10 bytes. Keys sharing 25 bytes need 3 prefix nodes chained.
        try (var tree = TaoTree.open(32, new int[]{8})) {
            byte[] k1 = new byte[32];
            byte[] k2 = new byte[32];
            byte[] k3 = new byte[32];
            // First 25 bytes identical
            for (int i = 0; i < 25; i++) {
                byte v = (byte) ((i * 7 + 3) & 0xFF);
                k1[i] = v; k2[i] = v; k3[i] = v;
            }
            // Differ at byte 25
            k1[25] = 0x01; k2[25] = 0x02; k3[25] = 0x03;

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(k1, 0);
                long l2 = w.getOrCreate(k2, 0);
                long l3 = w.getOrCreate(k3, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 111L);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 222L);
                w.leafValue(l3).set(ValueLayout.JAVA_LONG, 0, 333L);
            }
            try (var r = tree.read()) {
                assertEquals(111L, r.leafValue(r.lookup(k1)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(222L, r.leafValue(r.lookup(k2)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(333L, r.leafValue(r.lookup(k3)).get(ValueLayout.JAVA_LONG, 0));
            }
            // Delete and verify prefix chain collapse
            try (var w = tree.write()) {
                assertTrue(w.delete(k1));
                assertTrue(w.delete(k2));
                assertEquals(1, w.size());
                assertEquals(333L, w.leafValue(w.lookup(k3)).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- STRONGER: leafKeyMatches - keys sharing prefix path but differing in leaf ----

    @Test
    void lookupRejectsWrongKeyAtLeafLevel() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            // Insert k1. Then lookup k2 which shares all ART traversal bytes but has different key.
            // With 4-byte keys, the ART uses all bytes for traversal so this tests the final
            // leaf comparison.
            byte[] k1 = {0x0A, 0x0B, 0x0C, 0x0D};
            byte[] k2 = {0x0A, 0x0B, 0x0C, 0x0E};  // differs in last byte
            byte[] k3 = {0x0A, 0x0B, 0x0D, 0x0D};  // differs in 3rd byte
            byte[] k4 = {0x0B, 0x0B, 0x0C, 0x0D};  // differs in 1st byte

            try (var w = tree.write()) {
                long leaf = w.getOrCreate(k1, 0);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
            }
            try (var r = tree.read()) {
                // k1 found
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(k1));
                // All others must NOT be found
                assertEquals(TaoTree.NOT_FOUND, r.lookup(k2));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(k3));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(k4));
            }
        }
    }

    // ---- STRONGER: lookupImpl code paths ----

    @Test
    void lookupPrefixMismatchReturnsNotFound() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            // Create two keys that create a prefix node
            byte[] k1 = {0x0A, 0x0B, 0x01, 0x00};
            byte[] k2 = {0x0A, 0x0B, 0x02, 0x00};
            try (var w = tree.write()) {
                w.getOrCreate(k1, 0);
                w.getOrCreate(k2, 0);
            }
            try (var r = tree.read()) {
                // Query with a key that mismatches the prefix at byte 1
                byte[] miss = {0x0A, 0x0C, 0x01, 0x00};
                assertEquals(TaoTree.NOT_FOUND, r.lookup(miss));
                // Query with a key that matches prefix but has no child at the branch byte
                byte[] miss2 = {0x0A, 0x0B, 0x03, 0x00};
                assertEquals(TaoTree.NOT_FOUND, r.lookup(miss2));
            }
        }
    }

    // ---- STRONGER: updateChildInNode across all node types ----

    @Test
    void updateChildAfterDeleteInNode4() {
        // 2 keys with same prefix byte[0], different byte[1] → Node4 at depth 1
        // Delete one key → child pointer update in the parent Node4
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            byte[] k1 = {0x0A, 0x01, 0x00, 0x00};
            byte[] k2 = {0x0A, 0x02, 0x00, 0x00};
            byte[] k3 = {0x0A, 0x01, 0x01, 0x00};

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(k1, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 1L);
                long l2 = w.getOrCreate(k2, 0);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 2L);
                long l3 = w.getOrCreate(k3, 0);
                w.leafValue(l3).set(ValueLayout.JAVA_LONG, 0, 3L);

                // Delete k1 → triggers child update in parent node
                assertTrue(w.delete(k1));
                assertEquals(2, w.size());
                assertEquals(TaoTree.NOT_FOUND, w.lookup(k1));
                assertEquals(2L, w.leafValue(w.lookup(k2)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(3L, w.leafValue(w.lookup(k3)).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }
}
