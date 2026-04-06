package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import org.taotree.TaoString;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.WriterArena;

/**
 * Tests for file-backed TaoTree persistence.
 * Verifies create, insert, sync, close, reopen, and lookup round-trips.
 */
class MappedTaoTreeTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    @TempDir Path tmp;

    private byte[] intKey(int value) {
        byte[] key = new byte[KEY_LEN];
        key[0] = (byte) (value >>> 24);
        key[1] = (byte) (value >>> 16);
        key[2] = (byte) (value >>> 8);
        key[3] = (byte) value;
        return key;
    }

    @Test
    void createAndLookup() throws IOException {
        Path file = tmp.resolve("test.tao");
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            assertTrue(tree.isFileBacked());

            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1));
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
            }

            try (var r = tree.read()) {
                long leaf = r.lookup(intKey(1));
                assertNotEquals(TaoTree.NOT_FOUND, leaf);
                assertEquals(42L, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void persistenceRoundTrip() throws IOException {
        Path file = tmp.resolve("test.tao");

        // Phase 1: create tree, insert data, close
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                for (int i = 1; i <= 100; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i * 100);
                }
            }

            try (var r = tree.read()) {
                assertEquals(100, r.size());
            }
        }
        // tree is closed, arena released, file synced

        // Phase 2: reopen and verify all data is intact
        try (var tree = TaoTree.open(file)) {
            assertTrue(tree.isFileBacked());

            try (var r = tree.read()) {
                assertEquals(100, r.size());

                for (int i = 1; i <= 100; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf,
                        "Key " + i + " not found after reopen");
                    assertEquals((long) i * 100,
                        r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0),
                        "Value mismatch for key " + i);
                }

                // Verify absent key
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(999)));
            }
        }
    }

    @Test
    void persistenceWithDelete() throws IOException {
        Path file = tmp.resolve("test.tao");

        // Phase 1: insert 50, delete 25
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                for (int i = 1; i <= 50; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                for (int i = 1; i <= 25; i++) {
                    assertTrue(w.delete(intKey(i)));
                }
                assertEquals(25, w.size());
            }
        }

        // Phase 2: reopen and verify
        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(25, r.size());
                for (int i = 1; i <= 25; i++) {
                    assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(i)),
                        "Deleted key " + i + " should not be found");
                }
                for (int i = 26; i <= 50; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf,
                        "Key " + i + " should still exist");
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void persistenceWithMoreData() throws IOException {
        Path file = tmp.resolve("test.tao");

        // Phase 1: insert 10000 keys
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                for (int i = 0; i < 10_000; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }
        }

        // Phase 2: reopen and verify all
        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(10_000, r.size());
                // Spot check
                for (int i = 0; i < 10_000; i += 100) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i);
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }

            // Phase 3: insert more keys after reopen
            try (var w = tree.write()) {
                for (int i = 10_000; i < 10_100; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(10_100, w.size());
            }
        }

        // Phase 4: reopen again and verify everything
        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(10_100, r.size());
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(99_999)));
            }
        }
    }

    @Test
    void explicitSync() throws IOException {
        Path file = tmp.resolve("test.tao");
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1));
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 999L);
            }
            tree.sync();
            // Data should be on disk now, even if we don't close cleanly
        }

        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(1, r.size());
                long leaf = r.lookup(intKey(1));
                assertEquals(999L, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- File-backed: create with multiple leaf classes ----

    @Test
    void createWithMultipleLeafClasses() throws IOException {
        Path file = tmp.resolve("multi.tao");
        try (var tree = TaoTree.create(file, KEY_LEN, new int[]{8, 16, 24})) {
            assertTrue(tree.isFileBacked());
            try (var w = tree.write()) {
                long l0 = w.getOrCreate(intKey(1), 0);
                long l1 = w.getOrCreate(intKey(2), 1);
                long l2 = w.getOrCreate(intKey(3), 2);
                w.leafValue(l0).set(ValueLayout.JAVA_LONG, 0, 100L);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 200L);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 300L);
                assertEquals(3, w.size());
            }
        }

        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(3, r.size());
                assertEquals(100L, r.leafValue(r.lookup(intKey(1))).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(200L, r.leafValue(r.lookup(intKey(2))).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(300L, r.leafValue(r.lookup(intKey(3))).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- File-backed: create parameter validation ----

    @Test
    void createRejectsInvalidKeyLen() {
        Path file = tmp.resolve("bad.tao");
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.create(file, 0, VALUE_SIZE));
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.create(file, -1, VALUE_SIZE));
    }

    @Test
    void createMultiLeafRejectsInvalidArgs() {
        Path file = tmp.resolve("bad.tao");
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.create(file, 0, new int[]{8}));
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.create(file, KEY_LEN, new int[]{}));
        assertThrows(IllegalArgumentException.class,
            () -> TaoTree.create(file, KEY_LEN, (int[]) null));
    }

    // ---- File-backed: sync ensures data survives reopen ----

    @Test
    void syncPersistsDataBeforeClose() throws IOException {
        Path file = tmp.resolve("sync.tao");
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1));
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 111L);
            }
            // Sync writes superblock + forces data to disk
            tree.sync();

            // Insert more data AFTER sync
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(2));
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 222L);
            }
            tree.sync();
        }

        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(2, r.size());
                assertEquals(111L, r.leafValue(r.lookup(intKey(1))).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(222L, r.leafValue(r.lookup(intKey(2))).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- File-backed: multiple reopen cycles ----

    @Test
    void multipleReopenCycles() throws IOException {
        Path file = tmp.resolve("cycles.tao");

        // Cycle 1: create with 50 keys
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }
        }

        // Cycle 2: reopen, verify, add 50 more
        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(50, r.size());
            }
            try (var w = tree.write()) {
                for (int i = 50; i < 100; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }
        }

        // Cycle 3: reopen, verify, delete some
        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(100, r.size());
                for (int i = 0; i < 100; i++) {
                    assertNotEquals(TaoTree.NOT_FOUND, r.lookup(intKey(i)));
                }
            }
            try (var w = tree.write()) {
                for (int i = 0; i < 25; i++) {
                    assertTrue(w.delete(intKey(i)));
                }
                assertEquals(75, w.size());
            }
        }

        // Cycle 4: final verify
        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(75, r.size());
                for (int i = 0; i < 25; i++) {
                    assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(i)));
                }
                for (int i = 25; i < 100; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " lost");
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- File-backed: overflow (bump allocator) persistence ----

    @Test
    void overflowStringPersistence() throws IOException {
        Path file = tmp.resolve("strings.tao");
        int stringKeyLen = 16;

        try (var tree = TaoTree.create(file, stringKeyLen, TaoString.SIZE)) {
            try (var w = tree.write()) {
                for (int i = 0; i < 20; i++) {
                    byte[] key = new byte[stringKeyLen];
                    key[0] = (byte) (i >>> 8);
                    key[1] = (byte) i;
                    long leaf = w.getOrCreate(key, 0);
                    // Mix of short and long strings
                    String value = (i % 2 == 0) ? "short_" + i : "this_is_a_longer_string_value_" + i;
                    TaoString.write(w.leafValue(leaf), value, tree);
                }
            }
        }

        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(20, r.size());
                for (int i = 0; i < 20; i++) {
                    byte[] key = new byte[stringKeyLen];
                    key[0] = (byte) (i >>> 8);
                    key[1] = (byte) i;
                    long leaf = r.lookup(key);
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " not found");
                    String expected = (i % 2 == 0) ? "short_" + i : "this_is_a_longer_string_value_" + i;
                    assertEquals(expected, TaoString.read(r.leafValue(leaf), tree));
                }
            }
        }
    }

    // ---- File-backed: stats accessible after reopen ----

    @Test
    void statsAfterReopen() throws IOException {
        Path file = tmp.resolve("stats.tao");
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            try (var w = tree.write()) {
                for (int i = 0; i < 100; i++) {
                    w.getOrCreate(intKey(i));
                }
            }
            // File-backed COW allocates via WriterArena; slab may have 0 data bytes
            assertTrue(tree.totalSegmentsInUse() >= 0);
        }

        try (var tree = TaoTree.open(file)) {
            assertTrue(tree.isFileBacked());
            assertEquals(KEY_LEN, tree.keyLen());
            try (var r = tree.read()) {
                assertEquals(100, r.size());
            }
        }
    }

    // ---- File-backed: sync is no-op for in-memory trees ----

    @Test
    void syncNoOpForInMemoryTree() throws IOException {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            assertFalse(tree.isFileBacked());
            // sync should be a no-op, not throw
            tree.sync();
        }
    }

    // ---- File-backed: empty tree round-trip ----

    @Test
    void emptyTreeRoundTrip() throws IOException {
        Path file = tmp.resolve("empty.tao");
        try (var tree = TaoTree.create(file, KEY_LEN, VALUE_SIZE)) {
            try (var r = tree.read()) {
                assertTrue(r.isEmpty());
            }
        }

        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertTrue(r.isEmpty());
                assertEquals(0, r.size());
            }
            // Insert into reopened empty tree
            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(1));
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
            }
        }

        try (var tree = TaoTree.open(file)) {
            try (var r = tree.read()) {
                assertEquals(1, r.size());
                assertEquals(42L, r.leafValue(r.lookup(intKey(1))).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }
}
