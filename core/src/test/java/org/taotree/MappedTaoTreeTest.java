package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

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
}
