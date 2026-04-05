package org.taotree;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for COW (copy-on-write) mode: ROWEX-hybrid lock-free readers
 * and per-subtree CAS concurrent writers.
 */
class CowModeTest {

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

    // ---- Basic COW mode operations ----

    @Test
    void cowModeActivation() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            assertFalse(tree.isCowMode());
            tree.activateCowMode();
            assertTrue(tree.isCowMode());
        }
    }

    @Test
    void cowModeInsertAndLookup() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            tree.activateCowMode();

            try (var w = tree.write()) {
                long leaf = w.getOrCreate(intKey(42), 0);
                assertNotEquals(TaoTree.NOT_FOUND, leaf);
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
    void cowModeInsertMultipleKeys() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            tree.activateCowMode();

            try (var w = tree.write()) {
                for (int i = 0; i < 100; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, i * 10L);
                }
            }

            try (var r = tree.read()) {
                assertEquals(100, r.size());
                for (int i = 0; i < 100; i++) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " not found");
                    assertEquals(i * 10L, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
                // Missing key
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(999)));
            }
        }
    }

    @Test
    void cowModeDuplicateInsert() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            tree.activateCowMode();

            try (var w = tree.write()) {
                long leaf1 = w.getOrCreate(intKey(42), 0);
                long leaf2 = w.getOrCreate(intKey(42), 0);
                assertEquals(leaf1, leaf2);
                assertEquals(1, w.size());
            }
        }
    }

    @Test
    void cowModeDelete() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            tree.activateCowMode();

            // Insert
            try (var w = tree.write()) {
                for (int i = 0; i < 10; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
                assertEquals(10, w.size());
            }

            // Delete some
            try (var w = tree.write()) {
                assertTrue(w.delete(intKey(5)));
                assertTrue(w.delete(intKey(3)));
                assertFalse(w.delete(intKey(999))); // not found
                assertEquals(8, w.size());
            }

            // Verify
            try (var r = tree.read()) {
                assertEquals(8, r.size());
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(intKey(0)));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(3)));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(5)));
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(intKey(9)));
            }
        }
    }

    @Test
    void cowModeLargeInsert() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            tree.activateCowMode();

            int count = 10_000;
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            try (var r = tree.read()) {
                assertEquals(count, r.size());
                // Spot check
                for (int i = 0; i < count; i += 100) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " not found");
                    assertEquals((long) i, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- Lock-free readers ----

    @Test
    void lockFreeReadersDoNotBlock() throws InterruptedException {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            tree.activateCowMode();

            // Pre-populate
            try (var w = tree.write()) {
                for (int i = 0; i < 100; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            // Multiple reader threads running simultaneously
            int numReaders = 4;
            var latch = new CountDownLatch(numReaders);
            var errors = new AtomicInteger(0);

            for (int t = 0; t < numReaders; t++) {
                Thread.ofVirtual().start(() -> {
                    try {
                        for (int round = 0; round < 1000; round++) {
                            try (var r = tree.read()) {
                                for (int i = 0; i < 100; i++) {
                                    long found = r.lookup(intKey(i));
                                    if (found == TaoTree.NOT_FOUND) {
                                        errors.incrementAndGet();
                                    }
                                }
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            assertEquals(0, errors.get(), "Lock-free readers should see all pre-populated keys");
        }
    }

    // ---- Concurrent writers ----

    @Test
    void concurrentWritersDifferentKeys() throws Exception {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            tree.activateCowMode();

            int numWriters = 4;
            int keysPerWriter = 250;
            var barrier = new CyclicBarrier(numWriters);
            var errors = new AtomicInteger(0);
            var threads = new Thread[numWriters];

            for (int t = 0; t < numWriters; t++) {
                final int writerId = t;
                threads[t] = Thread.ofPlatform().start(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < keysPerWriter; i++) {
                            int key = writerId * keysPerWriter + i;
                            try (var w = tree.write()) {
                                long leaf = w.getOrCreate(intKey(key), 0);
                                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) key);
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                        e.printStackTrace();
                    }
                });
            }

            for (var t : threads) t.join();
            assertEquals(0, errors.get(), "No writer errors expected");

            // Verify all keys
            try (var r = tree.read()) {
                assertEquals(numWriters * keysPerWriter, r.size());
                for (int i = 0; i < numWriters * keysPerWriter; i++) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " not found");
                    assertEquals((long) i, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void concurrentReadersAndWriters() throws Exception {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            tree.activateCowMode();

            // Pre-populate
            try (var w = tree.write()) {
                for (int i = 0; i < 100; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            var readerErrors = new AtomicInteger(0);
            var writerErrors = new AtomicInteger(0);
            var running = new java.util.concurrent.atomic.AtomicBoolean(true);

            // Readers continuously read existing keys
            int numReaders = 3;
            var readerThreads = new Thread[numReaders];
            for (int t = 0; t < numReaders; t++) {
                readerThreads[t] = Thread.ofPlatform().start(() -> {
                    while (running.get()) {
                        try (var r = tree.read()) {
                            // Pre-populated keys should always be findable
                            for (int i = 0; i < 100; i++) {
                                long found = r.lookup(intKey(i));
                                if (found == TaoTree.NOT_FOUND) {
                                    readerErrors.incrementAndGet();
                                }
                            }
                        }
                    }
                });
            }

            // Writer inserts new keys
            Thread.ofPlatform().start(() -> {
                try {
                    for (int i = 100; i < 500; i++) {
                        try (var w = tree.write()) {
                            long leaf = w.getOrCreate(intKey(i), 0);
                            w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                        }
                    }
                } catch (Exception e) {
                    writerErrors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    running.set(false);
                }
            }).join();

            for (var t : readerThreads) t.join();

            assertEquals(0, readerErrors.get(),
                "Readers should never see missing pre-populated keys");
            assertEquals(0, writerErrors.get(), "No writer errors expected");

            // Verify final state
            try (var r = tree.read()) {
                assertEquals(500, r.size());
            }
        }
    }

    // ---- Transition from legacy to COW mode ----

    @Test
    void legacyToComModeTransition() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            // Insert in legacy mode
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            // Activate COW mode
            tree.activateCowMode();

            // Read existing data in COW mode
            try (var r = tree.read()) {
                assertEquals(50, r.size());
                for (int i = 0; i < 50; i++) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found);
                }
            }

            // Insert new data in COW mode
            try (var w = tree.write()) {
                for (int i = 50; i < 100; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            // Verify all data
            try (var r = tree.read()) {
                assertEquals(100, r.size());
                for (int i = 0; i < 100; i++) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " not found");
                    assertEquals((long) i, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }
}
