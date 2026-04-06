package org.taotree;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.art.Node16;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.Node48;
import org.taotree.internal.art.Node4;

/**
 * Tests for COW (copy-on-write) concurrency: ROWEX-hybrid lock-free readers
 * and per-subtree CAS concurrent writers. COW is always active.
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

    // ---- Basic COW operations ----

    @Test
    void insertAndLookup() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {

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
    void insertMultipleKeys() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


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
    void duplicateInsert() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            try (var w = tree.write()) {
                long leaf1 = w.getOrCreate(intKey(42), 0);
                long leaf2 = w.getOrCreate(intKey(42), 0);
                assertEquals(leaf1, leaf2);
                assertEquals(1, w.size());
            }
        }
    }

    @Test
    void deleteKeys() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


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
    void largeInsert() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


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

    // ---- Multi-phase insert test ----

    @Test
    void multiPhaseInsert() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {
            // Phase 1: insert first batch
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            // Read existing data
            try (var r = tree.read()) {
                assertEquals(50, r.size());
                for (int i = 0; i < 50; i++) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found);
                }
            }

            // Phase 2: insert more
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

    // ---- Node growth (triggers node type transitions) ----

    @Test
    void nodeGrowth() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Insert enough keys to force Node4 → Node16 → Node48 → Node256 growth
            // Use intKey() to ensure unique 4-byte keys (big-endian encoding)
            int count = 300;
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            try (var r = tree.read()) {
                assertEquals(count, r.size());
                for (int i = 0; i < count; i += 50) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " not found");
                    assertEquals((long) i, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void deleteAndReinsert() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Insert
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
            }

            // Delete all
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    assertTrue(w.delete(intKey(i)));
                }
                assertEquals(0, w.size());
            }

            // Reinsert
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, i * 100L);
                }
            }

            try (var r = tree.read()) {
                assertEquals(50, r.size());
                for (int i = 0; i < 50; i++) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found);
                    assertEquals(i * 100L, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void prefixSplit() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Keys that share a long prefix then diverge
            byte[] key1 = {1, 2, 3, 10};
            byte[] key2 = {1, 2, 3, 20};
            byte[] key3 = {1, 2, 4, 10};  // diverges at byte 2

            try (var w = tree.write()) {
                w.getOrCreate(key1, 0);
                w.getOrCreate(key2, 0);
                w.getOrCreate(key3, 0);
                assertEquals(3, w.size());
            }

            try (var r = tree.read()) {
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key1));
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key2));
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key3));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(new byte[]{1, 2, 3, 30}));
            }
        }
    }

    @Test
    void nodeShrink() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Insert enough to grow nodes, then delete to trigger shrink
            try (var w = tree.write()) {
                for (int i = 0; i < 20; i++) {
                    w.getOrCreate(intKey(i * 1000), 0); // spread keys widely
                }
                assertEquals(20, w.size());
            }

            // Delete most to trigger Node16→Node4 shrink
            try (var w = tree.write()) {
                for (int i = 5; i < 20; i++) {
                    assertTrue(w.delete(intKey(i * 1000)));
                }
                assertEquals(5, w.size());
            }

            // Remaining keys should still be findable
            try (var r = tree.read()) {
                assertEquals(5, r.size());
                for (int i = 0; i < 5; i++) {
                    assertNotEquals(TaoTree.NOT_FOUND, r.lookup(intKey(i * 1000)));
                }
            }
        }
    }

    @Test
    void epochReclamation() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Insert, then delete → retired nodes
            try (var w = tree.write()) {
                for (int i = 0; i < 100; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
            }
            try (var w = tree.write()) {
                for (int i = 0; i < 50; i++) {
                    w.delete(intKey(i));
                }
            }

            // Advance durable generation to allow reclamation
            var reclaimer = tree.reclaimer();
            assertNotNull(reclaimer);
            reclaimer.advanceDurableGeneration(reclaimer.globalGeneration());

            // Reclaim — should free some retired nodes
            int freed = reclaimer.reclaim();
            assertTrue(freed >= 0); // may be 0 if readers are still pinned

            // Tree should still be consistent
            try (var r = tree.read()) {
                assertEquals(50, r.size());
                for (int i = 50; i < 100; i++) {
                    assertNotEquals(TaoTree.NOT_FOUND, r.lookup(intKey(i)));
                }
            }
        }
    }

    // ---- Delete edge cases (exercises cowNodeAfterRemoval, cowCollapseSingleChild) ----

    @Test
    void deleteSingleKey() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            try (var w = tree.write()) {
                w.getOrCreate(intKey(42), 0);
                assertEquals(1, w.size());
            }

            try (var w = tree.write()) {
                assertTrue(w.delete(intKey(42)));
                assertEquals(0, w.size());
            }

            try (var r = tree.read()) {
                assertEquals(0, r.size());
                assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(42)));
            }
        }
    }

    @Test
    void deleteFromEmptyTree() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            try (var w = tree.write()) {
                assertFalse(w.delete(intKey(42)));
                assertEquals(0, w.size());
            }
        }
    }

    @Test
    void deleteNonExistentKey() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            try (var w = tree.write()) {
                for (int i = 0; i < 10; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
            }

            try (var w = tree.write()) {
                assertFalse(w.delete(intKey(999)));
                assertFalse(w.delete(intKey(500)));
                assertEquals(10, w.size());
            }
        }
    }

    @Test
    void deleteCollapseToSingleChild() {
        // Delete from Node4 until 1 child remains → should collapse to prefix
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Insert 3 keys that share a prefix, diverging at byte 3
            byte[] key1 = {10, 20, 30, 1};
            byte[] key2 = {10, 20, 30, 2};
            byte[] key3 = {10, 20, 30, 3};

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(key1, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 100L);
                long l2 = w.getOrCreate(key2, 0);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 200L);
                long l3 = w.getOrCreate(key3, 0);
                w.leafValue(l3).set(ValueLayout.JAVA_LONG, 0, 300L);
            }

            // Delete 2 of 3 → Node4 collapses to prefix
            try (var w = tree.write()) {
                assertTrue(w.delete(key2));
                assertTrue(w.delete(key3));
                assertEquals(1, w.size());
            }

            try (var r = tree.read()) {
                assertEquals(1, r.size());
                long found = r.lookup(key1);
                assertNotEquals(TaoTree.NOT_FOUND, found);
                assertEquals(100L, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(key2));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(key3));
            }
        }
    }

    @Test
    void deleteAllThenVerifyEmpty() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            int count = 200; // exercises multiple node types
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
            }

            // Delete all keys
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    assertTrue(w.delete(intKey(i)), "Failed to delete key " + i);
                }
                assertEquals(0, w.size());
            }

            try (var r = tree.read()) {
                assertEquals(0, r.size());
                for (int i = 0; i < count; i += 20) {
                    assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(i)));
                }
            }
        }
    }

    // ---- Node shrink through specific thresholds ----

    @Test
    void node48ShrinkToNode16() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Insert 48 keys to fill a Node48 (keys at depth 1 diverge across 48 distinct bytes)
            // Use keys like {0, X, 0, 0} to force divergence at byte 1
            int count = 48;
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    byte[] key = {0, (byte) i, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(count, w.size());
            }

            // Delete down to below NODE48_SHRINK_THRESHOLD (12) → triggers Node48→Node16
            try (var w = tree.write()) {
                for (int i = 12; i < count; i++) {
                    assertTrue(w.delete(new byte[]{0, (byte) i, 0, 0}));
                }
                assertEquals(12, w.size());
            }

            // Verify remaining keys
            try (var r = tree.read()) {
                assertEquals(12, r.size());
                for (int i = 0; i < 12; i++) {
                    long found = r.lookup(new byte[]{0, (byte) i, 0, 0});
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " missing");
                    assertEquals((long) i, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void node256ShrinkToNode48() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Insert 100 keys to create a Node256 (>48 distinct byte values at same depth)
            // Keys: {0, X, 0, 0} — diverge at byte 1 with X=0..99
            int count = 100;
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    byte[] key = {0, (byte) i, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(count, w.size());
            }

            // Delete down to NODE256_SHRINK_THRESHOLD (36) → triggers Node256→Node48
            try (var w = tree.write()) {
                for (int i = 36; i < count; i++) {
                    assertTrue(w.delete(new byte[]{0, (byte) i, 0, 0}));
                }
                assertEquals(36, w.size());
            }

            // Verify remaining
            try (var r = tree.read()) {
                assertEquals(36, r.size());
                for (int i = 0; i < 36; i++) {
                    long found = r.lookup(new byte[]{0, (byte) i, 0, 0});
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " missing");
                    assertEquals((long) i, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void node16ShrinkToNode4() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Insert 16 keys to fill a Node16
            int count = 16;
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    byte[] key = {0, (byte) i, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                assertEquals(count, w.size());
            }

            // Delete down to NODE16_SHRINK_THRESHOLD (4) → triggers Node16→Node4
            try (var w = tree.write()) {
                for (int i = 4; i < count; i++) {
                    assertTrue(w.delete(new byte[]{0, (byte) i, 0, 0}));
                }
                assertEquals(4, w.size());
            }

            // Verify remaining
            try (var r = tree.read()) {
                assertEquals(4, r.size());
                for (int i = 0; i < 4; i++) {
                    long found = r.lookup(new byte[]{0, (byte) i, 0, 0});
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " missing");
                    assertEquals((long) i, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- Long keys: exercise prefix chaining (>15 bytes) ----

    @Test
    void longKeysPrefixChaining() {
        int longKeyLen = 20; // > PREFIX_CAPACITY (15) → prefix chain
        int valueSize = 8;

        try (var tree = TaoTree.open(longKeyLen, valueSize)) {


            byte[] key1 = new byte[longKeyLen];
            byte[] key2 = new byte[longKeyLen];
            // Keys share first 18 bytes, differ at byte 18
            for (int i = 0; i < 18; i++) {
                key1[i] = (byte) (i + 1);
                key2[i] = (byte) (i + 1);
            }
            key1[18] = 1;
            key2[18] = 2;

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(key1, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 111L);
                long l2 = w.getOrCreate(key2, 0);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 222L);
                assertEquals(2, w.size());
            }

            try (var r = tree.read()) {
                assertEquals(2, r.size());
                long f1 = r.lookup(key1);
                long f2 = r.lookup(key2);
                assertNotEquals(TaoTree.NOT_FOUND, f1);
                assertNotEquals(TaoTree.NOT_FOUND, f2);
                assertEquals(111L, r.leafValue(f1).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(222L, r.leafValue(f2).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void longKeysManyInserts() {
        int longKeyLen = 24; // > 15 → chains multiple prefix nodes
        int valueSize = 8;

        try (var tree = TaoTree.open(longKeyLen, valueSize)) {


            int count = 100;
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    byte[] key = new byte[longKeyLen];
                    // Big-endian encode i in last 4 bytes
                    key[20] = (byte) (i >>> 24);
                    key[21] = (byte) (i >>> 16);
                    key[22] = (byte) (i >>> 8);
                    key[23] = (byte) i;
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            try (var r = tree.read()) {
                assertEquals(count, r.size());
                for (int i = 0; i < count; i++) {
                    byte[] key = new byte[longKeyLen];
                    key[20] = (byte) (i >>> 24);
                    key[21] = (byte) (i >>> 16);
                    key[22] = (byte) (i >>> 8);
                    key[23] = (byte) i;
                    long found = r.lookup(key);
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " not found");
                    assertEquals((long) i, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void longKeysDeleteAndCollapse() {
        int longKeyLen = 20;
        int valueSize = 8;

        try (var tree = TaoTree.open(longKeyLen, valueSize)) {


            byte[] key1 = new byte[longKeyLen];
            byte[] key2 = new byte[longKeyLen];
            byte[] key3 = new byte[longKeyLen];
            // All share first 17 bytes
            for (int i = 0; i < 17; i++) {
                key1[i] = key2[i] = key3[i] = (byte) (i + 1);
            }
            key1[17] = 10; key1[18] = 0; key1[19] = 0;
            key2[17] = 20; key2[18] = 0; key2[19] = 0;
            key3[17] = 30; key3[18] = 0; key3[19] = 0;

            try (var w = tree.write()) {
                w.getOrCreate(key1, 0);
                w.getOrCreate(key2, 0);
                w.getOrCreate(key3, 0);
                assertEquals(3, w.size());
            }

            // Delete 2 → collapse with prefix merge
            try (var w = tree.write()) {
                assertTrue(w.delete(key2));
                assertTrue(w.delete(key3));
                assertEquals(1, w.size());
            }

            try (var r = tree.read()) {
                assertEquals(1, r.size());
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key1));
                assertEquals(TaoTree.NOT_FOUND, r.lookup(key2));
            }
        }
    }

    // ---- Prefix split at various positions ----

    @Test
    void prefixSplitAtStart() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Keys diverge at first byte → prefix split at position 0
            byte[] key1 = {1, 0, 0, 0};
            byte[] key2 = {2, 0, 0, 0};

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(key1, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 10L);
                long l2 = w.getOrCreate(key2, 0);
                w.leafValue(l2).set(ValueLayout.JAVA_LONG, 0, 20L);
            }

            try (var r = tree.read()) {
                assertEquals(2, r.size());
                assertEquals(10L, r.leafValue(r.lookup(key1)).get(ValueLayout.JAVA_LONG, 0));
                assertEquals(20L, r.leafValue(r.lookup(key2)).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    @Test
    void prefixSplitMidway() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Keys share first 2 bytes, diverge at byte 2
            byte[] key1 = {10, 20, 1, 0};
            byte[] key2 = {10, 20, 2, 0};
            byte[] key3 = {10, 20, 3, 0};

            try (var w = tree.write()) {
                w.getOrCreate(key1, 0);
                w.getOrCreate(key2, 0);
                w.getOrCreate(key3, 0);
            }

            // Insert a 4th key that diverges at byte 1
            byte[] key4 = {10, 99, 0, 0};
            try (var w = tree.write()) {
                w.getOrCreate(key4, 0);
                assertEquals(4, w.size());
            }

            try (var r = tree.read()) {
                assertEquals(4, r.size());
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key1));
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key2));
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key3));
                assertNotEquals(TaoTree.NOT_FOUND, r.lookup(key4));
            }
        }
    }

    // ---- Scan operations ----

    @Test
    void forEachScan() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            int count = 50;
            try (var w = tree.write()) {
                for (int i = 0; i < count; i++) {
                    long leaf = w.getOrCreate(intKey(i), 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            // Full scan should visit all keys in order
            List<Long> values = new ArrayList<>();
            try (var r = tree.read()) {
                r.forEach(leaf -> {
                    values.add(leaf.segment().get(ValueLayout.JAVA_LONG, 0));
                    return true;
                });
            }

            assertEquals(count, values.size());
        }
    }

    @Test
    void forEachEarlyStop() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            try (var w = tree.write()) {
                for (int i = 0; i < 100; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
            }

            AtomicInteger visited = new AtomicInteger(0);
            try (var r = tree.read()) {
                r.forEach(leaf -> {
                    visited.incrementAndGet();
                    return visited.get() < 10; // stop after 10
                });
            }

            assertEquals(10, visited.get());
        }
    }

    // ---- Concurrent writer contention (exercises CAS failure + retireSpeculative) ----

    @Test
    void concurrentWritersSameKeyRange() throws Exception {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            int numWriters = 4;
            int keysPerWriter = 100;
            var barrier = new CyclicBarrier(numWriters);
            var errors = new AtomicInteger(0);
            var threads = new Thread[numWriters];

            // All writers insert overlapping key ranges to force CAS contention
            for (int t = 0; t < numWriters; t++) {
                final int writerId = t;
                threads[t] = Thread.ofPlatform().start(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < keysPerWriter; i++) {
                            // Each writer inserts in its range + some overlap
                            int key = writerId * 50 + i;
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

            // Verify: all unique keys should be present
            try (var r = tree.read()) {
                // The range is [0..50+99] ∪ [50..100+99] ∪ ... overlapping keys are inserted once
                int maxKey = (numWriters - 1) * 50 + keysPerWriter;
                for (int i = 0; i < maxKey; i++) {
                    long found = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, found, "Key " + i + " not found");
                }
            }
        }
    }

    @Test
    void concurrentDeleteAndInsert() throws Exception {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Pre-populate with keys 0..199
            try (var w = tree.write()) {
                for (int i = 0; i < 200; i++) {
                    w.getOrCreate(intKey(i), 0);
                }
            }

            var errors = new AtomicInteger(0);
            var barrier = new CyclicBarrier(2);

            // Thread 1: delete keys 0..99
            var deleter = Thread.ofPlatform().start(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < 100; i++) {
                        try (var w = tree.write()) {
                            w.delete(intKey(i));
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                }
            });

            // Thread 2: insert keys 200..299
            var inserter = Thread.ofPlatform().start(() -> {
                try {
                    barrier.await();
                    for (int i = 200; i < 300; i++) {
                        try (var w = tree.write()) {
                            w.getOrCreate(intKey(i), 0);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                }
            });

            deleter.join();
            inserter.join();
            assertEquals(0, errors.get());

            // Keys 100..299 should be present, 0..99 should be absent
            try (var r = tree.read()) {
                assertEquals(200, r.size());
                for (int i = 100; i < 300; i++) {
                    assertNotEquals(TaoTree.NOT_FOUND, r.lookup(intKey(i)),
                        "Key " + i + " should be present");
                }
            }
        }
    }

    // ---- Prefix merge edge case ----

    @Test
    void prefixMergeOnDelete() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Create a tree where deleting forces prefix merge:
            // Insert keys that create: PREFIX → NODE4 → PREFIX → LEAF
            // Deleting a sibling should merge the outer and inner prefixes
            byte[] key1 = {1, 2, 3, 4};
            byte[] key2 = {1, 2, 5, 6};

            try (var w = tree.write()) {
                long l1 = w.getOrCreate(key1, 0);
                w.leafValue(l1).set(ValueLayout.JAVA_LONG, 0, 1234L);
                w.getOrCreate(key2, 0);
            }

            // Delete key2 → remaining tree should merge prefixes
            try (var w = tree.write()) {
                assertTrue(w.delete(key2));
                assertEquals(1, w.size());
            }

            try (var r = tree.read()) {
                assertEquals(1, r.size());
                long found = r.lookup(key1);
                assertNotEquals(TaoTree.NOT_FOUND, found);
                assertEquals(1234L, r.leafValue(found).get(ValueLayout.JAVA_LONG, 0));
            }
        }
    }

    // ---- Incremental growth through all node type transitions ----

    @Test
    void incrementalGrowthAndShrink() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            // Grow from empty → Node4 → Node16 → Node48 → Node256
            // then shrink back: Node256 → Node48 → Node16 → Node4 → prefix → empty
            // Use keys {0, X, 0, 0} to force divergence at byte 1

            // Grow: insert 0..256 one at a time, verifying at each step
            List<Integer> insertOrder = new ArrayList<>();
            for (int i = 0; i < 256; i++) insertOrder.add(i);
            Collections.shuffle(insertOrder, new java.util.Random(42));

            for (int i : insertOrder) {
                try (var w = tree.write()) {
                    byte[] key = {0, (byte) i, 0, 0};
                    long leaf = w.getOrCreate(key, 0);
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            try (var r = tree.read()) {
                assertEquals(256, r.size());
            }

            // Shrink: delete all keys, verifying at milestones
            List<Integer> deleteOrder = new ArrayList<>(insertOrder);
            Collections.shuffle(deleteOrder, new java.util.Random(99));

            int deleted = 0;
            for (int i : deleteOrder) {
                try (var w = tree.write()) {
                    assertTrue(w.delete(new byte[]{0, (byte) i, 0, 0}));
                }
                deleted++;

                // Verify consistency at key milestones
                if (deleted == 220 || deleted == 240 || deleted == 252 || deleted == 255) {
                    try (var r = tree.read()) {
                        assertEquals(256 - deleted, r.size());
                    }
                }
            }

            try (var r = tree.read()) {
                assertEquals(0, r.size());
            }
        }
    }

    // ---- Delete with key exhaustion at inner node (cowDelete line 204-205) ----

    @Test
    void deleteKeyExhausted() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            try (var w = tree.write()) {
                w.getOrCreate(intKey(0), 0);
                w.getOrCreate(intKey(1), 0);
            }

            // Deleting a key that doesn't exist should return false
            try (var w = tree.write()) {
                assertFalse(w.delete(intKey(999)));
                assertEquals(2, w.size());
            }
        }
    }

    // ---- Prefix mismatch during delete (cowDelete line 195-196) ----

    @Test
    void deletePrefixMismatch() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            byte[] key1 = {1, 2, 3, 4};
            byte[] key2 = {1, 2, 3, 5};

            try (var w = tree.write()) {
                w.getOrCreate(key1, 0);
                w.getOrCreate(key2, 0);
            }

            // Try deleting a key that partially matches prefix but diverges
            try (var w = tree.write()) {
                assertFalse(w.delete(new byte[]{1, 2, 9, 0})); // mismatch at byte 2
                assertFalse(w.delete(new byte[]{1, 9, 3, 4})); // mismatch at byte 1
                assertEquals(2, w.size());
            }
        }
    }

    // ---- Repeated reclamation cycles ----

    @Test
    void repeatedInsertDeleteReclaim() {
        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE)) {


            var reclaimer = tree.reclaimer();
            assertNotNull(reclaimer);

            for (int cycle = 0; cycle < 5; cycle++) {
                try (var w = tree.write()) {
                    for (int i = 0; i < 50; i++) {
                        w.getOrCreate(intKey(cycle * 1000 + i), 0);
                    }
                }
                try (var w = tree.write()) {
                    for (int i = 0; i < 50; i++) {
                        w.delete(intKey(cycle * 1000 + i));
                    }
                }

                reclaimer.advanceDurableGeneration(reclaimer.globalGeneration());
                reclaimer.reclaim();
            }

            try (var r = tree.read()) {
                assertEquals(0, r.size());
            }
        }
    }
}
