package org.taotree;

import org.junit.jupiter.api.Test;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrency smoke tests for ART with shared tree.
 * Uses Arena.ofShared() for cross-thread access.
 */
class ConcurrencyTest {

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

    @Test
    void oneWriterMultipleReaders() throws Exception {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {

            int numEntries = 10_000;
            int numReaders = 4;
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var writerDone = new AtomicBoolean(false);
            var insertedCount = new AtomicInteger(0);

            // Writer thread: inserts keys 0..numEntries-1
            var writer = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < numEntries; i++) {
                        try (var w = tree.write()) {
                            long leaf = w.getOrCreate(intKey(i), 0);
                            w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                        }
                        insertedCount.incrementAndGet();
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    writerDone.set(true);
                }
            });

            // Reader threads: continuously look up random keys
            var readers = new ArrayList<Thread>();
            for (int r = 0; r < numReaders; r++) {
                readers.add(Thread.ofVirtual().start(() -> {
                    try {
                        var rng = ThreadLocalRandom.current();
                        while (!writerDone.get() || insertedCount.get() < numEntries) {
                            int maxKey = insertedCount.get();
                            if (maxKey <= 0) { Thread.yield(); continue; }

                            int keyToLookup = rng.nextInt(maxKey);
                            try (var read = tree.read()) {
                                long leaf = read.lookup(intKey(keyToLookup));
                                if (leaf != TaoTree.NOT_FOUND) {
                                    long value = read.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0);
                                    // Value must be the key itself (set by writer)
                                    assertEquals(keyToLookup, value,
                                        "Corrupted value for key " + keyToLookup);
                                }
                                // It's ok if leaf is EMPTY — writer might not have inserted it yet
                            }
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }));
            }

            writer.join(30_000);
            for (var reader : readers) reader.join(30_000);

            assertTrue(errors.isEmpty(),
                "Errors: " + errors.stream().map(Throwable::getMessage).toList());
            assertEquals(numEntries, insertedCount.get());

            // Final verification: all entries readable
            try (var r = tree.read()) {
                assertEquals(numEntries, r.size());
                for (int i = 0; i < numEntries; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i + " not found after writer done");
                    assertEquals(i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    @Test
    void dictionaryResolveNeverReturnsZero() throws Exception {
        try (var tree = TaoTree.forDictionaries()) {
            var dict = TaoDictionary.u16(tree);

            int numEntries = 1000;
            int numReaders = 4;
            var errors = new ConcurrentLinkedQueue<Throwable>();
            var writerDone = new AtomicBoolean(false);

            // Writer: assigns codes
            var writer = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < numEntries; i++) {
                        int code = dict.intern("name_" + i);
                        assertTrue(code > 0, "intern returned 0 for name_" + i);
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    writerDone.set(true);
                }
            });

            // Readers: resolve existing names
            var readers = new ArrayList<Thread>();
            for (int r = 0; r < numReaders; r++) {
                readers.add(Thread.ofVirtual().start(() -> {
                    try {
                        var rng = ThreadLocalRandom.current();
                        while (!writerDone.get()) {
                            int size = dict.size();
                            if (size <= 0) { Thread.yield(); continue; }

                            int idx = rng.nextInt(size);
                            int code = dict.resolve("name_" + idx);
                            // If found, code must be > 0 (never observe the zero intermediate)
                            if (code != -1) {
                                assertTrue(code > 0,
                                    "Reader saw code 0 for name_" + idx);
                            }
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }));
            }

            writer.join(30_000);
            for (var reader : readers) reader.join(30_000);

            assertTrue(errors.isEmpty(),
                "Errors: " + errors.stream().map(Throwable::getMessage).toList());
        }
    }

    @Test
    void readToWriteUpgradeFailsFast() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {

            // Acquire read lock
            try (var read = tree.read()) {
                // Attempting to acquire write lock should throw, not deadlock
                assertThrows(IllegalStateException.class, () -> tree.write());
            }
        }
    }

    @Test
    void readToWriteUpgradeViaDict() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            var dict = TaoDictionary.u16(tree);

            // Acquire read lock
            try (var read = tree.read()) {
                // Attempting intern (which needs write lock) should throw
                assertThrows(IllegalStateException.class,
                    () -> dict.intern("test"));
            }
        }
    }

    @Test
    void writeReentrantWithDict() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            var dict = TaoDictionary.u16(tree);

            // Write lock is reentrant — dict calls inside write scope should work
            try (var w = tree.write()) {
                int code = dict.intern("test");
                assertTrue(code > 0);
                // Also resolve under write lock (demotes to read internally, reentrant)
                assertEquals(code, dict.resolve("test"));
            }
        }
    }

    @Test
    void readReentrantWithDict() {
        try (var tree = TaoTree.open(KEY_LEN, new int[]{VALUE_SIZE})) {
            var dict = TaoDictionary.u16(tree);

            // Populate
            dict.intern("Animalia");

            // Read lock is reentrant — dict.resolve inside read scope should work
            try (var read = tree.read()) {
                int code = dict.resolve("Animalia");
                assertTrue(code > 0);
            }
        }
    }
}
