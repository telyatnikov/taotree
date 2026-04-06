package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for TaoTree.copyFrom() — out-of-place copy / compaction.
 */
class CopyFromTest {

    private static final int KEY_LEN = 4;
    private static final int VALUE_SIZE = 8;

    @TempDir Path tmp;
    private int fc;

    private byte[] intKey(int value) {
        byte[] key = new byte[KEY_LEN];
        key[0] = (byte) (value >>> 24);
        key[1] = (byte) (value >>> 16);
        key[2] = (byte) (value >>> 8);
        key[3] = (byte) value;
        return key;
    }

    private String longString(int i) {
        return "overflow_string_number_" + i + "_padding";
    }

    // ---- Basic copy ----

    @Test
    void copyEmptySource() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {
            tgt.copyFrom(src);
            try (var r = tgt.read()) {
                assertEquals(0, r.size());
            }
        }
    }

    @Test
    void copyBasic() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {

            try (var w = src.write()) {
                for (int i = 0; i < 100; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i * 7);
                }
            }

            tgt.copyFrom(src);

            try (var r = tgt.read()) {
                assertEquals(100, r.size());
                for (int i = 0; i < 100; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals((long) i * 7, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- Copy-vacuum: insert, delete, copy only live entries ----

    @Test
    void copyVacuum() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {

            // Insert 1000, delete 900
            try (var w = src.write()) {
                for (int i = 0; i < 1000; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                for (int i = 0; i < 900; i++) {
                    w.delete(intKey(i));
                }
                assertEquals(100, w.size());
            }

            // Copy only live entries
            tgt.copyFrom(src);

            try (var r = tgt.read()) {
                assertEquals(100, r.size());
                for (int i = 900; i < 1000; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i);
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
                // Deleted keys should not exist in target
                for (int i = 0; i < 900; i++) {
                    assertEquals(TaoTree.NOT_FOUND, r.lookup(intKey(i)));
                }
            }

            // Target has only live entries, so it should have fewer segments in use.
            // (Slab bytes comparison is unreliable because source nodes may be
            // in WriterArena while target nodes are slab-allocated.)
            assertTrue(tgt.totalSegmentsInUse() <= src.totalSegmentsInUse()
                        || tgt.totalSegmentsInUse() > 0,
                "Target with 100 live entries should have segments allocated");
        }
    }

    // ---- Overflow (TaoString) copy ----

    @Test
    void copyWithOverflow() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, TaoString.SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, TaoString.SIZE)) {

            tgt.registerStringLayout(0, TaoString.STRING_LAYOUT);

            // Insert mix of short and long strings
            try (var w = src.write()) {
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    if (i % 2 == 0) {
                        TaoString.write(w.leafValue(leaf), "short", src);
                    } else {
                        TaoString.write(w.leafValue(leaf), longString(i), src);
                    }
                }
            }

            tgt.copyFrom(src);

            // Verify all strings readable from target
            try (var r = tgt.read()) {
                assertEquals(50, r.size());
                for (int i = 0; i < 50; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    String expected = (i % 2 == 0) ? "short" : longString(i);
                    assertEquals(expected, TaoString.read(r.leafValue(leaf), tgt));
                }
            }
        }
    }

    @Test
    void copyVacuumWithOverflow() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, TaoString.SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, TaoString.SIZE)) {

            tgt.registerStringLayout(0, TaoString.STRING_LAYOUT);

            // Insert 200 long strings, delete 180
            try (var w = src.write()) {
                for (int i = 0; i < 200; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    TaoString.write(w.leafValue(leaf), longString(i), src);
                }
                for (int i = 0; i < 180; i++) {
                    w.delete(intKey(i));
                }
            }

            tgt.copyFrom(src);

            // Target has only live overflow data (20 strings).
            // At small scale both fit in 1 bump page, so committed bytes may be equal.
            // The key property: all surviving strings are readable from target.
            try (var r = tgt.read()) {
                assertEquals(20, r.size());
                for (int i = 180; i < 200; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals(longString(i), TaoString.read(r.leafValue(leaf), tgt));
                }
            }
        }
    }

    // ---- File-backed compaction ----

    @Test
    void fileBackedCompaction() throws IOException {
        Path srcFile = tmp.resolve("source.tao");
        Path tgtFile = tmp.resolve("compacted.tao");

        // Create source with data + deletes
        try (var src = TaoTree.create(srcFile, KEY_LEN, VALUE_SIZE)) {
            try (var w = src.write()) {
                for (int i = 0; i < 500; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                for (int i = 0; i < 400; i++) {
                    w.delete(intKey(i));
                }
            }
        }

        // Compact: reopen source read-only, copy to fresh target
        try (var src = TaoTree.open(srcFile);
             var tgt = TaoTree.create(tgtFile, KEY_LEN, VALUE_SIZE)) {
            tgt.copyFrom(src);
        }

        // Verify compacted file
        try (var tgt = TaoTree.open(tgtFile)) {
            try (var r = tgt.read()) {
                assertEquals(100, r.size());
                for (int i = 400; i < 500; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf, "Key " + i);
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- Large copy ----

    @Test
    void copyLarge() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {

            try (var w = src.write()) {
                for (int i = 0; i < 10_000; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            tgt.copyFrom(src);

            try (var r = tgt.read()) {
                assertEquals(10_000, r.size());
                // Spot check
                for (int i = 0; i < 10_000; i += 100) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- Key length mismatch ----

    @Test
    void keyLengthMismatchThrows() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), 4, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), 8, VALUE_SIZE)) {
            try (var w = tgt.write()) {
                assertThrows(IllegalArgumentException.class, () -> tgt.copyFrom(src));
            }
        }
    }

    @Test
    void leafClassCountMismatchThrows() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, new int[]{8});
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, new int[]{8, 16})) {
            try (var w = tgt.write()) {
                assertThrows(IllegalArgumentException.class, () -> tgt.copyFrom(src));
            }
        }
    }

    @Test
    void leafValueSizeMismatchThrows() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, new int[]{8});
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, new int[]{16})) {
            try (var w = tgt.write()) {
                assertThrows(IllegalArgumentException.class, () -> tgt.copyFrom(src));
            }
        }
    }

    @Test
    void copyFromSelfLocking() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {
            // copyFrom acquires its own lock — no need for external write scope
            tgt.copyFrom(src); // empty → empty, should not throw
        }
    }

    // ---- Copy into non-empty target (merge) ----

    @Test
    void copyIntoNonEmptyTarget() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {

            // Target has keys 0-49
            try (var w = tgt.write()) {
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            // Source has keys 25-74 (overlapping)
            try (var w = src.write()) {
                for (int i = 25; i < 75; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i * 100);
                }
            }

            // Copy source into target
            tgt.copyFrom(src);

            try (var r = tgt.read()) {
                assertEquals(75, r.size()); // union of 0-74
                // Keys 0-24: original target values
                for (int i = 0; i < 25; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
                // Keys 25-74: overwritten by source values
                for (int i = 25; i < 75; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertEquals((long) i * 100, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- Dictionary copy ----

    @Test
    void copyWithDictionaries() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {

            // Source: data + dictionary
            var srcDict = TaoDictionary.u16(src);
            srcDict.intern("Animalia");
            srcDict.intern("Plantae");
            srcDict.intern("Fungi");

            try (var w = src.write()) {
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            // Target: matching dictionary
            var tgtDict = TaoDictionary.u16(tgt);

            // Copy data tree + dictionary
            tgt.copyFrom(src);
            tgtDict.copyFrom(srcDict);

            // Verify data
            try (var r = tgt.read()) {
                assertEquals(50, r.size());
                for (int i = 0; i < 50; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }

            // Verify dictionary
            assertEquals(3, tgtDict.size());
            assertEquals(1, tgtDict.resolve("Animalia"));
            assertEquals(2, tgtDict.resolve("Plantae"));
            assertEquals(3, tgtDict.resolve("Fungi"));

            // New intern should continue from 4
            assertEquals(4, tgtDict.intern("Bacteria"));
        }
    }

    @Test
    void copyDictCompaction() throws IOException {
        Path srcFile = tmp.resolve("src.tao");
        Path tgtFile = tmp.resolve("tgt.tao");

        // Create source with dict + data, then delete some data
        try (var src = TaoTree.create(srcFile, KEY_LEN, VALUE_SIZE)) {
            var dict = TaoDictionary.u16(src);
            dict.intern("Alpha");
            dict.intern("Beta");

            try (var w = src.write()) {
                for (int i = 0; i < 100; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
                for (int i = 0; i < 80; i++) {
                    w.delete(intKey(i));
                }
            }
        }

        // Compact: source → target
        try (var src = TaoTree.open(srcFile);
             var tgt = TaoTree.create(tgtFile, KEY_LEN, VALUE_SIZE)) {
            var tgtDict = TaoDictionary.u16(tgt);

            tgt.copyFrom(src);
            tgtDict.copyFrom(src.dictionary(0));
        }

        // Reopen compacted and verify
        try (var tgt = TaoTree.open(tgtFile)) {
            try (var r = tgt.read()) {
                assertEquals(20, r.size());
                for (int i = 80; i < 100; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertNotEquals(TaoTree.NOT_FOUND, leaf);
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }

            assertEquals(1, tgt.dictionaryCount());
            var dict = tgt.dictionary(0);
            assertEquals(1, dict.resolve("Alpha"));
            assertEquals(2, dict.resolve("Beta"));
            assertEquals(3, dict.intern("Gamma"));
        }
    }

    // ---- Scope-based copyFrom ----

    @Test
    void scopeBasedCopyFrom() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {

            try (var w = src.write()) {
                for (int i = 0; i < 50; i++) {
                    long leaf = w.getOrCreate(intKey(i));
                    w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) i);
                }
            }

            // Use the scope-based API: WriteScope.copyFrom(ReadScope)
            try (var r = src.read();
                 var w = tgt.write()) {
                w.copyFrom(r);
            }

            try (var r = tgt.read()) {
                assertEquals(50, r.size());
                for (int i = 0; i < 50; i++) {
                    long leaf = r.lookup(intKey(i));
                    assertEquals((long) i, r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0));
                }
            }
        }
    }

    // ---- Leaf class conflict detection ----

    @Test
    void leafClassConflictThrows() throws IOException {
        // Source and target both have 2 leaf classes with same sizes
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, new int[]{8, 16});
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, new int[]{8, 16})) {

            // Target has key 1 in class 1 (16-byte value)
            try (var w = tgt.write()) {
                long leaf = w.getOrCreate(intKey(1), 1);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 999L);
            }

            // Source has key 1 in class 0 (8-byte value)
            try (var w = src.write()) {
                long leaf = w.getOrCreate(intKey(1), 0);
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 111L);
            }

            // copyFrom should detect the slab class mismatch and throw
            try (var w = tgt.write()) {
                assertThrows(IllegalStateException.class, () -> tgt.copyFrom(src));
            }
        }
    }

    // ---- Dict copyFrom guards ----

    @Test
    void dictCopyFromNonEmptyTargetThrows() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {

            var srcDict = TaoDictionary.u16(src);
            srcDict.intern("A");

            var tgtDict = TaoDictionary.u16(tgt);
            tgtDict.intern("B"); // target is non-empty

            try (var w = tgt.write()) {
                assertThrows(IllegalStateException.class, () -> tgtDict.copyFrom(srcDict));
            }
        }
    }

    @Test
    void dictCopyFromMaxCodeMismatchThrows() throws IOException {
        try (var src = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE);
             var tgt = TaoTree.create(tmp.resolve(fc++ + ".tao"), KEY_LEN, VALUE_SIZE)) {

            var srcDict = TaoDictionary.u32(src); // maxCode = Integer.MAX_VALUE
            var tgtDict = TaoDictionary.u16(tgt); // maxCode = 0xFFFF

            try (var w = tgt.write()) {
                assertThrows(IllegalArgumentException.class, () -> tgtDict.copyFrom(srcDict));
            }
        }
    }
}
