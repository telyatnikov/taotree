package org.taotree;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.art.Node256;
import org.taotree.internal.art.Node4;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;
import org.taotree.layout.LeafField;
import org.taotree.layout.LeafLayout;
import org.taotree.layout.QueryBuilder;

/**
 * Tests for scan/forEach APIs: QueryBuilder, prefix scan, ordered traversal.
 */
class ScanTest {

    @TempDir Path tmp;
    private int fc;

    // ---- Full scan ----

    @Test
    void forEachEmptyTree() throws IOException {
        var keyLayout = KeyLayout.of(KeyField.uint32("id"));
        var leafLayout = LeafLayout.of(LeafField.int32("value"));
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var count = new AtomicInteger();
            try (var r = tree.read()) {
                r.forEach(leaf -> { count.incrementAndGet(); return true; });
            }
            assertEquals(0, count.get());
        }
    }

    @Test
    void forEachAllEntries() throws IOException {
        var keyLayout = KeyLayout.of(KeyField.uint32("id"));
        var leafLayout = LeafLayout.of(LeafField.int32("value"));
        var VALUE = leafLayout.int32("value");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var ID = tree.keyUint32("id");
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                try (var w = tree.write()) {
                    for (int i = 1; i <= 100; i++) {
                        kb.set(ID, i);
                        w.getOrCreate(kb).set(VALUE, i);
                    }
                }
            }

            var values = new ArrayList<Integer>();
            try (var r = tree.read()) {
                r.forEach(leaf -> { values.add(leaf.get(VALUE)); return true; });
            }
            assertEquals(100, values.size());
            // Verify lexicographic order (uint32 big-endian = numeric order)
            for (int i = 1; i < values.size(); i++) {
                assertTrue(values.get(i) > values.get(i - 1),
                    "Values must be in ascending order: " + values.get(i - 1) + " >= " + values.get(i));
            }
        }
    }

    @Test
    void forEachEarlyTermination() throws IOException {
        var keyLayout = KeyLayout.of(KeyField.uint32("id"));
        var leafLayout = LeafLayout.of(LeafField.int32("value"));
        var VALUE = leafLayout.int32("value");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var ID = tree.keyUint32("id");
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                try (var w = tree.write()) {
                    for (int i = 1; i <= 100; i++) {
                        kb.set(ID, i);
                        w.getOrCreate(kb).set(VALUE, i);
                    }
                }
            }

            var count = new AtomicInteger();
            try (var r = tree.read()) {
                boolean completed = r.forEach(leaf -> {
                    count.incrementAndGet();
                    return count.get() < 10; // stop after 10
                });
                assertFalse(completed, "Should report early termination");
            }
            assertEquals(10, count.get());
        }
    }

    // ---- Prefix scan with dict-encoded keys ----

    @Test
    void scanPrefixWithDicts() throws IOException {
        var keyLayout = KeyLayout.of(
            KeyField.dict16("category"),
            KeyField.uint32("id")
        );
        var leafLayout = LeafLayout.of(LeafField.int32("value"));
        var VALUE = leafLayout.int32("value");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var CATEGORY = tree.keyDict16("category");
            var ID = tree.keyUint32("id");

            // Insert: 3 categories × 5 items = 15 entries
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                try (var w = tree.write()) {
                    for (String cat : new String[]{"Alpha", "Beta", "Gamma"}) {
                        for (int i = 1; i <= 5; i++) {
                            kb.set(CATEGORY, cat).set(ID, i);
                            w.getOrCreate(kb).set(VALUE, i);
                        }
                    }
                }

                // Full scan: all 15
                var count = new AtomicInteger();
                try (var r = tree.read()) {
                    r.forEach(leaf -> { count.incrementAndGet(); return true; });
                }
                assertEquals(15, count.get());

                // Prefix scan: only "Beta" entries
                var qb = tree.newQueryBuilder(arena);
                qb.set(CATEGORY, "Beta");
                var betaValues = new ArrayList<Integer>();
                try (var r = tree.read()) {
                    r.scan(qb, CATEGORY, leaf -> { betaValues.add(leaf.get(VALUE)); return true; });
                }
                assertEquals(5, betaValues.size());
            }
        }
    }

    @Test
    void scanUnknownDictValueReturnsEmpty() throws IOException {
        var keyLayout = KeyLayout.of(
            KeyField.dict16("category"),
            KeyField.uint32("id")
        );
        var leafLayout = LeafLayout.of(LeafField.int32("value"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var CATEGORY = tree.keyDict16("category");
            var ID = tree.keyUint32("id");

            // Insert one entry
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(CATEGORY, "Alpha").set(ID, 1);
                try (var w = tree.write()) {
                    w.getOrCreate(kb).set(leafLayout.int32("value"), 42);
                }

                // Query for unknown category — should return empty, no mutation
                var qb = tree.newQueryBuilder(arena);
                qb.set(CATEGORY, "UnknownCategory");
                assertFalse(qb.isSatisfiable(CATEGORY), "Unknown dict value should be unsatisfiable");

                var count = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, CATEGORY, leaf -> { count.incrementAndGet(); return true; });
                }
                assertEquals(0, count.get(), "Scan with unknown value should return empty");

                // Verify no dict mutation occurred
                assertEquals(1, tree.dictionaries().get(0).size(),
                    "Dictionary should not grow from a query");
            }
        }
    }

    @Test
    void scanUnsatisfiablePrefixSkipsTraversal() throws IOException {
        var keyLayout = KeyLayout.of(
            KeyField.dict16("a"),
            KeyField.dict16("b"),
            KeyField.uint32("c")
        );
        var leafLayout = LeafLayout.of(LeafField.int32("value"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var A = tree.keyDict16("a");
            var B = tree.keyDict16("b");

            // Insert data
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(A, "x").set(B, "y").set(tree.keyUint32("c"), 1);
                try (var w = tree.write()) {
                    w.getOrCreate(kb).set(leafLayout.int32("value"), 1);
                }

                // Query: A is known, B is unknown → scan up to B should be empty
                var qb = tree.newQueryBuilder(arena);
                qb.set(A, "x");
                qb.set(B, "UnknownB");
                assertTrue(qb.isSatisfiable(A), "A alone should be satisfiable");
                assertFalse(qb.isSatisfiable(B), "B unknown should be unsatisfiable");

                // Scan up to A works (A is satisfiable)
                var countA = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, A, leaf -> { countA.incrementAndGet(); return true; });
                }
                assertEquals(1, countA.get());

                // Scan up to B returns empty (B is unsatisfiable)
                var countB = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, B, leaf -> { countB.incrementAndGet(); return true; });
                }
                assertEquals(0, countB.get());
            }
        }
    }

    // ---- Prefix scan with compressed prefix nodes ----

    @Test
    void scanPrefixInsideCompressedPrefixNode() throws IOException {
        // Use a key layout where the prefix bytes are likely compressed:
        // [a:2][b:2][c:4] = 8 bytes. With few entries, the ART may
        // compress most of the key into PrefixNodes.
        var keyLayout = KeyLayout.of(
            KeyField.uint16("a"),
            KeyField.uint16("b"),
            KeyField.uint32("c")
        );
        var leafLayout = LeafLayout.of(LeafField.int32("value"));
        var VALUE = leafLayout.int32("value");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var A = tree.keyUint16("a");
            var B = tree.keyUint16("b");
            var C = tree.keyUint32("c");

            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                // Insert: a=1,b=1,c=1..5 and a=1,b=2,c=1..5 and a=2,b=1,c=1..3
                try (var w = tree.write()) {
                    for (int c = 1; c <= 5; c++) {
                        kb.set(A, (short) 1).set(B, (short) 1).set(C, c);
                        w.getOrCreate(kb).set(VALUE, 100 + c);
                    }
                    for (int c = 1; c <= 5; c++) {
                        kb.set(A, (short) 1).set(B, (short) 2).set(C, c);
                        w.getOrCreate(kb).set(VALUE, 200 + c);
                    }
                    for (int c = 1; c <= 3; c++) {
                        kb.set(A, (short) 2).set(B, (short) 1).set(C, c);
                        w.getOrCreate(kb).set(VALUE, 300 + c);
                    }
                }

                // Full scan: 5 + 5 + 3 = 13
                var count = new AtomicInteger();
                try (var r = tree.read()) {
                    r.forEach(leaf -> { count.incrementAndGet(); return true; });
                }
                assertEquals(13, count.get());

                // Prefix a=1: should match 10 entries
                var qb = tree.newQueryBuilder(arena);
                qb.set(A, (short) 1);
                var countA1 = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, A, leaf -> { countA1.incrementAndGet(); return true; });
                }
                assertEquals(10, countA1.get());

                // Prefix a=1,b=2: should match 5 entries
                qb.clear().set(A, (short) 1).set(B, (short) 2);
                var countA1B2 = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, B, leaf -> { countA1B2.incrementAndGet(); return true; });
                }
                assertEquals(5, countA1B2.get());

                // Prefix a=2: should match 3 entries
                qb.clear().set(A, (short) 2);
                var countA2 = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, A, leaf -> { countA2.incrementAndGet(); return true; });
                }
                assertEquals(3, countA2.get());

                // Prefix a=99: no match
                qb.clear().set(A, (short) 99);
                var countMiss = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, A, leaf -> { countMiss.incrementAndGet(); return true; });
                }
                assertEquals(0, countMiss.get());
            }
        }
    }

    // ---- QueryBuilder state tracking ----

    @Test
    void queryBuilderPerFieldState() throws IOException {
        var keyLayout = KeyLayout.of(
            KeyField.dict16("a"),
            KeyField.uint32("b")
        );
        var leafLayout = LeafLayout.of(LeafField.int32("v"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var A = tree.keyDict16("a");
            var B = tree.keyUint32("b");

            try (var arena = Arena.ofConfined()) {
                var qb = tree.newQueryBuilder(arena);

                // Nothing set
                assertFalse(qb.isSatisfiable(A));
                assertFalse(qb.isSatisfiable(B));

                // Set only B (skipping A)
                qb.set(B, 42);
                assertFalse(qb.isSatisfiable(A), "A not set");
                assertFalse(qb.isSatisfiable(B), "A not set, so B prefix is unsatisfiable");

                // Set A with known value
                // First intern "test" so it exists in the dict
                try (var w = tree.write()) {
                    var kb = tree.newKeyBuilder(arena);
                    kb.set(A, "test").set(B, 1);
                    w.getOrCreate(kb);
                }
                qb.clear().set(A, "test").set(B, 42);
                assertTrue(qb.isSatisfiable(A));
                assertTrue(qb.isSatisfiable(B));

                // Clear and set A with unknown value
                qb.clear().set(A, "doesNotExist");
                assertFalse(qb.isSatisfiable(A), "Unknown dict value");
            }
        }
    }

    @Test
    void queryBuilderClearResetsState() throws IOException {
        var keyLayout = KeyLayout.of(KeyField.uint32("a"));
        var leafLayout = LeafLayout.of(LeafField.int32("v"));

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var A = tree.keyUint32("a");
            try (var arena = Arena.ofConfined()) {
                var qb = tree.newQueryBuilder(arena);
                qb.set(A, 42);
                assertTrue(qb.isSatisfiable(A));

                qb.clear();
                assertFalse(qb.isSatisfiable(A), "After clear, field should be unset");
            }
        }
    }

    // ---- Node type coverage ----

    @Test
    void scanCoveringMultipleNodeTypes() throws IOException {
        // Insert enough entries to trigger Node4→16→48→256 growth
        var keyLayout = KeyLayout.of(KeyField.uint32("id"));
        var leafLayout = LeafLayout.of(LeafField.int32("value"));
        var VALUE = leafLayout.int32("value");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var ID = tree.keyUint32("id");
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                // Insert 300 entries — forces Node256 at the first byte level
                try (var w = tree.write()) {
                    for (int i = 0; i < 300; i++) {
                        kb.set(ID, i);
                        w.getOrCreate(kb).set(VALUE, i);
                    }
                }

                var scanned = new AtomicInteger();
                try (var r = tree.read()) {
                    r.forEach(leaf -> { scanned.incrementAndGet(); return true; });
                }
                assertEquals(300, scanned.get());
            }
        }
    }

    // ---- Single entry scan ----

    @Test
    void scanSingleEntry() throws IOException {
        var keyLayout = KeyLayout.of(KeyField.uint32("id"));
        var leafLayout = LeafLayout.of(LeafField.int32("value"));
        var VALUE = leafLayout.int32("value");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var ID = tree.keyUint32("id");
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 42);
                try (var w = tree.write()) {
                    w.getOrCreate(kb).set(VALUE, 99);
                }

                // Full scan of single entry
                var values = new ArrayList<Integer>();
                try (var r = tree.read()) {
                    r.forEach(leaf -> { values.add(leaf.get(VALUE)); return true; });
                }
                assertEquals(1, values.size());
                assertEquals(99, values.getFirst());

                // Prefix scan matching the single entry
                var qb = tree.newQueryBuilder(arena);
                qb.set(ID, 42);
                var prefixValues = new ArrayList<Integer>();
                try (var r = tree.read()) {
                    r.scan(qb, ID, leaf -> { prefixValues.add(leaf.get(VALUE)); return true; });
                }
                assertEquals(1, prefixValues.size());
                assertEquals(99, prefixValues.getFirst());

                // Prefix scan NOT matching
                qb.clear().set(ID, 99);
                var miss = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, ID, leaf -> { miss.incrementAndGet(); return true; });
                }
                assertEquals(0, miss.get());
            }
        }
    }

    // ---- QueryBuilder: exercise all handle types ----

    @Test
    void queryBuilderAllHandleTypes() throws IOException {
        var keyLayout = KeyLayout.of(
            KeyField.uint8("a"),
            KeyField.uint16("b"),
            KeyField.uint32("c"),
            KeyField.uint64("d"),
            KeyField.int64("e")
        );
        var leafLayout = LeafLayout.of(LeafField.int32("v"));
        var VALUE = leafLayout.int32("v");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var A = tree.keyUint8("a");
            var B = tree.keyUint16("b");
            var C = tree.keyUint32("c");
            var D = tree.keyUint64("d");
            var E = tree.keyInt64("e");

            // Verify handle metadata
            assertEquals(1, A.width()); assertEquals(0, A.fieldIndex());
            assertEquals(2, B.width()); assertEquals(1, B.fieldIndex());
            assertEquals(4, C.width()); assertEquals(2, C.fieldIndex());
            assertEquals(8, D.width()); assertEquals(3, D.fieldIndex());
            assertEquals(8, E.width()); assertEquals(4, E.fieldIndex());
            assertEquals(1, A.end());
            assertEquals(23, E.end());

            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(A, (byte) 1).set(B, (short) 2).set(C, 3).set(D, 4L).set(E, -5L);
                try (var w = tree.write()) {
                    w.getOrCreate(kb).set(VALUE, 99);
                }

                // QueryBuilder with all types
                var qb = tree.newQueryBuilder(arena);
                qb.set(A, (byte) 1).set(B, (short) 2).set(C, 3).set(D, 4L).set(E, -5L);
                assertTrue(qb.isSatisfiable(E));
                assertEquals(23, qb.prefixLength(E));

                var found = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(qb, E, leaf -> { found.incrementAndGet(); return true; });
                }
                assertEquals(1, found.get());

                // Partial prefix
                qb.clear().set(A, (byte) 1);
                assertTrue(qb.isSatisfiable(A));
                found.set(0);
                try (var r = tree.read()) {
                    r.scan(qb, A, leaf -> { found.incrementAndGet(); return true; });
                }
                assertEquals(1, found.get());

                // Wrong value → miss
                qb.clear().set(A, (byte) 99);
                found.set(0);
                try (var r = tree.read()) {
                    r.scan(qb, A, leaf -> { found.incrementAndGet(); return true; });
                }
                assertEquals(0, found.get());
            }
        }
    }

    // ---- Handle-based leaf access: exercise all types via scan ----

    @Test
    void scanWithAllLeafHandleTypes() throws IOException {
        var keyLayout = KeyLayout.of(KeyField.uint32("id"));
        var leafLayout = LeafLayout.of(
            LeafField.int8("a"),
            LeafField.int16("b"),
            LeafField.int32("c"),
            LeafField.int64("d"),
            LeafField.float32("e"),
            LeafField.float64("f"),
            LeafField.string("g"),
            LeafField.json("h")
        );

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var ID = tree.keyUint32("id");
            var A = tree.leafInt8("a");
            var B = tree.leafInt16("b");
            var C = tree.leafInt32("c");
            var D = tree.leafInt64("d");
            var E = tree.leafFloat32("e");
            var F = tree.leafFloat64("f");
            var G = tree.leafString("g");
            var H = tree.leafJson("h");

            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 1);
                try (var w = tree.write()) {
                    var leaf = w.getOrCreate(kb);
                    leaf.set(A, (byte) 42)
                        .set(B, (short) 1000)
                        .set(C, 123456)
                        .set(D, Long.MAX_VALUE)
                        .set(E, 3.14f)
                        .set(F, 2.718)
                        .set(G, "hello world")
                        .set(H, "{\"key\":\"value\"}");
                }

                // Verify via scan
                try (var r = tree.read()) {
                    r.forEach(leaf -> {
                        assertEquals((byte) 42, leaf.get(A));
                        assertEquals((short) 1000, leaf.get(B));
                        assertEquals(123456, leaf.get(C));
                        assertEquals(Long.MAX_VALUE, leaf.get(D));
                        assertEquals(3.14f, leaf.get(E));
                        assertEquals(2.718, leaf.get(F));
                        assertEquals("hello world", leaf.get(G));
                        assertEquals("{\"key\":\"value\"}", leaf.get(H));
                        return true;
                    });
                }
            }
        }
    }

    // ---- Scan with raw prefix bytes ----

    @Test
    void scanRawPrefix() throws IOException {
        var keyLayout = KeyLayout.of(KeyField.uint16("a"), KeyField.uint32("b"));
        var leafLayout = LeafLayout.of(LeafField.int32("v"));
        var VALUE = leafLayout.int32("v");

        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var A = tree.keyUint16("a");
            var B = tree.keyUint32("b");

            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);
                // Insert 3 entries: a=1,b=1; a=1,b=2; a=2,b=1
                try (var w = tree.write()) {
                    kb.set(A, (short) 1).set(B, 1); w.getOrCreate(kb).set(VALUE, 11);
                    kb.set(A, (short) 1).set(B, 2); w.getOrCreate(kb).set(VALUE, 12);
                    kb.set(A, (short) 2).set(B, 1); w.getOrCreate(kb).set(VALUE, 21);
                }

                // Raw prefix scan for a=1 (first 2 bytes)
                var prefix = arena.allocate(2);
                TaoKey.encodeU16(prefix, 0, (short) 1);
                var count = new AtomicInteger();
                try (var r = tree.read()) {
                    r.scan(prefix, 2, leaf -> { count.incrementAndGet(); return true; });
                }
                assertEquals(2, count.get());
            }
        }
    }

    // ---- KeyHandle: UInt8 factory ----

    @Test
    void keyUint8Handle() throws IOException {
        var keyLayout = KeyLayout.of(KeyField.uint8("x"), KeyField.uint32("y"));
        var leafLayout = LeafLayout.of(LeafField.int32("v"));
        try (var tree = TaoTree.create(tmp.resolve(fc++ + ".tao"), keyLayout, leafLayout)) {
            var X = tree.keyUint8("x");
            assertEquals("x", X.name());
            assertEquals(0, X.offset());
            assertEquals(1, X.width());
            assertEquals(0, X.fieldIndex());
        }
    }
}
