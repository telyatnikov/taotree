package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for same-logical-value write canonicalization.
 *
 * <p>Bug: the public {@code put(kb, attr, Value, ts)} path calls
 * {@code ValueCodec.encodeStandalone} which bump-allocates a fresh slot per
 * call, producing a new {@code valueRef} every time. Before the fix,
 * {@code TemporalWriter} compared {@code predValueRef == valueRef} by pointer,
 * so repeated identical observations never merged into the existing run and
 * instead created new {@code AttributeRun} leaves, new {@code EntityVersion}
 * leaves, and new CHAMP state roots.
 *
 * <p>After the fix, {@code ValueCodec.slotEquals} compares encoded bytes
 * (resolving overflow payloads when needed), so same-logical-value writes
 * extend {@code last_seen} on the existing run as intended.
 */
class ValueCanonicalizationTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    private int countRuns(TaoTree tree, org.taotree.layout.KeyHandle.UInt32 id, String attr) {
        AtomicInteger n = new AtomicInteger();
        try (var rs = tree.read(); var arena = Arena.ofConfined()) {
            var kb = tree.newKeyBuilder(arena);
            kb.set(id, 1);
            rs.history(kb, attr, (firstSeen, lastSeen, validTo, value) -> {
                n.incrementAndGet();
                return true;
            });
        }
        return n.get();
    }

    @Test
    void samePredecessorValueExtendsRunInsteadOfCreatingDuplicates() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            try (var ws = tree.write()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 1);
                ws.put(kb, "status", Value.ofString("ACTIVE"), 100L);
                ws.put(kb, "status", Value.ofString("ACTIVE"), 200L);
                ws.put(kb, "status", Value.ofString("ACTIVE"), 300L);
            }
            // Before the fix: 3 runs. After: 1 run spanning [100, 300].
            assertEquals(1, countRuns(tree, ID, "status"),
                    "Three identical forward writes must merge into one run");
        }
    }

    @Test
    void sameSuccessorValueMergesBackwardsInsteadOfCreatingDuplicates() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            try (var ws = tree.write()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 1);
                ws.put(kb, "status", Value.ofString("ACTIVE"), 200L);
                // Backfill with same logical value: should extend the
                // successor backward to first_seen=100, not create a new run.
                ws.put(kb, "status", Value.ofString("ACTIVE"), 100L);
            }
            assertEquals(1, countRuns(tree, ID, "status"),
                    "Backward same-value write must merge into the successor run");
        }
    }

    @Test
    void sameOverflowValueMergesAcrossSeparateEncodings() throws IOException {
        // String > INLINE_THRESHOLD (12 bytes) forces the overflow code path:
        // each write bump-allocates a fresh overflow blob AND a fresh slot,
        // so equality cannot rely on pointer identity of either.
        String longValue = "This is a value longer than 12 bytes to force overflow";
        assertTrue(longValue.length() > 12);

        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            try (var ws = tree.write()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 1);
                ws.put(kb, "desc", Value.ofString(longValue), 100L);
                ws.put(kb, "desc", Value.ofString(longValue), 200L);
                ws.put(kb, "desc", Value.ofString(longValue), 300L);
            }
            assertEquals(1, countRuns(tree, ID, "desc"),
                    "Overflow-value duplicates must also merge");
        }
    }

    @Test
    void differentValuesStillCreateSeparateRuns() throws IOException {
        // Sanity: the fix must not over-merge distinct values.
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            try (var ws = tree.write()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 1);
                ws.put(kb, "status", Value.ofString("A"), 100L);
                ws.put(kb, "status", Value.ofString("B"), 200L);
                ws.put(kb, "status", Value.ofString("A"), 300L);
            }
            assertEquals(3, countRuns(tree, ID, "status"),
                    "Alternating values must produce three distinct runs");
        }
    }

    @Test
    void interleavedSameAndDifferentValuesMergeCorrectly() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            try (var ws = tree.write()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 1);
                ws.put(kb, "status", Value.ofString("A"), 100L);
                ws.put(kb, "status", Value.ofString("A"), 150L); // merge
                ws.put(kb, "status", Value.ofString("B"), 200L); // new
                ws.put(kb, "status", Value.ofString("B"), 250L); // merge
                ws.put(kb, "status", Value.ofString("B"), 300L); // merge
                ws.put(kb, "status", Value.ofString("C"), 400L); // new
            }
            assertEquals(3, countRuns(tree, ID, "status"),
                    "Should collapse to three runs: A[100,150], B[200,300], C[400,...]");
        }
    }

    @Test
    void sameNumericValueAcrossEncodings() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            try (var ws = tree.write()) {
                var kb = tree.newKeyBuilder(arena);
                kb.set(ID, 1);
                ws.put(kb, "n", Value.ofInt(42), 100L);
                ws.put(kb, "n", Value.ofInt(42), 200L);
                ws.put(kb, "n", Value.ofInt(42), 300L);
            }
            assertEquals(1, countRuns(tree, ID, "n"),
                    "Identical int values must merge despite fresh slot allocations");
        }
    }
}
