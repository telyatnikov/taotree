package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.internal.art.ArtSearch;
import org.taotree.internal.art.NodePtr;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ArtSearch predecessor/successor operations.
 * Lives in org.taotree to access package-private slab() and ReadScope.root().
 */
class ArtSearchTest {

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

    /**
     * Build a TaoTree with the given keys and return it (caller must close).
     */
    private TaoTree buildTree(int... keys) throws IOException {
        Path file = tmp.resolve("tree-" + keys.length + "-" + keys.hashCode() + ".tao");
        var tree = TaoTree.create(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")));
        try (var w = tree.write()) {
            for (int k : keys) {
                long leaf = w.getOrCreate(intKey(k));
                w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, (long) k);
            }
        }
        tree.compact(); // migrate arena nodes to slab so slab::resolve works
        return tree;
    }

    /**
     * Read the value stored in a leaf (we store the int key as a long value).
     */
    private int leafValue(LongFunction<MemorySegment> resolver, long leafPtr, int keySlotSize) {
        MemorySegment seg = resolver.apply(leafPtr);
        return (int) seg.get(ValueLayout.JAVA_LONG, keySlotSize);
    }

    // ── predecessor tests ──

    @Test
    void predecessorExactMatch() throws IOException {
        try (var tree = buildTree(10, 20, 30, 40, 50)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(30)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                // Leaf key should be ≤ 30. Verify the key bytes.
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(30)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void predecessorBetweenKeys() throws IOException {
        try (var tree = buildTree(10, 20, 30, 40, 50)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Search for 25 — predecessor should be 20
                long leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(25)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(20)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void predecessorBeforeAll() throws IOException {
        try (var tree = buildTree(10, 20, 30)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Search for 5 — no predecessor exists
                long leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(5)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, leaf);
            }
        }
    }

    @Test
    void predecessorAfterAll() throws IOException {
        try (var tree = buildTree(10, 20, 30)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Search for 100 — predecessor should be 30 (the last key)
                long leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(100)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(30)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void predecessorEmptyTree() throws IOException {
        Path file = tmp.resolve("empty.tao");
        try (var tree = TaoTree.create(file, org.taotree.layout.KeyLayout.of(org.taotree.layout.KeyField.uint32("id")))) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(10)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, leaf);
            }
        }
    }

    @Test
    void predecessorSingleElement() throws IOException {
        try (var tree = buildTree(42)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Exact match
                long leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(42)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);

                // Search above
                leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(100)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);

                // Search below
                leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(10)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, leaf);
            }
        }
    }

    // ── successor tests ──

    @Test
    void successorExactMatch() throws IOException {
        try (var tree = buildTree(10, 20, 30, 40, 50)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long leaf = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(30)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(30)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void successorBetweenKeys() throws IOException {
        try (var tree = buildTree(10, 20, 30, 40, 50)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Search for 25 — successor should be 30
                long leaf = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(25)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(30)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void successorAfterAll() throws IOException {
        try (var tree = buildTree(10, 20, 30)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Search for 100 — no successor exists
                long leaf = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(100)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, leaf);
            }
        }
    }

    @Test
    void successorBeforeAll() throws IOException {
        try (var tree = buildTree(10, 20, 30)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Search for 5 — successor should be 10 (the first key)
                long leaf = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(5)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(10)), 0, KEY_LEN));
            }
        }
    }

    // ── rightmostLeaf / leftmostLeaf ──

    @Test
    void rightmostLeaf() throws IOException {
        try (var tree = buildTree(10, 20, 30, 40, 50)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long leaf = ArtSearch.rightmostLeaf(resolver, r.root());
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(50)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void leftmostLeaf() throws IOException {
        try (var tree = buildTree(10, 20, 30, 40, 50)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long leaf = ArtSearch.leftmostLeaf(resolver, r.root());
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(10)), 0, KEY_LEN));
            }
        }
    }

    // ── many keys (forces deeper ART structure) ──

    @Test
    void predecessorManyKeys() throws IOException {
        // Insert 100 keys: 0, 10, 20, ..., 990
        int[] keys = new int[100];
        for (int i = 0; i < 100; i++) keys[i] = i * 10;

        try (var tree = buildTree(keys)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // predecessor of 555 should be 550
                long leaf = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(555)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                MemorySegment seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(550)), 0, KEY_LEN));

                // successor of 555 should be 560
                leaf = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(555)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, leaf);
                seg = resolver.apply(leaf);
                assertEquals(0, ArtSearch.compareKeys(seg, 0,
                        MemorySegment.ofArray(intKey(560)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void predecessorAndSuccessorConsistency() throws IOException {
        // Insert keys 1..50 and verify predecessor/successor for each gap
        int[] keys = new int[50];
        for (int i = 0; i < 50; i++) keys[i] = (i + 1) * 100; // 100, 200, ..., 5000

        try (var tree = buildTree(keys)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                for (int i = 0; i < 49; i++) {
                    int lo = keys[i];
                    int hi = keys[i + 1];
                    int mid = (lo + hi) / 2;

                    // predecessor(mid) should be lo
                    long pred = ArtSearch.predecessor(resolver, r.root(),
                            MemorySegment.ofArray(intKey(mid)), KEY_LEN);
                    assertNotEquals(NodePtr.EMPTY_PTR, pred,
                            "predecessor should exist for mid=" + mid);
                    MemorySegment predSeg = resolver.apply(pred);
                    assertEquals(0, ArtSearch.compareKeys(predSeg, 0,
                            MemorySegment.ofArray(intKey(lo)), 0, KEY_LEN),
                            "predecessor of " + mid + " should be " + lo);

                    // successor(mid) should be hi
                    long succ = ArtSearch.successor(resolver, r.root(),
                            MemorySegment.ofArray(intKey(mid)), KEY_LEN);
                    assertNotEquals(NodePtr.EMPTY_PTR, succ,
                            "successor should exist for mid=" + mid);
                    MemorySegment succSeg = resolver.apply(succ);
                    assertEquals(0, ArtSearch.compareKeys(succSeg, 0,
                            MemorySegment.ofArray(intKey(hi)), 0, KEY_LEN),
                            "successor of " + mid + " should be " + hi);
                }
            }
        }
    }

    // ── Node48/Node256 path tests ──

    @Test
    void predecessorSuccessorWithNode48() throws IOException {
        // Create 20 keys that differ at byte 0 → triggers Node16→Node48 growth
        int[] keys = new int[20];
        for (int i = 0; i < 20; i++) keys[i] = (i + 1) << 24; // 0x01000000, 0x02000000, ..., 0x14000000

        try (var tree = buildTree(keys)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // predecessor of 0x0A800000 (between key 10 and 11)
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x0A_80_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                MemorySegment predSeg = resolver.apply(pred);
                assertEquals(0, ArtSearch.compareKeys(predSeg, 0,
                        MemorySegment.ofArray(intKey(10 << 24)), 0, KEY_LEN));

                // successor of 0x0A800000
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x0A_80_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                MemorySegment succSeg = resolver.apply(succ);
                assertEquals(0, ArtSearch.compareKeys(succSeg, 0,
                        MemorySegment.ofArray(intKey(11 << 24)), 0, KEY_LEN));

                // leftmost/rightmost
                long leftmost = ArtSearch.leftmostLeaf(resolver, r.root());
                assertNotEquals(NodePtr.EMPTY_PTR, leftmost);
                assertEquals(0, ArtSearch.compareKeys(
                        resolver.apply(leftmost), 0,
                        MemorySegment.ofArray(intKey(1 << 24)), 0, KEY_LEN));

                long rightmost = ArtSearch.rightmostLeaf(resolver, r.root());
                assertNotEquals(NodePtr.EMPTY_PTR, rightmost);
                assertEquals(0, ArtSearch.compareKeys(
                        resolver.apply(rightmost), 0,
                        MemorySegment.ofArray(intKey(20 << 24)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void predecessorSuccessorWithNode256() throws IOException {
        // Create 60 keys that differ at byte 0 → triggers Node48→Node256 growth
        int[] keys = new int[60];
        for (int i = 0; i < 60; i++) keys[i] = (i + 1) << 24;

        try (var tree = buildTree(keys)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // predecessor between key 30 and 31
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x1E_80_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(
                        resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(30 << 24)), 0, KEY_LEN));

                // successor between key 30 and 31
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x1E_80_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(
                        resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(31 << 24)), 0, KEY_LEN));

                // predecessor of first key → empty
                long empty = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x00_80_00_00)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, empty);

                // successor of last key → empty
                long emptySucc = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x3D_80_00_00)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, emptySucc);
            }
        }
    }

    @Test
    void scanWithNode256Covers() throws IOException {
        // Build tree with 100 keys varying at byte 0 for Node256, then scan all
        int[] keys = new int[100];
        for (int i = 0; i < 100; i++) keys[i] = (i + 1) << 24;

        try (var tree = buildTree(keys)) {
            try (var r = tree.read()) {
                assertEquals(100, r.size());
            }
        }
    }

    // ── Backtracking tests (Phase 2) ──

    @Test
    void predecessorBacktracksFromLeafGreaterThanSearch() throws IOException {
        // Tree with 2 branches: [0x01_00_00_00, 0x03_FF_00_00]
        // Search predecessor(0x03_00_00_00):
        //   Phase 1: descend via byte 0x03, reach leaf 0x03_FF_00_00 > search → dead end
        //   Phase 2: backtrack to root, largestChildBefore(0x03) finds 0x01 child
        //   → rightmostLeaf = 0x01_00_00_00
        try (var tree = buildTree(0x01_00_00_00, 0x03_FF_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x03_00_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x01_00_00_00)), 0, KEY_LEN),
                        "predecessor should backtrack to 0x01_00_00_00");
            }
        }
    }

    @Test
    void successorBacktracksFromLeafLessThanSearch() throws IOException {
        // Tree with 2 branches: [0x01_00_FF_FF, 0x03_00_00_00]
        // Search successor(0x01_FF_00_00):
        //   Phase 1: descend via byte 0x01, reach leaf 0x01_00_FF_FF < search → dead end
        //   Phase 2: backtrack to root, smallestChildAfter(0x01) finds 0x03 child
        //   → leftmostLeaf = 0x03_00_00_00
        try (var tree = buildTree(0x01_00_FF_FF, 0x03_00_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x01_FF_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x03_00_00_00)), 0, KEY_LEN),
                        "successor should backtrack to 0x03_00_00_00");
            }
        }
    }

    @Test
    void predecessorBacktracksMultipleLevels() throws IOException {
        // Tree: [0x01_01_00_00, 0x01_03_FF_00]
        // Search predecessor(0x01_03_00_00):
        //   Phase 1: root→byte 0x01→ inner node with [0x01, 0x03]
        //   descend byte 0x03 → leaf 0x01_03_FF_00 > search → dead end
        //   Phase 2: backtrack, largestChildBefore(0x03) → 0x01 child → 0x01_01_00_00
        try (var tree = buildTree(0x01_01_00_00, 0x01_03_FF_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x01_03_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x01_01_00_00)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void successorBacktracksMultipleLevels() throws IOException {
        // Tree: [0x01_01_00_FF, 0x01_03_00_00]
        // Search successor(0x01_01_FF_00):
        //   Phase 1: root→byte 0x01→ inner node with [0x01, 0x03]
        //   descend byte 0x01 → leaf 0x01_01_00_FF < search → dead end
        //   Phase 2: backtrack, smallestChildAfter(0x01) → 0x03 child → 0x01_03_00_00
        try (var tree = buildTree(0x01_01_00_FF, 0x01_03_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x01_01_FF_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x01_03_00_00)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void predecessorBacktrackNoSmallerChild() throws IOException {
        // Tree: [0x05_00_00_00, 0x05_00_FF_FF]
        // Search predecessor(0x05_00_00_01):
        //   Same first byte, then descend deeper. Should find 0x05_00_00_00 (exact or close).
        try (var tree = buildTree(0x05_00_00_00, 0x05_00_FF_FF)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x05_00_00_01)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x05_00_00_00)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void predecessorBacktrackToRootNoResult() throws IOException {
        // Tree: [0x10_00_00_00, 0x20_00_00_00]
        // Search predecessor(0x0F_FF_FF_FF): both branches are >= search,
        // but predecessorExact descend for byte 0x0F fails, no smaller child at root.
        try (var tree = buildTree(0x10_00_00_00, 0x20_00_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x0F_FF_FF_FF)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, pred,
                        "no predecessor should exist before first key");
            }
        }
    }

    @Test
    void successorBacktrackToRootNoResult() throws IOException {
        // Tree: [0x10_00_00_00, 0x20_00_00_00]
        // Search successor(0x20_00_00_01): both branches are <= search,
        // backtrack finds no larger child.
        try (var tree = buildTree(0x10_00_00_00, 0x20_00_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x20_00_00_01)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, succ,
                        "no successor should exist after last key");
            }
        }
    }

    // ── Prefix node divergence tests ──

    @Test
    void predecessorPrefixByteLessThanSearch() throws IOException {
        // Keys: [0x10_20_30_01, 0x10_20_30_02]  share prefix [0x10, 0x20, 0x30]
        // Search predecessor(0x10_20_31_00): prefix byte 0x30 < search byte 0x31
        // → rightmostLeaf of prefix subtree = 0x10_20_30_02
        try (var tree = buildTree(0x10_20_30_01, 0x10_20_30_02)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x10_20_31_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x10_20_30_02)), 0, KEY_LEN),
                        "prefix 0x30 < search 0x31: rightmostLeaf of subtree");
            }
        }
    }

    @Test
    void predecessorPrefixByteGreaterThanSearch() throws IOException {
        // Keys: [0x10_20_30_01, 0x10_20_30_02] share prefix [0x10, 0x20, 0x30]
        // Search predecessor(0x10_20_2F_FF): prefix byte 0x30 > search byte 0x2F
        // → all subtree keys are > search → backtrack → EMPTY_PTR
        try (var tree = buildTree(0x10_20_30_01, 0x10_20_30_02)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x10_20_2F_FF)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, pred,
                        "prefix 0x30 > search 0x2F: no predecessor exists");
            }
        }
    }

    @Test
    void successorPrefixByteGreaterThanSearch() throws IOException {
        // Keys: [0x10_20_30_01, 0x10_20_30_02] share prefix [0x10, 0x20, 0x30]
        // Search successor(0x10_20_2F_FF): prefix byte 0x30 > search byte 0x2F
        // → leftmostLeaf of subtree = 0x10_20_30_01
        try (var tree = buildTree(0x10_20_30_01, 0x10_20_30_02)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x10_20_2F_FF)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x10_20_30_01)), 0, KEY_LEN),
                        "prefix 0x30 > search 0x2F: leftmostLeaf of subtree");
            }
        }
    }

    @Test
    void successorPrefixByteLessThanSearch() throws IOException {
        // Keys: [0x10_20_30_01, 0x10_20_30_02] share prefix [0x10, 0x20, 0x30]
        // Search successor(0x10_20_31_00): prefix byte 0x30 < search byte 0x31
        // → all subtree keys are < search → backtrack → EMPTY_PTR
        try (var tree = buildTree(0x10_20_30_01, 0x10_20_30_02)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x10_20_31_00)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, succ,
                        "prefix 0x30 < search 0x31: no successor exists");
            }
        }
    }

    // ── Prefix with branches on both sides ──

    @Test
    void predecessorWithPrefixAndMultipleBranches() throws IOException {
        // Keys that create multiple branches above and below a prefix path
        // [0x01_00_00_00, 0x05_20_30_01, 0x05_20_30_02, 0x09_00_00_00]
        try (var tree = buildTree(0x01_00_00_00, 0x05_20_30_01, 0x05_20_30_02, 0x09_00_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // predecessor of 0x05_20_31_00 → prefix divergence, rightmostLeaf = 0x05_20_30_02
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x05_20_31_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x05_20_30_02)), 0, KEY_LEN));

                // predecessor of 0x05_20_2F_FF → prefix divergence, backtrack to root, find 0x01 branch
                pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x05_20_2F_FF)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x01_00_00_00)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void successorWithPrefixAndMultipleBranches() throws IOException {
        try (var tree = buildTree(0x01_00_00_00, 0x05_20_30_01, 0x05_20_30_02, 0x09_00_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // successor of 0x05_20_2F_FF → prefix divergence, leftmostLeaf = 0x05_20_30_01
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x05_20_2F_FF)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x05_20_30_01)), 0, KEY_LEN));

                // successor of 0x05_20_31_00 → prefix divergence, backtrack to root, find 0x09 branch
                succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x05_20_31_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x09_00_00_00)), 0, KEY_LEN));
            }
        }
    }

    // ── No-match inner node with largestChildBefore/smallestChildAfter at leaf level ──

    @Test
    void predecessorNoMatchFindsLargestChildBefore() throws IOException {
        // Tree: [0x10_00_00_01, 0x10_00_00_03, 0x10_00_00_05]
        // Search predecessor(0x10_00_00_04): inner node at byte 3 has [0x01, 0x03, 0x05]
        // No exact match for 0x04, largestChildBefore(0x04) → child at 0x03
        try (var tree = buildTree(0x10_00_00_01, 0x10_00_00_03, 0x10_00_00_05)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x10_00_00_04)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x10_00_00_03)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void successorNoMatchFindsSmallestChildAfter() throws IOException {
        // Tree: [0x10_00_00_01, 0x10_00_00_03, 0x10_00_00_05]
        // Search successor(0x10_00_00_02): no exact match, smallestChildAfter(0x02) → child at 0x03
        try (var tree = buildTree(0x10_00_00_01, 0x10_00_00_03, 0x10_00_00_05)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x10_00_00_02)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x10_00_00_03)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void predecessorNoMatchAndNoSmallerChild() throws IOException {
        // Tree: [0x10_00_00_05, 0x10_00_00_07]
        // Search predecessor(0x10_00_00_04): no exact match, largestChildBefore(0x04) → empty
        // → backtrack up, no more frames → backtrack again → EMPTY_PTR
        // But wait: depth 0..2 would share a prefix. At byte 3, Node4 with [0x05, 0x07].
        // largestChildBefore(0x04) → empty. Backtrack to prefix level, nothing there either.
        try (var tree = buildTree(0x10_00_00_05, 0x10_00_00_07)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x10_00_00_04)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, pred,
                        "no key ≤ 0x10_00_00_04 exists");
            }
        }
    }

    @Test
    void successorNoMatchAndNoLargerChild() throws IOException {
        // Tree: [0x10_00_00_05, 0x10_00_00_07]
        // Search successor(0x10_00_00_08): no exact match, smallestChildAfter(0x08) → empty
        try (var tree = buildTree(0x10_00_00_05, 0x10_00_00_07)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x10_00_00_08)), KEY_LEN);
                assertEquals(NodePtr.EMPTY_PTR, succ,
                        "no key ≥ 0x10_00_00_08 exists");
            }
        }
    }

    // ── Dense key set for Node16 backtracking ──

    @Test
    void predecessorBacktracksInNode16() throws IOException {
        // Create 6 branches at byte 1 (Node16), search forces backtrack
        // Keys: 0xAA_02_00_00, 0xAA_04_00_00, 0xAA_06_00_00,
        //       0xAA_08_00_00, 0xAA_0A_00_00, 0xAA_0C_FF_FF
        try (var tree = buildTree(
                0xAA_02_00_00, 0xAA_04_00_00, 0xAA_06_00_00,
                0xAA_08_00_00, 0xAA_0A_00_00, 0xAA_0C_FF_FF)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // predecessor of 0xAA_0C_00_00: descend byte 0x0C → leaf 0xAA_0C_FF_FF > search
                // backtrack: largestChildBefore(0x0C) → byte 0x0A → leaf 0xAA_0A_00_00
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0xAA_0C_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0xAA_0A_00_00)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void successorBacktracksInNode16() throws IOException {
        try (var tree = buildTree(
                0xAA_02_00_FF, 0xAA_04_00_00, 0xAA_06_00_00,
                0xAA_08_00_00, 0xAA_0A_00_00, 0xAA_0C_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // successor of 0xAA_02_FF_00: descend byte 0x02 → leaf 0xAA_02_00_FF < search
                // backtrack: smallestChildAfter(0x02) → byte 0x04 → leaf 0xAA_04_00_00
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0xAA_02_FF_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0xAA_04_00_00)), 0, KEY_LEN));
            }
        }
    }

    // ── Node48 backtracking ──

    @Test
    void predecessorBacktracksInNode48() throws IOException {
        // 20 keys varying at byte 1 → Node48 at depth 1
        int[] keys = new int[20];
        for (int i = 0; i < 20; i++) keys[i] = 0xBB_00_00_00 | ((i * 10 + 10) << 16); // bytes: 0xBB, 0x0A..0xC8, 0x00, 0x00
        // Last key's byte1 = 200 (0xC8), but store as big leaf
        // Actually let me simplify: make byte 1 vary, all others zero
        keys = new int[20];
        for (int i = 0; i < 19; i++) keys[i] = 0xBB_00_00_00 | ((i * 2 + 2) << 16);
        keys[19] = 0xBB_00_00_00 | (0xFF << 16); // 0xBB_FF_00_00 → highest branch, make its leaf have extra bytes
        // Actually, let me create a clean scenario for backtracking
        // 20 branches at byte 0 (to ensure Node48), one branch has a leaf > search
        keys = new int[20];
        for (int i = 0; i < 19; i++) keys[i] = (i + 1) << 24; // 0x01.. through 0x13..
        keys[19] = (20 << 24) | 0x00_FF_FF_FF; // 0x14_FF_FF_FF

        try (var tree = buildTree(keys)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // predecessor of 0x14_00_00_00: descend byte 0x14 → leaf 0x14_FF_FF_FF > search
                // backtrack: largestChildBefore(0x14 = 20) → byte 0x13 (=19) → leaf 0x13_00_00_00
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x14_00_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(19 << 24)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void successorBacktracksInNode48() throws IOException {
        int[] keys = new int[20];
        keys[0] = (1 << 24) | 0x00_00_01; // 0x01_00_00_01 — lowest branch, leaf < any search above it
        for (int i = 1; i < 20; i++) keys[i] = (i + 1) << 24; // 0x02.. through 0x14..

        try (var tree = buildTree(keys)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // successor of 0x01_00_00_02: descend byte 0x01 → leaf 0x01_00_00_01 < search
                // backtrack: smallestChildAfter(0x01) → byte 0x02 → leaf 0x02_00_00_00
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x01_00_00_02)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(2 << 24)), 0, KEY_LEN));
            }
        }
    }

    // ── compareKeys edge cases ──

    @Test
    void compareKeysEqual() {
        byte[] a = {0x01, 0x02, 0x03, 0x04};
        byte[] b = {0x01, 0x02, 0x03, 0x04};
        assertEquals(0, ArtSearch.compareKeys(MemorySegment.ofArray(a), 0,
                MemorySegment.ofArray(b), 0, 4));
    }

    @Test
    void compareKeysLessThan() {
        byte[] a = {0x01, 0x02, 0x03, 0x04};
        byte[] b = {0x01, 0x02, 0x03, 0x05};
        assertTrue(ArtSearch.compareKeys(MemorySegment.ofArray(a), 0,
                MemorySegment.ofArray(b), 0, 4) < 0);
    }

    @Test
    void compareKeysGreaterThan() {
        byte[] a = {0x01, 0x02, 0x04, 0x04};
        byte[] b = {0x01, 0x02, 0x03, 0x05};
        assertTrue(ArtSearch.compareKeys(MemorySegment.ofArray(a), 0,
                MemorySegment.ofArray(b), 0, 4) > 0);
    }

    @Test
    void compareKeysUnsignedComparison() {
        // 0xFF should be > 0x00 in unsigned comparison
        byte[] a = {(byte) 0xFF, 0x00, 0x00, 0x00};
        byte[] b = {0x00, 0x00, 0x00, 0x00};
        assertTrue(ArtSearch.compareKeys(MemorySegment.ofArray(a), 0,
                MemorySegment.ofArray(b), 0, 4) > 0);
    }

    @Test
    void compareKeysWithOffset() {
        // Test non-zero offset
        byte[] a = {0x00, 0x01, 0x02, 0x03, 0x04};
        byte[] b = {0x01, 0x02, 0x03, 0x04};
        assertEquals(0, ArtSearch.compareKeys(MemorySegment.ofArray(a), 1,
                MemorySegment.ofArray(b), 0, 4));
    }

    @Test
    void compareKeysZeroLength() {
        byte[] a = {0x01};
        byte[] b = {0x02};
        assertEquals(0, ArtSearch.compareKeys(MemorySegment.ofArray(a), 0,
                MemorySegment.ofArray(b), 0, 0));
    }

    // ── Deep tree with backtracking at multiple levels ──

    @Test
    void predecessorDeepBacktrack() throws IOException {
        // Create a tree where backtracking must pop multiple stack frames
        // Layer 1 (byte 0): [0x01, 0x02]
        // Under 0x02, layer 2 (byte 1): [0x01, 0x02]
        // Under 0x02_02, layer 3 (byte 2): single leaf 0x02_02_FF_00
        // Search predecessor(0x02_02_00_00): descend 0x02→0x02→leaf 0x02_02_FF_00 > search
        // backtrack: at 0x02 inner node, largestChildBefore(0x02) = 0x01 → 0x02_01_XX
        try (var tree = buildTree(0x01_00_00_00, 0x02_01_00_00, 0x02_02_FF_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x02_02_00_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x02_01_00_00)), 0, KEY_LEN));
            }
        }
    }

    @Test
    void successorDeepBacktrack() throws IOException {
        // Mirror of above
        try (var tree = buildTree(0x02_01_00_FF, 0x02_02_00_00, 0x03_00_00_00)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x02_01_FF_00)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x02_02_00_00)), 0, KEY_LEN));
            }
        }
    }

    // ── Adjacent key tests (boundary mutation killers) ──

    @Test
    void predecessorAdjacentKeys() throws IOException {
        // Keys differ by 1 in last byte: precise boundary test
        try (var tree = buildTree(0x00_00_00_10, 0x00_00_00_11, 0x00_00_00_12)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Exact match for middle key
                long pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x00_00_00_11)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x00_00_00_11)), 0, KEY_LEN),
                        "exact match should return the key itself");

                // One below middle (predecessor is the key below)
                pred = ArtSearch.predecessor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x00_00_00_10)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, pred);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(pred), 0,
                        MemorySegment.ofArray(intKey(0x00_00_00_10)), 0, KEY_LEN),
                        "exact match for lowest key");
            }
        }
    }

    @Test
    void successorAdjacentKeys() throws IOException {
        try (var tree = buildTree(0x00_00_00_10, 0x00_00_00_11, 0x00_00_00_12)) {
            LongFunction<MemorySegment> resolver = tree.slab()::resolve;
            try (var r = tree.read()) {
                // Exact match for middle key
                long succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x00_00_00_11)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x00_00_00_11)), 0, KEY_LEN),
                        "exact match should return the key itself");

                // Exact match for highest key
                succ = ArtSearch.successor(resolver, r.root(),
                        MemorySegment.ofArray(intKey(0x00_00_00_12)), KEY_LEN);
                assertNotEquals(NodePtr.EMPTY_PTR, succ);
                assertEquals(0, ArtSearch.compareKeys(resolver.apply(succ), 0,
                        MemorySegment.ofArray(intKey(0x00_00_00_12)), 0, KEY_LEN));
            }
        }
    }

    // ── rightmostLeaf/leftmostLeaf with empty tree ──

    @Test
    void rightmostLeafEmptyTree() {
        assertEquals(NodePtr.EMPTY_PTR, ArtSearch.rightmostLeaf(seg -> null, NodePtr.EMPTY_PTR));
    }

    @Test
    void leftmostLeafEmptyTree() {
        assertEquals(NodePtr.EMPTY_PTR, ArtSearch.leftmostLeaf(seg -> null, NodePtr.EMPTY_PTR));
    }
}

