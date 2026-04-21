package org.taotree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 6b: validates that {@code TIMELESS = 0L} sentinel handling is correct,
 * including the "promotion" behavior where a subsequent temporal write caps
 * the validity of a prior TIMELESS value.
 */
class TimelessPromotionTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void timelessThenTemporalPromotion() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "x", Value.ofInt(7));            // TIMELESS
                w.put(kb, "x", Value.ofInt(99), 500L);     // promote
            }
            try (var r = tree.read()) {
                // TIMELESS value is visible at any t before the promotion timestamp.
                assertEquals(Value.ofInt(7), r.getAt(kb, "x", 200L));
                // After the promotion ts, the new value wins.
                assertEquals(Value.ofInt(99), r.getAt(kb, "x", 600L));
                // Latest is the temporal value.
                assertEquals(Value.ofInt(99), r.get(kb, "x"));
            }
        }
    }

    @Test
    void timelessOverwriteOnSameAttr() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "n", Value.ofInt(1));
                w.put(kb, "n", Value.ofInt(2));
            }
            try (var r = tree.read()) {
                assertEquals(Value.ofInt(2), r.get(kb, "n"));
            }
        }
    }

    @Test
    void timelessThenDeleteMakesGetReturnNull() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "n", Value.ofInt(42));
            }
            try (var w = tree.write()) {
                assertTrue(w.delete(kb, "n"));
            }
            try (var r = tree.read()) {
                assertNull(r.get(kb, "n"));
            }
        }
    }

    @Test
    void copyFromReplaysTimelessAndTemporal() throws IOException {
        try (var src = TaoTree.create(path(), keyLayout());
             var dst = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var SID = src.keyUint32("id");
            var DID = dst.keyUint32("id");
            var skb = src.newKeyBuilder(arena);
            var dkb = dst.newKeyBuilder(arena);

            try (var w = src.write()) {
                skb.set(SID, 1);
                w.put(skb, "v", Value.ofInt(1));               // TIMELESS
                w.put(skb, "v", Value.ofInt(2), 500L);         // promotion
                w.put(skb, "v", Value.ofInt(3), 1000L);
            }
            try (var w = dst.write(); var r = src.read()) {
                w.copyFrom(r);
            }
            try (var r = dst.read()) {
                dkb.set(DID, 1);
                assertEquals(Value.ofInt(3), r.get(dkb, "v"));
                assertEquals(Value.ofInt(1), r.getAt(dkb, "v", 200L));
                assertEquals(Value.ofInt(2), r.getAt(dkb, "v", 800L));
                assertEquals(Value.ofInt(3), r.getAt(dkb, "v", 1500L));

                List<Long> firstSeenTs = new ArrayList<>();
                r.history(dkb, "v", (fs, ls, vt, val) -> { firstSeenTs.add(fs); return true; });
                assertEquals(3, firstSeenTs.size(), "expected 3 runs after copyFrom: " + firstSeenTs);
                assertEquals(0L, firstSeenTs.get(0), "first run must be TIMELESS (firstSeen=0)");
            }
        }
    }
}
