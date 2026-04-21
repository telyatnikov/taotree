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

class HistoryRangeTest {

    @TempDir Path tmp;
    private int fc;
    private Path path() { return tmp.resolve(fc++ + ".taotree"); }
    private KeyLayout keyLayout() { return KeyLayout.of(KeyField.uint32("id")); }

    @Test
    void historyRangeReturnsOnlyOverlappingRuns() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "v", Value.ofInt(10), 100L);
                w.put(kb, "v", Value.ofInt(20), 200L);
                w.put(kb, "v", Value.ofInt(30), 300L);
            }
            try (var r = tree.read()) {
                List<Integer> seen = new ArrayList<>();
                r.historyRange(kb, "v", 150L, 250L, (fs, ls, vt, v) -> {
                    seen.add(((Value.Int32) v).value());
                    return true;
                });
                // Window [150,250] must include the 100-run (still valid until 200)
                // and the 200-run (valid from 200 onward); excludes the 300-run.
                assertTrue(seen.contains(10), "expected run starting at 100, got " + seen);
                assertTrue(seen.contains(20), "expected run starting at 200, got " + seen);
                assertFalse(seen.contains(30), "300-run must not be in [150,250]: " + seen);
            }
        }
    }

    @Test
    void historyRangeWithNoOverlapYieldsNoCallbacks() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                // Three closed runs (each capped by the next put), then a final
                // open run. Range [10, 50] precedes them all → no overlap.
                w.put(kb, "v", Value.ofInt(10), 100L);
                w.put(kb, "v", Value.ofInt(20), 200L);
            }
            try (var r = tree.read()) {
                List<Value> seen = new ArrayList<>();
                r.historyRange(kb, "v", 10L, 50L, (fs, ls, vt, v) -> { seen.add(v); return true; });
                assertTrue(seen.isEmpty(), "no run overlaps [10,50]: " + seen);
            }
        }
    }

    @Test
    void historyRangeEarlyStop() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 1);
            try (var w = tree.write()) {
                w.put(kb, "v", Value.ofInt(10), 100L);
                w.put(kb, "v", Value.ofInt(20), 200L);
                w.put(kb, "v", Value.ofInt(30), 300L);
            }
            try (var r = tree.read()) {
                List<Value> seen = new ArrayList<>();
                boolean done = r.historyRange(kb, "v", 0L, 1000L, (fs, ls, vt, v) -> {
                    seen.add(v);
                    return false; // stop after first
                });
                assertFalse(done);
                assertEquals(1, seen.size());
            }
        }
    }

    @Test
    void historyRangeOnMissingEntityReturnsTrue() throws IOException {
        try (var tree = TaoTree.create(path(), keyLayout());
             var arena = Arena.ofConfined()) {
            var ID = tree.keyUint32("id");
            var kb = tree.newKeyBuilder(arena);
            kb.set(ID, 9999);
            try (var r = tree.read()) {
                boolean done = r.historyRange(kb, "v", 0L, 100L, (fs, ls, vt, v) -> true);
                assertTrue(done);
            }
        }
    }
}
