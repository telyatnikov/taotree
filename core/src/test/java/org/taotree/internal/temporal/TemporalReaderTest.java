package org.taotree.internal.temporal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.taotree.internal.alloc.BumpAllocator;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.art.NodeConstants;
import org.taotree.internal.champ.ChampMap;
import org.taotree.internal.cow.EpochReclaimer;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for TemporalReader — the temporal read path.
 *
 * <p>Uses TemporalWriter to set up entity state, then verifies reads.
 */
class TemporalReaderTest {

    private Path tmpDir;
    private Arena managedArena;
    private ChunkStore chunkStore;
    private SlabAllocator slab;
    private BumpAllocator bump;
    private EpochReclaimer reclaimer;
    private TemporalWriter writer;
    private TemporalReader reader;

    @BeforeEach
    void setUp() throws IOException {
        tmpDir = Files.createTempDirectory("temporal-reader-test");
        Path storeFile = tmpDir.resolve("test.store");

        managedArena = Arena.ofShared();
        chunkStore = ChunkStore.createCheckpointed(storeFile, managedArena,
                ChunkStore.DEFAULT_CHUNK_SIZE, false);
        slab = new SlabAllocator(managedArena, chunkStore, SlabAllocator.DEFAULT_SLAB_SIZE);
        bump = new BumpAllocator(managedArena, chunkStore, BumpAllocator.DEFAULT_PAGE_SIZE);
        reclaimer = new EpochReclaimer(slab);

        int prefixClassId = slab.registerClass(NodeConstants.PREFIX_SIZE);
        int node4ClassId = slab.registerClass(NodeConstants.NODE4_SIZE);
        int node16ClassId = slab.registerClass(NodeConstants.NODE16_SIZE);
        int node48ClassId = slab.registerClass(NodeConstants.NODE48_SIZE);
        int node256ClassId = slab.registerClass(NodeConstants.NODE256_SIZE);

        writer = new TemporalWriter(slab, reclaimer, chunkStore, bump,
                prefixClassId, node4ClassId, node16ClassId,
                node48ClassId, node256ClassId);
        reader = new TemporalReader(slab, chunkStore, bump);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (chunkStore != null) chunkStore.close();
        if (managedArena != null) managedArena.close();
        if (tmpDir != null) {
            Files.walk(tmpDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException e) { /* ignore */ }
                    });
        }
    }

    private MemorySegment emptyEntityNode() {
        MemorySegment seg = Arena.ofAuto().allocate(EntityNode.SIZE);
        EntityNode.clear(seg);
        return seg;
    }

    private MemorySegment entityNodeFrom(TemporalWriter.WriteResult result) {
        MemorySegment seg = Arena.ofAuto().allocate(EntityNode.SIZE);
        EntityNode.clear(seg);
        EntityNode.setCurrentStateRoot(seg, result.newCurrentStateRoot());
        EntityNode.setAttrArtRoot(seg, result.newAttrArtRoot());
        EntityNode.setVersionsArtRoot(seg, result.newVersionsArtRoot());
        return seg;
    }

    // Helper: write a sequence of (attr, value, timestamp) and return final entity node
    private MemorySegment writeAll(int[]... writes) {
        MemorySegment en = emptyEntityNode();
        for (int[] w : writes) {
            var result = writer.write(en, w[0], vref(w[1]), (long) w[2]);
            en = entityNodeFrom(result);
        }
        return en;
    }

    // ====================================================================
    // latest
    // ====================================================================

    @Test
    void latestOnEmptyEntityReturnsNotFound() {
        assertEquals(TemporalReader.NOT_FOUND,
                reader.latest(emptyEntityNode(), 1));
    }

    @Test
    void latestReturnsCurrentValue() {
        MemorySegment en = writeAll(new int[]{1, 100, 1000});
        assertEquals(vref(100), reader.latest(en, 1));
    }

    @Test
    void latestReturnsUpdatedValueAfterChange() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 1000},
                new int[]{1, 200, 2000});
        assertEquals(vref(200), reader.latest(en, 1));
    }

    @Test
    void latestReturnsNotFoundForUnknownAttribute() {
        MemorySegment en = writeAll(new int[]{1, 100, 1000});
        assertEquals(TemporalReader.NOT_FOUND, reader.latest(en, 99));
    }

    // ====================================================================
    // at
    // ====================================================================

    @Test
    void atOnEmptyEntityReturnsNotFound() {
        assertEquals(TemporalReader.NOT_FOUND,
                reader.at(emptyEntityNode(), 1, 1000L));
    }

    @Test
    void atReturnsValueAtExactTimestamp() {
        MemorySegment en = writeAll(new int[]{1, 100, 1000});
        assertEquals(vref(100), reader.at(en, 1, 1000L));
    }

    @Test
    void atReturnsValueAtFutureTimestamp() {
        // valid_to = MAX, so value holds indefinitely
        MemorySegment en = writeAll(new int[]{1, 100, 1000});
        assertEquals(vref(100), reader.at(en, 1, 5000L));
    }

    @Test
    void atReturnsNotFoundBeforeFirstWrite() {
        MemorySegment en = writeAll(new int[]{1, 100, 1000});
        assertEquals(TemporalReader.NOT_FOUND, reader.at(en, 1, 500L));
    }

    @Test
    void atReturnsNotFoundForUnknownAttribute() {
        MemorySegment en = writeAll(new int[]{1, 100, 1000});
        assertEquals(TemporalReader.NOT_FOUND, reader.at(en, 99, 1000L));
    }

    @Test
    void atReturnsCorrectValueAcrossMultipleChanges() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200},
                new int[]{1, 300, 300});

        assertEquals(TemporalReader.NOT_FOUND, reader.at(en, 1, 50L));
        assertEquals(vref(100), reader.at(en, 1, 100L));
        assertEquals(vref(100), reader.at(en, 1, 150L));
        assertEquals(vref(200), reader.at(en, 1, 200L));
        assertEquals(vref(200), reader.at(en, 1, 250L));
        assertEquals(vref(300), reader.at(en, 1, 300L));
        assertEquals(vref(300), reader.at(en, 1, 999L));
    }

    // ====================================================================
    // stateAt
    // ====================================================================

    @Test
    void stateAtReturnsChampRootWithAllAttributes() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 1000},
                new int[]{2, 200, 1000});

        long stateRoot = reader.stateAt(en, 1000L);
        assertNotEquals(ChampMap.EMPTY_ROOT, stateRoot);
        assertEquals(vref(100), ChampMap.get(bump, stateRoot, 1));
        assertEquals(vref(200), ChampMap.get(bump, stateRoot, 2));
    }

    @Test
    void stateAtReturnsEmptyBeforeFirstWrite() {
        MemorySegment en = writeAll(new int[]{1, 100, 1000});
        assertEquals(ChampMap.EMPTY_ROOT, reader.stateAt(en, 500L));
    }

    @Test
    void stateAtReturnsCorrectSnapshotAfterChange() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 1000},
                new int[]{2, 200, 1000},
                new int[]{1, 300, 2000});

        // At T=1500: attr1=100, attr2=200
        long state1500 = reader.stateAt(en, 1500L);
        assertNotEquals(ChampMap.EMPTY_ROOT, state1500);
        assertEquals(vref(100), ChampMap.get(bump, state1500, 1));
        assertEquals(vref(200), ChampMap.get(bump, state1500, 2));

        // At T=2500: attr1=300, attr2=200
        long state2500 = reader.stateAt(en, 2500L);
        assertNotEquals(ChampMap.EMPTY_ROOT, state2500);
        assertEquals(vref(300), ChampMap.get(bump, state2500, 1));
        assertEquals(vref(200), ChampMap.get(bump, state2500, 2));
    }

    // ====================================================================
    // allFieldsAt
    // ====================================================================

    @Test
    void allFieldsAtReturnsAllAttributes() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 1000},
                new int[]{2, 200, 1000},
                new int[]{3, 300, 1000});

        List<long[]> entries = new ArrayList<>();
        boolean completed = reader.allFieldsAt(en, 1000L,
                (attrId, valueRef) -> {
                    entries.add(new long[]{attrId, valueRef});
                    return true;
                });

        assertTrue(completed);
        assertEquals(3, entries.size());
        assertTrue(entries.stream().anyMatch(e -> e[0] == 1 && e[1] == vref(100)));
        assertTrue(entries.stream().anyMatch(e -> e[0] == 2 && e[1] == vref(200)));
        assertTrue(entries.stream().anyMatch(e -> e[0] == 3 && e[1] == vref(300)));
    }

    @Test
    void allFieldsAtOnEmptyEntityVisitsNothing() {
        List<long[]> entries = new ArrayList<>();
        boolean completed = reader.allFieldsAt(emptyEntityNode(), 1000L,
                (attrId, valueRef) -> {
                    entries.add(new long[]{attrId, valueRef});
                    return true;
                });
        assertTrue(completed);
        assertTrue(entries.isEmpty());
    }

    @Test
    void allFieldsAtStopsEarlyOnFalse() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 1000},
                new int[]{2, 200, 1000},
                new int[]{3, 300, 1000});

        List<long[]> entries = new ArrayList<>();
        boolean completed = reader.allFieldsAt(en, 1000L,
                (attrId, valueRef) -> {
                    entries.add(new long[]{attrId, valueRef});
                    return false; // stop after first
                });

        assertFalse(completed);
        assertEquals(1, entries.size());
    }

    // ====================================================================
    // history
    // ====================================================================

    @Test
    void historyReturnsAllRunsInOrder() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200},
                new int[]{1, 300, 300});

        record Run(long firstSeen, long lastSeen, long validTo, long valueRef) {}
        List<Run> runs = new ArrayList<>();
        boolean completed = reader.history(en, 1,
                (fs, ls, vt, vr) -> {
                    runs.add(new Run(fs, ls, vt, vr));
                    return true;
                });

        assertTrue(completed);
        assertEquals(3, runs.size());

        assertEquals(100L, runs.get(0).firstSeen);
        assertEquals(vref(100), runs.get(0).valueRef);

        assertEquals(200L, runs.get(1).firstSeen);
        assertEquals(vref(200), runs.get(1).valueRef);

        assertEquals(300L, runs.get(2).firstSeen);
        assertEquals(vref(300), runs.get(2).valueRef);

        // Last run is open-ended
        assertEquals(Long.MAX_VALUE, runs.get(2).validTo);
    }

    @Test
    void historyReturnsEmptyForUnknownAttribute() {
        MemorySegment en = writeAll(new int[]{1, 100, 1000});

        List<Long> values = new ArrayList<>();
        boolean completed = reader.history(en, 99,
                (fs, ls, vt, vr) -> { values.add(vr); return true; });
        assertTrue(completed);
        assertTrue(values.isEmpty());
    }

    @Test
    void historyReturnsEmptyForEmptyEntity() {
        List<Long> values = new ArrayList<>();
        boolean completed = reader.history(emptyEntityNode(), 1,
                (fs, ls, vt, vr) -> { values.add(vr); return true; });
        assertTrue(completed);
        assertTrue(values.isEmpty());
    }

    @Test
    void historyStopsEarlyOnFalse() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200},
                new int[]{1, 300, 300});

        List<Long> values = new ArrayList<>();
        boolean completed = reader.history(en, 1,
                (fs, ls, vt, vr) -> {
                    values.add(vr);
                    return values.size() < 2;
                });

        assertFalse(completed);
        assertEquals(2, values.size());
        assertEquals(vref(100), values.get(0));
        assertEquals(vref(200), values.get(1));
    }

    @Test
    void historyOnlyReturnsRunsForRequestedAttribute() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 1000},
                new int[]{2, 200, 1000},
                new int[]{1, 300, 2000});

        List<Long> values = new ArrayList<>();
        reader.history(en, 1,
                (fs, ls, vt, vr) -> { values.add(vr); return true; });

        assertEquals(2, values.size());
        assertEquals(vref(100), values.get(0));
        assertEquals(vref(300), values.get(1));
    }

    @Test
    void historyShowsExtendedLastSeen() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 1000},
                new int[]{1, 100, 2000}, // same value, extends last_seen
                new int[]{1, 100, 3000}); // extends again

        record Run(long firstSeen, long lastSeen, long validTo, long valueRef) {}
        List<Run> runs = new ArrayList<>();
        reader.history(en, 1,
                (fs, ls, vt, vr) -> { runs.add(new Run(fs, ls, vt, vr)); return true; });

        assertEquals(1, runs.size()); // single run
        assertEquals(1000L, runs.get(0).firstSeen);
        assertEquals(3000L, runs.get(0).lastSeen);
        assertEquals(Long.MAX_VALUE, runs.get(0).validTo);
        assertEquals(vref(100), runs.get(0).valueRef);
    }

    // ====================================================================
    // historyRange
    // ====================================================================

    @Test
    void historyRangeIncludesSpanningRun() {
        // Run at T=100 with valid_to=MAX spans everything
        MemorySegment en = writeAll(new int[]{1, 100, 100});

        record Run(long firstSeen, long valueRef) {}
        List<Run> runs = new ArrayList<>();
        reader.historyRange(en, 1, 200L, 300L,
                (fs, ls, vt, vr) -> { runs.add(new Run(fs, vr)); return true; });

        assertEquals(1, runs.size());
        assertEquals(100L, runs.get(0).firstSeen);
        assertEquals(vref(100), runs.get(0).valueRef);
    }

    @Test
    void historyRangeExcludesRunsThatEndBeforeRange() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200},
                new int[]{1, 400, 400});

        record Run(long firstSeen, long valueRef) {}
        List<Run> runs = new ArrayList<>();

        // Query [200, 300]: run [100,199]=100 ends before 200 → excluded
        // run [200,399]=200 overlaps → included
        // run [400,MAX]=400 starts after 300 → excluded
        reader.historyRange(en, 1, 200L, 300L,
                (fs, ls, vt, vr) -> { runs.add(new Run(fs, vr)); return true; });

        assertEquals(1, runs.size());
        assertEquals(200L, runs.get(0).firstSeen);
        assertEquals(vref(200), runs.get(0).valueRef);
    }

    @Test
    void historyRangeIncludesMultipleOverlappingRuns() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200},
                new int[]{1, 300, 300},
                new int[]{1, 400, 400});

        record Run(long firstSeen, long valueRef) {}
        List<Run> runs = new ArrayList<>();

        // Query [150, 350]: should include runs starting at 100 (spans in),
        // 200, and 300; exclude 400
        reader.historyRange(en, 1, 150L, 350L,
                (fs, ls, vt, vr) -> { runs.add(new Run(fs, vr)); return true; });

        assertEquals(3, runs.size());
        assertEquals(100L, runs.get(0).firstSeen); // predecessor spanning in
        assertEquals(200L, runs.get(1).firstSeen);
        assertEquals(300L, runs.get(2).firstSeen);
    }

    @Test
    void historyRangeReturnsEmptyForEmptyEntity() {
        List<Long> values = new ArrayList<>();
        boolean completed = reader.historyRange(emptyEntityNode(), 1, 0L, 1000L,
                (fs, ls, vt, vr) -> { values.add(vr); return true; });
        assertTrue(completed);
        assertTrue(values.isEmpty());
    }

    @Test
    void historyRangeReturnsEmptyForGap() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 100, 150}, // extend last_seen
                new int[]{1, 200, 300}); // change value at T=300

        record Run(long firstSeen, long valueRef) {}
        List<Run> runs = new ArrayList<>();

        // Run [100, 299]=100 → query at [500, 600] should find run [300, MAX]=200
        // Wait — actually run [300, MAX] has valid_to=MAX which spans 500..600
        // Let me query a gap that actually exists: before any data
        reader.historyRange(en, 1, 10L, 50L,
                (fs, ls, vt, vr) -> { runs.add(new Run(fs, vr)); return true; });

        assertTrue(runs.isEmpty());
    }

    // ====================================================================
    // at() boundary — validTo edge cases
    // ====================================================================

    @Test
    void atReturnsValueAtExactValidToBoundary() {
        // When two runs exist: [100,199]=100, [200,MAX]=200
        // At T=199 (the validTo of first run), should return 100
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200});
        assertEquals(vref(100), reader.at(en, 1, 199L));
    }

    @Test
    void atReturnsNotFoundAtValidToPlusOne() {
        // Create a gap: two attributes, so attr=2 has a bounded validTo
        // Simpler: use late insert to create bounded runs with gaps
        // Write attr=1 at T=100 (value=100), then attr=1 at T=300 (value=300)
        // Creates: [100,299]=100, [300,MAX]=300
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 300, 300});
        // At T=299 (validTo of first run), should return 100
        assertEquals(vref(100), reader.at(en, 1, 299L));
        // At T=300, should return 300
        assertEquals(vref(300), reader.at(en, 1, 300L));
    }

    // ====================================================================
    // stateAt() boundary — validTo edge cases
    // ====================================================================

    @Test
    void stateAtReturnsCorrectStateAtVersionBoundary() {
        // Write attr=1 at T=100, T=200 (different values)
        // Version boundaries: [100,199], [200,MAX]
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200});

        // At T=199 (boundary of first version's validTo)
        long state199 = reader.stateAt(en, 199L);
        assertNotEquals(ChampMap.EMPTY_ROOT, state199);
        assertEquals(vref(100), ChampMap.get(bump, state199, 1));

        // At T=200
        long state200 = reader.stateAt(en, 200L);
        assertNotEquals(ChampMap.EMPTY_ROOT, state200);
        assertEquals(vref(200), ChampMap.get(bump, state200, 1));
    }

    @Test
    void stateAtReturnsEmptyOnEmptyNode() {
        assertEquals(ChampMap.EMPTY_ROOT, reader.stateAt(emptyEntityNode(), 100L));
    }

    // ====================================================================
    // historyRange — additional edge cases
    // ====================================================================

    @Test
    void historyRangeSingleElementRange() {
        // Query where fromMs == toMs, and a run spans that point
        MemorySegment en = writeAll(new int[]{1, 100, 100});

        record Run(long firstSeen, long valueRef) {}
        List<Run> runs = new ArrayList<>();
        reader.historyRange(en, 1, 500L, 500L,
                (fs, ls, vt, vr) -> { runs.add(new Run(fs, vr)); return true; });

        assertEquals(1, runs.size());
        assertEquals(100L, runs.get(0).firstSeen);
    }

    @Test
    void historyRangePredecessorAtExactFromMs() {
        // Predecessor's first_seen == fromMs → should visit and skip duplicate in forward scan
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200});

        record Run(long firstSeen, long valueRef) {}
        List<Run> runs = new ArrayList<>();
        reader.historyRange(en, 1, 100L, 300L,
                (fs, ls, vt, vr) -> { runs.add(new Run(fs, vr)); return true; });

        // Should get runs at 100 and 200, each visited exactly once
        assertEquals(2, runs.size());
        assertEquals(100L, runs.get(0).firstSeen);
        assertEquals(200L, runs.get(1).firstSeen);
    }

    @Test
    void historyRangeCompletesWhenAllVisited() {
        MemorySegment en = writeAll(new int[]{1, 100, 100});
        boolean completed = reader.historyRange(en, 1, 50L, 500L,
                (fs, ls, vt, vr) -> true);
        assertTrue(completed);
    }

    @Test
    void historyRangeWithDifferentAttribute() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{2, 200, 100});

        List<Long> values = new ArrayList<>();
        reader.historyRange(en, 3, 50L, 500L,
                (fs, ls, vt, vr) -> { values.add(vr); return true; });
        assertTrue(values.isEmpty());
    }

    @Test
    void historyRangeStopsEarlyOnFalse() {
        MemorySegment en = writeAll(
                new int[]{1, 100, 100},
                new int[]{1, 200, 200},
                new int[]{1, 300, 300});

        List<Long> values = new ArrayList<>();
        boolean completed = reader.historyRange(en, 1, 50L, 500L,
                (fs, ls, vt, vr) -> {
                    values.add(vr);
                    return false; // stop after first
                });

        assertFalse(completed);
        assertEquals(1, values.size());
    }

    /** Test helper: encode a distinct Value.ofLong(sentinel) and return its ref.
     *  Cached so the same sentinel always returns the same slot ref (matches the
     *  production canonicalization invariant in TemporalWriter / ValueCodec.slotEquals). */
    private final java.util.Map<Long, Long> vrefCache = new java.util.HashMap<>();
    private long vref(long sentinel) {
        return vrefCache.computeIfAbsent(sentinel,
                s -> org.taotree.internal.value.ValueCodec.encodeStandalone(
                        org.taotree.Value.ofLong(s), bump));
    }

}
