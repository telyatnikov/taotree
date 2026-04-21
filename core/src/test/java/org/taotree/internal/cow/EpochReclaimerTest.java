package org.taotree.internal.cow;

import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import org.taotree.internal.alloc.ChunkStore;
import org.taotree.internal.alloc.SlabAllocator;
import org.taotree.internal.cow.EpochReclaimer;

class EpochReclaimerTest {

    private static final int SEGMENT_SIZE = 64;

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    void retireAndReclaim() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var slab = new SlabAllocator(arena, cs, 1 << 20);
            int classId = slab.registerClass(SEGMENT_SIZE);

            var reclaimer = new EpochReclaimer(slab);

            // Allocate and retire some nodes
            long ptr1 = slab.allocate(classId);
            long ptr2 = slab.allocate(classId);
            long ptr3 = slab.allocate(classId);
            assertEquals(3, slab.totalSegmentsInUse());

            reclaimer.retire(ptr1);
            reclaimer.retire(ptr2);
            reclaimer.retire(ptr3);

            // Advance generation past retirement gen (all retired at gen 0)
            reclaimer.advanceGeneration(); // gen → 1
            // Set durable gen high enough
            reclaimer.advanceDurableGeneration(10);

            int freed = reclaimer.reclaim();
            assertEquals(3, freed);
            assertEquals(0, slab.totalSegmentsInUse());

            reclaimer.close();
        }
    }

    @Test
    void readerPinsPreventReclamation() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var slab = new SlabAllocator(arena, cs, 1 << 20);
            int classId = slab.registerClass(SEGMENT_SIZE);

            var reclaimer = new EpochReclaimer(slab);

            // Enter epoch (reader pins at gen 0)
            int slot = reclaimer.enterEpoch();

            // Allocate and retire
            long ptr = slab.allocate(classId);
            reclaimer.retire(ptr);
            reclaimer.advanceGeneration(); // gen → 1
            reclaimer.advanceDurableGeneration(10);

            // Reader is pinned at gen 0, retirement was at gen 0 → safeGen = min(10, 0) = 0
            // Cannot reclaim because retireGen (0) is NOT < safeGen (0) (strict <)
            int freed = reclaimer.reclaim();
            assertEquals(0, freed);
            assertEquals(1, slab.totalSegmentsInUse());

            // Exit epoch → reader unpinned
            reclaimer.exitEpoch(slot);

            // Now safeGen = min(10, MAX_VALUE) = 10, retire gen 0 < 10 → reclaimable
            freed = reclaimer.reclaim();
            assertEquals(1, freed);
            assertEquals(0, slab.totalSegmentsInUse());

            reclaimer.close();
        }
    }

    @Test
    void dualPinConstraint() throws Exception {
        try (var arena = Arena.ofConfined()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var slab = new SlabAllocator(arena, cs, 1 << 20);
            int classId = slab.registerClass(SEGMENT_SIZE);

            var reclaimer = new EpochReclaimer(slab);

            // Advance global to 5 before the reader enters
            for (int i = 0; i < 5; i++) reclaimer.advanceGeneration();

            // Enter reader epoch at gen 5
            // Then retire at gen 5
            long ptr = slab.allocate(classId);

            // Enter reader at gen 5 — this pins at current global gen (5)
            int slot = reclaimer.enterEpoch();

            reclaimer.retire(ptr); // retired at gen 5
            reclaimer.advanceGeneration(); // gen → 6

            // Advance durable to 10
            reclaimer.advanceDurableGeneration(10);

            // safeGen = min(10, 5) = 5, retireGen = 5, 5 < 5 is false → can't reclaim
            int freed = reclaimer.reclaim();
            assertEquals(0, freed);

            // Exit reader
            reclaimer.exitEpoch(slot);

            // safeGen = min(10, MAX_VALUE) = 10, retireGen = 5, 5 < 10 → reclaimable
            freed = reclaimer.reclaim();
            assertEquals(1, freed);
            assertEquals(0, slab.totalSegmentsInUse());

            reclaimer.close();
        }
    }

    @Test
    void concurrentRetireAndReclaim() throws Exception {
        try (var arena = Arena.ofShared()) {
            Path tmp = Files.createTempFile("slab-test-", ".dat");
            tmp.toFile().deleteOnExit();
            Files.delete(tmp);
            var cs = ChunkStore.createCheckpointed(tmp, arena, ChunkStore.DEFAULT_CHUNK_SIZE, false);
            var slab = new SlabAllocator(arena, cs, 1 << 20);
            int classId = slab.registerClass(SEGMENT_SIZE);

            var reclaimer = new EpochReclaimer(slab);
            reclaimer.advanceDurableGeneration(Long.MAX_VALUE - 1);

            int writerCount = 4;
            int retiresPerWriter = 100;
            int totalExpected = writerCount * retiresPerWriter;
            var barrier = new CyclicBarrier(writerCount);
            var errors = Collections.synchronizedList(new ArrayList<Throwable>());

            // Phase 1: Multiple writer threads retire concurrently.
            // Each thread has its own retire list (ThreadLocal), so retire() is safe.
            var writers = new Thread[writerCount];
            for (int w = 0; w < writerCount; w++) {
                writers[w] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < retiresPerWriter; i++) {
                            long ptr;
                            synchronized (slab) {
                                ptr = slab.allocate(classId);
                            }
                            reclaimer.retire(ptr);
                            reclaimer.advanceGeneration();
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                });
                writers[w].start();
            }

            for (var w : writers) w.join(10_000);
            assertTrue(errors.isEmpty(), "Writer errors: " + errors);

            // Phase 2: All retires are complete. Verify pending count and reclaim.
            assertEquals(totalExpected, reclaimer.pendingRetiredCount());
            assertEquals(totalExpected, (int) slab.totalSegmentsInUse());

            // Reclaim everything in one pass (no concurrent modification)
            int freed = reclaimer.reclaim();
            assertEquals(totalExpected, freed);
            assertEquals(0, slab.totalSegmentsInUse());
            assertEquals(0, reclaimer.pendingRetiredCount());

            reclaimer.close();
        }
    }
}
