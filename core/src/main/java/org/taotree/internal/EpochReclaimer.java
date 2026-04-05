package org.taotree.internal;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Epoch-based dual-pinned memory reclaimer for off-heap ART nodes.
 *
 * <p>When a writer replaces a node (COW), the old node is <em>retired</em> rather
 * than freed immediately, because concurrent readers may still reference it.
 * Each retired node is tagged with the global generation at the time of retirement.
 *
 * <p>A retired node at generation G is reclaimable when
 * {@code safeReclaimGeneration >= G}, where:
 * <pre>
 *   safeReclaimGeneration = min(durableGeneration, min(activeReaderGenerations))
 * </pre>
 *
 * <p>The two pins ensure:
 * <ul>
 *   <li><b>Reader pin:</b> no active reader has a generation &lt;= the retired generation.
 *   <li><b>Durability pin:</b> the last durable checkpoint has a generation &gt;= the
 *       retired generation, so the node is no longer needed for recovery.
 * </ul>
 *
 * <p><b>Thread safety:</b> {@link #enterEpoch()} and {@link #exitEpoch(int)} are
 * lock-free (single {@code setRelease} each). {@link #retire(long)} is thread-local
 * (no CAS, no lock). {@link #reclaim()} may be called from any single thread.
 */
public final class EpochReclaimer implements AutoCloseable {

    private static final long INACTIVE = Long.MAX_VALUE;
    private static final int DEFAULT_MAX_READER_SLOTS = 256;

    private final AtomicLong globalGeneration = new AtomicLong(0);
    private final AtomicLong durableGeneration = new AtomicLong(0);

    private final AtomicLong[] readerSlots;
    private final AtomicInteger nextSlotIndex = new AtomicInteger(0);
    private final ThreadLocal<Integer> threadSlotIndex;

    private final ThreadLocal<List<RetiredNode>> retireList;
    private final ConcurrentLinkedQueue<List<RetiredNode>> allRetireLists = new ConcurrentLinkedQueue<>();

    private final SlabAllocator slab;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    public EpochReclaimer(SlabAllocator slab) {
        this(slab, DEFAULT_MAX_READER_SLOTS);
    }

    public EpochReclaimer(SlabAllocator slab, int maxReaderSlots) {
        this.slab = slab;
        this.readerSlots = new AtomicLong[maxReaderSlots];
        for (int i = 0; i < maxReaderSlots; i++) {
            readerSlots[i] = new AtomicLong(INACTIVE);
        }
        this.threadSlotIndex = ThreadLocal.withInitial(() -> {
            int idx = nextSlotIndex.getAndIncrement();
            if (idx >= readerSlots.length) {
                throw new IllegalStateException(
                    "Reader slots exhausted (" + readerSlots.length + ")");
            }
            return idx;
        });
        this.retireList = ThreadLocal.withInitial(() -> {
            var list = new ArrayList<RetiredNode>();
            allRetireLists.add(list);
            return list;
        });
    }

    // -----------------------------------------------------------------------
    // Reader protocol
    // -----------------------------------------------------------------------

    /**
     * Enter a read epoch. Sets this thread's reader slot to the current global
     * generation with release semantics.
     *
     * @return the slot index (pass to {@link #exitEpoch(int)})
     */
    public int enterEpoch() {
        int slot = threadSlotIndex.get();
        readerSlots[slot].setRelease(globalGeneration.get());
        return slot;
    }

    /**
     * Exit a read epoch. Sets the slot to {@code INACTIVE} with release semantics.
     */
    public void exitEpoch(int slotIndex) {
        readerSlots[slotIndex].setRelease(INACTIVE);
    }

    // -----------------------------------------------------------------------
    // Writer protocol
    // -----------------------------------------------------------------------

    /**
     * Retire an old node pointer. The node is added to this thread's retire list
     * tagged with the current global generation. No synchronization is required.
     */
    public void retire(long nodePtr) {
        retireList.get().add(new RetiredNode(nodePtr, globalGeneration.get()));
    }

    /**
     * Advance the global generation. Called by writers after publication.
     *
     * @return the new generation value
     */
    public long advanceGeneration() {
        return globalGeneration.incrementAndGet();
    }

    // -----------------------------------------------------------------------
    // Durability protocol
    // -----------------------------------------------------------------------

    /**
     * Advance the durable generation. Called by the compactor/checkpoint thread
     * after a checkpoint is persisted.
     */
    public void advanceDurableGeneration(long gen) {
        durableGeneration.setRelease(gen);
    }

    public long durableGeneration() {
        return durableGeneration.get();
    }

    public long globalGeneration() {
        return globalGeneration.get();
    }

    // -----------------------------------------------------------------------
    // Reclamation
    // -----------------------------------------------------------------------

    /**
     * Compute the safe reclaim boundary and free all retired nodes below it.
     *
     * @return the number of nodes freed
     */
    public int reclaim() {
        long safeGen = safeReclaimGeneration();
        int freed = 0;
        for (List<RetiredNode> list : allRetireLists) {
            freed += drainList(list, safeGen);
        }
        return freed;
    }

    /**
     * Compute the current safe reclaim generation:
     * {@code min(durableGeneration, min(activeReaderGenerations))}.
     */
    public long safeReclaimGeneration() {
        long min = durableGeneration.get();
        int allocated = nextSlotIndex.get();
        for (int i = 0; i < allocated; i++) {
            long g = readerSlots[i].getAcquire();
            if (g < min) min = g;
        }
        return min;
    }

    /** Total retired nodes across all threads (for testing). */
    public int pendingRetiredCount() {
        int count = 0;
        for (List<RetiredNode> list : allRetireLists) {
            count += list.size();
        }
        return count;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Drain all retire lists unconditionally, freeing every retired node
     * regardless of generation. For clean shutdown only.
     */
    @Override
    public void close() {
        for (List<RetiredNode> list : allRetireLists) {
            for (RetiredNode rn : list) {
                slab.free(rn.nodePtr);
            }
            list.clear();
        }
    }

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    private int drainList(List<RetiredNode> list, long safeGen) {
        int freed = 0;
        Iterator<RetiredNode> it = list.iterator();
        while (it.hasNext()) {
            RetiredNode rn = it.next();
            if (rn.retireGeneration < safeGen) {
                slab.free(rn.nodePtr);
                it.remove();
                freed++;
            }
        }
        return freed;
    }

    private record RetiredNode(long nodePtr, long retireGeneration) {}
}
