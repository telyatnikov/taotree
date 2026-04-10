package org.taotree.internal.cow;

/**
 * Open-addressing hash set for primitive {@code long} values — no boxing.
 *
 * <p>Replaces {@code HashSet<Long>} on the deferred-commit hot path
 * ({@code WriteScope.seenLeafPtrs}) to eliminate autoboxing overhead on every
 * {@code contains()} and {@code add()} call.
 *
 * <p>Uses linear probing with a power-of-two table. The sentinel value
 * {@code 0L} marks empty slots (valid because arena/slab pointers are never 0).
 *
 * <p><b>Thread safety:</b> not thread-safe. Owned by a single {@code WriteScope}.
 */
public final class LongOpenHashSet {

    private static final long EMPTY = 0L;
    private static final int MIN_CAPACITY = 16;
    private static final float LOAD_FACTOR = 0.75f;

    private long[] table;
    private int size;
    private int threshold;

    public LongOpenHashSet() {
        this(MIN_CAPACITY);
    }

    public LongOpenHashSet(int initialCapacity) {
        int cap = Integer.highestOneBit(Math.max(initialCapacity, MIN_CAPACITY) - 1) << 1;
        table = new long[cap];
        threshold = (int) (cap * LOAD_FACTOR);
    }

    /**
     * Add a value.
     *
     * @return {@code true} if the value was newly added; {@code false} if already present
     */
    public boolean add(long value) {
        if (value == EMPTY) throwEmpty();
        int mask = table.length - 1;
        int idx = mix(value) & mask;
        while (true) {
            long slot = table[idx];
            if (slot == EMPTY) {
                table[idx] = value;
                if (++size > threshold) grow();
                return true;
            }
            if (slot == value) return false;
            idx = (idx + 1) & mask;
        }
    }

    /** Cold-path helper — extracted so the hot {@link #add} stays small for JIT inlining. */
    private static void throwEmpty() {
        throw new IllegalArgumentException(
            "LongOpenHashSet does not support 0L as a value (sentinel for empty slots)");
    }

    public boolean contains(long value) {
        if (value == EMPTY) return false;
        int mask = table.length - 1;
        int idx = mix(value) & mask;
        while (true) {
            long slot = table[idx];
            if (slot == value) return true;
            if (slot == EMPTY) return false;
            idx = (idx + 1) & mask;
        }
    }

    public int size() { return size; }

    public void clear() {
        java.util.Arrays.fill(table, EMPTY);
        size = 0;
    }

    private void grow() {
        long[] old = table;
        int newCap = old.length << 1;
        table = new long[newCap];
        threshold = (int) (newCap * LOAD_FACTOR);
        int mask = newCap - 1;
        for (long val : old) {
            if (val != EMPTY) {
                int idx = mix(val) & mask;
                while (table[idx] != EMPTY) idx = (idx + 1) & mask;
                table[idx] = val;
            }
        }
    }

    /** Fibonacci hash for good distribution of pointer-like longs. */
    private static int mix(long v) {
        v ^= v >>> 33;
        v *= 0xff51afd7ed558ccdL;
        v ^= v >>> 33;
        return (int) v;
    }
}
