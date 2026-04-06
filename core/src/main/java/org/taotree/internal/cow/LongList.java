package org.taotree.internal.cow;

import java.util.Arrays;

/**
 * Growable primitive {@code long} list — no boxing, no {@link Long} objects.
 *
 * <p>Replaces {@code ArrayList<Long>} on the COW hot path (retiree tracking)
 * to eliminate autoboxing overhead. Typical usage: 0-20 elements per mutation.
 *
 * <p><b>Thread safety:</b> not thread-safe. Each instance is owned by a single
 * writer thread or write scope.
 */
public final class LongList {

    private long[] data;
    private int size;

    /** Create with default initial capacity (8). */
    public LongList() {
        this(8);
    }

    /** Create with the given initial capacity. */
    public LongList(int initialCapacity) {
        this.data = new long[initialCapacity];
    }

    /** Append a value. */
    public void add(long value) {
        if (size == data.length) {
            data = Arrays.copyOf(data, data.length * 2);
        }
        data[size++] = value;
    }

    /** Append all values from another LongList. */
    public void addAll(LongList other) {
        int needed = size + other.size;
        if (needed > data.length) {
            data = Arrays.copyOf(data, Math.max(needed, data.length * 2));
        }
        System.arraycopy(other.data, 0, data, size, other.size);
        size += other.size;
    }

    /** Get the value at the given index. */
    public long get(int index) {
        return data[index];
    }

    /** Number of elements. */
    public int size() {
        return size;
    }

    /** True if empty. */
    public boolean isEmpty() {
        return size == 0;
    }

    /** Remove all elements (retains backing array). */
    public void clear() {
        size = 0;
    }

    /** Returns an empty, immutable LongList singleton. */
    public static LongList empty() {
        return EMPTY;
    }

    private static final LongList EMPTY = new LongList(0);
}
