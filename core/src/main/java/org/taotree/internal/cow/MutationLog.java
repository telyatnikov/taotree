package org.taotree.internal.cow;

import java.lang.foreign.MemorySegment;

/**
 * Records mutations performed during a deferred-commit {@code WriteScope}
 * so they can be replayed against a newer root on conflict.
 *
 * <p>Each entry stores the key bytes (copied), leaf class, the leaf pointer
 * from the writer's private COW tree, and optionally a snapshot of the leaf
 * value at the time of first access (before the application modifies it).
 *
 * <p>On rebase, each entry is replayed: {@code getOrCreateCopy} against the
 * new root, then a {@link org.taotree.ConflictResolver} merges the values
 * using the pending, published, and snapshot leaf values.
 *
 * <p>Uses parallel arrays for cache-friendliness and zero per-entry object
 * allocation. Entries are de-duplicated by leaf pointer (only the first
 * access per key is recorded).
 */
public final class MutationLog {

    private static final int INITIAL_CAPACITY = 64;

    private byte[][] keys;
    private int[] keyLens;
    private int[] leafClasses;
    private long[] leafPtrs;
    private byte[][] snapshotValues; // snapshot leaf value at time of first access
    private int count;

    public MutationLog() {
        keys = new byte[INITIAL_CAPACITY][];
        keyLens = new int[INITIAL_CAPACITY];
        leafClasses = new int[INITIAL_CAPACITY];
        leafPtrs = new long[INITIAL_CAPACITY];
        snapshotValues = new byte[INITIAL_CAPACITY][];
    }

    /**
     * Record a mutation with a snapshot of the leaf value at first access.
     * The key bytes and snapshot value are copied defensively.
     */
    public void record(MemorySegment key, int keyLen, int leafClass,
                       long leafPtr, MemorySegment snapshotValue, int valueSize) {
        if (count == keys.length) grow();
        byte[] keyCopy = new byte[keyLen];
        MemorySegment.copy(key, 0, MemorySegment.ofArray(keyCopy), 0, keyLen);
        byte[] snapCopy = new byte[valueSize];
        MemorySegment.copy(snapshotValue, 0, MemorySegment.ofArray(snapCopy), 0, valueSize);
        keys[count] = keyCopy;
        keyLens[count] = keyLen;
        leafClasses[count] = leafClass;
        leafPtrs[count] = leafPtr;
        snapshotValues[count] = snapCopy;
        count++;
    }

    public int size() { return count; }

    public byte[] key(int i) { return keys[i]; }
    public int keyLen(int i) { return keyLens[i]; }
    public int leafClass(int i) { return leafClasses[i]; }
    public long leafPtr(int i) { return leafPtrs[i]; }
    public byte[] snapshotValue(int i) { return snapshotValues[i]; }

    private void grow() {
        int newCap = keys.length * 2;
        var newKeys = new byte[newCap][];
        var newKeyLens = new int[newCap];
        var newLeafClasses = new int[newCap];
        var newLeafPtrs = new long[newCap];
        var newSnapshots = new byte[newCap][];
        System.arraycopy(keys, 0, newKeys, 0, count);
        System.arraycopy(keyLens, 0, newKeyLens, 0, count);
        System.arraycopy(leafClasses, 0, newLeafClasses, 0, count);
        System.arraycopy(leafPtrs, 0, newLeafPtrs, 0, count);
        System.arraycopy(snapshotValues, 0, newSnapshots, 0, count);
        keys = newKeys;
        keyLens = newKeyLens;
        leafClasses = newLeafClasses;
        leafPtrs = newLeafPtrs;
        snapshotValues = newSnapshots;
    }
}
