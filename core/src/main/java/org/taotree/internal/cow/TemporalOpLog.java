package org.taotree.internal.cow;

/**
 * Records high-level temporal operations performed during a deferred-commit
 * {@code WriteScope} so they can be <em>replayed</em> against a newer root on
 * conflict, instead of the lower-level {@link MutationLog} leaf-segment copy
 * which clobbers concurrent EntityNode CHAMP/ART updates.
 *
 * <p>Each entry captures the logical intent of a single API call:
 * <ul>
 *   <li>{@link #KIND_PUT} — {@code put(entity, attr, value@ts)}, with a stable
 *       {@code valueRef} (opaque encoded-value pointer in the writer's arena)</li>
 *   <li>{@link #KIND_DELETE_ATTR} — {@code delete(entity, attr)}</li>
 * </ul>
 *
 * <p>Entity-level deletes ({@code delete(KeyBuilder)}) transition the scope
 * to locked mode and are therefore not routed through this log.
 */
public final class TemporalOpLog {

    public static final int KIND_PUT = 0;
    public static final int KIND_DELETE_ATTR = 1;

    private static final int INITIAL_CAPACITY = 32;

    private byte[][] entityKeys;
    private int[] attrIds;
    private long[] valueRefs;
    private long[] timestamps;
    private int[] kinds;
    private int count;

    public TemporalOpLog() {
        entityKeys = new byte[INITIAL_CAPACITY][];
        attrIds = new int[INITIAL_CAPACITY];
        valueRefs = new long[INITIAL_CAPACITY];
        timestamps = new long[INITIAL_CAPACITY];
        kinds = new int[INITIAL_CAPACITY];
    }

    public void recordPut(byte[] entityKey, int attrId, long valueRef, long ts) {
        record(KIND_PUT, entityKey, attrId, valueRef, ts);
    }

    public void recordDeleteAttr(byte[] entityKey, int attrId) {
        record(KIND_DELETE_ATTR, entityKey, attrId, 0L, 0L);
    }

    private void record(int kind, byte[] entityKey, int attrId, long valueRef, long ts) {
        if (count == entityKeys.length) grow();
        byte[] copy = new byte[entityKey.length];
        System.arraycopy(entityKey, 0, copy, 0, entityKey.length);
        entityKeys[count] = copy;
        attrIds[count] = attrId;
        valueRefs[count] = valueRef;
        timestamps[count] = ts;
        kinds[count] = kind;
        count++;
    }

    public int size() { return count; }
    public byte[] entityKey(int i) { return entityKeys[i]; }
    public int attrId(int i) { return attrIds[i]; }
    public long valueRef(int i) { return valueRefs[i]; }
    public long timestamp(int i) { return timestamps[i]; }
    public int kind(int i) { return kinds[i]; }

    private void grow() {
        int newCap = entityKeys.length * 2;
        var newKeys = new byte[newCap][];
        var newAttrs = new int[newCap];
        var newVals = new long[newCap];
        var newTs = new long[newCap];
        var newKinds = new int[newCap];
        System.arraycopy(entityKeys, 0, newKeys, 0, count);
        System.arraycopy(attrIds, 0, newAttrs, 0, count);
        System.arraycopy(valueRefs, 0, newVals, 0, count);
        System.arraycopy(timestamps, 0, newTs, 0, count);
        System.arraycopy(kinds, 0, newKinds, 0, count);
        entityKeys = newKeys;
        attrIds = newAttrs;
        valueRefs = newVals;
        timestamps = newTs;
        kinds = newKinds;
    }
}
