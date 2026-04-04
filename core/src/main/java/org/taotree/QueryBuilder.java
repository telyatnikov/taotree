package org.taotree;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Resolve-only key builder for query/scan operations.
 *
 * <p>Unlike {@link KeyBuilder}, dictionary fields use {@link TaoDictionary#resolve}
 * (read lock) instead of {@link TaoDictionary#intern} (write lock). This makes
 * {@code QueryBuilder} safe to use inside a {@link TaoTree.ReadScope}.
 *
 * <p>Tracks per-field state:
 * <ul>
 *   <li><b>set</b> — was this field explicitly set?</li>
 *   <li><b>resolved</b> — did the value resolve successfully? (always true for non-dict fields;
 *       false for dict fields where the string is unknown)</li>
 * </ul>
 *
 * <p>A scan using this builder is <b>satisfiable</b> up to a given handle only if all fields
 * from index 0 through that handle's field index are both set and resolved.
 * If any field is unset or unresolved, the scan returns empty without traversing.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * var qb = tree.newQueryBuilder(arena);
 * qb.set(KINGDOM, "Animalia");   // dict.resolve() — read lock only
 * qb.set(PHYLUM, "Chordata");
 *
 * try (var r = tree.read()) {
 *     r.scan(qb, PHYLUM, leaf -> {
 *         int count = leaf.get(COUNT);
 *         return true; // continue
 *     });
 * }
 * }</pre>
 *
 * <p>Not thread-safe. Use one builder per thread.
 */
public final class QueryBuilder {

    private final KeyLayout layout;
    private final MemorySegment buf;
    private final boolean[] fieldSet;
    private final boolean[] fieldResolved;

    /**
     * Create a query builder for the given layout.
     *
     * @param layout the key layout (must have bound dictionaries for dict fields)
     * @param arena  arena for allocating the reusable key buffer
     */
    public QueryBuilder(KeyLayout layout, Arena arena) {
        this.layout = layout;
        this.buf = arena.allocate(layout.totalWidth(), 1);
        this.fieldSet = new boolean[layout.fieldCount()];
        this.fieldResolved = new boolean[layout.fieldCount()];
    }

    // -- Handle-based setters (resolve-only for dicts) --

    /** Set a dict16 field. Uses {@code resolve()} — safe under read lock. */
    public QueryBuilder set(KeyHandle.Dict16 h, String value) {
        int fi = h.fieldIndex();
        fieldSet[fi] = true;
        if (value == null) {
            TaoKey.encodeU16(buf, h.offset(), (short) 0);
            fieldResolved[fi] = true;
        } else {
            int code = h.dict().resolve(value);
            fieldResolved[fi] = (code >= 0);
            if (code > 0) {
                TaoKey.encodeU16(buf, h.offset(), (short) code);
            }
        }
        return this;
    }

    /** Set a dict32 field. Uses {@code resolve()} — safe under read lock. */
    public QueryBuilder set(KeyHandle.Dict32 h, String value) {
        int fi = h.fieldIndex();
        fieldSet[fi] = true;
        if (value == null) {
            TaoKey.encodeU32(buf, h.offset(), 0);
            fieldResolved[fi] = true;
        } else {
            int code = h.dict().resolve(value);
            fieldResolved[fi] = (code >= 0);
            if (code > 0) {
                TaoKey.encodeU32(buf, h.offset(), code);
            }
        }
        return this;
    }

    /** Set a UInt8 field. Always resolves. */
    public QueryBuilder set(KeyHandle.UInt8 h, byte value) {
        TaoKey.encodeU8(buf, h.offset(), value);
        fieldSet[h.fieldIndex()] = true;
        fieldResolved[h.fieldIndex()] = true;
        return this;
    }

    /** Set a UInt16 field. Always resolves. */
    public QueryBuilder set(KeyHandle.UInt16 h, short value) {
        TaoKey.encodeU16(buf, h.offset(), value);
        fieldSet[h.fieldIndex()] = true;
        fieldResolved[h.fieldIndex()] = true;
        return this;
    }

    /** Set a UInt32 field. Always resolves. */
    public QueryBuilder set(KeyHandle.UInt32 h, int value) {
        TaoKey.encodeU32(buf, h.offset(), value);
        fieldSet[h.fieldIndex()] = true;
        fieldResolved[h.fieldIndex()] = true;
        return this;
    }

    /** Set a UInt64 field. Always resolves. */
    public QueryBuilder set(KeyHandle.UInt64 h, long value) {
        TaoKey.encodeU64(buf, h.offset(), value);
        fieldSet[h.fieldIndex()] = true;
        fieldResolved[h.fieldIndex()] = true;
        return this;
    }

    /** Set an Int64 field. Always resolves. */
    public QueryBuilder set(KeyHandle.Int64 h, long value) {
        TaoKey.encodeI64(buf, h.offset(), value);
        fieldSet[h.fieldIndex()] = true;
        fieldResolved[h.fieldIndex()] = true;
        return this;
    }

    // -- State queries --

    /**
     * Returns true if a prefix scan up to (and including) the given handle is satisfiable.
     *
     * <p>All fields from index 0 through {@code h.fieldIndex()} must be set and resolved.
     */
    public boolean isSatisfiable(KeyHandle h) {
        for (int i = 0; i <= h.fieldIndex(); i++) {
            if (!fieldSet[i] || !fieldResolved[i]) return false;
        }
        return true;
    }

    /**
     * Returns the prefix length in bytes up to and including the given handle's field.
     */
    public int prefixLength(KeyHandle h) {
        return h.end();
    }

    /** The encoded key buffer. */
    public MemorySegment key() { return buf; }

    /** The full key length in bytes. */
    public int keyLen() { return layout.totalWidth(); }

    /** Reset all fields to unset/unresolved for reuse. */
    public QueryBuilder clear() {
        buf.fill((byte) 0);
        java.util.Arrays.fill(fieldSet, false);
        java.util.Arrays.fill(fieldResolved, false);
        return this;
    }
}
