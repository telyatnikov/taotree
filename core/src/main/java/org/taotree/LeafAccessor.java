package org.taotree;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.taotree.layout.LeafField;
import org.taotree.layout.LeafHandle;
import org.taotree.layout.LeafLayout;

/**
 * Typed, named accessor for leaf values structured by a {@link LeafLayout}.
 *
 * <p>Wraps a raw {@link MemorySegment} (the leaf value portion) and provides
 * type-safe getters and setters for each field defined in the layout.
 *
 * <p>For nullable fields, use {@link #isNull(LeafHandle)} to check presence and
 * {@link #setNull(LeafHandle)} to mark a field as absent. Setting a value via any
 * {@code set()} method automatically clears the null bit.
 *
 * <p>Obtained from {@link TaoTree.ReadScope#leaf} (read-only) or
 * {@link TaoTree.WriteScope#leaf} (read-write). Setters throw
 * {@link UnsupportedOperationException} on read-only accessors.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * try (var w = tree.write()) {
 *     long ptr = w.getOrCreate(key);
 *     var leaf = w.leaf(ptr, leafLayout);
 *     leaf.set(COUNT, 42);
 *     leaf.set(LAT, 48.5);           // auto-clears null bit
 *     leaf.setNull(ELEV);            // marks elevation as null
 * }
 * try (var r = tree.read()) {
 *     long ptr = r.lookup(key);
 *     var leaf = r.leaf(ptr, leafLayout);
 *     if (!leaf.isNull(ELEV)) {
 *         double elev = leaf.get(ELEV);
 *     }
 * }
 * }</pre>
 *
 * <p><b>Lifetime:</b> valid only while the enclosing scope is open.
 * Do not retain after {@code close()}.
 */
public final class LeafAccessor {

    private MemorySegment segment;
    private final LeafLayout layout;
    private final TaoTree tree;

    LeafAccessor(MemorySegment segment, LeafLayout layout, TaoTree tree) {
        this.segment = segment;
        this.layout = layout;
        this.tree = tree;
    }

    /**
     * Package-private: re-point this accessor to a different leaf segment.
     * Used by scan APIs for zero-allocation iteration.
     */
    void rebind(MemorySegment newSegment) {
        this.segment = newSegment;
    }

    /** The underlying raw segment. For advanced use only. */
    public MemorySegment segment() { return segment; }

    /** The layout describing this leaf's fields. */
    public LeafLayout layout() { return layout; }

    // -----------------------------------------------------------------------
    // Null bitmap — query and mutation
    // -----------------------------------------------------------------------

    /**
     * Check whether a nullable field is currently null.
     * Always returns {@code false} for non-nullable fields.
     */
    public boolean isNull(LeafHandle h) {
        int bit = h.nullBit();
        if (bit < 0) return false;
        return readNullBit(bit);
    }

    /**
     * Mark a nullable field as null. Zeroes the field's bytes and sets the null bit.
     *
     * @throws IllegalStateException if the field is not nullable
     */
    public LeafAccessor setNull(LeafHandle h) {
        int bit = h.nullBit();
        if (bit < 0) {
            throw new IllegalStateException("Field '" + h.name() + "' is not nullable");
        }
        setNullBit(bit);
        zeroField(h);
        return this;
    }

    private boolean readNullBit(int bit) {
        int byteIdx = layout.nullBitmapOffset() + (bit >>> 3);
        byte b = segment.get(ValueLayout.JAVA_BYTE, byteIdx);
        return ((b >>> (bit & 7)) & 1) != 0;
    }

    private void setNullBit(int bit) {
        int byteIdx = layout.nullBitmapOffset() + (bit >>> 3);
        byte b = segment.get(ValueLayout.JAVA_BYTE, byteIdx);
        segment.set(ValueLayout.JAVA_BYTE, byteIdx, (byte) (b | (1 << (bit & 7))));
    }

    private void clearNullBit(int bit) {
        int byteIdx = layout.nullBitmapOffset() + (bit >>> 3);
        byte b = segment.get(ValueLayout.JAVA_BYTE, byteIdx);
        segment.set(ValueLayout.JAVA_BYTE, byteIdx, (byte) (b & ~(1 << (bit & 7))));
    }

    private void autoClear(int nullBit) {
        if (nullBit >= 0) clearNullBit(nullBit);
    }

    private void autoClear(LeafHandle h) {
        autoClear(h.nullBit());
    }

    private void autoClearByIndex(int fieldIndex) {
        autoClear(layout.nullBitIndex(fieldIndex));
    }

    private void zeroField(LeafHandle h) {
        // Determine field width from offset and the handle type
        int width = fieldWidth(h);
        segment.asSlice(h.offset(), width).fill((byte) 0);
    }

    private static int fieldWidth(LeafHandle h) {
        return switch (h) {
            case LeafHandle.Int8 _    -> 1;
            case LeafHandle.Int16 _   -> 2;
            case LeafHandle.Int32 _   -> 4;
            case LeafHandle.Int64 _   -> 8;
            case LeafHandle.Float32 _ -> 4;
            case LeafHandle.Float64 _ -> 8;
            case LeafHandle.Str _     -> TaoString.SIZE;
            case LeafHandle.Json _    -> TaoString.SIZE;
            case LeafHandle.Extras _  -> TaoString.SIZE;
            case LeafHandle.Dict16 _  -> 2;
            case LeafHandle.Dict32 _  -> 4;
        };
    }

    // -----------------------------------------------------------------------
    // Getters — by index
    // -----------------------------------------------------------------------

    public byte getInt8(int fieldIndex) {
        return segment.get(ValueLayout.JAVA_BYTE, layout.offset(fieldIndex));
    }

    public short getInt16(int fieldIndex) {
        return segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, layout.offset(fieldIndex));
    }

    public int getInt32(int fieldIndex) {
        return segment.get(ValueLayout.JAVA_INT_UNALIGNED, layout.offset(fieldIndex));
    }

    public long getInt64(int fieldIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, layout.offset(fieldIndex));
    }

    public float getFloat32(int fieldIndex) {
        return segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, layout.offset(fieldIndex));
    }

    public double getFloat64(int fieldIndex) {
        return segment.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, layout.offset(fieldIndex));
    }

    /**
     * Read a {@link TaoString} field.
     */
    public String getString(int fieldIndex) {
        return TaoString.read(stringSlice(fieldIndex), tree);
    }

    /**
     * Read a JSON field (stored as {@link TaoString}).
     */
    public String getJson(int fieldIndex) {
        return TaoString.read(stringSlice(fieldIndex), tree);
    }

    /**
     * Read a dict16 field as the raw integer code.
     */
    public int getDict16Code(int fieldIndex) {
        return Short.toUnsignedInt(
            segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, layout.offset(fieldIndex)));
    }

    /**
     * Read a dict32 field as the raw integer code.
     */
    public int getDict32Code(int fieldIndex) {
        return segment.get(ValueLayout.JAVA_INT_UNALIGNED, layout.offset(fieldIndex));
    }

    // -----------------------------------------------------------------------
    // Getters — by name
    // -----------------------------------------------------------------------

    public byte getInt8(String name)      { return getInt8(layout.fieldIndex(name)); }
    public short getInt16(String name)    { return getInt16(layout.fieldIndex(name)); }
    public int getInt32(String name)      { return getInt32(layout.fieldIndex(name)); }
    public long getInt64(String name)     { return getInt64(layout.fieldIndex(name)); }
    public float getFloat32(String name)  { return getFloat32(layout.fieldIndex(name)); }
    public double getFloat64(String name) { return getFloat64(layout.fieldIndex(name)); }
    public String getString(String name)  { return getString(layout.fieldIndex(name)); }
    public String getJson(String name)    { return getJson(layout.fieldIndex(name)); }
    public int getDict16Code(String name) { return getDict16Code(layout.fieldIndex(name)); }
    public int getDict32Code(String name) { return getDict32Code(layout.fieldIndex(name)); }

    // -----------------------------------------------------------------------
    // Setters — by index (throw on read-only segments)
    // -----------------------------------------------------------------------

    public LeafAccessor setInt8(int fieldIndex, byte value) {
        autoClearByIndex(fieldIndex);
        segment.set(ValueLayout.JAVA_BYTE, layout.offset(fieldIndex), value);
        return this;
    }

    public LeafAccessor setInt16(int fieldIndex, short value) {
        autoClearByIndex(fieldIndex);
        segment.set(ValueLayout.JAVA_SHORT_UNALIGNED, layout.offset(fieldIndex), value);
        return this;
    }

    public LeafAccessor setInt32(int fieldIndex, int value) {
        autoClearByIndex(fieldIndex);
        segment.set(ValueLayout.JAVA_INT_UNALIGNED, layout.offset(fieldIndex), value);
        return this;
    }

    public LeafAccessor setInt64(int fieldIndex, long value) {
        autoClearByIndex(fieldIndex);
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, layout.offset(fieldIndex), value);
        return this;
    }

    public LeafAccessor setFloat32(int fieldIndex, float value) {
        autoClearByIndex(fieldIndex);
        segment.set(ValueLayout.JAVA_FLOAT_UNALIGNED, layout.offset(fieldIndex), value);
        return this;
    }

    public LeafAccessor setFloat64(int fieldIndex, double value) {
        autoClearByIndex(fieldIndex);
        segment.set(ValueLayout.JAVA_DOUBLE_UNALIGNED, layout.offset(fieldIndex), value);
        return this;
    }

    /**
     * Write a {@link TaoString} field.
     */
    public LeafAccessor setString(int fieldIndex, String value) {
        autoClearByIndex(fieldIndex);
        TaoString.write(stringSlice(fieldIndex), value, tree);
        return this;
    }

    /**
     * Write a JSON field (stored as {@link TaoString}).
     */
    public LeafAccessor setJson(int fieldIndex, String json) {
        autoClearByIndex(fieldIndex);
        TaoString.write(stringSlice(fieldIndex), json, tree);
        return this;
    }

    /**
     * Write a dict16 field. Interns the string through the field's dictionary.
     *
     * @throws IllegalArgumentException if the field is not a DictU16
     */
    public LeafAccessor setDict16(int fieldIndex, String value) {
        var field = LeafField.unwrap(layout.field(fieldIndex));
        if (!(field instanceof LeafField.DictU16 d)) {
            throw new IllegalArgumentException(
                "Field " + fieldIndex + " (" + field.name() + ") is not a dict16 field");
        }
        autoClearByIndex(fieldIndex);
        int code = (value != null) ? d.dict().intern(value) : 0;
        segment.set(ValueLayout.JAVA_SHORT_UNALIGNED, layout.offset(fieldIndex), (short) code);
        return this;
    }

    /**
     * Write a dict32 field. Interns the string through the field's dictionary.
     *
     * @throws IllegalArgumentException if the field is not a DictU32
     */
    public LeafAccessor setDict32(int fieldIndex, String value) {
        var field = LeafField.unwrap(layout.field(fieldIndex));
        if (!(field instanceof LeafField.DictU32 d)) {
            throw new IllegalArgumentException(
                "Field " + fieldIndex + " (" + field.name() + ") is not a dict32 field");
        }
        autoClearByIndex(fieldIndex);
        int code = (value != null) ? d.dict().intern(value) : 0;
        segment.set(ValueLayout.JAVA_INT_UNALIGNED, layout.offset(fieldIndex), code);
        return this;
    }

    // -----------------------------------------------------------------------
    // Setters — by name
    // -----------------------------------------------------------------------

    public LeafAccessor setInt8(String name, byte value)      { return setInt8(layout.fieldIndex(name), value); }
    public LeafAccessor setInt16(String name, short value)    { return setInt16(layout.fieldIndex(name), value); }
    public LeafAccessor setInt32(String name, int value)      { return setInt32(layout.fieldIndex(name), value); }
    public LeafAccessor setInt64(String name, long value)     { return setInt64(layout.fieldIndex(name), value); }
    public LeafAccessor setFloat32(String name, float value)  { return setFloat32(layout.fieldIndex(name), value); }
    public LeafAccessor setFloat64(String name, double value) { return setFloat64(layout.fieldIndex(name), value); }
    public LeafAccessor setString(String name, String value)  { return setString(layout.fieldIndex(name), value); }
    public LeafAccessor setJson(String name, String json)     { return setJson(layout.fieldIndex(name), json); }
    public LeafAccessor setDict16(String name, String value)  { return setDict16(layout.fieldIndex(name), value); }
    public LeafAccessor setDict32(String name, String value)  { return setDict32(layout.fieldIndex(name), value); }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private MemorySegment stringSlice(int fieldIndex) {
        return segment.asSlice(layout.offset(fieldIndex), TaoString.SIZE);
    }

    private MemorySegment stringSliceAt(int offset) {
        return segment.asSlice(offset, TaoString.SIZE);
    }

    // -----------------------------------------------------------------------
    // Handle-based getters — type-safe, zero-lookup
    // -----------------------------------------------------------------------

    public byte   get(LeafHandle.Int8 h)    { return segment.get(ValueLayout.JAVA_BYTE, h.offset()); }
    public short  get(LeafHandle.Int16 h)   { return segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, h.offset()); }
    public int    get(LeafHandle.Int32 h)    { return segment.get(ValueLayout.JAVA_INT_UNALIGNED, h.offset()); }
    public long   get(LeafHandle.Int64 h)    { return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, h.offset()); }
    public float  get(LeafHandle.Float32 h)  { return segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, h.offset()); }
    public double get(LeafHandle.Float64 h)  { return segment.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, h.offset()); }
    public String get(LeafHandle.Str h)      { return TaoString.read(stringSliceAt(h.offset()), tree); }
    public String get(LeafHandle.Json h)     { return TaoString.read(stringSliceAt(h.offset()), tree); }
    public int    get(LeafHandle.Dict16 h)   { return Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, h.offset())); }
    public int    get(LeafHandle.Dict32 h)   { return segment.get(ValueLayout.JAVA_INT_UNALIGNED, h.offset()); }

    /** Read the raw JSON string from an extras field. */
    public String get(LeafHandle.Extras h)   { return TaoString.read(stringSliceAt(h.offset()), tree); }

    // -----------------------------------------------------------------------
    // Handle-based setters — type-safe, zero-lookup, chainable
    // -----------------------------------------------------------------------

    public LeafAccessor set(LeafHandle.Int8 h, byte value) {
        autoClear(h);
        segment.set(ValueLayout.JAVA_BYTE, h.offset(), value);
        return this;
    }
    public LeafAccessor set(LeafHandle.Int16 h, short value) {
        autoClear(h);
        segment.set(ValueLayout.JAVA_SHORT_UNALIGNED, h.offset(), value);
        return this;
    }
    public LeafAccessor set(LeafHandle.Int32 h, int value) {
        autoClear(h);
        segment.set(ValueLayout.JAVA_INT_UNALIGNED, h.offset(), value);
        return this;
    }
    public LeafAccessor set(LeafHandle.Int64 h, long value) {
        autoClear(h);
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, h.offset(), value);
        return this;
    }
    public LeafAccessor set(LeafHandle.Float32 h, float value) {
        autoClear(h);
        segment.set(ValueLayout.JAVA_FLOAT_UNALIGNED, h.offset(), value);
        return this;
    }
    public LeafAccessor set(LeafHandle.Float64 h, double value) {
        autoClear(h);
        segment.set(ValueLayout.JAVA_DOUBLE_UNALIGNED, h.offset(), value);
        return this;
    }
    public LeafAccessor set(LeafHandle.Str h, String value) {
        autoClear(h);
        TaoString.write(stringSliceAt(h.offset()), value, tree);
        return this;
    }
    public LeafAccessor set(LeafHandle.Json h, String value) {
        autoClear(h);
        TaoString.write(stringSliceAt(h.offset()), value, tree);
        return this;
    }
    public LeafAccessor set(LeafHandle.Dict16 h, String value) {
        autoClear(h);
        int code = (value != null) ? h.dict().intern(value) : 0;
        segment.set(ValueLayout.JAVA_SHORT_UNALIGNED, h.offset(), (short) code);
        return this;
    }
    public LeafAccessor set(LeafHandle.Dict32 h, String value) {
        autoClear(h);
        int code = (value != null) ? h.dict().intern(value) : 0;
        segment.set(ValueLayout.JAVA_INT_UNALIGNED, h.offset(), code);
        return this;
    }

    /** Write a raw JSON string to an extras field. */
    public LeafAccessor set(LeafHandle.Extras h, String value) {
        autoClear(h);
        TaoString.write(stringSliceAt(h.offset()), value, tree);
        return this;
    }

    // -----------------------------------------------------------------------
    // Extras — structured access via ExtrasWriter / ExtrasReader
    // -----------------------------------------------------------------------

    /**
     * Create an {@link ExtrasWriter} to batch-write key-value pairs to the extras field.
     * Call {@link ExtrasWriter#write()} to serialize and flush.
     */
    public ExtrasWriter extrasWriter(LeafHandle.Extras h) {
        return new ExtrasWriter(stringSliceAt(h.offset()), tree, h.nullBit() >= 0 ? h.nullBit() : -1, this);
    }

    /**
     * Create an {@link ExtrasReader} to read key-value pairs from the extras field.
     */
    public ExtrasReader extrasReader(LeafHandle.Extras h) {
        String raw = TaoString.read(stringSliceAt(h.offset()), tree);
        return new ExtrasReader(raw);
    }
}
