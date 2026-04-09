package org.taotree.layout;

import org.taotree.LeafAccessor;
import org.taotree.TaoString;
import org.taotree.TaoTree;
/**
 * Defines the structure of a leaf value as an ordered list of {@link LeafField}s.
 *
 * <p>Pre-computes byte offsets for each field at construction time.
 * Used by {@link LeafAccessor} to provide typed, named access to leaf values.
 *
 * <p>If any field is declared {@link LeafField#nullable()}, a compact null bitmap
 * (⌈K/8⌉ bytes, where K = number of nullable fields) is appended after the fixed
 * fields. Schemas with no nullable fields have zero overhead.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * var layout = LeafLayout.of(
 *     LeafField.int32("count"),
 *     LeafField.float64("lat").nullable(),
 *     LeafField.string("description"),
 *     LeafField.extras("extras")
 * );
 * int totalBytes = layout.totalWidth(); // fields + bitmap
 * }</pre>
 */
public final class LeafLayout {

    private final LeafField[] fields;
    private final int[] offsets;
    private final int[] nullBitIndices;
    private final int nullBitmapOffset;
    private final int nullBitmapSize;
    private final int totalWidth;

    private LeafLayout(LeafField[] fields) {
        this.fields = fields.clone();
        this.offsets = new int[fields.length];
        this.nullBitIndices = new int[fields.length];

        int off = 0;
        int nullCount = 0;
        int extrasCount = 0;
        for (int i = 0; i < fields.length; i++) {
            offsets[i] = off;
            off += fields[i].width();
            if (fields[i] instanceof LeafField.Nullable) {
                nullBitIndices[i] = nullCount++;
            } else {
                nullBitIndices[i] = -1;
            }
            if (LeafField.unwrap(fields[i]) instanceof LeafField.Extras) {
                extrasCount++;
            }
        }
        if (extrasCount > 1) {
            throw new IllegalArgumentException("At most one extras field per layout (found " + extrasCount + ")");
        }
        this.nullBitmapOffset = off;
        this.nullBitmapSize = (nullCount + 7) / 8;
        this.totalWidth = off + nullBitmapSize;
    }

    /**
     * Create a layout from the given fields.
     *
     * @throws IllegalArgumentException if no fields are provided, or more than one extras field
     */
    public static LeafLayout of(LeafField... fields) {
        if (fields.length == 0) throw new IllegalArgumentException("At least one field required");
        return new LeafLayout(fields);
    }

    /** Total leaf value width in bytes (fixed fields + null bitmap). */
    public int totalWidth() { return totalWidth; }

    /** Number of fields. */
    public int fieldCount() { return fields.length; }

    /** Get the field at the given index. */
    public LeafField field(int index) { return fields[index]; }

    /** Get the byte offset of the field at the given index. */
    public int offset(int index) { return offsets[index]; }

    /** Byte offset of the null bitmap within the leaf (after all fixed fields). */
    public int nullBitmapOffset() { return nullBitmapOffset; }

    /** Size of the null bitmap in bytes (0 if no nullable fields). */
    public int nullBitmapSize() { return nullBitmapSize; }

    /**
     * Null-bit index for the field at the given position.
     *
     * @return bit index (≥ 0) for nullable fields, or -1 for non-nullable fields
     */
    public int nullBitIndex(int fieldIndex) { return nullBitIndices[fieldIndex]; }

    /**
     * Look up a field index by name.
     *
     * @throws IllegalArgumentException if no field has the given name
     */
    public int fieldIndex(String name) {
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].name().equals(name)) return i;
        }
        throw new IllegalArgumentException("No field named '" + name + "' in this leaf layout");
    }

    // -----------------------------------------------------------------------
    // Handle factories — derive typed, pre-computed handles from this layout
    // -----------------------------------------------------------------------

    /** Derive an Int8 handle for the named field. */
    public LeafHandle.Int8 int8(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Int8.class, name);
        return new LeafHandle.Int8(name, offsets[i], nullBitIndices[i]);
    }

    /** Derive an Int16 handle for the named field. */
    public LeafHandle.Int16 int16(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Int16.class, name);
        return new LeafHandle.Int16(name, offsets[i], nullBitIndices[i]);
    }

    /** Derive an Int32 handle for the named field. */
    public LeafHandle.Int32 int32(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Int32.class, name);
        return new LeafHandle.Int32(name, offsets[i], nullBitIndices[i]);
    }

    /** Derive an Int64 handle for the named field. */
    public LeafHandle.Int64 int64(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Int64.class, name);
        return new LeafHandle.Int64(name, offsets[i], nullBitIndices[i]);
    }

    /** Derive a Float32 handle for the named field. */
    public LeafHandle.Float32 float32(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Float32.class, name);
        return new LeafHandle.Float32(name, offsets[i], nullBitIndices[i]);
    }

    /** Derive a Float64 handle for the named field. */
    public LeafHandle.Float64 float64(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Float64.class, name);
        return new LeafHandle.Float64(name, offsets[i], nullBitIndices[i]);
    }

    /** Derive a String handle for the named field. */
    public LeafHandle.Str string(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Str.class, name);
        return new LeafHandle.Str(name, offsets[i], nullBitIndices[i]);
    }

    /** Derive a JSON handle for the named field. */
    public LeafHandle.Json json(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Json.class, name);
        return new LeafHandle.Json(name, offsets[i], nullBitIndices[i]);
    }

    /** Derive a Dict16 handle for the named field. */
    public LeafHandle.Dict16 dict16(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.DictU16.class, name);
        var f = (LeafField.DictU16) LeafField.unwrap(fields[i]);
        return new LeafHandle.Dict16(name, offsets[i], nullBitIndices[i], f.dict());
    }

    /** Derive a Dict32 handle for the named field. */
    public LeafHandle.Dict32 dict32(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.DictU32.class, name);
        var f = (LeafField.DictU32) LeafField.unwrap(fields[i]);
        return new LeafHandle.Dict32(name, offsets[i], nullBitIndices[i], f.dict());
    }

    /** Derive an Extras handle for the named field. */
    public LeafHandle.Extras extras(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Extras.class, name);
        return new LeafHandle.Extras(name, offsets[i], nullBitIndices[i]);
    }

    private void checkFieldType(int index, Class<?> expected, String name) {
        var f = LeafField.unwrap(fields[index]);
        if (!expected.isInstance(f)) {
            throw new IllegalArgumentException(
                "Field '" + name + "' is " + f.getClass().getSimpleName()
                + ", not " + expected.getSimpleName());
        }
    }

    /**
     * Returns true if this layout contains any string, JSON, or extras fields
     * that use overflow storage ({@link TaoString}).
     */
    public boolean hasStringFields() {
        for (var f : fields) {
            var inner = LeafField.unwrap(f);
            if (inner instanceof LeafField.Str || inner instanceof LeafField.Json
                    || inner instanceof LeafField.Extras) return true;
        }
        return false;
    }

    /**
     * Build an array of {@link TaoString.Layout} descriptors for {@link TaoTree#copyFrom}.
     * Returns one entry per string/json/extras field, or an empty array if none.
     */
    public TaoString.Layout[] stringLayouts() {
        int count = 0;
        for (var f : fields) {
            var inner = LeafField.unwrap(f);
            if (inner instanceof LeafField.Str || inner instanceof LeafField.Json
                    || inner instanceof LeafField.Extras) count++;
        }
        if (count == 0) return new TaoString.Layout[0];

        var result = new TaoString.Layout[count];
        int idx = 0;
        for (int i = 0; i < fields.length; i++) {
            var inner = LeafField.unwrap(fields[i]);
            if (inner instanceof LeafField.Str || inner instanceof LeafField.Json
                    || inner instanceof LeafField.Extras) {
                // TaoString layout: [len:4B @ offset][prefix:4B][overflowPtr:8B @ offset+8]
                result[idx++] = new TaoString.Layout(offsets[i], offsets[i] + 8, TaoString.SHORT_THRESHOLD);
            }
        }
        return result;
    }
}
