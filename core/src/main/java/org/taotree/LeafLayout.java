package org.taotree;

/**
 * Defines the structure of a leaf value as an ordered list of {@link LeafField}s.
 *
 * <p>Pre-computes byte offsets for each field at construction time.
 * Used by {@link LeafAccessor} to provide typed, named access to leaf values.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * var layout = LeafLayout.of(
 *     LeafField.int32("count"),
 *     LeafField.string("description"),
 *     LeafField.json("extras")
 * );
 * int totalBytes = layout.totalWidth(); // e.g. 4 + 16 + 16 = 36
 * }</pre>
 */
public final class LeafLayout {

    private final LeafField[] fields;
    private final int[] offsets;
    private final int totalWidth;

    private LeafLayout(LeafField[] fields) {
        this.fields = fields.clone();
        this.offsets = new int[fields.length];
        int off = 0;
        for (int i = 0; i < fields.length; i++) {
            offsets[i] = off;
            off += fields[i].width();
        }
        this.totalWidth = off;
    }

    /**
     * Create a layout from the given fields.
     *
     * @throws IllegalArgumentException if no fields are provided
     */
    public static LeafLayout of(LeafField... fields) {
        if (fields.length == 0) throw new IllegalArgumentException("At least one field required");
        return new LeafLayout(fields);
    }

    /** Total leaf value width in bytes (sum of all field widths). */
    public int totalWidth() { return totalWidth; }

    /** Number of fields. */
    public int fieldCount() { return fields.length; }

    /** Get the field at the given index. */
    public LeafField field(int index) { return fields[index]; }

    /** Get the byte offset of the field at the given index. */
    public int offset(int index) { return offsets[index]; }

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
        return new LeafHandle.Int8(name, offsets[i]);
    }

    /** Derive an Int16 handle for the named field. */
    public LeafHandle.Int16 int16(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Int16.class, name);
        return new LeafHandle.Int16(name, offsets[i]);
    }

    /** Derive an Int32 handle for the named field. */
    public LeafHandle.Int32 int32(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Int32.class, name);
        return new LeafHandle.Int32(name, offsets[i]);
    }

    /** Derive an Int64 handle for the named field. */
    public LeafHandle.Int64 int64(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Int64.class, name);
        return new LeafHandle.Int64(name, offsets[i]);
    }

    /** Derive a Float32 handle for the named field. */
    public LeafHandle.Float32 float32(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Float32.class, name);
        return new LeafHandle.Float32(name, offsets[i]);
    }

    /** Derive a Float64 handle for the named field. */
    public LeafHandle.Float64 float64(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Float64.class, name);
        return new LeafHandle.Float64(name, offsets[i]);
    }

    /** Derive a String handle for the named field. */
    public LeafHandle.Str string(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Str.class, name);
        return new LeafHandle.Str(name, offsets[i]);
    }

    /** Derive a JSON handle for the named field. */
    public LeafHandle.Json json(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.Json.class, name);
        return new LeafHandle.Json(name, offsets[i]);
    }

    /** Derive a Dict16 handle for the named field. */
    public LeafHandle.Dict16 dict16(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.DictU16.class, name);
        var f = (LeafField.DictU16) fields[i];
        return new LeafHandle.Dict16(name, offsets[i], f.dict());
    }

    /** Derive a Dict32 handle for the named field. */
    public LeafHandle.Dict32 dict32(String name) {
        int i = fieldIndex(name);
        checkFieldType(i, LeafField.DictU32.class, name);
        var f = (LeafField.DictU32) fields[i];
        return new LeafHandle.Dict32(name, offsets[i], f.dict());
    }

    private void checkFieldType(int index, Class<?> expected, String name) {
        if (!expected.isInstance(fields[index])) {
            throw new IllegalArgumentException(
                "Field '" + name + "' is " + fields[index].getClass().getSimpleName()
                + ", not " + expected.getSimpleName());
        }
    }

    /**
     * Returns true if this layout contains any string or JSON fields
     * that use overflow storage ({@link TaoString}).
     */
    public boolean hasStringFields() {
        for (var f : fields) {
            if (f instanceof LeafField.Str || f instanceof LeafField.Json) return true;
        }
        return false;
    }

    /**
     * Build an array of {@link StringLayout} descriptors for {@link TaoTree#copyFrom}.
     * Returns one entry per string/json field, or an empty array if none.
     */
    StringLayout[] stringLayouts() {
        int count = 0;
        for (var f : fields) {
            if (f instanceof LeafField.Str || f instanceof LeafField.Json) count++;
        }
        if (count == 0) return new StringLayout[0];

        var result = new StringLayout[count];
        int idx = 0;
        for (int i = 0; i < fields.length; i++) {
            if (fields[i] instanceof LeafField.Str || fields[i] instanceof LeafField.Json) {
                // TaoString layout: [len:4B @ offset][prefix:4B][overflowPtr:8B @ offset+8]
                result[idx++] = new StringLayout(offsets[i], offsets[i] + 8, TaoString.SHORT_THRESHOLD);
            }
        }
        return result;
    }
}
