package org.taotree.layout;

/**
 * Defines the structure of a compound key as an ordered list of {@link KeyField}s.
 *
 * <p>Pre-computes byte offsets for each field at construction time. Used by
 * {@link KeyBuilder} to produce binary-comparable keys at zero per-field overhead.
 */
public final class KeyLayout {

    private final KeyField[] fields;
    private final int[] offsets;
    private final int totalWidth;

    private KeyLayout(KeyField[] fields) {
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
     */
    public static KeyLayout of(KeyField... fields) {
        if (fields.length == 0) throw new IllegalArgumentException("At least one field required");
        return new KeyLayout(fields);
    }

    /** Total key width in bytes (sum of all field widths). */
    public int totalWidth() { return totalWidth; }

    /** Number of fields. */
    public int fieldCount() { return fields.length; }

    /** Get the field at the given index. */
    public KeyField field(int index) { return fields[index]; }

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
        throw new IllegalArgumentException("No field named '" + name + "' in this layout");
    }
}
