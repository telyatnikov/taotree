package org.taotree;

import java.lang.foreign.MemorySegment;

/**
 * Batch writer for schemaless extras fields.
 *
 * <p>Collects key-value pairs and serializes them as a flat JSON object
 * into a {@link TaoString} slot when {@link #write()} is called.
 *
 * <p>Supports typed values: strings, ints, longs, doubles, and booleans.
 * Null values are omitted (not written to the JSON). Keys must be non-null.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * var eb = leaf.extrasWriter(EXTRAS);
 * eb.put("basisOfRecord", "HUMAN_OBSERVATION")
 *   .put("coordinateUncertainty", 50.0)
 *   .put("hasCoordinates", true)
 *   .write();
 * }</pre>
 *
 * <p>Calling {@link #write()} is required to persist the data. If no pairs
 * are added, {@code write()} writes an empty string (clearing the field).
 */
public final class ExtrasWriter {

    private final MemorySegment slot;
    private final TaoTree tree;
    private final int nullBit;
    private final LeafAccessor accessor;
    private final StringBuilder sb = new StringBuilder(64);
    private boolean first = true;

    ExtrasWriter(MemorySegment slot, TaoTree tree, int nullBit, LeafAccessor accessor) {
        this.slot = slot;
        this.tree = tree;
        this.nullBit = nullBit;
        this.accessor = accessor;
        sb.append('{');
    }

    /** Add a string key-value pair. If {@code value} is null, the key is skipped. */
    public ExtrasWriter put(String key, String value) {
        if (value == null) return this;
        appendKey(key);
        sb.append('"');
        escapeJson(value);
        sb.append('"');
        return this;
    }

    /** Add an int key-value pair. */
    public ExtrasWriter put(String key, int value) {
        appendKey(key);
        sb.append(value);
        return this;
    }

    /** Add a long key-value pair. */
    public ExtrasWriter put(String key, long value) {
        appendKey(key);
        sb.append(value);
        return this;
    }

    /** Add a double key-value pair. Writes NaN and Infinity as null. */
    public ExtrasWriter put(String key, double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) return this;
        appendKey(key);
        sb.append(value);
        return this;
    }

    /** Add a boolean key-value pair. */
    public ExtrasWriter put(String key, boolean value) {
        appendKey(key);
        sb.append(value);
        return this;
    }

    /**
     * Whether any key-value pairs have been added.
     */
    public boolean isEmpty() { return first; }

    /**
     * Serialize and write the collected pairs to the TaoString slot.
     * If no pairs were added, writes an empty string to clear the field.
     */
    public void write() {
        if (first) {
            // No entries — write empty (zero the TaoString slot)
            TaoString.write(slot, "", tree);
        } else {
            sb.append('}');
            TaoString.write(slot, sb.toString(), tree);
        }
    }

    private void appendKey(String key) {
        if (!first) sb.append(',');
        first = false;
        sb.append('"');
        escapeJson(key);
        sb.append("\":");
    }

    private void escapeJson(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') sb.append("\\\"");
            else if (c == '\\') sb.append("\\\\");
            else if (c < 0x20) sb.append(' ');
            else sb.append(c);
        }
    }
}
