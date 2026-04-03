package org.taotree;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Builds compound keys as binary-comparable byte sequences for {@link TaoTree}.
 *
 * <p>Pre-allocates a reusable key buffer. Call the setter methods for each field,
 * then use {@link #key()} and {@link #keyLen()} to get the encoded key.
 *
 * <p>Fields can be set by index (fastest) or by name (most readable):
 * <pre>{@code
 * var keyBuilder = new KeyBuilder(layout, arena);
 * keyBuilder.setDict("kingdom", "Animalia")
 *           .setDict("species", "Haliaeetus leucocephalus")
 *           .setU32("year", 2024);
 * }</pre>
 *
 * <p>Not thread-safe. Use one builder per thread (or per call site).
 */
public final class KeyBuilder {

    private final KeyLayout layout;
    private final MemorySegment buf;

    /**
     * Create a builder for the given layout.
     *
     * @param layout the key layout
     * @param arena  arena for allocating the reusable key buffer
     */
    public KeyBuilder(KeyLayout layout, Arena arena) {
        this.layout = layout;
        this.buf = arena.allocate(layout.totalWidth(), 1);
    }

    /** Set a U8 field. */
    public KeyBuilder setU8(int fieldIndex, byte value) {
        TaoKey.encodeU8(buf, layout.offset(fieldIndex), value);
        return this;
    }

    /** Set a U16 field. */
    public KeyBuilder setU16(int fieldIndex, short value) {
        TaoKey.encodeU16(buf, layout.offset(fieldIndex), value);
        return this;
    }

    /** Set a U32 field. */
    public KeyBuilder setU32(int fieldIndex, int value) {
        TaoKey.encodeU32(buf, layout.offset(fieldIndex), value);
        return this;
    }

    /** Set a U64 field. */
    public KeyBuilder setU64(int fieldIndex, long value) {
        TaoKey.encodeU64(buf, layout.offset(fieldIndex), value);
        return this;
    }

    /** Set an I64 (signed) field. */
    public KeyBuilder setI64(int fieldIndex, long value) {
        TaoKey.encodeI64(buf, layout.offset(fieldIndex), value);
        return this;
    }

    /**
     * Set a dictionary-backed field by index. Interns the string value through the field's
     * {@link TaoDictionary} (acquires a write lock internally), then encodes the resulting code.
     */
    public KeyBuilder setDict(int fieldIndex, String value) {
        KeyField field = layout.field(fieldIndex);
        return switch (field) {
            case KeyField.DictU16 d -> {
                int code = (value != null) ? d.dict().intern(value) : 0;
                TaoKey.encodeU16(buf, layout.offset(fieldIndex), (short) code);
                yield this;
            }
            case KeyField.DictU32 d -> {
                int code = (value != null) ? d.dict().intern(value) : 0;
                TaoKey.encodeU32(buf, layout.offset(fieldIndex), code);
                yield this;
            }
            default -> throw new IllegalArgumentException(
                "Field " + fieldIndex + " (" + field.name() + ") is not a dictionary field");
        };
    }

    // -- Name-based setters (look up field index by name) --

    /** Set a U8 field by name. */
    public KeyBuilder setU8(String fieldName, byte value) {
        return setU8(layout.fieldIndex(fieldName), value);
    }

    /** Set a U16 field by name. */
    public KeyBuilder setU16(String fieldName, short value) {
        return setU16(layout.fieldIndex(fieldName), value);
    }

    /** Set a U32 field by name. */
    public KeyBuilder setU32(String fieldName, int value) {
        return setU32(layout.fieldIndex(fieldName), value);
    }

    /** Set a U64 field by name. */
    public KeyBuilder setU64(String fieldName, long value) {
        return setU64(layout.fieldIndex(fieldName), value);
    }

    /** Set an I64 (signed) field by name. */
    public KeyBuilder setI64(String fieldName, long value) {
        return setI64(layout.fieldIndex(fieldName), value);
    }

    /** Set a dictionary-backed field by name. */
    public KeyBuilder setDict(String fieldName, String value) {
        return setDict(layout.fieldIndex(fieldName), value);
    }

    /** The encoded key buffer. Valid after all fields have been set. */
    public MemorySegment key() { return buf; }

    /** The key length in bytes. */
    public int keyLen() { return layout.totalWidth(); }
}
