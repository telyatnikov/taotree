package org.taotree;

import java.util.Arrays;

/**
 * Tagged union representing a single attribute value in the unified temporal {@link TaoTree}.
 *
 * <p>Schemaless: each {@code put} carries the value's type with it. Variants:
 * <ul>
 *   <li>{@link Int32} — 4-byte signed int</li>
 *   <li>{@link Int64} — 8-byte signed long</li>
 *   <li>{@link Float32} — IEEE 754 binary32</li>
 *   <li>{@link Float64} — IEEE 754 binary64</li>
 *   <li>{@link Bool} — true/false</li>
 *   <li>{@link Str} — UTF-8 string</li>
 *   <li>{@link Json} — UTF-8 JSON document (semantic alias for string)</li>
 *   <li>{@link Bytes} — raw byte array</li>
 *   <li>{@link Null} — explicit absence</li>
 * </ul>
 *
 * <p>Storage is decided by {@link org.taotree.internal.value.ValueCodec}: payloads
 * up to 12 bytes live inline in a 16-byte slot; larger payloads are written to
 * the {@code BumpAllocator} and referenced by an 8-byte overflow pointer.
 */
public sealed interface Value {

    /** The runtime type tag of this value. */
    Type type();

    // ── Static factories ────────────────────────────────────────────────────

    static Value ofInt(int v)        { return new Int32(v); }
    static Value ofLong(long v)      { return new Int64(v); }
    static Value ofFloat32(float v)  { return new Float32(v); }
    static Value ofFloat64(double v) { return new Float64(v); }
    static Value ofBool(boolean v)   { return v ? Bool.TRUE : Bool.FALSE; }
    static Value ofString(String v)  { return v == null ? Null.INSTANCE : new Str(v); }
    static Value ofJson(String v)    { return v == null ? Null.INSTANCE : new Json(v); }
    static Value ofBytes(byte[] v)   { return v == null ? Null.INSTANCE : new Bytes(v.clone()); }
    static Value ofNull()            { return Null.INSTANCE; }

    // ── Typed accessors (throw IllegalStateException on mismatch) ───────────

    default int asInt() {
        if (this instanceof Int32 i) return i.value();
        throw mismatch("int");
    }
    default long asLong() {
        if (this instanceof Int64 i) return i.value();
        if (this instanceof Int32 i) return i.value();
        throw mismatch("long");
    }
    default float asFloat32() {
        if (this instanceof Float32 f) return f.value();
        throw mismatch("float");
    }
    default double asFloat64() {
        if (this instanceof Float64 f) return f.value();
        if (this instanceof Float32 f) return f.value();
        throw mismatch("double");
    }
    default boolean asBool() {
        if (this instanceof Bool b) return b.value();
        throw mismatch("bool");
    }
    default String asString() {
        if (this instanceof Str s) return s.value();
        if (this instanceof Json j) return j.value();
        throw mismatch("string");
    }
    default byte[] asBytes() {
        if (this instanceof Bytes b) return b.value().clone();
        throw mismatch("bytes");
    }
    default boolean isNull() {
        return this instanceof Null;
    }

    private IllegalStateException mismatch(String expected) {
        return new IllegalStateException(
            "Value type mismatch: expected " + expected + " but was " + type());
    }

    // ── Type tag enum ───────────────────────────────────────────────────────

    /**
     * Compact 1-byte tag for on-disk encoding by {@code ValueCodec}.
     * Codes are stable across versions — append-only, never reordered.
     */
    enum Type {
        NULL    (0),
        INT32   (1),
        INT64   (2),
        FLOAT32 (3),
        FLOAT64 (4),
        BOOL    (5),
        STRING  (6),
        JSON    (7),
        BYTES   (8);

        public final byte tag;
        Type(int tag) { this.tag = (byte) tag; }

        private static final Type[] BY_TAG;
        static {
            BY_TAG = new Type[16];
            for (Type t : values()) BY_TAG[t.tag & 0xFF] = t;
        }
        public static Type fromTag(byte tag) {
            int idx = tag & 0xFF;
            if (idx >= BY_TAG.length || BY_TAG[idx] == null) {
                throw new IllegalArgumentException("Unknown Value tag: " + idx);
            }
            return BY_TAG[idx];
        }
    }

    // ── Variant records ─────────────────────────────────────────────────────

    record Int32(int value) implements Value {
        public Type type() { return Type.INT32; }
    }
    record Int64(long value) implements Value {
        public Type type() { return Type.INT64; }
    }
    record Float32(float value) implements Value {
        public Type type() { return Type.FLOAT32; }
    }
    record Float64(double value) implements Value {
        public Type type() { return Type.FLOAT64; }
    }

    final class Bool implements Value {
        public static final Bool TRUE  = new Bool(true);
        public static final Bool FALSE = new Bool(false);
        private final boolean value;
        private Bool(boolean v) { this.value = v; }
        public boolean value() { return value; }
        public Type type() { return Type.BOOL; }
        @Override public boolean equals(Object o) { return o instanceof Bool b && b.value == value; }
        @Override public int hashCode() { return Boolean.hashCode(value); }
        @Override public String toString() { return "Bool[" + value + "]"; }
    }

    record Str(String value) implements Value {
        public Type type() { return Type.STRING; }
    }
    record Json(String value) implements Value {
        public Type type() { return Type.JSON; }
    }

    final class Bytes implements Value {
        private final byte[] value;
        Bytes(byte[] v) { this.value = v; }
        public byte[] value() { return value; }
        public Type type() { return Type.BYTES; }
        @Override public boolean equals(Object o) {
            return o instanceof Bytes b && Arrays.equals(b.value, value);
        }
        @Override public int hashCode() { return Arrays.hashCode(value); }
        @Override public String toString() { return "Bytes[len=" + value.length + "]"; }
    }

    final class Null implements Value {
        public static final Null INSTANCE = new Null();
        private Null() {}
        public Type type() { return Type.NULL; }
        @Override public String toString() { return "Null"; }
    }
}
