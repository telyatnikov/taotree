package org.taotree;

/**
 * Describes a single field in a compound key layout.
 *
 * <p>Each field has a name, a fixed byte width, and an encoding strategy.
 * TaoDictionary-backed fields carry a reference to their {@link TaoDictionary}.
 *
 * <h3>Usage (static factories):</h3>
 * <pre>{@code
 * var layout = KeyLayout.of(
 *     KeyField.uint16("kingdom"),
 *     KeyField.dict16("species", speciesDict),
 *     KeyField.uint32("year")
 * );
 * }</pre>
 */
public sealed interface KeyField {
    String name();
    int width();

    // -- Static factories --

    static KeyField uint8(String name)  { return new UInt8(name); }
    static KeyField uint16(String name) { return new UInt16(name); }
    static KeyField uint32(String name) { return new UInt32(name); }
    static KeyField uint64(String name) { return new UInt64(name); }
    static KeyField int64(String name)  { return new Int64(name); }

    static KeyField dict16(String name, TaoDictionary dict) { return new DictU16(name, dict); }
    static KeyField dict32(String name, TaoDictionary dict) { return new DictU32(name, dict); }

    /** Declare a dict16 field without a dictionary instance. The tree creates it automatically. */
    static KeyField dict16(String name) { return new DictU16(name, null); }
    /** Declare a dict32 field without a dictionary instance. The tree creates it automatically. */
    static KeyField dict32(String name) { return new DictU32(name, null); }

    // -- Record implementations --

    record UInt8 (String name) implements KeyField { public int width() { return 1; } }
    record UInt16(String name) implements KeyField { public int width() { return 2; } }
    record UInt32(String name) implements KeyField { public int width() { return 4; } }
    record UInt64(String name) implements KeyField { public int width() { return 8; } }
    record Int64 (String name) implements KeyField { public int width() { return 8; } }

    record DictU16(String name, TaoDictionary dict) implements KeyField {
        public int width() { return 2; }
    }
    record DictU32(String name, TaoDictionary dict) implements KeyField {
        public int width() { return 4; }
    }
}
