package org.taotree.layout;

import org.taotree.TaoDictionary;
import org.taotree.TaoString;
/**
 * Describes a single field in a leaf value layout.
 *
 * <p>Each field has a name, a fixed byte width, and a storage strategy.
 * String and JSON fields use {@link TaoString} (16 bytes: inline ≤ 12 bytes,
 * overflow pointer for longer values). Dictionary-backed fields carry a
 * reference to their {@link TaoDictionary}.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * var layout = LeafLayout.of(
 *     LeafField.int32("count"),
 *     LeafField.int64("timestamp"),
 *     LeafField.string("description"),
 *     LeafField.dict16("status", statusDict),
 *     LeafField.float64("latitude"),
 *     LeafField.json("extras")
 * );
 * }</pre>
 */
public sealed interface LeafField {
    String name();
    int width();

    // -- Static factories --

    static LeafField int8(String name)    { return new Int8(name); }
    static LeafField int16(String name)   { return new Int16(name); }
    static LeafField int32(String name)   { return new Int32(name); }
    static LeafField int64(String name)   { return new Int64(name); }
    static LeafField float32(String name) { return new Float32(name); }
    static LeafField float64(String name) { return new Float64(name); }

    /** A {@link TaoString} field (16 bytes: inline ≤ 12 bytes, overflow for longer). */
    static LeafField string(String name) { return new Str(name); }

    /** A JSON field stored as a {@link TaoString}. Same layout as {@code string()}. */
    static LeafField json(String name) { return new Json(name); }

    /** A dictionary-encoded u16 field (2 bytes). */
    static LeafField dict16(String name, TaoDictionary dict) { return new DictU16(name, dict); }

    /** A dictionary-encoded u32 field (4 bytes). */
    static LeafField dict32(String name, TaoDictionary dict) { return new DictU32(name, dict); }

    // -- Record implementations --

    record Int8    (String name) implements LeafField { public int width() { return 1; } }
    record Int16   (String name) implements LeafField { public int width() { return 2; } }
    record Int32   (String name) implements LeafField { public int width() { return 4; } }
    record Int64   (String name) implements LeafField { public int width() { return 8; } }
    record Float32 (String name) implements LeafField { public int width() { return 4; } }
    record Float64 (String name) implements LeafField { public int width() { return 8; } }

    record Str  (String name) implements LeafField { public int width() { return TaoString.SIZE; } }
    record Json (String name) implements LeafField { public int width() { return TaoString.SIZE; } }

    record DictU16(String name, TaoDictionary dict) implements LeafField {
        public int width() { return 2; }
    }
    record DictU32(String name, TaoDictionary dict) implements LeafField {
        public int width() { return 4; }
    }
}
