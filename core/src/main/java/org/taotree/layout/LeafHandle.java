package org.taotree.layout;

import org.taotree.LeafAccessor;
import org.taotree.TaoDictionary;
/**
 * Pre-computed, typed handle for accessing a leaf value field.
 *
 * <p>Derived from a {@link LeafLayout} via methods like {@link LeafLayout#int32(String)}.
 * Each handle records the field's byte offset within the leaf value, enabling
 * zero-lookup, type-safe access through {@link LeafAccessor#set} / {@link LeafAccessor#get}.
 *
 * <p>Nullable fields carry a non-negative {@code nullBit} index into the leaf's null bitmap.
 * Non-nullable fields have {@code nullBit == -1}.
 *
 * <p>Mirrors the {@link java.lang.invoke.VarHandle} pattern: define a layout,
 * derive handles, use handles for access.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * var layout = LeafLayout.of(
 *     LeafField.int32("count"),
 *     LeafField.float64("lat").nullable(),
 *     LeafField.string("locality")
 * );
 * var COUNT    = layout.int32("count");
 * var LAT      = layout.float64("lat");
 * var LOCALITY = layout.string("locality");
 *
 * leaf.set(COUNT, 42);           // compile-time type: int
 * leaf.set(LAT, 48.5);           // auto-clears null bit
 * leaf.setNull(LAT);             // marks field as null
 * boolean absent = leaf.isNull(LAT);
 * }</pre>
 */
public sealed interface LeafHandle {
    /** The field name. */
    String name();
    /** The pre-computed byte offset within the leaf value. */
    int offset();
    /** Null-bitmap bit index, or -1 if this field is not nullable. */
    int nullBit();
    /** Whether this handle represents a nullable field. */
    default boolean isNullable() { return nullBit() >= 0; }

    record Int8    (String name, int offset, int nullBit) implements LeafHandle {}
    record Int16   (String name, int offset, int nullBit) implements LeafHandle {}
    record Int32   (String name, int offset, int nullBit) implements LeafHandle {}
    record Int64   (String name, int offset, int nullBit) implements LeafHandle {}
    record Float32 (String name, int offset, int nullBit) implements LeafHandle {}
    record Float64 (String name, int offset, int nullBit) implements LeafHandle {}
    record Str     (String name, int offset, int nullBit) implements LeafHandle {}
    record Json    (String name, int offset, int nullBit) implements LeafHandle {}
    record Dict16  (String name, int offset, int nullBit, TaoDictionary dict) implements LeafHandle {}
    record Dict32  (String name, int offset, int nullBit, TaoDictionary dict) implements LeafHandle {}
    record Extras  (String name, int offset, int nullBit) implements LeafHandle {}
}
