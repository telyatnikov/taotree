package org.taotree;

/**
 * Pre-computed, typed handle for encoding a key field.
 *
 * <p>Derived from a {@link TaoTree} via methods like {@link TaoTree#keyDict16(String)}.
 * Dict handles carry a reference to the tree's {@link TaoDictionary}, so
 * {@link KeyBuilder#set(KeyHandle.Dict16, String)} can intern the string automatically.
 *
 * <p>Each handle records the field's byte offset, width, and index within the key layout,
 * enabling prefix-length calculation for scan/query operations.
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * var KINGDOM = tree.keyDict16("kingdom");
 * var YEAR    = tree.keyUint32("year");
 *
 * kb.set(KINGDOM, "Animalia").set(YEAR, 2024);
 * }</pre>
 */
public sealed interface KeyHandle {
    /** The field name. */
    String name();
    /** The pre-computed byte offset within the key. */
    int offset();
    /** The field width in bytes. */
    int width();
    /** The field index within the key layout. */
    int fieldIndex();
    /** The byte position immediately after this field ({@code offset() + width()}). */
    default int end() { return offset() + width(); }

    record UInt8  (String name, int offset, int fieldIndex) implements KeyHandle { public int width() { return 1; } }
    record UInt16 (String name, int offset, int fieldIndex) implements KeyHandle { public int width() { return 2; } }
    record UInt32 (String name, int offset, int fieldIndex) implements KeyHandle { public int width() { return 4; } }
    record UInt64 (String name, int offset, int fieldIndex) implements KeyHandle { public int width() { return 8; } }
    record Int64  (String name, int offset, int fieldIndex) implements KeyHandle { public int width() { return 8; } }
    record Dict16 (String name, int offset, int fieldIndex, TaoDictionary dict) implements KeyHandle { public int width() { return 2; } }
    record Dict32 (String name, int offset, int fieldIndex, TaoDictionary dict) implements KeyHandle { public int width() { return 4; } }
}
