package org.taotree;

/**
 * Pre-computed, typed handle for encoding a key field.
 *
 * <p>Derived from a {@link TaoTree} via methods like {@link TaoTree#keyDict16(String)}.
 * Dict handles carry a reference to the tree's {@link TaoDictionary}, so
 * {@link KeyBuilder#set(KeyHandle.Dict16, String)} can intern the string automatically.
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

    record UInt8  (String name, int offset) implements KeyHandle {}
    record UInt16 (String name, int offset) implements KeyHandle {}
    record UInt32 (String name, int offset) implements KeyHandle {}
    record UInt64 (String name, int offset) implements KeyHandle {}
    record Int64  (String name, int offset) implements KeyHandle {}
    record Dict16 (String name, int offset, TaoDictionary dict) implements KeyHandle {}
    record Dict32 (String name, int offset, TaoDictionary dict) implements KeyHandle {}
}
