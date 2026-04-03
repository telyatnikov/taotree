package org.taotree;

/**
 * Describes the layout of a string header within a leaf value.
 *
 * <p>Strings use a dual representation inspired by
 * <a href="https://cedardb.com/blog/strings_deep_dive/">Umbra/CedarDB "German Strings"</a>:
 * short strings are stored <b>inline</b> in the fixed-size leaf header, while longer
 * strings store a prefix and a pointer to the <b>out-of-line</b> body in the bump allocator.
 *
 * <p>This descriptor tells {@link TaoTree#copyFrom} where the length field and
 * out-of-line pointer are located, so it can copy the string body from the source
 * tree's bump allocator to the target's.
 *
 * <p>Register via {@link TaoTree#registerStringLayout(int, StringLayout)}.
 *
 * @param lenOffset       byte offset of the 4-byte int length field within the leaf value
 * @param ptrOffset       byte offset of the 8-byte out-of-line pointer within the leaf value
 * @param inlineThreshold if length &le; this value, the string is inline (no out-of-line pointer)
 */
public record StringLayout(int lenOffset, int ptrOffset, int inlineThreshold) {
}
