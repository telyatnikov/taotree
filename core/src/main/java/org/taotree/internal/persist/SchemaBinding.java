package org.taotree.internal.persist;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;

/**
 * Serialization of a {@link KeyLayout} fingerprint into the
 * {@code SECTION_SCHEMA_BINDING} payload of a v3 checkpoint.
 *
 * <p>Purpose: fail-fast on reopen when the caller-supplied {@code KeyLayout}
 * does not match the one that was used to create the store. Before this was
 * added, {@code TaoTree.open(Path, KeyLayout)} silently accepted any layout
 * whose total byte width matched, which could result in corrupt reads with no
 * surface error.
 *
 * <h3>On-disk layout (little-endian, variable width):</h3>
 * <pre>
 * Offset  Size   Field
 * ──────  ────   ─────
 * 0       1      version (currently 1)
 * 1       1      reserved (0)
 * 2       2      fieldCount (uint16)
 * 4       2      totalWidth (uint16, sanity sum of field widths)
 * 6       2      reserved (0)
 * 8       ...    per field:
 *                   1B kind     (see {@link #KIND_UINT8} … {@link #KIND_DICT32})
 *                   1B width
 *                   2B nameLen (uint16)
 *                   nameLen bytes   UTF-8 name
 *                   pad to 4-byte alignment (zero)
 * </pre>
 *
 * <p>Two fingerprints are considered compatible iff their serialized bytes
 * are byte-for-byte equal. Dict instances held by {@link KeyField.DictU16} /
 * {@link KeyField.DictU32} are intentionally ignored — only the declared
 * kind + name + width identify a field.
 */
public final class SchemaBinding {

    private SchemaBinding() {}

    public static final byte VERSION = 1;

    // Kind codes. These are persisted and must never be renumbered.
    public static final byte KIND_UINT8   = 0;
    public static final byte KIND_UINT16  = 1;
    public static final byte KIND_UINT32  = 2;
    public static final byte KIND_UINT64  = 3;
    public static final byte KIND_INT64   = 4;
    public static final byte KIND_DICT16  = 5;
    public static final byte KIND_DICT32  = 6;

    private static final int HEADER_SIZE = 8;

    /**
     * Serialize a {@link KeyLayout} into its on-disk fingerprint.
     */
    public static byte[] serialize(KeyLayout layout) {
        // Pre-compute total size: header + per-field (4B fixed + name + pad).
        int size = HEADER_SIZE;
        for (int i = 0; i < layout.fieldCount(); i++) {
            byte[] name = layout.field(i).name().getBytes(StandardCharsets.UTF_8);
            int rec = 4 + name.length;
            rec = (rec + 3) & ~3; // pad to 4
            size += rec;
        }
        var buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(VERSION);
        buf.put((byte) 0); // reserved
        buf.putShort((short) layout.fieldCount());
        buf.putShort((short) layout.totalWidth());
        buf.putShort((short) 0); // reserved
        for (int i = 0; i < layout.fieldCount(); i++) {
            var f = layout.field(i);
            byte[] name = f.name().getBytes(StandardCharsets.UTF_8);
            if (name.length > 0xFFFF) {
                throw new IllegalArgumentException("Key field name too long: " + f.name());
            }
            buf.put(kindOf(f));
            buf.put((byte) f.width());
            buf.putShort((short) name.length);
            buf.put(name);
            int pad = ((4 + name.length + 3) & ~3) - (4 + name.length);
            for (int j = 0; j < pad; j++) buf.put((byte) 0);
        }
        if (buf.remaining() != 0) {
            throw new IllegalStateException("SchemaBinding serialize size mismatch");
        }
        return buf.array();
    }

    /**
     * Deserialize a fingerprint into a {@link KeyLayout}. Dict fields are
     * created without dict instances ({@code dict == null}); the caller
     * must rebind them to the tree's dictionaries.
     *
     * @throws IllegalArgumentException if the payload is malformed
     */
    public static KeyLayout deserialize(byte[] payload) {
        if (payload == null || payload.length < HEADER_SIZE) {
            throw new IllegalArgumentException("SchemaBinding payload too short");
        }
        var buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        byte version = buf.get();
        if (version != VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported SchemaBinding version " + (version & 0xFF)
                    + " (this build understands " + (VERSION & 0xFF) + ")");
        }
        buf.get(); // reserved
        int fieldCount = Short.toUnsignedInt(buf.getShort());
        int totalWidth = Short.toUnsignedInt(buf.getShort());
        buf.getShort(); // reserved
        if (fieldCount == 0) {
            throw new IllegalArgumentException("SchemaBinding has zero fields");
        }
        var fields = new KeyField[fieldCount];
        int widthSum = 0;
        for (int i = 0; i < fieldCount; i++) {
            byte kind = buf.get();
            int width = buf.get() & 0xFF;
            int nameLen = Short.toUnsignedInt(buf.getShort());
            if (nameLen > buf.remaining()) {
                throw new IllegalArgumentException(
                        "SchemaBinding name length " + nameLen + " exceeds payload");
            }
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            int pad = ((4 + nameLen + 3) & ~3) - (4 + nameLen);
            for (int j = 0; j < pad; j++) buf.get();
            fields[i] = fieldOf(kind, name, width);
            widthSum += width;
        }
        if (widthSum != totalWidth) {
            throw new IllegalArgumentException(
                    "SchemaBinding totalWidth=" + totalWidth
                    + " does not match field sum=" + widthSum);
        }
        return KeyLayout.of(fields);
    }

    /**
     * Compare two fingerprint byte arrays. Returns a human-readable mismatch
     * description or {@code null} if equal.
     */
    public static String firstMismatch(byte[] stored, byte[] expected) {
        if (stored == expected) return null;
        if (stored == null) return "no stored schema binding on file";
        if (expected == null) return "no expected schema binding";
        if (stored.length != expected.length) {
            return "schema binding length differs: stored=" + stored.length
                    + " expected=" + expected.length;
        }
        for (int i = 0; i < stored.length; i++) {
            if (stored[i] != expected[i]) {
                return "schema binding byte differs at offset " + i
                        + " (stored=" + (stored[i] & 0xFF)
                        + " expected=" + (expected[i] & 0xFF) + ")";
            }
        }
        return null;
    }

    private static byte kindOf(KeyField f) {
        return switch (f) {
            case KeyField.UInt8 u -> KIND_UINT8;
            case KeyField.UInt16 u -> KIND_UINT16;
            case KeyField.UInt32 u -> KIND_UINT32;
            case KeyField.UInt64 u -> KIND_UINT64;
            case KeyField.Int64 u -> KIND_INT64;
            case KeyField.DictU16 u -> KIND_DICT16;
            case KeyField.DictU32 u -> KIND_DICT32;
        };
    }

    private static KeyField fieldOf(byte kind, String name, int width) {
        return switch (kind) {
            case KIND_UINT8  -> expectWidth(KeyField.uint8(name),  1, width, name);
            case KIND_UINT16 -> expectWidth(KeyField.uint16(name), 2, width, name);
            case KIND_UINT32 -> expectWidth(KeyField.uint32(name), 4, width, name);
            case KIND_UINT64 -> expectWidth(KeyField.uint64(name), 8, width, name);
            case KIND_INT64  -> expectWidth(KeyField.int64(name),  8, width, name);
            case KIND_DICT16 -> expectWidth(KeyField.dict16(name), 2, width, name);
            case KIND_DICT32 -> expectWidth(KeyField.dict32(name), 4, width, name);
            default -> throw new IllegalArgumentException(
                    "Unknown SchemaBinding kind code " + (kind & 0xFF) + " for field " + name);
        };
    }

    private static KeyField expectWidth(KeyField f, int expected, int got, String name) {
        if (got != expected) {
            throw new IllegalArgumentException(
                    "SchemaBinding width mismatch for " + name
                    + ": stored=" + got + " expected=" + expected);
        }
        return f;
    }
}
