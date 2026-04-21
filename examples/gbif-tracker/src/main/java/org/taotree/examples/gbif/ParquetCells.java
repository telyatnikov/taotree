package org.taotree.examples.gbif;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import org.taotree.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Convert a single Parquet cell (column value from a {@link RowReader}) into a
 * TaoTree {@link Value} and back. The conversion is designed to be
 * <em>lossless</em> modulo:
 *
 * <ul>
 *   <li>Parquet NULL returns {@code null} (caller omits the attribute);</li>
 *   <li>Parquet empty UTF-8 string returns {@code Value.ofString("")} — this
 *       preserves the null-vs-empty distinction when read back;</li>
 *   <li>Parquet repeated columns are encoded as a canonical JSON array and
 *       stored via {@code Value.ofJson}; element order is preserved.</li>
 * </ul>
 *
 * <p>All string-valued GBIF columns use {@link PhysicalType#BYTE_ARRAY} at the
 * Parquet level; we treat them as UTF-8.
 */
public final class ParquetCells {

    private ParquetCells() {}

    // ── Cell → Value (write path) ────────────────────────────────────────

    /**
     * Read one column for the current row and map it to a TaoTree
     * {@link Value}. Returns {@code null} iff the Parquet cell is NULL, in
     * which case the caller should not write the attribute at all.
     */
    public static Value read(RowReader rows, GbifSchema.Column col) {
        if (col.isRepeated()) {
            return readRepeated(rows, col);
        }
        if (rows.isNull(col.name())) return null;
        return switch (col.type()) {
            case INT32         -> Value.ofInt(rows.getInt(col.name()));
            case INT64         -> Value.ofLong(rows.getLong(col.name()));
            case FLOAT         -> Value.ofFloat32(rows.getFloat(col.name()));
            case DOUBLE        -> Value.ofFloat64(rows.getDouble(col.name()));
            case BOOLEAN       -> Value.ofBool(rows.getBoolean(col.name()));
            case BYTE_ARRAY,
                 FIXED_LEN_BYTE_ARRAY -> Value.ofString(rows.getString(col.name()));
            case INT96         -> Value.ofBytes(rows.getBinary(col.name()));
        };
    }

    private static Value readRepeated(RowReader rows, GbifSchema.Column col) {
        if (rows.isNull(col.name())) return null;
        var list = rows.getList(col.name());
        if (list == null) return null;
        // Preserve order, keep empty lists distinct from null.
        var items = new ArrayList<String>(list.size());
        for (int i = 0; i < list.size(); i++) {
            if (list.isNull(i)) {
                items.add(null);
            } else {
                Object v = list.get(i);
                items.add(v == null ? null : v.toString());
            }
        }
        return Value.ofJson(encodeJsonArray(items));
    }

    // ── Value → printable (reverse path) ─────────────────────────────────

    /**
     * Reconstruct a canonical Parquet-native Java representation for a column
     * given the {@link Value} read back from TaoTree. Returns {@code null} if
     * the Value is {@code null} or {@link Value.Null}. The returned type
     * matches what {@link RowReader#getValue(String)} would return:
     *
     * <ul>
     *   <li>INT32  → {@link Integer}</li>
     *   <li>INT64  → {@link Long}</li>
     *   <li>FLOAT  → {@link Float}</li>
     *   <li>DOUBLE → {@link Double}</li>
     *   <li>BOOLEAN → {@link Boolean}</li>
     *   <li>BYTE_ARRAY (scalar string) → {@link String}</li>
     *   <li>repeated → {@link List}&lt;String&gt;</li>
     * </ul>
     */
    public static Object reconstruct(Value v, GbifSchema.Column col) {
        if (v == null || v instanceof Value.Null) return null;
        if (col.isRepeated()) {
            if (!(v instanceof Value.Json j))
                throw new IllegalStateException("expected JSON list for " + col.name() + " got " + v);
            return decodeJsonArray(j.value());
        }
        return switch (col.type()) {
            case INT32   -> ((Value.Int32) v).value();
            case INT64   -> ((Value.Int64) v).value();
            case FLOAT   -> ((Value.Float32) v).value();
            case DOUBLE  -> ((Value.Float64) v).value();
            case BOOLEAN -> ((Value.Bool) v).value();
            case BYTE_ARRAY,
                 FIXED_LEN_BYTE_ARRAY -> ((Value.Str) v).value();
            case INT96   -> ((Value.Bytes) v).value();
        };
    }

    // ── Minimal JSON array codec (strings only, null-tolerant) ───────────

    /**
     * Encode a list of strings (possibly containing nulls) as a compact JSON
     * array. Chosen as minimal because the only GBIF repeated fields are
     * string lists. Intentionally independent of any JSON library so the
     * example has no extra runtime dependency.
     */
    static String encodeJsonArray(List<String> items) {
        var sb = new StringBuilder(16 + items.size() * 8);
        sb.append('[');
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(',');
            String s = items.get(i);
            if (s == null) { sb.append("null"); continue; }
            sb.append('"');
            for (int k = 0; k < s.length(); k++) {
                char ch = s.charAt(k);
                switch (ch) {
                    case '"'  -> sb.append("\\\"");
                    case '\\' -> sb.append("\\\\");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    default   -> {
                        if (ch < 0x20) sb.append(String.format("\\u%04x", (int) ch));
                        else sb.append(ch);
                    }
                }
            }
            sb.append('"');
        }
        sb.append(']');
        return sb.toString();
    }

    /** Inverse of {@link #encodeJsonArray(List)}. Accepts only arrays of string/null. */
    static List<String> decodeJsonArray(String json) {
        var out = new ArrayList<String>();
        int i = 0;
        int n = json.length();
        while (i < n && Character.isWhitespace(json.charAt(i))) i++;
        if (i >= n || json.charAt(i) != '[')
            throw new IllegalArgumentException("not a JSON array: " + json);
        i++;
        while (i < n) {
            while (i < n && Character.isWhitespace(json.charAt(i))) i++;
            if (i >= n) throw new IllegalArgumentException("unterminated array");
            char ch = json.charAt(i);
            if (ch == ']') return out;
            if (ch == 'n' && json.startsWith("null", i)) {
                out.add(null);
                i += 4;
            } else if (ch == '"') {
                var sb = new StringBuilder();
                i++;
                while (i < n) {
                    char c = json.charAt(i++);
                    if (c == '"') break;
                    if (c == '\\') {
                        if (i >= n) throw new IllegalArgumentException("bad escape");
                        char e = json.charAt(i++);
                        switch (e) {
                            case '"'  -> sb.append('"');
                            case '\\' -> sb.append('\\');
                            case 'n'  -> sb.append('\n');
                            case 'r'  -> sb.append('\r');
                            case 't'  -> sb.append('\t');
                            case 'u'  -> {
                                if (i + 4 > n) throw new IllegalArgumentException("bad unicode escape");
                                sb.append((char) Integer.parseInt(json.substring(i, i + 4), 16));
                                i += 4;
                            }
                            default -> throw new IllegalArgumentException("bad escape: \\" + e);
                        }
                    } else {
                        sb.append(c);
                    }
                }
                out.add(sb.toString());
            } else {
                throw new IllegalArgumentException("unexpected char in array: " + ch);
            }
            while (i < n && Character.isWhitespace(json.charAt(i))) i++;
            if (i < n && json.charAt(i) == ',') { i++; continue; }
            if (i < n && json.charAt(i) == ']') return out;
        }
        throw new IllegalArgumentException("unterminated array: " + json);
    }
}
