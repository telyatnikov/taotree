package org.taotree;

/**
 * Typed reader for schemaless extras fields stored as flat JSON objects.
 *
 * <p>Lazily parses the JSON string on first access. All getters return
 * {@code null} for absent keys. The underlying JSON must be a flat object
 * (no nested objects or arrays).
 *
 * <h3>Usage:</h3>
 * <pre>{@code
 * var er = leaf.extrasReader(EXTRAS);
 * String basis = er.getString("basisOfRecord");    // "HUMAN_OBSERVATION" or null
 * Double uncert = er.getDouble("coordinateUncertainty"); // 50.0 or null
 * boolean has = er.has("license");
 * }</pre>
 */
public final class ExtrasReader {

    private final String raw;

    ExtrasReader(String raw) {
        this.raw = raw;
    }

    /** The raw JSON string (may be null or empty). */
    public String raw() { return raw; }

    /** Whether the extras contain any data. */
    public boolean isEmpty() {
        return raw == null || raw.isEmpty() || raw.equals("{}");
    }

    /** Whether the extras contain a value for the given key. */
    public boolean has(String key) {
        return findValue(key) != null;
    }

    /**
     * Get a string value, or {@code null} if absent.
     * Strips surrounding quotes and unescapes JSON escape sequences.
     */
    public String getString(String key) {
        String v = findValue(key);
        if (v == null) return null;
        if (v.startsWith("\"") && v.endsWith("\"")) {
            return unescapeJson(v.substring(1, v.length() - 1));
        }
        if ("null".equals(v)) return null;
        return v;
    }

    /** Get an integer value, or {@code null} if absent or not a valid integer. */
    public Integer getInt(String key) {
        String v = findValue(key);
        if (v == null || "null".equals(v)) return null;
        // Strip quotes if present
        if (v.startsWith("\"")) v = v.substring(1, v.length() - 1);
        try { return Integer.parseInt(v); }
        catch (NumberFormatException e) { return null; }
    }

    /** Get a long value, or {@code null} if absent or not a valid long. */
    public Long getLong(String key) {
        String v = findValue(key);
        if (v == null || "null".equals(v)) return null;
        if (v.startsWith("\"")) v = v.substring(1, v.length() - 1);
        try { return Long.parseLong(v); }
        catch (NumberFormatException e) { return null; }
    }

    /** Get a double value, or {@code null} if absent or not a valid number. */
    public Double getDouble(String key) {
        String v = findValue(key);
        if (v == null || "null".equals(v)) return null;
        if (v.startsWith("\"")) v = v.substring(1, v.length() - 1);
        try { return Double.parseDouble(v); }
        catch (NumberFormatException e) { return null; }
    }

    /** Get a boolean value, or {@code null} if absent or not a valid boolean. */
    public Boolean getBoolean(String key) {
        String v = findValue(key);
        if (v == null || "null".equals(v)) return null;
        if ("true".equals(v)) return Boolean.TRUE;
        if ("false".equals(v)) return Boolean.FALSE;
        return null;
    }

    // -----------------------------------------------------------------------
    // Minimal flat-JSON parser — finds the value for a given key
    // -----------------------------------------------------------------------

    /**
     * Find the raw JSON value string for the given key, or null if absent.
     * Handles quoted string values, numbers, booleans, and null.
     */
    private String findValue(String key) {
        if (raw == null || raw.isEmpty()) return null;
        // Search for "key": pattern
        String needle = "\"" + key + "\":";
        int idx = raw.indexOf(needle);
        if (idx < 0) return null;
        int valStart = idx + needle.length();
        if (valStart >= raw.length()) return null;

        // Parse the value starting at valStart
        char first = raw.charAt(valStart);
        if (first == '"') {
            // Quoted string — find the closing quote (respecting escapes)
            int end = valStart + 1;
            while (end < raw.length()) {
                char c = raw.charAt(end);
                if (c == '\\') {
                    if (end + 1 >= raw.length()) break; // trailing escape — stop safely
                    end += 2;
                    continue;
                }
                if (c == '"') { end++; break; }
                end++;
            }
            return raw.substring(valStart, end);
        } else {
            // Number, boolean, or null — read until comma, closing brace, or end
            int end = valStart;
            while (end < raw.length()) {
                char c = raw.charAt(end);
                if (c == ',' || c == '}') break;
                end++;
            }
            return raw.substring(valStart, end).trim();
        }
    }

    private static String unescapeJson(String s) {
        if (s.indexOf('\\') < 0) return s;
        var sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(++i);
                switch (next) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    default -> { sb.append('\\'); sb.append(next); }
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
