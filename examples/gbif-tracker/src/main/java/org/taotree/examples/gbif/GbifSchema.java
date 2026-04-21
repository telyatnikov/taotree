package org.taotree.examples.gbif;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Schema model for GBIF Simple Parquet files.
 *
 * <p>GBIF Simple files carry 46 top-level columns (50 leaf columns in the
 * Parquet schema because 5 of them are {@code LIST}-group repeated fields
 * with a leaf named {@code array_element}). This class captures:
 *
 * <ul>
 *   <li>the fixed set of taxonomic fields that form the TaoTree prefix key
 *       (Approach B: taxonomy-prefix + temporal observations);</li>
 *   <li>the fixed set of repeated top-level field names whose cells are
 *       encoded as JSON lists for lossless round-trip;</li>
 *   <li>a persisted column-order sidecar ({@code schema.json}) so the tree
 *       can be read back without needing the original Parquet file.</li>
 * </ul>
 */
public final class GbifSchema {

    private GbifSchema() {}

    // ── Taxonomy prefix key (ordered, 7 fields) ──────────────────────────

    /** Ordered list of Parquet column names that form the taxonomic prefix. */
    public static final List<String> TAXONOMY_FIELDS = List.of(
        "kingdom", "phylum", "class", "order", "family", "genus", "species"
    );

    /** The Parquet column used as the primary identity suffix of the entity key. */
    public static final String GBIFID_FIELD = "gbifid";

    /** The Parquet column used as the observation timestamp. Nullable → TIMELESS. */
    public static final String EVENTDATE_FIELD = "eventdate";

    // ── Repeated top-level columns ───────────────────────────────────────

    /**
     * Top-level names of repeated fields in the GBIF Simple schema. These are
     * the columns whose Parquet leaf path ends in {@code array.array_element}.
     * On write they become a single JSON array stored via {@code Value.ofJson}.
     */
    public static final Set<String> REPEATED_FIELDS = Set.of(
        "identifiedby", "recordedby", "typestatus", "mediatype", "issue"
    );

    // ── Taxonomy-null sentinel ───────────────────────────────────────────

    /**
     * Dictionary-interned sentinel used for NULL taxonomic rank cells. Chosen
     * to be distinct from any real taxonomic name by using a control-character
     * wrapper. The reverse-reader maps this back to Parquet NULL.
     */
    public static final String NULL_TAXON_SENTINEL = "\u0000NULL\u0000";

    // ── Column descriptor ────────────────────────────────────────────────

    /**
     * One logical (top-level) column in the GBIF schema. This deliberately
     * ignores the intermediate {@code .array.array_element} path of repeated
     * fields: a repeated list is written and read back as a single named cell.
     */
    public record Column(String name, PhysicalType type, boolean repeated) {
        public boolean isRepeated() { return repeated; }
    }

    /**
     * Build the ordered list of top-level columns from a Parquet file schema.
     * Repeated leaves (path tail {@code array.array_element}) are collapsed
     * into a single entry keyed by {@link dev.hardwood.metadata.FieldPath#topLevelName()}.
     */
    public static List<Column> topLevelColumns(FileSchema schema) {
        var seen = new LinkedHashMap<String, Column>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            ColumnSchema c = schema.getColumn(i);
            String top = c.fieldPath().topLevelName();
            if (seen.containsKey(top)) continue;
            boolean repeated = REPEATED_FIELDS.contains(top)
                || c.fieldPath().elements().size() > 1;
            seen.put(top, new Column(top, c.type(), repeated));
        }
        return List.copyOf(seen.values());
    }

    // ── schema.json sidecar  ─────────────────────────────────────────────

    /**
     * Write the column list as a very small JSON sidecar next to the TaoTree
     * store file so the tree can be read back without the original Parquet.
     *
     * <p>Format (one column per line for readability):
     * <pre>{@code
     * {
     *   "version": 1,
     *   "columns": [
     *     {"name":"gbifid","type":"BYTE_ARRAY","repeated":false},
     *     ...
     *   ]
     * }
     * }</pre>
     */
    public static void writeSidecar(Path sidecarPath, List<Column> columns) throws java.io.IOException {
        try (BufferedWriter w = Files.newBufferedWriter(sidecarPath, StandardCharsets.UTF_8)) {
            w.write("{\n  \"version\": 1,\n  \"columns\": [\n");
            for (int i = 0; i < columns.size(); i++) {
                Column c = columns.get(i);
                w.write("    {\"name\":\"");
                w.write(escape(c.name()));
                w.write("\",\"type\":\"");
                w.write(c.type().name());
                w.write("\",\"repeated\":");
                w.write(c.repeated() ? "true" : "false");
                w.write('}');
                if (i + 1 < columns.size()) w.write(',');
                w.write('\n');
            }
            w.write("  ]\n}\n");
        }
    }

    /** Read the schema sidecar back. */
    public static List<Column> readSidecar(Path sidecarPath) throws java.io.IOException {
        var lines = new ArrayList<String>();
        try (BufferedReader r = Files.newBufferedReader(sidecarPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) lines.add(line.trim());
        }
        // Single-line per column format; parse with tiny regex-free state machine.
        var out = new ArrayList<Column>();
        for (String line : lines) {
            if (!line.startsWith("{\"name\"")) continue;
            String name = extract(line, "\"name\":\"", "\"");
            String type = extract(line, "\"type\":\"", "\"");
            boolean rep = line.contains("\"repeated\":true");
            out.add(new Column(name, PhysicalType.valueOf(type), rep));
        }
        return List.copyOf(out);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String extract(String line, String opener, String closer) {
        int a = line.indexOf(opener);
        if (a < 0) throw new IllegalStateException("malformed sidecar line: " + line);
        int b = line.indexOf(closer, a + opener.length());
        if (b < 0) throw new IllegalStateException("malformed sidecar line: " + line);
        return line.substring(a + opener.length(), b);
    }
}
