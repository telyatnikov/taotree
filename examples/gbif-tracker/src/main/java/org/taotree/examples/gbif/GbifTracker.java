package org.taotree.examples.gbif;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.taotree.*;

import java.io.File;
import java.lang.foreign.Arena;
import java.util.Arrays;

/**
 * GBIF Species Observation Tracker — demonstrates TaoTree with real biodiversity data.
 *
 * <p>Reads GBIF Simple Parquet occurrence files using <a href="https://hardwood.dev">Hardwood</a>
 * and indexes species observations into a {@link TaoTree} with:
 * <ul>
 *   <li>Dictionary-encoded compound keys (taxonomy + location)</li>
 *   <li>Typed leaf values via {@link LeafLayout} / {@link LeafHandle}</li>
 *   <li>TaoString fields for variable-length data (locality, recordedBy)</li>
 *   <li>JSON catch-all field for remaining Parquet columns</li>
 * </ul>
 *
 * <p>All field access uses pre-computed, type-safe handles — no string literals
 * at the call site, no manual byte offsets.
 *
 * <p>Usage: {@code java -cp ... org.taotree.examples.gbif.GbifTracker <parquet-dir-or-file>}
 */
public class GbifTracker {

    // -----------------------------------------------------------------------
    // Schema: declared once, used everywhere via handles
    // -----------------------------------------------------------------------

    // Key layout (taxonomy + location) — dicts created automatically by the tree
    private static final KeyLayout KEY_LAYOUT = KeyLayout.of(
        KeyField.dict16("kingdom"),
        KeyField.dict16("phylum"),
        KeyField.dict16("class"),
        KeyField.dict16("order"),
        KeyField.dict16("family"),
        KeyField.dict32("species"),
        KeyField.dict16("countryCode"),
        KeyField.dict16("stateProvince")
    );

    // Leaf layout (observation data + JSON catch-all)
    private static final LeafLayout LEAF_LAYOUT = LeafLayout.of(
        LeafField.int32("count"),               //  4B  observation count
        LeafField.int32("year"),                 //  4B  latest observation year
        LeafField.int32("month"),                //  4B  latest observation month
        LeafField.int32("day"),                  //  4B  latest observation day
        LeafField.float64("decimalLatitude"),    //  8B  last known lat
        LeafField.float64("decimalLongitude"),   //  8B  last known lon
        LeafField.float64("elevation"),          //  8B  elevation in metres
        LeafField.int32("individualCount"),      //  4B
        LeafField.int32("taxonKey"),             //  4B  GBIF backbone taxon key
        LeafField.int32("speciesKey"),           //  4B  GBIF backbone species key
        LeafField.string("locality"),            // 16B  TaoString
        LeafField.string("recordedBy"),          // 16B  TaoString
        LeafField.json("extras")                 // 16B  TaoString holding JSON
    );                                           // total: 100 bytes

    // Pre-computed leaf handles — type-safe, zero-lookup access
    private static final LeafHandle.Int32   COUNT    = LEAF_LAYOUT.int32("count");
    private static final LeafHandle.Int32   YEAR     = LEAF_LAYOUT.int32("year");
    private static final LeafHandle.Int32   MONTH    = LEAF_LAYOUT.int32("month");
    private static final LeafHandle.Int32   DAY      = LEAF_LAYOUT.int32("day");
    private static final LeafHandle.Float64 LAT      = LEAF_LAYOUT.float64("decimalLatitude");
    private static final LeafHandle.Float64 LON      = LEAF_LAYOUT.float64("decimalLongitude");
    private static final LeafHandle.Float64 ELEV     = LEAF_LAYOUT.float64("elevation");
    private static final LeafHandle.Int32   IND_CNT  = LEAF_LAYOUT.int32("individualCount");
    private static final LeafHandle.Int32   TAXON_K  = LEAF_LAYOUT.int32("taxonKey");
    private static final LeafHandle.Int32   SPECIES_K = LEAF_LAYOUT.int32("speciesKey");
    private static final LeafHandle.Str     LOCALITY = LEAF_LAYOUT.string("locality");
    private static final LeafHandle.Str     RECORDED = LEAF_LAYOUT.string("recordedBy");
    private static final LeafHandle.Json    EXTRAS   = LEAF_LAYOUT.json("extras");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: GbifTracker <parquet-dir-or-file>");
            System.out.println("  e.g.: GbifTracker data/gbif/000001.parquet");
            System.out.println("  e.g.: GbifTracker data/gbif/");
            return;
        }

        File input = new File(args[0]);
        File[] files;
        if (input.isDirectory()) {
            files = input.listFiles((dir, name) -> name.endsWith(".parquet"));
            if (files == null || files.length == 0) {
                System.err.println("No .parquet files in " + input);
                return;
            }
            Arrays.sort(files);
        } else {
            files = new File[]{input};
        }

        System.out.println("TaoTree GBIF Species Tracker");
        System.out.println("Files: " + files.length);
        System.out.println();

        // Tree creation: dicts are created automatically from the key layout
        try (var tree = TaoTree.open(KEY_LAYOUT, LEAF_LAYOUT)) {

            // Key handles — derived from the tree (carries bound dict references)
            var KINGDOM  = tree.keyDict16("kingdom");
            var PHYLUM   = tree.keyDict16("phylum");
            var CLASS    = tree.keyDict16("class");
            var ORDER    = tree.keyDict16("order");
            var FAMILY   = tree.keyDict16("family");
            var SPECIES  = tree.keyDict32("species");
            var COUNTRY  = tree.keyDict16("countryCode");
            var STATE    = tree.keyDict16("stateProvince");

            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);

                long totalRows = 0;
                long forwarded = 0;
                long suppressed = 0;
                long skipped = 0;
                long startNs = System.nanoTime();

                for (File file : files) {
                    System.out.printf("Reading %s ...%n", file.getName());

                    try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file.toPath()));
                         RowReader rows = fileReader.createRowReader()) {

                        while (rows.hasNext()) {
                            rows.next();
                            totalRows++;

                            String species = nullSafe(rows, "species");
                            if (species == null) { skipped++; continue; }

                            // Build compound key — handle-based, type-safe
                            kb.set(KINGDOM, nullSafe(rows, "kingdom"))
                              .set(PHYLUM,  nullSafe(rows, "phylum"))
                              .set(CLASS,   nullSafe(rows, "class"))
                              .set(ORDER,   nullSafe(rows, "order"))
                              .set(FAMILY,  nullSafe(rows, "family"))
                              .set(SPECIES, species)
                              .set(COUNTRY, nullSafe(rows, "countrycode"))
                              .set(STATE,   nullSafe(rows, "stateprovince"));

                            // Read value fields from Parquet
                            int year  = safeInt(rows, "year");
                            int month = safeInt(rows, "month");
                            int day   = safeInt(rows, "day");
                            double lat  = safeDouble(rows, "decimallatitude");
                            double lon  = safeDouble(rows, "decimallongitude");
                            double elev = safeDouble(rows, "elevation");

                            // Insert / update — handle-based, type-safe
                            try (var w = tree.write()) {
                                var leaf = w.getOrCreate(kb);

                                int existingCount = leaf.get(COUNT);
                                if (existingCount == 0) {
                                    // New entry
                                    leaf.set(COUNT, 1)
                                        .set(YEAR, year)
                                        .set(MONTH, month)
                                        .set(DAY, day)
                                        .set(LAT, lat)
                                        .set(LON, lon)
                                        .set(ELEV, elev)
                                        .set(IND_CNT, safeInt(rows, "individualcount"))
                                        .set(TAXON_K, safeInt(rows, "taxonkey"))
                                        .set(SPECIES_K, safeInt(rows, "specieskey"));

                                    String locality = nullSafe(rows, "locality");
                                    if (locality != null) leaf.set(LOCALITY, locality);
                                    String recordedBy = nullSafe(rows, "recordedby");
                                    if (recordedBy != null) leaf.set(RECORDED, recordedBy);
                                    String extras = buildExtras(rows);
                                    if (extras != null) leaf.set(EXTRAS, extras);
                                    forwarded++;
                                } else {
                                    // Update: increment count, keep latest observation
                                    leaf.set(COUNT, existingCount + 1);
                                    if (year > leaf.get(YEAR)
                                        || (year == leaf.get(YEAR) && month > leaf.get(MONTH))) {
                                        leaf.set(YEAR, year)
                                            .set(MONTH, month)
                                            .set(DAY, day)
                                            .set(LAT, lat)
                                            .set(LON, lon);
                                        forwarded++;
                                    } else {
                                        suppressed++;
                                    }
                                }
                            }

                            if (totalRows % 100_000 == 0) {
                                double elapsedSec = (System.nanoTime() - startNs) / 1e9;
                                try (var r = tree.read()) {
                                    System.out.printf("  %,d rows | %,d entries | %.1f rows/sec%n",
                                        totalRows, r.size(), totalRows / elapsedSec);
                                }
                            }
                        }
                    }
                }

                double elapsedSec = (System.nanoTime() - startNs) / 1e9;

                System.out.println();
                System.out.println("=== Results ===");
                System.out.printf("Total rows:     %,d%n", totalRows);
                System.out.printf("Forwarded:      %,d (new or updated)%n", forwarded);
                System.out.printf("Suppressed:     %,d (older duplicate)%n", suppressed);
                System.out.printf("Skipped:        %,d (no species)%n", skipped);
                try (var r = tree.read()) {
                    System.out.printf("ART entries:    %,d%n", r.size());
                }
                System.out.printf("Key size:       %d bytes (%d fields)%n",
                    tree.keyLayout().totalWidth(), tree.keyLayout().fieldCount());
                System.out.printf("Leaf size:      %d bytes (%s)%n",
                    LEAF_LAYOUT.totalWidth(), describeLeafLayout(LEAF_LAYOUT));
                System.out.printf("Elapsed:        %.2f sec%n", elapsedSec);
                System.out.printf("Throughput:     %,.0f rows/sec%n", totalRows / elapsedSec);
                System.out.println();
                System.out.println("=== Dictionaries ===");
                for (var dict : tree.dictionaries()) {
                    // Find the dict's field name from the key layout
                    var kl = tree.keyLayout();
                    for (int i = 0; i < kl.fieldCount(); i++) {
                        var f = kl.field(i);
                        if (f instanceof KeyField.DictU16 d && d.dict() == dict
                         || f instanceof KeyField.DictU32 d2 && d2.dict() == dict) {
                            System.out.printf("  %-16s %,d entries%n", f.name() + ":", dict.size());
                        }
                    }
                }
                System.out.println();
                System.out.println("=== Memory ===");
                System.out.printf("  SlabAllocator: %,.1f MB (%,d segments in use)%n",
                    tree.totalSlabBytes() / (1024.0 * 1024),
                    tree.totalSegmentsInUse());
                System.out.printf("  BumpAllocator: %,.1f MB (%,d pages)%n",
                    tree.totalOverflowBytes() / (1024.0 * 1024),
                    tree.overflowPageCount());
            }
        }
    }

    /** Read a nullable string field from the Parquet row. Returns null for arrays/structs. */
    private static String nullSafe(RowReader rows, String field) {
        try {
            return rows.isNull(field) ? null : rows.getString(field);
        } catch (IllegalArgumentException | ClassCastException e) {
            return null;
        }
    }

    /** Safely read an integer field with type mismatch fallback. */
    private static int safeInt(RowReader rows, String field) {
        if (rows.isNull(field)) return 0;
        try {
            return rows.getInt(field);
        } catch (ClassCastException e) {
            try {
                return (int) rows.getLong(field);
            } catch (ClassCastException e2) {
                try {
                    return Integer.parseInt(rows.getString(field));
                } catch (NumberFormatException e3) {
                    return 0;
                }
            }
        }
    }

    /** Safely read a double field with type mismatch fallback. */
    private static double safeDouble(RowReader rows, String field) {
        if (rows.isNull(field)) return Double.NaN;
        try {
            return rows.getDouble(field);
        } catch (ClassCastException e) {
            try {
                return Double.parseDouble(rows.getString(field));
            } catch (NumberFormatException e2) {
                return Double.NaN;
            }
        }
    }

    /** Build a compact JSON string from supplementary Parquet fields. */
    private static String buildExtras(RowReader rows) {
        var sb = new StringBuilder(64);
        sb.append('{');
        boolean first = true;

        first = appendStr(sb, first, "basisOfRecord", nullSafe(rows, "basisofrecord"));
        first = appendStr(sb, first, "license", nullSafe(rows, "license"));
        first = appendStr(sb, first, "occurrenceStatus", nullSafe(rows, "occurrencestatus"));
        first = appendStr(sb, first, "scientificName", nullSafe(rows, "scientificname"));
        first = appendStr(sb, first, "institutionCode", nullSafe(rows, "institutioncode"));
        first = appendStr(sb, first, "catalogNumber", nullSafe(rows, "catalognumber"));
        first = appendStr(sb, first, "eventDate", nullSafe(rows, "eventdate"));

        if (!rows.isNull("coordinateuncertaintyinmeters")) {
            if (!first) sb.append(',');
            sb.append("\"coordinateUncertainty\":").append(rows.getDouble("coordinateuncertaintyinmeters"));
            first = false;
        }

        sb.append('}');
        return first ? null : sb.toString();
    }

    private static boolean appendStr(StringBuilder sb, boolean first, String key, String value) {
        if (value == null) return first;
        if (!first) sb.append(',');
        sb.append('"').append(key).append("\":\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"') sb.append("\\\"");
            else if (c == '\\') sb.append("\\\\");
            else if (c < 0x20) sb.append(' ');
            else sb.append(c);
        }
        sb.append('"');
        return false;
    }

    private static String describeLeafLayout(LeafLayout layout) {
        var sb = new StringBuilder();
        for (int i = 0; i < layout.fieldCount(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(layout.field(i).name()).append(':').append(layout.field(i).width()).append('B');
        }
        return sb.toString();
    }
}
