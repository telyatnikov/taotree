package org.taotree.examples.gbif;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.taotree.*;

import java.io.File;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.taotree.TaoTree;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;
import org.taotree.layout.LeafField;
import org.taotree.layout.LeafHandle;
import org.taotree.layout.LeafLayout;

/**
 * GBIF Species Observation Tracker — demonstrates TaoTree with real biodiversity data.
 *
 * <p>Reads GBIF Simple Parquet occurrence files using <a href="https://hardwood.dev">Hardwood</a>
 * and indexes species observations into a persistent, file-backed {@link TaoTree}.
 *
 * <p>The store file ({@code gbif-tracker.taotree}) is created next to the source Parquet
 * files. Each run rebuilds the store from the input Parquet data.
 *
 * <p>Usage: {@code java -cp ... org.taotree.examples.gbif.GbifTracker <parquet-dir-or-file>}
 */
public class GbifTracker {

    static final String STORE_NAME = "gbif-tracker.taotree";

    // -----------------------------------------------------------------------
    // Schema: declared once, used everywhere via handles
    // -----------------------------------------------------------------------

    static final KeyLayout KEY_LAYOUT = KeyLayout.of(
        KeyField.dict16("kingdom"),
        KeyField.dict16("phylum"),
        KeyField.dict16("family"),
        KeyField.dict32("species"),
        KeyField.dict16("countryCode"),
        KeyField.dict16("stateProvince")
    );

    static final LeafLayout LEAF_LAYOUT = LeafLayout.of(
        LeafField.int32("count"),
        LeafField.int32("year"),
        LeafField.int32("month"),
        LeafField.int32("day"),
        LeafField.float64("decimalLatitude"),
        LeafField.float64("decimalLongitude"),
        LeafField.float64("elevation"),
        LeafField.int32("individualCount"),
        LeafField.int32("taxonKey"),
        LeafField.int32("speciesKey"),
        LeafField.string("locality"),
        LeafField.string("recordedBy"),
        LeafField.json("extras")
    );

    static final LeafHandle.Int32   COUNT     = LEAF_LAYOUT.int32("count");
    static final LeafHandle.Int32   YEAR      = LEAF_LAYOUT.int32("year");
    static final LeafHandle.Int32   MONTH     = LEAF_LAYOUT.int32("month");
    static final LeafHandle.Int32   DAY       = LEAF_LAYOUT.int32("day");
    static final LeafHandle.Float64 LAT       = LEAF_LAYOUT.float64("decimalLatitude");
    static final LeafHandle.Float64 LON       = LEAF_LAYOUT.float64("decimalLongitude");
    static final LeafHandle.Float64 ELEV      = LEAF_LAYOUT.float64("elevation");
    static final LeafHandle.Int32   IND_CNT   = LEAF_LAYOUT.int32("individualCount");
    static final LeafHandle.Int32   TAXON_K   = LEAF_LAYOUT.int32("taxonKey");
    static final LeafHandle.Int32   SPECIES_K = LEAF_LAYOUT.int32("speciesKey");
    static final LeafHandle.Str     LOCALITY  = LEAF_LAYOUT.string("locality");
    static final LeafHandle.Str     RECORDED  = LEAF_LAYOUT.string("recordedBy");
    static final LeafHandle.Json    EXTRAS    = LEAF_LAYOUT.json("extras");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: GbifTracker <parquet-dir-or-file>");
            System.out.println("  e.g.: GbifTracker data/gbif/");
            System.out.println("  e.g.: GbifTracker data/gbif/000001.parquet");
            return;
        }

        File input = new File(args[0]);
        File[] files;
        File storeDir;
        if (input.isDirectory()) {
            files = input.listFiles((dir, name) -> name.endsWith(".parquet"));
            if (files == null || files.length == 0) {
                System.err.println("No .parquet files in " + input);
                return;
            }
            Arrays.sort(files);
            storeDir = input;
        } else {
            files = new File[]{input};
            storeDir = input.getParentFile() != null ? input.getParentFile() : new File(".");
        }

        Path storePath = storeDir.toPath().resolve(STORE_NAME);

        System.out.println("TaoTree GBIF Species Tracker");
        System.out.println("Files:  " + files.length);
        System.out.println("Store:  " + storePath);
        System.out.println();

        // Create a fresh persistent store
        try (var tree = createFresh(storePath)) {

            // COW mode is always active (lock-free readers, concurrent writers)

            var KINGDOM = tree.keyDict16("kingdom");
            var PHYLUM  = tree.keyDict16("phylum");
            var FAMILY  = tree.keyDict16("family");
            var SPECIES = tree.keyDict32("species");
            var COUNTRY = tree.keyDict16("countryCode");
            var STATE   = tree.keyDict16("stateProvince");

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

                        var w = tree.write();
                        try {
                        while (rows.hasNext()) {
                            rows.next();
                            totalRows++;

                            String species = nullSafe(rows, "species");
                            if (species == null) { skipped++; continue; }

                            kb.set(KINGDOM, nullSafe(rows, "kingdom"))
                              .set(PHYLUM,  nullSafe(rows, "phylum"))
                              .set(FAMILY,  nullSafe(rows, "family"))
                              .set(SPECIES, species)
                              .set(COUNTRY, nullSafe(rows, "countrycode"))
                              .set(STATE,   nullSafe(rows, "stateprovince"));

                            int year  = safeInt(rows, "year");
                            int month = safeInt(rows, "month");
                            int day   = safeInt(rows, "day");
                            double lat  = safeDouble(rows, "decimallatitude");
                            double lon  = safeDouble(rows, "decimallongitude");
                            double elev = safeDouble(rows, "elevation");

                            var leaf = w.getOrCreate(kb);

                            int existingCount = leaf.get(COUNT);
                            if (existingCount == 0) {
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
                                // Existing entry: increment count, update all fields if newer
                                leaf.set(COUNT, existingCount + 1);
                                if (year > leaf.get(YEAR)
                                    || (year == leaf.get(YEAR) && month > leaf.get(MONTH))) {
                                    leaf.set(YEAR, year)
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
                                    suppressed++;
                                }
                            }

                            if (totalRows % 100_000 == 0) {
                                // Close scope, sync, reopen for progress reporting
                                w.close();
                                tree.sync();
                                double elapsedSec = (System.nanoTime() - startNs) / 1e9;
                                try (var r = tree.read()) {
                                    System.out.printf("  %,d rows | %,d entries | %.1f rows/sec%n",
                                        totalRows, r.size(), totalRows / elapsedSec);
                                }
                                w = tree.write();
                            }
                        }
                        } finally {
                            w.close();
                        }
                    }
                }

                // Flush to disk
                tree.sync();

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
                System.out.printf("Leaf size:      %d bytes (%d fields)%n",
                    LEAF_LAYOUT.totalWidth(), LEAF_LAYOUT.fieldCount());
                System.out.printf("Store file:     %s (%,.1f MB)%n",
                    storePath.getFileName(), Files.size(storePath) / (1024.0 * 1024));
                System.out.printf("Elapsed:        %.2f sec%n", elapsedSec);
                System.out.printf("Throughput:     %,.0f rows/sec%n", totalRows / elapsedSec);
                System.out.println();
                System.out.println("=== Dictionaries ===");
                for (var dict : tree.dictionaries()) {
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
                System.out.printf("  Slab:     %,.1f MB (%,d segments in use)%n",
                    tree.totalSlabBytes() / (1024.0 * 1024),
                    tree.totalSegmentsInUse());
                System.out.printf("  Overflow: %,.1f MB (%,d pages)%n",
                    tree.totalOverflowBytes() / (1024.0 * 1024),
                    tree.overflowPageCount());
            }
        }
    }

    private static TaoTree createFresh(Path storePath) throws java.io.IOException {
        if (Files.exists(storePath)) {
            System.out.println("Replacing existing store...");
            Files.delete(storePath);
        } else {
            System.out.println("Creating new store...");
        }
        return TaoTree.create(storePath, KEY_LAYOUT, LEAF_LAYOUT);
    }

    /** Read a nullable string field. For array fields (e.g. recordedBy), joins elements with "; ". */
    static String nullSafe(RowReader rows, String field) {
        if (rows.isNull(field)) return null;
        try {
            return rows.getString(field);
        } catch (IllegalArgumentException | ClassCastException e) {
            // Field is a LIST<STRING> — join elements
            try {
                var list = rows.getList(field);
                if (list == null || list.isEmpty()) return null;
                var sb = new StringBuilder();
                for (var s : list.strings()) {
                    if (s != null) {
                        if (!sb.isEmpty()) sb.append("; ");
                        sb.append(s);
                    }
                }
                return sb.isEmpty() ? null : sb.toString();
            } catch (Exception e2) {
                return null;
            }
        }
    }

    static int safeInt(RowReader rows, String field) {
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

    static double safeDouble(RowReader rows, String field) {
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

    static String buildExtras(RowReader rows) {
        var sb = new StringBuilder(64);
        sb.append('{');
        boolean first = true;
        first = appendStr(sb, first, "class", nullSafe(rows, "class"));
        first = appendStr(sb, first, "order", nullSafe(rows, "order"));
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
}
