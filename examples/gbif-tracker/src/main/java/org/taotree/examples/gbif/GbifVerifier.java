package org.taotree.examples.gbif;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.taotree.*;

import java.io.File;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.taotree.LeafAccessor;
import org.taotree.TaoTree;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyLayout;
import org.taotree.layout.LeafField;
import org.taotree.layout.LeafHandle;
import org.taotree.layout.LeafLayout;

/**
 * Independent verification of gbif-tracker.taotree against Parquet source.
 *
 * <p>IMPORTANT: This verifier does NOT reuse any GbifTracker helper methods.
 * It reads Parquet fields using independent parsing logic so that bugs in
 * GbifTracker's conversion (safeInt, nullSafe, buildExtras) are detected.
 *
 * <p>If both the tracker and verifier had the same bug, the comparison would
 * silently pass. Using independent code paths prevents this.
 *
 * Usage: java -cp ... org.taotree.examples.gbif.GbifVerifier <parquet-dir>
 */
public class GbifVerifier {

    // Own layout declarations — not referencing GbifTracker's fields
    static final KeyLayout KEY_LAYOUT = KeyLayout.of(
        KeyField.dict16("kingdom"), KeyField.dict16("phylum"),
        KeyField.dict16("family"), KeyField.dict32("species"),
        KeyField.dict16("countryCode"), KeyField.dict16("stateProvince")
    );
    static final LeafLayout LEAF_LAYOUT = LeafLayout.of(
        LeafField.int32("count"), LeafField.int32("year").nullable(),
        LeafField.int32("month").nullable(), LeafField.int32("day").nullable(),
        LeafField.float64("decimalLatitude").nullable(), LeafField.float64("decimalLongitude").nullable(),
        LeafField.float64("elevation").nullable(), LeafField.int32("individualCount").nullable(),
        LeafField.int32("taxonKey").nullable(), LeafField.int32("speciesKey").nullable(),
        LeafField.string("locality").nullable(), LeafField.string("recordedBy").nullable(),
        LeafField.extras("extras")
    );

    static final LeafHandle.Int32   COUNT = LEAF_LAYOUT.int32("count");
    static final LeafHandle.Int32   YEAR  = LEAF_LAYOUT.int32("year");
    static final LeafHandle.Int32   MONTH = LEAF_LAYOUT.int32("month");
    static final LeafHandle.Int32   DAY   = LEAF_LAYOUT.int32("day");
    static final LeafHandle.Float64 LAT   = LEAF_LAYOUT.float64("decimalLatitude");
    static final LeafHandle.Float64 LON   = LEAF_LAYOUT.float64("decimalLongitude");
    static final LeafHandle.Float64 ELEV  = LEAF_LAYOUT.float64("elevation");
    static final LeafHandle.Int32   IND   = LEAF_LAYOUT.int32("individualCount");
    static final LeafHandle.Int32   TAX   = LEAF_LAYOUT.int32("taxonKey");
    static final LeafHandle.Int32   SPK   = LEAF_LAYOUT.int32("speciesKey");
    static final LeafHandle.Str     LOC   = LEAF_LAYOUT.string("locality");
    static final LeafHandle.Str     REC   = LEAF_LAYOUT.string("recordedBy");
    static final LeafHandle.Extras  EXT   = LEAF_LAYOUT.extras("extras");

    record Expected(int count, int year, int month, int day,
                    double lat, double lon, double elev,
                    int indCnt, int taxonKey, int speciesKey,
                    String locality, String recordedBy, String extras) {}

    /** Result of a verification run. */
    record VerifyResult(long treeEntries, long expectedEntries, int checked,
                        int missing, int fieldErrors, List<String> errors) {
        boolean passed() { return treeEntries == expectedEntries && missing == 0 && fieldErrors == 0; }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) { System.out.println("Usage: GbifVerifier <parquet-dir>"); return; }

        File input = new File(args[0]);
        File[] files = input.isDirectory()
            ? input.listFiles((d, n) -> n.endsWith(".parquet"))
            : new File[]{input};
        if (files == null || files.length == 0) { System.err.println("No parquet files"); return; }
        Arrays.sort(files);

        Path storePath = (input.isDirectory() ? input : input.getParentFile())
            .toPath().resolve("gbif-tracker.taotree");
        if (!Files.exists(storePath)) {
            System.err.println("Store not found: " + storePath + " — run GbifTracker first.");
            return;
        }

        System.out.println("=== Independent Verification ===");
        System.out.println("Store:   " + storePath);
        System.out.println("Sources: " + files.length + " parquet files");
        System.out.println();

        var result = verify(storePath, files);

        System.out.println();
        System.out.println("=== Results ===");
        System.out.printf("  Tree entries:     %,d%n", result.treeEntries);
        System.out.printf("  Expected entries: %,d  %s%n", result.expectedEntries,
            result.treeEntries == result.expectedEntries ? "OK" : "MISMATCH!");
        System.out.printf("  Entries checked:  %,d%n", result.checked);
        System.out.printf("  Missing:          %d%n", result.missing);
        System.out.printf("  Field mismatches: %d%n", result.fieldErrors);
        if (!result.errors.isEmpty()) {
            System.out.println();
            result.errors.stream().limit(20).forEach(e -> System.out.println("    " + e));
        }
        System.out.println();
        System.out.println(result.passed()
            ? "RESULT: ALL FIELDS MATCH (independent verification)"
            : "RESULT: MISMATCHES FOUND");
    }

    /**
     * Verify a store against Parquet source files.
     * Uses independent parsing (not GbifTracker helpers) to detect bugs.
     *
     * @param storePath path to the .taotree file
     * @param files     the Parquet source files
     * @return verification result
     */
    static VerifyResult verify(Path storePath, File[] files) throws Exception {

        // Phase 1: Build expected state with INDEPENDENT parsing
        System.out.println("Phase 1: Reading Parquet with independent parser...");
        var expected = new LinkedHashMap<String, Expected>();
        long totalRows = 0, skipped = 0;

        for (File file : files) {
            try (var reader = ParquetFileReader.open(InputFile.of(file.toPath()));
                 var rows = reader.createRowReader()) {
                while (rows.hasNext()) {
                    rows.next(); totalRows++;

                    // Independent string reading — NOT using GbifTracker.nullSafe
                    String species = readString(rows, "species");
                    if (species == null) { skipped++; continue; }

                    String key = readString(rows, "kingdom") + "|"
                        + readString(rows, "phylum") + "|"
                        + readString(rows, "family") + "|"
                        + species + "|"
                        + readString(rows, "countrycode") + "|"
                        + readString(rows, "stateprovince");

                    // Independent numeric reading — NOT using GbifTracker.safeInt/safeDouble
                    int year  = readInt(rows, "year");
                    int month = readInt(rows, "month");
                    int day   = readInt(rows, "day");
                    double lat  = readDouble(rows, "decimallatitude");
                    double lon  = readDouble(rows, "decimallongitude");
                    double elev = readDouble(rows, "elevation");
                    int indCnt  = readInt(rows, "individualcount");
                    int taxK    = readInt(rows, "taxonkey");
                    int spK     = readInt(rows, "specieskey");
                    String loc  = readString(rows, "locality");
                    String rec  = readStringOrList(rows, "recordedby");

                    // Independent JSON building — NOT using GbifTracker.buildExtras
                    String ext = buildExtrasIndependent(rows);

                    var existing = expected.get(key);
                    if (existing == null) {
                        expected.put(key, new Expected(1, year, month, day, lat, lon, elev,
                            indCnt, taxK, spK, loc, rec, ext));
                    } else {
                        int newCount = existing.count + 1;
                        if (isNewer(year, month, day, lat, lon, taxK, spK, indCnt,
                                loc, rec, ext,
                                existing.year, existing.month, existing.day,
                                existing.lat, existing.lon,
                                existing.taxonKey, existing.speciesKey, existing.indCnt,
                                existing.locality, existing.recordedBy, existing.extras)) {
                            expected.put(key, new Expected(newCount, year, month, day,
                                lat, lon, elev, indCnt, taxK, spK, loc, rec, ext));
                        } else {
                            expected.put(key, new Expected(newCount, existing.year,
                                existing.month, existing.day, existing.lat, existing.lon,
                                existing.elev, existing.indCnt, existing.taxonKey,
                                existing.speciesKey, existing.locality, existing.recordedBy,
                                existing.extras));
                        }
                    }
                }
            }
        }
        System.out.printf("  Rows: %,d  Skipped: %,d  Unique keys: %,d%n%n", totalRows, skipped, expected.size());

        // Phase 2: Check every field in the TaoTree
        System.out.println("Phase 2: Checking every entry and field in TaoTree...");

        try (var tree = TaoTree.open(storePath, KEY_LAYOUT, LEAF_LAYOUT)) {
            var KINGDOM = tree.keyDict16("kingdom");
            var PHYLUM  = tree.keyDict16("phylum");
            var FAMILY  = tree.keyDict16("family");
            var SPECIES = tree.keyDict32("species");
            var COUNTRY = tree.keyDict16("countryCode");
            var STATE   = tree.keyDict16("stateProvince");

            long treeEntries;
            try (var r = tree.read()) { treeEntries = r.size(); }

            int checked = 0, missing = 0, fieldErrors = 0;
            var errors = new ArrayList<String>();

            // Re-read Parquet to encode keys with the tree's dicts
            var checkedKeys = new HashSet<String>();
            try (var arena = Arena.ofConfined()) {
                var kb = tree.newKeyBuilder(arena);

                for (File file : files) {
                    try (var reader = ParquetFileReader.open(InputFile.of(file.toPath()));
                         var rows = reader.createRowReader()) {
                        while (rows.hasNext()) {
                            rows.next();
                            String species = readString(rows, "species");
                            if (species == null) continue;

                            String mapKey = readString(rows, "kingdom") + "|"
                                + readString(rows, "phylum") + "|"
                                + readString(rows, "family") + "|"
                                + species + "|"
                                + readString(rows, "countrycode") + "|"
                                + readString(rows, "stateprovince");

                            if (!checkedKeys.add(mapKey)) continue;
                            var exp = expected.get(mapKey);
                            if (exp == null) continue;

                            kb.set(KINGDOM, readString(rows, "kingdom"))
                              .set(PHYLUM,  readString(rows, "phylum"))
                              .set(FAMILY,  readString(rows, "family"))
                              .set(SPECIES, species)
                              .set(COUNTRY, readString(rows, "countrycode"))
                              .set(STATE,   readString(rows, "stateprovince"));

                            try (var r = tree.read()) {
                                var leaf = r.lookup(kb);
                                if (leaf == null) {
                                    missing++;
                                    if (errors.size() < 20) errors.add("MISSING: " + mapKey);
                                    continue;
                                }
                                checked++;

                                fieldErrors += cmp("count", exp.count, leaf.get(COUNT), mapKey, errors);
                                fieldErrors += cmp("year", exp.year, readNullable(leaf, YEAR), mapKey, errors);
                                fieldErrors += cmp("month", exp.month, readNullable(leaf, MONTH), mapKey, errors);
                                fieldErrors += cmp("day", exp.day, readNullable(leaf, DAY), mapKey, errors);
                                fieldErrors += cmpD("lat", exp.lat, readNullableD(leaf, LAT), mapKey, errors);
                                fieldErrors += cmpD("lon", exp.lon, readNullableD(leaf, LON), mapKey, errors);
                                fieldErrors += cmpD("elev", exp.elev, readNullableD(leaf, ELEV), mapKey, errors);
                                fieldErrors += cmp("indCnt", exp.indCnt, readNullable(leaf, IND), mapKey, errors);
                                fieldErrors += cmp("taxonKey", exp.taxonKey, readNullable(leaf, TAX), mapKey, errors);
                                fieldErrors += cmp("speciesKey", exp.speciesKey, readNullable(leaf, SPK), mapKey, errors);
                                if (exp.locality != null)
                                    fieldErrors += cmpS("locality", exp.locality, safeGet(leaf, LOC), mapKey, errors);
                                if (exp.recordedBy != null)
                                    fieldErrors += cmpS("recordedBy", exp.recordedBy, safeGet(leaf, REC), mapKey, errors);
                                if (exp.extras != null)
                                    fieldErrors += cmpS("extras", exp.extras, safeGet(leaf, EXT), mapKey, errors);
                            }
                        }
                    }
                }
            }

            System.out.printf("  Checked: %,d  Missing: %d  Field errors: %d%n",
                checked, missing, fieldErrors);

            return new VerifyResult(treeEntries, expected.size(), checked, missing, fieldErrors, errors);
        }
    }

    // -----------------------------------------------------------------------
    // Independent Parquet reading — does NOT share code with GbifTracker
    // -----------------------------------------------------------------------

    /** Read a string field. Returns null if null/empty. Handles only primitive STRING columns. */
    private static String readString(RowReader rows, String field) {
        if (rows.isNull(field)) return null;
        try {
            String v = rows.getString(field);
            return (v == null || v.isEmpty()) ? null : v;
        } catch (IllegalArgumentException | ClassCastException e) {
            return null; // array/struct → skip (handled separately by readStringOrList)
        }
    }

    /** Read a string-or-list field (like recordedBy). Joins list elements with "; ". */
    private static String readStringOrList(RowReader rows, String field) {
        if (rows.isNull(field)) return null;
        try {
            String v = rows.getString(field);
            return (v == null || v.isEmpty()) ? null : v;
        } catch (IllegalArgumentException | ClassCastException e) {
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

    /** Read an int field with type fallbacks. Independent implementation. */
    private static int readInt(RowReader rows, String field) {
        if (rows.isNull(field)) return 0;
        try {
            return rows.getInt(field);
        } catch (ClassCastException e) {
            try {
                return (int) rows.getLong(field);
            } catch (ClassCastException e2) {
                try {
                    String s = rows.getString(field);
                    return (s == null || s.isEmpty()) ? 0 : Integer.parseInt(s.trim());
                } catch (Exception e3) {
                    return 0;
                }
            }
        }
    }

    /** Read a double field with type fallbacks. Independent implementation. */
    private static double readDouble(RowReader rows, String field) {
        if (rows.isNull(field)) return Double.NaN;
        try {
            return rows.getDouble(field);
        } catch (ClassCastException e) {
            try {
                String s = rows.getString(field);
                return (s == null || s.isEmpty()) ? Double.NaN : Double.parseDouble(s.trim());
            } catch (Exception e2) {
                return Double.NaN;
            }
        }
    }

    /**
     * Build extras JSON independently — same fields as GbifTracker but own implementation.
     * Any difference in escaping, field ordering, or value reading would be caught.
     */
    private static String buildExtrasIndependent(RowReader rows) {
        var sb = new StringBuilder(64);
        sb.append('{');
        boolean first = true;
        first = jsonStr(sb, first, "class", readString(rows, "class"));
        first = jsonStr(sb, first, "order", readString(rows, "order"));
        first = jsonStr(sb, first, "basisOfRecord", readString(rows, "basisofrecord"));
        first = jsonStr(sb, first, "license", readString(rows, "license"));
        first = jsonStr(sb, first, "occurrenceStatus", readString(rows, "occurrencestatus"));
        first = jsonStr(sb, first, "scientificName", readString(rows, "scientificname"));
        first = jsonStr(sb, first, "institutionCode", readString(rows, "institutioncode"));
        first = jsonStr(sb, first, "catalogNumber", readString(rows, "catalognumber"));
        first = jsonStr(sb, first, "eventDate", readString(rows, "eventdate"));
        if (!rows.isNull("coordinateuncertaintyinmeters")) {
            if (!first) sb.append(',');
            sb.append("\"coordinateUncertainty\":").append(rows.getDouble("coordinateuncertaintyinmeters"));
            first = false;
        }
        sb.append('}');
        return first ? null : sb.toString();
    }

    private static boolean jsonStr(StringBuilder sb, boolean first, String key, String value) {
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

    private static String safeGet(LeafAccessor leaf, LeafHandle.Str h) {
        try { String v = leaf.get(h); return (v == null || v.isEmpty()) ? null : v; }
        catch (Exception e) { return null; }
    }

    private static String safeGet(LeafAccessor leaf, LeafHandle.Extras h) {
        try { String v = leaf.get(h); return (v == null || v.isEmpty()) ? null : v; }
        catch (Exception e) { return null; }
    }

    // -----------------------------------------------------------------------
    // Comparison helpers
    // -----------------------------------------------------------------------

    /** Read a nullable int32 field, mapping null → 0 (the verifier's sentinel for null ints). */
    private static int readNullable(LeafAccessor leaf, LeafHandle.Int32 h) {
        return leaf.isNull(h) ? 0 : leaf.get(h);
    }

    /** Read a nullable float64 field, mapping null → NaN (the verifier's sentinel for null doubles). */
    private static double readNullableD(LeafAccessor leaf, LeafHandle.Float64 h) {
        return leaf.isNull(h) ? Double.NaN : leaf.get(h);
    }

    private static int cmp(String f, int exp, int act, String key, List<String> errors) {
        if (exp == act) return 0;
        if (errors.size() < 20) errors.add(f + ": exp=" + exp + " act=" + act + " | " + key);
        return 1;
    }

    private static int cmpD(String f, double exp, double act, String key, List<String> errors) {
        if (Double.isNaN(exp) && Double.isNaN(act)) return 0;
        if (!Double.isNaN(exp) && !Double.isNaN(act) && Math.abs(exp - act) <= 0.0001) return 0;
        if (errors.size() < 20) errors.add(f + ": exp=" + exp + " act=" + act + " | " + key);
        return 1;
    }

    private static int cmpS(String f, String exp, String act, String key, List<String> errors) {
        if (Objects.equals(exp, act)) return 0;
        if (errors.size() < 20) {
            String e = exp != null && exp.length() > 40 ? exp.substring(0, 40) + "..." : exp;
            String a = act != null && act.length() > 40 ? act.substring(0, 40) + "..." : act;
            errors.add(f + ": exp=[" + e + "] act=[" + a + "] | " + key);
        }
        return 1;
    }

    /**
     * Independent duplicate of the "is newer" comparison — does NOT delegate to
     * {@link GbifTracker#isNewer} so that bugs in GbifTracker are detectable.
     */
    private static boolean isNewer(int y, int m, int d, double lat, double lon,
                                   int taxon, int species, int ind,
                                   String loc, String rec, String ext,
                                   int ey, int em, int ed, double elat, double elon,
                                   int etaxon, int especies, int eind,
                                   String eloc, String erec, String eext) {
        if (y != ey) return y > ey;
        if (m != em) return m > em;
        if (d != ed) return d > ed;
        int c = Double.compare(lat, elat);
        if (c != 0) return c > 0;
        c = Double.compare(lon, elon);
        if (c != 0) return c > 0;
        if (taxon != etaxon) return taxon > etaxon;
        if (species != especies) return species > especies;
        if (ind != eind) return ind > eind;
        c = compareNullable(loc, eloc);
        if (c != 0) return c > 0;
        c = compareNullable(rec, erec);
        if (c != 0) return c > 0;
        return compareNullable(ext, eext) > 0;
    }

    private static int compareNullable(String a, String b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        return a.compareTo(b);
    }
}
