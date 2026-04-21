package org.taotree.examples.gbif;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.taotree.TaoTree;
import org.taotree.Value;
import org.taotree.layout.KeyBuilder;
import org.taotree.layout.KeyField;
import org.taotree.layout.KeyHandle;
import org.taotree.layout.KeyLayout;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * GBIF Species Observation Tracker — Approach B (taxonomy-prefix + temporal).
 *
 * <p>This example demonstrates the non-trivial capabilities of a unified
 * temporal TaoTree on a real-world dataset:
 * <ul>
 *   <li>7-field taxonomic prefix key → ordered range scans across taxonomy
 *       (e.g. all Mammalia within a country);</li>
 *   <li>per-entity CHAMP maps → non-taxonomic cells attached to each
 *       occurrence as attributes;</li>
 *   <li>observation timestamp ({@code eventdate}) → temporal history, lets
 *       queries travel back in time for the same entity;</li>
 *   <li>{@link dev.hardwood} Parquet reader → real ingestion from the GBIF
 *       Simple download format.</li>
 * </ul>
 *
 * <p>The example is <em>fully reversible</em>: every cell of every input
 * Parquet row is restorable from the TaoTree store plus a small
 * {@code schema.json} sidecar.
 *
 * <h3>Subcommands</h3>
 * <ul>
 *   <li>{@code ingest &lt;parquet-dir&gt; &lt;store.taotree&gt;} — read Parquet files
 *       and populate a new store.</li>
 *   <li>{@code verify &lt;parquet-dir&gt; &lt;store.taotree&gt;} — re-read the
 *       Parquet files, reconstruct every row from the store, and assert
 *       every cell round-trips exactly.</li>
 *   <li>{@code query &lt;store.taotree&gt; &lt;kingdom&gt; [phylum] [class] …}
 *       — range-scan the store by a taxonomic prefix and print matches.</li>
 *   <li>{@code stats &lt;store.taotree&gt;} — print tree size, dictionary
 *       sizes, a few sample entities.</li>
 * </ul>
 */
public final class GbifTracker {

    private GbifTracker() {}

    // ── Entry point ──────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        if (args.length == 0) { usage(); return; }
        String cmd = args[0];
        switch (cmd) {
            case "ingest" -> {
                requireArgs(args, 3, "ingest <parquet-dir> <store.taotree>");
                ingest(new File(args[1]), Path.of(args[2]));
            }
            case "verify" -> {
                requireArgs(args, 3, "verify <parquet-dir> <store.taotree>");
                verify(new File(args[1]), Path.of(args[2]));
            }
            case "query" -> {
                if (args.length < 3) {
                    System.err.println("query <store.taotree> <kingdom> [phylum] [class] [order] [family] [genus] [species]");
                    return;
                }
                Path store = Path.of(args[1]);
                String[] levels = Arrays.copyOfRange(args, 2, args.length);
                query(store, levels);
            }
            case "stats" -> {
                requireArgs(args, 2, "stats <store.taotree>");
                stats(Path.of(args[1]));
            }
            default -> { usage(); }
        }
    }

    private static void usage() {
        System.out.println("""
            TaoTree GBIF Tracker (Approach B: taxonomy-prefix + temporal)
            Usage:
              ingest <parquet-dir>   <store.taotree>
              verify <parquet-dir>   <store.taotree>
              query  <store.taotree> <kingdom> [phylum] ... [species]
              stats  <store.taotree>
            """);
    }

    private static void requireArgs(String[] args, int n, String help) {
        if (args.length < n) { System.err.println(help); System.exit(2); }
    }

    // ── Key layout ───────────────────────────────────────────────────────

    /**
     * Build the 7-taxonomy + gbifid key layout.
     *
     * Widths:
     * <ul>
     *   <li>kingdom, phylum, class, order, family — dict16 (2 B each) →
     *       supports up to 65 534 distinct values each (GBIF has ~7 kingdoms,
     *       ~80 phyla, ~300 classes, ~1 200 orders, ~6 000 families).</li>
     *   <li>genus, species — dict32 (4 B each) → covers GBIF's ~60 000 genera
     *       and ~2 000 000 species.</li>
     *   <li>gbifid — uint64 (8 B).</li>
     * </ul>
     * Total key width: 5×2 + 2×4 + 8 = 26 bytes.
     */
    static KeyLayout buildLayout() {
        return KeyLayout.of(
            KeyField.dict16("kingdom"),
            KeyField.dict16("phylum"),
            KeyField.dict16("clazz"),     // "class" is a Java keyword; dictionary stores real column name
            KeyField.dict16("ordr"),      // "order" can collide semantically; keep distinct
            KeyField.dict16("family"),
            KeyField.dict32("genus"),
            KeyField.dict32("species"),
            KeyField.uint64("gbifid")
        );
    }

    /** Handles to the 8 key fields, cached once per open-tree lifetime. */
    static final class Handles {
        final KeyHandle.Dict16 kingdom, phylum, clazz, ordr, family;
        final KeyHandle.Dict32 genus, species;
        final KeyHandle.UInt64 gbifid;
        final KeyHandle[] taxonomy; // in Parquet column order (length 7)

        Handles(TaoTree t) {
            this.kingdom = t.keyDict16("kingdom");
            this.phylum  = t.keyDict16("phylum");
            this.clazz   = t.keyDict16("clazz");
            this.ordr    = t.keyDict16("ordr");
            this.family  = t.keyDict16("family");
            this.genus   = t.keyDict32("genus");
            this.species = t.keyDict32("species");
            this.gbifid  = t.keyUint64("gbifid");
            this.taxonomy = new KeyHandle[] { kingdom, phylum, clazz, ordr, family, genus, species };
        }
    }

    // ── Ingest ───────────────────────────────────────────────────────────

    static void ingest(File parquetInput, Path storePath) throws Exception {
        File[] files = resolveFiles(parquetInput);
        if (files == null || files.length == 0) return;

        // Snapshot the schema (column order) from the first file — GBIF Simple
        // files share a schema so this is safe.
        List<GbifSchema.Column> columns;
        try (var fr = ParquetFileReader.open(InputFile.of(files[0].toPath()))) {
            columns = GbifSchema.topLevelColumns(fr.getFileSchema());
        }

        System.out.println("TaoTree GBIF Tracker — ingest");
        System.out.printf("Files:     %d%n", files.length);
        System.out.printf("Store:     %s%n", storePath);
        System.out.printf("Columns:   %d (%d repeated)%n",
            columns.size(),
            (int) columns.stream().filter(GbifSchema.Column::isRepeated).count());

        Files.deleteIfExists(storePath);
        try (TaoTree tree = TaoTree.create(storePath, buildLayout())) {
            var h = new Handles(tree);

            // Which columns go into attributes (everything that is not part of
            // the taxonomic prefix, not the gbifid key suffix, and — optionally
            // — not the eventdate that we store as the timestamp axis). We
            // still keep eventdate as an attribute so Parquet null ≠ TIMELESS
            // is preserved (TIMELESS on the history axis does not tell us
            // whether the Parquet cell was null or missing).
            List<GbifSchema.Column> attrCols = columns.stream()
                .filter(c -> !GbifSchema.TAXONOMY_FIELDS.contains(c.name()))
                .filter(c -> !c.name().equals(GbifSchema.GBIFID_FIELD))
                .toList();

            long totalRows = 0, ingested = 0, skippedNoId = 0;
            long t0 = System.nanoTime();

            for (File file : files) {
                try (var fr = ParquetFileReader.open(InputFile.of(file.toPath()));
                     RowReader rows = fr.createRowReader();
                     Arena arena = Arena.ofConfined()) {
                    KeyBuilder kb = tree.newKeyBuilder(arena);
                    Map<String, Value> attrs = new LinkedHashMap<>(attrCols.size());

                    while (rows.hasNext()) {
                        rows.next();
                        totalRows++;

                        long gbifId = parseGbifId(rows);
                        if (gbifId == 0) { skippedNoId++; continue; }

                        // Build the key from the 7 taxonomy fields + gbifid.
                        setDict(kb, h.kingdom, readTaxon(rows, "kingdom"));
                        setDict(kb, h.phylum,  readTaxon(rows, "phylum"));
                        setDict(kb, h.clazz,   readTaxon(rows, "class"));
                        setDict(kb, h.ordr,    readTaxon(rows, "order"));
                        setDict(kb, h.family,  readTaxon(rows, "family"));
                        setDictU32(kb, h.genus,   readTaxon(rows, "genus"));
                        setDictU32(kb, h.species, readTaxon(rows, "species"));
                        kb.set(h.gbifid, gbifId);

                        // Gather attributes.
                        attrs.clear();
                        for (GbifSchema.Column col : attrCols) {
                            Value v = ParquetCells.read(rows, col);
                            if (v != null) attrs.put(col.name(), v);
                        }

                        // Timestamp axis: eventdate, or TIMELESS when NULL.
                        long ts = rows.isNull(GbifSchema.EVENTDATE_FIELD)
                            ? TaoTree.TIMELESS
                            : rows.getLong(GbifSchema.EVENTDATE_FIELD);

                        try (var w = tree.write()) {
                            w.putAll(kb, attrs, ts);
                        }
                        ingested++;

                        if (ingested % 50_000 == 0) {
                            System.out.printf("  %,d rows ingested (%.1fs)%n",
                                ingested, (System.nanoTime() - t0) / 1e9);
                        }
                    }
                }
            }

            tree.sync();
            double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("%nDone. %,d ingested, %,d skipped (no gbifid) of %,d rows in %.1fs (%,.0f rows/s)%n",
                ingested, skippedNoId, totalRows, secs, ingested / secs);

            // Write the schema sidecar next to the store for standalone reverse-reads.
            Path sidecar = sidecarPath(storePath);
            GbifSchema.writeSidecar(sidecar, columns);
            System.out.printf("Schema sidecar: %s%n", sidecar);
        }
    }

    // ── Verify (reversibility check) ─────────────────────────────────────

    static void verify(File parquetInput, Path storePath) throws Exception {
        File[] files = resolveFiles(parquetInput);
        if (files == null || files.length == 0) return;

        List<GbifSchema.Column> columns = GbifSchema.readSidecar(sidecarPath(storePath));
        List<GbifSchema.Column> attrCols = columns.stream()
            .filter(c -> !GbifSchema.TAXONOMY_FIELDS.contains(c.name()))
            .filter(c -> !c.name().equals(GbifSchema.GBIFID_FIELD))
            .toList();

        System.out.println("TaoTree GBIF Tracker — verify");
        System.out.printf("Files:     %d%n", files.length);
        System.out.printf("Store:     %s%n", storePath);

        long totalRows = 0, checked = 0, mismatches = 0, skippedNoId = 0;
        try (TaoTree tree = TaoTree.open(storePath, buildLayout());
             Arena arena = Arena.ofConfined()) {
            var h = new Handles(tree);
            KeyBuilder kb = tree.newKeyBuilder(arena);

            for (File file : files) {
                try (var fr = ParquetFileReader.open(InputFile.of(file.toPath()));
                     RowReader rows = fr.createRowReader()) {
                    while (rows.hasNext()) {
                        rows.next();
                        totalRows++;

                        long gbifId = parseGbifId(rows);
                        if (gbifId == 0) { skippedNoId++; continue; }

                        // Rebuild the same key used during ingest.
                        setDict(kb, h.kingdom, readTaxon(rows, "kingdom"));
                        setDict(kb, h.phylum,  readTaxon(rows, "phylum"));
                        setDict(kb, h.clazz,   readTaxon(rows, "class"));
                        setDict(kb, h.ordr,    readTaxon(rows, "order"));
                        setDict(kb, h.family,  readTaxon(rows, "family"));
                        setDictU32(kb, h.genus,   readTaxon(rows, "genus"));
                        setDictU32(kb, h.species, readTaxon(rows, "species"));
                        kb.set(h.gbifid, gbifId);

                        Map<String, Value> stored;
                        try (var r = tree.read()) {
                            stored = r.getAll(kb);
                        }

                        for (GbifSchema.Column col : attrCols) {
                            Value expected = ParquetCells.read(rows, col);
                            Value actual   = stored.get(col.name());
                            if (!valueEquals(expected, actual)) {
                                mismatches++;
                                if (mismatches <= 8) {
                                    System.out.printf("  mismatch gbifid=%d col=%s expected=%s actual=%s%n",
                                        gbifId, col.name(), expected, actual);
                                }
                            }
                        }
                        checked++;
                    }
                }
            }
        }

        System.out.printf("%nrows=%,d  checked=%,d  skipped(no-id)=%,d  cell-mismatches=%,d%n",
            totalRows, checked, skippedNoId, mismatches);
        if (mismatches == 0) System.out.println("  ✓ reversible round-trip verified");
        else System.exit(1);
    }

    // ── Query: prefix scan by taxonomic levels ───────────────────────────

    static void query(Path storePath, String[] levels) throws Exception {
        try (TaoTree tree = TaoTree.open(storePath, buildLayout());
             Arena arena = Arena.ofConfined()) {
            var h = new Handles(tree);
            KeyBuilder prefix = tree.newKeyBuilder(arena);

            // Set as many taxonomic fields as provided.
            KeyHandle lastHandle = null;
            for (int i = 0; i < levels.length && i < h.taxonomy.length; i++) {
                KeyHandle handle = h.taxonomy[i];
                if (handle instanceof KeyHandle.Dict16 d) {
                    prefix.set(d, levels[i]);
                } else if (handle instanceof KeyHandle.Dict32 d) {
                    prefix.set(d, levels[i]);
                }
                lastHandle = handle;
            }
            if (lastHandle == null) throw new IllegalArgumentException("provide at least a kingdom");

            final KeyHandle up = lastHandle;
            System.out.printf("Prefix scan under %s=%s%s%n",
                String.join("/", Arrays.stream(h.taxonomy)
                    .limit(levels.length).map(KeyHandle::name).toList()),
                String.join("/", levels),
                levels.length == h.taxonomy.length ? " (full path)" : "");

            // Resolve dictionaries once for pretty-printing key bytes.
            int[] count = {0};
            try (var r = tree.read()) {
                r.scan(prefix, up, entityKey -> {
                    count[0]++;
                    if (count[0] <= 20) {
                        String decoded = decodeKey(entityKey, h);
                        System.out.println("  " + decoded);
                    }
                    return true;
                });
            }
            System.out.printf("matches=%,d%n", count[0]);
        }
    }

    // ── Stats ────────────────────────────────────────────────────────────

    static void stats(Path storePath) throws Exception {
        try (TaoTree tree = TaoTree.open(storePath, buildLayout());
             Arena arena = Arena.ofConfined()) {
            var h = new Handles(tree);
            long size;
            try (var r = tree.read()) { size = r.size(); }
            System.out.printf("entities:       %,d%n", size);
            System.out.printf("dictionaries:   %d%n", tree.dictionaries().size());
            for (int i = 0; i < tree.dictionaries().size(); i++) {
                var d = tree.dictionaries().get(i);
                System.out.printf("  [%d] size=%,d%n", i, d.size());
            }
            System.out.println("sample entities:");
            try (var r = tree.read()) {
                int[] n = {0};
                r.forEach(key -> {
                    if (n[0]++ < 5) System.out.println("  " + decodeKey(key, h));
                    return n[0] < 5;
                });
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    static long parseGbifId(RowReader rows) {
        if (rows.isNull("gbifid")) return 0L;
        try {
            return Long.parseUnsignedLong(rows.getString("gbifid"));
        } catch (Exception ignore) { return 0L; }
    }

    static String readTaxon(RowReader rows, String col) {
        if (rows.isNull(col)) return GbifSchema.NULL_TAXON_SENTINEL;
        String s = rows.getString(col);
        return (s == null) ? GbifSchema.NULL_TAXON_SENTINEL : s;
    }

    static void setDict(KeyBuilder kb, KeyHandle.Dict16 h, String value) { kb.set(h, value); }
    static void setDictU32(KeyBuilder kb, KeyHandle.Dict32 h, String value) { kb.set(h, value); }

    static String decodeKey(byte[] key, Handles h) {
        var sb = new StringBuilder(128);
        decodeDict(sb, key, h.kingdom);   sb.append('/');
        decodeDict(sb, key, h.phylum);    sb.append('/');
        decodeDict(sb, key, h.clazz);     sb.append('/');
        decodeDict(sb, key, h.ordr);      sb.append('/');
        decodeDict(sb, key, h.family);    sb.append('/');
        decodeDict32(sb, key, h.genus);   sb.append('/');
        decodeDict32(sb, key, h.species); sb.append("  gbifid=");
        long id = 0;
        for (int i = 0; i < 8; i++) id = (id << 8) | (key[h.gbifid.offset() + i] & 0xffL);
        sb.append(Long.toUnsignedString(id));
        return sb.toString();
    }

    static void decodeDict(StringBuilder out, byte[] key, KeyHandle.Dict16 h) {
        int code = ((key[h.offset()] & 0xff) << 8) | (key[h.offset() + 1] & 0xff);
        appendTaxon(out, h.dict().reverseLookup(code));
    }

    static void decodeDict32(StringBuilder out, byte[] key, KeyHandle.Dict32 h) {
        int code = ((key[h.offset()]     & 0xff) << 24)
                 | ((key[h.offset() + 1] & 0xff) << 16)
                 | ((key[h.offset() + 2] & 0xff) << 8)
                 |  (key[h.offset() + 3] & 0xff);
        appendTaxon(out, h.dict().reverseLookup(code));
    }

    static void appendTaxon(StringBuilder out, String s) {
        if (s == null || GbifSchema.NULL_TAXON_SENTINEL.equals(s)) out.append("<null>");
        else out.append(s);
    }

    static boolean valueEquals(Value a, Value b) {
        if (a == null) a = Value.ofNull();
        if (b == null) b = Value.ofNull();
        if (a instanceof Value.Null && b instanceof Value.Null) return true;
        if (a.getClass() != b.getClass()) return false;
        return switch (a) {
            case Value.Int32 x    -> x.value() == ((Value.Int32) b).value();
            case Value.Int64 x    -> x.value() == ((Value.Int64) b).value();
            case Value.Float32 x  -> Float.compare(x.value(), ((Value.Float32) b).value()) == 0;
            case Value.Float64 x  -> Double.compare(x.value(), ((Value.Float64) b).value()) == 0;
            case Value.Bool x     -> x.value() == ((Value.Bool) b).value();
            case Value.Str x      -> x.value().equals(((Value.Str) b).value());
            case Value.Json x     -> x.value().equals(((Value.Json) b).value());
            case Value.Bytes x    -> Arrays.equals(x.value(), ((Value.Bytes) b).value());
            case Value.Null x     -> true;
        };
    }

    // ── File helpers ─────────────────────────────────────────────────────

    static File[] resolveFiles(File input) {
        if (input.isDirectory()) {
            File[] files = input.listFiles((d, n) -> n.endsWith(".parquet"));
            if (files == null || files.length == 0) {
                System.err.println("No .parquet files in " + input);
                return null;
            }
            Arrays.sort(files);
            return files;
        }
        return new File[]{input};
    }

    static Path sidecarPath(Path storePath) {
        String n = storePath.getFileName().toString();
        int dot = n.lastIndexOf('.');
        String base = (dot >= 0) ? n.substring(0, dot) : n;
        Path parent = storePath.getParent();
        return (parent == null ? Path.of(".") : parent).resolve(base + ".schema.json");
    }
}
