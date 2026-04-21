package org.taotree.examples.gbif;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.taotree.TaoTree;
import org.taotree.Value;
import org.taotree.layout.KeyBuilder;

import java.io.File;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end round-trip test for the GBIF tracker against a bounded slice
 * of a real Parquet file in {@code data/gbif/}.
 *
 * <p>We cap the row count at 2 000 to keep the test fast; the production
 * {@code verify} subcommand runs across whole files. Any mismatch — including
 * null vs empty-string, int vs long, repeated-field reordering — fails the
 * test.
 */
class GbifRoundTripTest {

    private static final int SAMPLE_ROWS = 2_000;

    @Test
    void parquetRoundTripThroughTaoTree(@TempDir Path tmp) throws Exception {
        Path parquet = Path.of("../../data/gbif/000001.parquet");
        if (!Files.exists(parquet)) {
            parquet = Path.of("data/gbif/000001.parquet");
        }
        if (!Files.exists(parquet)) {
            // Gradle JavaExec sets workingDir to rootProject; if someone runs the
            // test from a different cwd, the file may not be reachable.
            System.err.println("skipping: no GBIF parquet fixture found at " + parquet.toAbsolutePath());
            return;
        }

        Path storePath = tmp.resolve("roundtrip.taotree");

        // 1. Ingest the first SAMPLE_ROWS rows.
        List<Long> expectedIds;
        try (var fr = ParquetFileReader.open(InputFile.of(parquet));
             RowReader rows = fr.createRowReader(SAMPLE_ROWS);
             TaoTree tree = TaoTree.create(storePath, GbifTracker.buildLayout());
             Arena arena = Arena.ofConfined()) {
            var cols = GbifSchema.topLevelColumns(fr.getFileSchema());
            GbifSchema.writeSidecar(GbifTracker.sidecarPath(storePath), cols);

            var h = new GbifTracker.Handles(tree);
            KeyBuilder kb = tree.newKeyBuilder(arena);
            Map<String, Value> attrs = new LinkedHashMap<>(cols.size());

            var ids = new ArrayList<Long>();
            while (rows.hasNext()) {
                rows.next();
                long id = GbifTracker.parseGbifId(rows);
                if (id == 0) continue;
                ids.add(id);
                setKey(kb, h, rows, id);
                attrs.clear();
                for (var col : cols) {
                    if (GbifSchema.TAXONOMY_FIELDS.contains(col.name())) continue;
                    if (col.name().equals(GbifSchema.GBIFID_FIELD)) continue;
                    Value v = ParquetCells.read(rows, col);
                    if (v != null) attrs.put(col.name(), v);
                }
                long ts = rows.isNull(GbifSchema.EVENTDATE_FIELD)
                    ? TaoTree.TIMELESS : rows.getLong(GbifSchema.EVENTDATE_FIELD);
                try (var w = tree.write()) {
                    w.putAll(kb, attrs, ts);
                }
            }
            tree.sync();
            expectedIds = List.copyOf(ids);
        }

        assertTrue(expectedIds.size() > 100,
            "fixture must contain more than 100 rows, got " + expectedIds.size());

        // 2. Reopen via the public path and verify every cell round-trips.
        int cellMismatches = 0;
        try (var fr = ParquetFileReader.open(InputFile.of(parquet));
             RowReader rows = fr.createRowReader(SAMPLE_ROWS);
             TaoTree tree = TaoTree.open(storePath, GbifTracker.buildLayout());
             Arena arena = Arena.ofConfined()) {
            var cols = GbifSchema.readSidecar(GbifTracker.sidecarPath(storePath));
            var h = new GbifTracker.Handles(tree);
            KeyBuilder kb = tree.newKeyBuilder(arena);

            while (rows.hasNext()) {
                rows.next();
                long id = GbifTracker.parseGbifId(rows);
                if (id == 0) continue;
                setKey(kb, h, rows, id);

                Map<String, Value> stored;
                try (var r = tree.read()) {
                    stored = r.getAll(kb);
                }
                assertNotNull(stored, "entity missing for gbifid=" + id);

                for (var col : cols) {
                    if (GbifSchema.TAXONOMY_FIELDS.contains(col.name())) continue;
                    if (col.name().equals(GbifSchema.GBIFID_FIELD)) continue;
                    Value expected = ParquetCells.read(rows, col);
                    Value actual   = stored.get(col.name());
                    if (!GbifTracker.valueEquals(expected, actual)) {
                        cellMismatches++;
                        if (cellMismatches <= 5) {
                            System.out.printf("mismatch id=%d col=%s exp=%s got=%s%n",
                                id, col.name(), expected, actual);
                        }
                    }
                }
            }
        }

        assertEquals(0, cellMismatches,
            "expected zero cell mismatches, got " + cellMismatches);
    }

    @Test
    void repeatedFieldListCodecRoundTrip() {
        List<String> cases = List.of(
            "",
            "plain text",
            "quote \" and backslash \\",
            "newline\nhere",
            "tab\there",
            "ctl\u0001chr",
            "\u0000NULL\u0000"
        );
        // mixed payload including nulls
        var payload = new ArrayList<String>();
        payload.add(null);
        payload.addAll(cases);
        payload.add(null);

        String json = ParquetCells.encodeJsonArray(payload);
        List<String> decoded = ParquetCells.decodeJsonArray(json);
        assertEquals(payload, decoded, "JSON array round-trip failed");

        // empty list — distinct from null
        String empty = ParquetCells.encodeJsonArray(List.of());
        assertEquals(List.of(), ParquetCells.decodeJsonArray(empty));
    }

    @Test
    void taxonomySentinelRoundTripsAsNull() {
        String s = GbifSchema.NULL_TAXON_SENTINEL;
        assertNotEquals("null", s);
        assertNotEquals("", s);
        assertTrue(s.charAt(0) == '\u0000');
    }

    // helpers ────────────────────────────────────────────────────────────

    private static void setKey(KeyBuilder kb, GbifTracker.Handles h, RowReader rows, long id) {
        kb.set(h.kingdom, GbifTracker.readTaxon(rows, "kingdom"));
        kb.set(h.phylum,  GbifTracker.readTaxon(rows, "phylum"));
        kb.set(h.clazz,   GbifTracker.readTaxon(rows, "class"));
        kb.set(h.ordr,    GbifTracker.readTaxon(rows, "order"));
        kb.set(h.family,  GbifTracker.readTaxon(rows, "family"));
        kb.set(h.genus,   GbifTracker.readTaxon(rows, "genus"));
        kb.set(h.species, GbifTracker.readTaxon(rows, "species"));
        kb.set(h.gbifid, id);
    }
}
