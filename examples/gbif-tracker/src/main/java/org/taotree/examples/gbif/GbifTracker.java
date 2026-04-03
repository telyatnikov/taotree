package org.taotree.examples.gbif;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.taotree.*;

import java.io.File;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * GBIF Species Observation Tracker — demonstrates TaoTree with real biodiversity data.
 *
 * <p>Reads GBIF Parquet occurrence files using <a href="https://hardwood.dev">Hardwood</a>
 * and indexes species observations into a {@link TaoTree} with dictionary-encoded key
 * fields. Tracks observation state per (taxonomy, location) pair and reports
 * FORWARD/SUPPRESS statistics.
 *
 * <p>Usage: {@code java -cp ... org.taotree.examples.gbif.GbifTracker <parquet-dir-or-file>}
 */
public class GbifTracker {

    // Leaf value layout: [witness:8B][count:4B] + 12B padding = 24B
    private static final int VALUE_SIZE = 24;
    private static final long OFF_WITNESS = 0;  // long: FNV-1a hash of field values
    private static final long OFF_COUNT   = 8;  // int:  observation count

    // Key layout: 16 bytes total
    // [kingdom:2][phylum:2][family:2][species:4][country:2][state:2][field:2]
    private static final int KEY_LEN = 16;

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

        try (var tree = TaoTree.open(KEY_LEN, VALUE_SIZE, 4 * 1024 * 1024)) {

            // Dictionaries — intern variable-length strings into compact integer codes
            var kingdomCodes = TaoDictionary.u16(tree);
            var phylumCodes  = TaoDictionary.u16(tree);
            var familyCodes  = TaoDictionary.u16(tree);
            var speciesCodes = TaoDictionary.u32(tree);
            var countryCodes = TaoDictionary.u16(tree);
            var stateCodes   = TaoDictionary.u16(tree);
            var fieldCodes   = TaoDictionary.u16(tree);

            var layout = KeyLayout.of(
                KeyField.dict16("kingdom", kingdomCodes),
                KeyField.dict16("phylum",  phylumCodes),
                KeyField.dict16("family",  familyCodes),
                KeyField.dict32("species", speciesCodes),
                KeyField.dict16("country", countryCodes),
                KeyField.dict16("state",   stateCodes),
                KeyField.dict16("field",   fieldCodes)
            );

            try (var arena = Arena.ofConfined()) {
                var keyBuilder = new KeyBuilder(layout, arena);

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

                            String kingdom = rows.isNull("kingdom")  ? null : rows.getString("kingdom");
                            String phylum  = rows.isNull("phylum")   ? null : rows.getString("phylum");
                            String family  = rows.isNull("family")   ? null : rows.getString("family");
                            String species = rows.isNull("species")  ? null : rows.getString("species");
                            String country = rows.isNull("countrycode")    ? null : rows.getString("countrycode");
                            String state   = rows.isNull("stateprovince")  ? null : rows.getString("stateprovince");

                            if (species == null) { skipped++; continue; }

                            // Build the compound key (setDict interns strings via dictionary)
                            keyBuilder.setDict("kingdom", kingdom)
                                      .setDict("phylum",  phylum)
                                      .setDict("family",  family)
                                      .setDict("species", species)
                                      .setDict("country", country)
                                      .setDict("state",   state)
                                      .setDict("field",   "header");

                            try (var w = tree.write()) {
                                long leaf = w.getOrCreate(keyBuilder.key());
                                MemorySegment value = w.leafValue(leaf);

                                long witness = computeWitness(kingdom, phylum, family, species,
                                                              country, state);
                                long existingWitness = value.get(ValueLayout.JAVA_LONG_UNALIGNED, OFF_WITNESS);

                                if (existingWitness == 0) {
                                    value.set(ValueLayout.JAVA_LONG_UNALIGNED, OFF_WITNESS, witness);
                                    value.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_COUNT, 1);
                                    forwarded++;
                                } else if (existingWitness != witness) {
                                    value.set(ValueLayout.JAVA_LONG_UNALIGNED, OFF_WITNESS, witness);
                                    int count = value.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_COUNT);
                                    value.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_COUNT, count + 1);
                                    forwarded++;
                                } else {
                                    int count = value.get(ValueLayout.JAVA_INT_UNALIGNED, OFF_COUNT);
                                    value.set(ValueLayout.JAVA_INT_UNALIGNED, OFF_COUNT, count + 1);
                                    suppressed++;
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
                System.out.printf("Forwarded:      %,d (new or changed)%n", forwarded);
                System.out.printf("Suppressed:     %,d (duplicate)%n", suppressed);
                System.out.printf("Skipped:        %,d (no species)%n", skipped);
                try (var r = tree.read()) {
                    System.out.printf("ART entries:    %,d%n", r.size());
                }
                System.out.printf("Elapsed:        %.2f sec%n", elapsedSec);
                System.out.printf("Throughput:     %,.0f rows/sec%n", totalRows / elapsedSec);
                System.out.println();
                System.out.println("=== Dictionaries ===");
                System.out.printf("  kingdom:  %,d entries%n", kingdomCodes.size());
                System.out.printf("  phylum:   %,d entries%n", phylumCodes.size());
                System.out.printf("  family:   %,d entries%n", familyCodes.size());
                System.out.printf("  species:  %,d entries%n", speciesCodes.size());
                System.out.printf("  country:  %,d entries%n", countryCodes.size());
                System.out.printf("  state:    %,d entries%n", stateCodes.size());
                System.out.printf("  field:    %,d entries%n", fieldCodes.size());
                System.out.println();
                System.out.println("=== Memory ===");
                System.out.printf("  SlabAllocator:  %,.1f MB (%,d segments in use)%n",
                    tree.totalSlabBytes() / (1024.0 * 1024),
                    tree.totalSegmentsInUse());
                System.out.printf("  BumpAllocator: %,.1f MB (%,d pages)%n",
                    tree.totalOverflowBytes() / (1024.0 * 1024),
                    tree.overflowPageCount());
            }
        }
    }

    private static long computeWitness(String... parts) {
        long hash = 0xcbf29ce484222325L; // FNV-1a offset basis
        for (String part : parts) {
            if (part != null) {
                byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
                for (byte b : bytes) {
                    hash ^= (b & 0xFF);
                    hash *= 0x100000001b3L; // FNV-1a prime
                }
            }
            hash ^= 0xFF;
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
