# GBIF Species Observation Tracker

Example application for [TaoTree](../..) demonstrating the unified temporal
API on the real-world [GBIF Simple](https://www.gbif.org/data-quality-requirements-occurrences)
Parquet occurrence dataset.

## Approach B — Taxonomy-prefix + temporal observations

This example implements **Approach B** from `plan.md` — the richest of the
three designs sketched for this dataset. It uses the full TaoTree feature
set:

| TaoTree feature             | How the example uses it |
|-----------------------------|-------------------------|
| Dictionary-encoded key      | 7 taxonomy fields + `gbifid` |
| Ordered range scan          | "All *Animalia/Chordata* in Austria …" |
| Per-entity CHAMP map        | 39 non-taxonomy columns as attributes per occurrence |
| Temporal history axis       | `eventdate` epoch millis; missing dates → `TIMELESS` |
| Lossless `Value` round-trip | Every Parquet cell restorable byte-for-byte |

## Key layout (26 bytes)

```
kingdom : dict16   (2 B)
phylum  : dict16   (2 B)
class   : dict16   (2 B)   — field named "clazz" ("class" is a Java keyword)
order   : dict16   (2 B)   — field named "ordr"  (avoid SQL-keyword confusion)
family  : dict16   (2 B)
genus   : dict32   (4 B)
species : dict32   (4 B)
gbifid  : uint64   (8 B)
```

Null taxonomic ranks round-trip through a reserved sentinel
(`\u0000NULL\u0000`) interned in each dictionary.

## Attributes

All remaining 39 GBIF Simple columns are stored as CHAMP attributes on the
entity, carrying their original Parquet type tag in the `Value`:

* scalars — int32 / int64 / float64 / utf8 / bytes;
* repeated columns (`identifiedby`, `recordedby`, `typestatus`, `mediatype`,
  `issue`) — encoded as a compact JSON array so element order is preserved;
* Parquet `NULL` cells are *not written*; on readback `getAll` omits them,
  which is distinct from "empty string".

## Reversibility contract

The example is fully round-trippable: from the `.taotree` store plus its
`.schema.json` sidecar (column order + types), every original Parquet cell
can be reconstructed exactly. The `verify` subcommand enforces this against
the source Parquet files:

```
$ ./gradlew :examples:gbif-tracker:verify
rows=306,000  checked=306,000  skipped(no-id)=0  cell-mismatches=0
  ✓ reversible round-trip verified
```

## CLI subcommands

```bash
# ingest every .parquet under data/gbif/ into build/gbif/gbif.taotree
./gradlew :examples:gbif-tracker:ingest

# verify the ingested store round-trips cell-for-cell against data/gbif/
./gradlew :examples:gbif-tracker:verify

# profiler run (JFR recording in build/reports/profiling/)
./gradlew :examples:gbif-tracker:ingestJfr

# ad-hoc subcommands (query, stats)
./gradlew :examples:gbif-tracker:run --args="stats build/gbif/gbif.taotree"
./gradlew :examples:gbif-tracker:run --args="query build/gbif/gbif.taotree Animalia Chordata"
```

## Designs we did not implement

Two alternatives were considered in `plan.md`:

* **Approach A — Row-as-entity flat archive.** Key = `gbifid` alone; all 46
  columns as attributes. Simplest possible model, trivially reversible, but
  exercises none of TaoTree's prefix/temporal strengths.
* **Approach C — Two linked trees (dimension + fact).** Separate `taxa`
  tree holding deduplicated species metadata; `occurrences` tree holding
  observations with a foreign key into the taxa tree. Most space-efficient
  but requires a join on every readback.

Either can be layered on top of the shared infrastructure here
(`GbifSchema`, `ParquetCells`) without re-doing the Parquet ingestion
plumbing.
