# TaoTree

**Off-heap, file-backed, always-temporal entity store for Java.**

TaoTree stores entities (addressed by a typed composite key) with named
attributes whose values are versioned over time. Every `put` optionally
carries a timestamp; every `get` optionally asks "as of when?". All data
lives off-heap in mmap-backed slab and bump allocators — no GC pressure on
the hot path.

> **What this library is:** a temporal, attribute-keyed store with a fixed
> composite-key schema, dictionary-encoded string keys, and persistent
> copy-on-write history.
>
> **What this library is not:** a general-purpose Adaptive Radix Tree with
> user-defined leaf layouts. Earlier versions exposed `LeafLayout` /
> `LeafField` / `LeafAccessor` etc. for arbitrary struct leaves; those
> were removed in the temporal-unification refactor. The ART node types
> are still the internal index structure, but they are not part of the
> public API.

## Quick start

```java
// Schema: composite key only — values are schemaless (tagged Value union).
var keyLayout = KeyLayout.of(
    KeyField.dict16("kingdom"),
    KeyField.dict32("species"),
    KeyField.uint32("year")
);

try (var tree = TaoTree.create(Path.of("data.tao"), keyLayout);
     var arena = Arena.ofConfined()) {

    // Typed key handles (pre-computed offsets, compile-time safe).
    var KINGDOM = tree.keyDict16("kingdom");
    var SPECIES = tree.keyDict32("species");
    var YEAR    = tree.keyUint32("year");

    var kb = tree.newKeyBuilder(arena)
        .set(KINGDOM, "Animalia")
        .set(SPECIES, "Haliaeetus leucocephalus")
        .set(YEAR, 2024);

    // Write — put(path, attr, Value) or put(path, attr, Value, ts).
    try (var w = tree.write()) {
        w.put(kb, "count",    Value.ofInt(1));                  // timeless
        w.put(kb, "lat",      Value.ofDouble(48.5));            // timeless
        w.put(kb, "locality", Value.ofString("Lake Tahoe"));    // timeless
        w.put(kb, "count",    Value.ofInt(12), 1_700_000_000L); // temporal
    }

    // Read — latest / point-in-time / full history.
    try (var r = tree.read()) {
        Value latest = r.get(kb, "count");                       // -> 12
        Value atT    = r.getAt(kb, "count", 1_600_000_000L);     // -> 1
        Map<String, Value> all = r.getAll(kb);
        r.history(kb, "count", (firstSeen, lastSeen, validTo, v) -> {
            // visit each AttributeRun in time order
            return true;
        });
    }

    // Prefix scan — all entities with kingdom=Animalia, all years.
    var qb = tree.newQueryBuilder(arena).set(KINGDOM, "Animalia");
    try (var r = tree.read()) {
        r.scan(qb, KINGDOM, entityKey -> {
            // Per-attr reads via r.get(...) / r.getAll(...) using entityKey.
            return true;
        });
    }
}
```

### Reopen

```java
// Supply the same KeyLayout used at create time; TaoTree verifies the
// schema fingerprint stored in the checkpoint's SchemaBinding section and
// throws on mismatch.
try (var tree = TaoTree.open(Path.of("data.tao"), keyLayout)) { ... }

// Or let TaoTree reconstruct the layout from the stored binding:
try (var tree = TaoTree.open(Path.of("data.tao"))) { ... }
```

## Concepts

| Concept | What it is |
| --- | --- |
| **Entity** | A row addressed by a composite `KeyLayout` key. |
| **Attribute** | A named field on an entity. Name → uint32 dictionary code. |
| **Value** | Tagged union: `int`, `long`, `float`, `double`, `bool`, `string`, `bytes`, `json`, `null`. Payloads ≤ 12 B are inlined; larger payloads overflow into a bump allocator. |
| **Timestamp** | Epoch ms, or the sentinel `TIMELESS = 0L` (a fact with no time dimension). |
| **AttributeRun** | `[first_seen, last_seen, valid_to, value_ref]` — a contiguous interval during which one attribute held one value. Canonical write layer. |
| **EntityVersion** | `[first_seen, valid_to, state_root]` — a CHAMP snapshot of the whole entity state at a point in time. Materialized lazily as a read index. |
| **Dictionary** | `TaoDictionary`: string ↔ uint32 intern table, backed by a nested ART. Used for `dict16`/`dict32` key fields and attribute names. |

## Features

- **Always temporal.** `put(..., ts)` appends to history; `put(...)` without `ts` is a timeless fact. First `put(..., ts)` on a previously-timeless attribute caps the timeless run at `ts`.
- **Off-heap storage.** All index and value bytes in `MemorySegment`s; slab allocator for fixed-size nodes, bump allocator for variable-length overflow, `ChunkStore` for single-file mmap persistence.
- **Crash-safe.** Checkpoint v3 with mirrored A/B slots, CRC-32C, shadow-page commit protocol. `sync()` is cheap (one commit record + dirty chunks); full checkpoint every N commits.
- **Concurrent.** ROWEX: lock-free epoch-pinned readers never block on writers; writers run optimistic COW outside any lock, then take a short commit lock to publish. On conflict, one bounded redo against the newer root using a per-scope `TemporalOpLog` to replay logical put/delete ops — sibling-attribute updates from other writers are preserved.
- **Concurrency tested.** Fray (schedule-point instrumentation, ~1 min), Lincheck (linearizability stress). Some paths of the slab allocator use manual `Thread.yield()` schedule points because Fray cannot instrument VarHandle on off-heap `MemorySegment`.

## Build

Requires **Java 25+** (FFM API + `--enable-native-access=ALL-UNNAMED`).

```bash
./gradlew :core:test                   # all tests (forks per class)
./gradlew :core:frayTest               # Fray concurrency tests only (~1 min)
./gradlew :core:pitest                 # mutation testing (≥70% threshold)
./gradlew :core:jacocoTestReport       # line coverage
```

## RAM disk for tests (optional, faster)

- **macOS:** `./ramdisk.sh up` creates `/Volumes/RAMDisk`, auto-detected by tests.
- **Linux:** `/dev/shm` is auto-detected.
- **Custom:** `./gradlew :core:test -PtestTmpDir=/my/path`.

## Project layout

```
taotree/
  core/         ART + temporal core, public API in org.taotree{,layout}
  examples/
    gbif-tracker/    real-world GBIF species dataset demo
  jmh/          micro-benchmarks
  docs/
    design.md   full design (see §18 for the temporal layer)
```

## Public API surface

- `org.taotree` — `TaoTree`, `Value`, `EntityVisitor`, `ValueHistoryVisitor`, `TaoKey`, `TaoDictionary`, `TaoString` (internal; package-private class, referenced only for value-payload helpers).
- `org.taotree.layout` — `KeyLayout`, `KeyField`, `KeyHandle`, `KeyBuilder`, `QueryBuilder`.

All other types (ART nodes, CHAMP, CowEngine, Compactor, PersistenceManager, allocators, TemporalWriter/Reader) are internal under `org.taotree.internal.*`.

## Design document

See [docs/design.md](docs/design.md). Sections 0–17 cover the original ART / persistence / concurrency design; section 18 describes the temporal layer that is now the public API. The §18.1 status table tracks which parts are implemented; known implementation-vs-design pivots (inline 40-byte `EntityNode` instead of an 8-byte `EntityNodeRef`, `Value` instead of raw `byte[]`, unified temporal v3 instead of v2 regular/temporal) are called out at the top of §18.
