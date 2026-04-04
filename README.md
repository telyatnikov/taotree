# TaoTree

Adaptive Radix Tree (ART) library for Java using the Foreign Function & Memory API.
All data lives off-heap in slab allocators. Zero GC pressure on the hot path.

## Quick Start

```java
// Define schema — layouts are pure declarations
var keyLayout = KeyLayout.of(
    KeyField.dict16("kingdom"),
    KeyField.dict32("species"),
    KeyField.uint32("year")
);
var leafLayout = LeafLayout.of(
    LeafField.int32("count"),
    LeafField.float64("latitude"),
    LeafField.string("locality"),
    LeafField.json("extras")
);

// Create tree — dicts created automatically from the key layout
try (var tree = TaoTree.open(keyLayout, leafLayout)) {

    // Derive typed handles (pre-computed offsets, compile-time type safety)
    var KINGDOM  = tree.keyDict16("kingdom");
    var SPECIES  = tree.keyDict32("species");
    var YEAR     = tree.keyUint32("year");
    var COUNT    = tree.leafInt32("count");
    var LAT      = tree.leafFloat64("latitude");
    var LOCALITY = tree.leafString("locality");

    try (var arena = Arena.ofConfined()) {
        var kb = tree.newKeyBuilder(arena);

        // Write — handle-based, chainable
        kb.set(KINGDOM, "Animalia")
          .set(SPECIES, "Haliaeetus leucocephalus")
          .set(YEAR, 2024);

        try (var w = tree.write()) {
            w.getOrCreate(kb)
             .set(COUNT, 1)
             .set(LAT, 48.5)
             .set(LOCALITY, "Lake Tahoe");
        }

        // Read
        try (var r = tree.read()) {
            var leaf = r.lookup(kb);
            if (leaf != null) {
                int count  = leaf.get(COUNT);
                double lat = leaf.get(LAT);
            }
        }

        // Prefix scan — all entries where kingdom=Animalia
        var qb = tree.newQueryBuilder(arena);  // resolve-only, safe under read lock
        qb.set(KINGDOM, "Animalia");

        try (var r = tree.read()) {
            r.scan(qb, KINGDOM, leaf -> {
                int count = leaf.get(COUNT);
                return true;  // continue (false = stop early)
            });
        }
    }
}
```

### File-backed (persistent)

```java
// Create
try (var tree = TaoTree.create(Path.of("data.tao"), keyLayout, leafLayout)) {
    // ... same handle-based API ...
    tree.sync();
}
// Reopen with layout binding
try (var tree = TaoTree.open(Path.of("data.tao"), keyLayout, leafLayout)) {
    var KINGDOM = tree.keyDict16("kingdom");  // re-bound to restored dicts
    // ...
}
```

## Features

- **Typed layouts** — `KeyLayout` + `LeafLayout` define the schema; `KeyHandle` + `LeafHandle` provide pre-computed, type-safe field access (inspired by `MemoryLayout` / `VarHandle`)
- **Dictionary encoding** — `KeyField.dict16("species")` auto-creates a `TaoDictionary` that interns variable-length strings into compact integer codes
- **Prefix scan** — `scan(queryBuilder, handle, visitor)` with ordered traversal, early termination, and compressed-prefix-aware descent
- **QueryBuilder** — resolve-only key builder safe under read lock; unknown dict values produce empty scans without mutation
- **TaoString** — 16-byte German-style strings (inline ≤ 12 bytes, overflow pointer for longer)
- **JSON catch-all** — `LeafField.json("extras")` stores arbitrary JSON as a TaoString
- **Off-heap storage** — all data in `MemorySegment`, zero GC pressure
- **File persistence** — `ChunkStore` with mmap'd chunks, optional OS-native preallocation; reopen with layout binding
- **Concurrency** — fair `ReentrantReadWriteLock` with scoped access (`tree.read()` / `tree.write()`)
- **Lincheck tests** — linearizability stress tests as a safety net for future lock-free refactoring

## Build

Requires Java 25+.

```bash
./gradlew build                          # full build (core + jmh + examples)
./gradlew :core:test                     # run tests
./gradlew :core:cleanTest :core:test     # force re-run tests
./gradlew :core:pitest                   # mutation testing (STRONGER mutators, ≥75% threshold)
./gradlew :core:jacocoTestReport         # line coverage report
```

## RAM Disk for Tests (optional)

File-backed tests use `java.io.tmpdir` which defaults to the system temp directory.
For faster tests, use a RAM-backed filesystem:

**macOS:**
```bash
./ramdisk.sh up       # create RAM disk at /Volumes/RAMDisk
./gradlew :core:test  # auto-detected, tests run in RAM
./ramdisk.sh down     # tear down when done
```

**Linux:** `/dev/shm` is tmpfs by default — auto-detected, no setup needed.

**Custom path:**
```bash
./gradlew :core:test -PtestTmpDir=/my/ramdisk
```

## Project Structure

```
taotree/
  core/           — ART library (org.taotree + org.taotree.internal)
  jmh/            — JMH benchmarks
  examples/       — Example applications (GBIF species tracker)
  docs/           — Design documentation
```

## Architecture

- `TaoTree` — off-heap ART with scoped read/write access and layout-based API
- `KeyLayout` / `LeafLayout` — schema declarations for keys and leaf values
- `KeyHandle` / `LeafHandle` — pre-computed typed field accessors
- `LeafAccessor` — typed read/write access to leaf values
- `QueryBuilder` — resolve-only key builder for scan operations
- `LeafVisitor` — boolean-returning visitor for ordered scan with early termination
- `TaoDictionary` — string-to-int intern table backed by ART
- `TaoString` — 16-byte inline/overflow string representation
- `SlabAllocator` — fixed-size slab allocator (in-memory or file-backed)
- `BumpAllocator` — append-only allocator for variable-length overflow data
- `ChunkStore` — single-file storage with chunked mmap windows
- `Preallocator` — OS-native block reservation (macOS `F_PREALLOCATE`, Linux `fallocate`)

See [docs/design.md](docs/design.md) for the full design document.
