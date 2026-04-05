# TaoTree Agent Notes

## Build & Test Commands

```bash
./gradlew :core:test                       # run all tests (forks per class for Lincheck isolation)
./gradlew :core:test --tests "org.taotree.Lincheck*"  # Lincheck tests only
./gradlew :core:test --tests "org.taotree.ScanTest"   # scan/query tests only
./gradlew :core:pitest                     # mutation testing (≥75% threshold)
./gradlew :core:jacocoTestReport           # coverage report → core/build/reports/jacoco/
./gradlew :examples:gbif-tracker:run --args="<parquet-dir>"  # GBIF example (persistent)
```

## Key Conventions

- Java 25+ required (`--enable-native-access=ALL-UNNAMED`)
- All tests use JUnit 5 (Jupiter)
- Lincheck tests are excluded from PIT (`excludedTestClasses = "org.taotree.Lincheck*"`)
- `forkEvery = 1` isolates test classes in separate JVMs (Lincheck Arena leak mitigation)
- File-backed tests auto-detect RAMDisk at `/Volumes/RAMDisk` (macOS) or `/dev/shm` (Linux)
- File-backed trees use v2 checkpoint format (mirrored A/B slots, CRC-32C, shadow paging)
- `Superblock.java` retained as internal data model for `CheckpointIO` serialization

## Architecture Quick Reference

- Public API: `org.taotree` package — `TaoTree`, `KeyLayout`, `LeafLayout`, `KeyHandle`, `LeafHandle`, `LeafAccessor`, `KeyBuilder`, `QueryBuilder`, `LeafVisitor`, `KeyField`, `LeafField`, `TaoDictionary`, `TaoString`, `TaoKey`
- Internal: `org.taotree.internal` — `SlabAllocator`, `BumpAllocator`, `ChunkStore`, `Node4/16/48/256`, `PrefixNode`, `NodePtr`, `OverflowPtr`, `Preallocator`
- Internal (v2 persistence): `CheckpointV2`, `CheckpointIO`, `RecordHeader`, `CommitRecord`, `ShadowPagingRecovery`, `Superblock` (data model)
- Internal (COW concurrency): `CowEngine`, `EpochReclaimer`, `WriterArena`, `Compactor`

## API Patterns

- `TaoTree.open(KeyLayout, LeafLayout)` — auto-creates dicts for `KeyField.dict16/dict32`
- `TaoTree.open(Path, KeyLayout, LeafLayout)` — reopens file-backed tree, rebinds dicts by order
- `tree.activateCowMode()` — switches to ROWEX-hybrid COW (lock-free readers, concurrent writers)
- `tree.compact()` — post-order compaction: arena→slab migration, checkpoint, epoch reclamation
- `tree.keyDict16("name")` / `tree.leafInt32("name")` — derive typed handles from the tree
- `tree.newKeyBuilder(arena)` — for writes (uses `intern()`, needs write lock for dict fields)
- `tree.newQueryBuilder(arena)` — for reads (uses `resolve()`, safe under read lock)
- `r.forEach(visitor)` — full ordered scan; `r.scan(qb, handle, visitor)` — prefix scan
- `LeafVisitor` returns boolean: `true` = continue, `false` = stop early
- `LeafAccessor` in scan callbacks is reusable — do not retain references

## GBIF Example

- Store file: `<parquet-dir>/gbif-tracker.taotree` (created on first run, reopened on subsequent)
- Verification tools: `GbifVerifier` (field coverage), `GbifCrossCheck` (aggregate), `GbifFieldCheck` (field-by-field)
- All 13 leaf fields verified against Parquet source: 0 mismatches
