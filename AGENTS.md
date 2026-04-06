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

- Public core: `org.taotree` — `TaoTree`, `TaoDictionary`, `TaoKey`, `TaoString`, `LeafAccessor`, `LeafVisitor`
- Public layout: `org.taotree.layout` — `KeyField`, `KeyLayout`, `KeyHandle`, `KeyBuilder`, `QueryBuilder`, `LeafField`, `LeafLayout`, `LeafHandle`
- Internal ART nodes: `org.taotree.internal.art` — `Node4/16/48/256`, `PrefixNode`, `NodePtr`, `NodeConstants`
- Internal allocators: `org.taotree.internal.alloc` — `SlabAllocator`, `BumpAllocator`, `ChunkStore`, `Preallocator`, `OverflowPtr`, `WriterArena`
- Internal persistence: `org.taotree.internal.persist` — `PersistenceManager`, `CheckpointV2`, `CheckpointIO`, `RecordHeader`, `CommitRecord`, `ShadowPagingRecovery`, `Superblock`
- Internal COW: `org.taotree.internal.cow` — `CowEngine`, `CowContext`, `CowInsert`, `CowDelete`, `CowNodeOps`, `EpochReclaimer`, `Compactor`

## API Patterns

- `TaoTree.open(KeyLayout, LeafLayout)` — auto-creates dicts for `KeyField.dict16/dict32`
- `TaoTree.open(Path, KeyLayout, LeafLayout)` — reopens file-backed tree, rebinds dicts by order
- Threading: ROWEX model — lock-free readers (epoch-based snapshot), concurrent writers (optimistic COW + commit lock)
- `ReadScope` captures `PublicationState` (root + size atomically) via VarHandle acquire + enters epoch; never blocks on writers
- `WriteScope` performs optimistic COW outside any lock on the first mutation, then acquires a lightweight commit lock to publish; subsequent mutations run under the commit lock; no lock is held between `write()` and the first mutation
- On conflict (another writer published during optimistic COW): one redo COW against the new root, bounded to a single retry
- Persistence operations (`sync()`/`compact()`/`close()`) still use the global write lock for exclusive file I/O
- `PublicationState` record — atomic root+size snapshot, the CAS unit for future enhancements
- Child trees (dictionaries) share the parent's `EpochReclaimer` for unified reclamation
- `ChunkStore.allocPages()` and `BumpAllocator.allocate()` are `synchronized` for concurrent writer safety
- `CowEngine` accepts per-scope `WriterArena` for file-backed concurrent COW isolation
- `ChunkStore.syncDirty()` forces only chunks written since the last sync (dirty-page tracking via `BitSet`)
- `tree.compact()` — post-order compaction: arena→slab migration, checkpoint, epoch reclamation
- `tree.keyDict16("name")` / `tree.leafInt32("name")` — derive typed handles from the tree
- `tree.newKeyBuilder(arena)` — for writes (uses `intern()`, needs dict lock for dict fields)
- `tree.newQueryBuilder(arena)` — for reads (uses `resolve()`, lock-free)
- `r.forEach(visitor)` — full ordered scan; `r.scan(qb, handle, visitor)` — prefix scan
- `LeafVisitor` returns boolean: `true` = continue, `false` = stop early
- `LeafAccessor` in scan callbacks is reusable — do not retain references

## GBIF Example

- Store file: `<parquet-dir>/gbif-tracker.taotree` (created on first run, reopened on subsequent)
- Verification tools: `GbifVerifier` (field coverage), `GbifCrossCheck` (aggregate), `GbifFieldCheck` (field-by-field)
- All 13 leaf fields verified against Parquet source: 0 mismatches
- **Known bug:** `Duplicate key in cowExpandLeaf` crash at ~1.4M rows (file 3). Pre-existing memory corruption: WriterArena NodePtr values overwrite a dict slab leaf's key bytes. Suspected cause: WriterArena 20-bit page encoding overflow after ~22M page allocations from per-scope arenas. See investigation prompt for details.
