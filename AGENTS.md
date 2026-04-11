# TaoTree Agent Notes

## Build & Test Commands

```bash
./gradlew :core:test                       # run all tests (forks per class for Lincheck isolation)
./gradlew :core:frayTest                   # Fray concurrency tests only (~1 min)
./gradlew :core:test --tests "org.taotree.Lincheck*"  # Lincheck tests only
./gradlew :core:test --tests "org.taotree.ScanTest"   # scan/query tests only
./gradlew :core:pitest                     # mutation testing (≥75% threshold)
./gradlew :core:jacocoTestReport           # coverage report → core/build/reports/jacoco/
./gradlew :examples:gbif-tracker:run --args="<parquet-dir>"  # GBIF example (persistent)
```

## Key Conventions

- Java 25+ required (`--enable-native-access=ALL-UNNAMED`)
- All tests use JUnit 5 (Jupiter)
- Lincheck and Fray tests are excluded from PIT (`excludedTestClasses = "org.taotree.Lincheck*"` and `"org.taotree.fray.*"`)
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

- `TaoTree.create(Path, KeyLayout, LeafLayout)` — creates file-backed tree, auto-creates dicts for `KeyField.dict16/dict32`
- `TaoTree.create(Path, int keyLen, int valueSize)` — creates file-backed tree with raw key/value sizes
- `TaoTree.forDictionaries(Path)` — creates file-backed infrastructure-only tree (dictionaries without primary data)
- `TaoTree.open(Path, KeyLayout, LeafLayout)` — reopens file-backed tree, rebinds dicts by order
- All trees are file-backed (in-memory mode removed); `ChunkStore` required for all allocators
- Threading: ROWEX model — lock-free readers (epoch-based snapshot), concurrent writers (optimistic COW + commit lock)
- `ReadScope` captures `PublicationState` (root + size atomically) via VarHandle acquire + enters epoch; never blocks on writers
- `WriteScope` performs optimistic COW outside any lock on the first mutation, then acquires a lightweight commit lock to publish; subsequent mutations run under the commit lock; no lock is held between `write()` and the first mutation
- On conflict (another writer published during optimistic COW): one redo COW against the new root, bounded to a single retry
- Persistence operations (`sync()`/`compact()`/`close()`) acquire global write lock then commit lock (lock ordering: writeLock → commitLock) for safe coordination with concurrent writers
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
- **Fixed:** `Duplicate key in cowExpandLeaf` crash was caused by a race condition in `ChunkStore.allocPagesSlow()` — `nextPage.set()` could overwrite a concurrent fast-path CAS, causing page double-allocation and memory corruption. Fix: replaced `set()` with a CAS loop.

## Fray Concurrency Testing

- Fray 0.8.3 plugin (`org.pastalab.fray.gradle`) requires JDK 25; auto-instruments locks, atomics, `Thread.yield()`
- Fray **cannot** instrument VarHandle on off-heap `MemorySegment` — manual `Thread.yield()` schedule points in `SlabAllocator.casInSlab()` and `free()`
- `frayTest` task filters by `@Tag("FrayTest")` (added automatically by `@FrayTest` annotation)
- Design rules for Fray tests (state-space explosion otherwise):
  - 2 threads max per test
  - 1–3 operations per thread
  - 10 iterations (POS scheduler is effective in few iterations)
  - Pre-populate data in main thread before spawning concurrent threads
  - CHUNK = 256 × 4096 (1 MB) — slab allocator needs ≥256 pages
  - Use `Files.createTempDirectory` (not `@TempDir` — doesn't work under Fray)
  - Always `deleteRecursively` in finally
