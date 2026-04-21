# TaoTree Agent Notes

## Build & Test Commands

```bash
./gradlew :core:test                                          # all tests (forks per class for Lincheck isolation)
./gradlew :core:frayTest                                      # Fray concurrency tests only (~1 min)
./gradlew :core:test --tests "org.taotree.Lincheck*"          # Lincheck tests only
./gradlew :core:pitest                                        # mutation testing (≥70% threshold)
./gradlew :core:jacocoTestReport                              # coverage report → core/build/reports/jacoco/
./gradlew :examples:gbif-tracker:run --args="<parquet-dir>"   # GBIF example
```

## Key Conventions

- Java 25+ required (`--enable-native-access=ALL-UNNAMED`).
- All tests use JUnit 5 (Jupiter).
- Lincheck and Fray tests are **excluded from PIT** (`excludedTestClasses = "org.taotree.Lincheck*"` and `"org.taotree.fray.*"`). PIT's 70% threshold is set accordingly; the remaining headroom sits in concurrency paths that only Lincheck/Fray reach.
- `forkEvery = 1` isolates test classes in separate JVMs (Lincheck Arena leak mitigation).
- File-backed tests auto-detect RAMDisk at `/Volumes/RAMDisk` (macOS) or `/dev/shm` (Linux).
- Checkpoint format: **v3** (mirrored A/B slots, CRC-32C, shadow paging, `FEATURE_INCOMPAT_UNIFIED_TEMPORAL = 0x0004`). v2 files are rejected with a friendly `IOException`.
- The library is **always temporal**: there is no user-defined `LeafLayout`, `LeafField`, `LeafHandle`, `LeafAccessor`, or `ConflictResolver` any more — those were removed in Phase 4 of the temporal-unification refactor.

## Architecture Quick Reference

- **Public API** (`org.taotree`): `TaoTree`, `Value`, `EntityVisitor`, `ValueHistoryVisitor`, `TaoKey`, `TaoDictionary`.
- **Public layout** (`org.taotree.layout`): `KeyField`, `KeyLayout`, `KeyHandle`, `KeyBuilder`, `QueryBuilder`.
- **Internal ART** (`org.taotree.internal.art`): `Node4/16/48/256`, `PrefixNode`, `NodePtr`, `NodeConstants`.
- **Internal allocators** (`org.taotree.internal.alloc`): `SlabAllocator`, `BumpAllocator`, `ChunkStore`, `Preallocator`, `OverflowPtr`, `WriterArena`.
- **Internal persistence** (`org.taotree.internal.persist`): `PersistenceManager`, `CheckpointV2`, `CheckpointIO`, `RecordHeader`, `CommitRecord`, `ShadowPagingRecovery`, `Superblock`.
- **Internal COW** (`org.taotree.internal.cow`): `CowEngine`, `CowContext`, `CowInsert`, `CowDelete`, `CowNodeOps`, `EpochReclaimer`, `Compactor`, `TemporalOpLog`.
- **Internal temporal** (`org.taotree.internal.temporal`): `EntityNode` (24-byte inline struct: `current_state_root_ref`, `attr_art_root_ref`, `versions_art_root_ref`), `AttributeRun`, `EntityVersion`, `TemporalWriter`, `TemporalReader`, `HistoryVisitor`.
- **Internal CHAMP** (`org.taotree.internal.champ`): `ChampMap`, `ChampVisitor`.

## API Patterns

### Factories / lifecycle

- `TaoTree.create(Path, KeyLayout)` — create a new file-backed always-temporal tree.
- `TaoTree.open(Path, KeyLayout)` — reopen with explicit layout; fingerprint in `SchemaBinding` section is verified and mismatch fails fast.
- `TaoTree.open(Path)` — self-describing reopen; reconstructs the layout from the stored binding.
- `tree.sync()` — write commit record + dirty chunks (cheap); full checkpoint every N commits.
- `tree.compact()` — post-order re-pack of the outer ART + fresh checkpoint + durable-generation advance. **Caveat:** does not currently recompact or retire the nested CHAMP / AttributeRuns / EntityVersions per entity (space + locality gap, not a correctness bug). Tracked as `p8-compactor-temporal`.
- `tree.close()` — flush + close mmap chunks.

### Write API (inside `try (var w = tree.write())`)

- `w.put(kb, attr, Value)` — timeless fact (first observation with `ts = TIMELESS`).
- `w.put(kb, attr, Value, ts)` — temporal observation.
- `w.putAll(kb, Map<String,Value>)` / `w.putAll(kb, Map<String,Value>, ts)` — batch.
- `w.delete(kb, attr)` — writes a tombstone AttributeRun and removes from the current CHAMP state. Reads past the tombstone return "not found" until a later observation overrides it.
- `w.delete(kb)` — retire the whole entity.

### Read API (inside `try (var r = tree.read())`)

- `r.get(kb, attr)` → `Value` — latest; falls back to the open-ended TIMELESS run.
- `r.getAt(kb, attr, ts)` → `Value` — point-in-time.
- `r.getAll(kb)` → `Map<String,Value>` — current full state.
- `r.getAllAt(kb, ts)` → `Map<String,Value>`.
- `r.history(kb, attr, ValueHistoryVisitor)` — visit every run in time order.
- `r.historyRange(kb, attr, fromMs, toMs, visitor)`.
- `r.forEach(EntityVisitor)` — visit every entity key.
- `r.scan(qb, prefixHandle, EntityVisitor)` — prefix scan over entity keys.

### Key builders

- `tree.newKeyBuilder(arena)` — write-side; interns dict values.
- `tree.newQueryBuilder(arena)` — read-side; resolve-only, lock-free, unknown values yield an empty scan.

## Concurrency Model

- **Threading:** ROWEX. Lock-free readers (epoch-pinned snapshot via `PublicationState`), concurrent writers (optimistic COW + short commit lock).
- `ReadScope` captures `PublicationState` (root + size atomically) via VarHandle acquire + enters an epoch; never blocks on writers.
- `WriteScope` performs optimistic COW outside any lock on the first mutation; acquires a short commit lock only to publish; subsequent mutations run under the commit lock. No lock is held between `write()` and the first mutation.
- On conflict (another writer published during optimistic COW): one bounded redo against the new root, replaying logical ops from a per-scope `TemporalOpLog` so sibling-attribute CHAMP updates from other writers are preserved.
- Persistence operations (`sync()`/`compact()`/`close()`) acquire the global write lock then the commit lock (ordering: `writeLock → commitLock`).
- Child trees (dictionaries) share the parent's `EpochReclaimer`.
- `ChunkStore.allocPages()` and `BumpAllocator.allocate()` are `synchronized` for concurrent-writer safety.
- `CowEngine` accepts per-scope `WriterArena` for file-backed concurrent COW isolation.
- `ChunkStore.syncDirty()` forces only chunks written since the last sync (dirty-page tracking via `BitSet`).

## Known Pivots from the Original Design

The code has diverged from `docs/design.md` in a few places. These pivots are real; design.md is being updated incrementally. Places to double-check when reading the design:

- **Inline 24-byte `EntityNode`**, not an 8-byte `EntityNodeRef` or the 40-byte draft. Each leaf of the global ART holds the full `EntityNode` struct (`current_state_root_ref`, `attr_art_root_ref`, `versions_art_root_ref`). `KeyLen + 24` is the leaf slab-class size. (Two reserved fields for cold-tier archive + per-entity policy were dropped in Phase 4; re-adding them requires a checkpoint format bump.)
- **`Value` tagged union**, not raw `byte[]`. Public values are instances of `Value`; off-heap they serialize through `ValueCodec` into a 16-byte slot or a bump-allocated overflow payload.
- **Unified checkpoint v3**, not v2 regular/temporal. `FEATURE_INCOMPAT_UNIFIED_TEMPORAL` bit gates opens.
- **§18.12.3 compactor locality claim is aspirational.** The current `Compactor` walks only the outer ART; per-entity nested structures are not repacked. See `CompactorSpaceReclaimTest` for the empirical guardrails.
- **`TemporalOpLog`-based rebase.** Originally the design used `MutationLog` with segment-copy; that clobbered sibling-attribute updates. The rebase path now replays logical put/delete-attr ops against a force-copied entity leaf under the new root.

## GBIF Example

- Store file: `<parquet-dir>/gbif-tracker.taotree` (created on first run, reopened on subsequent).
- *(The example still references the pre-unification API — rewrite tracked as plan.md `p8-examples`.)*

## Fray Concurrency Testing

- Fray 0.8.3 plugin (`org.pastalab.fray.gradle`) requires JDK 25; auto-instruments locks, atomics, `Thread.yield()`.
- Fray **cannot** instrument VarHandle on off-heap `MemorySegment` — manual `Thread.yield()` schedule points in `SlabAllocator.casInSlab()` and `free()`.
- `frayTest` task filters by `@Tag("FrayTest")` (added automatically by `@FrayTest` annotation).
- Design rules for Fray tests (otherwise state-space explodes):
  - 2 threads max per test.
  - 1–3 operations per thread.
  - 10 iterations (POS scheduler is effective in few iterations).
  - Pre-populate data in the main thread before spawning concurrent threads.
  - CHUNK = 256 × 4096 (1 MB) — slab allocator needs ≥ 256 pages.
  - Use `Files.createTempDirectory` (not `@TempDir` — doesn't work under Fray).
  - Always `deleteRecursively` in `finally`.

## Phase tracking

`~/.copilot/session-state/<session>/plan.md` has the full phased plan. As of
2026-04-21 the following Phase 8 items remain open:
`p8-compactor-temporal` (P2; space-reclaim-only gap, not correctness).

Recently closed: `p8-schema-binding`, `p8-tombstone-semantics`,
`p8-value-canonicalization`, `p8-split-taotree` (WriteScopeOps / ArtRead
extraction), `p8-examples`, `p8-champ-iterate-perf`, `p8-perf` baseline
pass.
