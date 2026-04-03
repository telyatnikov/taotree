# TaoTree Agent Notes

## Build & Test Commands

```bash
./gradlew :core:test                       # run all tests (forks per class for Lincheck isolation)
./gradlew :core:test --tests "org.taotree.Lincheck*"  # Lincheck tests only
./gradlew :core:pitest                     # mutation testing (≥75% threshold)
./gradlew :core:jacocoTestReport           # coverage report → core/build/reports/jacoco/
./gradlew :examples:gbif-tracker:run --args="<parquet-dir>"  # GBIF example
```

## Key Conventions

- Java 25+ required (`--enable-native-access=ALL-UNNAMED`)
- All tests use JUnit 5 (Jupiter)
- Lincheck tests are excluded from PIT (`excludedTestClasses = "org.taotree.Lincheck*"`)
- `forkEvery = 1` isolates test classes in separate JVMs (Lincheck Arena leak mitigation)
- File-backed tests auto-detect RAMDisk at `/Volumes/RAMDisk` (macOS) or `/dev/shm` (Linux)

## Architecture Quick Reference

- Public API: `org.taotree` package — `TaoTree`, `KeyLayout`, `LeafLayout`, `KeyHandle`, `LeafHandle`, `LeafAccessor`, `KeyBuilder`, `KeyField`, `LeafField`, `TaoDictionary`, `TaoString`, `TaoKey`
- Internal: `org.taotree.internal` — `SlabAllocator`, `BumpAllocator`, `ChunkStore`, `Node4/16/48/256`, `PrefixNode`, `NodePtr`, `OverflowPtr`, `Superblock`, `Preallocator`
- The layout-based API (`TaoTree.open(KeyLayout, LeafLayout)`) auto-creates dictionaries for `KeyField.dict16/dict32` fields
- Handles are pre-computed offset records derived from layouts; used for zero-lookup typed access
