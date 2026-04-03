# TaoTree

Adaptive Radix Tree (ART) library for Java using the Foreign Function & Memory API.
All data lives off-heap in slab allocators. Zero GC pressure on the hot path.

## Quick Start

```java
// In-memory
try (var tree = TaoTree.open(16, 24)) {
    try (var w = tree.write()) {
        long leaf = w.getOrCreate(key);
        w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
    }
    try (var r = tree.read()) {
        long leaf = r.lookup(key);
        long value = r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0);
    }
}

// File-backed (persistent)
try (var tree = TaoTree.create(Path.of("data.tao"), 16, 24)) {
    var dict = TaoDictionary.u16(tree);
    dict.intern("Animalia");
    tree.sync();
}
// Reopen
try (var tree = TaoTree.open(Path.of("data.tao"))) {
    var dict = tree.dictionary(0);
    dict.resolve("Animalia"); // returns 1
}
```

## Build

Requires Java 25+.

```bash
gradle build                          # full build (core + jmh + examples)
gradle :core:test                     # run tests
gradle :core:cleanTest :core:test     # force re-run tests
gradle :core:pitest                   # mutation testing (STRONGER mutators)
gradle :core:jacocoTestReport         # line coverage report
```

## RAM Disk for Tests (optional)

File-backed tests use `@TempDir` which defaults to the system temp directory.
For faster tests, use a RAM-backed filesystem:

**macOS:**
```bash
./ramdisk.sh up       # create 256 MB RAM disk at /Volumes/RAMDisk
gradle :core:test     # auto-detected, tests run in RAM
./ramdisk.sh down     # tear down when done
```

**Linux:** `/dev/shm` is tmpfs by default — auto-detected, no setup needed.

**Custom path:**
```bash
gradle :core:test -PtestTmpDir=/my/ramdisk
```

## Project Structure

```
taotree/
  core/           — ART library (org.taotree + org.taotree.internal)
  jmh/            — JMH benchmarks
  examples/       — Example applications
  docs/           — Design documentation
```

## Architecture

- `TaoTree` — off-heap ART with scoped read/write access
- `TaoDictionary` — string-to-int intern table backed by ART
- `SlabAllocator` — fixed-size slab allocator (in-memory or file-backed)
- `BumpAllocator` — append-only allocator for variable-length overflow data
- `ChunkStore` — single-file storage with 64 MB chunked mmap windows
- `Preallocator` — OS-native block reservation (macOS `F_PREALLOCATE`, Linux `fallocate`)
- `NodePtr` / `OverflowPtr` — position-independent swizzled pointers

See [docs/design.md](docs/design.md) for the full design document.
