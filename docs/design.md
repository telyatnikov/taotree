# TaoTree: Adaptive Radix Tree Library for Java Foreign Function & Memory API

**Name:** TaoTree (`org.taotree`)  
**Status:** Draft  
**Date:** 2026-04-02

---

## 0. Implementation Status

### Implemented

| Component | Status | Notes |
|---|---|---|
| `SlabAllocator` | Done | Fixed-size slab allocator with bitmask occupancy, file-backed via `ChunkStore` |
| `BumpAllocator` | Done | Bump allocator for variable-length immutable payloads, file-backed via `ChunkStore` |
| `NodePtr` / `OverflowPtr` | Done | 64-bit tagged swizzled pointer encoding |
| Node types (4/16/48/256/PrefixNode) | Done | Static operations on off-heap `MemorySegment` slices |
| `TaoTree` core (lookup/insert/delete) | Done | Fixed-width keys, pessimistic prefix compression, zero-initialized leaf values |
| `TaoTree` owns infrastructure | Done | Owns `ChunkStore`, allocators, node class IDs, `ReentrantReadWriteLock` + `commitLock` |
| `TaoTree.ReadScope` / `TaoTree.WriteScope` | Done | Epoch-based lock-free reads; optimistic COW writes with deferred commit |
| `LeafField` / `LeafLayout` / `LeafHandle` | Done | Typed leaf schema with pre-computed offsets and compile-time type safety |
| `LeafAccessor` / `LeafVisitor` | Done | Typed read/write access to leaf values; boolean-returning visitor for scan |
| `QueryBuilder` | Done | Resolve-only key builder (lock-free); unknown dict values → empty scan |
| `ConflictResolver` | Done | Pluggable merge strategy for deferred-commit rebase conflicts |
| `TaoDictionary` | Done | Self-locking string→int dictionary (fixed padded keys, 128B max) |
| `TaoKey` | Done | Binary-comparable encoding for u8-u64, i8-i64, strings |
| `TaoString` | Done | 16-byte inline/overflow string representation |
| `KeyField` / `KeyLayout` / `KeyBuilder` | Done | Compound key definition and encoding |
| Scan / prefix scan | Done | `forEach(visitor)`, `scan(qb, handle, visitor)` with ordered traversal and early termination |
| COW + epoch reclamation | Done | `CowEngine`, `CowInsert`, `CowDelete`, `CowNodeOps`, `EpochReclaimer`, `Compactor` |
| File-backed persistence | Done | `ChunkStore` + v2 checkpoint (mirrored A/B slots, CRC-32C, shadow paging) |
| Compaction | Done | `tree.compact()` — post-order arena→slab migration, checkpoint, epoch reclamation |
| JMH benchmarks | Done | ART lookup/insert, dictionary resolve throughput |
| GBIF species tracker example | Done | Reads Parquet, 7 dict-encoded key fields, 13 verified leaf fields |
| Fray concurrency testing | Done | Controlled schedule-point testing for race conditions (JDK 25, Fray 0.8.3) |
| Lincheck linearizability tests | Done | Stress-based linearizability verification against sequential spec |

### Concurrency model

ROWEX (Read-Optimistic Write-EXclusive) model:

- **Readers** are lock-free. `ReadScope` captures a `PublicationState` (root + size atomically)
  via `VarHandle.getAcquire` and enters an epoch. Readers never block on writers.
- **Writers** perform optimistic COW outside any lock on the first mutation, then acquire a
  lightweight `commitLock` to publish the new root. On conflict (another writer published during
  COW), one redo against the new root is attempted (bounded single retry).
- **Persistence** operations (`sync()`/`compact()`/`close()`) acquire the global `writeLock`
  then `commitLock` (lock ordering: writeLock → commitLock) for exclusive file I/O coordination.
- **Dictionaries** (`TaoDictionary`) are self-locking (own lock for `intern`, lock-free for `resolve`).
- **Epoch reclamation** (`EpochReclaimer`) defers freeing of COW-replaced nodes until all readers
  that could see the old root have exited. Child trees share the parent's reclaimer.

### Future work (not yet implemented)

| Feature | Notes |
|---|---|
| Builder API for ART | Currently uses constructor directly |
| `LEAF_INLINE` | Inline small leaf values in the `NodePtr` payload |
| Variable-length ART keys | Current implementation uses fixed-width keys only; dictionary ARTs pad strings to 128B |
| Configurable `prefixCapacity` | Hardcoded to 15 (fits in 24B prefix node) |
| Reserved-range dictionary APIs | `TaoDictionary.u32WithReserved()` for pre-assigned codes |
| Persistence: WAL | Write-ahead log for crash recovery |

---

## 1. Abstract

This document specifies the design of a general-purpose Adaptive Radix Tree (ART) library
implemented entirely in Java using the Foreign Function & Memory (FFM) API. All data structures
live off-heap in `MemorySegment`-backed slab allocators. There is no reliance on Java object
headers, garbage collection, or heap allocation on the hot path.

The library is designed to serve two roles within a single codebase:

1. **Data ART** — high-cardinality index with fixed-width compound keys and structured leaf values
   (e.g., per-record observation state in the GBIF species tracker).
2. **TaoDictionary ART** — low-cardinality string-to-integer mapping with variable-length keys and
   small fixed-width leaf values (e.g., taxonomy, geography, and field-name dictionaries).

Both roles use the same ART core, the same node types, and the same slab allocator. Only the
key encoding and leaf size differ per instance.

---

## 2. Goals

1. **Performance-first.** Benchmark-driven from day one. Point-lookup throughput is the primary
   metric.
2. **Fully off-heap.** Zero GC pressure for all index state. All memory managed via FFM
   `Arena` and `MemorySegment`.
3. **Universal ART core.** The trie operates on opaque `(byte*, len)` keys. Fixed-width and
   variable-length keys are both supported without specialization.
4. **Configurable key schema.** Users declare typed fields (raw integers, dictionary-backed
   strings) and the library compiles them into binary-comparable key encoders at construction
   time using pre-resolved `VarHandle` accessors — zero overhead at runtime.
5. **Configurable leaf size.** Different ART instances may use different leaf sizes, from 4 bytes
   (dictionary code) to 32+ bytes (structured record), allocated from size-classed slab allocators.
6. **Unified memory model.** TaoDictionary ARTs and data ARTs share the same allocator
   infrastructure. No split between heap dictionaries and off-heap data.

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                      Application Layer                       │
│   tree.upsert("Animalia", "Chordata", "Accipitridae", ...) │
└───────────────────────┬──────────────────────────────────────┘
                        │ resolves dictionaries, encodes key
┌───────────────────────▼──────────────────────────────────────┐
│                      KeyLayout                               │
│   VarHandle[] handles  (pre-resolved, big-endian)            │
│   int totalWidth       (fixed, known at construction)        │
│   TaoDictionary[] dicts (one per DICT field, nullable)       │
└───────────────────────┬──────────────────────────────────────┘
                        │ passes (MemorySegment, len) — type-erased
┌───────────────────────▼──────────────────────────────────────┐
│                    ART Core (byte-level)                     │
│   Node4 / Node16 / Node48 / Node256 / PrefixNode    │
│   Supports both fixed-width and variable-length keys         │
│   Returns leaf references (swizzled tagged pointers)         │
└───────────────────────┬──────────────────────────────────────┘
                        │ allocates / dereferences nodes and leaves
┌───────────────────────▼──────────────────────────────────────┐
│                  SlabAllocator (off-heap, fixed-size)         │
│   Size-classed segments backed by MemorySegment slabs        │
│   Supports multiple leaf size classes per ART instance        │
│   Tagged/swizzled pointers: (slabId, offset, metadata)       │
├──────────────────────────────────────────────────────────────┤
│                  BumpAllocator (off-heap, variable-length)    │
│   Bump-allocated pages for immutable variable-length data    │
│   TaoString overflow payloads, long prefix chains            │
│   Swizzled pointers: (pageId, offset)                        │
└──────────────────────────────────────────────────────────────┘
```

### 3.1 Instance topology for the GBIF species tracker

```
┌──────────────────────────────────────────────────────────┐
│  Everything is ART.  Everything is FFM.  Everything is   │
│  off-heap.                                               │
├──────────────────────────────────────────────────────────┤
│  kingdomDict ART   string keys, 4B leaves,   ~7          │
│  phylumDict ART    string keys, 4B leaves,   ~80         │
│  familyDict ART    string keys, 4B leaves,   ~10,000     │
│  speciesDict ART   string keys, 4B leaves,   ~2,500,000  │
│  countryDict ART   string keys, 4B leaves,   ~250        │
│  stateDict ART     string keys, 4B leaves,   ~50,000     │
│  fieldDict ART     string keys, 4B leaves,   ~20         │
│  data ART          16B fixed keys, var leaves, ~millions  │
├──────────────────────────────────────────────────────────┤
│              Shared SlabAllocator + BumpAllocator         │
│              Shared node types (4/16/48/256/PrefixNode)   │
└──────────────────────────────────────────────────────────┘
```

---

## 4. Pointer Encoding

All references between ART nodes and between nodes and leaves are encoded as a single 64-bit
value called a `NodePtr`. This value combines pointer swizzling (position-independent
addressing) with pointer tagging (inline metadata).

### 4.1 Layout

```
 63      56 55                                              0
┌──────────┬────────────────────────────────────────────────┐
│ metadata │                  payload                       │
│  8 bits  │                  56 bits                       │
└──────────┴────────────────────────────────────────────────┘
```

### 4.2 Metadata byte

```
 bit 7:    gate flag (reserved for nested ART / future use)
 bits 6-4: reserved
 bits 3-0: node type tag
```

Node type tags:

| Tag | Type        | Description                                      |
|-----|-------------|--------------------------------------------------|
| 0   | EMPTY       | Null / absent child                              |
| 1   | PREFIX      | Prefix compression node (variable-length chain)  |
| 2   | NODE_4      | Inner node, up to 4 children                     |
| 3   | NODE_16     | Inner node, up to 16 children                    |
| 4   | NODE_48     | Inner node, up to 48 children                    |
| 5   | NODE_256    | Inner node, up to 256 children                   |
| 6   | LEAF        | Leaf allocated from a slab allocator              |
| 7   | LEAF_INLINE | Leaf value inlined in the pointer payload         |

### 4.3 Payload interpretation

For node types `PREFIX`, `NODE_4`, `NODE_16`, `NODE_48`, `NODE_256`, and `LEAF`:

```
 payload = (slabClassId : 8 bits) | (slabId : 16 bits) | (offset : 32 bits)
```

The `SlabAllocator` resolves `(slabClassId, slabId, offset)` to a `MemorySegment` slice.

For `LEAF_INLINE`:

```
 payload = up to 7 bytes of inlined leaf data
```

This is useful for dictionary ARTs where the leaf is a single `u32` code (4 bytes). The code
is stored directly in the pointer, eliminating one dereference. Whether to inline is decided
at ART construction time based on the configured leaf size.

### 4.4 Java encoding

```java
public final class NodePtr {
    // Pack
    static long pack(int metadata, int slabClassId, int slabId, int offset) {
        return ((long)(metadata & 0xFF) << 56)
             | ((long)(slabClassId & 0xFF) << 48)
             | ((long)(slabId & 0xFFFF) << 32)
             | (offset & 0xFFFFFFFFL);
    }

    // Unpack
    static int  metadata(long ptr)     { return (int)(ptr >>> 56) & 0xFF; }
    static int  nodeType(long ptr)     { return (int)(ptr >>> 56) & 0x0F; }
    static int  slabClassId(long ptr)  { return (int)(ptr >>> 48) & 0xFF; }
    static int  slabId(long ptr)       { return (int)(ptr >>> 32) & 0xFFFF; }
    static int  offset(long ptr)       { return (int)(ptr & 0xFFFFFFFFL); }

    static boolean isEmpty(long ptr)   { return ptr == 0; }
}
```

---

## 5. SlabAllocator

### 5.1 Overview

The `SlabAllocator` manages off-heap memory for all ART nodes and leaves. It organizes memory
into **slab classes**, where each class allocates fixed-size segments from large contiguous
`MemorySegment` buffers (slabs).

### 5.2 Slab classes

Each ART instance registers the slab classes it needs. The ART core requires classes for the
internal node types. Additional classes are registered for leaf values.

Standard node slab classes:

| Slab class | Segment size | Contents                                |
|------------|-------------|-----------------------------------------|
| 0          | configurable | Prefix node (count byte + N key bytes + child ptr) |
| 1          | 40 bytes     | Node4 (1B count + 4B keys + 4×8B children)  |
| 2          | 152 bytes    | Node16 (1B count + 16B keys + 16×8B children) |
| 3          | 648 bytes    | Node48 (1B count + 256B index + 48×8B children) |
| 4          | 2056 bytes   | Node256 (2B count + 256×8B children)     |

Leaf slab classes (registered per ART instance):

| Example | Segment size | Use case                                  |
|---------|-------------|-------------------------------------------|
| 5       | 4 bytes      | TaoDictionary code leaf (u32)               |
| 6       | 16 bytes     | Attribute witness leaf (u64 + u32 + pad) |
| 7       | 16 bytes     | TaoString leaf (len + inline/ptr)    |
| 8       | 32 bytes     | Entity header leaf                       |

### 5.3 Variable-length leaves

Rather than one fixed leaf size per ART, the allocator supports **multiple leaf slab classes**
registered at ART construction time. The `NodePtr` tag encodes which slab class the leaf
belongs to, allowing different leaves in the same tree to have different sizes.

```java
var tree = TaoTree.open(16, new int[]{4, 16, 32});
// leafClass 0 = 4-byte leaves (dict code)
// leafClass 1 = 16-byte leaves (witness)
// leafClass 2 = 32-byte leaves (observation header)

// Insert with specific leaf class
try (var w = art.write()) {
    long leafRef = w.getOrCreate(key, 16, 1);  // leafClass 1
}
```

When the ART has only one leaf class (the common case for dictionary ARTs), the leaf class
parameter can be omitted and defaults to 0.

This design avoids the fixed-size waste of allocating 32 bytes for a 4-byte dictionary code,
while keeping the allocator simple — each class is still fixed-size internally.

### 5.4 Slab structure

Each slab is a contiguous `MemorySegment` containing:

```
┌──────────────────────────────────────────────────┐
│  Occupancy bitmask (1 bit per segment)           │
│  ceil(segmentsPerSlab / 64) × 8 bytes            │
├──────────────────────────────────────────────────┤
│  Segment 0  [segmentSize bytes]                  │
│  Segment 1  [segmentSize bytes]                  │
│  ...                                             │
│  Segment N  [segmentSize bytes]                  │
└──────────────────────────────────────────────────┘
```

### 5.5 Allocation and deallocation

```java
public final class SlabAllocator {
    private final Arena arena;
    private final SlabClass[] classes;

    // Allocate a segment from the given slab class
    long allocate(int slabClassId);

    // Free a segment
    void free(long ptr);

    // Resolve a pointer to a MemorySegment slice
    MemorySegment resolve(long ptr);

    // Resolve with explicit length (for reads that don't need the full segment)
    MemorySegment resolve(long ptr, int length);
}
```

### 5.6 Slab growth

When a slab class runs out of segments, a new slab is allocated from the `Arena`. Slabs are
never moved (their base address is stable for the arena's lifetime), so existing pointers
remain valid.

### 5.7 Vacuum

When occupancy drops below a threshold (e.g., 10% of a slab), segments can be compacted into
fewer slabs. This requires rewriting all `NodePtr` values that reference the vacuumed slab,
which is done by a full tree traversal (same approach as DuckDB's `Vacuum` operation).

---

## 5b. BumpAllocator

### 5b.1 Overview

The `BumpAllocator` is a companion allocator to `SlabAllocator` for **variable-length,
immutable** data. Its primary use case is storing overflow payloads for TaoString leaves
(strings longer than 12 bytes), but it is a general-purpose facility available for any
immutable blob that doesn't fit a fixed slab class.

Design properties:

- **Append-only.** Allocation is a bump-pointer advance within a page. O(1), branchless on
  the common path (when the current page has space).
- **Immutable payloads.** Once written, a payload is never modified in place. This eliminates
  concurrency hazards for readers and simplifies persistence (payloads can be mmap'd directly).
- **Deferred reclamation.** When a leaf referencing an overflow payload is deleted, the payload
  becomes dead space. It is not immediately freed. Dead space is reclaimed only during vacuum
  (compaction), which rewrites live payloads into fresh pages.

### 5b.2 Page structure

```
┌──────────────────────────────────────────────────────────┐
│  Page (64 KB default, configurable)                      │
├──────────────────────────────────────────────────────────┤
│  Payload 0  [len0 bytes]                                 │
│  Payload 1  [len1 bytes]                                 │
│  Payload 2  [len2 bytes]                                 │
│  ...                                                     │
│  [free space ← bump pointer]                             │
│                                                          │
│  (no per-payload header — length is stored in the        │
│   TaoString leaf, not in the arena)                       │
└──────────────────────────────────────────────────────────┘
```

Payloads are packed contiguously with no per-entry metadata. The length of each payload is
known by the caller (it is stored in the TaoString leaf's `len` field). This maximizes
packing density.

### 5b.3 Overflow pointer encoding

Overflow pointers are 64-bit swizzled references stored in the TaoString leaf (or
anywhere that needs to reference overflow data):

```
 63      56 55                  32 31                       0
┌──────────┬─────────────────────┬──────────────────────────┐
│ reserved │  pageId (24 bits)   │  offset (32 bits)        │
│  8 bits  │                     │                          │
└──────────┴─────────────────────┴──────────────────────────┘
```

- `pageId` — index into the arena's page array. 24 bits supports up to 16M pages
  (16M × 64 KB = 1 TB total overflow capacity).
- `offset` — byte offset within the page. 32 bits supports pages up to 4 GB (far larger than
  the typical 64 KB page size; future-proof).
- `reserved` — 8 bits for flags (e.g., storage class tagging, compression indicator).

```java
public final class OverflowPtr {
    static long pack(int pageId, int offset) {
        return ((long)(pageId & 0x00FFFFFF) << 32)
             | (offset & 0xFFFFFFFFL);
    }

    static int pageId(long ptr) { return (int)(ptr >>> 32) & 0x00FFFFFF; }
    static int offset(long ptr) { return (int)(ptr & 0xFFFFFFFFL); }
}
```

### 5b.4 API

```java
public final class BumpAllocator implements AutoCloseable {
    private final Arena arena;
    private final int pageSize;            // default 64 KB
    private MemorySegment[] pages;         // grows dynamically
    private int currentPage;
    private int bumpOffset;                // next free byte in current page

    // Allocate space for a payload of the given length.
    // Returns an OverflowPtr (packed long).
    public long allocate(int length);

    // Resolve an overflow pointer to a MemorySegment slice of the given length.
    public MemorySegment resolve(long overflowPtr, int length);

    // Total bytes allocated (including dead space from deletions).
    public long bytesAllocated();
}
```

### 5b.5 Allocation flow

```
allocate(length):
    if bumpOffset + length > pageSize:
        // Current page full, allocate a new one
        currentPage++
        if currentPage >= pages.length:
            grow pages array (double capacity)
        pages[currentPage] = arena.allocate(pageSize, 1)
        bumpOffset = 0

    ptr = OverflowPtr.pack(currentPage, bumpOffset)
    bumpOffset += length
    return ptr
```

For payloads larger than `pageSize`, a dedicated oversized page is allocated (exactly the
payload size). This is rare — most strings are far shorter than 64 KB.

### 5b.6 Vacuum / compaction

Since payloads are immutable, dead space accumulates when TaoString leaves are deleted.
Compaction works as follows:

1. Allocate a fresh `BumpAllocator` (the "target").
2. Walk the ART. For each live TaoString leaf with `len > 12` (long representation):
   a. Read the payload from the old arena.
   b. Allocate in the target arena, copy the payload.
   c. Update the overflow pointer in the leaf.
3. Swap the old arena for the target. Release old pages.

This is a full-tree operation, same as slab vacuum. In practice, both vacuums are done in a
single tree walk.

### 5b.7 Memory accounting

The bump allocator tracks:

- `bytesAllocated` — total bytes consumed (including dead space).
- Page count and fill levels.

The ART or tree layer can query these to decide when compaction is worthwhile (e.g., when
dead space exceeds 50% of allocated space).

---

## 6. ART Node Layouts

All node layouts use explicit `MemoryLayout` definitions with exact byte offsets. No Java
object headers. No padding surprises.

### 6.1 PrefixNode

```
┌───────┬────────────────────────┬──────────┐
│ count │ key bytes [0..count-1] │ child    │
│ 1B    │ up to prefixCapacity B │ 8B (ptr) │
└───────┴────────────────────────┴──────────┘
```

The `prefixCapacity` is configurable per ART instance (DuckDB uses 8 by default). If a
compressed path exceeds the capacity, multiple prefix nodes are chained.

Path compression follows DuckDB's hybrid approach from the ART paper:
- Store up to `prefixCapacity` bytes pessimistically (compare during lookup).
- If the prefix is longer, switch to optimistic mode (skip bytes, verify at leaf).

### 6.2 Node4

```java
MemoryLayout NODE4 = MemoryLayout.structLayout(
    ValueLayout.JAVA_BYTE.withName("count"),           // 1B
    MemoryLayout.sequenceLayout(4, JAVA_BYTE)
        .withName("keys"),                             // 4B
    MemoryLayout.paddingLayout(3),                     // 3B alignment
    MemoryLayout.sequenceLayout(4, JAVA_LONG)
        .withName("children")                          // 32B (4 × 8B NodePtr)
);
// Total: 40 bytes
```

- Keys sorted by byte value.
- Lookup: linear scan over 4 entries.
- Grows to Node16 when full.

### 6.3 Node16

```java
MemoryLayout NODE16 = MemoryLayout.structLayout(
    ValueLayout.JAVA_BYTE.withName("count"),           // 1B
    MemoryLayout.paddingLayout(7),                     // 7B alignment
    MemoryLayout.sequenceLayout(16, JAVA_BYTE)
        .withName("keys"),                             // 16B
    MemoryLayout.sequenceLayout(16, JAVA_LONG)
        .withName("children")                          // 128B (16 × 8B NodePtr)
);
// Total: 152 bytes
```

- Keys sorted by byte value.
- Lookup: binary search or linear scan (SIMD not available in Java, but 16 bytes fits a cache
  line and the JIT may auto-vectorize the comparison loop).
- Grows to Node48 when full. Shrinks to Node4 when count drops to 4.

### 6.4 Node48

```java
MemoryLayout NODE48 = MemoryLayout.structLayout(
    ValueLayout.JAVA_BYTE.withName("count"),           // 1B
    MemoryLayout.paddingLayout(7),                     // 7B alignment
    MemoryLayout.sequenceLayout(256, JAVA_BYTE)
        .withName("childIndex"),                       // 256B
    MemoryLayout.sequenceLayout(48, JAVA_LONG)
        .withName("children")                          // 384B (48 × 8B NodePtr)
);
// Total: 648 bytes
```

- `childIndex[byte]` stores the position in the children array, or `EMPTY_MARKER` (48).
- Lookup: one array access to get index, one to get child pointer. O(1).
- Grows to Node256 when full. Shrinks to Node16 when count drops to 12.

### 6.5 Node256

```java
MemoryLayout NODE256 = MemoryLayout.structLayout(
    ValueLayout.JAVA_SHORT.withName("count"),          // 2B
    MemoryLayout.paddingLayout(6),                     // 6B alignment
    MemoryLayout.sequenceLayout(256, JAVA_LONG)
        .withName("children")                          // 2048B (256 × 8B NodePtr)
);
// Total: 2056 bytes
```

- `children[byte]` is the child pointer directly. `NodePtr.isEmpty()` to check presence.
- Lookup: single array access. O(1).
- Shrinks to Node48 when count drops to 36.

### 6.6 Grow / shrink thresholds

| Transition       | Trigger                    |
|------------------|----------------------------|
| Node4 → Node16   | Insert when count == 4     |
| Node16 → Node48  | Insert when count == 16    |
| Node48 → Node256 | Insert when count == 48    |
| Node256 → Node48 | Delete when count == 36    |
| Node48 → Node16  | Delete when count == 12    |
| Node16 → Node4   | Delete when count == 4     |

---

## 7. ART Core Operations

### 7.1 API

```java
public final class TaoTree implements AutoCloseable {
    private final int keyLen;

    public static TaoTree open(int keyLen, int[] leafValueSizes);

    // Scoped access — the primary public API
    public ReadScope read();    // acquires read lock
    public WriteScope write();  // acquires write lock

    public final class ReadScope implements AutoCloseable {
        public long lookup(MemorySegment key, int keyLen);
        public long lookup(byte[] key);
        public MemorySegment leafValue(long leafPtr);  // read-only segment
        public long size();
        public boolean isEmpty();
    }

    public final class WriteScope implements AutoCloseable {
        // All ReadScope operations, plus:
        public long getOrCreate(MemorySegment key, int keyLen, int leafClass);
        public long getOrCreate(byte[] key, int leafClass);
        public boolean delete(MemorySegment key, int keyLen);
        public boolean delete(byte[] key);
        public MemorySegment leafValue(long leafPtr);  // writable segment
    }
}
```

Typical usage:

```java
try (var tree = TaoTree.open(16, 24)) {

    try (var w = tree.write()) {
        long leaf = w.getOrCreate(key, 16, 0);
        w.leafValue(leaf).set(ValueLayout.JAVA_LONG, 0, 42L);
    }

    try (var r = tree.read()) {
        long leaf = r.lookup(key, 16);
        long value = r.leafValue(leaf).get(ValueLayout.JAVA_LONG, 0);
    }
}
```

**Important:** `NodePtr` values and `MemorySegment` references are valid only while the
view is open. Do not retain them after `close()`.

### 7.2 Lookup algorithm

Following the ART paper and DuckDB implementation:

```
lookup(node, key, depth):
    if node is EMPTY:
        return NOT_FOUND

    if node is LEAF or LEAF_INLINE:
        // Lazy expansion: verify full key match
        if leafKeyMatches(node, key, keyLen):
            return node
        return NOT_FOUND

    // Traverse prefix chain
    while node is PREFIX:
        prefix = resolve(node)
        for i in 0..prefix.count-1:
            if depth >= keyLen or key[depth] != prefix.keys[i]:
                return NOT_FOUND
            depth++
        node = prefix.child

    // Find child for the next key byte
    child = findChild(node, key[depth])
    if child is EMPTY:
        return NOT_FOUND
    return lookup(child, key, depth + 1)
```

### 7.3 Insert algorithm

```
getOrCreate(node, key, depth, leafClass):
    if node is EMPTY:
        // Create leaf, install prefix for remaining key bytes
        create leaf with leafClass
        wrap in prefix nodes for key[depth..keyLen-1]
        return leaf

    if node is LEAF:
        // Lazy expansion: find divergence point
        existingKey = loadKey(node)
        mismatch = findMismatchPos(key, existingKey, depth)
        if mismatch == keyLen:
            return node  // exact match, return existing leaf

        // Create new inner node at divergence point
        create Node4
        install prefix for shared bytes key[depth..mismatch-1]
        add existing leaf at existingKey[mismatch]
        add new leaf at key[mismatch]
        return new leaf

    // Traverse prefix, handle prefix mismatch (split)
    ...same as DuckDB...

    // Find or create child
    child = findChild(node, key[depth])
    if child is EMPTY:
        create leaf, wrap in prefix
        insertChild(node, key[depth], prefixedLeaf)
        return leaf
    return getOrCreate(child, key, depth + 1, leafClass)
```

### 7.4 Delete algorithm

Symmetric to insert. Remove the leaf, shrink inner nodes when underfull, merge prefix nodes
when a parent has only one remaining child.

---

## 8. Key Encoding

### 8.1 Binary-comparable key transformation

All keys stored in the ART must be binary-comparable: lexicographic byte comparison of the
encoded key must produce the same ordering as the logical comparison of the original values.

Transformations (following ART paper Section IV and DuckDB's `Radix` class):

| Type   | Transformation                                               |
|--------|--------------------------------------------------------------|
| u8     | Identity                                                     |
| u16    | Big-endian byte order                                        |
| u32    | Big-endian byte order                                        |
| u64    | Big-endian byte order                                        |
| i8     | Flip sign bit (XOR 0x80)                                     |
| i16    | Flip sign bit + big-endian                                   |
| i32    | Flip sign bit + big-endian                                   |
| i64    | Flip sign bit + big-endian (`value ^ Long.MIN_VALUE`, big-endian) |
| String | Escape bytes ≤ 0x01 with 0x01 prefix + null-terminate       |

### 8.2 String key encoding (for dictionary ARTs)

Following DuckDB's `ARTKey::CreateARTKey<string_t>`:

```java
static byte[] encodeString(String value) {
    byte[] raw = value.getBytes(StandardCharsets.UTF_8);
    int escapeCount = 0;
    for (byte b : raw) {
        if ((b & 0xFF) <= 1) escapeCount++;
    }
    byte[] key = new byte[raw.length + escapeCount + 1];
    int pos = 0;
    for (byte b : raw) {
        if ((b & 0xFF) <= 1) key[pos++] = 0x01;
        key[pos++] = b;
    }
    key[pos] = 0x00;  // null terminator
    return key;
}
```

The escape ensures no string key is a prefix of another, which is required for correct ART
behavior with lazy expansion.

### 8.3 Compound key encoding (for data ART)

Fixed-width compound keys are constructed by concatenating big-endian encoded fields at
known offsets. No delimiters or escaping needed because all fields are fixed-width.

```java
// Data ART key: 16 bytes total (GBIF species tracker)
// Offset 0:  kingdomCode  u16 big-endian  (2 bytes)
// Offset 2:  phylumCode   u16 big-endian  (2 bytes)
// Offset 4:  familyCode   u16 big-endian  (2 bytes)
// Offset 6:  speciesCode  u32 big-endian  (4 bytes)
// Offset 10: countryCode  u16 big-endian  (2 bytes)
// Offset 12: stateCode    u16 big-endian  (2 bytes)
// Offset 14: fieldCode    u16 big-endian  (2 bytes)
```

---

## 9. KeyLayout and KeyBuilder

### 9.1 Field types

```java
public sealed interface KeyField {
    String name();
    int width();

    static KeyField uint8 (String name) { return new UInt8(name);  }
    static KeyField uint16(String name) { return new UInt16(name); }
    static KeyField uint32(String name) { return new UInt32(name); }
    static KeyField uint64(String name) { return new UInt64(name); }
    static KeyField int64 (String name) { return new Int64(name);  }

    static KeyField dict16(String name, TaoDictionary dict) { return new DictU16(name, dict); }
    static KeyField dict32(String name, TaoDictionary dict) { return new DictU32(name, dict); }

    record UInt8 (String name) implements KeyField { public int width() { return 1;  } }
    record UInt16(String name) implements KeyField { public int width() { return 2;  } }
    record UInt32(String name) implements KeyField { public int width() { return 4;  } }
    record UInt64(String name) implements KeyField { public int width() { return 8;  } }
    record Int64 (String name) implements KeyField { public int width() { return 8;  } }

    record DictU16(String name, TaoDictionary dict) implements KeyField {
        public int width() { return 2; }
    }
    record DictU32(String name, TaoDictionary dict) implements KeyField {
        public int width() { return 4; }
    }
}
```

### 9.2 KeyLayout

```java
public final class KeyLayout {
    private final KeyField[] fields;
    private final int[]      offsets;     // byte offset of each field
    private final int        totalWidth;  // sum of all field widths
    private final VarHandle[] handles;    // pre-resolved, BIG_ENDIAN

    public static KeyLayout of(KeyField... fields) { ... }
}
```

`VarHandle` instances are resolved once at construction time using
`MethodHandles.memorySegmentViewVarHandle()` with the appropriate `ValueLayout` and
`ByteOrder.BIG_ENDIAN`. At runtime, `VarHandle.set()` compiles to a single MOV instruction
after JIT optimization.

### 9.3 KeyBuilder

```java
public final class KeyBuilder {
    private final KeyLayout layout;
    private final MemorySegment buf;   // pre-allocated, reusable (thread-local)

    public KeyBuilder setU64(int fieldIndex, long value)     { ... return this; }
    public KeyBuilder setU32(int fieldIndex, int value)      { ... return this; }
    public KeyBuilder setU16(int fieldIndex, short value)    { ... return this; }
    public KeyBuilder setI64(int fieldIndex, long value)     { ... return this; }
    public KeyBuilder setDict(int fieldIndex, String value)  { ... return this; }

    public MemorySegment key()  { return buf; }
    public int keyLen()         { return layout.totalWidth; }
}
```

### 9.4 Usage example

```java
var tree = TaoTree.forDictionaries();

var kingdomDict = TaoDictionary.u16(tree);
var phylumDict  = TaoDictionary.u16(tree);
var familyDict  = TaoDictionary.u16(tree);
var speciesDict = TaoDictionary.u32(tree);
var countryDict = TaoDictionary.u16(tree);
var stateDict   = TaoDictionary.u16(tree);
var fieldDict   = TaoDictionary.u16(tree);
// totalWidth = 2 + 2 + 2 + 4 + 2 + 2 + 2 = 16 bytes

var enc = new KeyBuilder(layout, arena);
enc.setDict(0, "Animalia")
   .setDict(1, "Chordata")
   .setDict(2, "Accipitridae")
   .setDict(3, "Haliaeetus leucocephalus")
   .setDict(4, "US")
   .setDict(5, "Wyoming")
   .setDict(6, "header");

try (var w = dataArt.write()) {
    long leafRef = w.getOrCreate(enc.key(), enc.keyLen(), 0);
    w.leafValue(leafRef).set(ValueLayout.JAVA_LONG, 0, witnessHash);
}
```

---

## 10. TaoDictionary

### 10.1 Overview

A `TaoDictionary` is a small ART with variable-length string keys and fixed-size integer
leaves. It maps string identifiers (taxonomy names, geographic names, field names) to
compact monotonic integer codes.

### 10.2 Structure

```java
public final class TaoDictionary {
    private final TaoTree owner;
    private final TaoTree tree;             // string-keyed ART, 4-byte leaves
    private int nextCode;               // monotonic counter
    private final int maxCode;          // budget cap

    public static TaoDictionary u16(TaoTree tree) {
        return new TaoDictionary(tree, 0xFFFF, 1);
    }

    public static TaoDictionary u32(TaoTree tree) {
        return new TaoDictionary(tree, Integer.MAX_VALUE, 1);
    }
}
```

### 10.3 Resolve-or-assign

Self-locking: acquires the tree's write lock (reentrant if already held by a `WriteScope`).

```java
public int intern(String value) {
    byte[] padded = encodeAndPad(value);
    owner.acquireWriteLock();
    try {
        long leafRef = tree.getOrCreateImpl(...);
        int existing = tree.leafValueImpl(leafRef).get(JAVA_INT_UNALIGNED, 0);
        if (existing != 0) return existing;

        int code = nextCode++;
        tree.leafValueImpl(leafRef).set(JAVA_INT_UNALIGNED, 0, code);
        return code;
    } finally {
        owner.releaseWriteLock();
    }
}
```

### 10.4 Resolve-only (no assignment)

Self-locking: acquires the owner's read lock.

```java
public int resolve(String value) {
    byte[] padded = encodeAndPad(value);
    owner.acquireReadLock();
    try {
        long leafRef = tree.lookupImpl(...);
        if (NodePtr.isEmpty(leafRef)) return -1;
        return tree.leafValueImpl(leafRef).get(JAVA_INT_UNALIGNED, 0);
    } finally {
        owner.releaseReadLock();
    }
}
```

### 10.5 Reserved ranges

Dictionaries support reserving a range of codes for well-known values. For example, the
species tracker's field dictionary might reserve:

```
code = 0              reserved (null sentinel)
code = 1              observation header
code = 2..31          well-known fields (observer, locality, scientificName, ...)
code = 32..N          custom fields (monotonic)
```

The `reservedRangeEnd` parameter allows pre-populating known attribute names at fixed IDs
before opening the monotonic allocation range for custom attributes.

### 10.6 Persistence

TaoDictionary ARTs serialize the same way as data ARTs. On restart:

1. Deserialize dictionary ART from its persisted form (or replay append-only log).
2. Scan all leaves to find `max(existing codes)`.
3. Set `nextCode = max + 1`.
4. Resume normal operation.

No separate persistence format needed. One serialization path for all ARTs.

---

## 11. Leaf Value Layouts

The ART core is agnostic to leaf contents. It allocates a fixed-size segment per leaf class
and returns a `MemorySegment` reference. The caller interprets the bytes.

### 11.1 TaoDictionary code leaf (4 bytes)

Used by `TaoDictionary`.

```java
StructLayout DICT_LEAF = MemoryLayout.structLayout(
    ValueLayout.JAVA_INT.withName("code")
);
```

If the ART instance has leaf size ≤ 7 bytes and `LEAF_INLINE` is enabled, the code is stored
directly in the `NodePtr` payload, avoiding a slab allocation entirely.

### 11.2 Witness leaf (16 bytes)

Used by the data ART for lightweight change-detection (stores a hash of the record).

```java
StructLayout WITNESS_LEAF = MemoryLayout.structLayout(
    ValueLayout.JAVA_LONG.withName("witness"),       // 8B
    ValueLayout.JAVA_INT .withName("timestamp"),     // 4B
    MemoryLayout.paddingLayout(4)                    // 4B pad to 16
);
```

### 11.3 Observation header leaf (32 bytes)

Used by the data ART for structured per-record metadata (e.g., GBIF species-location state).

```java
StructLayout HEADER_LEAF = MemoryLayout.structLayout(
    ValueLayout.JAVA_INT  .withName("firstSeen"),            // 4B
    ValueLayout.JAVA_INT  .withName("lastSeenObserved"),     // 4B
    ValueLayout.JAVA_INT  .withName("lastSeenForwarded"),    // 4B
    ValueLayout.JAVA_INT  .withName("lastUpsertedMinute"),   // 4B
    ValueLayout.JAVA_SHORT.withName("upsertCount"),          // 2B
    ValueLayout.JAVA_SHORT.withName("flags"),                // 2B
    MemoryLayout.paddingLayout(12)                           // 12B pad to 32
);
```

### 11.4 TaoString leaf (16 bytes)

A 16-byte fixed-width leaf that stores variable-length string values using a dual
representation inspired by [CedarDB/Umbra "German Strings"](https://cedardb.com/blog/strings_deep_dive/):
short strings are inlined entirely, long strings store a prefix plus a
swizzled pointer to the `BumpAllocator`.

#### 11.4.1 Layout

```
Short string (len ≤ 12):
┌──────────────────┬──────────────────────────────────────┐
│ len     (4B)     │ data inline (12B, zero-padded)       │
└──────────────────┴──────────────────────────────────────┘

Long string (len > 12):
┌──────────────────┬──────────────┬───────────────────────┐
│ len     (4B)     │ prefix (4B)  │ overflowPtr (8B)      │
│                  │ first 4 chars│ swizzled → BumpAllocator│
└──────────────────┴──────────────┴───────────────────────┘
```

Both representations occupy exactly 16 bytes — a single slab class.

```java
StructLayout TAO_STRING_LEAF = MemoryLayout.structLayout(
    ValueLayout.JAVA_INT.withName("len"),              // 4B
    MemoryLayout.sequenceLayout(12, JAVA_BYTE)
        .withName("payload")                           // 12B
);
```

#### 11.4.2 Key design properties

1. **Short strings are free.** Strings ≤ 12 bytes (which covers hostnames, IPs, status enums,
   ISO codes, most metric names) live entirely in the 16-byte slab segment. Zero overflow
   allocation, zero pointer chasing.

2. **Fast equality rejection.** Comparing the first 8 bytes (len + first 4 bytes of data) as
   a single `long` rejects most mismatches in one instruction — before touching overflow
   memory. This is the CedarDB insight: in joins and filters, most comparisons are
   *inequalities*, and the prefix resolves them cheaply.

3. **Fixed slab size.** The leaf header is always 16 bytes regardless of string length. The
   variable-length part lives in the `BumpAllocator`. This keeps the slab allocator simple.

4. **Immutable payloads.** The overflow data never changes after write. This enables lock-free
   reads and trivial persistence (the overflow pages can be mmap'd directly).

#### 11.4.3 TaoString utility class (`org.taotree.TaoString`)

```java
public final class TaoString {
    private static final int SHORT_THRESHOLD = 12;

    // --- Write ---

    /** Write a string value into a TaoString leaf. */
    public static void write(MemorySegment leaf, byte[] utf8, TaoTree tree) {
        // Short: inline entirely in the 16-byte leaf
        // Long: store 4-byte prefix + allocate overflow via tree
    }

    // --- Read ---

    /** Read the full string bytes from a TaoString leaf. */
    public static byte[] read(MemorySegment leaf, TaoTree tree) { ... }

    // --- Equality ---

    /** Fast equality: rejects most mismatches via 8-byte first-word compare. */
    public static boolean equals(MemorySegment a, MemorySegment b, TaoTree tree) { ... }

    /** Compare a TaoString leaf against a raw byte[]. */
    public static boolean equalsBytes(MemorySegment leaf, byte[] utf8, TaoTree tree) { ... }

    // --- Helpers ---

    public static boolean isShort(MemorySegment leaf) { ... }
    public static int length(MemorySegment leaf) { ... }
}
```

#### 11.4.4 When to use TaoString leaves

| Scenario | Leaf class | Why |
|---|---|---|
| Attribute values that must be forwarded downstream | TaoString | Need the actual value, not just a witness hash |
| Entity display names, labels, tags | TaoString | Variable-length, mostly short |
| General-purpose off-heap string KV store | TaoString | Makes the ART a real key-value store |
| Change detection (witness only) | Witness (11.2) | Only need hash + timestamp, not the value |

The TaoString leaf can coexist with witness leaves in the same ART using different leaf
classes. The caller selects the class at insert time based on the attribute's role.

### 11.5 Leaf class selection at insert time

```java
static final int LEAF_CLASS_DICT    = 0;  //  4 bytes
static final int LEAF_CLASS_ATTR    = 1;  // 16 bytes
static final int LEAF_CLASS_HEADER  = 2;  // 32 bytes
static final int LEAF_CLASS_GSTRING = 3;  // 16 bytes (TaoString)

// The caller knows which class to use based on the attribute's role:
int leafClass = switch (attrRole) {
    case HEADER        -> LEAF_CLASS_HEADER;
    case WITNESS_ONLY  -> LEAF_CLASS_ATTR;
    case VALUE_STRING  -> LEAF_CLASS_GSTRING;
};
long leafRef = dataArt.getOrCreate(key, keyLen, leafClass);
```

---

## 12. Radix Key Encoding Utilities

```java
public final class TaoKey {

    // Unsigned integers: big-endian byte swap
    public static void encodeU16(MemorySegment seg, long offset, short value) {
        seg.set(ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN),
                offset, value);
    }

    public static void encodeU32(MemorySegment seg, long offset, int value) {
        seg.set(ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN),
                offset, value);
    }

    public static void encodeU64(MemorySegment seg, long offset, long value) {
        seg.set(ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN),
                offset, value);
    }

    // Signed integers: flip sign bit + big-endian
    public static void encodeI64(MemorySegment seg, long offset, long value) {
        seg.set(ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN),
                offset, value ^ Long.MIN_VALUE);
    }

    // Strings: escape 0x00/0x01 bytes, null-terminate
    public static byte[] encodeString(String value) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        int escapeCount = 0;
        for (byte b : raw) {
            if ((b & 0xFF) <= 1) escapeCount++;
        }
        byte[] key = new byte[raw.length + escapeCount + 1];
        int pos = 0;
        for (byte b : raw) {
            if ((b & 0xFF) <= 1) key[pos++] = 0x01;
            key[pos++] = b;
        }
        key[pos] = 0x00;
        return key;
    }
}
```

---

## 13. Memory Budget and Eviction

### 13.1 Per-ART memory accounting

Each ART tracks its total allocated memory across all slab classes. This enables:

- Per-instance memory limits (e.g., data ART capped at 500 MB).
- Per-dictionary memory limits (e.g., attribute dictionary capped at 10 MB).

### 13.2 Eviction (data ART only)

Under memory pressure, cold state may be evicted. The eviction strategy is outside the ART
core library — it is the responsibility of the application layer built on top.

Possible approaches:

- **Subtree eviction**: remove all entries for a specific key prefix (e.g., a kingdom or family).
- **LRU/CLOCK at the application level**: track access recency in the observation header leaf
  and periodically scan + delete cold records.

The ART core provides the `scan` and `delete` primitives needed for these strategies.

---

## 14. Persistence

The ART supports file-backed persistence via memory-mapped I/O. All pointers are swizzled
`(slabClassId, slabId, offset)` values — position-independent and serializable without
pointer translation.

### 14.1 File format

A single file with 4 KB pages, mapped via `FileChannel.map()` in 64 MB chunked windows:

```
┌──────────────────────────────────────────────────────────────┐
│  Page 0: Superblock (magic, version, all metadata)           │
├──────────────────────────────────────────────────────────────┤
│  Pages 1+: Slab data, bitmask data, bump pages              │
│  (allocated sequentially by SlabAllocator + BumpAllocator)   │
│                                                              │
│  [slab data: 256 pages = 1 MB] [bitmask: 1-2 pages]         │
│  [bump page: 16 pages = 64 KB]                               │
│  [slab data: 256 pages] [bitmask: 1-2 pages]                │
│  ...                                                         │
│  [free space]                                                │
└──────────────────────────────────────────────────────────────┘
```

The file grows by appending 64 MB chunks (one `mmap()` per chunk, keeping VMA count low).
On macOS/APFS, files are sparse — physical disk usage matches actual data written.

### 14.2 Checkpoint (v2 format)

Uses a v2 checkpoint format with mirrored A/B slots, CRC-32C integrity, and shadow paging
for crash-safe updates. Contains all metadata needed to reconstruct allocator and tree state
on reopen:

- ChunkStore state: totalPages, nextPage, chunkSize
- SlabAllocator class registry: per-class segmentSize, slabCount, page locations, segmentsInUse
- BumpAllocator state: pageCount, currentPage, bumpOffset, page locations
- Tree descriptors: root pointer, size, keyLen, node class IDs, leaf class IDs
- Dictionary descriptors: nextCode, maxCode, treeIndex

### 14.3 Chunked mapping windows

Each 64 MB chunk is one `FileChannel.map()` call returning a single `MemorySegment`.
Slabs and bump pages are `chunk.asSlice()` from the containing chunk.

When a page allocation would straddle a chunk boundary, it skips to the next chunk.
This ensures every slab is contiguous within a single mapping.

### 14.4 File-backed mode

All trees are file-backed via `ChunkStore`. The `SlabAllocator` and `BumpAllocator`
allocate pages from the chunk store, which maps them via `FileChannel.map()`.

Bitmasks use `MemorySegment` backed by the mapped file. Bitmask writes are
immediately visible in the mapped file — no separate sync step for allocation tracking.

### 14.5 API

```java
// Create a new file-backed tree
var tree = TaoTree.create(Path.of("data.tao"), 16, 24);

// Open an existing file-backed tree
var tree = TaoTree.open(Path.of("data.tao"));

// Explicit sync (writes superblock, forces mappings)
tree.sync();

// Close (auto-syncs)
tree.close();
```

### 14.6 Durability model

- `sync()` writes the checkpoint (v2: mirrored A/B with CRC-32C) and calls `force()` on dirty chunks.
- `compact()` migrates arena nodes to slab, writes checkpoint, reclaims epochs.
- `close()` calls `sync()` automatically.
- All three acquire `writeLock` → `commitLock` to coordinate with concurrent writers.
- Crash between writes and `sync()` recovers from the last valid checkpoint slot (shadow paging).
- On macOS, `force()` uses `msync(MS_SYNC)`. For full durability, `F_FULLFSYNC` is needed
  (future enhancement).

### 14.7 Future work

- WAL for fine-grained crash recovery (currently relies on checkpoint + shadow paging)
- `F_FULLFSYNC` on macOS for guaranteed durability

---

## 15. Performance Considerations

### 15.1 Cache efficiency

- Node4 (40B) fits in one cache line.
- Node16 (152B) fits in 2-3 cache lines.
- Prefix nodes are compact and often fit in one cache line.
- Leaf inlining for dictionary ARTs eliminates one pointer chase per lookup.
- TaoString short representation (≤ 12 chars) avoids overflow pointer chase entirely.
- TaoString equality check rejects most mismatches by comparing 8 bytes
  (len + prefix) as a single `long` — one instruction, no overflow access.

### 15.2 JIT-friendly patterns

- `VarHandle` operations are JVM intrinsics — they compile to single MOV/BSWAP instructions.
- Sealed `KeyField` hierarchy enables exhaustive switch — JIT devirtualizes and inlines.
- No boxing on the hot path — all key components are primitives.
- `NodePtr` encoding/decoding is pure bitwise arithmetic — no allocation.

### 15.3 Benchmarking plan

JMH benchmarks from day one:

1. **Point lookup throughput** — the primary hot path. Target: millions of ops/sec.
2. **Insert throughput** — new records. Measure with varying key distributions.
3. **Mixed workload** — 95% lookup + 5% insert, simulating steady-state ingestion.
4. **TaoDictionary resolve throughput** — string-to-code resolution via `TaoDictionary`.
5. **Comparison baselines** — `HashMap<Long, byte[]>`, Chronicle Map, flat open-addressed
   hash table over FFM.

---

## 15b. GBIF Benchmark: Species Observation Tracker

### 15b.1 Overview

The primary benchmark for TaoTree uses the **GBIF Species Occurrences** dataset — 2.8+ billion
documented observations of living organisms on Earth, published by the
[Global Biodiversity Information Facility](https://www.gbif.org/).

The benchmark frames TaoTree as a **species observation tracker**: each species is an entity,
each geographic location where it's been observed is an attribute of that entity. When a new
observation arrives, TaoTree decides FORWARD (new or changed) or SUPPRESS (duplicate).

This benchmark uses a publicly available, non-proprietary dataset with natural hierarchy and
dictionary encoding at every key level.

### 15b.2 Dataset

**Source:** GBIF Parquet occurrence snapshots on AWS Open Data  
**Bucket:** `s3://gbif-open-data-us-east-1/occurrence/{YYYY-MM-DD}/occurrence.parquet/`  
**License:** CC0 / CC-BY 4.0  
**Size:** ~2 billion occurrence records, 6,638 Parquet partitions, **269 GB** total  
**Download:** `aws s3 cp s3://gbif-open-data-us-east-1/occurrence/2026-04-01/occurrence.parquet/000001 . --no-sign-request`  
**Schema:** All column names are lowercase. Key columns are strings. Coordinates are doubles.  
**Updates:** Monthly snapshots. Each snapshot is a full replacement.

Relevant columns from the Parquet schema:

| Column | Parquet type | Example | Role |
|---|---|---|---|
| `kingdom` | string | "Animalia" | key: dict:u16 |
| `phylum` | string | "Chordata" | key: dict:u16 |
| `family` | string | "Accipitridae" | key: dict:u16 |
| `species` | string | "Haliaeetus leucocephalus" | key: dict:u32 |
| `countrycode` | string | "US" | key: dict:u16 |
| `stateprovince` | string | "Wyoming" | key: dict:u16 |
| `gbifid` | string | "4753425043" | occurrence identity |
| `specieskey` | string | "2480528" | GBIF backbone integer (not used in key) |
| `locality` | string | "Yellowstone Lake, near fishing bridge" | TaoString value |
| `recordedby` | list\<struct\> | [{"array_element": "John Smith"}] | TaoString value |
| `scientificname` | string | "Haliaeetus leucocephalus (Linnaeus, 1766)" | TaoString value |
| `basisofrecord` | string | "HUMAN_OBSERVATION" | TaoString value |
| `eventdate` | timestamp[ms] | 2026-03-15T00:00:00Z | leaf field |
| `decimallatitude` | double | 10.955249 | leaf field |
| `decimallongitude` | double | -74.295559 | leaf field |

### 15b.3 Real data characteristics (from partition 000001, 306K rows)

**Null rates** — critical for key field selection:

| Field | Nulls | Rate | Key viable? |
|---|---|---|---|
| `kingdom` | 0 | 0.0% | Yes |
| `phylum` | 0 | 0.0% | Yes |
| **`class`** | **146,684** | **47.9%** | **No — too many nulls** |
| `order` | 1,932 | 0.6% | Ok with sentinel |
| `family` | 1,981 | 0.6% | Yes (sentinel for nulls) |
| `species` | 7,287 | 2.4% | Yes (sentinel for nulls) |
| `countrycode` | 0 | 0.0% | Yes |
| **`stateprovince`** | **122,168** | **39.9%** | **Yes (sentinel for nulls)** |

`class` is 48% null because many organisms (fish, invertebrates, microbes) are not classified
to class level in the GBIF backbone. We use `phylum` + `family` instead, which together cover
the same taxonomic resolution with ≤1% null rate.

`stateprovince` is 40% null because many countries don't report sub-national locations. Null
maps to sentinel code 0 ("unknown") in the dictionary — a perfectly valid key component.

**Cardinality per field** (single partition):

| Field | Unique values | Expected full dataset |
|---|---|---|
| `kingdom` | 4 | ~7 |
| `phylum` | 12 | ~80 |
| `family` | 860 | ~10,000 |
| `species` | 7,883 | ~2,500,000 |
| `countrycode` | 2 (CO, FR) | ~250 |
| `stateprovince` | 2 | ~50,000 |
| `basisofrecord` | 1 | ~8 |

Note: single partition has low geographic diversity (data is not partitioned by country).

**TaoString length distributions:**

| Field | Non-null | Avg len | Median | ≤12B (short) | >12B (long) |
|---|---|---|---|---|---|
| `species` | 298,713 | 17.4B | 17B | 9.9% | 90.1% |
| `scientificname` | 306,000 | 35.6B | 35B | 0.7% | 99.3% |
| `locality` | 295,463 | 41.1B | 40B | 3.9% | 96.1% |

Species names peak at 13-20 bytes (71% of values), just over the 12-byte TaoString short
threshold. Locality strings are almost always long (median 40 bytes) — heavy BumpAllocator
usage. This is a realistic stress test for the long-string path.

**Species name length histogram:**

```
  0- 8 bytes:      0 ( 0.0%)
  9-12 bytes: 29,460 ( 9.9%) ###
 13-16 bytes: 97,603 (32.7%) #############
 17-20 bytes:114,361 (38.3%) ###############
 21-25 bytes: 55,672 (18.6%) #######
 26-30 bytes:  1,602 ( 0.5%)
 31-40 bytes:     15 ( 0.0%)
```

### 15b.4 Data model mapping

```
Key field:                      GBIF benchmark:
──────────────────────          ──────────────────────────────────────
Hierarchy level 1            →  kingdom(dict:u16)       "Animalia"
Hierarchy level 2            →  phylum(dict:u16)        "Chordata"
Hierarchy level 3            →  family(dict:u16)        "Accipitridae"
Entity identity              →  species(dict:u32)       "Haliaeetus leucocephalus"
Location (country)           →  country(dict:u16)       "US"
Location (region)            →  state(dict:u16)         "Wyoming"
Field selector               →  field(dict:u16)         "observer"
```

A **species** is the entity. The **locations** where it's been observed (country + state) are
analogous to attributes of that entity. Each (species, location) pair may have multiple
**fields** tracking the latest observation state.

### 15b.5 Key layout

```
kingdom(dict:u16) | phylum(dict:u16) | family(dict:u16) | species(dict:u32) | country(dict:u16) | state(dict:u16) | field(dict:u16)
     2B               2B                2B                 4B                  2B                  2B                2B  = 16 bytes
```

Seven dictionary-encoded string fields in 16 bytes. All resolved by TaoTree's `TaoDictionary`
— no external pre-assigned integer IDs from GBIF. Null values map to sentinel code 0.

```java
var tree = TaoTree.forDictionaries();

var kingdomDict  = TaoDictionary.u16(tree);   // ~7 values
var phylumDict   = TaoDictionary.u16(tree);   // ~80 values
var familyDict   = TaoDictionary.u16(tree);   // ~10,000 values
var speciesDict  = TaoDictionary.u32(tree);   // ~2,500,000 values
var countryDict  = TaoDictionary.u16(tree);   // ~250 values
var stateDict    = TaoDictionary.u16(tree);   // ~50,000 values
var fieldDict    = TaoDictionary.u16(tree);   // ~20 values

var layout = KeyLayout.of(
    KeyField.dict16("kingdom", kingdomDict),
    KeyField.dict16("phylum",  phylumDict),
    KeyField.dict16("family",  familyDict),
    KeyField.dict32("species", speciesDict),
    KeyField.dict16("country", countryDict),
    KeyField.dict16("state",   stateDict),
    KeyField.dict16("field",   fieldDict)
);
// totalWidth = 2 + 2 + 2 + 4 + 2 + 2 + 2 = 16 bytes
```

### 15b.6 Null handling

Null taxonomy or geography values are resolved to sentinel code 0 in the corresponding
dictionary. The empty string `""` is encoded as the null-terminated escape sequence `[0x00]`
by `TaoKey.encodeString()` and maps to a valid ART key in the dictionary.

```java
// In the ingest loop:
String species = record.species();  // may be null
encoder.setDict(3, species != null ? species : "");  // "" → sentinel code 0
```

This means "unclassified to species level" observations still get indexed. Their key simply
contains 0x0000 in the species field, and they cluster together in the ART under the
sentinel prefix — which is actually useful for queries like "show me all unidentified
observations in family Gerreidae".

### 15b.7 Leaf types

Three leaf classes coexist in the same ART:

**Observation header leaf (24 bytes)** — stored at `field="header"`:

```java
StructLayout OBSERVATION_LEAF = MemoryLayout.structLayout(
    ValueLayout.JAVA_INT  .withName("firstSeenDate"),      // 4B  epoch day
    ValueLayout.JAVA_INT  .withName("lastSeenDate"),       // 4B  epoch day
    ValueLayout.JAVA_SHORT.withName("observationCount"),   // 2B
    ValueLayout.JAVA_SHORT.withName("flags"),              // 2B
    ValueLayout.JAVA_LONG .withName("witness"),            // 8B  hash of latest observation
    MemoryLayout.paddingLayout(4)                          // 4B  pad to 24
);
```

**TaoString leaf (16 bytes)** — stored at `field="observer"`, `field="locality"`,
`field="scientificName"`, `field="basisOfRecord"`:

```java
// "Elops smithi"       → 12 bytes → short representation, fully inline (just fits!)
// "Centropomus undecimalis" → 23 bytes → 4B prefix "Cent" + overflow
// "Isla Del Rosario, Ciénaga Grande de Santa Marta" → 48 bytes → prefix + overflow
// "HUMAN_OBSERVATION"  → 17 bytes → 4B prefix "HUMA" + overflow
```

Real-world distribution: ~10% of species names fit in the short representation,
~96% of locality strings require overflow. This exercises both paths.

**TaoDictionary code leaf (4 bytes)** — used internally by the 7 `TaoDictionary` instances.

### 15b.8 Prefix sharing in the ART

The 16-byte compound key enables deep prefix sharing:

```
All Animalia share prefix:                         [00 01 | ...]                         (2B)
All Animalia → Chordata share prefix:              [00 01 | 00 02 | ...]                 (4B)
All Animalia → Chordata → Accipitridae share:      [00 01 | 00 02 | 01 A3 | ...]        (6B)
All Bald Eagles share prefix:                      [... | 00 00 04 3F | ...]             (10B)
All Bald Eagles in US share:                       [... | 00 FE | ...]                   (12B)
All Bald Eagles in US → Wyoming share:             [... | 00 FE | 03 A1 | ...]          (14B)
```

With 2.8B occurrences mapped to ~2.5M species × ~50K locations, the ART's inner nodes
at the taxonomy levels are heavily shared, reducing memory consumption compared to a flat
hash map.

### 15b.9 Prefix scan queries

The hierarchical key enables natural prefix scans via `scan(queryBuilder, handle, visitor)`:

```
// All observations in kingdom Animalia
scan(encode(kingdom="Animalia"), 2, consumer)

// All Chordata observations
scan(encode(kingdom="Animalia", phylum="Chordata"), 4, consumer)

// All birds of prey (family Accipitridae)
scan(encode(..., family="Accipitridae"), 6, consumer)

// All Bald Eagle observations worldwide
scan(encode(..., species="Haliaeetus leucocephalus"), 10, consumer)

// All Bald Eagles in the US
scan(encode(..., country="US"), 12, consumer)

// All Bald Eagles in Wyoming
scan(encode(..., state="Wyoming"), 14, consumer)
```

### 15b.10 Upsert flow

```
New GBIF observation arrives (from Parquet row):
  kingdom="Animalia", phylum="Chordata", family="Accipitridae",
  species="Haliaeetus leucocephalus",
  countrycode="US", stateprovince="Wyoming",
  recordedby=[{"array_element": "John Smith"}],
  locality="Yellowstone Lake, near fishing bridge",
  basisofrecord="HUMAN_OBSERVATION",
  eventdate=2026-03-15T00:00:00Z

Step 1: Resolve 7 dictionaries (TaoDictionary lookups)
  "Animalia"                      → kingdom  code 1
  "Chordata"                      → phylum   code 2
  "Accipitridae"                  → family   code 419
  "Haliaeetus leucocephalus"      → species  code 1087
  "US"                            → country  code 254
  "Wyoming"                       → state    code 929
  "header"                        → field    code 1

Step 2: Encode 16-byte key
  [00 01 | 00 02 | 01 A3 | 00 00 04 3F | 00 FE | 03 A1 | 00 01]

Step 3: Inside a write scope:
  try (var w = dataArt.write()) {
      long leaf = w.getOrCreate(key, 16, 0);
      → Leaf exists?
        Yes → read witness → hash(observation) == witness?
          Same   → SUPPRESS (duplicate sighting, skip)
          Changed → FORWARD (update leaf: lastSeenDate, witness, count++)
        No  → FORWARD (new species-location pair, write initial leaf)
  }

Step 4: For TaoString fields, encode keys with field="observer", "locality", etc.
  try (var w = dataArt.write()) {
      long leaf = w.getOrCreate(key_observer, 16, LEAF_CLASS_TAOSTRING);
      TaoString.write(w.leafValue(leaf), "John Smith".getBytes(UTF_8), tree);
        → 10 bytes, short representation, fully inline in 16B leaf
  }
```

### 15b.11 Benchmark scenarios

| Benchmark | What it measures | Expected workload |
|---|---|---|
| **Full ingest** | Insert throughput, dictionary build-up | Load all 2.8B occurrences, measure wall clock and memory |
| **Duplicate detection** | Suppress rate, lookup throughput | Re-ingest same data, expect ~95%+ SUPPRESS rate |
| **Incremental update** | Mixed insert/lookup | Load base dataset, then apply a day's worth of new observations |
| **Prefix scan** | Range query performance | "All birds in Germany", "All mammals in Brazil" |
| **Memory efficiency** | Bytes per entry vs. HashMap baseline | Compare total off-heap memory for N entries |
| **TaoDictionary overhead** | TaoDictionary resolve throughput | Resolve 2.5M species names to u32 codes, measure ops/sec |
| **TaoString overflow** | BumpAllocator stress test | Locality strings (median 40B) exercise the long-string path |

### 15b.12 Dataset sizing

| Subset | Partitions | Records | Download | Use |
|---|---|---|---|---|
| 1 partition | 1 | ~306K | 17 MB | Unit tests, schema exploration |
| 10 partitions | 10 | ~3M | ~200 MB | Development, quick benchmarks |
| 100 partitions | 100 | ~30M | ~4 GB | Integration benchmarks |
| Full planet | 6,638 | ~2B | 269 GB (Parquet) | Headline benchmark |

Start with 1 partition for development. Scale to 10-100 for integration benchmarks.
Full planet for the headline number.

**Download commands:**

```bash
# Single partition (development)
aws s3 cp s3://gbif-open-data-us-east-1/occurrence/2026-04-01/occurrence.parquet/000001 \
  gbif-data/000001.parquet --no-sign-request

# First 10 partitions (quick benchmark)
for i in $(seq -w 1 10); do
  aws s3 cp s3://gbif-open-data-us-east-1/occurrence/2026-04-01/occurrence.parquet/0000${i} \
    gbif-data/0000${i}.parquet --no-sign-request
done

# Full dataset (headline benchmark)
aws s3 sync s3://gbif-open-data-us-east-1/occurrence/2026-04-01/occurrence.parquet/ \
  gbif-data/ --no-sign-request
```

---

## 16. Open Questions

1. **Prefix node capacity.** DuckDB uses 8 bytes. Should we use a larger default (e.g., 12 or
   16) given that our data ART keys are 16 bytes and dictionary ART keys can be longer?
2. **LEAF_INLINE threshold.** Inline leaves in the NodePtr payload for leaves ≤ 7 bytes? This
   helps dictionary ARTs (4-byte codes) but adds a branch to every leaf access.
3. **Concurrency model.** Resolved: ROWEX model — lock-free readers (epoch-based snapshots),
   concurrent writers (optimistic COW + `commitLock`), coordinated persistence
   (`writeLock` → `commitLock`). Fray + Lincheck provide ongoing safety verification.
4. **Slab sizing.** How large should each slab be? Larger slabs reduce allocation frequency
   but increase memory granularity. 1 MB or 4 MB per slab are reasonable starting points.
5. **Vacuum frequency.** How often to compact slab occupancy, and whether to do it
   incrementally or in batch.
6. **Key storage at leaves.** For variable-length keys, the full key must be stored at the leaf
   for lazy expansion verification. For fixed-width keys, this can be omitted if the key is
   fully consumed by the trie path (no lazy expansion). Should we always store keys, or
   optimize for the fixed-width case?
7. **TaoString update semantics.** When a TaoString leaf's value changes (e.g., an
   entity attribute is updated with a new string value), the old overflow payload becomes dead
   space. Should we track update frequency to trigger compaction proactively, or rely on a
   periodic/threshold-based vacuum schedule?
8. **BumpAllocator page size.** 64 KB is the default, but larger pages (256 KB, 1 MB) reduce
   page-array growth at the cost of more wasted tail space. Should page size be configurable
   per arena, or globally?

---

## 17. Component Dependency Order

Implementation should proceed in this order:

```
1. SlabAllocator + BumpAllocator + NodePtr / OverflowPtr encoding
   └─ foundation for everything else

2. ART node types (Node4, Node16, Node48, Node256, Prefix)
   └─ depends on SlabAllocator

3. ART core operations (lookup, insert, delete)
   └─ depends on node types

4. TaoKey encoding utilities
   └─ standalone, no dependencies

5. TaoString leaf utility
   └─ depends on SlabAllocator + BumpAllocator

6. KeyLayout + KeyBuilder
   └─ depends on TaoKey

7. TaoDictionary
   └─ depends on ART core + TaoKey (string encoding)

8. JMH benchmark harness
   └─ depends on all of the above

9. GBIF species tracker (benchmark application)
   └─ depends on all of the above
```
