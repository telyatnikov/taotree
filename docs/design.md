# TaoTree: Adaptive Radix Tree Library for Java Foreign Function & Memory API

**Name:** TaoTree (`org.taotree`)  
**Status:** Draft  
**Date:** 2026-04-02 (last revised 2026-04-20)

> **2026-04-20 notice — design vs code.** The public API and on-disk format
> diverged from this document during the temporal-unification refactor
> (Phases 1–7). The code is the source of truth; this document is being
> updated incrementally. Known pivots:
>
> 1. **TaoTree is always temporal.** There is no more user-defined
>    `LeafLayout` / `LeafField` / `LeafHandle` / `LeafAccessor` /
>    `ConflictResolver`. Those types are deleted from the public API.
>    Sections that still describe a generic "data ART with structured leaf
>    values" describe an earlier state — read them as historical context.
> 2. **24-byte inline `EntityNode`, not 8-byte `EntityNodeRef` or 40-byte.** Each
>    leaf of the global ART holds the full `EntityNode` struct directly; there
>    is no indirection. Layout: `{current_state_root_ref, attr_art_root_ref,
>    versions_art_root_ref}` — three 8-byte fields. An earlier draft of this
>    doc described a 40-byte layout with archive/metadata refs — those
>    fields were dropped in Phase 4 (they would only be populated by
>    not-yet-designed archive tiers). See §18.3.
> 3. **`Value` tagged union, not raw `byte[]`.** Attribute values are
>    instances of `org.taotree.Value` (int / long / float / double / bool /
>    string / bytes / json / null) and serialize through `ValueCodec` into
>    a 16-byte slot or a bump-allocated overflow payload.
> 4. **Checkpoint v3 with `FEATURE_INCOMPAT_UNIFIED_TEMPORAL = 0x0004`.**
>    v2 files are rejected.
> 5. **§18.12.3 compactor is aspirational.** The shipped `Compactor` walks
>    only the outer ART; per-entity nested CHAMP / AttributeRuns ART /
>    EntityVersions ART are not repacked and old slab nodes are not
>    retired. This is a space + locality gap, not data loss —
>    `CompactorSpaceReclaimTest` in `core/src/test/java/org/taotree/`
>    encodes the empirical bound. Tracked as `p8-compactor-temporal`.
> 6. **Rebase on conflict uses `TemporalOpLog`, not `MutationLog` segment
>    copy.** See §0 "Concurrency model" below.
> 7. **Schema fingerprint on reopen is implemented** (`SchemaBinding`
>    section + `open(Path, KeyLayout)` verification + `open(Path)`
>    self-describing reconstruction). Closes former `p8-schema-binding`.
> 8. **`delete(kb, attr)` removes from CHAMP + writes a tombstone run.**
>    Reads past a tombstone return "not found" until a later observation
>    overrides it. Closes former `p8-tombstone-semantics`.
> 9. **Same-logical-value writes extend a run's `last_seen` instead of
>    allocating a new AttributeRun / EntityVersion / CHAMP root.** The
>    public `Value` write path allocates a fresh encoded slot per call, so
>    the writer detects duplicates via `ValueCodec.slotEquals` (byte-level
>    comparison of slot contents), not pointer equality. Raw-API callers
>    of `putTemporal(byte[], int, long, long)` must pass refs produced by
>    `ValueCodec.encodeStandalone` — opaque synthetic longs alias into
>    bump-page bytes and are not supported.

---

## 0. Implementation Status

### Implemented

| Component | Status | Notes |
|---|---|---|
| `SlabAllocator` | Done | Fixed-size slab allocator with bitmask occupancy, file-backed via `ChunkStore` |
| `BumpAllocator` | Done | Bump allocator for variable-length immutable payloads, file-backed via `ChunkStore` |
| `NodePtr` / `OverflowPtr` | Done | 64-bit tagged swizzled pointer encoding |
| Node types (4/16/48/256/PrefixNode) | Done | Static operations on off-heap `MemorySegment` slices |
| `TaoTree` core (lookup/insert/delete) | Done | Fixed-width keys, pessimistic prefix compression |
| `TaoTree` owns infrastructure | Done | Owns `ChunkStore`, allocators, node class IDs, locks |
| `TaoTree.ReadScope` / `TaoTree.WriteScope` | Done | Epoch-based lock-free reads; optimistic COW writes with deferred commit |
| `KeyField` / `KeyLayout` / `KeyHandle` / `KeyBuilder` | Done | Compound-key schema + typed accessors |
| `QueryBuilder` | Done | Resolve-only key builder (lock-free); unknown dict values → empty scan |
| `TaoDictionary` | Done | Self-locking string→int dictionary (fixed padded keys, 128B max) |
| `TaoKey` | Done | Binary-comparable encoding for u8-u64, i8-i64, strings |
| `Value` / `ValueCodec` | Done | Tagged union (9 variants); 12 B inline, overflow to bump |
| Temporal layer (§18) | Done | `EntityNode`, `AttributeRuns` ART, `EntityVersions` ART, `ChampMap`, `TemporalWriter`, `TemporalReader`; unified always-temporal public API (`put`/`getAt`/`history`/...) |
| Scan / prefix scan | Done | `forEach(EntityVisitor)`, `scan(qb, handle, EntityVisitor)` over entity keys |
| COW + epoch reclamation | Done | `CowEngine`, `CowInsert`, `CowDelete`, `CowNodeOps`, `EpochReclaimer` |
| Rebase-on-conflict via `TemporalOpLog` | Done | Per-scope op log replayed against the new root on publish conflict |
| File-backed persistence | Done | `ChunkStore` + checkpoint v3 (mirrored A/B slots, CRC-32C, shadow paging) |
| Compaction (outer ART) | Partial | Post-order repack of outer ART; nested per-entity structures not yet recompacted (see pivot #5 above) |
| JMH benchmarks | Partial | Present; not yet rewritten for unified `Value`/`put` API |
| GBIF species tracker example | Partial | Present; not yet rewritten for unified API |
| Fray concurrency testing | Done | Schedule-point testing (JDK 25, Fray 0.8.3) |
| Lincheck linearizability tests | Done | Stress-based linearizability verification |

### Removed in Phase 4 (temporal unification)

| Type | Replacement |
|---|---|
| `LeafField` / `LeafLayout` / `LeafHandle` | — (leaves are now the internal 40-byte `EntityNode`) |
| `LeafAccessor` / `LeafVisitor` | `EntityVisitor` + `r.get(...)` / `r.getAll(...)` |
| `ExtrasReader` / `ExtrasWriter` | `Value.ofJson(...)` stored as any attribute |
| `ConflictResolver` | Per-scope `TemporalOpLog` replay (not user-pluggable) |
| `TaoTree.create(Path, KeyLayout, LeafLayout)` and raw `create(Path, int, int)` factories | `TaoTree.create(Path, KeyLayout)` |
| `createTemporal` / `openTemporal` / `forDictionaries` | Folded into the primary `create` / `open` |

### Concurrency model

ROWEX (Read-Optimistic Write-EXclusive):

- **Readers** are lock-free. `ReadScope` captures a `PublicationState` (root + size atomically) via `VarHandle.getAcquire` and enters an epoch. Readers never block on writers.
- **Writers** perform optimistic COW outside any lock on the first mutation, then acquire a lightweight `commitLock` to publish the new root. On conflict (another writer published during COW), one bounded redo against the new root is attempted.
- **Rebase on conflict (temporal trees):** each logical write is recorded in a per-scope `TemporalOpLog` (kind ∈ {PUT, DELETE_ATTR}, entityKey, attrId, valueRef, ts). On rebase, the scope's ops are replayed against the newly-published root — each op force-copies the entity leaf under the new root (picking up any other writer's `EntityNode` merges) and re-applies the temporal write via `TemporalWriter.write` / `ChampMap.remove`. Non-temporal child trees (dictionaries) use the legacy `MutationLog` segment-copy path.
- **Persistence** operations (`sync()`/`compact()`/`close()`) acquire the global `writeLock` then `commitLock` (ordering: `writeLock → commitLock`).
- **Dictionaries** (`TaoDictionary`) are self-locking (own lock for `intern`, lock-free for `resolve`).
- **Epoch reclamation** (`EpochReclaimer`) defers freeing of COW-replaced nodes until all readers that could see the old root have exited. Child trees share the parent's reclaimer.

### Future work (not yet implemented)

| Feature | Tracking |
|---|---|
| `p8-compactor-temporal` — recursive per-entity recompaction + retire of old slab nodes (P2) | plan.md Phase 8 |
| `LEAF_INLINE` | Inline small leaf values in the `NodePtr` payload |
| Configurable `prefixCapacity` | Hardcoded to 15 (fits in 24B prefix node); variable-length prefix nodes planned |
| Reserved-range dictionary APIs | `TaoDictionary.u32WithReserved()` |
| Persistence: WAL | Write-ahead log for crash recovery |
| `ImmutableTaoTree.freeze()` | Cold store; see §18.14 |
| HINT index / `scanOverlap` | Hierarchical interval indexing; see §18.15 |
| Archive manifests / cold stubs | See §18.13 |

### Recently closed

| Item | Landed in |
|---|---|
| `p8-schema-binding` — `KeyLayout` fingerprint in `SECTION_SCHEMA_BINDING`, fail-fast on reopen mismatch | `SchemaBinding` + `TaoTree.open` |
| `p8-tombstone-semantics` — explicit tombstone `AttributeRun` for `delete(kb, attr)` | `AttributeRun.TOMBSTONE_VALUE_REF` + readers |
| `p8-value-canonicalization` — byte-level `ValueCodec.slotEquals`, canonical `effectiveValueRef` in `AttrUpsertResult`, fail-fast validation on raw `putTemporal` | `ValueCanonicalizationTest` |
| `p8-split-taotree` — extract `WriteScopeOps` + `ArtRead` from the 3k-line `TaoTree.java` | `WriteScopeOps` / `ArtRead` |
| `p8-champ-iterate-perf` — `ChampMap.iterateRec` reads directly from `BumpAllocator.page()` | 6× iterate speedup |
| `p8-examples` / `p8-perf` | GBIF rewrite + JMH baseline suite in `performance/` |

---

## 1. Abstract

This document specifies the design of TaoTree: a file-backed, always-temporal
entity store implemented entirely in Java using the Foreign Function & Memory
(FFM) API. All data structures live off-heap in `MemorySegment`-backed slab
and bump allocators. There is no reliance on Java object headers, garbage
collection, or heap allocation on the hot path.

> **Historical note.** Earlier revisions of this document described a
> general-purpose ART library with user-defined `LeafLayout` leaves. That
> surface was removed in the Phase 4 temporal-unification refactor. The ART
> node types (`Node4/16/48/256`, `PrefixNode`) are still the internal index
> structure, but they are not part of the public API — they now index entities
> whose values are versioned attributes, addressed via the temporal API
> described in §18.

Internally, the same ART core serves two roles:

1. **Global entity ART** — high-cardinality index with fixed-width compound
   keys (declared by `KeyLayout`) whose leaves are 40-byte `EntityNode`
   structs.
2. **TaoDictionary ART** — low-cardinality string-to-integer mapping with
   variable-length keys, used for `KeyField.dict16`/`dict32` and attribute
   names.

Both roles use the same ART core, the same node types, and the same slab
allocator. Only the key encoding and leaf size differ per instance.

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
│   Fixed-width keys (length known at construction time)       │
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

10. CHAMP persistent map (off-heap)
    └─ depends on SlabAllocator, BumpAllocator, EpochReclaimer

11. TaoTree v3 (temporal layer: EntityNode, AttributeRuns, EntityVersions)
    └─ depends on CHAMP, ART core, TaoDictionary, CowEngine

12. ImmutableTaoTree (cold store)
    └─ depends on TaoTree v3


---

## 18. Temporal Store

### 18.1 Design rationale

TaoTree v3 is a **temporal entity store**. The public class `TaoTree` provides the
temporal API (put, latest, at, history). Internally, it uses a **two-layer entity-local
temporal architecture**: a global ART routes compound keys to per-entity aggregate roots,
each of which owns independent temporal indexes and immutable state snapshots.

**Two layers:**

1. **Global ART** — routes `(tenant, type, object)` prefixes to `EntityNode` pointers
2. **Entity-local structures** — per-entity ART indexes (AttributeRuns, EntityVersions) and
   CHAMP persistent snapshot maps, all reachable from the `EntityNode`

**Why two layers instead of flat ART keys?**

Putting timestamps and attribute IDs in the global ART key (e.g.,
`[prefix | attr | invertedFirstSeen]`) creates problems:

1. **Changing `last_seen` requires delete + reinsert** — it's in the key, so any timestamp
   update is a structural trie mutation (two full COW paths).
2. **"All attributes at time T" requires a full scan** — timestamps are at the end of the key,
   so there is no prefix that selects "all attributes for entity X at time T." The CHAMP
   snapshot map solves this: one predecessor search → one CHAMP iteration.
3. **Multiple access patterns need multiple key orders** — entity-first for history,
   time-first for overlap queries. One trie can't serve both efficiently.

The entity-local design solves all three: `last_seen` updates are leaf-value mutations
(no key change), `allFieldsAt(T)` is one predecessor search in EntityVersions + one CHAMP
iteration, and alternative access patterns (HINT) can be added as a separate index.

**Why not a monolithic block per entity?**

An earlier design stored all of an entity's temporal data in a single contiguous block
(field directory + segment arrays). This is simple but scales poorly:

1. **Full block copy on any mutation** — changing one attribute requires copying all attributes.
   For entities with 13 attributes and 26 runs, that's ~1 KB per `put()`.
2. **No independent attribute access** — reading one attribute requires resolving the entire block.
3. **No structural sharing** — two adjacent EntityVersions that differ in a single attribute
   must each store a complete copy of all attribute values.

The CHAMP snapshot map provides O(1) structural sharing: updating one attribute creates a
new root, sharing most nodes with the previous snapshot. Two adjacent EntityVersions that
differ in one attribute share all CHAMP nodes except the path to the changed entry.

**Why separate AttributeRuns and EntityVersions?**

AttributeRuns are the **canonical source of truth** — they record exactly what each attribute
reported and when. EntityVersions are a **materialized read index** — they cache the
full entity state at each state-change boundary for efficient time-travel queries.

This separation is critical because:

1. **`last_seen` updates are common and cheap.** An attribute reporting the same value only
   extends `last_seen` in its AttributeRun. This does NOT create a new EntityVersion,
   avoiding version explosion.
2. **Late inserts have bounded impact.** A late-arriving attribute reading affects only the
   local window of that attribute's history. The affected-window algorithm (§18.10) rewrites
   only the EntityVersions whose state actually changed.
3. **EntityVersions are reconstructible.** If EntityVersions are lost (cold tier eviction,
   corruption), they can be rebuilt from AttributeRuns by replaying attribute events in order.

### 18.2 Architecture

```
+----------------------------------------------------------------------+
|                       TaoTree (public API)                            |
|                                                                       |
|  +----------------------------------------------------------------+  |
|  |  Global ART (CowEngine #1)                                     |  |
|  |  key: (tenant, type, object) via KeyLayout                     |  |
|  |  leaf value: 8B EntityNodeRef (slab pointer)                   |  |
|  +--------+-------------------------------------------------------+  |
|           |                                                           |
|           v                                                           |
|  +----------------------------------------------------------------+  |
|  |  EntityNode (40B, fixed-size slab allocation)                  |  |
|  |                                                                 |  |
|  |  current_state_root_ref ──> CHAMP (latest full snapshot)       |  |
|  |  attr_art_root_ref ────> AttributeRuns ART (CowEngine #2)      |  |
|  |  versions_art_root_ref ──> EntityVersions ART (CowEngine #3)  |  |
|  |  archive_manifest_ref ──> cold storage manifest                |  |
|  |  metadata_ref ───────────> policy / TTL metadata               |  |
|  +--------+---+---+------------------------------------------+---+  |
|           |   |   |                                           |      |
|     +-----+   |   +-----+                              +-----+      |
|     v         v         v                              v             |
|  +-------+ +--------+ +--------+            +-----------------+      |
|  | CHAMP | | Attr   | | Entity |            | Archive         |      |
|  | Map   | | Runs   | |Versions|            | Manifest        |      |
|  |       | | ART    | | ART    |            | (cold tier)     |      |
|  +-------+ +--------+ +--------+            +-----------------+      |
|  attr_id  key:12B    key:8B                                        |
|  -> value   (sid+fs)   (first_seen)                                  |
|     _ref    leaf:24B   leaf:16B                                      |
|             (SRun)     (ObjVer)                                      |
|                                                                       |
|  +----------------------------------------------------------------+  |
|  |  TaoDictionary: attribute names -> uint32 codes                   |  |
|  +----------------------------------------------------------------+  |
|  +----------------------------------------------------------------+  |
|  |  Shared infrastructure:                                         |  |
|  |    ChunkStore (mmap'd pages via FFM)                            |  |
|  |    SlabAllocator (ART nodes, EntityNodes, CHAMP inner nodes)    |  |
|  |    BumpAllocator (CHAMP entry arrays, value payloads)           |  |
|  |    EpochReclaimer (unified across all CowEngines + CHAMP)       |  |
|  +----------------------------------------------------------------+  |
+----------------------------------------------------------------------+
```

**Data flow summary:**

- **Write** mutates AttributeRuns ART, conditionally updates EntityVersions ART and CHAMP
  maps, writes new EntityNode, publishes via global ART COW.
- **`latest()`** reads EntityNode → `current_state_root` → CHAMP `get(attr_id)`.
  No ART traversal of EntityVersions needed.
- **`at(T)`** reads EntityNode → predecessor in EntityVersions ART → CHAMP
  `get(attr_id)` from the version's `state_root_ref`.
- **`history(attribute)`** reads EntityNode → prefix scan in AttributeRuns ART for
  `[attr_id | *]`.

### 18.3 EntityNode layout

The `EntityNode` is the **aggregate root** for each tracked entity. It is stored
inline as the leaf value of the global ART (i.e., the `(KeyLen + 24)`-byte leaf
slab slot holds the key bytes followed by the EntityNode struct). There is no
indirection through an `EntityNodeRef`.

**EntityNode (24 bytes, inline in global-ART leaf slot):**

```
Offset  Size  Field                    Description
------  ----  -----------------------  -------------------------------------------
0       8     current_state_root_ref   CHAMP root: latest full entity state
8       8     attr_art_root_ref        Per-entity AttributeRuns ART root (NodePtr)
16      8     versions_art_root_ref    Per-entity EntityVersions ART root (NodePtr)
```

**Total: 24 bytes.** An earlier draft reserved two additional 8-byte fields
(`archive_manifest_ref`, `metadata_ref`) for cold-tier archiving and
per-entity policy metadata — both tiers were deferred to a future phase
and the fields were dropped in Phase 4. If/when those tiers are
revived, adding fields extends the leaf slab class by 8 bytes each and
requires a checkpoint format bump.

**Semantics:**

- `current_state_root_ref` is a CHAMP node pointer (§18.6). It represents the latest
  complete state of the entity — a map from `attr_id` (uint32) to `value_ref` (8 bytes).
  This is the fast path for `latest()` queries.
- `attr_art_root_ref` is a `NodePtr` to the root of this entity's AttributeRuns ART.
  `NodePtr.EMPTY_PTR` (0) means no attribute data has been recorded.
- `versions_art_root_ref` is a `NodePtr` to the root of this entity's EntityVersions ART.
  `NodePtr.EMPTY_PTR` means no materialized versions exist (newly created entity or
  cold-evicted).
- `archive_manifest_ref` points to a cold-tier manifest (§18.13). Zero when the entity
  has no archived data.
- `metadata_ref` is reserved for per-entity policy (TTL, correction horizon override).
  Zero when default policy applies.

**Global ART leaf value (8 bytes):**

```
Offset  Size  Field
------  ----  ---------------
0       8     EntityNodeRef (long, slab pointer to EntityNode)
```

When `EntityNodeRef == 0`, the entity has been tombstoned or not yet initialized.

### 18.4 AttributeRuns layout and invariants

AttributeRuns are the **canonical write layer** — the source of truth for all temporal
data. Each AttributeRun records a contiguous time interval during which an attribute reported
a specific value.

**AttributeRuns ART key (12 bytes):**

```
Offset  Size  Field
------  ----  ---------------
0       4     attr_id (uint32, big-endian, from TaoDictionary)
4       8     first_seen (int64, big-endian, epoch milliseconds)
```

The key is ordered `(attr_id, first_seen)`. This enables:
- Prefix scan with `[attr_id | *]` for all runs of a given attribute
- Predecessor search with `[attr_id | T]` for the run covering time T

**AttributeRun leaf value (24 bytes):**

```
Offset  Size  Field
------  ----  ---------------
0       8     last_seen (int64, epoch milliseconds)
8       8     valid_to (int64, epoch milliseconds; Long.MAX_VALUE = open-ended)
16      8     value_ref (long, pointer to value payload in BumpAllocator)
```

**Total leaf per entry:** 12B key + 24B value = 36 bytes (slab-allocated, aligned to
40 or 48 bytes depending on slab class granularity).

**Field semantics:**

- `first_seen` (in key): earliest timestamp at which this attribute reported this value.
- `last_seen`: latest timestamp at which this attribute confirmed this value. Updated
  in-place (on a COW'd copy) without creating a new EntityVersion.
- `valid_to`: the timestamp at which this run's validity ends. For the latest run of a
  attribute, `valid_to = Long.MAX_VALUE` (open-ended). For historical runs,
  `valid_to = next_run.first_seen - 1`. This field enables efficient interval queries
  and is maintained by the write path during insert/split/merge.
- `value_ref`: pointer to the value payload. Values are stored in the `BumpAllocator`
  as length-prefixed byte arrays. Two runs with equal values share the same `value_ref`
  (pointer equality is sufficient for deduplication).

**Invariants (enforced by the write path, verified by tests):**

1. **Sorted by `first_seen`**: within each attribute, runs are ordered by ascending
   `first_seen`. The ART key ordering guarantees this automatically.
2. **Non-overlapping per attribute**: for any two runs R1, R2 of the same attribute where
   `R1.first_seen < R2.first_seen`, it holds that `R1.valid_to < R2.first_seen`.
3. **Adjacent equal values merged**: if an insert would create two adjacent runs with
   the same `value_ref` for the same attribute, they are merged into a single run spanning
   both intervals. This prevents unbounded run proliferation.
4. **`valid_to` consistency**: for consecutive runs R1, R2 of the same attribute,
   `R1.valid_to = R2.first_seen - 1`. The last run of an attribute has
   `valid_to = Long.MAX_VALUE`.

### 18.5 EntityVersions layout and invariants

EntityVersions are a **materialized read index** derived from AttributeRuns. Each
EntityVersion records a point in time at which the aggregate entity state changed,
together with a CHAMP snapshot of the full state at that point.

**EntityVersions ART key (8 bytes):**

```
Offset  Size  Field
------  ----  ---------------
0       8     first_seen (int64, big-endian, epoch milliseconds)
```

**EntityVersion leaf value (16 bytes):**

```
Offset  Size  Field
------  ----  ---------------
0       8     valid_to (int64, epoch milliseconds; Long.MAX_VALUE = open-ended)
8       8     state_root_ref (long, CHAMP node pointer to full state snapshot)
```

**Total leaf per entry:** 8B key + 16B value = 24 bytes.

**Semantics:**

- An EntityVersion is created **only** when a attribute value change alters the aggregate
  entity state. Specifically, when a `put()` for attribute S at time T introduces a new
  value (not just a `last_seen` extension), and this changes what `allFieldsAt(T)` would
  return, a new EntityVersion is created (or an existing one is updated).
- `state_root_ref` points to a CHAMP persistent map (§18.6) containing the full
  `attr_id → value_ref` mapping at this point in time. Thanks to structural sharing,
  adjacent EntityVersions that differ in a single attribute share all CHAMP nodes except
  the path to the changed entry.
- `valid_to` follows the same convention as AttributeRuns: `Long.MAX_VALUE` for the latest
  version, `next_version.first_seen - 1` for historical versions.

**EntityVersions do NOT store:**

- `last_seen` per attribute (stored in AttributeRuns only — prevents version explosion)
- Attribute-level metadata (which attribute triggered the version change)
- Undated/undetermined state (see §18.11 for handling)

**Invariants:**

1. **Sorted by `first_seen`**: the ART key ordering guarantees this.
2. **Non-overlapping**: consecutive versions have non-overlapping validity intervals.
3. **No duplicate state**: adjacent EntityVersions with identical `state_root_ref` are
   merged. Since CHAMP maps are canonical (§18.6), pointer equality of roots implies
   content equality.
4. **Derivable from AttributeRuns**: EntityVersions can always be rebuilt by replaying all
   AttributeRuns in `first_seen` order and materializing a new CHAMP snapshot at each
   state-change boundary.

### 18.6 CHAMP persistent snapshot map

The CHAMP (Compressed Hash-Array Mapped Prefix-tree) provides immutable, structurally
shared maps from `attr_id` (uint32) to `value_ref` (8 bytes). It is based on
Steindorfer's thesis (*Efficient Immutable Collections*, 2017) adapted for off-heap
storage via FFM.

#### 18.6.1 Design overview

CHAMP is a hash-trie where the hash of the key determines the path through the tree.
For a 32-bit `attr_id`, the hash is the identity function (attribute IDs are already
well-distributed dictionary codes). The 32-bit hash is consumed 5 bits at a time,
giving a maximum depth of 7 levels (5 × 7 = 35 > 32).

Each inner node contains two 32-bit bitmaps (`dataMap` and `nodeMap`) that indicate
which of the 32 positions at this level contain inline data entries vs. child node
pointers. Entries and child pointers are packed contiguously in a single array,
eliminating null slots.

**Key properties:**

- **Canonical form**: for any given logical map content, there is exactly one CHAMP
  tree structure. This enables pointer equality as a content equality test.
- **Structural sharing**: `put()` creates a new root and copies only the nodes on the
  path from root to the affected leaf. All other nodes are shared with the old tree.
- **Compact**: no null slots in arrays. A node with 3 data entries and 2 child pointers
  uses exactly `8B header + 3×12B data + 2×8B children = 60B`.

#### 18.6.2 Node layout (off-heap)

**CHAMP inner node (variable-size, BumpAllocator):**

```
Offset  Size           Field
------  -------------  ------
0       4              dataMap (uint32 bitmap: which positions have inline data)
4       4              nodeMap (uint32 bitmap: which positions have child pointers)
8       12 × D         data entries [D = popcount(dataMap)]
                         each: [attr_id:4B | value_ref:8B]
8+12D   8 × C          child node pointers [C = popcount(nodeMap)]
                         each: 8B CHAMP node pointer (BumpAllocator OverflowPtr)
```

**Total node size:** `8 + 12×D + 8×C` bytes, where D = data entry count, C = child count.

Maximum node size: `8 + 12×32 + 8×32 = 648B` (all 32 positions occupied — extremely
rare in practice).

Typical node size: `8 + 12×4 + 8×2 = 72B` (4 data entries, 2 children — common at
interior levels).

**CHAMP collision node (for hash collisions at max depth):**

```
Offset  Size           Field
------  -------------  ------
0       4              count (uint32, number of entries)
4       4              reserved (0)
8       12 × count     data entries [attr_id:4B | value_ref:8B]
```

At depth 7, if two attribute IDs have the same 32-bit hash (identity, so this means the
same attr_id — which is impossible for distinct keys), a collision node is used. In
practice, since `attr_id` IS the hash, collisions cannot occur and this node type
exists only for correctness at the algorithmic level.

#### 18.6.3 Hash function and trie path

```
hash(attr_id) = attr_id    // identity — dictionary codes are sequential uint32

level 0: bits [0..4]   → index 0..31
level 1: bits [5..9]   → index 0..31
level 2: bits [10..14] → index 0..31
level 3: bits [15..19] → index 0..31
level 4: bits [20..24] → index 0..31
level 5: bits [25..29] → index 0..31
level 6: bits [30..31] → index 0..3 (2 bits remaining)
```

```
indexAtLevel(hash, level):
  shift = level * 5
  return (hash >>> shift) & 0x1F    // 5-bit mask, except level 6 uses 0x03
```

For typical attribute counts (tens to low hundreds), most maps fit in 1–2 levels. A map
with 32 attributes where all hash to different level-0 positions is a single root node
with 32 inline entries (8 + 12×32 = 392 bytes).

#### 18.6.4 Operations

**`get(root, attr_id) → value_ref`:**

```
get(node, attr_id):
  hash = attr_id
  for level = 0 to 6:
    idx = indexAtLevel(hash, level)
    bit = 1 << idx

    if (node.dataMap & bit) != 0:
      // Inline data at this position
      dataIndex = popcount(node.dataMap & (bit - 1))
      entry = node.data[dataIndex]
      if entry.attr_id == attr_id:
        return entry.value_ref
      else:
        return NOT_FOUND    // hash prefix matches but key differs

    if (node.nodeMap & bit) != 0:
      // Child node at this position
      childIndex = popcount(node.nodeMap & (bit - 1))
      node = resolve(node.children[childIndex])
      continue

    return NOT_FOUND    // position empty

  // Reached max depth — check collision node
  return linearSearch(node, attr_id)
```

**`put(root, attr_id, value_ref) → new_root`:**

```
put(node, attr_id, value_ref, level):
  idx = indexAtLevel(attr_id, level)
  bit = 1 << idx

  if (node.dataMap & bit) != 0:
    dataIndex = popcount(node.dataMap & (bit - 1))
    existing = node.data[dataIndex]
    if existing.attr_id == attr_id:
      if existing.value_ref == value_ref:
        return node    // no change — preserve identity
      // Replace value: copy node with updated entry
      newNode = copyNode(node)
      newNode.data[dataIndex].value_ref = value_ref
      return allocate(newNode)

    // Hash collision at this level — push both entries down one level
    subNode = put(emptyNode(), existing.attr_id, existing.value_ref, level + 1)
    subNode = put(subNode, attr_id, value_ref, level + 1)
    // Replace inline data with child pointer
    newNode = copyNodeWithDataRemovedAndChildAdded(node, dataIndex, bit, subNode)
    return allocate(newNode)

  if (node.nodeMap & bit) != 0:
    childIndex = popcount(node.nodeMap & (bit - 1))
    oldChild = resolve(node.children[childIndex])
    newChild = put(oldChild, attr_id, value_ref, level + 1)
    if newChild == oldChild:
      return node    // no change in subtree
    newNode = copyNode(node)
    newNode.children[childIndex] = newChild
    return allocate(newNode)

  // Empty position — insert inline data
  newNode = copyNodeWithDataAdded(node, bit, attr_id, value_ref)
  return allocate(newNode)
```

**`remove(root, attr_id) → new_root`:**

```
remove(node, attr_id, level):
  idx = indexAtLevel(attr_id, level)
  bit = 1 << idx

  if (node.dataMap & bit) != 0:
    dataIndex = popcount(node.dataMap & (bit - 1))
    if node.data[dataIndex].attr_id != attr_id:
      return node    // key not present
    // Remove inline data entry
    newNode = copyNodeWithDataRemoved(node, dataIndex, bit)
    return allocate(newNode)

  if (node.nodeMap & bit) != 0:
    childIndex = popcount(node.nodeMap & (bit - 1))
    oldChild = resolve(node.children[childIndex])
    newChild = remove(oldChild, attr_id, level + 1)
    if newChild == oldChild:
      return node    // key not found in subtree
    if isEmpty(newChild):
      newNode = copyNodeWithChildRemoved(node, childIndex, bit)
      return allocate(newNode)
    if isSingleton(newChild):
      // Inline the single remaining entry (CHAMP compaction rule)
      entry = singleEntry(newChild)
      newNode = copyNodeWithChildReplacedByData(node, childIndex, bit, entry)
      return allocate(newNode)
    newNode = copyNode(node)
    newNode.children[childIndex] = newChild
    return allocate(newNode)

  return node    // position empty, key not present
```

**`iterate(root, visitor)`:**

```
iterate(node, visitor):
  // Visit inline data entries first (in bitmap order for determinism)
  for i = 0 to popcount(node.dataMap) - 1:
    if !visitor.visit(node.data[i].attr_id, node.data[i].value_ref):
      return false    // early termination
  // Recurse into child nodes
  for i = 0 to popcount(node.nodeMap) - 1:
    child = resolve(node.children[i])
    if !iterate(child, visitor):
      return false
  return true
```

#### 18.6.5 Structural sharing mechanics

When `put()` updates one attribute, only nodes on the root-to-entry path are copied:

```
Before put(attr_42, new_value):         After put:

    root_A                                    root_B (new)
   / | \                                    / | \
  n1  n2  n3                              n1  n2' n3
      |                                       |
      n4                                      n4' (new)
     / \                                     / \
   e42  e99                               e42' e99
                                          (new)

Shared nodes: n1, n3, e99 (not copied)
New nodes: root_B, n2', n4', e42' (4 allocations)
```

For a map with S attributes, the copy cost per `put()` is O(log₃₂ S) nodes — at most 7
for any uint32 key space. In practice, with S < 100 attributes, most maps are 1–2 levels
deep, so a `put()` copies 1–2 nodes.

#### 18.6.6 Allocation and reclamation

- **Inner nodes**: allocated in the `BumpAllocator` (variable-size, append-only).
  Each `put()` or `remove()` allocates new nodes for the modified path.
- **Reclamation**: CHAMP nodes are immutable once published. Old nodes (replaced by
  `put()`/`remove()`) are retired via the shared `EpochReclaimer`. When all readers
  that could see the old root have exited their epoch, the old nodes become reclaimable.
- **No reference counting**: structural sharing means a single node may be reachable
  from multiple CHAMP roots (multiple EntityVersions). Epoch-based reclamation handles
  this correctly — a node is only reclaimed when no reader can reach ANY root that
  references it. Since old roots are retired in order, and readers hold epoch pins,
  this is guaranteed.

### 18.7 CowEngine configuration

Three `CowEngine` instances share the same `SlabAllocator`, `ChunkStore`,
`BumpAllocator`, and `EpochReclaimer`. ART internal node classes (Node4, Node16, Node48,
Node256, PrefixNode) are key-length independent, so they are shared. Only leaf classes
differ:

| CowEngine | keyLen | leafValueSize | Purpose |
|---|---|---|---|
| Main ART (global) | prefix bytes | 24B (inline `EntityNode`) | Global entity routing |
| AttributeRuns ART | 12B | 24B (AttributeRun) | Per-entity attribute history |
| EntityVersions ART | 8B | 16B (EntityVersion) | Per-entity state timeline |

**CHAMP is NOT a CowEngine.** It is a separate persistent map implementation using
the shared `BumpAllocator` for node allocation and the shared `EpochReclaimer` for
lifetime management. CHAMP nodes are not ART nodes — they have their own layout (§18.6.2)
and do not participate in ART COW paths.

**Engine registration:**

All three CowEngines and the CHAMP allocator are created at `TaoTree.create()` time.
The AttributeRuns and EntityVersions CowEngines are each registered once with the shared
`SlabAllocator` — all per-entity ARTs of the same type share leaf and node class IDs.
This means:

- All AttributeRuns ARTs across all entities use the same slab classes.
- All EntityVersions ARTs across all entities use the same slab classes.
- EntityNode structs have their own slab class (40 bytes).

**Slab classes registered:**

| Slab class | Size | Used by |
|---|---|---|
| EntityNode | 40B | EntityNode aggregate roots |
| MainART leaf | keySlot + 8B | Global ART leaf slots |
| AttributeRuns leaf | aligned(12) + 24B | AttributeRuns ART leaf slots |
| EntityVersions leaf | aligned(8) + 16B | EntityVersions ART leaf slots |
| Node4 | 160B | All three CowEngines (shared) |
| Node16 | 528B | All three CowEngines (shared) |
| Node48 | 656B | All three CowEngines (shared) |
| Node256 | 2064B | All three CowEngines (shared) |
| PrefixNode | 24B | All three CowEngines (shared) |

### 18.8 Write path

The write path performs optimistic COW through the global ART, mutates per-entity
structures, and publishes atomically. The key invariant: all mutations within a single
`WriteScope` are visible as a single atomic state transition.

#### 18.8.1 Full write flow

```
put(prefix, attr_name, value, T):
  // --- Phase 1: Resolve identifiers ---
  attr_id = attrDict.intern(attr_name)    // TaoDictionary, self-locking
  value_ref = internValue(value)                 // BumpAllocator, deduplicated

  // --- Phase 2: Global ART COW to EntityNode ---
  mainCow = mainCowEngine.beginCow(mainRoot, prefix)
  objNodeRef = mainCow.getOrCreateLeaf(prefix) → read EntityNodeRef
  objNode = resolveEntityNode(objNodeRef)        // slab resolve to 40B struct
  // If new entity: objNode fields are all zero (fresh slab slot)

  // --- Phase 3: AttributeRuns upsert ---
  attrArtRoot = objNode.attr_art_root_ref
  attrKey = encodeAttrKey(attr_id, T)      // [attr_id:4 | T:8]
  {newAttrRoot, stateChanged, affectedWindow} =
      upsertAttributeRun(attrArtRoot, attr_id, T, value_ref)

  // --- Phase 4: Conditional EntityVersions update ---
  versionsArtRoot = objNode.versions_art_root_ref
  currentStateRoot = objNode.current_state_root_ref
  if stateChanged:
    {newVersionsRoot, newCurrentStateRoot} =
        updateEntityVersions(versionsArtRoot, currentStateRoot,
                             attr_id, value_ref, affectedWindow)
  else:
    newVersionsRoot = versionsArtRoot
    newCurrentStateRoot = currentStateRoot

  // --- Phase 5: Write new EntityNode ---
  newObjNode = allocEntityNode()
  newObjNode.current_state_root_ref = newCurrentStateRoot
  newObjNode.attr_art_root_ref = newAttrRoot
  newObjNode.versions_art_root_ref = newVersionsRoot
  newObjNode.archive_manifest_ref = objNode.archive_manifest_ref
  newObjNode.metadata_ref = objNode.metadata_ref
  mainCow.writeLeafValue(newObjNodeRef)

  // --- Phase 6: Publish ---
  retireOldEntityNode(objNodeRef)
  mainCow.publish()    // atomic root swap via CAS
```

#### 18.8.2 AttributeRuns upsert (detail)

```
upsertAttributeRun(attrArtRoot, attr_id, T, value_ref):
  // Step 1: Find predecessor run for this attribute at time T
  searchKey = encodeAttrKey(attr_id, T)
  predRun = predecessor(attrArtRoot, searchKey, 12)

  // Step 2: Check if T falls within an existing run of this attribute
  if predRun != null && predRun.attr_id == attr_id:
    if predRun.value_ref == value_ref:
      // Same value — extend last_seen, no state change
      newLastSeen = max(predRun.last_seen, T)
      if newLastSeen == predRun.last_seen:
        return {attrArtRoot, stateChanged=false, affectedWindow=null}
      // COW-update the run's last_seen
      newRoot = cowUpdateLeafValue(attrArtRoot, predRun.key,
                                   predRun.withLastSeen(newLastSeen))
      return {newRoot, stateChanged=false, affectedWindow=null}

    if T >= predRun.first_seen && T <= predRun.valid_to:
      // Different value, T inside existing run → split run
      return splitAndInsert(attrArtRoot, predRun, attr_id, T, value_ref)

  // Step 3: Find successor run of this attribute (for valid_to computation)
  succRun = successor(attrArtRoot, searchKey, 12, attr_id)

  // Step 4: Check if merge with successor is possible
  if succRun != null && succRun.value_ref == value_ref:
    // Same value as successor — extend successor backwards
    newRoot = cowDeleteAndReinsert(attrArtRoot, succRun.key,
                                   succRun.withFirstSeen(T))
    affectedWindow = {from=T, to=succRun.first_seen - 1}
    return {newRoot, stateChanged=true, affectedWindow}

  // Step 5: Check if merge with predecessor is possible
  if predRun != null && predRun.attr_id == attr_id
     && predRun.value_ref == value_ref:
    // predRun was checked for same value above and failed...
    // (This case is handled by step 2, same-value branch)
    // Not reachable here.

  // Step 6: Insert new run
  newValidTo = (succRun != null) ? succRun.first_seen - 1 : Long.MAX_VALUE
  newRun = AttributeRun{first_seen=T, last_seen=T, valid_to=newValidTo, value_ref}

  // Update predecessor's valid_to if it exists for this attribute
  if predRun != null && predRun.attr_id == attr_id:
    newRoot = cowUpdateLeafValue(attrArtRoot, predRun.key,
                                 predRun.withValidTo(T - 1))
    newRoot = cowInsert(newRoot, encodeAttrKey(attr_id, T), newRun)
  else:
    newRoot = cowInsert(attrArtRoot, encodeAttrKey(attr_id, T), newRun)

  affectedWindow = {from=T, to=newValidTo}
  return {newRoot, stateChanged=true, affectedWindow}
```

#### 18.8.3 Split-and-insert for out-of-order within a run

```
splitAndInsert(attrArtRoot, existingRun, attr_id, T, value_ref):
  // existingRun covers [first_seen .. valid_to] with a different value
  // T falls inside this range

  // Up to three runs result, depending on whether there are
  // observations of the old value strictly past T:
  //   [existingRun.first_seen, T-1] → existingRun.value_ref  (prefix, if T > first_seen)
  //   [T, insertValidTo]             → value_ref              (insert)
  //   [T+1, existingRun.valid_to]    → existingRun.value_ref  (suffix, only if last_seen > T)

  root = attrArtRoot

  // Shrink existing run to prefix portion
  if T > existingRun.first_seen:
    prefixRun = AttributeRun{first_seen=existingRun.first_seen,
                           last_seen=existingRun.last_seen,
                           valid_to=T-1, value_ref=existingRun.value_ref}
    root = cowUpdateLeafValue(root, existingRun.key, prefixRun)
  else:
    // T == existingRun.first_seen — no prefix portion, delete existing
    root = cowDelete(root, existingRun.key)

  // A suffix run exists only when the old value was observed strictly past T.
  // Without that, the "assumed valid" tail of the existing run collapses into
  // the new value — the new run inherits existingRun.valid_to. This avoids a
  // phantom suffix in the notable case where existingRun.last_seen ≤ T (e.g.
  // overwriting a TIMELESS value, where first_seen == last_seen == T == 0
  // but valid_to == Long.MAX_VALUE).
  hasSuffix = (T < existingRun.valid_to) and (existingRun.last_seen > T)
  insertValidTo = hasSuffix ? T : existingRun.valid_to

  // Insert new run at T
  newRun = AttributeRun{first_seen=T, last_seen=T,
                        valid_to=insertValidTo, value_ref}
  root = cowInsert(root, encodeAttrKey(attr_id, T), newRun)

  if hasSuffix:
    suffixRun = AttributeRun{first_seen=T+1, last_seen=existingRun.last_seen,
                           valid_to=existingRun.valid_to,
                           value_ref=existingRun.value_ref}
    root = cowInsert(root, encodeAttrKey(attr_id, T+1), suffixRun)
    affectedWindow = {from=T, to=T}
    revertWindow  = {from=T+1, to=existingRun.valid_to}
  else:
    affectedWindow = {from=T, to=insertValidTo}
    revertWindow   = null

  return {root, stateChanged=true, affectedWindow, revertWindow}
```

### 18.9 Read path

All read operations begin with a `ReadScope` that atomically captures the global ART
root via `VarHandle.getAcquire()` and enters an epoch. Since the EntityNode contains
roots for all per-entity structures, and all nodes are immutable once published, the
entire multi-level structure is consistent from this single root snapshot.

#### 18.9.1 `latest(prefix, attribute)`

Returns the current value of a specific attribute for an entity. This is the fastest
read path — it uses `current_state_root` directly, bypassing both per-entity ARTs.

```
latest(prefix, attr_name):
  // 1. Resolve attribute ID (lock-free via TaoDictionary.resolve)
  attr_id = attrDict.resolve(attr_name)
  if attr_id == UNKNOWN:
    return NOT_FOUND

  // 2. Global ART lookup
  objNodeRef = mainArt.lookup(mainRoot, prefix)
  if objNodeRef == 0:
    return NOT_FOUND
  objNode = resolveEntityNode(objNodeRef)

  // 3. CHAMP lookup in current state
  value_ref = champGet(objNode.current_state_root_ref, attr_id)
  if value_ref == NOT_FOUND:
    return NOT_FOUND

  return resolveValue(value_ref)
```

**Complexity:** O(P + log₃₂ S) where P = prefix key length, S = attribute count.
In practice, O(P + 1) for S < 32.

#### 18.9.2 `at(prefix, attribute, T)`

Returns the value of a specific attribute at a given point in time.

```
at(prefix, attr_name, T):
  attr_id = attrDict.resolve(attr_name)
  if attr_id == UNKNOWN:
    return NOT_FOUND

  objNodeRef = mainArt.lookup(mainRoot, prefix)
  if objNodeRef == 0:
    return NOT_FOUND
  objNode = resolveEntityNode(objNodeRef)

  // Predecessor search in EntityVersions ART: find version with first_seen ≤ T
  versionKey = encodeVersionKey(T)    // [T:8B]
  version = predecessor(objNode.versions_art_root_ref, versionKey, 8)
  if version == null:
    return NOT_FOUND    // no version exists at or before T

  // Check validity: version.valid_to >= T
  if version.valid_to < T:
    return NOT_FOUND    // gap in version timeline (should not happen if invariants hold)

  // CHAMP lookup in version's state snapshot
  value_ref = champGet(version.state_root_ref, attr_id)
  if value_ref == NOT_FOUND:
    return NOT_FOUND    // attribute not present in entity state at time T

  return resolveValue(value_ref)
```

**Complexity:** O(P + 8 + log₃₂ S) — prefix lookup + EntityVersions predecessor + CHAMP get.

#### 18.9.3 `allFieldsAt(prefix, T)`

Returns the complete entity state (all attribute values) at a given point in time.

```
allFieldsAt(prefix, T, visitor):
  objNodeRef = mainArt.lookup(mainRoot, prefix)
  if objNodeRef == 0:
    return    // entity not found

  objNode = resolveEntityNode(objNodeRef)

  // Predecessor search in EntityVersions ART
  version = predecessor(objNode.versions_art_root_ref, encodeVersionKey(T), 8)
  if version == null:
    return    // no version at or before T

  if version.valid_to < T:
    return    // gap in timeline

  // Iterate entire CHAMP map at this version's state
  champIterate(version.state_root_ref, (attr_id, value_ref) -> {
    attr_name = attrDict.reverseLookup(attr_id)
    value = resolveValue(value_ref)
    return visitor.visit(attr_name, value)    // false = stop early
  })
```

**Complexity:** O(P + 8 + S) — prefix lookup + predecessor + CHAMP full iteration.
This is a major improvement over the per-field predecessor approach in the old design,
which cost O(P + F × 12) for F fields.

#### 18.9.4 `history(prefix, attribute)`

Returns the full history of a specific attribute as a sequence of AttributeRuns.

```
history(prefix, attr_name, visitor):
  attr_id = attrDict.resolve(attr_name)
  if attr_id == UNKNOWN:
    return

  objNodeRef = mainArt.lookup(mainRoot, prefix)
  if objNodeRef == 0:
    return

  objNode = resolveEntityNode(objNodeRef)

  // Prefix scan in AttributeRuns ART: all runs with key prefix [attr_id:4B]
  attrPrefix = encodeAttrPrefix(attr_id)    // 4 bytes
  scan(objNode.attr_art_root_ref, attrPrefix, 4, (key, run) -> {
    return visitor.visit(AttributeRun{
      first_seen = extractFirstSeen(key),
      last_seen = run.last_seen,
      valid_to = run.valid_to,
      value = resolveValue(run.value_ref)
    })
  })
```

**Complexity:** O(P + 12 × R) where R = number of runs for this attribute.

#### 18.9.5 `history(prefix, attribute, fromMs, toMs)` — bounded range

```
history(prefix, attr_name, fromMs, toMs, visitor):
  attr_id = attrDict.resolve(attr_name)
  if attr_id == UNKNOWN:
    return

  objNode = resolveEntityNode(mainArt.lookup(mainRoot, prefix))
  if objNode == null:
    return

  // Start from predecessor of fromMs to catch runs that span the boundary
  startKey = encodeAttrKey(attr_id, fromMs)
  predRun = predecessor(objNode.attr_art_root_ref, startKey, 12)

  // If predecessor exists and overlaps [fromMs, toMs], include it
  if predRun != null && predRun.attr_id == attr_id
     && predRun.valid_to >= fromMs:
    visitor.visit(predRun)

  // Scan forward from fromMs through toMs
  scan(objNode.attr_art_root_ref, startKey, 12, (key, run) -> {
    first_seen = extractFirstSeen(key)
    if first_seen > toMs:
      return false    // past end of range
    return visitor.visit(run)
  })
```

### 18.10 Affected window algorithm

When a late insert arrives at time T for attribute S with value V, the affected window
determines which EntityVersions need rewriting. This is the core complexity-bounding
mechanism that keeps late inserts efficient.

#### 18.10.1 Window computation

```
computeAffectedWindow(attrArtRoot, attr_id, T, old_value_ref, new_value_ref):
  // The affected window spans from T to the next state change of this attribute.
  // "Next state change" = the first_seen of the next run of this attribute that
  // has a DIFFERENT value_ref than new_value_ref.

  if old_value_ref == new_value_ref:
    return null    // no state change → no affected window

  // Find the next run of this attribute after T
  searchKey = encodeAttrKey(attr_id, T + 1)
  nextRun = successor(attrArtRoot, searchKey, attr_id)

  if nextRun == null:
    // This is the last run of the attribute — window extends to end of time
    return {from=T, to=Long.MAX_VALUE}

  if nextRun.value_ref == new_value_ref:
    // Next run has the same value as the insert — window ends before it
    // (the merged run covers from T through nextRun.valid_to)
    return {from=T, to=nextRun.first_seen - 1}

  // Next run has a different value — window extends to its boundary
  return {from=T, to=nextRun.first_seen - 1}
```

#### 18.10.2 EntityVersions rewrite within the window

```
updateEntityVersions(versionsArtRoot, currentStateRoot,
                     attr_id, new_value_ref, affectedWindow):
  root = versionsArtRoot

  // Step 1: Find all EntityVersions with first_seen in [window.from, window.to]
  //         These versions need their CHAMP state rewritten.
  affectedVersions = []
  scanRange(root, affectedWindow.from, affectedWindow.to, (versionKey, version) -> {
    affectedVersions.add({key=versionKey, version=version})
    return true
  })

  // Step 2: Find the predecessor version (the version active just before the window)
  predVersion = predecessor(root, encodeVersionKey(affectedWindow.from), 8)

  // Step 3: Determine the base state for the window start
  if predVersion != null:
    baseStateRoot = predVersion.state_root_ref
  else:
    baseStateRoot = CHAMP_EMPTY_ROOT    // no prior state

  // Step 4: Compute new state at window.from
  newStateRoot = champPut(baseStateRoot, attr_id, new_value_ref)

  // Step 5: Check if we need a new EntityVersion at window.from
  needNewVersion = true
  if predVersion != null && predVersion.state_root_ref == newStateRoot:
    needNewVersion = false    // state didn't actually change (CHAMP canonical form)
  if affectedVersions is not empty && affectedVersions[0].key == affectedWindow.from:
    needNewVersion = false    // there's already a version at this timestamp; update it

  // Step 6: Insert or update the version at window.from
  if needNewVersion:
    validTo = affectedVersions.isEmpty()
              ? affectedWindow.to
              : affectedVersions[0].first_seen - 1
    newVersion = EntityVersion{valid_to=validTo, state_root_ref=newStateRoot}
    root = cowInsert(root, encodeVersionKey(affectedWindow.from), newVersion)
    // Update predecessor's valid_to
    if predVersion != null:
      root = cowUpdateLeafValue(root, predVersion.key,
                                 predVersion.withValidTo(affectedWindow.from - 1))

  // Step 7: Rewrite each affected version's CHAMP root
  for each {key, version} in affectedVersions:
    updatedStateRoot = champPut(version.state_root_ref, attr_id, new_value_ref)
    root = cowUpdateLeafValue(root, key,
                               version.withStateRoot(updatedStateRoot))

  // Step 8: Merge adjacent versions with equal state_root_ref
  root = mergeAdjacentVersions(root, affectedWindow)

  // Step 9: Update current state if the window touches the present
  if affectedWindow.to == Long.MAX_VALUE:
    newCurrentStateRoot = newStateRoot
  else:
    newCurrentStateRoot = currentStateRoot
    // Current state may need updating if we changed the latest version
    latestVersion = rightmostLeaf(root)
    if latestVersion != null:
      newCurrentStateRoot = latestVersion.state_root_ref

  return {root, newCurrentStateRoot}
```

#### 18.10.3 Merge adjacent versions

```
mergeAdjacentVersions(root, window):
  // Scan versions in the window and one beyond
  prev = null
  toDelete = []
  scanRange(root, window.from, window.to + 1, (key, version) -> {
    if prev != null && prev.state_root_ref == version.state_root_ref:
      // Adjacent versions with identical state — merge
      toDelete.add(key)
      prev.valid_to = version.valid_to    // extend predecessor
    else:
      prev = {key, version}
    return true
  })

  for each key in toDelete:
    root = cowDelete(root, key)

  // Update the surviving merged version's valid_to
  if prev != null:
    root = cowUpdateLeafValue(root, prev.key, prev.version)

  return root
```

#### 18.10.4 Worked example

```
Entity: bird-42, Attributes: {species, location}

Initial state (2 attribute runs, 1 entity version):
  AttributeRuns:
    [species, T=100] → {last_seen=200, valid_to=MAX, value="Eagle"}
    [location, T=100] → {last_seen=200, valid_to=MAX, value="Wyoming"}

  EntityVersions:
    [T=100] → {valid_to=MAX, state={species→"Eagle", location→"Wyoming"}}

Late insert: put(bird-42, species, "Hawk", T=150)

Step 1: upsertAttributeRun
  predecessor([species, 150]) → run [species, T=100, "Eagle"]
  T=150 inside [100..MAX], value differs → splitAndInsert
  Result:
    [species, T=100] → {last_seen=200, valid_to=149, value="Eagle"}
    [species, T=150] → {last_seen=150, valid_to=MAX, value="Hawk"}

Step 2: computeAffectedWindow
  Next run of species after T=150: none (Hawk is the last run)
  affectedWindow = {from=150, to=MAX}

Step 3: updateEntityVersions
  Affected versions: [T=100] has first_seen=100, which is < 150 → NOT in window
  (No existing versions in [150..MAX])

  predVersion = [T=100] → state={species→"Eagle", location→"Wyoming"}
  newStateRoot = champPut(predVersion.state, species, "Hawk")
               = {species→"Hawk", location→"Wyoming"}  (new CHAMP root, shares location node)

  needNewVersion = true (no version at T=150)
  Insert: [T=150] → {valid_to=MAX, state={species→"Hawk", location→"Wyoming"}}
  Update: [T=100] → {valid_to=149, state={species→"Eagle", location→"Wyoming"}}

  currentStateRoot = newStateRoot (window.to == MAX)

Final state:
  AttributeRuns:
    [species, T=100] → {last_seen=200, valid_to=149, value="Eagle"}
    [species, T=150] → {last_seen=150, valid_to=MAX, value="Hawk"}
    [location, T=100] → {last_seen=200, valid_to=MAX, value="Wyoming"}

  EntityVersions:
    [T=100] → {valid_to=149, state={species→"Eagle", location→"Wyoming"}}
    [T=150] → {valid_to=MAX, state={species→"Hawk", location→"Wyoming"}}

  CHAMP sharing: both versions share the location→"Wyoming" CHAMP node.
  Only the species entry differs.
```

### 18.11 Temporal edge-case rules

These rules resolve ambiguous cases in the temporal store. They must be enforced by
the implementation and verified by tests.

1. **Same-timestamp, different-value tie-break:** Last-write-wins. If two `put()` calls
   for the same entity, attribute, and timestamp arrive with different values, the second
   write overwrites the first. Within a single `WriteScope`, mutations are ordered.
   Across concurrent `WriteScope`s, the COW commit order determines the winner.

2. **Adjacent equal-value AttributeRuns are merged eagerly.** Unlike the old bucket design,
   AttributeRun merge is performed on the write path (invariant 3 in §18.4). After a
   split creates `[100,149,"Eagle"]` and `[151,200,"Eagle"]` around `[150,150,"Hawk"]`,
   if a subsequent write overwrites T=150 back to "Eagle", the three runs are merged
   into a single `[100,200,"Eagle"]`.

3. **Adjacent equal-state EntityVersions are merged eagerly.** After an affected-window
   rewrite (§18.10), the merge step checks adjacent versions. If two versions have the
   same `state_root_ref` (CHAMP canonical form guarantees pointer equality = content
   equality), they are merged into one version spanning both intervals.

4. **`last_seen` update does not create EntityVersions.** An attribute reporting the same
   value it already reports only updates `AttributeRun.last_seen`. No EntityVersion is
   created or modified. This prevents version explosion from high-frequency
   attribute-confirm patterns.

5. **First insert for a new attribute.** When an attribute reports a value for the first time
   for a given entity, a new AttributeRun is created AND a new EntityVersion is created
   (or the existing latest EntityVersion is extended if the state change was a no-op,
   which cannot happen for a new attribute). The CHAMP map grows by one entry.

6. **Attribute removal.** Removing an attribute (setting its value to a tombstone sentinel)
   creates a new AttributeRun with `value_ref = TOMBSTONE_REF` and a new EntityVersion
   whose CHAMP map has the attribute entry removed via `champRemove()`. The AttributeRun
   remains in the history for auditability.

7. **Empty entity cleanup.** If all attributes of an entity are tombstoned and all
   EntityVersions have empty CHAMP maps, the EntityNode can be replaced with a
   tombstone EntityNodeRef in the global ART. Full removal is deferred to compaction.

8. **Undated observations removed.** The two-layer architecture does not support undated
   observations. All `put()` calls require an explicit timestamp. The rationale:
   AttributeRuns and EntityVersions are keyed by time, and "timeless" data cannot
   participate in the temporal ordering, state materialization, or affected-window
   algorithms. Applications that need to store non-temporal attributes should use a
   separate non-temporal TaoTree instance (the existing v2 key-value API).

### 18.12 Persistence

All structures — EntityNodes, AttributeRuns ART nodes, EntityVersions ART nodes, CHAMP
nodes, and value payloads — are stored in the shared `SlabAllocator` and
`BumpAllocator`, both backed by the same `ChunkStore`.

#### 18.12.1 Predecessor search and rightmost-leaf (ART operations)

The per-entity ARTs require **predecessor search**: given key K, find the leaf with
the largest key ≤ K. The existing ART has lookup (exact match) and prefix scan, but
NOT predecessor search.

Implementation: descend the ART as for exact lookup. When a child for the next byte
is missing, backtrack to the nearest node with a smaller child, then descend to its
rightmost leaf. This is O(key length) — same as lookup.

```
predecessor(root, key, keyLen):
  Descend from root matching key bytes.
  At each inner node, if the exact child byte is missing:
    Find the largest child byte < target byte in this node.
    If found: descend to that child, then follow rightmost path to leaf.
    If not found: backtrack up the path to find the previous subtree.
  At a leaf: if leaf key <= search key, return it. Else backtrack.
```

This operation is critical for:
- `at(prefix, attribute, T)`: predecessor in EntityVersions ART finds the version
  with `first_seen ≤ T`
- AttributeRuns upsert: predecessor in AttributeRuns ART finds the run covering time T

The `latest()` path does NOT need predecessor search — it uses `current_state_root`
directly for O(1) CHAMP access.

**Rightmost-leaf under a prefix** is used by the affected-window algorithm to find
the latest EntityVersion:

```
rightmostLeaf(root, prefix, prefixLen):
  Descend from root matching prefix bytes.
  After prefix is fully matched, descend the subtree always choosing
  the largest child byte at each inner node.
  Return the leaf reached (the rightmost leaf under this prefix).
```

This is O(prefix length + remaining key depth) = O(key length).

#### 18.12.2 Sync and recovery

On `sync()`:

1. `ChunkStore.syncDirty()` forces modified pages to disk (including all ART nodes,
   EntityNodes, CHAMP nodes, and value payloads).
2. Checkpoint writes the global ART state (root, size, slab class tables) using the
   existing v2 format (mirrored A/B slots, CRC-32C, shadow paging).
3. Recovery: open global ART → leaf slots contain `EntityNodeRef` → each EntityNode
   contains `attr_art_root_ref`, `versions_art_root_ref`, `current_state_root_ref`
   → the entire multi-level structure is reachable from the single global root.

No separate checkpointing is needed for per-entity ARTs or CHAMP maps — they are
all stored in the same `SlabAllocator`/`BumpAllocator` pages that the `ChunkStore`
manages.

**Temporal descriptor (persisted in checkpoint, for reopen safety):**

```
Offset  Size  Field
------  ----  -----
0       var   KeyLayout field definitions (prefix layout: count, types, names)
var     4     attribute dictionary index (which dict entry is the attribute-name dict)
var     4     AttributeRuns keyLen (12)
var     4     AttributeRuns leafValueSize (24)
var     4     EntityVersions keyLen (8)
var     4     EntityVersions leafValueSize (16)
var     4     EntityNode slab class ID
var     4     CHAMP format version
var     4     format version
```

This lets `TaoTree.open(Path)` reconstruct the `KeyLayout`, rebind the attribute
dictionary, and re-create the AttributeRuns and EntityVersions `CowEngine` instances
with correct parameters.

#### 18.12.3 Compaction

> **Implementation status (2026-04-20):** the pseudocode below is the
> *intended* design. The shipped `Compactor` walks only the outer ART
> (step 3 and a verbatim `MemorySegment.copy` of each `EntityNode` leaf);
> steps 2a–2d and CHAMP deduplication are **not** implemented, and the
> compactor does not call `EpochReclaimer.retire()` on old nodes. Net
> effect: `compact()` does not shrink the file or repack per-entity
> structures. Correctness is unaffected because pointers into the
> nested structures remain valid (no page is physically reclaimed). See
> `CompactorSpaceReclaimTest` for the empirical guardrails. Tracked as
> `p8-compactor-temporal`.

`TaoTree.compact()` provides temporal-aware vacuum logic:

1. Walk the global ART.
2. For each EntityNode:
   a. Walk its AttributeRuns ART — migrate all leaf slots and ART nodes to target slabs.
   b. Walk its EntityVersions ART — migrate all leaf slots and ART nodes.
   c. Walk all reachable CHAMP nodes (from all `state_root_ref` pointers in
      EntityVersions + `current_state_root`). Deduplicate shared CHAMP nodes
      (track visited set by pointer to avoid double-copying).
   d. Migrate the EntityNode itself.
3. Migrate the global ART nodes and leaf slots.
4. Write checkpoint.
5. Epoch reclamation for old allocator state.

CHAMP structural sharing requires care during compaction: a single CHAMP inner node
may be referenced by multiple EntityVersion `state_root_ref` pointers. The compaction
must track which CHAMP nodes have already been migrated (pointer → new-pointer map)
to preserve sharing in the target.

### 18.13 Hot/cold tiering and archival

The temporal store supports a **correction horizon** (watermark): a configurable
timestamp boundary. Data older than this is considered immutable and eligible for
archival.

#### 18.13.1 Correction horizon

```
correction_horizon = configurable timestamp (e.g., now() - 30 days)
```

- Data with `valid_to < correction_horizon` is immutable — no late inserts can affect it.
- The write path checks: if a `put()` arrives with `T < correction_horizon`, it is
  rejected (or routed to a correction log for manual review).
- This bounds the affected-window algorithm: late inserts can only affect EntityVersions
  within the mutable window (after the correction horizon).

#### 18.13.2 Archive manifests

Each entity may have an `archive_manifest_ref` in its EntityNode, pointing to a list
of cold segments:

```
ArchiveManifest {
  segment_count: uint32
  segments: ArchivedSegment[]
}

ArchivedSegment {
  from_timestamp: int64       // start of archived range
  to_timestamp: int64         // end of archived range
  attr_runs_ref: long       // pointer to cold-stored AttributeRuns
  versions_ref: long          // pointer to cold-stored EntityVersions (may be 0)
  checksum: int32             // CRC-32C of the cold segment
}
```

**AttributeRuns** are the canonical archive layer — they are always fully preserved in
cold storage. **EntityVersions** in cold storage may be sparse or absent — they are
reconstructible from AttributeRuns when needed.

#### 18.13.3 TypeArchiveIndex

For browsing cold entities by type, a `TypeArchiveIndex` maps
`(tenant, object_type)` to a list of archived entity segments:

```
ArchivedObjectSegment {
  entity_id_min: bytes        // first entity ID in this segment
  entity_id_max: bytes        // last entity ID in this segment
  entity_count: uint32        // number of entities
  archived_until: int64       // latest timestamp in this segment
  archive_ref: long           // pointer to cold storage block
}
```

This enables efficient cold-entity enumeration without scanning the global ART.

#### 18.13.4 Cold stubs

When an entity is fully archived (all data older than the correction horizon), its
global ART entry is replaced with a **cold stub**: a minimal EntityNode with only
`archive_manifest_ref` set, and all ART roots set to `EMPTY_PTR`. CHAMP
`current_state_root_ref` points to the latest archived state snapshot.

Reading a cold-stubbed entity triggers archive retrieval (transparent to the caller
or explicit via an async API — deferred to implementation).

### 18.14 Cold store: ImmutableTaoTree (deferred)

`TaoTree.freeze(Path)` will produce an `ImmutableTaoTree` — a read-only,
size-optimized snapshot. Deferred to phase 2.

The cold store will use per-attribute `AttributeValueStats` to select optimal value encodings:

| Stats condition | Encoding | Size per value |
|---|---|---|
| `allNumeric` + max fits int32 | `int32` | 4B |
| `allNumeric` + max fits int64 | `int64` | 8B |
| `distinctCount ≤ 256` | `dict8` | 1B |
| `distinctCount ≤ 65536` | `dict16` | 2B |
| `distinctCount ≤ 2^32` + short values | `dict32` | 4B |
| otherwise | raw bytes | variable |

AttributeRuns in the cold store are run-length encoded (each run is a contiguous value
interval), which is already the natural format. EntityVersions may use delta-encoded
CHAMP diffs instead of full snapshots for further compression.

### 18.15 HINT index (deferred)

Hierarchical Interval Indexing for `scanOverlap(fromMs, toMs)` queries — finding all
entities with data overlapping a time range. Deferred to a future phase.

When implemented, HINT will be a **separate ART** within `TaoTree` (not a child tree
of the per-entity ART). The key insight: AttributeRuns' `[first_seen, valid_to]` intervals
are the input to the HINT decomposition. The existing `HintPartitioner` class provides
the interval decomposition algorithm.

### 18.16 Operational notes

**Epoch slot sizing:** The `EpochReclaimer` allocates reader slots via thread-local
tokens. Slots are recycled only when threads terminate and their `ThreadLocal` is
reclaimed. For long-lived thread pools (the common case in server applications), the
maximum reader slot count is an operational setting that must be sized to the expected
thread pool width. Exhausting reader slots will cause `ReadScope` creation to fail.
This is not a design flaw but an operational constraint that should be configured at
`TaoTree.create()` time.

**Durability caveats:** The persistence model (mmap + `ChunkStore.syncDirty()` + A/B
checkpoint with CRC-32C) provides crash-safe metadata recovery. However, power-loss
durability for data pages depends on OS-level `msync` / `fsync` behavior, which varies
by platform. The design does not claim stronger guarantees than the underlying OS provides.

**CHAMP memory overhead:** Each EntityVersion holds a `state_root_ref` to a CHAMP map.
For an entity with S attributes and V versions, worst-case CHAMP memory is
`V × O(S)` — but structural sharing means the actual memory is typically
`O(S + V × log₃₂ S)` because adjacent versions share most nodes. For S=20 attributes
and V=100 versions, this is approximately `100 × (8 + 12×20) = 24.8 KB` worst case
vs. `~20 × 248 + 100 × 3 × 60 = 22.9 KB` with sharing (one shared root + 3 path
nodes per version change).

**EntityNode slab fragmentation:** EntityNodes are fixed-size (40 bytes) and
slab-allocated, so there is no fragmentation. Tombstoned entities leave holes in the
slab that are reused by new entities via the slab free list.

**Per-entity ART sizing:** Most entities have tens of attributes and hundreds of runs.
A AttributeRuns ART with 100 entries (12-byte keys) typically fits in 3–4 inner nodes.
An EntityVersions ART with 50 entries (8-byte keys) fits in 2–3 inner nodes. The
per-entity ART overhead is modest: ~500 bytes per entity for typical attribute counts.

**Writer contention per entity:** Two concurrent writers modifying different attributes
of the same entity still conflict on the EntityNode (serialization point per entity).
The single-retry optimistic COW handles this correctly — the second writer redoes its
COW against the new root. This is acceptable for the dominant pattern of sequential
per-entity ingestion. If truly independent concurrent attribute streams are needed, a
future enhancement could use per-attribute sub-locking within the EntityNode.

### 18.17 Complexity analysis

| Operation | Algorithm | Complexity |
|---|---|---|
| `latest(prefix, attribute)` | Global ART lookup + CHAMP `get(attr_id)` | O(P + log₃₂ S) |
| `at(prefix, attribute, T)` | Global ART lookup + EntityVersions predecessor + CHAMP get | O(P + 8 + log₃₂ S) |
| `allFieldsAt(prefix, T)` | Global ART lookup + EntityVersions predecessor + CHAMP iterate | O(P + 8 + S) |
| `history(prefix, attribute)` | Global ART lookup + AttributeRuns prefix scan | O(P + 12 × R) |
| `history(prefix, attribute, from, to)` | Global ART lookup + predecessor + bounded scan | O(P + 12 + 12 × R') |
| `put()` — last_seen only | Global ART COW + AttributeRuns COW (leaf update) | O(P + 12) |
| `put()` — new value | Global ART COW + AttributeRuns COW + EntityVersions COW + CHAMP put | O(P + 12 + 8 + log₃₂ S) |
| `put()` — late insert | Global ART COW + AttributeRuns split + affected window rewrite | O(P + 12 + W × log₃₂ S) |
| `put()` — new attribute | Global ART COW + AttributeRuns insert + EntityVersions insert + CHAMP put | O(P + 12 + 8 + log₃₂ S) |

Where:
- P = prefix key length (bytes)
- S = number of attributes per entity
- R = number of AttributeRuns for the queried attribute
- R' = runs in the queried time range
- W = number of EntityVersions in the affected window
- log₃₂ S = CHAMP depth (1 for S < 32, 2 for S < 1024)

**Key improvements over the Y-fast trie design:**

| Query | Old (Y-fast) | New (entity-local) | Improvement |
|---|---|---|---|
| `latest(prefix, attribute)` | O(P + 12) | O(P + 1) | No ART traversal — direct CHAMP |
| `allFieldsAt(prefix, T)` | O(P + F × 12) | O(P + 8 + S) | One predecessor + one CHAMP walk |
| `put()` — coalesce | O(P + 12 + B) | O(P + 12) | No bucket copy — leaf-value update |

### 18.18 API reference

Phase 1 (hot store) API. Cold store (freeze), HINT (scanOverlap), and archive
operations are deferred to phase 2+.

```java
public class TaoTree implements AutoCloseable {

    // --- Factory ---
    static TaoTree create(Path path, KeyLayout prefixLayout);
    static TaoTree open(Path path);

    // --- Write ---
    WriteScope write();

    class WriteScope implements AutoCloseable {
        /** Insert or update a attribute value at a specific timestamp. */
        void put(KeyHandle prefix, String attribute, byte[] value, long timestampMs);

        /** Remove an attribute (tombstone) at a specific timestamp. */
        void remove(KeyHandle prefix, String attribute, long timestampMs);
    }

    // --- Read ---
    ReadScope read();

    class ReadScope implements AutoCloseable {
        /** Latest value of an attribute (uses current_state_root — fastest path). */
        AttributeValue latest(QueryHandle prefix, String attribute);

        /** Value of an attribute at a specific point in time. */
        AttributeValue at(QueryHandle prefix, String attribute, long timestampMs);

        /** All attribute values at a specific point in time. */
        void allFieldsAt(QueryHandle prefix, long timestampMs,
                         AttributeVisitor visitor);

        /** Full history of an attribute (all runs, ordered by first_seen). */
        void history(QueryHandle prefix, String attribute,
                     AttributeRunVisitor visitor);

        /** Bounded history of an attribute within a time range. */
        void history(QueryHandle prefix, String attribute,
                     long fromMs, long toMs, AttributeRunVisitor visitor);

        /** Number of tracked entitys in the global ART. */
        long size();
    }

    // --- Key builders ---
    KeyBuilder newKeyBuilder(WriterArena arena);
    QueryBuilder newQueryBuilder(WriterArena arena);

    // --- Lifecycle ---
    void sync();
    void compact();
    void close();
}

/** Immutable value returned by latest() and at(). */
public record AttributeValue(byte[] value) {
    static final AttributeValue NOT_FOUND = null;
}

/** A single attribute run (contiguous time interval with one value). */
public record AttributeRun(long firstSeenMs, long lastSeenMs, long validToMs,
                        byte[] value) {
    /** True if this run is active at the given timestamp. */
    boolean activeAt(long timestampMs) {
        return firstSeenMs <= timestampMs && validToMs >= timestampMs;
    }

    /** True if this run overlaps the given time range. */
    boolean overlaps(long fromMs, long toMs) {
        return firstSeenMs <= toMs && validToMs >= fromMs;
    }
}

@FunctionalInterface
public interface AttributeRunVisitor {
    /** Return true to continue, false to stop early. */
    boolean visit(AttributeRun run);
}

@FunctionalInterface
public interface AttributeVisitor {
    /** Return true to continue, false to stop early. */
    boolean visit(String attribute, byte[] value);
}
```

**API notes:**

- `open(Path)` takes no layout parameter — the persisted temporal descriptor carries
  the `KeyLayout` definition and attribute dictionary binding, ensuring safe reopen
  without risk of layout mismatch.
- Values are `byte[]` rather than `String` — the temporal store is value-type agnostic.
  String encoding is the caller's responsibility. This allows numeric, binary, and
  structured values without conversion overhead.
- `KeyBuilder` requires a `WriterArena` (for dictionary `intern()` calls).
  `QueryBuilder` also requires a `WriterArena` for arena-scoped key allocation, but
  uses lock-free `resolve()` for dictionary lookups.
- The `AttributeRun.validToMs` field is exposed for advanced callers that need interval
  semantics. For simple queries, `activeAt()` and `overlaps()` are sufficient.
- `remove()` creates a tombstone AttributeRun. The attribute disappears from
  `allFieldsAt()` results for timestamps after the removal. History still shows the
  full lifecycle including the tombstone event.
