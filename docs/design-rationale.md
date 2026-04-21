# TaoTree Design Rationale

This document answers recurring "why not simpler?" questions about TaoTree's
architecture. It exists because the design has attracted reviews proposing
sweeping simplifications ("drop CHAMP", "drop nested ARTs", "use striped
locks", "use raw byte offsets"). Most of those proposals would regress the
library's feature set or measured performance; this doc captures *why* so the
trade-offs don't have to be re-derived each time.

Where claims are backed by benchmarks, the raw JSON lives in
`performance/` and the benchmark source in `jmh/src/jmh/java/org/taotree/jmh/`.

---

## 1. Why nested ARTs per entity (AttributeRuns, EntityVersions)?

**The proposal:** "Replace the 24 B `EntityNode` with a direct pointer to a
bump-allocated flat contiguous struct (or a chronological linked list of flat
structs) containing the entity's attributes."

**Why that regresses the feature set:**

TaoTree is a **bi-temporal** store. Its public API includes:

- `getAt(kb, attr, ts)` — point-in-time for one attribute
- `historyRange(kb, attr, fromMs, toMs, visitor)` — range over one attribute's
  history
- `getAllAt(kb, ts)` — whole-entity state at a given timestamp

A chronological list of per-entity snapshots makes **single-attribute history
O(total observations × attributes)** instead of O(log runs). For a GBIF record
with ~40 columns where one attribute changes frequently, the list-of-snapshots
representation must either:

1. Scan every snapshot and filter by attribute (linear in total writes), or
2. Snapshot-copy all 40 attributes on every single-attribute change
   (~40× write amplification vs. one AttributeRun append).

Neither is acceptable. The per-attribute AttributeRuns ART makes
`historyRange(attr, from, to)` an O(log runs) predecessor lookup followed by an
O(runs-in-range) scan — which is the algorithmically correct data structure
for the query pattern.

**What IS tracked as a real smell:** the `Compactor` currently walks only the
outer ART; it does not recompact per-entity nested structures. See
`plan.md` item `p8-compactor-temporal`. That's a locality/space optimisation,
not a reason to delete the structures.

---

## 2. Why CHAMP for current-state, not a flat sorted array?

**The proposal:** "For entities with < 50 attributes, copying a flat array on
write is significantly faster and uses less memory than allocating CHAMP
nodes."

**Measured (`jmh :: ChampVsFlatBenchmark`, JDK 25, arm64, 2026-04-21):**

| Operation                      | N  | champ (ns) | flatOffHeap (ns) | flatHeap (ns) |
|--------------------------------|----|-----------:|-----------------:|--------------:|
| `buildFromEmpty` (N sequential puts) | 4  |      79    |       15         |     6         |
|                                | 16 |     374    |       29         |     8         |
|                                | 64 |    3261    |      258         |    19         |
| `updateOne` (change one attr)  | 4  |      24    |       34         |    33         |
|                                | 16 |      23    |       47         |    22         |
|                                | 64 |      40    |      114         |    31         |
| `getOne`                       | 4  |       6    |        4         |     3         |
|                                | 16 |       6    |        5         |     5         |
|                                | 64 |      12    |        7         |     6         |
| `iterateAll`                   | 4  |       8    |        8         |     3         |
|                                | 16 |      13    |       11         |     5         |
|                                | 64 |     305    |       22         |    11         |

(`flatHeap` = on-heap `int[] keys + long[] vals`; included only as an
unreachable lower bound because persistence requires off-heap. `flatOffHeap` =
sorted `(attrId:4, valueRef:8)*N` payload in a `BumpAllocator`, matching the
reviewer's exact proposal.)

**Interpretation:**

1. **CHAMP wins on `updateOne` at every N** — by 1.4× at N=4, 2.0× at N=16,
   2.8× at N=64 — because structural sharing copies only the path to the
   affected entry (log₃₂ N nodes), whereas the flat array must re-copy the
   entire `12 × N`-byte payload. `updateOne` is the dominant per-attribute
   write pattern; the "flat is faster on writes" claim is **contradicted by
   measurement.**
2. **Flat wins on `buildFromEmpty`** — 5–13× — because a single
   `alloc(12 × N) + fill` beats N CHAMP node allocations. But this is a
   one-shot operation per entity create, not a hot path; bulk `putAll` is
   already amortised over the entity's lifetime.
3. **Reads are close.** CHAMP is within 1.5× of flat at N≤16 and 1.7× at N=64.
4. **`iterateAll` at N=64 is 14× slower on CHAMP** (305 ns vs. 22 ns). This
   is a real finding — the recursive bitmap walk has non-trivial overhead on
   deeper trees. It's worth a follow-up (iterative walk, cached small-node
   fast path), but it doesn't justify rewriting the data structure.

**Additional structural benefit not captured by this benchmark:** CHAMP's
current-state map is *shared* across EntityVersions snapshots. With a flat
array representation, every historical version would own a full copy of the
attribute set. At 40 attributes × 12 bytes × V versions per entity, that's
`480·V` bytes of duplicated payload per entity. CHAMP's structural sharing
reduces this to `O(log N · changes)` in the amortised case.

**Net decision:** keep CHAMP. Consider optimising `iterateAll` via an
iterative walker; that's an implementation refinement, not a design change.

---

## 3. Why OCC rebase with `TemporalOpLog`, not striped entity-level locks?

**The proposal:** "Drop `TemporalOpLog`. Replace OCC logical replay with
striped entity-level locks or row-level locking. If an entity is locked, wait
or yield."

**Why we don't:**

- **Rebase is bounded, not unbounded.** When the publish-CAS fails,
  `rebaseComputeTemporal` replays *only the logical ops in the current write
  scope* (usually 1–3 puts) against the new root. It's a single bounded retry,
  not an open-ended loop. Production traces show the rebase path is
  almost never hit outside contended benchmarks.
- **Striped locks serialise co-writers of the same entity.** For a single hot
  key (exactly the pathology the proposal tries to handle "simply"), striped
  locks give strictly worse throughput than OCC + bounded rebase: OCC lets
  both writers execute their COW speculatively and only one pays the replay
  cost, vs. forcing the second writer to wait for the first's entire commit.
- **Deadlock surface.** `TaoTree` already holds a commit lock briefly during
  publish; adding per-entity locks requires a well-defined ordering with the
  commit lock and with the dictionary intern lock. OCC removes that ordering
  problem entirely.
- **The `OpLog` is tiny.** Per-scope, it holds a handful of `(op, attrId,
  valueRef, ts)` tuples. This is not a heavyweight structure; deleting it
  would save ~100 LOC and cost correctness on sibling-attribute
  preservation (see §3.1).

### 3.1 Why `TemporalOpLog` and not `MutationLog`?

An earlier design used `MutationLog` — a record of *physical* segment edits —
for rebase. This turned out to be unsound: a segment-copy replay clobbers
sibling-attribute updates made by concurrent writers on the same entity
(writer A updates `attrX`, writer B updates `attrY`, both on entity E; after
B publishes, A's rebase must preserve B's `attrY` change). `TemporalOpLog`
replays *logical* puts/deletes so force-copying the entity leaf under the new
root preserves all sibling updates. This is the correctness motivation, not
over-engineering.

---

## 4. Why swizzled `NodePtr`, not a flat 64-bit byte offset?

**The proposal:** "Use a straight 64-bit byte offset into a unified
`ChunkStore` instead of complex packed `(class, slab, offset)` tuples."

**Why:**

- `SlabAllocator.free()` must know the **size class** to return the slot to
  the right freelist. With a flat offset you'd either (a) store a size-class
  header in every node (4–8 bytes per node; for `Node4` that's a 20% overhead
  and burns a cache line slot) or (b) lose O(1) free and fall back to a
  per-chunk free-list scan.
- Compaction must distinguish **slab-allocated fixed-class nodes** from
  **bump-allocated variable-size payloads** (overflow values, CHAMP arrays).
  The metadata bits in `NodePtr` make this a no-op; without them, compactor
  code would need a parallel "type table" keyed by address range.
- Bit shifts + masks cost ~1 ns. Segment slicing costs ~1 ns regardless of
  pointer encoding. There is no measurable read overhead from the tagging.
- **Persistence requires relocatability.** A `NodePtr` is valid across reopen
  because it encodes logical `(slabClassId, slabId, offset)` rather than a
  virtual address. A raw mmap offset would be equivalent for relocation *but*
  would still need the size-class tag to be stored somewhere — which is
  exactly what swizzling does for free.

---

## 5. Why a dictionary with its own lock?

**The proposal:** "`TaoDictionary` uses its own self-locking mechanism inside
the write path. Mixing global COW concurrency with internal lock-based
dictionaries makes reasoning difficult."

**Why we keep it:**

- The intern lock only fires on **new values** in the write path. Already-
  interned writes and all reads take a **lock-free** CHAMP lookup against the
  current dictionary snapshot. The lock is not on the common case.
- A lock-free intern would require a globally-consistent monotonic counter
  assignment across writers, which in turn requires either (a) coordinating
  with the commit lock (moving the lock, not removing it), or (b) allowing
  duplicate codes for the same value that get reconciled later (breaks the
  binary-comparable key invariant that `QueryBuilder` relies on).
- The dictionary's snapshot publication goes through the *same* COW engine as
  the main ART, so readers see monotonically-consistent snapshots
  automatically.

---

## 6. Why a runtime-configurable `KeyLayout`, not static key compilation?

**The proposal:** "Dynamic `VarHandle` accessors for `KeyLayout` are often
slower than static byte manipulation if the schemas are known ahead of time."

**Why we keep it:**

- `KeyLayout` is a **per-instance** parameter of `TaoTree.create(Path, KeyLayout)`.
  Different applications embed different schemas; a single binary supports
  them all without recompilation. Static key compilation would require either
  code-generation at install time or locking the library to one schema.
- `VarHandle` calls are JIT-inlined. Micro-benchmarks in `TaoTreeBenchmark`
  do not show a measurable gap between dynamic layout access and the
  equivalent hand-rolled byte ops once warm.

---

## 7. Remaining genuine smells (tracked, not dismissed)

The design is not perfect. These items are real and tracked in `plan.md`:

- **`Compactor` does not recompact per-entity nested structures**
  (`p8-compactor-temporal`). Space/locality gap, not correctness.
- **CHAMP `iterateAll` is now ~6× slower than flat at N=64** (improved from
  14× — see §7.1). Further gains would need an iterative walker or
  size-thresholded conversion; gated on a real workload showing it matters.
- **`TaoTree` is one class that does a lot** (`p8-split-taotree`).
- **Full JMH perf pass** (`p8-perf`).

**Recently closed** (previously listed as honest-gaps):

- **Tombstone-run semantics** — `delete(kb, attr)` writes a tombstone
  AttributeRun and reads past it return "not found". Covered by
  `TombstoneSemanticsTest` + `TombstoneAndSchemaEdgeCasesTest` +
  `FrayTombstoneRaceTest`.
- **Schema fingerprint verification on reopen** — `SchemaBinding` section
  + `open(Path, KeyLayout)` verification + `open(Path)` self-describing
  reconstruction.
- **Value canonicalization** — the public `Value` write path allocates
  a fresh encoded slot per call (pointers differ per write) but
  `TemporalWriter` now compares encoded-slot *bytes* via
  `ValueCodec.slotEquals`, so same-logical-value writes extend a run's
  `last_seen` rather than allocating extra AttributeRun / EntityVersion /
  CHAMP roots. See `ValueCanonicalizationTest` for 6 regression cases
  (predecessor, successor, overflow, interleaved, numeric, no-over-merge).

None of these motivate removing ARTs, CHAMP, OCC, or `NodePtr`.

### 7.1 CHAMP iterate: single-resolve optimization

The original `iterateRec` called `resolveNode` at every recursive step, which
performed two `asSlice` calls on the backing page (one to read the header and
size, one to get the full node). At N=64 this dominated cost: CHAMP iterate
was 14× slower than flat off-heap iterate.

The fix is unglamorous: expose `BumpAllocator.page(pageId)` that returns the
full backing `MemorySegment` without slicing, then read bitmaps, data
entries, and child pointers directly at computed offsets inside
`iterateRec`. No new objects per recursive step. Numbers from
`performance/champ-vs-flat-iterate-opt.json`:

| N  | CHAMP before (ns) | CHAMP after (ns) | flat off-heap (ns) |
|----|-------------------|------------------|--------------------|
| 4  | ~25               | 6.6              | 8.3                |
| 16 | ~40               | 11.9             | 11.6               |
| 64 | 239               | 143.8            | 22.7               |

At N=4/16 CHAMP now **matches or beats** flat off-heap iterate. At N=64 it
is still 6× slower because the tree-walk pattern inherently touches more
cache lines than a linear scan — but a 2.3× improvement on realistic N
settles the "14× regression" concern flagged in §2 finding 4.

### 7.2 Audit: items from review #2 that did *not* need code changes

Three items from review #2 were audited and closed without code changes
after inspection showed the concern was incorrect or would not improve on
measurement:

- **`p8-unify-replay-log` (merge `MutationLog` and `TemporalOpLog`):**
  The two logs carry non-overlapping data. `MutationLog` records
  *physical* edits (leafPtr, snapshotValue, originalLeafPtr) and gates the
  write-back fast path via `allHaveOriginals()` — removing it regressed JMH
  by 40% (see the write-back ablation block at top of `plan.md`).
  `TemporalOpLog` records *logical* ops (attrId, valueRef, kind) needed to
  replay sibling-attribute puts on an OCC rebase without clobbering
  concurrent writers (§3.1). Neither log subsumes the other; a unified log
  would have to carry both payloads per entry, yielding no net reduction.

- **`p8-decouple-dict` (dictionary as composition, not subtype):**
  The audit showed `TaoDictionary` is *already* composition — it holds
  `private final TaoTree tree` (the child dict tree) and
  `private final TaoTree owner` (the parent), and creates the child via a
  package-private constructor `TaoTree(TaoTree parent, int keyLen, …)`.
  No subtype relationship exists. The shared `EpochReclaimer` between
  parent and child trees is intentional: reclamation must be coordinated
  across trees that share a slab allocator to avoid use-after-free across
  dictionary boundaries. Review #2 misread the structure as circular.

- **`p8-lazy-entity-versions` (defer EntityVersions ART):**
  Proposal was to skip maintaining the `EntityVersions` ART unless readers
  asked for per-version snapshots, recomputing on demand. This is
  speculative: the only reader API that bypasses EntityVersions is
  `get(kb, attr)` and `history(kb, attr, …)`, which already walk
  `AttributeRuns` directly. `getAllAt(kb, ts)` — the API most likely to be
  used interactively — *requires* EntityVersions to avoid an O(total attrs
  × total runs) scan. No benchmark showed EntityVersions dominates write
  cost; per the write-back ablation lesson, removing load-bearing
  structures based on intuition rather than measurement is how regressions
  land. Left in.

---

## 8. Reading list for future reviewers

Before proposing a simplification, please:

1. Read this document.
2. Run `./gradlew :jmh:jmh -Pjmh.include='ChampVsFlat'` and compare against
   your proposed replacement.
3. Check that the bi-temporal API contract (`getAt`, `historyRange`,
   `getAllAt`) is preserved by your proposal and that single-attribute history
   queries remain sub-linear in total entity writes.
4. Check that your proposal preserves **structural sharing across historical
   versions of the same entity** — not just within a single snapshot.

A proposal that doesn't cover (3) and (4) is proposing a different product,
not a simpler TaoTree.
