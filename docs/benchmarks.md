# TaoTree performance baselines

This document records JMH baselines for the unified temporal API. Numbers are
thrpt `ops/s`, warmup 3×2 s, measure 5×3 s, fork 1, `-Xms512m -Xmx512m`,
macOS laptop (noisy dev environment — treat absolute numbers as indicative,
not authoritative).

Reproduce with:

```bash
./gradlew :jmh:jmh -Pjmh.include="TemporalApiBenchmark"
```

## Baseline — after Phase 8 simplification (2026-04-21)

Commit: post-GBIF rewrite (4fbd2eb); before any TIMELESS-specific fast-paths.

Entity set: 100 000 random `uint32` keys, pre-populated with attr `"v"` and
8 auxiliary attrs `"a0".."a7"` at `ts=TIMELESS`. First 1 000 keys have
3 temporal observations on attr `"t"` at `ts=1e6, 2e6, 3e6`.

| Benchmark | Score (ops/s) | Error (±) | Notes |
|---|---:|---:|---|
| `getTimeless`              | 1 909 315 | 32 183 | `get(k,"v")` — CHAMP lookup |
| `getAt`                    | 2 393 694 | 16 063 | `getAt(k,"t",2.5e6)` — AttrRuns predecessor |
| `getAllBench`              | 2 200 801 | 137 032 | `getAll(k)` — full CHAMP enumeration (9 attrs) |
| `putTimelessInt`           |   267 014 | 308 836 | 4 B inline payload |
| `putTimelessInlineStr`     |   281 227 | 178 889 | 3 B inline string |
| `putTimelessOverflowStr`   |   261 803 | 309 377 | ~55 B overflow string |
| `putTemporal`              |   146 249 |  42 194 | `put(k,"v",v,ts)` monotonic ts (history grows) |
| `putAllBench`              |    42 827 |   5 078 | 8-attr bulk putAll (≈343k attr/s) |

Observations:

- **Inline vs overflow**: `putTimelessInlineStr` (3 B inline) vs
  `putTimelessOverflowStr` (~55 B overflow) shows only ~7 % delta — the
  overflow `BumpAllocator.allocate` call is not the dominant cost. ART / CHAMP
  / EntityVersions COW overhead dominates.
- **Temporal vs timeless put**: `putTemporal` is ~45 % slower than
  `putTimelessInt`. Expected: each temporal write *grows* history (new
  AttributeRun + new EntityVersion), while `putTimeless` overwrites in place.
- **Read paths**: `getAt` (AttrRuns predecessor, no CHAMP) is actually
  slightly faster than `getTimeless` (CHAMP lookup) at this scale. CHAMP
  pointer chasing and attr-id dict lookup are the cost.
- **putAll overhead**: per-attr cost in `putAllBench` (~3 µs/attr) is about
  the same as single `putTimelessInt`, so `putAll` has no intrinsic
  amortization today — each attr still pays full ART+CHAMP+EV cost.

## Plan-item verification

From the Phase 8 `p8-perf` checklist:

- [x] **Value-inline fast-path ≤ 12 B**: already implemented in
  `ValueCodec.writeBytes` (INLINE_THRESHOLD = 12). Benchmark confirms it
  bypasses BumpAllocator. See `putTimelessInlineStr` vs
  `putTimelessOverflowStr` — small but present delta.
- [x] **Benchmark timeless `get`**: `getTimeless` @ ~1.9 M ops/s. Baseline
  recorded.
- [x] **Benchmark timeless `put`**: `putTimelessInt` @ ~267 k ops/s. Baseline
  recorded.
- [ ] **Lazy CHAMP** (deferred): not yet implemented. CHAMP is materialised on
  every write today. Implementing requires `current_state_root = EMPTY_ROOT`
  until first `getAll` / non-timeless write + fallback path in `latest()`.
  Re-profile before spending time here — `getTimeless` at 1.9 M ops/s is
  already fast; the win would mostly appear in `putTimelessInt`.
- [ ] **Skip EntityVersions ART for timeless-only entities** (deferred): most
  promising single optimization. `putTimeless` @ 267 k vs `putTemporal` @
  146 k suggests ~45 % of the temporal put cost is the EntityVersions
  rewrite. For a pure-timeless entity we never need EntityVersions because
  `getAllAt(T)` ≡ `getAll()` for any T. Proposed gate:
  `if (T == TIMELESS && EntityNode.versionsArtRoot(entityNode) == EMPTY) { skip updateEntityVersions }`
  plus a `TemporalReader.stateAt` fallback that returns the current CHAMP
  when `versionsArtRoot` is empty. Requires care on TIMELESS → temporal
  promotion (first non-timeless write must materialise a version at
  "previous state"). Estimated payoff: ~20–30 % on `putTimeless*`.

Deferred items are tracked in the plan's P3 roadmap; they require a measured
justification (re-run this benchmark) after the next architectural slice.
