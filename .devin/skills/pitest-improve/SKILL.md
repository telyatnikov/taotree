# Skill: Analyze Pitest Mutation Results and Improve Tests

## When to use
When the user asks to analyze mutation testing results, improve test coverage, or kill surviving mutations.

## Steps

### 1. Run pitest and parse results

```bash
cd /Users/alexander.telyatnikov/dev/taotree
./gradlew :core:pitest
```

Parse the XML report (much better than HTML for analysis):

```bash
python3 -c "
import xml.etree.ElementTree as ET
tree = ET.parse('core/build/reports/pitest/mutations.xml')
root = tree.getroot()

from collections import defaultdict
stats = defaultdict(lambda: {'total': 0, 'killed': 0, 'survived': 0, 'no_cov': 0, 'timed_out': 0})

for m in root.findall('mutation'):
    f = m.find('sourceFile').text.replace('.java', '')
    status = m.get('status')
    stats[f]['total'] += 1
    if status == 'KILLED': stats[f]['killed'] += 1
    elif status == 'SURVIVED': stats[f]['survived'] += 1
    elif status == 'NO_COVERAGE': stats[f]['no_cov'] += 1
    elif status == 'TIMED_OUT': stats[f]['timed_out'] += 1

print(f'{\"Class\":<18} {\"Total\":>6} {\"Kill\":>6} {\"T/O\":>5} {\"Surv\":>6} {\"NoCov\":>6} {\"Kill%\":>6}')
print('-' * 55)
for f in sorted(stats.keys()):
    s = stats[f]
    eff = s['killed'] + s['timed_out']
    pct = round(100 * eff / s['total']) if s['total'] > 0 else 0
    print(f'{f:<18} {s[\"total\"]:>6} {s[\"killed\"]:>6} {s[\"timed_out\"]:>5} {s[\"survived\"]:>6} {s[\"no_cov\"]:>6} {pct:>5}%')
"
```

### 2. Extract surviving mutations by class

```bash
python3 -c "
import xml.etree.ElementTree as ET
tree = ET.parse('core/build/reports/pitest/mutations.xml')
root = tree.getroot()

for m in root.findall('mutation'):
    if m.get('status') in ('SURVIVED', 'NO_COVERAGE'):
        f = m.find('sourceFile').text.replace('.java', '')
        method = m.find('mutatedMethod').text
        line = m.find('lineNumber').text
        desc = m.find('description').text
        status = m.get('status')
        print(f'{status:12s} {f:18s} L{line:>4} {method:30s} {desc}')
"
```

### 3. Analyze and classify mutations

Before writing tests, classify each surviving mutation:

| Category | Example | Killable? | Strategy |
|----------|---------|-----------|----------|
| **Safety guard removal** | `removed call to checkAccess` | Yes | Test that closed/invalid scope throws |
| **Validation bypass** | `removed call to validateKeyLen` | Yes | Test with wrong-length keys |
| **Return value change** | `replaced return with null/0/true` | Yes | Assert the actual return value |
| **Boundary condition** | `changed conditional boundary` (`<=` → `<`) | Maybe | Only if the boundary value is reachable |
| **Memory cleanup** | `removed call to SlabAllocator::free` | Hard | Check `totalSegmentsInUse()` before/after delete |
| **Lock release removal** | `removed call to unlock` | Yes | Causes deadlock → TIMED_OUT = killed |
| **Equivalent mutation** | `<= 0` → `< 0` when input is never 0 | No | Skip these |
| **File I/O** | `removed call to sync/close/force` | Yes | Round-trip test: write → close → reopen → verify |
| **Init zeroing** | `removed call to MemorySegment::set` in init | Hard | Only if non-zeroed memory causes visible failure |

### 4. Prioritize by ROI

Focus on classes with lowest kill rates first. Skip classes already at 95%+.

Priority order for test writing:
1. **NO_COVERAGE mutations** — just need any test that exercises the code path
2. **Return value mutations** — add assertions on return values
3. **Safety guard removals** — test misuse scenarios (closed scope, wrong thread, bad args)
4. **File-backed round-trips** — create → insert → close → reopen → verify
5. **Memory management** — insert many, delete all, check `totalSegmentsInUse()` decreased

### 5. Write tests

Follow existing test patterns in the project:
- Use `try (var tree = TaoTree.open(...))` for in-memory tests
- Use `@TempDir Path tmp` + `TaoTree.create(file, ...)` for file-backed tests
- Use `try (var w = tree.write()) { ... }` and `try (var r = tree.read()) { ... }` for scoped access
- DO NOT modify existing tests — only add new ones
- Append new test methods before the final `}` of each test class

### 6. Verify

```bash
./gradlew :core:test          # all tests pass
./gradlew :core:pitest        # check improvement
```

## Project structure

- Source: `core/src/main/java/org/taotree/` and `core/src/main/java/org/taotree/internal/`
- Tests: `core/src/test/java/org/taotree/` and `core/src/test/java/org/taotree/internal/`
- Pitest config: `core/build.gradle.kts` (mutators = `STRONGER`)
- Reports: `core/build/reports/pitest/mutations.xml` (XML), `core/build/reports/pitest/index.html` (HTML)

## Current state

Mutator group: **STRONGER** (14 mutators — DEFAULTS + RemoveConditionals EQUAL_ELSE + ExperimentalSwitch)

| Metric | Value |
|--------|-------|
| Mutations generated | 1,662 |
| Mutations killed | 1,317 (79%) |
| Line coverage | 92% |
| NO_COVERAGE | 71 |
| Test methods | 295 |

Classes at 100%: KeyField, KeyLayout, NodePtr, OverflowPtr

Weakest classes: BumpAllocator (55%), ChunkStore (64%), SlabAllocator (70%), TaoTree (72%)

Remaining survivors are predominantly: equivalent boundary mutations, internal allocator arithmetic, file-backed error paths, and `SlabAllocator::free` removal mutations (memory leaks invisible to functional tests).
