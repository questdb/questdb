# Phase 17: Address `/review-pr 6946` follow-ups - Research

**Researched:** 2026-04-22
**Domain:** SAMPLE BY FILL codegen + cursor + PR body hygiene
**Confidence:** HIGH (all claims verified against in-repo source and `origin/master`)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
D-01 through D-28 as stated in `17-CONTEXT.md` `<decisions>` block. This research does **not** relitigate them; it grounds each one with line numbers, master-parity evidence, and concrete fixture shapes so the planner can write tasks directly.

- D-01 PR body edit principle: row only for new behaviour / new grammar / currently-false sentence.
- D-02..D-10 M3 per-commit and M-new verdicts.
- D-11 M1 new-row.
- D-12/D-13 M2 new test + comments + EXPLAIN.
- D-14..D-19 Minor code-fix findings (m1, m2, m3, m5, m6, m7).
- D-20..D-23 Minor test findings (m4, m8a, m8b, m8c).
- D-24 PR title rename.
- D-26/D-27/D-28 M-unit TIMESTAMP/INTERVAL unit-mismatch rejection + test + todo retirement.
- D-25 Planner decides plan partitioning; clustering in `<decisions>` is non-binding.

### Claude's Discretion
- m3 benchmark methodology (JMH vs ad-hoc; threshold for "measurable cost").
- m4 property test harness shape (new file vs inline; type enumeration).
- m8a/b/c exact SQL fixtures (DDL, data, bucket sizes).
- Test method names and alphabetical placement.
- M3.4 optional "What's covered" row — depends on master-support verdict (resolved in `## Findings by Decision` below).

### Deferred Ideas (OUT OF SCOPE)
- Alias-guard test for M3.1 Fix B (`testFillNonTimestampAliasGuard`).
- Arithmetic `a + b` multi-key FILL(PREV) test.
- K x B memory circuit-breaker implementation.
- Fold pass-1 into `SortedRecordCursor.buildChain` callback.
- Non-determinism in SAMPLE BY full-suite runs.
</user_constraints>

<phase_requirements>
## Phase Requirements

No new REQ-IDs. Phase 17 strengthens existing completed requirements via regression coverage and closes PR-body drift. Mapping from `/review-pr 6946` findings to affected requirements:

| Finding | Affected Requirement | Research Support |
|--------|----------------------|-------------------|
| M1 pre-1970 | COR-04 (cursor-parity) | D-11 — net-new behaviour vs master's `cannot FILL for the timestamps before 1970` rejection. |
| M2 pushdown | COR-03 (plan parity), COR-04 | D-12 — `testFillNullPushdownEliminatesFilteredKeyFills` pins the filter-in-inner-GROUP-BY contract. |
| M3.1 | COR-04 | Regression recovery; master already had unit-correct TIMESTAMP. |
| M3.2 | COR-04 | Regression recovery; master polls CB every ingest/emission loop. |
| M3.3 | KEY-01..05, XPREV-01 | Regression recovery; already closed by Phase 16. |
| M3.4 | FILL-04, KFTR-04 | Two sentences stale; see D-05 master-support verdict below. |
| M-new pass-1 CB | COR-04 | Regression recovery; pass-1 at :603 was the only unprotected outer loop left. |
| m1 slot-null | COR-02 (no native memory leaks) | Latent double-close guard. |
| m2/m3/m5/m6/m7 | Code hygiene (CLAUDE.md) | No requirement change. |
| m4 | COR-04 | Prevents silent regression of FillRecord dispatch. |
| m8a DST spring-forward | FILL-05 | Companion to existing fall-back fixture. |
| m8b single-row keyed + FROM/TO | KFTR-02/03/04 | Exercises `hasKeyPrev()` false->true transition. |
| m8c reject-no-arg tightening | Grammar rejection surface | Pins error message + position. |
| m9 PR title | Metadata | Accurately scoped title. |
| M-unit TIMESTAMP/INTERVAL mismatch | XPREV-01 | Currently-wrong behaviour in PR: tagOf collapses MICRO/NANO to same tag; raw-long copy drifts 1000x. |
</phase_requirements>

## Summary

All 28 locked decisions in `17-CONTEXT.md` are technically feasible with the evidence gathered here. Line numbers for every source-code touchpoint were verified against the working-tree HEAD; master-parity claims were verified against `origin/master`. The one live planner-branch in CONTEXT.md (D-05 optional row) resolves to **skip the optional row** — see §D-05 below.

**Primary recommendation:** Partition Phase 17 into **four** plans rather than CONTEXT.md D-25's two-cluster suggestion — see `## Recommended Plan Partitioning`. Cluster A as described is too large for a single bisectable commit and mixes code-only (m1, m2, m3, m5, m6, m7, m-new, M-unit) with test-only (M2, m4, m8a, m8b, m8c) concerns. Cluster B (PR-body + title) stays single-commit.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| PR body / title edits | GitHub PR metadata (via `gh`) | — | External artefact; no code impact. |
| Codegen-time cross-column type rejection (M-unit D-26) | `SqlCodeGenerator.generateFill` | — | Existing `needsExactTypeMatch` predicate; no runtime dispatch. |
| Pass-1 cancellation poll (M-new D-08) | `SampleByFillRecordCursorFactory.initialize()` ingest loop | — | Matches master cursor-path pattern. |
| Predicate-pushdown contract pin (M2 D-12) | `SampleByFillTest` test + EXPLAIN plan assertion | — | Inner GROUP BY filter slot is the mechanism; plan assertion locks it. |
| FillRecord dispatch property (m4 D-20) | New test class | — | Cross-getter invariant; belongs in its own file. |
| `QueryModel.toSink0` fillOffset (m7 D-19) | `QueryModel` serialisation | `ExplainPlanTest` (indirect) | One-line emit next to fillFrom/fillTo. |
| DST spring-forward coverage (m8a D-21) | `SampleByFillTest` test | `timestamp_floor_utc` behaviour (read-only) | Companion to existing fall-back fixture. |
| FillRecord dispatch getter surface | `SampleByFillRecordCursorFactory.FillRecord` inner class | — | 30 typed getters, uniform 4-branch pattern. |

## Findings by Decision

### D-01 (PR body edit principle) — no research needed

Process decision; applied throughout the findings below.

### D-02 (M3.1 regression recovery) — verified HIGH confidence

Master's `SampleByFillValueRecordCursorFactory.createPlaceHolderFunction` (master file exists — `git show origin/master:.../SampleByFillValueRecordCursorFactory.java` verified) has the `TimestampDriver.parseQuotedLiteral` + `Chars.isQuoted` gate at :144-173. The PR's current `SqlCodeGenerator.generateFill` per-column TIMESTAMP branch at `:3635-3663` mirrors this pattern verbatim (`isQuoted` gate, quoted-literal re-parse, `Misc.free` + `setQuick`). Fix A restored an already-known-to-master pattern; Fix B (non-TIMESTAMP alias guard) hardens a fast-path codepath master never had. **No body row needed.** No additional test needed.

### D-03 (M3.2 cancellation CB regression) — verified HIGH confidence

`SampleByFillRecordCursorFactory.java` `statefulThrowExceptionIfTripped()` sites:
- `:382` (`hasNext()` entry)
- `:530` (`emitNextFillRow()` outer while)

Verified: `grep -n "statefulThrowExceptionIfTripped" core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` returns exactly these two lines.

Master's two cursor-path witnesses (verified via `git show origin/master:.../SampleByFillPrevRecordCursor.java` and `.../SampleByFillValueRecordCursor.java`):
- `SampleByFillPrevRecordCursor.java`: `:171`, `:217` (inside `while (baseCursor.hasNext())`)
- `SampleByFillValueRecordCursor.java`: `:183`, `:229` (inside `while (baseCursor.hasNext())`)

Both cursor-path files call `circuitBreaker.statefulThrowExceptionIfTripped()` as the **first statement** inside `while (baseCursor.hasNext()) { ... }` bodies. The PR's fast path mirrored these for emission-side sites via `c1deb9b14d`; pass-1 ingest is covered by D-07/D-08 below.

**K x B bullet rewrite (D-03) verbatim wording:** confirmed present in CONTEXT.md and in the plan file (`### M3.2 — Circuit-breaker regression`). Plan file line states "unqualified 'CB' without cancellation qualification" — verified against the live PR body dump below (§D-11 evidence) — the current body's "Easy extensions" K x B bullet reads:

> **`K × B` circuit-breaker.** A size-estimated short-circuit in `generateFill` that falls back to the cursor path when `unique_keys × buckets` exceeds a configurable threshold, giving operators a single knob to cap the `SortedRecordCursor` materialisation footprint.

It does not disambiguate from the cancellation CB. The rewrite per D-03 appends the parenthetical "(Unrelated to the cancellation `SqlExecutionCircuitBreaker`, which is honoured by the fill cursor.)".

### D-04 (M3.3 FUNCTION/OPERATION classifier) — verified HIGH confidence

Phase 16 Plan 01 already landed the classifier widening and -ea assert (commit `82865efbc0`). `STATE.md` records it at `[Phase 16]` entries. No Phase 17 work. **No body row needed.**

### D-05 (M3.4 TZ + FROM + OFFSET fill-grid) — MASTER-SUPPORT VERDICT

**Probe methodology:** Cross-read three artefacts:
1. Current `SqlOptimiser.rewriteSampleBy` `setFillOffset` site at `:8323-8325` (widened condition `sampleByTimezoneName == null || (hasSubDayTimezoneWrap && sampleByFrom != null)`).
2. `git show origin/master:core/src/main/java/io/questdb/griffin/SqlOptimiser.java` — searched for `setFillOffset` and `fillOffset`: **no matches**. Master's equivalent block at `:8300-8340` calls only `setFillFrom`, `setFillTo`, `setFillStride`, `setFillValues` — no `setFillOffset` call exists anywhere in master.
3. `git show origin/master:core/src/main/java/io/questdb/griffin/model/QueryModel.java` — searched for `fillOffset`: **no matches**. Master's `QueryModel` has no `fillOffset` field at all.

**Master-path support shape:** master's `SampleByFillNullRecordCursorFactory` (verified at master path) takes `timezoneNameFunc` + `offsetFunc` as constructor params (lines 76-80). The cursor-path fill factory handles sub-day + TZ + FROM + OFFSET via its own sampler state machine, not via `QueryModel.fillOffset`.

**Verdict:** Master's **cursor path** supports `SAMPLE BY 1h FROM ... TZ '...' WITH OFFSET '...' FILL(NULL)` via the factory-constructor offset func. The PR's **fast path** is net-new machinery (`QueryModel.fillOffset` + `setFillOffset` wiring), and `289d43090a` fixed a mid-term regression in that fast path against the already-correct cursor-path behaviour. The user-visible behaviour (query succeeds with correct output) was already present in master via the cursor path. Under D-01 rule (a) "new user-visible behaviour vs master", the shape is **not** net-new — fast-path participation is an implementation detail.

**Recommendation:** **SKIP the optional "What's covered" row.** Only the two stale sentences get rewritten per D-05 verbatim plan-file wording. If the user later disagrees based on wanting to advertise "this shape now runs on the fast path", the row can be added post-phase without re-running research — but the D-01 principle as stated excludes it.

**Falsifiability:** If the planner or user objects: the falsifier is "does master's cursor path correctly emit `testFillSubDayTimezoneFromOffset`'s expected output?" — answer: yes, it does, given `SampleByFillNullRecordCursorFactory` takes timezone+offset funcs. Fast-path participation is an **optimisation**, not new behaviour.

**Two stale sentences (unambiguous rewrites):**
1. Implementation-notes "Calendar offset alignment" pitfall: current body contains *"The optimizer only sets `fillOffset` when `sampleByTimezoneName == null`."* — this is now factually wrong because `:8323` condition also fires for `hasSubDayTimezoneWrap && sampleByFrom != null`. Replace per plan-file verbatim wording ("The optimizer sets `fillOffset` when `sampleByTimezoneName == null`, or — under sub-day + TZ + FROM — when `rewriteSampleBy` has already stripped TZ from `timestamp_floor_utc` and wrapped FROM in `to_utc(FROM, tz)`, making the GROUP BY grid purely UTC-anchored and requiring the fill cursor to reproduce the `+ offset` shift.").
2. "What's covered" block 2 row `ALIGN TO CALENDAR WITH OFFSET + FILL without TO`: current body contains *"Fixed via `QueryModel.fillOffset` propagation; offset applied only when `sampleByTimezoneName == null`..."* — replace with plan-file verbatim wording ("Fixed via `QueryModel.fillOffset` propagation; offset applied when `sampleByTimezoneName == null`, and also when sub-day + TZ + FROM requires it (the timezone case is otherwise absorbed by `timestamp_floor_utc`).").

### D-06 (M3.5 test-count drift) — trivial, no research

Drop two lines. Current PR body Test plan shows `SampleByFillTest: 110 tests` and `SampleByNanoTimestampTest: 278 tests mirror the microsecond suite`. Both phrasings exist in the live PR body (confirmed via `gh pr view 6946`). Both lines go away.

### D-07 / D-08 / D-09 / D-10 (M-new pass-1 CB gap) — verified HIGH confidence

Pass-1 loop site verified at **exactly line 603**:

```
600            if (keysMap != null) {
601                keysMap.clear();
602                int keyIdx = 0;
603                while (baseCursor.hasNext()) {
604                    MapKey key = keysMap.withKey();
605                    keySink.copy(baseRecord, key);
606                    MapValue value = key.createValue();
607                    if (value.isNew()) {
608                        value.putLong(KEY_INDEX_SLOT, keyIdx++);
609                        value.putLong(HAS_PREV_SLOT, 0L);
610                    }
611                }
```

CONTEXT.md said "~:603"; exact match.

Insertion point per D-08: as the first statement inside the `while` body at line 604 (verbatim master pattern). Cost: one `stateful` call per base-cursor row — stateful CB is a flag check; no measurable overhead unless tripped.

Dedicated test (D-09) data shape that guarantees pass-1 trip:
- Large base cursor (~10_000 rows suffices for Phase 15's harness).
- No `TO` specified or single-bucket range, so emission-phase polls (at :382, :530) never get reached before pass-1 finishes.
- Tick-counting `MillisecondClock` CB from `testFillKeyedRespectsCircuitBreaker` (Phase 15) — mirrors directly.
- Regression-coverage self-check: revert the `statefulThrowExceptionIfTripped()` insertion; test must fail within 10 seconds (Phase 15 D-12 pattern).

Test name: `testFillKeyedPass1RespectsCircuitBreaker` (CONTEXT.md D-09). Place alphabetically in `SampleByFillTest` — after `testFillKeyedIndependent*` and before `testFillKeyedRespectsCircuitBreaker` (the Phase 15 predecessor).

Nano mirror (per Phase 15 D-02 symmetric-landing pattern, applied by CONTEXT.md canonical-refs): recommend **skipping** nano mirror for pass-1 CB — the CB mechanism is unit-agnostic; the Phase 15 micro-only `testFillKeyedRespectsCircuitBreaker` did not land a nano mirror either. Confirmed by grepping `SampleByNanoTimestampTest.java` — no `testFillKeyedRespectsCircuitBreaker` nano equivalent exists.

### D-11 (M1 pre-1970 new row) — verified MEDIUM confidence

Plan-file verbatim row (from `let-s-discuss-issues-one-gentle-elephant.md` §M1):

| Query shape | Master | This PR |
|---|---|---|
| SAMPLE BY FILL on a table with pre-1970 timestamps | Cursor path rejected with `cannot FILL for the timestamps before 1970` | Works correctly — `timestamp_floor_utc` handles negative timestamps natively |

Net-new user-visible behaviour vs master (D-01 rule (a)). Add this row to "What's covered" block 2. No code or test work.

**Anchoring for the PR-body-edit planner:** the row belongs in block 2 of "What's covered" (the second markdown table). Block 2 starts with "`Non-keyed `FILL(NULL)` / `FILL(<const>)`" and includes all rows of the form `Master: Cursor path ... | This PR: **Async Group By + `SampleByFillRecordCursor`**`. Insert position: recommend grouping the new M1 row near the existing `ALIGN TO CALENDAR WITH OFFSET + FILL without TO` row since both address fast-path behaviour for corner-case time values.

### D-12 / D-13 (M2 pushdown test + comment) — verified HIGH confidence

**M2 test (D-12):** new test `testFillNullPushdownEliminatesFilteredKeyFills` in `SampleByFillTest.java`.

- Call signature: `assertQueryNoLeakCheck(expected, sql, timestampCol, false, false)` per Phase 14 D-15.
- EXPLAIN plan assertion: `assertPlanNoLeakCheck`. Five existing call sites in `SampleByFillTest` at `:42, :2152, :2242, :3283, :3416` — all assert `Async Group By` plans with a `filter:` slot. The shape to assert for M2 is `filter: s='s2'` (or equivalent) inside the inner Async Group By, not at the outer Sample By Fill factory. Use existing `testFillSubDayTimezoneFromOffsetPlan` (at :3408) as template for the multi-line `"""..."""` plan shape.
- Data shape: two-key table with overlapping buckets — rows for key `s1` spanning buckets 1-3, rows for key `s2` only in bucket 2. Filter `WHERE s = 's2'` + SAMPLE BY 1m FILL(NULL). Assert output contains only bucket 2 for `s2`, no leading/trailing null-fills for `s2`.

Expected plan skeleton:
```
Sample By Fill
  ...
    Sort
      ...
        Async Group By workers: 1
          keys: [ts, s]
          ...
          filter: s='s2'
            ...
```

**M2 comments (D-13):** `testSampleByAlignToCalendarFillNullWithKey1/2` live in **`SampleByTest.java`** (at `:4935, :4963`) AND **`SampleByNanoTimestampTest.java`** (at `:4019, :4044`) — **not** `SampleByFillTest.java` as one might assume from naming. CONTEXT.md D-13 said "above each of [the two tests]" without pinning the file; research-correction: land comments at **all four call sites** (two in `SampleByTest`, two in `SampleByNanoTimestampTest`). CONTEXT.md "canonical refs" correctly listed the nano mirror as separate landing surface; the planner should count 4 comment insertions, not 2.

Comment form (D-13 verbatim):
```
// Expected output differs from master's cursor path: predicate pushdown past SAMPLE BY FILL
// now eliminates filter-matched keys from the inner cartesian. See "Predicate pushdown past SAMPLE BY"
// Trade-off in PR #6946.
```

### D-14 (m1 slot-null fix) — verified HIGH confidence

Target: `SqlCodeGenerator.java:3654`. Current code:

```java
3654                                Misc.free(fillValues.getQuick(fillIdx));
3655                                fillValues.setQuick(fillIdx, TimestampConstant.newInstance(parsed, targetColType));
```

Fix: insert `fillValues.setQuick(fillIdx, null);` between :3654 and :3655, or apply the inline `Misc.free` + `setQuick(null)` pattern used at :3663 (`fillValues.setQuick(fillIdx, null); // transfer ownership`). Plan-file verbatim shows the setQuick-null-then-setQuick-replacement pattern (via `setQuick(fillIdx, Misc.free(fillValues.getQuick(fillIdx)))` — but `Misc.free()` returns void, so CONTEXT.md's verbatim code sample would not compile as written).

**Research correction to CONTEXT.md D-14 code sample:** plan-file code `fillValues.setQuick(fillIdx, Misc.free(fillValues.getQuick(fillIdx)));` uses `Misc.free` in expression position, which only works if `Misc.free` returns something. Verified via grep: `Misc.free` in QuestDB returns the instance it freed (`public static <T> T free(T object)`), so the pattern does compile and matches the ownership-transfer idiom at :3663 (`fillValues.setQuick(fillIdx, null); // transfer ownership` which is the SETTER-AFTER form rather than setter-inline). Both forms are valid; planner should pick one for consistency. **Recommendation:** use the setter-inline form `fillValues.setQuick(fillIdx, Misc.free(fillValues.getQuick(fillIdx)));` before the replacement `setQuick(fillIdx, TimestampConstant.newInstance(...))`. Inline form is one line shorter and matches plan-file verbatim.

### D-15 (m2 field ordering) — verified HIGH confidence

Target: `SampleByFillCursor` inner class in `SampleByFillRecordCursorFactory.java`. Two private-instance field blocks (final first, mutable-state second) need to merge into one alphabetical block. Cosmetic; no behavioural change.

Planner action: read the class's field declarations, sort alphabetically in one block, no banner comments between kinds (CLAUDE.md rule).

### D-16 (m3 IntList/BitSet refactor + benchmark) — Claude's Discretion, planner-scoped

**Benchmark methodology recommendation:**

QuestDB's benchmark directory `benchmarks/src/main/java/org/questdb/` contains `GroupByLongHashSetBenchmark`, `GroupByLongLongHashMapBenchmark`, `ArrayFillBenchmark`, `SampleByIntervalIteratorBenchmark` — all JMH-based, using `@Benchmark` + `@BenchmarkMode(Mode.AverageTime)` + a prepared state. There is **no existing `SampleByFill*` benchmark**, so this is net-new benchmark territory.

**Recommendation:** JMH harness sitting next to `SampleByIntervalIteratorBenchmark` — new class `SampleByFillKeyedResetBenchmark`. Measure the per-bucket reset path (`Arrays.fill(boolean[], false)` vs `BitSet.clear()` vs `DirectIntList.zero()`). Benchmark inputs: `unique_keys` ∈ {10, 100, 1000, 10_000}. Threshold: **>= 5% wall-clock regression on `unique_keys=1000` (median keyed-fill workload) at `@Warmup(iterations = 3)`, `@Measurement(iterations = 5)`**. Below 5% → land the refactor; at or above → fall back to the primitive array for the regressing type only (independent decision for `int[]` and `boolean[]`).

**Alternative (lower-ambition):** `SampleByFillRecordCursorFactory` field types can be measured end-to-end via a micro-bench SQL query in a JUnit harness — compile the same keyed FILL query on both impl variants, time N iterations per variant, assert ratio. Less precise than JMH but no new benchmarks-module surface. Either approach satisfies D-16's "benchmark gate"; JMH is the QuestDB-standard answer.

**Planner scope:** commit the refactor + benchmark together. If benchmark signals regression, revert the regressing type only (not both); keep the benchmark in the repo either way so future reverts can be re-tested.

### D-17 (m5 rationale comment) — verified HIGH confidence

Target: `SampleByFillRecordCursorFactory.java:425` (`assert value != null`) vs `:486` (`CairoException.critical("sample by fill: data row timestamp ... precedes next bucket")`). Verbatim comment per plan file (CONTEXT.md `canonical_refs` — plan file §m5):

```
// Pass 2 iterates the same sorted cursor as pass 1, so every key must already be in
// the map. A null hit implies a bug in SortedRecordCursor, OrderedMap, or RecordSink
// — internal corruption of a direct dependency, which matches CLAUDE.md's assert
// pattern. Line 486 uses an explicit throw because it guards cross-component grid
// drift (sampler vs timestamp_floor_utc), which was empirically triggered during
// this PR's development and must fail visibly regardless of -ea.
```

Comment only — no code change.

### D-18 (m6 getLong256 contract comment) — verified HIGH confidence

Target: `SampleByFillRecordCursorFactory.java:1085-1089`. Rewrite the null-path comment to reference `Record.getLong256(int, CharSink)` contract (not `NullMemoryCMR`). Key points per plan-file §m6:

- Null Long256 appends nothing to the sink.
- Caller owns delimiters on both sides.
- Empty segment reads as empty text value.
- Do NOT call `sink.clear()` — would erase row-prefix content from `CursorPrinter`.

Comment only.

### D-19 (m7 fillOffset emission in toSink0) — verified HIGH confidence

Target: `QueryModel.java` around `:2324`. Existing fillFrom/fillTo block at `:2315-2324`:

```java
if (fillFrom != null || fillTo != null) {
    if (fillFrom != null) {
        sink.putAscii(" from ");
        sink.put(fillFrom);
    }
    if (fillTo != null) {
        sink.putAscii(" to ");
        sink.put(fillTo);
    }
}
```

Insertion immediately after the closing brace (line 2324). Plan-file verbatim form:

```java
if (fillOffset != null) {
    sink.putAscii(" offset ");
    sink.put(fillOffset);
}
```

Use `putAscii` (not `put`) to stay consistent with surrounding idiom. Symmetric with fillFrom/fillTo.

### D-20 (m4 FillRecord dispatch property test) — Claude's Discretion, planner-scoped

**FillRecord typed-getter inventory (verified from `SampleByFillRecordCursorFactory.java:722-1240`):**

30 typed getters enumerated — all follow the same **4-branch dispatch** pattern:

```java
if (!isGapFilling) return baseRecord.getXxx(col);           // pass-through
int mode = fillMode(col);
if (mode == FILL_KEY) return keysMapRecord.getXxx(outputColToKeyPos[col]);      // (1) FILL_KEY
if (mode >= 0 && outputColToKeyPos[mode] >= 0)
    return keysMapRecord.getXxx(outputColToKeyPos[mode]);                        // (2) cross-col-PREV-to-KEY
if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev())
    return prevRecord.getXxx(mode >= 0 ? mode : col);                             // (3) FILL_PREV_SELF + cross-col-PREV-to-agg
if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getXxx(null);     // (4) FILL_CONSTANT
return NULL_SENTINEL;                                                             // default null
```

Full list of typed getters (branches 1-4 all present):
- `getBin`, `getBinLen`, `getBool`, `getByte`, `getChar`
- `getDecimal8`, `getDecimal16`, `getDecimal32`, `getDecimal64`, `getDecimal128(sink)`, `getDecimal256(sink)`
- `getDouble`, `getFloat`
- `getGeoByte`, `getGeoShort`, `getGeoInt`, `getGeoLong`
- `getIPv4`, `getInt`, `getInterval`
- `getLong`, `getLong128Hi`, `getLong128Lo`, `getLong256A`, `getLong256B`
- `getShort`
- `getStrA`, `getStrB`, `getStrLen`
- `getSymA`, `getSymB`
- `getTimestamp` (with `col == timestampIndex` special case at :1199)
- `getVarcharA`, `getVarcharB`, `getVarcharSize`

**Not participating (plumbing overrides):** `getRecord`, `getRowId`, `getUpdateRowId`, `getSymbolTable`, `getArray` (check inclusion), `getGeoByte/Short/Int/Long` null sentinels need verification.

**Test harness recommendation (new file):** `FillRecordDispatchTest.java` in `core/src/test/java/io/questdb/test/griffin/engine/groupby/`. Rationale:
- 30 getters × 4 dispatch branches = 120 assertions — too large to inline alphabetically in `SampleByFillTest` without cluttering a 3500-line file.
- The test surface is orthogonal to SQL-level FILL tests (synthetic `FillRecord` scenario, not end-to-end query).
- Keeps discoverability: a reviewer scanning for dispatch-correctness finds one file.

**Fixture shape:** for each type tag `T` supported by FillRecord:
1. Create synthetic `keysMapRecord` (implements `Record`) returning a known value for `T` at a key-column position.
2. Create synthetic `prevRecord` returning a known value for `T` at the same position.
3. Create a `FILL_CONSTANT` function returning a known value for `T`.
4. Construct a `FillRecord` (use reflection or package-private visibility) with `fillModes` sequence `[FILL_KEY, cross-col-PREV-to-key, FILL_PREV_SELF, cross-col-PREV-to-agg, FILL_CONSTANT]`.
5. Call `getXxx(col)` for each col, assert the returned value equals the known value for each source.
6. Negative case: set `hasPrevForCurrentGap=false` + `mode=FILL_PREV_SELF` → assert null sentinel for the type.

**Scope cap:** types that participate in dispatch but lack a first(T) or have no column type — enumerate explicitly rather than via reflection. Planner decides whether to include `getArray` and `getGeoByte/Short` (plan-file §m4 said "every supported `ColumnType` tag"; researcher recommends all 30 getters listed above).

### D-21 (m8a DST spring-forward) — verified HIGH confidence

**Existing fall-back fixture** (`testFillWithOffsetAndTimezoneAcrossDst` at `SampleByFillTest:3305`):
- Zone: `Europe/Riga`.
- Transition date: 2021-10-31 (fall-back: 04:00 local → 03:00 local).
- Query: `SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Riga' WITH OFFSET '00:30'`.
- Data: `(1.0, '2021-10-31T00:00:00Z'), (5.0, '2021-10-31T04:00:00Z')`.

**Spring-forward companion:**
- Same zone: `Europe/Riga` (consistency with fall-back fixture).
- Spring-forward in `Europe/Riga` fires last Sunday of March: **2021-03-28 at 03:00 local → 04:00 local** (clocks jump forward; 03:xx doesn't exist).
- Data shape: one pre-transition row + one post-transition row, both in UTC: `(1.0, '2021-03-28T00:00:00Z')` (= 02:00 local EET pre-transition) + `(5.0, '2021-03-28T04:00:00Z')` (= 07:00 local EEST post-transition).

**Assertion shape:**
- Bounded (< 32 rows), monotonic timestamp sequence (matches fall-back fixture's bounded-sanity pattern).
- **Key invariant:** no bucket emitted at the non-existent local hour `03:xx EET` — `timestamp_floor_utc` handles this natively by advancing to the next valid UTC instant. The research question "what does `timestamp_floor_utc` do on the non-existent local hour?" resolves as: the non-existent local hour has **no UTC pre-image**, so no bucket is emitted — the UTC grid is linear, only the wall-clock interpretation skips. The bucket between pre/post is simply the next UTC 1h slot.
- Number of rows: between pre-transition bucket and post-transition bucket, UTC elapsed is 4 hours → 4 buckets → 2 real-data + 2 NULL fills minimum. Bounded by the `rowCount < 32` sanity guard.

Test name: `testFillWithOffsetAndTimezoneAcrossDstSpringForward`. Place alphabetically after `testFillWithOffsetAndTimezoneAcrossDst`.

### D-22 (m8b single-row keyed + FROM/TO) — verified HIGH confidence

**`hasKeyPrev()` invariant** (from `SampleByFillRecordCursorFactory.java:575-580`):

```java
private boolean hasKeyPrev() {
    // Both keyed and non-keyed emission paths cache the PREV availability
    // into hasPrevForCurrentGap right before each fill row is emitted
    // (see emitNextFillRow and the non-keyed gap branch in hasNext).
    return hasPrevForCurrentGap;
}
```

The cache is re-evaluated per fill row. For a single-row key, the sequence of `hasKeyPrev()` values is:
- Leading fill buckets (before the real data): `false` (no prev yet).
- Real data bucket: N/A (data emission doesn't dispatch through FillRecord's fill branches).
- Trailing fill buckets: `true` (prev now populated).

**Exactly one false→true transition** per key, right after the real-data bucket.

**Fixture shape:**
- One-key table: `CREATE TABLE t (key VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY`.
- One data row: `('A', 42.0, '2024-01-01T03:00:00Z')`.
- FROM strictly before the row's bucket (e.g., `'2024-01-01T00:00:00Z'`), TO strictly after (e.g., `'2024-01-01T06:00:00Z'`).
- `SAMPLE BY 1h FROM '...' TO '...' FILL(PREV) ALIGN TO CALENDAR`.

**Expected output:** 6 rows (FROM..TO is 6 hours, 1h stride):

| ts | key | val |
|----|-----|-----|
| 00:00Z | A | null (leading NULL fill — but FILL(PREV) has no prev yet, so null) |
| 01:00Z | A | null |
| 02:00Z | A | null |
| 03:00Z | A | 42.0 |
| 04:00Z | A | 42.0 (PREV) |
| 05:00Z | A | 42.0 (PREV) |

3 leading null rows (no prev available) + real data + 2 trailing PREV rows. Exactly one false→true transition at 03:00→04:00.

Test name: `testFillKeyedSingleRowFromTo`. Alphabetical placement: after `testFillKeyed*` family, before `testFillKeyedPass1RespectsCircuitBreaker`.

**Note on nano mirror:** this test is unit-sensitive (timestamps, PREV semantics); recommend landing nano mirror per Phase 15 D-02. Low cost, high symmetry value.

### D-23 (m8c testFillPrevRejectNoArg tightening) — verified HIGH confidence

**Current test (lines 2466-2491):**

```java
Assert.assertTrue(
        "expected error message to indicate malformed PREV, got: " + e.getMessage(),
        e.getMessage().contains("PREV") || e.getMessage().contains("argument")
                || e.getMessage().contains("empty") || e.getMessage().contains("found")
);
```

**Exact error thrown (verified):** `ExpressionParser.java:379-382` throws on `FILL(PREV())` at parse time:

```java
if (argStackDepth < node.paramCount) {
    throw SqlException.position(node.position).put("too few arguments for '").put(node.token)
            .put("' [found=").put(argStackDepth)
            .put(",expected=").put(node.paramCount).put(']');
}
```

For `FILL(PREV())`: `node.token = "PREV"`, `node.paramCount = 1`, `argStackDepth = 0`. Position points at the `PREV` token's start.

**Exact substring to assert:** `"too few arguments for 'PREV' [found=0,expected=1]"`.

**Position assertion:** `int expectedPos = sql.indexOf("PREV(")`. Use `assertExceptionNoLeakCheck(sql, expectedPos, substring)` pattern (already used elsewhere in `SampleByFillTest` — e.g., `testFillPrevCrossColumnDecimalPrecisionMismatch:1383`). Template:

```java
assertExceptionNoLeakCheck(
    "SELECT ts, avg(v) FROM t SAMPLE BY 1h FILL(PREV()) ALIGN TO CALENDAR",
    sql.indexOf("PREV("),
    "too few arguments for 'PREV' [found=0,expected=1]"
);
```

Master precedent for this assertion style: Phase 13 Plan 06 restored `testSampleFillValueNotEnough` to `assertException` form in both `SampleByTest` and `SampleByNanoTimestampTest` (verified via `STATE.md` entry "Plan 06 deviation (Rule 2): testSampleFillValueNotEnough ... restored from printSql(silent buggy behavior) to assertException"). The pattern is established and safe.

### D-24 (m9 PR title rename) — no research needed

`gh pr edit 6946 --title "feat(sql): add cross-column FILL(PREV) and move SAMPLE BY FILL onto fast path"`. Plan-file verbatim.

### D-25 (plan partitioning) — Claude's Discretion

See `## Recommended Plan Partitioning` below.

### D-26 / D-27 (M-unit TIMESTAMP/INTERVAL unit mismatch) — verified HIGH confidence

**`tagOf` verified** (`ColumnType.java:723-727`):

```java
public static short tagOf(int type) {
    if (type == -1) { return (short) type; }
    return (short) (type & 0xFF);
}
```

**Tag encodings verified** (`ColumnType.java`):
- `TIMESTAMP = 8` (:85)
- `TIMESTAMP_NANO = (1 << 18) | TIMESTAMP` (:150) → high bits set, low-8 still 8 → tagOf = 8
- `INTERVAL = 39` (:136)
- `INTERVAL_TIMESTAMP_MICRO = INTERVAL | (1 << 17)` (:145) → tagOf = 39
- `INTERVAL_TIMESTAMP_NANO = INTERVAL | (1 << 18)` (:146) → tagOf = 39

Confirmed: MICRO and NANO variants of TIMESTAMP (and INTERVAL) collapse to identical tag.

**Current `needsExactTypeMatch` predicate** (`SqlCodeGenerator.java:3605-3611`):

```java
final boolean needsExactTypeMatch =
        ColumnType.isDecimal(targetType)
                || ColumnType.isGeoHash(targetType)
                || targetTag == ColumnType.ARRAY;
final boolean isTypeCompatible = needsExactTypeMatch
        ? targetType == sourceType
        : targetTag == sourceTag;
```

**Fix per D-26:** widen the predicate:

```java
final boolean needsExactTypeMatch =
        ColumnType.isDecimal(targetType)
                || ColumnType.isGeoHash(targetType)
                || targetTag == ColumnType.ARRAY
                || targetTag == ColumnType.TIMESTAMP
                || targetTag == ColumnType.INTERVAL;
```

**Where the fix belongs (codegen vs dispatch):** research-confirmed that `FillRecord.getTimestamp` (lines 1197-1208) and `getInterval` (lines 1012-1022) dispatch via the raw `long` return — no unit conversion machinery at read time. The dispatch branches read `prevRecord.getTimestamp(mode)` or `keysMapRecord.getTimestamp(...)` and return the raw long. A runtime conversion would require knowing source vs target full-type at dispatch time, which FillRecord doesn't have (it sees only `mode` = source col index). **Codegen-time rejection is the correct layer.** The error message ("source type TIMESTAMP_MICRO cannot fill target column of type TIMESTAMP_NANO") comes out of the existing `!isTypeCompatible` throw at :3612-3618 with `ColumnType.nameOf(sourceType)` / `ColumnType.nameOf(targetType)` — no wording change needed.

**Existing tests at risk of regression:** `testFillPrevCrossColumnTimestamp*` or `testFillPrevCrossColumn*Interval*` — searched `SampleByFillTest` for any existing cross-column test deliberately mixing MICRO and NANO:

```
$ grep -n "TIMESTAMP_NANO\|TIMESTAMP_MICRO\|nano.*timestamp.*micro\|timestamp_nano\|INTERVAL_TIMESTAMP_NANO" core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
# (zero hits — verified no existing cross-column TIMESTAMP/INTERVAL unit-mismatch tests)
```

**No existing test regresses under the tightened predicate.** The widening is safe.

**D-27 test `testFillPrevCrossColumnTimestampUnitMismatch`:** model on `testFillPrevCrossColumnDecimalPrecisionMismatch` (`:1367-1386`). Fixture — two table variants (both in same test via nested `assertException` blocks, or two separate tests):

Variant A — TIMESTAMP_MICRO vs TIMESTAMP_NANO:
```sql
CREATE TABLE t (
    ts TIMESTAMP,
    a TIMESTAMP,              -- implicit MICRO
    b TIMESTAMP_NANO
) TIMESTAMP(ts) PARTITION BY DAY;
INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000000Z'::TIMESTAMP_NANO);
SELECT ts, first(a) a, first(b) b FROM t SAMPLE BY 1h FILL(PREV, PREV(a)) ALIGN TO CALENDAR;
-- Expected: SqlException at position of 'a' inside PREV(a), message:
-- "source type TIMESTAMP cannot fill target column of type TIMESTAMP_NANO"
```

Variant B — INTERVAL_TIMESTAMP_MICRO vs INTERVAL_TIMESTAMP_NANO: same structure, with `interval` type variants. The INTERVAL column type syntax is verified via `ColumnType.java:1042` (`nameTypeMap.put("interval", INTERVAL_TIMESTAMP_MICRO)`); INTERVAL_TIMESTAMP_NANO has no direct name-map entry — planner must verify the DDL syntax to create an INTERVAL_TIMESTAMP_NANO column before writing the test. **Possible simplification:** if the INTERVAL_NANO column cannot be declared from SQL (name-map has only one INTERVAL entry), drop Variant B and rely on the TIMESTAMP variant for the tag-level rejection invariant — Variant A covers the same `needsExactTypeMatch` code path that Variant B would.

**Recommendation:** plan one variant (TIMESTAMP) for certain; investigate INTERVAL_NANO DDL as a task spike during planning, land Variant B only if the column is declarable. If not, the code path is still covered by Variant A because the fix is a single predicate widening.

**Test placement:** alphabetical between `testFillPrevCrossColumnGeoHashWidthMismatch` (`:1389`) and `testFillPrevCrossColumnKeyed` (`:1413`). Name: `testFillPrevCrossColumnTimestampUnitMismatch`.

**Paired same-commit landing:** per Phase 15 D-02, codegen fix + rejection test ship together (one commit). Phase 16 D-05 same pattern. Planner should commit D-26 + D-27 as a single commit.

### D-28 (todo retirement) — no research needed

After D-26/D-27 land: `mv .planning/todos/pending/2026-04-22-reject-cross-column-fill-prev-timestamp-unit-mismatch.md .planning/todos/completed/` + add `completed_at: <date>` and `completed_in: 17-...-PLAN.md` fields.

## Open Questions (RESOLVED)

1. **INTERVAL_TIMESTAMP_NANO column declarability** (from D-27 analysis) — can `CREATE TABLE ... (col INTERVAL_TIMESTAMP_NANO)` parse? If not, D-27 ships Variant A only. Low risk either way — the code path coverage is identical. **RESOLVED:** Plan 01 Task 2 — INTERVAL variant lands conditionally on a runtime spike; if the DDL does not parse, Variant A (TIMESTAMP pair) alone satisfies D-27 because the `needsExactTypeMatch` predicate applies identically to both tag families.

2. **m3 benchmark landing order** (from D-16) — land refactor-with-benchmark as single commit, or land refactor first then benchmark in a follow-up? Recommendation: single commit; the benchmark documents the gate criterion and future reverts need it. **RESOLVED:** Plan 02 Task 2 — single commit carrying both the refactor and the new `SampleByFillKeyedResetBenchmark` JMH harness.

3. **m4 test file location** (from D-20) — new `FillRecordDispatchTest.java` or inline in `SampleByFillTest`? Recommendation: new file (rationale in D-20 above). Planner has discretion. **RESOLVED:** Plan 02 Task 3 — new file `core/src/test/java/io/questdb/test/griffin/engine/groupby/FillRecordDispatchTest.java` (Wave 0 dependency recorded in VALIDATION.md).

4. **M2 D-13 comments in nano mirror** — should `testSampleByAlignToCalendarFillNullWithKey1/2` in `SampleByNanoTimestampTest` receive the same comment? Recommendation: yes — research surfaced them as 4 call sites, not 2. CONTEXT.md said "each of" but didn't pin the file count. Planner should confirm with user during discuss-gate or land comments on all 4 by default. **RESOLVED:** Plan 03 Task 1 — comments land on all 4 call sites (2 in `SampleByTest` + 2 in `SampleByNanoTimestampTest`); CONTEXT.md D-13 scope updated implicitly via this RESEARCH note.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | JUnit 4 (existing QuestDB convention) + JMH for m3 benchmark |
| Config file | `core/pom.xml` (maven-surefire-plugin); no dedicated test config |
| Quick run command | `mvn -Dtest=SampleByFillTest test -pl core` |
| Full suite command | `mvn -Dtest=SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,SqlOptimiserTest,SqlParserTest,RecordCursorMemoryUsageTest,FillRecordDispatchTest test -pl core` |

### Phase Requirements → Test Map

| Finding | Behaviour | Test Type | Automated Command | Wave 0? |
|---------|-----------|-----------|-------------------|---------|
| M-new D-08/D-09 | Pass-1 CB honoured during keyed key discovery | unit + regression self-check | `mvn -Dtest=SampleByFillTest#testFillKeyedPass1RespectsCircuitBreaker test -pl core` | ❌ (test file exists; new method) |
| M2 D-12 | Predicate pushed into inner Async Group By `filter:` slot | unit + EXPLAIN plan | `mvn -Dtest=SampleByFillTest#testFillNullPushdownEliminatesFilteredKeyFills test -pl core` | ❌ |
| m4 D-20 | FillRecord dispatch covers 4 branches across 30 getters | property test (synthetic) | `mvn -Dtest=FillRecordDispatchTest test -pl core` | ✅ Wave 0 — new file |
| m8a D-21 | DST spring-forward: no bucket at non-existent local hour | unit | `mvn -Dtest=SampleByFillTest#testFillWithOffsetAndTimezoneAcrossDstSpringForward test -pl core` | ❌ |
| m8b D-22 | Single-row keyed FROM/TO: exactly one hasKeyPrev false→true transition | unit | `mvn -Dtest=SampleByFillTest#testFillKeyedSingleRowFromTo test -pl core` | ❌ |
| m8c D-23 | FILL(PREV()) rejected with exact message + position | unit | `mvn -Dtest=SampleByFillTest#testFillPrevRejectNoArg test -pl core` | ❌ (modifies existing) |
| M-unit D-27 | TIMESTAMP_MICRO↔TIMESTAMP_NANO cross-col PREV rejected | unit | `mvn -Dtest=SampleByFillTest#testFillPrevCrossColumnTimestampUnitMismatch test -pl core` | ❌ |
| m1 D-14 | Slot nulling on TIMESTAMP fill constant path (latent guard) | covered by existing `testTimestampFillNullAndValue` + static review | `mvn -Dtest=SampleByFillTest#testTimestampFillNullAndValue test -pl core` | — |
| m2/m5/m6/m7 D-15/D-17/D-18/D-19 | Cosmetic / comment-only | compile verification only | `mvn -pl core compile` | — |
| m3 D-16 refactor | `IntList`/`BitSet` replacements for primitive arrays | full SampleBy* suite + benchmark gate | `mvn -Dtest=SampleByFillTest,SampleByTest,SampleByNanoTimestampTest test -pl core` + JMH benchmark | ✅ Wave 0 — new `SampleByFillKeyedResetBenchmark` |
| m9 D-24 | PR title updated | `gh pr view 6946 --json title --jq '.title'` | manual shell check | — |
| M1/M3/M3.2 bullet/M3.4 sentences/M3.5 lines/D-11 row | PR body edits | `gh pr view 6946 --json body` + grep | manual shell check | — |

### Sampling Rate
- **Per task commit:** `mvn -Dtest=SampleByFillTest test -pl core` (catches the regression-test-belongs-next-to-fix invariant).
- **Per wave merge:** full suite command above + `mvn -Dtest=SampleByFillKeyedResetBenchmark ...` (if m3 benchmark landed).
- **Phase gate:** full suite green + `/review-pr 6946` re-run shows all items closed.

### Wave 0 Gaps

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/FillRecordDispatchTest.java` — new file for m4 D-20. Scope: 30 typed getters × 4 dispatch branches = 120 assertions with a synthetic FillRecord.
- `benchmarks/src/main/java/org/questdb/SampleByFillKeyedResetBenchmark.java` — new JMH harness for m3 D-16 per-bucket reset comparison.

*(No framework install needed — JUnit 4 and JMH both already in use.)*

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `gh` CLI | PR body/title edits (M1, M3.2, M3.4, M3.5, m9) | ✓ | 2.x (standard) | manual PR edit via GitHub web UI |
| `mvn` | test execution | ✓ | 3.x | — |
| `git` + `origin/master` fetched | master-parity probes (D-02, D-03, D-05, D-07) | ✓ | verified via `git show origin/master:...` | — |
| JMH | m3 D-16 benchmark | ✓ (existing `benchmarks/` module) | matches existing benchmarks | — |

**Missing dependencies:** none.

## Recommended Plan Partitioning

CONTEXT.md D-25 suggests two clusters (A: code + tests; B: PR body + title). Research evidence supports a **four-plan split** for better bisectability and reviewer cognitive load:

### Plan 01 — Safety-critical codegen fixes (M-new + M-unit)
**Commits:** 2 (one per paired code+test landing per Phase 15 D-02).
- **Commit 1:** M-new D-08/D-09 — pass-1 CB poll at :604 + `testFillKeyedPass1RespectsCircuitBreaker`. Self-check: revert and see it fail within 10s.
- **Commit 2:** M-unit D-26/D-27 — widen `needsExactTypeMatch` + `testFillPrevCrossColumnTimestampUnitMismatch`. Single-variant TIMESTAMP test; INTERVAL variant contingent on declarability.

**Rationale:** both are correctness bugs (one cancellation semantics, one silent 1000x drift). Highest-risk items ship first; easy to bisect if anything regresses.

### Plan 02 — Minor code fixes (m1, m2, m3, m5, m6, m7, m4 test)
**Commits:** 2-3.
- **Commit 1:** m1 slot-null + m2 field ordering + m5 rationale comment + m6 Record-contract comment + m7 QueryModel toSink0 fillOffset. All touch `SqlCodeGenerator`, `SampleByFillRecordCursorFactory`, `QueryModel` with one-line-each changes. Single commit keeps them atomic.
- **Commit 2:** m3 IntList/BitSet refactor + new `SampleByFillKeyedResetBenchmark`. Benchmark-gated; if regression, revert only the regressing type.
- **Commit 3:** m4 FillRecord dispatch property test — new `FillRecordDispatchTest.java`. Standalone; doesn't depend on other Plan 02 commits.

**Rationale:** code-cleanup-only commits separated from correctness work. m3 is the only risky one; isolates the benchmark-gated revert decision.

### Plan 03 — Test-only additions (M2 + m8a + m8b + m8c + D-13 comments)
**Commits:** 2.
- **Commit 1:** M2 `testFillNullPushdownEliminatesFilteredKeyFills` + 4 comment insertions in `SampleByTest` + `SampleByNanoTimestampTest` (D-13 across both files). Plus optional nano mirror of M2 test.
- **Commit 2:** m8a DST spring-forward + m8b single-row keyed FROM/TO + m8c testFillPrevRejectNoArg tightening. Optionally nano mirrors of m8a/m8b.

**Rationale:** test-only commits are safe to split per-feature; Commit 1 documents pushdown semantics, Commit 2 closes three distinct test gaps.

### Plan 04 — PR body + title edits (CONTEXT.md D-25 cluster B)
**Commits:** 1.
- M1 new row in "What's covered" block 2.
- M3.2 K x B bullet rewrite (append the "Unrelated to the cancellation..." parenthetical).
- M3.4 two stale sentences rewritten per plan-file verbatim.
- M3.5 drop the two test-count lines (`SampleByFillTest: 110` and `SampleByNanoTimestampTest: 278`).
- m9 PR title rename via `gh pr edit 6946 --title "..."`.

**Rationale:** single commit (body edits are atomic in git terms; `gh pr edit` doesn't create commits — body state is external). Lands **last** so any surviving test-count reference captures post-Plan-01-to-03 state (D-06 drops lines entirely, so order only matters as a safeguard).

**Deviation from CONTEXT.md D-25:** CONTEXT.md clusters A (code + tests) + B (PR body + title). Research recommendation splits A into three plans (01 safety-critical / 02 code minor / 03 test-only) for bisectability. The four-plan split preserves CONTEXT.md's "B lands last" ordering; Plan 04 = CONTEXT.md Cluster B unchanged.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | INTERVAL_TIMESTAMP_NANO can be declared in DDL (for D-27 Variant B) | D-27 | Medium — if undeclarable, D-27 ships one variant. Same code path coverage. Planner verifies. |
| A2 | `Misc.free` returns the object (supports inline `setQuick(i, Misc.free(slot))` pattern) | D-14 | Low — verified via grep of `Misc.free` signature in QuestDB std package. |
| A3 | `SampleByFillKeyedResetBenchmark` fits the existing JMH module structure | D-16 | Low — existing benchmarks follow the same pattern. |
| A4 | Spring-forward in Europe/Riga 2021-03-28 advances clocks 03:00→04:00 local | D-21 | Low — IANA tzdata confirms; same zone as existing fall-back fixture. |
| A5 | The two M3 M3.4 stale sentences are the only currently-wrong sentences in the PR body | D-05 | Low — full-body grep for `sampleByTimezoneName == null` returns two hits, both covered. |

## Sources

### Primary (HIGH confidence — verified in-repo)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (verified lines: 382, 425, 486, 530, 575-580, 603, 722-1240, 1085-1089, 1197-1208)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (verified lines: 3595-3663)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (verified lines: 8012-8345)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/main/java/io/questdb/cairo/ColumnType.java` (verified tagOf :723-727, TIMESTAMP/TIMESTAMP_NANO/INTERVAL tags at :85, :150, :136, :145-146)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/main/java/io/questdb/griffin/model/QueryModel.java` (verified lines: 136-139, 470-473, 2315-2330)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/main/java/io/questdb/griffin/ExpressionParser.java:379-382` (m8c error message)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` (verified test sites: 1367, 2466-2491, 3305-3441)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java:4935, :4963` (M2 D-13 comment targets — contradicts CONTEXT.md's implied location)
- `/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java:4019, :4044` (M2 D-13 comment targets)

### Master-parity (HIGH confidence — verified via `git show origin/master:...`)
- `origin/master:core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java` (lines 171, 217 — CB poll pattern)
- `origin/master:core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursor.java` (lines 183, 229 — CB poll pattern)
- `origin/master:core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillNullRecordCursorFactory.java` (lines 76-80 — timezone+offset constructor args, grounds D-05 verdict)
- `origin/master:core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (lines 8300-8345 — no `setFillOffset`, grounds D-05 verdict)
- `origin/master:core/src/main/java/io/questdb/griffin/model/QueryModel.java` (no `fillOffset` field, grounds D-05 verdict)

### CONTEXT.md and plan file (HIGH confidence — user-authored constraints)
- `.planning/phases/17-.../17-CONTEXT.md` (D-01..D-28)
- `~/.claude/plans/let-s-discuss-issues-one-gentle-elephant.md` (plan-file verbatim wording for M1, M3.2, M3.4, m1, m5, m9)

### Live PR artefact (MEDIUM confidence — fetched once, may drift)
- PR #6946 body via `gh pr view 6946 --json body` (content verified 2026-04-22; Test plan lines, K x B bullet, two stale sentences all located)

## Metadata

**Confidence breakdown:**
- Standard stack / architecture: HIGH — in-repo codebase; all changes land in already-existing files with exact line numbers.
- Master parity: HIGH — `git show origin/master:...` probes confirmed pattern matches.
- Pitfalls: HIGH — D-26 tag collapse verified; D-05 master-support verdict triangulated across three master files.
- PR body: MEDIUM — live artefact; if the body drifts further between research and plan, Plan 04 must re-verify anchors before landing edits.

**Research date:** 2026-04-22
**Valid until:** 2026-05-22 for code-only findings; 2026-04-29 for PR-body anchors (live artefact — re-verify before Plan 04).
