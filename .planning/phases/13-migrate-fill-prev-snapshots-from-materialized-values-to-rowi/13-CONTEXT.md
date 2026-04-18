# Phase 13 Context: Migrate FILL(PREV) snapshots to rowId-based replay

**Gathered:** 2026-04-17
**Status:** Ready for planning
**Source branches referenced:** `sm_fill_prev_fast_all_types`, `sm_fill_prev_keysmap_driven`

## Domain

Replace per-type materialization of FILL(PREV) snapshots in the fast-path fill
cursor with a single chain rowId per key, replayed on emit via
`baseCursor.recordAt(prevRecord, prevRowId)`. Unlock all currently-unsupported
PREV types (UUID, LONG128, LONG256, DECIMAL128, DECIMAL256, STRING, VARCHAR,
SYMBOL, BINARY, array, INTERVAL) so retro-fallback machinery becomes dead
code and can be deleted. Close Phase 12 Success Criterion #1 by fully
resolving the SEED-002 cursor defects and restoring `testSampleByFillNeedFix`
to master's 3-row form.

## Driving findings from branch scouting

Scouting `sm_fill_prev_fast_all_types` and `sm_fill_prev_keysmap_driven`
surfaced that the source branch's **research** concluded GO on rowId
(commit `4ebfa3243c` "docs(12): second-pass research — revise NO-GO verdict
to GO"), but the **implementation** on `sm_fill_prev_keysmap_driven`
abandoned rowId mid-effort (commit `fe487c06a9` "Harden fill cursor: value
buffering replaces rowId+recordAt"). The abandonment reason per that
commit: "intermittent `AbstractMemoryCR.addressOf` assertion that fired in
combined-suite runs (rowIds captured from a `SortedRecordCursor`'s chain
were resolving to offsets past `mem.size()` under test-order-dependent state
accumulation)".

Per the research's analysis at `SortedRecordCursor.of()` (lines 96-106):
- When `isOpen == true` (cursor reused), the `else` branch takes no action
  on the chain. Subsequent data is appended on top of stale records.
- With repeated reuse, chain memory reallocates; previously-captured rowIds
  become invalid offsets.
- The research's proposed fix (add `chain.clear()` in the else branch) was
  applied in PoC commit `a437758268`, but the source branch still pivoted
  to value buffering — suggesting a second accumulation or lifetime path
  that `chain.clear()` alone does not address.

Phase 13 treats this as **investigate-then-implement** rather than
"blind port". The investigation output is a required first artifact of
this phase.

## Design intent

**rowId is the required mechanism.** Value buffering is explicitly
ruled out. If the investigation confirms `SortedRecordCursor` is structurally
unsuitable for stable rowId offsets, the pivot is to **a different
rowId-capable vehicle in the QuestDB codebase** — not to per-type value
buffering.

The cursor-storage rewrite must end in a single uniform scheme where
`baseCursor.recordAt(prevRecord, savedRowId)` replays previous aggregate
rows. `savedRowId` is one `long` per key (keyed fill) or one `long` for
the non-keyed path. All per-type dispatch (`KIND_LONG_BITS`, `KIND_SYMBOL`,
`KIND_LONG128`, `KIND_DECIMAL128`, `KIND_LONG256`, `KIND_DECIMAL256`,
`KIND_STRING`, `KIND_VARCHAR`, `KIND_BIN`, `KIND_ARRAY`) is removed.

## Locked approach decisions

### D-01: rowId is the required mechanism

Chosen: rowId + `recordAt()` replay, with one `prevRowId` slot per key in
the keysMap `MapValue` (keyed path) or one `simplePrevRowId` field
(non-keyed path). Every type-aware slot, per-type allocator, and
flyweight sink from the source-branch value-buffering approach is
deleted.

**Not chosen:** value buffering. Per the user, rowId is a principled
architectural choice; duplicating aggregate values per bucket is not.

### D-02: Investigate fe487c06a9 first, then implement

Before writing any rowId code:

1. Read `fe487c06a9` in full (both commit message and diff) on
   `sm_fill_prev_keysmap_driven`.
2. Reproduce the combined-suite failure (or confirm it no longer
   reproduces) on our current branch state with `chain.clear()` applied.
3. If the failure reproduces after `chain.clear()`: investigate the
   residual mechanism. Possibilities per the research:
   - Second chain-accumulation path — chain grows DURING pass 1 while
     rowIds are being captured, reallocation mid-pass invalidates
     already-captured offsets.
   - Factory-reuse path that skips `of()` (e.g., `toTop()` direct calls
     in a pool pathway) — `chain.clear()` in `of()` doesn't fire.
   - `MapValue`-held rowId outliving its chain slot.
4. If root cause is fixable inside `SortedRecordCursor`, fix it.
5. If root cause is structural to `SortedRecordCursor`, survey the
   codebase for **other sort/wrap mechanisms** that preserve rowIds
   across iterations. Candidates to search for:
   - `GroupByRecordCursor` + any existing pre-sort wrapper
   - `AsyncGroupByRecordCursor` with serial collector
   - Any `SortedRecordCursorFactory` variant with independent chain
     lifecycle
   - Other downstream order-preserving factories already in use for
     ASOF / LATERAL / similar SAMPLE BY adjacent paths
   Pick whichever candidate provides stable chain-offset rowIds
   under combined-suite state. Use that as the vehicle.

**Fallback is never value buffering.** If no rowId-capable vehicle
exists, the phase surfaces an architectural decision for the milestone
level, not an in-phase pivot.

### D-03: chain.clear() fix ships as the first commit of phase 13

`SortedRecordCursor.of()` currently only calls `chain.reopen()` in the
`!isOpen` branch. Add `chain.clear()` in the `else` branch (or equivalent
guard in the single-branch form). This is a one-line correctness fix
that benefits every consumer, not just fill. Ship it as the first
atomic commit of phase 13 so:
- It's backportable on its own.
- If the rowId rewrite reverts, this fix survives.
- PR reviewers can assess it independently.

Commit title: `fix(sql): clear chain in SortedRecordCursor.of() on reuse`.

### D-04: Retro-fallback deleted if rowId unlocks all types

If the rowId rewrite successfully unlocks every type currently routed
through retro-fallback (UUID, LONG128, LONG256, DECIMAL128, DECIMAL256,
STRING, VARCHAR, SYMBOL, BINARY, array, INTERVAL), delete:
- `FallbackToLegacyException.java` entirely.
- `QueryModel.stashedSampleByNode` field + `IQueryModel` declarations +
  getter/setter + `clear()` reset.
- `SqlOptimiser.rewriteSampleBy` stash write.
- `SqlCodeGenerator.generateFill` codegen-time unsupported-type check.
- The three try/catch blocks at the `generateFill` call sites.

If only a subset of types is unlocked (unlikely given the source
branch's value-buffering approach unlocked all), retro-fallback stays;
revisit during the phase's verification gate.

### D-05: SEED-001 disposition

- **WR-01, WR-02, WR-03:** obsolete under D-04 (the code they patch is
  deleted). Close them implicitly when retro-fallback goes.
- **WR-04** (precise-position chain rejection): in scope for phase 13.
  Upgrade the `fillValuesExprs.getQuick(0).position` generic to a per-
  column `ExpressionNode` lookup (~4 extra lines of bookkeeping in the
  build loop). Error now points at the specific offending `PREV(...)`.
- **Defect 3** (insufficient fill values grammar): in scope for phase
  13. Match master's rejection of `FILL(PREV,PREV,PREV,PREV,0)` for 7
  aggregates at the position of the under-specified fill clause.

Since we are already touching `generateFill` for the rowId rewrite and
retro-fallback deletion, bundling WR-04 + Defect 3 here is cheap.

### D-06: SEED-002 defects fully resolved in-phase

**Full resolution, not partial.** The phase ends with both defects
fixed:

- **Defect 1 (CTE-wrap + outer projection corruption).** Investigate
  whether the corruption is absorbed by the rowId rewrite (cursor-
  level) or lives in factory plumbing above the cursor. If absorbed:
  verify via `testSampleByFillNeedFix` restoration. If in factory
  plumbing: fix in this phase regardless (user directive: "end of this
  phase should be a full solution"). Minimal scope — reproduce the
  defect, bisect snapshot-buffer vs factory-plumbing layer, fix at
  the layer where it lives.
- **Defect 2 (`toTop()` state corruption).** HIGHLY LIKELY absorbed by
  the rowId rewrite (rowId replaces `simplePrev[]` + `hasSimplePrev`
  state). Verify by restoring `testSampleByFillNeedFix` assertion #1
  to master's 3-row form and observing it pass.

**Deliverable:** `testSampleByFillNeedFix` restored to master's
expected 3-row output in both assertions. Closes Phase 12 Success
Criterion #1 (deferred via Option A).

### D-07: Test cherry-pick timing

Sequence:

1. **Commit 1:** chain.clear() fix (D-03).
2. **Artifact:** investigation report on fe487c06a9 (D-02). If
   architectural pivot needed, surface to user before Commit 2.
3. **Commit 2:** rowId rewrite in `SampleByFillRecordCursorFactory` +
   codegen adjustments.
4. **Commit 3:** cherry-pick the 13 per-type tests from commit
   `f43a3d7057` per `13-VALIDATION-INPUTS.md`. All must pass on the
   fast path (plan output shows `Sample By Fill`, not `Sample By`).
5. **Commit 4:** retro-fallback cleanup — delete machinery per D-04.
6. **Commit 5:** SEED-002 defect resolution — restore
   `testSampleByFillNeedFix` + any factory-plumbing fixes for Defect 1.
7. **Commit 6:** SEED-001 grammar items — WR-04 precise chain-rejection
   position + Defect 3 insufficient fill values grammar.

This progression ensures each commit is verifiable independently
(bisectable), and red tests never sit in intermediate states.

### D-08: ALIGN TO FIRST OBSERVATION out of scope

SEED-003 remains the vehicle for FO extension. Do not bundle here.
Phase 13's surface stays rowId migration + retro-fallback cleanup +
SEED-001 grammar items + SEED-002 defect resolution.

## Non-goals

- Any form of value buffering (D-01).
- Extending the fast path to ALIGN TO FIRST OBSERVATION (D-08;
  SEED-003).
- Extending the fast path to two-token stride syntax.
- Unrelated `SortedRecordCursor` changes beyond the `chain.clear()` fix.
- Replacing `SortedRecordCursor` with a different sort vehicle for all
  callers — only the SAMPLE BY fill path is rewired, and only if D-02
  investigation concludes `SortedRecordCursor` is unsuitable.

## Scope boundary summary

| In scope | Out of scope |
|---|---|
| chain.clear() fix in `SortedRecordCursor.of()` | Other `SortedRecordCursor` callers' behavior |
| rowId investigation (read fe487c06a9, reproduce, pick vehicle) | Value buffering as a backup |
| rowId rewrite in `SampleByFillRecordCursorFactory` | Type matrix for non-PREV paths |
| Remove per-type dispatch (KIND_* constants) | FILL(LINEAR) |
| Port 13 per-type tests from `f43a3d7057` | Port 17 FO tests from `e4e3f442a9` |
| Delete retro-fallback machinery (if D-04 applies) | Touching the legacy `SampleByFillPrev*` cursors |
| WR-04 precise chain-rejection position | Any other SEED-001 item (WR-01/02/03 obsolete) |
| Defect 3 insufficient fill values grammar | New grammar rules beyond D-05..D-09 |
| Defects 1 & 2 fully resolved | Partial or punt-to-next-phase resolution |
| Restore `testSampleByFillNeedFix` to 3-row form | Other SampleByTest adjustments unrelated to rowId |
| ALIGN TO FIRST OBSERVATION (SEED-003) |

## Investigation deliverable (D-02)

Before implementation starts, produce `13-INVESTIGATION.md` (or name of
planner's choosing) containing:

1. Full reading of `fe487c06a9` — commit message + diff.
2. Live reproduction attempt: apply `chain.clear()` fix locally, run
   the combined-suite shape that failed on the source branch
   (`SampleByFillTest` + `SampleByTest` back to back, or the specific
   ordering cited in the source branch's notes).
3. Verdict:
   - **(a) No residual issue** — `chain.clear()` is sufficient, rowId
     via `SortedRecordCursor` is viable. Proceed with implementation.
   - **(b) Residual issue, root cause identified** — document the
     actual accumulation/lifetime path; propose the additional fix.
     Proceed if fix is low-cost; escalate to user if high-cost.
   - **(c) Residual issue, `SortedRecordCursor` structurally
     unsuitable** — survey the QuestDB codebase for alternative
     sort/wrap mechanisms that preserve rowId stability. Present at
     least two candidates with pros/cons. User picks the vehicle
     before implementation.

## Success criteria

1. `SortedRecordCursor.of()` calls `chain.clear()` on reuse (isOpen=true
   branch). Shipped as the first independent commit of phase 13.
2. `SampleByFillRecordCursorFactory` stores exactly one `prevRowId` per
   key (keyed) or one `simplePrevRowId` (non-keyed). No per-type
   copied-value buffers. No KIND_* dispatch.
3. Gap-fill emit path: `baseCursor.recordAt(prevRecord, savedRowId)`
   followed by typed getters on `prevRecord`. Uniform across every
   supported type.
4. All 13 per-type FILL(PREV) tests from commit `f43a3d7057` land on
   this branch and pass on the fast path — plan output shows
   `Sample By Fill`, not `Sample By`. (13-VALIDATION-INPUTS.md)
5. Retro-fallback machinery deleted (D-04): `FallbackToLegacyException`
   class, `QueryModel.stashedSampleByNode`, stash write in
   `rewriteSampleBy`, codegen unsupported-type check,
   `FallbackToLegacyException` try/catch at the three `generateFill`
   call sites — all removed. OR if the type subset is incomplete,
   retro-fallback stays and the limitation is documented.
6. SEED-001 WR-04 precise chain-rejection position in place: chain
   error points at the offending `PREV(...)`, not at the first fill
   expression. `testFillPrevRejectMutualChain` / `*ThreeHopChain`
   assertions updated to reflect the precise position.
7. SEED-001 Defect 3 insufficient fill values grammar: `FILL(PREV,
   PREV, ..., 0)` with fewer expressions than aggregate columns raises
   a positioned SqlException (matching master's behavior). New
   regression test `testFillInsufficientFillValues`.
8. SEED-002 Defect 1 resolved. CTE-wrap + outer projection corruption
   either absorbed by the rowId rewrite (no additional fix) or fixed
   at the factory-plumbing layer in this phase.
9. SEED-002 Defect 2 resolved. `toTop()` state corruption absorbed by
   the rowId rewrite.
10. `testSampleByFillNeedFix` restored to master's 3-row expected
    output in both assertion #1 and #2. Closes Phase 12 Success
    Criterion #1.
11. Full suite green: `mvn -pl core -Dtest='SampleByFillTest,
    SampleByTest, SampleByNanoTimestampTest, ExplainPlanTest,
    RecordCursorMemoryUsageTest' test` exits 0.
12. Combined-suite run of `SampleByFillTest + SampleByTest` in either
    order exits 0 — proves the `chain.clear()` fix and any further
    rowId-lifetime fixes are sufficient in the real test environment.

## Canonical references

Downstream agents MUST read these before planning or implementing.

### Source-branch commits (authoritative on mechanism tradeoffs)

- **Research GO verdict:** `sm_fill_prev_fast_all_types` commit
  `4ebfa3243c` — "docs(12): second-pass research — revise NO-GO verdict
  to GO". States `chain.clear()` is the one-line fix and rowId via
  `SortedRecordCursor` is viable. Contradicts the implementation
  outcome — treat as a prior analysis, not a binding conclusion.
- **Value-buffering extension:** `sm_fill_prev_fast_all_types` commit
  `f43a3d7057` — "Extend fill-cursor fast path to all column types for
  FILL(PREV)". Source of the 13 per-type tests we cherry-pick. Value-
  buffering production code is NOT borrowed.
- **rowId PoC:** `sm_fill_prev_keysmap_driven` commit `a437758268` —
  "PoC: inject hidden min(rowid) aggregate as sort key". First
  implementation of rowId; reference for debugging.
- **rowId revert:** `sm_fill_prev_keysmap_driven` commit `fe487c06a9` —
  "Harden fill cursor: value buffering replaces rowId+recordAt".
  Documents the failure mode to investigate. MUST read in full
  during D-02 investigation.
- **keysmap_driven tip:** `sm_fill_prev_keysmap_driven` commit
  `1424bd5216` — final state. Reference only; we're not porting it.

### External design docs

- `/Users/sminaev/projects/questdb/plans/fill-fast-path-overview.md` §9
  — unsupported types fall back to legacy (superseded by rowId if
  D-04 applies).
- `/Users/sminaev/projects/questdb/plans/fill-fast-path-design-review_v2.md`
  §3, §8 — two-layer type defense framing (retro-fallback's design
  rationale; becomes moot if D-04 fires).

### Prior phase context

- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-CONTEXT.md`
  — D-01 retro-fallback mechanism; D-05..D-09 grammar rules we inherit.
- `.planning/phases/11-hardening-review-findings-fixes-and-missing-test-coverage/11-CONTEXT.md`
  — FILL_KEY dispatch for 128/256-bit types; Null sentinels.
- `.planning/phases/04-cross-column-prev/04-CONTEXT.md` — cross-col PREV
  semantics; deferred PREV(expression) and type coercion.

### Source code anchors

- `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursor.java`
  §96-106 — `of()` method; chain.clear() goes in the else branch.
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
  — rowId rewrite target. `SampleByFillCursor` inner class holds the
  iteration state.
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`
  §3392-3398 — dead code left; §3501-3512 equivalent — retro-fallback
  check location.
- `core/src/main/java/io/questdb/griffin/model/QueryModel.java`
  — `stashedSampleByNode` field to delete under D-04.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java:5943`
  — `testSampleByFillNeedFix` in 6-row form; restore to master's 3-row.

### Phase 13 artifacts already planted

- `.planning/phases/13-.../13-VALIDATION-INPUTS.md` — 13 per-type tests
  to cherry-pick from `f43a3d7057`.
- `.planning/seeds/SEED-001-phase12-followups-plumbing-grammar.md` —
  disposition per D-05.
- `.planning/seeds/SEED-002-phase12-cursor-defects-rowid-dependent.md` —
  disposition per D-06.
- `.planning/seeds/SEED-003-align-to-first-observation-fast-path.md` —
  out of scope (D-08).

## Files expected to change

### Phase 13 commit sequence (D-07)

**Commit 1 — chain.clear() fix:**
- `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursor.java`

**Commit 2 — rowId rewrite (after investigation):**
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — map
  value type sizing, remove per-type dispatch
- Possibly new file for alternative sort vehicle wrapper if D-02 picks
  that route

**Commit 3 — cherry-pick per-type tests:**
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`

**Commit 4 — retro-fallback deletion (if D-04 fires):**
- `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java`
  (delete)
- `core/src/main/java/io/questdb/griffin/model/QueryModel.java` (delete
  stashedSampleByNode field + accessors)
- `core/src/main/java/io/questdb/griffin/model/IQueryModel.java` (delete
  declarations)
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (delete stash
  write in rewriteSampleBy)
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (delete
  codegen check + three try/catch blocks; simplify gate)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`
  (delete testFillPrevSymbolLegacyFallback,
  testFillPrevCrossColumnUnsupportedFallback, 5 retro-fallback tests
  from phase 12)

**Commit 5 — SEED-002 defect resolution:**
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java`
  — `testSampleByFillNeedFix` restored to 3-row
- Possibly additional cursor/factory fixes for Defect 1 if not absorbed
  by rowId

**Commit 6 — SEED-001 grammar items:**
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — WR-04
  precise chain-rejection position; Defect 3 insufficient-fill grammar
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`
  — `testFillInsufficientFillValues` and updated chain-rejection tests

## Implementation decisions summary

- **D-01:** rowId is the required mechanism. No value buffering.
- **D-02:** Investigate fe487c06a9 before implementing; fallback is a
  different rowId vehicle, never value buffering.
- **D-03:** chain.clear() as independent first commit.
- **D-04:** Retro-fallback deleted if rowId unlocks all types;
  WR-01..WR-03 obsolete, WR-04 + Defect 3 remain in scope.
- **D-05:** WR-04 (precise chain-rejection position) + Defect 3
  (insufficient fill values grammar) ship in phase 13.
- **D-06:** SEED-002 Defects 1 & 2 fully resolved in-phase; no punt.
  `testSampleByFillNeedFix` restored to master's 3-row form.
- **D-07:** Test cherry-pick after rowId rewrite, before retro-fallback
  cleanup. Six commits total in the sequence.
- **D-08:** ALIGN TO FIRST OBSERVATION out of scope; SEED-003 handles
  later.

### Claude's discretion

- Commit 2's structure — single rowId rewrite commit vs. split into
  "add rowId scaffolding" + "remove per-type dispatch" — planner's call
  based on mechanical size.
- Exact wording of SqlException messages for WR-04 precise position
  and Defect 3 insufficient-fill grammar — use positioned
  `SqlException.$(pos, msg).put(...)` style; specific wording at
  implementation time.
- Whether to use `SortedRecordCursor`'s own chain or an alternative
  vehicle — decided by D-02 investigation output.

## Deferred ideas

- Value buffering as an alternative to rowId — explicitly rejected by
  user; do not revisit in this phase.
- ALIGN TO FIRST OBSERVATION — SEED-003 (trigger: scope expansion or
  user report).
- PREV numeric widening (INT→LONG, FLOAT→DOUBLE) — deferred from
  Phase 12 D-09.
- FILL(LINEAR) on fast path — per project roadmap.
- Old cursor path (`SampleByFillPrev*RecordCursor`) removal — separate
  phase once FO + LINEAR land.
- Two-token stride syntax on fast path — per project roadmap.

---

*Phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi*
*Context gathered: 2026-04-17*
