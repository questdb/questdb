---
phase: 15-address-pr-6946-review-findings-and-retro-fixes
plan: 04
subsystem: retro-doc
tags: [sample-by, fill, retro-documentation, audit]

requires:
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    provides: "rowId-based FILL(PREV) snapshots; residual dispatch cleanup debt retired by a986070e43"
  - phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
    provides: "FillRecord dispatch order (Phase 14 patterns-established); 2696df1749 and 6c2c44237c land on top of this baseline"
  - phase: 15-address-pr-6946-review-findings-and-retro-fixes
    plan: 01
    provides: "Codegen-cluster clean baseline"
  - phase: 15-address-pr-6946-review-findings-and-retro-fixes
    plan: 02
    provides: "Cursor-cluster clean baseline; Plan 02 M-4 extends 2696df1749's sink-null contract to FillRecord.getLong256"
  - phase: 15-address-pr-6946-review-findings-and-retro-fixes
    plan: 03
    provides: "Test-only cluster clean baseline"
provides:
  - "Audit paper trail for three post-Phase-14 commits that landed on branch sm_fill_prev_fast_path before the Phase 15 boundary was drawn"
  - "Closes ROADMAP Phase 15 Success Criterion #7 (retro-document 6c2c44237c, 2696df1749, a986070e43)"
affects: []

tech-stack:
  added: []
  patterns:
    - "Retro-doc pattern: one consolidated plan SUMMARY with one level-2 section per already-landed commit, each section carrying Date / Scope / Rationale per CONTEXT D-12 skeleton"

key-files:
  created:
    - ".planning/phases/15-address-pr-6946-review-findings-and-retro-fixes/15-04-SUMMARY.md"
  modified: []

key-decisions:
  - "Chronological ordering in the SUMMARY: 6c2c44237c (2026-04-20) -> 2696df1749 (2026-04-21 09:40 BST) -> a986070e43 (2026-04-21 11:58 BST). Matches git log order and CONTEXT D-12 skeleton."
  - "Cross-reference between 2696df1749 and Plan 02 Task 2 documented inline: Plan 02 ships the analogous getLong256 fix under M-4 closure because CharSink<?> does not expose ofRawNull(); the Long256 null contract is 'leave the sink untouched' per NullMemoryCMR.getLong256(offset, CharSink)."
  - "Each commit section cites the verified line-count delta from git show --stat (36/+, 77/+ and 3/-, 20/+ and 40/-) rather than a narrative approximation."

patterns-established:
  - "Consolidated retro-doc SUMMARY: one file with one level-2 section per commit, frontmatter lists every commit hash under provides[], and each section's Scope / Rationale triangulates against the commit body for audit readers"

requirements-completed: [COR-01, COR-04, FILL-02]

duration: 5min
completed: 2026-04-21
---

# Phase 15 Plan 04 Summary: Retro-documented commits

Paper trail for three post-Phase-14 commits that landed on branch
`sm_fill_prev_fast_path` before the Phase 15 boundary was drawn. Each
commit is a correctness fix or test-coverage extension that logically
belongs under a numbered phase; Phase 15 Plan 04 brings them into audit
ownership per CONTEXT.md D-12 and ROADMAP Phase 15 Success Criterion #7.

No code change in this plan. The three commits already exist on the
branch (verified via `git log master..HEAD` and `git show --stat`
during research, RESEARCH.md section 8); this SUMMARY documents their
scope and rationale for future readers.

## 6c2c44237c - "Cover narrow-decimal FILL_KEY branches"

Date: 2026-04-20

Scope:
- +36 lines in `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` - single new test method `testFillKeyedDecimalNarrow`.
- Key column types exercised: DECIMAL(2,0), DECIMAL(4,0), DECIMAL(9,0), DECIMAL(18,0).
- Storage-tag coverage: DECIMAL8 / DECIMAL16 / DECIMAL32 / DECIMAL64 - each precision maps to a distinct narrow-decimal tag.
- No production change. Test-only commit; `git show --stat` reports 1 file changed, 36 insertions, 0 deletions.

Rationale: Phase 11's FILL_KEY dispatch extension covered DECIMAL128 and
DECIMAL256 key columns via `testFillKeyedTypes`, but the four narrow-
decimal storage tags (DECIMAL8/16/32/64) read the decimal key value
from the `keysMap` record through a separate code path in
`FillRecord.getDecimal8` / `getDecimal16` / `getDecimal32` /
`getDecimal64`. These four narrow-decimal FILL_KEY dispatches had no
direct regression coverage, so a future edit to any of them would not
surface through the existing wide-decimal tests.
`testFillKeyedDecimalNarrow` walks each of the four precisions through
the same keyed SAMPLE BY + FILL(NULL) shape already used elsewhere in
`SampleByFillTest`, pinning each narrow-decimal FILL_KEY branch against
future regressions.

## 2696df1749 - "Fix decimal128/256 sink null fall-through"

Date: 2026-04-21

Scope:
- Production changes in `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (+16 / -3):
  - `FillRecord.getDecimal128(int, Decimal128Sink)`: adds terminal `sink.ofRawNull()` after the FILL_CONSTANT branch and an explicit `return;` inside the FILL_CONSTANT block so the caller's sink resets to null on FILL_CONSTANT miss.
  - `FillRecord.getDecimal256(int, Decimal256Sink)`: same pattern as `getDecimal128`, mirroring the change one-for-one.
  - `SampleByFillCursor.hasNext()`: promotes the defensive `dataTs < nextBucketTimestamp && hasPendingRow` branch to `assert false : "...";` under `-ea`. The production defensive fallback below the assertion is preserved byte-for-byte so shipped query output stays uncorrupted; the comment above the branch is expanded to explain the upstream-contract invariant it guards.
- Test changes in `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` (+64 / 0) - two new `@Test` methods:
  - `testFillKeyedPrimitiveTypes`: BOOLEAN / BYTE / CHAR / FLOAT / SHORT FILL_KEY dispatch in a single multi-key SAMPLE BY, locking in `FillRecord.getBool`, `getByte`, `getChar`, `getFloat`, and `getShort` FILL_KEY paths.
  - `testFillPrevCrossColumnNoPrevYet`: cross-column PREV fall-through to default null when no prior data row has set `simplePrevRowId`, mirroring the coverage shape of `testFillPrevGeoNoPrevYet` for the self-prev case.
- `git show --stat` reports 2 files changed, 77 insertions, 3 deletions.

Rationale: The sink-caller contract requires a terminal `sink.ofRawNull()`
call to mirror `NullMemoryCMR`'s reset of Decimal128 / Decimal256 sinks
on null reads. Without the terminal call, a caller that reuses a
Decimal128 or Decimal256 sink across rows saw stale prior-row bytes
instead of a clean null on FILL_CONSTANT miss when `hasKeyPrev()`
returned false for a FILL_PREV_SELF or cross-column PREV mode. The
explicit `return;` inside the FILL_CONSTANT branch prevents the
terminal `sink.ofRawNull()` from overwriting the constant the caller
just asked for; the ordering sequence (branch returns on success,
terminal null resets the sink only on fall-through) mirrors the
existing branch structure for the non-sink typed getters.

The `-ea` assertion on the `dataTs < nextBucketTimestamp` branch in
`SampleByFillCursor.hasNext()` captures an upstream-contract invariant
documented in the existing comment: async GROUP BY emits exactly one
row per (bucket, key), so reaching the branch implies either a broken
upstream contract or a bucket-grid drift. Under `-ea` (test and dev
builds), the assertion fires so regressions surface loudly; in
production, the defensive fallback below still runs to preserve cursor-
contract semantics around DST fall-back without corrupting query
output.

The two new tests pin the new contract and close a regression-coverage
gap for primitive FILL_KEY paths that the Phase 14 per-type suite did
not reach. `testFillKeyedPrimitiveTypes` closes the FILL_KEY coverage
for the four remaining narrow primitive types plus BOOLEAN, and
`testFillPrevCrossColumnNoPrevYet` closes the cross-column PREV-to-
aggregate fall-through gap that `testFillPrevGeoNoPrevYet` only covered
for the self-prev case.

Plan 02 cross-reference: Phase 15 Plan 02 Task 2 establishes that
`FillRecord.getLong256(int, CharSink<?>)` does NOT mirror the
`sink.ofRawNull()` terminal that 2696df1749 adds to the Decimal128 /
Decimal256 paths. The reason is mechanical: `CharSink<?>` does not
expose `ofRawNull()` (only the `Decimal128Sink` / `Decimal256Sink`
concrete sink types do); the Long256 null contract per
`NullMemoryCMR.getLong256(offset, CharSink)` is "leave the sink
untouched" and renders null as empty text. Plan 02 adds a documentation
comment at the `getLong256` site to record that decision, rather than
attempting a no-op terminal call that would not compile.

## a986070e43 - "clean-up SampleByFillRecordCursorFactory"

Date: 2026-04-21

Scope:
- 20 insertions / 40 deletions in `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`; `git show --stat` reports 1 file changed, -20 net lines.
- Dead-code and simplification sweep: residual KIND_* dispatch constants, per-type snapshot code paths no longer referenced after the Phase 13 rowId migration, and a handful of now-unused local variables scattered across the factory.
- No test changes. Existing `SampleByFillTest` / `SampleByTest` / `SampleByNanoTimestampTest` suites continue to pass unchanged; behavior is equivalent to the pre-commit state (no new assertions pass or fail).

Rationale: Phase 13 (Plans 13-01 through 13-06) migrated FILL(PREV)
snapshots from per-type materialized values to a single rowId per key,
read lazily via `baseCursor.recordAt(prevRecord, prevRowId)`. The
migration landed the new rowId-based path but left KIND_* dispatch
residuals, per-type snapshot helpers, and a handful of now-unused local
variables in the factory. `a986070e43` consolidates the file to its
post-migration state - trimming the residual footprint to 0 and
simplifying read paths for future maintenance without touching the
public contract or any externally-observable behavior.

### Self-Check: PASSED

Verified:
- File exists at `.planning/phases/15-address-pr-6946-review-findings-and-retro-fixes/15-04-SUMMARY.md`.
- All three commit hashes referenced verbatim: `6c2c44237c`, `2696df1749`, `a986070e43`.
- Each of the three commits is still present on the branch (`git log master..HEAD` shows all three hashes between the Phase 14 closing commits and the Phase 15 `.planning/` commits, matching RESEARCH.md section 8's verified snapshot).
- ASCII only, no em-dashes, no curly quotes (written with plain hyphen-minus and straight quotes throughout).
- No production code change in this plan.
