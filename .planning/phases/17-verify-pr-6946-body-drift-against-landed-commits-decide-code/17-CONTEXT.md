# Phase 17: Address `/review-pr 6946` follow-ups - Context

**Gathered:** 2026-04-22
**Status:** Ready for planning
**Source:** `~/.claude/plans/let-s-discuss-issues-one-gentle-elephant.md` (canonical scope document; must be re-read by downstream agents)

<domain>
## Phase Boundary

Close all verified findings from the second `/review-pr 6946` follow-up pass: 1 new-behavior Moderate (M1), 1 semantic-change Moderate (M2), 1 drift cluster (M3 — four landed commits across `9df205bac5`, `c1deb9b14d`, `82865efbc0`, `289d43090a` plus test-count drift M3.5), 9 Minor findings (m1-m9), plus 1 sibling regression surfaced during discuss-phase (M-new: pass-1 circuit-breaker gap at `SampleByFillRecordCursorFactory.java:603`).

Phase 17 applies a narrow principle to PR body edits (D-01 below): a body row is warranted only when the PR introduces new user-visible behavior vs master, new grammar or rejections, or a currently-false statement needs correcting. Mid-term regressions introduced and fixed during PR development get code and tests, not body rows - they are just "the PR works correctly".

**In scope:**
- M1 pre-1970 FILL guard: one new row in "What's covered" block 2 (new behavior vs master - cursor path rejected).
- M2 predicate-pushdown: new dedicated test + EXPLAIN assertion + one-line comments on the two silently-updated existing tests (`testSampleByAlignToCalendarFillNullWithKey1/2`).
- M3.1 `9df205bac5` (TIMESTAMP unit drift + non-TIMESTAMP alias guard): no body edit, no new test. Regression recovery.
- M3.2 `c1deb9b14d` (cancellation CB regression): no body row. Only the "K x B circuit-breaker" Future-work bullet gets rewritten to disambiguate cancellation CB (landed) from memory-budget CB (still future). Currently misleading wording, not regression documentation.
- M3.3 `82865efbc0` (FUNCTION/OPERATION classifier): no body edit. Regression recovery.
- M3.4 `289d43090a` (TZ + FROM + OFFSET fill-grid): two currently-false sentences in the PR body rewritten or deleted. An optional new "What's covered" row depends on master-support verdict (see D-05).
- M3.5: drop the precise test-count lines from the Test plan.
- M-new (surfaced during discuss): code fix at `SampleByFillRecordCursorFactory.java:603` inside `initialize()`'s pass-1 keyed key-discovery loop + dedicated regression test. No body row (regression recovery - master's cursor-path equivalents poll the CB in the same position).
- m1 slot-null fix at `SqlCodeGenerator.java:3647-3659`.
- m2 `SampleByFillCursor` field ordering (single alphabetical block).
- m3 `int[]`/`boolean[]` -> `IntList`/`BitSet` refactor with benchmark gate.
- m4 FillRecord dispatch property test.
- m5 rationale comment at `SampleByFillRecordCursorFactory.java:425`.
- m6 `getLong256(int, CharSink)` null-path comment rewrite.
- m7 `QueryModel.toSink0` fillOffset emission.
- m8a DST spring-forward companion test.
- m8b Single-row keyed + FROM/TO test.
- m8c Tighten `testFillPrevRejectNoArg` (exact-substring + `getPosition()`).
- m9 PR title rename.
- M-unit (surfaced 2026-04-22 via pending todo): cross-column `FILL(PREV(col))` silently copies raw long across TIMESTAMP_MICRO <-> TIMESTAMP_NANO and INTERVAL_TIMESTAMP_MICRO <-> INTERVAL_TIMESTAMP_NANO, producing 1000x unit drift. Same review source (`/review-pr 6946`). Fix: extend `needsExactTypeMatch` in `SqlCodeGenerator.generateFill` to include TIMESTAMP and INTERVAL tags; add rejection test. Folded in under D-01 because it is a correctness bug (currently-wrong behavior in the PR), not regression recovery - see D-26.

**Out of scope:**
- Alias-guard test for M3.1 Fix B. Internal hardening without a review-driven justification; skip-fill path provides indirect coverage.
- "What's covered" rows or pitfall notes for mid-term regressions (M3.1, M3.2 cancellation CB site, M3.3). Covered by D-01.
- K x B memory circuit-breaker implementation. Stays in Future work; phase 17 only disambiguates the bullet wording.
- Any change to `testSampleByAlignToCalendarFillNullWithKey1/2` expected output. Phase adds comments only.
- CI "questdb-butler" failure flagged in the review. Separate investigation per plan file Verification section.

</domain>

<decisions>

### D-01 PR body edit principle

- **D-01** - A PR body row, pitfall note, or Implementation-notes bullet lands for finding X only when X represents (a) new user-visible behavior vs master, (b) a new grammar rejection or syntactic contract, or (c) correction of a currently-false statement in the PR body. Mid-term regressions introduced and fixed during PR development do not warrant documentation - the feature works correctly in the final state and noise in the body dilutes real signal. This principle reframes M3.1 / M3.2 cancellation-CB / M3.3 / M-new as "code+tests only" and tightens M3.4 to stale-sentence fixes (+ optional row if researcher verifies master gap). Not introduced by the plan file; adopted during discuss-phase after the reviewer (user) flagged that the plan file was over-documenting regression recovery.

### M3 per-commit verdicts

- **D-02** - M3.1 (`9df205bac5`, TIMESTAMP unit drift + non-TIMESTAMP alias guard). Pure mid-term regression recovery on both sub-fixes. Fix A restored unit-correct TIMESTAMP constant handling that master's cursor path already had via `SampleByFillValueRecordCursorFactory.createPlaceHolderFunction:144-173`. Fix B hardened the new fast-path classifier against non-TIMESTAMP alias resolution (`SELECT ts::LONG AS ts` shadowing); master's cursor path never hit this codepath at all. Neither sub-fix alters user-visible behavior vs master. No PR body edit. No dedicated alias-guard test (internal hardening without review justification; skip-fill path exercises Fix B indirectly).

- **D-03** - M3.2 (`c1deb9b14d`, cancellation circuit-breaker regression). Pure regression recovery. Master's cursor-path fill factories (`SampleByFillPrevRecordCursor`, `SampleByFillValueRecordCursor`) poll `circuitBreaker.statefulThrowExceptionIfTripped()` in every ingest/emission loop. The new fast-path `SampleByFillCursor` initially had zero polls; `c1deb9b14d` added the two emission-side sites (`hasNext()` entry at :382 + `emitNextFillRow()` outer `while(true)` at :530). No "What's covered" row. The ONLY PR body edit from this commit is a rewrite of the existing "K x B circuit-breaker" Future-work bullet, which currently reads "CB" without qualifying cancellation vs memory-budget and thus misleads readers into thinking cancellation CB support is entirely future work. Rewrite per plan file verbatim:

  > **`K x B` memory circuit-breaker.** A size-estimated short-circuit in `generateFill` that falls back to the cursor path when `unique_keys x buckets` exceeds a configurable threshold, capping the `SortedRecordCursor` materialisation footprint. (Unrelated to the cancellation `SqlExecutionCircuitBreaker`, which is honoured by the fill cursor.)

- **D-04** - M3.3 (`82865efbc0`, Phase 16 FUNCTION/OPERATION classifier). Pure regression recovery. Master's cursor-path cartesian handled `interval(lo, hi)` / `concat(a, b)` / `a || b` / `cast(x AS STRING)` grouping keys correctly; the PR introduced a fast-path classifier that initially broke them; Phase 16 restored correctness. Generic "Keyed SAMPLE BY now on Async Group By" body row already covers the superset implicitly. No new "What's covered" row, no new Implementation-notes bullet. Phase 16's 5 regression tests + `-ea` assertion already lock the behavior; no additional work in Phase 17.

- **D-05** - M3.4 (`289d43090a`, TZ + FROM + OFFSET fill-grid). The PR body has two currently-false statements that must be corrected regardless of master-support verdict:
  1. The Implementation-notes "Calendar offset alignment" pitfall sentence: *"offset must NOT be double-applied when `timestamp_floor_utc` already accounts for it (timezone case). The optimizer only sets `fillOffset` when `sampleByTimezoneName == null`."* The second sentence is factually wrong after `289d43090a` widened the condition to `sampleByTimezoneName == null || (hasSubDayTimezoneWrap && sampleByFrom != null)`.
  2. The "`ALIGN TO CALENDAR WITH OFFSET` + FILL without `TO`" row in "What's covered" block 2: *"Fixed via `QueryModel.fillOffset` propagation; offset applied only when `sampleByTimezoneName == null`..."* Same staleness.

  Phase 17 rewrites both per the plan file's verbatim wording. Optional new "What's covered" row for sub-day `SAMPLE BY` + `TIME ZONE` + `FROM` + `ALIGN TO CALENDAR WITH OFFSET` + FILL depends on researcher verification: if master's cursor path supported this shape, skip the row (regression recovery); if master did not support it, add the row (net-new behavior vs master). The 95-line regression test block landed in `289d43090a` (micro + nano parity + empty base + plan assertion) covers the behavior; no additional test needed.

- **D-06** - M3.5 (test-count drift). Counts in the plan file (`SampleByFillTest: 110 -> 123`, `SampleByNanoTimestampTest: 278 -> 279`) were already stale when the plan was written. Phase 17 will add more tests (M2, M-new, m4, m8a-c). Drop the two test-count lines from the Test plan entirely. Reviewers can count if they care; nothing else reads the numbers and the lines create constant maintenance drift.

### M-new: pass-1 circuit-breaker gap

- **D-07** - Gap confirmed. `SampleByFillRecordCursorFactory.initialize()`'s keyed pass-1 loop at `~:603`:

  ```
  while (baseCursor.hasNext()) {
      MapKey key = keysMap.withKey();
      keySink.copy(baseRecord, key);
      MapValue value = key.createValue();
      ...
  }
  ```

  has no breaker poll. Master's cursor-path equivalents (`SampleByFillPrevRecordCursor`, `SampleByFillValueRecordCursor`) call `circuitBreaker.statefulThrowExceptionIfTripped()` as the first statement inside identical `while (baseCursor.hasNext())` key-map-building bodies. `c1deb9b14d` fixed two emission-side sites but missed this ingest-side loop. Real regression vs master.

- **D-08** - Fix: add `circuitBreaker.statefulThrowExceptionIfTripped();` as the first statement inside the pass-1 while body at `:603`, verbatim master pattern (no periodic polling, no mask arithmetic - the breaker is already `stateful`, meaning the method is a flag check in the fast case).

- **D-09** - Dedicated regression test `testFillKeyedPass1RespectsCircuitBreaker` in `SampleByFillTest`, mirroring Phase 15's `testFillKeyedRespectsCircuitBreaker` tick-counting `MillisecondClock` CB harness. Data shape guarantees the trip fires during pass-1 (large keyed base cursor, single bucket or no `TO` specified so emission-phase polls never get reached). Regression-coverage self-check: test must fail under reverted production code within 10 seconds (Phase 15 D-12 pattern).

- **D-10** - No PR body row. Regression recovery under D-01. The K x B bullet rewrite in D-03 already covers all three cancellation-CB sites collectively ("honoured by the fill cursor" - true across `hasNext()`, `emitNextFillRow()`, and pass-1 after this phase).

### M1 new behavior vs master

- **D-11** - M1 pre-1970 FILL guard removal. Net-new behavior vs master (cursor path rejected `SAMPLE BY FILL` on tables with pre-1970 timestamps with `cannot FILL for the timestamps before 1970`; fast path handles negative timestamps natively via `timestamp_floor_utc`). Add the plan-file-verbatim row to "What's covered" block 2. No code or test work - the existing sqllogictest coverage pinned the behavior.

### M2 pushdown semantic change

- **D-12** - Dedicated test + EXPLAIN assertion. `testFillNullPushdownEliminatesFilteredKeyFills` in `SampleByFillTest`:
  - Two-key table, data straddling multiple buckets.
  - `SELECT * FROM (... SAMPLE BY 1m FILL(NULL) ALIGN TO CALENDAR) WHERE s = 's2'`.
  - Assert: result contains only buckets where `s2` appears, no leading/trailing NULL-fills for `s2` outside its observed range.
  - EXPLAIN / plan assertion: the filter appears inside the inner GROUP BY's `filter:` slot. Makes the test self-documenting about the mechanism, not just the output.
  - Uses `assertQueryNoLeakCheck(..., false, false)` per Phase 14 D-15 (SAMPLE BY FILL factory contract).

- **D-13** - One-line comment above each of `testSampleByAlignToCalendarFillNullWithKey1` and `testSampleByAlignToCalendarFillNullWithKey2` pointing at the "Predicate pushdown past SAMPLE BY" Trade-off in the PR body. Form:

  > // Expected output differs from master's cursor path: predicate pushdown past SAMPLE BY FILL now eliminates filter-matched keys from the inner cartesian. See "Predicate pushdown past SAMPLE BY" Trade-off in PR #6946.

  Closes the review finding's "tests carry no comment" concern without touching the expected output strings.

### Minor code-fix findings (per plan file)

- **D-14** - m1 slot-null fix at `SqlCodeGenerator.java:3647-3659`. Apply `fillValues.setQuick(fillIdx, null)` after `Misc.free(fillValues.getQuick(fillIdx))` and before `setQuick` installs the replacement, matching the adjacent transfer pattern at `:3663`. Latent double-close guard if `TimestampConstant.newInstance` ever starts throwing.

- **D-15** - m2 field ordering. Merge the two private-instance field blocks in `SampleByFillCursor` (final first, mutable-state second) into one alphabetically-sorted block per CLAUDE.md. No behavioural change.

- **D-16** - m3 `int[] outputColToKeyPos` -> `IntList` initialised with `setAll(columnCount, -1)`, accessed via `getQuick/setQuick`. `boolean[] keyPresent` -> `BitSet` (or `DirectIntList` used as a bitmap if profiling shows the wrapped access to be hot). Benchmark gate: compare the per-bucket reset against the current `Arrays.fill` path; fall back to the primitive array if the bitmap adds measurable cost. Planner scopes the benchmark methodology (JMH vs ad-hoc; threshold for "measurable cost").

- **D-17** - m5 rationale comment at `SampleByFillRecordCursorFactory.java:425`. Add the block comment per plan file verbatim wording, explaining why `assert value != null` here vs the explicit throw at `:486`: pass-2 iterates the same sorted cursor as pass-1, so every key must already be in the map - a null hit is internal corruption (CLAUDE.md assert pattern). Line 486 guards cross-component grid drift (sampler vs `timestamp_floor_utc`), empirically triggered during PR development, so it must fail visibly regardless of `-ea`.

- **D-18** - m6 `getLong256(int, CharSink)` null-path comment at `SampleByFillRecordCursorFactory.java:1085-1089`. Rewrite to reference the `Record.getLong256(int, CharSink)` contract directly rather than pointing at `NullMemoryCMR`. Document: null Long256 appends nothing to the sink; caller owns delimiters on both sides; empty segment reads as empty text value. Do not call `sink.clear()` - it would erase row-prefix content written by `CursorPrinter`. Comment-only change.

- **D-19** - m7 emit `fillOffset` in `QueryModel.toSink0`. Add `if (fillOffset != null) sink.put(" offset ").put(fillOffset);` next to the existing fillFrom/fillTo serialization. Symmetric one-line change.

### Minor test findings (per plan file)

- **D-20** - m4 FillRecord dispatch property test. Enumerate every supported `ColumnType` tag, construct a synthetic `FillRecord` scenario exercising each of the five source modes (FILL_KEY, cross-column-to-key, FILL_PREV_SELF, cross-column-to-prev, FILL_CONSTANT), assert the typed getter for each tag dispatches correctly. Planner picks test file location (new `FillRecordDispatchTest` vs inline in `SampleByFillTest`). Goal: future additions or refactors cannot silently omit a branch in any getter without the test failing.

- **D-21** - m8a DST spring-forward test in `SampleByFillTest`. Companion to existing `testFillWithOffsetAndTimezoneAcrossDst` (fall-back only). Use spring-forward transition at `Europe/Riga` or `America/New_York`. Assert no bucket emitted at the non-existent local hour; real-data bucket on the far side of the transition correctly positioned.

- **D-22** - m8b single-row keyed + FROM/TO in `SampleByFillTest`. One data row for one key; FROM before and TO after the row's bucket. Assert leading NULL fills, the real row, then trailing PREV fills. Exercises the `hasKeyPrev()` false->true transition exactly once.

- **D-23** - m8c tighten `testFillPrevRejectNoArg`. Replace the contains-any-of error-message check with an exact-substring assertion + `SqlException.getPosition()` assertion pinning the offending token's position.

### m9 PR title rename

- **D-24** - Rename PR #6946 title from `feat(sql): add FILL(PREV) support on GROUP BY fast path` to `feat(sql): add cross-column FILL(PREV) and move SAMPLE BY FILL onto fast path` per plan file. Leads with the new grammar feature (cross-column PREV); fast-path move is the secondary clause. Accurately scoped - bare FILL(PREV) already existed on cursor path. Update via `gh pr edit 6946 --title "..."`.

### M-unit: cross-column FILL(PREV) TIMESTAMP/INTERVAL unit mismatch

- **D-26** - Reject cross-column `FILL(PREV(col))` across unit-mismatched TIMESTAMP and INTERVAL pairs. `ColumnType.tagOf(t)` returns `t & 0xFF`, so TIMESTAMP_MICRO (tag 8) and TIMESTAMP_NANO (`(1<<18) | 8`) share a tag; same for INTERVAL_TIMESTAMP_MICRO vs _NANO. The current `needsExactTypeMatch` predicate at `SqlCodeGenerator.java:3605-3611` only forces full-int equality for DECIMAL / GEOHASH / ARRAY, so a MICRO-sourced PREV populates a NANO target via raw long copy through `FillRecord.getTimestamp` / `getInterval`, yielding silent 1000x drift. Net-new bug surfaced by `/review-pr 6946` (pending todo `2026-04-22-reject-cross-column-fill-prev-timestamp-unit-mismatch.md`); NOT regression recovery - D-01 does not suppress documentation here, but there is no PR body row either because the PR currently has no body claim about cross-column PREV TIMESTAMP/INTERVAL - the fix just makes the rejection honest.

  Fix: widen the `needsExactTypeMatch` clause to cover both tags:

  ```
  final boolean needsExactTypeMatch =
          ColumnType.isDecimal(targetType)
                  || ColumnType.isGeoHash(targetType)
                  || targetTag == ColumnType.ARRAY
                  || targetTag == ColumnType.TIMESTAMP
                  || targetTag == ColumnType.INTERVAL;
  ```

  Rejects MICRO <-> NANO mixes at codegen with the existing error message ("source type TIMESTAMP_MICRO cannot fill target column of type TIMESTAMP_NANO"). No runtime conversion - cross-column PREV is rare enough that strict rejection is preferred (per the todo's own follow-up note).

- **D-27** - Regression test `testFillPrevCrossColumnTimestampUnitMismatch` in `SampleByFillTest` modeled on the DECIMAL precision-mismatch test (`testFillPrevCrossColumnDecimalPrecisionMismatch`). Two table variants cover both unit pairs: TIMESTAMP_MICRO column + TIMESTAMP_NANO column; INTERVAL_TIMESTAMP_MICRO column + INTERVAL_TIMESTAMP_NANO column. Assert the SqlException is raised with the expected message and position (pointing at `fillExpr.rhs.position`). Paired same-commit with D-26 per Phase 15 D-02.

- **D-28** - After D-26/D-27 land, move `.planning/todos/pending/2026-04-22-reject-cross-column-fill-prev-timestamp-unit-mismatch.md` to `.planning/todos/completed/` with a `completed_at:` field and a back-reference to the Phase 17 plan ID that shipped the fix.

### Test-hygiene addendum (Plan 17-05, surfaced during Phase 17 execution)

Two pending todos auto-created on 2026-04-22T17:10:07 during Plan 03 execution surfaced three lax-assertion findings in the same theme as D-23 (m8c). All three land in a Plan 17-05 addendum rather than Phase 18; phase is still `executing` per STATE.md until `/review-pr 6946` re-run confirms SC#8.

- **D-29** - Finding 4.6: `SampleByFillTest#testFillKeyedRespectsCircuitBreaker` at `:350` assigns `circuitBreakerConfiguration` to the `protected static` field on `AbstractCairoTest` and never restores it. `staticOverrides.reset()` only fires in `@AfterClass`; `@Before setUp()` does not touch this field. No current bleed (no other test in the class reads it), but latent foot-gun for any future test in the same class constructing `NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration, ...)`. Caveat: 4 other test classes (`ParallelFilterTest:828`, `OrderByTimeoutTest`, `CheckpointTest`, `ParallelGroupByFuzzTest`) have the identical pattern; fixing this one test diverges from project-wide precedent.
  
  Fix: wrap the body in `try { ... } finally { circuitBreakerConfiguration = null; }`. Land the divergence; the SampleByFillTest class is the one with the highest future-test growth potential (Phase 15 + Phase 17 already added CB tests here). Project precedent is a weak argument when a test class is actively accreting related tests.

- **D-30** - Finding 4.7: `SampleByFillTest#testSortedRecordCursorFactoryConstructorThrow` at `:3248` (shifted from :3122 after Plan 03 insertions) catches `Throwable` and accepts one of 6 substrings (`"max pages" || "maxPages" || "Maximum number of pages" || "limit" || "overflow" || "breached"`). The broad matcher accepts any exception that mentions a limit; a single canonical substring suffices because all `LimitOverflowException` sites for `sqlSortKeyMaxPages = -1` produce stable text.
  
  Fix: narrow the catch to `CairoException` (the `LimitOverflowException` superclass) and use a single canonical substring via `TestUtils.assertContains(ex.getFlyweightMessage(), "Maximum number of pages")`. JVM-level `Error`s propagate instead of being swallowed; re-wrapping to `SqlException` on the construct path fails loud instead of hiding.

- **D-31** - Finding 4.1 (sibling todo): `SqlOptimiserTest#testSampleByFromToKeyedQuery` at `:4256` is the ONLY `printSql(...)`-only test in that file (vs 167 `assertPlan` + 75 `assertSql` + 12 `assertModel` siblings). PR #6946 renamed it from master's `testSampleByFromToDisallowedQueryWithKey` (which used `assertException`) and replaced the body with a bare `printSql` inside `assertMemoryLeak`. Passes under silent regressions: cursor-path fallback, `LIMIT 6` drop, cartesian collapse, lost `Async Group By` parallelism. A positive-case regression test already exists at `SampleByTest:7015` (`testSampleByFromToIsAllowedForKeyedQueries` with `count(*) rows, count_distinct(x) keys` wrapper asserting 9 buckets x 479 keys = 4311 rows).
  
  Fix: convert to `assertPlanNoLeakCheck` pinning the fast-path plan. Probe-and-freeze per Phase 15 D-02 — run once with a temporary `printSql` call to capture the exact plan string, then replace with the plan assertion. Alternative: delete the method (SampleByTest already covers correctness). Choose conversion over deletion because the test name signals coverage of the optimizer rewrite specifically, and keeping it in SqlOptimiserTest is consistent with siblings.

- **D-32** - Plan 17-05 ships all three findings in a single test-only plan. One commit suggested (three edits, same file + one other file) OR two commits (4.6 / 4.7 bundle in `SampleByFillTest.java`; 4.1 as separate commit in `SqlOptimiserTest.java`). Planner's judgment; bisectability favors two. Under D-01, none warrant PR body rows — all three are regression-recovery / test-quality improvements with no user-visible surface change. After the plan lands, move `2026-04-22-tighten-samplebyfilltest-cb-field-reset-and-constructor-thro.md` and `2026-04-22-upgrade-sqloptimisertest-testsamplebyfromtokeyedquery-from-p.md` to `.planning/todos/completed/` with `completed_at` + `completed_in: 17-05-PLAN.md`.

### Plan / commit structure

- **D-25** - Planner decides plan partitioning and commit boundaries. Suggested (non-binding) clustering:
  - Cluster A (code/test, by file): m1 slot-null + m2 field ordering + m3 IntList/BitSet refactor (+benchmark) all touch `SampleByFillCursor` / `SqlCodeGenerator` / `SampleByFillRecordCursorFactory`; m5 + m6 comments in `SampleByFillRecordCursorFactory`; m7 toSink0 in `QueryModel`; M-new CB at :603 + test; M2 test + two comments; m4 property test; m8a/b/c tests; M-unit D-26/D-27 cross-column TIMESTAMP/INTERVAL unit guard + paired rejection test.
  - Cluster B (PR body + title, single commit): M1 row + M3.2 K x B bullet rewrite + M3.4 two stale-sentence rewrites (+ optional row from D-05) + M3.5 count lines removal + m9 title rename.

  Cluster B lands last so counts in the Test plan (if kept) reflect post-cluster-A state. Phase 15 D-02 same-commit rule still applies for any restored-test-body pair that cannot pass without a paired codegen fix.

### Claude's Discretion

- m3 benchmark methodology (JMH harness vs ad-hoc measurement; threshold for "measurable cost"; whether to land the refactor as-is and let the benchmark gate a follow-up revert).
- m4 property test harness shape (new file `FillRecordDispatchTest` vs inline; parametrization approach; which types count as "supported" for the dispatch matrix).
- m8a/b/c exact SQL fixtures (table DDL, data shape, bucket sizes).
- Test method names and alphabetical placement within `SampleByFillTest`.
- M3.4 optional "What's covered" row: researcher verifies whether master's cursor path handles sub-day + TZ + FROM + OFFSET; row included iff master did not support the shape.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase scope (canonical)

- `~/.claude/plans/let-s-discuss-issues-one-gentle-elephant.md` - the plan file with per-finding proposed resolutions. Phase 17 overrides the plan file for M3.1 / M3.2 / M3.3 / M-new per D-01 (regression recovery does not warrant PR body edits). Plan file wording is verbatim for M1, M3.2 K x B bullet rewrite, M3.4 stale-sentence rewrites, m1 code, m5 rationale comment, m9 title.

### Project conventions

- `CLAUDE.md` (project) - ASCII-only log messages, `is/has` boolean naming, alphabetical member ordering (no banner comments), zero-GC data path, `assertMemoryLeak` / `assertQueryNoLeakCheck` test helpers, commit hygiene (no Conventional Commits prefix for commit titles; `feat(sql):` prefix acceptable for PR title per m9).

### Prior phase CONTEXTs (patterns to respect)

- `.planning/phases/16-fix-multi-key-fill-prev-with-inline-function-grouping-keys/16-CONTEXT.md` - D-04 (`assertQueryNoLeakCheck(..., false, false)` for SAMPLE BY FILL factory contract); D-05 (same-commit -ea defensive assert lands with classifier fix). Test alphabetization pattern.
- `.planning/phases/15-address-pr-6946-review-findings-and-retro-fixes/15-CONTEXT.md` - D-02 (test-restoration commits paired with codegen fix when the test body regresses without the fix). Phase 15 Plan 02's `testFillKeyedRespectsCircuitBreaker` harness is the mirror template for M-new's dedicated test (D-09).
- `.planning/phases/14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-/14-CONTEXT.md` - D-15 (`assertQueryNoLeakCheck(..., false, false)` default for SAMPLE BY FILL); D-18 chain-walk factory-type probe pattern (used indirectly if Phase 17 ever needs a similar probe; not required by current decisions).
- `.planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-CONTEXT.md` - FillRecord 4-branch dispatch order (FILL_KEY -> cross-col-PREV-to-key -> FILL_PREV_SELF/cross-col-PREV-to-agg -> FILL_CONSTANT -> null). Grounds D-20 m4 property test's 5-source enumeration (adds FILL_CONSTANT as the fifth source alongside the four dispatch branches).

### Production file hooks

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
  - `:425` - `assert value != null` - D-17 rationale comment target.
  - `:486` - `CairoException.critical("sample by fill: data row timestamp ... precedes next bucket")` - grid-drift guard referenced by D-17 comment to contrast with assert-vs-throw.
  - `:603` (approximate; inside `initialize()`'s `if (keysMap != null)` branch, first statement inside `while (baseCursor.hasNext())`) - M-new D-08 CB poll insertion point.
  - `:1085-1089` - `getLong256(int, CharSink)` - D-18 comment rewrite target.
  - `SampleByFillCursor` inner class private-instance field declarations - D-15 alphabetical merge target; `outputColToKeyPos`/`keyPresent` fields - D-16 refactor target.

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`
  - `:3605-3611` - D-26 `needsExactTypeMatch` predicate; widen to include `TIMESTAMP` and `INTERVAL` tags.
  - `:3612-3618` - existing `!isTypeCompatible` throw path; D-26 reuses its error message verbatim (no wording change).
  - `:3647-3659` - D-14 m1 slot-null fix target (per-column FILL_CONSTANT TIMESTAMP branch introduced in `9df205bac5`).
  - `:3663` - existing ownership-transfer `setQuick(i, null)` pattern (template for D-14).

- `core/src/main/java/io/questdb/cairo/ColumnType.java`
  - `:85` `TIMESTAMP = DATE + 1` (= 8) and `:136` `INTERVAL = PARAMETER + 1` (= 39) - tag constants referenced by D-26 `needsExactTypeMatch` clause. `tagOf(t)` = `t & 0xFF` so MICRO vs NANO variants collapse to the same tag without the widened predicate.

- `core/src/main/java/io/questdb/griffin/model/QueryModel.java`
  - `toSink0` method - D-19 m7 fillOffset emission target.

### Master-reference source (for M-new parity)

- `origin/master:core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java` - canonical `while (baseCursor.hasNext()) { circuitBreaker.statefulThrowExceptionIfTripped(); ... }` pattern for key-map building. Grounds D-07 regression claim and D-08 verbatim fix.
- `origin/master:core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursor.java` - same pattern, second witness.
- `origin/master:core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursorFactory.java:144-173` - `createPlaceHolderFunction` TIMESTAMP re-parse pattern. Already mirrored in the PR by `9df205bac5`; cited here for context on why M3.1 is mid-term regression recovery.

### Test file hooks

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`
  - `testSampleByAlignToCalendarFillNullWithKey1` / `testSampleByAlignToCalendarFillNullWithKey2` - D-13 comment targets.
  - `testFillKeyedRespectsCircuitBreaker` (Phase 15) - template for D-09 `testFillKeyedPass1RespectsCircuitBreaker`.
  - `testFillWithOffsetAndTimezoneAcrossDst` - D-21 m8a companion target (spring-forward is the missing sibling).
  - `testFillPrevRejectNoArg` - D-23 m8c tightening target.
  - `testFillSubDayTimezoneFromOffset{,Empty,Plan}` (landed with `289d43090a`) - reference tests for D-05 M3.4 master-support probe; M3.4 does not add new tests.
  - `testFillPrevCrossColumnDecimalPrecisionMismatch` - D-27 template for `testFillPrevCrossColumnTimestampUnitMismatch` (rejection test pattern: build two aggregates of mismatched full-type, fill one from the other, assertException on expected message + position).

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` - nano mirrors land for any new test requiring nano parity (D-09 pass-1 CB test, per Phase 15 D-02 symmetric-landing pattern). Planner decides whether M2, m4, m8a, m8b, m8c need nano twins based on the fixture's unit-sensitivity.

### Live PR artefact

- PR #6946 body (retrievable via `gh pr view 6946 --json body --jq '.body'`) - target for D-11 M1 row addition, D-03 K x B bullet rewrite, D-05 two stale-sentence rewrites, D-06 M3.5 count removal. D-24 edits the title via `gh pr edit 6946 --title "..."`.

### Plan-file section references

- Plan file `### M1 - pre-1970 FILL guard` - verbatim table row for D-11.
- Plan file `**M3.2 - Circuit-breaker regression**` - K x B bullet rewrite wording for D-03.
- Plan file `**M3.4 - TZ + FROM + WITH OFFSET fill-grid alignment**` - both stale-sentence rewrites for D-05.
- Plan file `### m1 - null the slot before reassigning` - exact code form for D-14.
- Plan file `### m5 - assert value != null...` - rationale comment wording for D-17.
- Plan file `### m8 - Test gaps` - m8a/b/c fixtures guidance for D-21/22/23.
- Plan file `### m9 - Rename PR title` - exact new title for D-24.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `circuitBreaker.statefulThrowExceptionIfTripped()` - the `stateful` prefix means the method is a flag check in the fast case; no amortization needed. Master uses it as the first statement inside ingest/emission loops verbatim. M-new D-08 uses the same pattern.
- `testFillKeyedRespectsCircuitBreaker` harness (Phase 15 Plan 02) - tick-counting `MillisecondClock` CB with regression-coverage self-check. Template for D-09.
- `assertQueryNoLeakCheck(..., false, false)` - Phase 14 D-15 canonical call signature for SAMPLE BY FILL tests (random-access=false, timestamp=false).
- `Misc.free(fillValues.getQuick(i))` + `fillValues.setQuick(i, null)` ownership-transfer pattern at `SqlCodeGenerator.java:3663` - template for D-14 m1 fix.
- `timestamp_floor_utc` (used by fast-path rewrite) - handles pre-1970 negative timestamps natively; grounds D-11 M1's "new behavior vs master" claim.
- `gh pr view 6946` / `gh pr edit 6946` - PR body/title manipulation.

### Established Patterns

- **Regression-recovery-does-not-warrant-body-edit principle** (D-01) - new for Phase 17. Prior phases 14/15 leaned toward over-documenting their own regression recovery; this phase tightens.
- **Same-commit landing for codegen fix + test restoration** (Phase 15 D-02) - still applies, but no D-02 restoration pairs in Phase 17 (the 9df205bac5 tests are already restored on-branch).
- **Same-commit landing for -ea defensive assertion + classifier fix** (Phase 16 D-05) - still applies; no new classifier widening in Phase 17 though.
- **Alphabetical member ordering, no banner comments** (CLAUDE.md) - governs D-15 m2 field merge and all new test method placement.
- **Plan-file-verbatim PR body wording** - D-03, D-05, D-11, D-24 all copy plan file wording literally. Preserves reviewer traceability from finding to edit.

### Integration Points

- M-new D-08 - single-line insertion inside existing `while` body at `SampleByFillRecordCursorFactory.initialize():~603`.
- M2 D-12 / D-13 - new test + two comment lines in `SampleByFillTest`.
- m1 D-14 - single-line insertion (setQuick to null) in `SqlCodeGenerator.java:3647-3659`.
- m2 D-15 - cosmetic reorder in `SampleByFillCursor`.
- m3 D-16 - type swap + API-call changes in `SampleByFillCursor` (outputColToKeyPos + keyPresent sites).
- m4 D-20 - new test file or new test method; no production changes.
- m5/m6 D-17/D-18 - comment-only changes.
- m7 D-19 - single-line addition in `QueryModel.toSink0`.
- m8a/b/c D-21/22/23 - new test methods in `SampleByFillTest` (+ possibly `SampleByNanoTimestampTest`).
- M1/M3/m9 PR-body/title - external to source via `gh pr edit`.

</code_context>

<specifics>
## Specific Ideas

- Master's canonical CB pattern for M-new (D-08) was confirmed via `git show origin/master:SampleByFillPrevRecordCursor.java` and `SampleByFillValueRecordCursor.java`. Both files call `circuitBreaker.statefulThrowExceptionIfTripped()` as the first statement inside `while (baseCursor.hasNext()) { ... }` key-map-building loops. The PR's `c1deb9b14d` mirrored this for two emission-side sites but missed the ingest-side site. D-08 lands verbatim at :603.
- The K x B Future-work bullet (D-03) currently reads as an unqualified "CB" in the PR body. A careless reader can conflate the memory-budget CB (still future) with the cancellation CB (landed in `c1deb9b14d` + M-new pass-1 fix). Plan-file-verbatim rewrite disambiguates: "(Unrelated to the cancellation `SqlExecutionCircuitBreaker`, which is honoured by the fill cursor.)" This one sentence is the only regression-related PR body edit in Phase 17.
- D-05 M3.4 has an explicit planner branch: if researcher verifies master's cursor path supported sub-day + TZ + FROM + OFFSET, skip the optional "What's covered" row (regression recovery). If master did not, add the row (net-new behavior). Stale-sentence fixes happen regardless.
- D-13 comment wording should reference the exact Trade-off bullet heading in the PR body so `grep "Predicate pushdown past SAMPLE BY"` finds both the test comment and the Trade-off. Preserves reviewer traceability.
- D-20 m4 property test should iterate all `ColumnType.*` tags supported by FillRecord's typed getters (not every ColumnType in existence - e.g. CURSOR, RECORD, PARAMETER are not fill-able). Planner derives the exact tag set from FillRecord's getter surface.
- D-23 m8c exact-substring assertion should also lock the error message wording (currently "not enough fill values" or similar); planner copies the live master error text verbatim into the test.
- The Phase 17 final-cluster commit (D-25 cluster B) must happen after all test-adding commits (cluster A) so any surviving test-count reference captures the final state. D-06 drops the lines entirely, so order only matters if D-25 cluster B is split further.
- If M3.4 D-05 researcher verification cannot be resolved cleanly (master's cursor-path behavior is unclear), default to "stale-sentence cleanup only, no new row" - smaller PR body diff, accepts marginal ambiguity in exchange for not over-documenting.

</specifics>

<deferred>
## Deferred Ideas

- **Alias-guard test for M3.1 Fix B** - `testFillNonTimestampAliasGuard` pinning that `SELECT ts::LONG AS ts ... SAMPLE BY 1h FILL(NULL)` returns `AsyncGroupByFactory` unchanged. Rejected for Phase 17 (internal hardening without review justification; skip-fill path exercises the guard indirectly). Can be added to a post-merge correctness phase if the guard ever silently regresses.
- **Arithmetic OPERATION multi-key FILL(PREV) test** - Phase 16 covers `a || b` (string OPERATION) but not arithmetic `a + b`. Rejected for Phase 17 (classifier's third arm already handles it per D-02 definition in Phase 16; Phase 16's `-ea` assert would trap any drift). Can be added if a real-world arithmetic-OPERATION report surfaces.
- **K x B memory circuit-breaker implementation** - size-estimated fallback to cursor path when `unique_keys x buckets` exceeds configurable threshold. Remains Future work; Phase 17 only disambiguates the bullet wording. File as a standalone phase if pursued.
- **Fold pass-1 into `SortedRecordCursor.buildChain` callback** - the pass-1 CB fix (D-08) adds a per-row CB poll inside pass-1; a future refactor that eliminates pass-1 entirely by upserting keys during the sort's tree walk would supersede both the poll and the loop. Remains Future work per existing PR body bullet.
- **Non-determinism in SAMPLE BY full-suite runs** - orthogonal test-harness concern surfaced during earlier phase investigation; already listed in PR body Future work. Out of scope for Phase 17.

</deferred>

---

*Phase: 17-verify-pr-6946-body-drift-against-landed-commits-decide-code*
*Context gathered: 2026-04-22*
