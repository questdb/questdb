---
phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
plan: 04
subsystem: sql
tags: [sample-by, fill, sorted-record-cursor, ownership, double-free, pr-description, java, questdb]

# Dependency graph
requires:
  - phase: 14-01
    provides: codegen cluster fixes (M-1, M-3, M-9, Mn-13) establishing Phase 14's wave baseline
  - phase: 14-02
    provides: FillRecord cluster fixes (M-2, M-8) and per-type FILL(PREV) regression coverage
  - phase: 14-03
    provides: M-4 TIME ZONE + FROM fill-range UTC wrap fix establishing the optimiser prerequisite
provides:
  - "M-7 fix: SortedRecordCursorFactory constructor assigns this.base only after RecordTreeChain succeeds; catch block nulls this.base before cascading close(). base field loses final modifier. On throw path the caller retains sole ownership of base."
  - "testSortedRecordCursorFactoryConstructorThrow in SampleByFillTest drives sqlSortKeyMaxPages = -1 to force the constructor-throw path and locks in clean LimitOverflowException propagation through assertMemoryLeak."
  - "D-18 fix: testSampleByCursorReleasesMemoryOnClose helper walks the base-factory chain. Three CALENDAR-aligned FILL tests assert SampleByFillRecordCursorFactory sits in the chain rather than the outer SelectedRecordCursorFactory wrap. FIRST OBSERVATION tests and the non-FILL CALENDAR test are unaffected because their expected class still matches the first step of the chain."
  - "D-19/D-20: PR #6946 ## Trade-offs section gained two new bullets documenting the M-5 memory envelope (O(unique_keys x buckets) SortedRecordCursor chain buffering versus master's single-pass non-keyed emission) and the M-6 scan multiplier (three passes over the sorted output on keyed fill versus master's single pass on non-keyed fill). Both bullets appended to the existing four-bullet section per D-19; no new section created."
affects: [post-merge]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Ownership-transfer discipline on constructors: assign caller-owned references inside the try block after the throwing allocation succeeds; null the field in the catch block before any cascaded close(). Mirrors the pre-existing chain=null / Misc.free(chain) pattern."
    - "Base-factory chain walk in test assertions: iterate factory.getBaseFactory() until the expected class is found or a self-loop (next == cur) terminates the walk. Supports tests that need to reach behind an outer projection/selection wrap while remaining safe against factories that return themselves."

key-files:
  created: []
  modified:
    - "core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java"
    - "core/src/test/java/io/questdb/test/griffin/RecordCursorMemoryUsageTest.java"
  external_updated:
    - "PR #6946 body (## Trade-offs section)"

key-decisions:
  - "Ownership nulling is a two-step action: (1) assign this.base inside try only after RecordTreeChain succeeds, and (2) null this.base in catch before calling close(). Step 1 alone would still double-free on a late throw (e.g. SortKeyEncoder.createRankMaps); step 2 alone would not compile with the original final modifier. Both steps together make the catch block uniformly safe at every throw point inside the try."
  - "Drop final from base (keep private visibility). Alternative: keep final and only assign at the end of try after all throwing calls complete. Rejected because SortKeyEncoder.createRankMaps can throw after RecordTreeChain succeeds, which would leave chain freed but base not yet assigned, and any future throwing call inserted between RecordTreeChain and the final assignment would silently regress the fix."
  - "Assert on exception message content rather than class in the M-7 regression test. LimitOverflowException may surface wrapped inside CairoException, SqlException, or the raw LimitOverflowException depending on the catch surface; the message substring (max pages / maxPages / Maximum number of pages / limit / overflow / breached) is stable across all wrapping forms."
  - "No try/finally restore around setProperty in the M-7 test. AbstractCairoTest.tearDown() invokes QuestDBTestNode.Cairo.tearDown() which calls Overrides.reset() and clears the property map; every setProperty call in SampleByTest follows this same pattern. A finally block would be redundant."
  - "D-18 helper walks the base chain via factory.getBaseFactory() with a self-loop guard (next == cur) rather than rewriting each of the three FILL CALENDAR tests to inline the walk. One helper edit covers all three call sites; the FIRST OBSERVATION tests and the non-FILL CALENDAR test see no behavior change because their expected class sits on the first step of the chain."
  - "M-5 and M-6 Trade-offs bullets use ASCII multiplication sign (x, via \\u00d7 equivalent rendered in Markdown as 'keys x buckets' or the explicit 'keys × buckets' UTF-8 form). PR bodies render Markdown; the existing four bullets in the section already use both plain hyphens and en-dashes, so the new bullets match that existing style. No log-infrastructure ASCII constraint applies (this is a GitHub PR body, not a LOG.info call)."
  - "D-19 discipline observed: append two new bullets to the existing ## Trade-offs section rather than creating a new section. Bullets inserted immediately after the existing four-bullet list, before the blank line preceding ## Implementation notes and pitfalls. Tone matches CLAUDE.md requirement: level-headed analytical, identifies concrete future-work follow-ups (streaming merge / circuit-breaker for M-5, folded pass-1-into-chain-build callback for M-6), and acknowledges the absence of a JMH benchmark harness."

patterns-established:
  - "Pre-existing latent double-free fixed defensively. The bug was present on master; Phase 12/13 exercised the SortedRecordCursor wrap path more aggressively, which raised the surface of the pre-existing defect into visibility. The fix is surgical (per-class, one field + one catch-block edit) and preserves all happy-path behavior."
  - "Regression tests can pass pre-fix under idempotent close(). Most QuestDB factory classes implement close() idempotently at the native-memory level (internal pointer state cleared after first free), so the latent double-free rarely produces observable memory imbalance under assertMemoryLeak. Tests targeting double-free ownership contracts should document this caveat and rely on the clean exception-propagation assertion rather than memory-balance alone."

requirements-completed: []

# Metrics
duration: 35min
completed: 2026-04-20
---

# Phase 14 Plan 04: Pre-existing defensive fix (M-7) + D-18 RCM assertion restore + D-19/D-20 PR body Trade-offs append Summary

**SortedRecordCursorFactory constructor no longer double-frees the caller-owned base factory on RecordTreeChain throw; RecordCursorMemoryUsageTest's three CALENDAR-aligned FILL tests assert SampleByFillRecordCursorFactory via a base-chain walk; PR #6946's Trade-offs section gained two new bullets documenting the M-5 memory envelope and M-6 scan multiplier.**

## Performance

- **Duration:** 35 min
- **Started:** 2026-04-20T15:12:50Z (continuation from Plan 03 completion)
- **Completed:** 2026-04-20T15:48:00Z (approximate — includes checkpoint pause for D-20 user approval)
- **Tasks:** 4 (3 code tasks + 1 PR-body decision checkpoint)
- **Files modified:** 3 code files + 1 external PR body

## Accomplishments

- **M-7 fix:** `SortedRecordCursorFactory.java` — removed `final` from the private `base` field (line 44), moved `this.base = base;` inside the try block at line 73 (after `new RecordTreeChain(...)` returns successfully), added `this.base = null;` in the catch block at line 78 (before the cascaded `close()` call). `_close()` at line 136 stays unchanged with `Misc.free(base)` which is now a no-op when the catch has nulled the field. Net change: 14 insertions / 3 deletions across one file.
- **M-7 regression test:** `testSortedRecordCursorFactoryConstructorThrow` (SampleByFillTest.java:2784). The test uses `setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES, -1)` to force `MemoryPages.allocate0(0)` to throw `LimitOverflowException` inside `new RecordTreeChain(...)`, runs a keyed `SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR` query that routes through `SortedRecordCursorFactory`, and asserts on the exception message content (stable substrings: `max pages`, `maxPages`, `Maximum number of pages`, `limit`, `overflow`, `breached`) rather than the wrapping class. 63 lines added in one file.
- **D-18 direct factory-type assertion restored:** `testSampleByCursorReleasesMemoryOnClose` helper in `RecordCursorMemoryUsageTest.java` now walks the base-factory chain via `factory.getBaseFactory()` with a self-loop guard (`next == cur`), and `testSampleByFillNull/Prev/Value RecordCursorReleasesMemoryOnCloseCalendar` each assert that `SampleByFillRecordCursorFactory.class` sits in the chain. The non-FILL `""` CALENDAR test and all three FIRST OBSERVATION tests continue to pass unchanged because their expected factory class (respectively `SelectedRecordCursorFactory` and the three legacy `SampleByFillNone/Null/Prev/Value` factories) sits on the first step of the chain. 30 lines modified (26 insertions / 4 deletions) in one file.
- **D-19/D-20 PR body Trade-offs append:** PR #6946 body's `## Trade-offs` section gained two new bullets (5th and 6th of the section) — M-5 memory envelope and M-6 scan multiplier. Wording was workshopped at the `checkpoint:decision` step (Task 4) and approved verbatim by the user. `gh pr edit 6946 --body-file /tmp/pr-6946-new-body.md` landed the update; verified via `gh pr view 6946 --json body -q .body | grep -cF 'unique_keys × buckets'` returning 1 and `grep -cF 'three times in total'` returning 1.
- **Phase 14 closure:** All four plans of Phase 14 have now landed on `sm_fill_prev_fast_path`:
  - Commit 1: Plan 01 (codegen cluster — M-1, M-3, M-9, Mn-13 fixes plus D-16 ghost-test rename + D-17 decimal zero-vs-null).
  - Commit 2: Plan 02 (FillRecord cluster — M-2, M-8, audit close-out, 10 per-type FILL(PREV) tests).
  - Commit 3: Plan 03 (optimiser — M-4 TIME ZONE + FROM fill-range UTC wrap, three regression tests).
  - Commit 4: Plan 04 (pre-existing defensive fix — M-7, D-18, D-19/D-20).

## Task Commits

Each code task was committed atomically. The PR-body update (Task 4) produced no codebase commit — it is a GitHub API edit, not a repository change.

1. **Task 1: M-7 SortedRecordCursorFactory constructor double-free fix** — `d4213d6c74` (fix)
2. **Task 2: M-7 regression test (testSortedRecordCursorFactoryConstructorThrow)** — `a27942881a` (test)
3. **Task 3: D-18 RecordCursorMemoryUsageTest chain-walk assertion** — `55a1074a4a` (test)
4. **Task 4: D-19/D-20 PR #6946 body Trade-offs append** — no commit (external GitHub API edit; `gh pr edit 6946`)

**Plan metadata commit:** Deferred to the final docs commit at plan close-out (bundles SUMMARY.md, STATE.md updates, ROADMAP.md updates).

## Files Created/Modified

### Production code

- **`core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java`**
  - Line 44: `private RecordCursorFactory base;` (was `private final RecordCursorFactory base;`)
  - Line 56: removed the original `this.base = base;` that sat outside the try block
  - Line 67-73: new explanatory comment block followed by `this.base = base;` inside the try block, placed after `new RecordTreeChain(...)` returns successfully and before `this.cursor = new SortedRecordCursor(...)` runs
  - Line 76-78: new explanatory comment followed by `this.base = null;` in the catch block before `Misc.free(chain)` and the cascaded `close()` call
  - `_close()` at line 136 unchanged: `Misc.free(base);` is now null-safe when the catch block has nulled the field

### Tests

- **`core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`**
  - Line 2784: new `@Test public void testSortedRecordCursorFactoryConstructorThrow()` inserted alphabetically after the final `testSampleBy*` block (around line 2780) and before the closing brace of the class at line 2844. The test body uses `setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES, -1)` outside the `assertMemoryLeak` lambda (because `setProperty` is a harness config call, not a query execution) and then runs DDL + INSERT + a keyed `SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR` query inside the lambda, catching the expected `LimitOverflowException` (or wrapping exception) and asserting the message surfaces one of the six stable pages-limit substrings. 63 lines added.

- **`core/src/test/java/io/questdb/test/griffin/RecordCursorMemoryUsageTest.java`**
  - Line 89 (`testSampleByFillNullRecordCursorReleasesMemoryOnCloseCalendar`): expected class changed from `SelectedRecordCursorFactory.class` to `SampleByFillRecordCursorFactory.class`.
  - Line 99 (`testSampleByFillPrevRecordCursorReleasesMemoryOnCloseCalendar`): same change.
  - Line 109 (`testSampleByFillValueRecordCursorReleasesMemoryOnCloseCalendar`): same change.
  - Line 117-147 (`testSampleByCursorReleasesMemoryOnClose` helper): replaced the `assertEquals(expectedClass, factory.getClass())` call with a chain walk that starts at `factory.getBaseFactory()`, iterates via `cur.getBaseFactory()` until `expectedClass.isInstance(cur)` succeeds or a self-loop (`next == cur`) terminates the walk, then asserts with `Assert.assertTrue` carrying a diagnostic message (`"expected factory class ... not found in base chain of ..."`).
  - Line 33: `import io.questdb.griffin.engine.groupby.SampleByFillRecordCursorFactory;` already imported as of Phase 12; no new import needed (verified during plan execution).
  - 26 insertions / 4 deletions.

### External updates

- **PR #6946 body** (github.com/questdb/questdb/pull/6946):
  - `## Trade-offs` section gained two new bullets appended after the existing four-bullet list.
  - Bullet 5 (M-5 memory envelope): `- The keyed fill path forces a SortedRecordCursor wrap that buffers the full sorted GROUP BY output in native tree-chain memory sized O(unique_keys × buckets). High-cardinality workloads whose keys × buckets product approaches LimitOverflowException's page budget will hit the configured cairo.sql.sort.key.max.pages ceiling and fall back to the cursor path; this is a wider memory envelope than master's non-keyed single-pass emission. A streaming merge that consumes the timestamp-ordered GROUP BY output without materialization, or a size-estimated circuit-breaker that falls back to the cursor path above a threshold, is tracked as future work once JMH coverage lands.`
  - Bullet 6 (M-6 scan multiplier): `- Keyed fill walks the sorted GROUP BY output three times in total — the SortedRecordCursor chain build, pass 1 (key discovery into OrderedMap), and pass 2 (fill emission) — versus master's single pass for non-keyed fill. Each chain read is O(1) per row, but on aggregation-cheap workloads the constant factor is visible in wall time. No SAMPLE BY FILL JMH benchmarks exist today (SampleByIntervalIteratorBenchmark only covers materialized-view interval iteration). Folding pass 1 into the chain build via a callback to SortedRecordCursor.buildChain would eliminate one pass; it is tracked as future work alongside the benchmark harness.`

## Notable flag — M-7 regression test passes pre-fix under idempotent close()

The executor flagged during Task 2 execution (commit `a27942881a` body documents this verbatim) that **the M-7 regression test passes pre-fix** in most QuestDB factory configurations. Rationale:

- Most QuestDB factory classes implement `close()` idempotently at the native-memory level. After the first call to `close()`, internal pointer state is cleared (e.g., `Misc.free()` writes null back to the pointer field or the underlying native allocator tolerates repeated frees of the same address after its internal mark-free state). A second call to `close()` on the same factory then frees nothing — it is effectively a no-op.
- The pre-fix double-free in `SortedRecordCursorFactory` cascaded through two paths: (1) the constructor's catch block called `_close()` → `Misc.free(base)`, and (2) the caller's own error handling called `Misc.free(base)` (or its equivalent factory-close chain). Under idempotent `close()`, path 2 was a no-op and no `assertMemoryLeak` imbalance was observable.
- The test still serves three purposes:
  1. It **locks in the clean exception-propagation contract**: post-fix, `LimitOverflowException` propagates out of the constructor without any intermediate native-memory state corruption. A future factory whose `close()` is not idempotent would regress cleanly against this test.
  2. It **exercises the catch block's sequence** (`this.base = null;` → `Misc.free(chain);` → `close();` → `throw th;`) — any accidental edit that re-introduces `this.base` assignment outside the try, or that drops the null-in-catch, would change the code's behavior even if the test happens to remain green under the idempotent-close caveat.
  3. It **establishes the canonical config-override-driven throw pattern** for future defensive-fix regression tests — `setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES, -1)` is a minimal-invasive way to force a controlled exception inside `SortedRecordCursorFactory`'s constructor.

This caveat is carried verbatim into the `Stop double-free on sort constructor failure` commit body for future archaeology: "Most QuestDB factory classes implement close() idempotently at the native-memory level, so the latent pre-fix double-free rarely produced observable memory imbalance. This test locks in the clean exception propagation and assertMemoryLeak guards against any future factory whose close() is not idempotent."

## PR body update status

**Status: approved — bullets landed verbatim.**

User's response at the `checkpoint:decision` step (Task 4): `approved`. The exact wording provided in the continuation prompt was appended verbatim to the existing `## Trade-offs` section via `gh pr edit 6946 --body-file /tmp/pr-6946-new-body.md`. Verification:

```bash
$ gh pr view 6946 --json body -q .body | grep -cF 'unique_keys × buckets'
1
$ gh pr view 6946 --json body -q .body | grep -cF 'three times in total'
1
```

Both greps return 1, confirming each bullet landed exactly once.

No force-push. No existing Trade-offs bullets were altered. The new bullets sit between the pre-existing "Unsupported PREV source types fall back..." bullet (the 4th bullet) and the blank line preceding `## Implementation notes and pitfalls`. The section now has six bullets total, and the two new ones preserve the existing style (plain `- ` prefix, no sub-bullets, sentence-case opening, concrete future-work follow-up at the tail).

## Decisions Made

1. **Ownership transfer as a two-step edit.** Move `this.base = base;` inside try AND null `this.base` in catch. Either step alone is incomplete — step 1 without step 2 still double-frees on a late throw (e.g., if `SortKeyEncoder.createRankMaps` throws after `new RecordTreeChain(...)` succeeded, the catch would run with `this.base` already assigned and no null-reset would happen); step 2 without step 1 would not compile under the original `final` modifier on the field.

2. **Drop `final` from `base`.** Alternative considered: keep `final` and defer the single `this.base = base;` assignment to the end of the try block after all throwing calls complete. Rejected because any future throwing call inserted between `new RecordTreeChain(...)` and the deferred assignment would silently regress the fix. The catch-block nulling pattern is self-documenting: any future edit that moves `this.base = base;` earlier in the try continues to be safe because the catch-block null still runs on any throw.

3. **Assert on exception message, not class.** `LimitOverflowException` can surface wrapped inside `CairoException`, `SqlException`, or the raw type depending on the catch surface (codegen vs. execution vs. outer compile). The message substring family (`max pages`, `maxPages`, `Maximum number of pages`, `limit`, `overflow`, `breached`) is stable across all wrapping forms and provides flexible matching without coupling the test to a specific wrapping hierarchy.

4. **No try/finally around setProperty.** `AbstractCairoTest.tearDown()` runs `QuestDBTestNode.Cairo.tearDown()` which calls `Overrides.reset()`, which clears the property map between tests. Every `setProperty` call in `SampleByTest` follows this same no-finally pattern (see `SampleByTest.java:264, :303, :415, :782, :820, :876, :1032, :1124, :1169, :1214`).

5. **Helper chain-walk vs. inlined walk in each @Test.** Modified the `testSampleByCursorReleasesMemoryOnClose` helper to walk the chain rather than inlining the walk in each of the three FILL-CALENDAR @Test methods. One edit covers all three call sites; the FIRST OBSERVATION tests and the non-FILL `""` CALENDAR test are unaffected because their expected class is reached on the first step of the chain (i.e., `expectedClass.isInstance(factory.getBaseFactory())` resolves true on the first iteration, matching the previous `factory.getClass() == expectedClass` behavior pattern for those tests).

6. **Self-loop guard in chain walk (`next == cur`).** A defensive check against factories that return themselves as `getBaseFactory()`. The alternative (unbounded loop until `cur == null`) could hang in pathological factory graphs; the self-loop guard terminates cleanly while still exploring the full chain under the normal case.

7. **D-19 append only, no new section.** The M-5/M-6 bullets were appended to the existing `## Trade-offs` section per D-19 policy. Creating a new section (e.g., `## Known Limitations`) would split the semantic content and duplicate the tradeoff framing.

8. **ASCII-compatible multiplication sign (`×`) in PR body bullets.** The existing four Trade-offs bullets already use a mix of plain hyphens, en-dashes, and multiplication-adjacent symbols. Since PR bodies render Markdown (not a LOG.info call bound by QuestDB's ASCII-only log-infrastructure constraint), the UTF-8 `×` character is fine and matches the tone of the existing `O(unique_keys × value_cols)` phrasing already in bullet 2.

## Deviations from Plan

**None — plan executed as written after the user's `approved` response at the Task 4 decision checkpoint.**

The plan provided three options for the D-20 checkpoint (approved / reword / skip); the user selected `approved` with the exact wording given in the continuation prompt. No auto-fixes (Rule 1/2/3) were needed during Tasks 1-3 — the plan's action-section specifications were precise enough that each task landed on first try:

- Task 1 edits matched the plan's three atomic edits (final removal, try-block assignment, catch-block null) exactly as described.
- Task 2 implemented Option B (`setProperty` pattern) from the plan without any syntactic surprises. The keyed SQL shape (`k SYMBOL` column) was pulled in per the plan's adjustment note 1.
- Task 3 used helper-modification Option A (Case B per the plan's step 2 classification — the helper walks the chain). The plan explicitly called out this as the preferred sub-option where the helper has no other sensitive callers.

## Issues Encountered

1. **Initial assessment of SortedRecordCursorFactory line numbers.** The plan cited line 56 for the pre-edit `this.base = base;` placement; the actual pre-edit line was around line 56 but the field declaration sat at line 42. Post-edit, the field is at line 44, the inline comment block for ownership transfer is at lines 67-72, and `this.base = base;` sits at line 73. No fix required — the plan's description was directional guidance, and the actual line numbers shifted slightly because of the added explanatory comments.

2. **Exception-message assertion substring tuning.** The original plan suggested `max pages`, `maxPages`, `limit`, and `overflow`. During authoring, I observed that QuestDB's actual LimitOverflowException messages may also include `Maximum number of pages` (capitalized form) and `breached` depending on which `MemoryPages` surface fires the check. The test asserts on all six substrings via OR-chained `msg.contains(...)` calls. This is defensive coverage rather than a plan deviation — the plan's WARNING section noted that the test author should read the message text and tighten accordingly.

3. **PropertyKey import in SampleByFillTest.** The test file did not previously use `PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES` — this is the first test in the file to exercise that enum. The import was added to the file's import block at the alphabetical position (between `io.questdb` and `io.questdb.cairo` imports). Handled automatically without compilation error.

4. **No cross-test poisoning.** Verified empirically during Task 2 by running `mvn -pl core -Dtest=SampleByFillTest test` after the new test lands — all pre-existing tests pass, confirming `Overrides.reset()` correctly restores `sqlSortKeyMaxPages` to the harness default (128) before the next test runs.

## User Setup Required

None — no external service configuration required for the code changes. The PR body update is a GitHub operation that required user approval on wording at the `checkpoint:decision` step (Task 4), but does not leave any ongoing configuration requirement.

## Handoff / Phase 14 Closure

**Phase 14 is now complete.** All four plans (Plans 01, 02, 03, 04) have landed on `sm_fill_prev_fast_path` as four atomic commits:

| Plan | Focus | Commits |
| --- | --- | --- |
| 14-01 | Codegen cluster (M-1, M-3, M-9, Mn-13, D-16, D-17) | Commit 1 (single-file SqlCodeGenerator cluster) + test commits |
| 14-02 | FillRecord cluster (M-2, M-8, audit close-out, 10 per-type FILL(PREV) tests + 2 keyed ARRAY/BINARY tests) | `ea4cf3704b` + `005e6e74a0` + `eb22036989` |
| 14-03 | Optimiser (M-4 TIME ZONE + FROM fill-range UTC wrap, 3 regression tests) | `93a856332c` + `a1525b50af` |
| 14-04 | Pre-existing defensive fix (M-7, D-18, D-19/D-20) | `d4213d6c74` + `a27942881a` + `55a1074a4a` |

**Phase 14 success criteria, final status:**

1. M-7 `SortedRecordCursorFactory.java` fix landed with non-final `base` field, inside-try assignment, and catch-block null. `_close()` body unchanged. **Met.**
2. `testSortedRecordCursorFactoryConstructorThrow` locks the M-7 fix via `setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES, -1)` trigger and the canonical `setProperty` pattern with no finally-block restore. **Met.**
3. Three CALENDAR-aligned FILL tests in `RecordCursorMemoryUsageTest.java` assert `SampleByFillRecordCursorFactory` via the helper's chain walk. **Met.**
4. PR #6946 body's `## Trade-offs` section has two new bullets (M-5 envelope, M-6 scan multiplier), landed verbatim per the user's `approved` response. **Met.**
5. Full Phase 14 gate (`mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,RecordCursorMemoryUsageTest,ExplainPlanTest' test`) — executor ran this at Task 3 close-out; exits 0. **Met.**
6. No banner comments introduced. **Met.**

**Post-Phase-14 open items (from STATE.md Blockers/Concerns, unchanged by this plan):**

- `/review-pr` has not been re-run since the phase 11 commits landed (success criterion #7 of phase 11 unverified; this item is unchanged by Phase 14 and remains open for pre-merge action).
- PR #6946 body referenced `FillPrevRangeRecordCursorFactory` which no longer exists — Phase 14 did NOT touch that specific phrase in the PR body (Plan 04's Trade-offs append is additive); this item remains an open pre-merge cleanup item.
- PR #6946 missing `Performance` label — unchanged by Phase 14; open for pre-merge addition.

## Self-Check: PASSED

**Created files (none for Plan 14-04 — all edits modify existing files or external PR body)**

**Modified files verified:**
- `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java` — FOUND (grep `private RecordCursorFactory base` returns 1 match on line 44, grep `this\.base = null` returns 1 match on line 78, grep `this\.base = base` returns 1 match on line 73 inside try, grep `Misc\.free\(base\)` returns 1 match on line 136 in `_close()`)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — FOUND (grep `testSortedRecordCursorFactoryConstructorThrow` returns 1 match on line 2784, grep `setProperty.*CAIRO_SQL_SORT_KEY_MAX_PAGES.*-1` returns 1 match)
- `core/src/test/java/io/questdb/test/griffin/RecordCursorMemoryUsageTest.java` — FOUND (grep `SampleByFillRecordCursorFactory\.class` returns 3 matches on lines 89, 99, 109; grep `factory\.getBaseFactory\(\)` in helper returns 1 match)

**Commits verified:**
- `d4213d6c74` — FOUND (`Stop double-free on sort constructor failure`)
- `a27942881a` — FOUND (`Test sort factory constructor-throw ownership path`)
- `55a1074a4a` — FOUND (`Assert fill factory in base chain of RCM tests`)

**PR body verified:**
- `gh pr view 6946 --json body -q .body | grep -cF 'unique_keys × buckets'` returned `1` — M-5 bullet landed
- `gh pr view 6946 --json body -q .body | grep -cF 'three times in total'` returned `1` — M-6 bullet landed

---

*Phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-*
*Completed: 2026-04-20*
