# Phase 17: Address `/review-pr 6946` follow-ups - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `17-CONTEXT.md` - this log preserves the alternatives considered.

**Date:** 2026-04-22
**Phase:** 17-verify-pr-6946-body-drift-against-landed-commits-decide-code
**Areas discussed:** M3 per-commit verdicts, M-new pass-1 circuit-breaker gap, M2 pushdown test shape

---

## Gray area selection (initial)

Four candidate areas presented to user:

| Area | Selected |
|---|---|
| M3 per-commit verdicts | yes |
| M-new pass-1 CB + M2 test shape | yes |
| m3 refactor + m4 property test design | no |
| Plan/commit structure + PR body + m9 title | no |

User added a new finding mid-discussion (M-new: missing CB check in pass-1 keyed key discovery at `SampleByFillRecordCursorFactory.java:603`) - folded into the M-new + M2 area.

---

## M3 per-commit verdicts

User's reframing (turn 4): *"If PR had regression mid term and it's fixed now, there is no need to flood the description. Or we changed the behavior?"*

This established the D-01 principle (PR body edits only for new behavior vs master, new grammar/rejections, or currently-false statements). Reframed each sub-item accordingly.

### M3.1 (9df205bac5 TIMESTAMP unit drift + non-TIMESTAMP alias guard)

**Evolution captured:** master had correct TIMESTAMP handling via `SampleByFillValueRecordCursorFactory.createPlaceHolderFunction:144-173`; PR mid-state introduced a 1000x drift and altered test expectations to mask it; `9df205bac5` mirrored master's helper and restored tests. Fix B (non-TIMESTAMP alias guard) hardened new-to-PR codepath with no master analog.

| Option | Description | Selected |
|--------|-------------|----------|
| Skip (Recommended) | No new test, no PR body edit. Regression recovery. | yes |
| Add testFillNonTimestampAliasGuard | Internal-hardening test for Fix B (micro + nano). | |

**User's choice:** Skip. Fix B exercised indirectly by skip-fill path; no review-driven justification for dedicated hardening test.

### M3.2 (c1deb9b14d cancellation circuit-breaker regression)

**Evolution captured:** master's cursor-path fill factories poll CB in every ingest/emission loop; PR's new `SampleByFillCursor` had zero polls initially; `c1deb9b14d` fixed emission-side (hasNext entry + emitNextFillRow outer while). Pure mid-term regression recovery. Existing "K x B circuit-breaker" Future-work bullet is currently misleading because it doesn't qualify cancellation vs memory-budget.

Decision applied inline (no dedicated AskUserQuestion): no "What's covered" row; rewrite the K x B bullet only (currently-false-or-misleading category under D-01).

### M3.3 (82865efbc0 FUNCTION/OPERATION classifier, Phase 16)

**Evolution captured:** master's cursor-path cartesian handled interval/concat/||/cast grouping keys correctly; PR introduced fast-path classifier that missed them; Phase 16 restored. Regression recovery.

Decision applied inline: no PR body edit.

### M3.4 (289d43090a TZ + FROM + OFFSET fill-grid)

**Evolution captured:** PR body has two currently-false statements post-289d43090a ("offset applied only when `sampleByTimezoneName == null`" in Implementation-notes pitfall + same claim in "ALIGN+FILL without TO" row). Commit landed 95 lines of regression tests (micro + nano + empty + plan). Whether this represents net-new behavior vs master or mid-term regression depends on master's cursor-path support for sub-day + TZ + FROM + OFFSET, which was not confirmed in discuss.

| Option | Description | Selected |
|--------|-------------|----------|
| Rewrite to current behavior (Recommended) | Fix stale sentences per plan file verbatim. | |
| Delete stale lines | Remove pitfall + row entirely. | |
| First verify master support, then decide | Researcher/planner resolves. | yes |

**User's choice:** Verify master first. The two currently-false sentences get fixed regardless of verdict; optional new "What's covered" row depends on whether master supported the shape.

### M3.5 (test-count drift)

**Evolution captured:** plan file numbers already stale at write time; Phase 17 will add more tests.

| Option | Description | Selected |
|--------|-------------|----------|
| Drop the precise counts (Recommended) | Remove the numeric claims. | yes |
| Recount at final commit | Keep numbers, recount after Phase 17 tests land. | |
| Soften to approximate | "~135+" / "~280+" and update rarely. | |

**User's choice:** Drop the precise counts.

---

## M-new pass-1 circuit-breaker gap

Surfaced by user mid-discussion with full technical detail (file, line, code-path trace, reason real, suggested fix).

**Evolution captured:** master's cursor-path key-discovery loops all poll CB; PR's new fast-path had zero polls initially; `c1deb9b14d` fixed two emission-side sites (hasNext entry at :382, emitNextFillRow outer while at :530) but missed the ingest-side pass-1 loop inside `initialize()` at ~:603. Real regression vs master, one-line fix, cheap to ship.

| Option | Description | Selected |
|--------|-------------|----------|
| Code fix + dedicated test (Recommended) | statefulThrowExceptionIfTripped() at :603; testFillKeyedPass1RespectsCircuitBreaker mirroring Phase 15 harness with pass-1-tripping shape. | yes |
| Code fix + reuse existing test | Retune existing testFillKeyedRespectsCircuitBreaker shape. | |
| Periodic polling every 4096 rows | Counter+mask bookkeeping; no perf gain. | |
| Defer to post-merge phase | File as standalone correctness phase. | |

**User's choice:** (a) - code fix at :603 + dedicated test. No PR body row (regression recovery under D-01).

---

## M2 pushdown semantic change

**Evolution captured:** master's `SampleByFillNullRecordCursorFactory` blocked predicate pushdown past SAMPLE BY; PR enables it on fast path, filter moves into inner GROUP BY's `filter:` slot. Observable change when predicate selects by key. Trade-off paragraph documents it; two existing tests (`testSampleByAlignToCalendarFillNullWithKey1/2`) were silently updated without comments; no dedicated pin test.

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated test + comment + EXPLAIN (Recommended) | New testFillNullPushdownEliminatesFilteredKeyFills + EXPLAIN assertion + one-line comments on the two existing updated tests. | yes |
| Dedicated test + comment, no EXPLAIN | Same but output-only. | |
| Dedicated test only (plan file literal) | No comments on existing tests. | |
| Comment-only, no new test | Smallest diff; relies on existing tests as pin. | |

**User's choice:** Dedicated test + comment + EXPLAIN.

---

## Finalization

| Option | Description | Selected |
|--------|-------------|----------|
| I'm ready for context (Recommended) | Write CONTEXT.md; plan file defaults for unselected findings. | yes |
| Explore more gray areas | m3 benchmark, m4 harness, m8 fixtures, m9 wording, plan structure. | |

**User's choice:** Ready for context.

---

## Claude's Discretion

- m3 benchmark methodology (JMH vs ad-hoc; "measurable cost" threshold)
- m4 property test harness shape (new file vs inline; parametrization)
- m8a/b/c exact SQL fixtures
- Test method names and alphabetical placement within SampleByFillTest
- Whether M3.4's optional "What's covered" row lands (researcher verdict on master support)

## Deferred Ideas

- Alias-guard test for M3.1 Fix B (`testFillNonTimestampAliasGuard`)
- Arithmetic OPERATION multi-key FILL(PREV) test (`a + b` as grouping key)
- K x B memory circuit-breaker implementation (remains Future work)
- Fold pass-1 into `SortedRecordCursor.buildChain` callback (Future work)
- Non-determinism in SAMPLE BY full-suite runs (orthogonal harness concern)
