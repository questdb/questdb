# Phase 14: Fix Moderate `/review-pr 6946` findings — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `14-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-04-20
**Phase:** 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
**Areas discussed:** Plan / commit grouping, M-4 fix choice, Test scope from Mn-11, M-5/M-6 PR description wording

---

## Plan / commit grouping

| Option | Description | Selected |
|--------|-------------|----------|
| 1 plan per finding (8 plans) | Max bisectability; highest review overhead. | |
| Grouped by area (~3 plans) | Codegen / Cursor / Optimiser clusters. | ✓ |
| Hybrid: grouped + standalone | Tricky fixes standalone, simple ones bundled. | |
| Single bundled plan | All 8 in one plan. Not recommended. | |

**User's choice:** Grouped by area (~3 plans), plus M-7 as standalone last commit → 4 plans total.
**Notes:** User explicitly said "fix here as last commit for the phase" for M-7, which converts the 3-plan grouping into 4 plans.

### M-7 scope

| Option | Description | Selected |
|--------|-------------|----------|
| Fix here | Include in Phase 14 as defensive fix. | ✓ |
| Defer out of this PR | Document only; don't touch SortedRecordCursorFactory.java. | |
| You decide | Claude picks. | |

**User's choice:** Fix here as last commit for the phase.

### Commit hygiene enforcement timing

| Option | Description | Selected |
|--------|-------------|----------|
| During each plan (strict) | Every Phase 14 commit CLAUDE.md-compliant from commit 1. | ✓ |
| Squash at the end | Allow noisy commits during; squash before merge. | |
| Both | Clean commits during Phase 14 AND squash pre-Phase-14 noise. | |

**User's choice:** During each plan (strict).
**Notes:** Pre-Phase-14 Phase 12/13 history squash deferred to a separate pre-merge pass.

### Regression-test depth for hard triggers

| Option | Description | Selected |
|--------|-------------|----------|
| Best-effort per finding | Try triggers; accept manual-inspection + rationale if reproduction is hard. | |
| Skip hard triggers | Code fix + commit-message rationale; no test. | |
| Invest in triggers | Extra time to construct triggering queries even for M-1 and M-7. | ✓ |

**User's choice:** Invest in triggers.

---

## M-4 fix choice

| Option | Description | Selected |
|--------|-------------|----------|
| Propagate to_utc-wrapped FROM/TO | Optimiser wraps FROM/TO; fill grid aligns with GROUP BY grid. | ✓ |
| Compile-time guard, bypass fast path | Reject/bypass; falls back to legacy path (which also has the bug). | |
| Both: propagate AND guard as safety net | Belt and suspenders. | |

**User's choice:** Propagate to_utc-wrapped FROM/TO.
**Notes:** User asked for elaboration on master behavior and concrete timestamps before choosing. After walking through London-BST cases A/B/C (empty base, sparse data, dense data), user picked Option 1. See Case A/B/C analysis in CONTEXT.md specifics section.

### Parse mode for wrapped FROM/TO

| Option | Description | Selected |
|--------|-------------|----------|
| Reuse functionParser path | ExpressionNode flows through existing functionParser.parseFunction; zero new cursor state. | ✓ |
| Carry raw + timezone to cursor | Cursor computes to_utc at init() time; more state. | |

**User's choice:** Reuse functionParser path (recommended).

### Mn-13 ownership timing

| Option | Description | Selected |
|--------|-------------|----------|
| After factory returns | Free residuals between factory construction success and return. | ✓ |
| Before factory call | Free explicitly before factory; preserves catch-block semantics. | |

**User's choice:** After factory returns (recommended).

---

## Test scope from Mn-11

| Option | Description | Selected |
|--------|-------------|----------|
| Per-type dedicated FILL(PREV) tests | BOOLEAN/BYTE/SHORT/INT/LONG/DATE/TIMESTAMP/IPv4/BINARY + INTERVAL (mandatory for M-8). | ✓ |
| Ghost-test cleanup | testSampleByFromToIsDisallowedForKeyedQueries rename/assert/delete. | ✓ |
| Tighten fuzzy assertion | testFillPrevRejectNoArg single-message assertion. | |
| Decimal zero-vs-null distinction test | FILL(PREV) legitimate 0 vs no-prev-yet. | ✓ |

**User's choice:** 3 of 4 selected (fuzzy-assertion tightening descoped).

### Restore RecordCursorMemoryUsageTest direct factory coverage

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — restore | Assert SampleByFillRecordCursorFactory in chain. | |
| No — current is fine | Keep weakened assertion. | |
| You decide | Claude picks. | ✓ |

**User's choice:** You decide.
**Claude's decision:** Yes — restore. Aligns with the stricter assertQueryNoLeakCheck preference and catches regressions in the new factory layer's close-path.

### Test helper preference

| Option | Description | Selected |
|--------|-------------|----------|
| assertQueryNoLeakCheck | Factory properties asserted. | ✓ |
| assertSql | Simpler; data-only. | |
| Mixed | Stricter by default, fall back only on documented limitation. | |

**User's choice:** assertQueryNoLeakCheck (recommended).
**Notes:** Mixed fallback is effectively what assertQueryNoLeakCheck already supports (supportsRandomAccess=false is a valid contract value).

---

## M-5/M-6 PR description wording

| Option | Description | Selected |
|--------|-------------|----------|
| Existing Trade-offs section | Both under ## Trade-offs alongside existing bullets. | ✓ |
| Existing Future work section | Both under ## Future work → architecture changes. | |
| Split: M-5 Trade-offs, M-6 Future work | Memory is shipping, CPU is future. | |

**User's choice:** Existing Trade-offs section.

### Authorship

| Option | Description | Selected |
|--------|-------------|----------|
| I propose, you edit | Claude drafts; user revises. | ✓ |
| You write, I review | User drafts; Claude reviews. | |
| Just add a placeholder, defer drafting | TODO marker in PR body. | |

**User's choice:** I propose, you edit.

### Benchmark guidance

User asked clarifying question: "do we have benchmark for this at all?"

Claude investigated: no SAMPLE BY FILL query-level JMH benchmarks exist. `SampleByIntervalIteratorBenchmark` covers materialized-view interval iteration only. `GroupBy*HashMap/HashSet` benchmarks cover collections, not full queries. PR's "37 fresh-JVM runs" stress check was correctness-only.

After re-ask:

| Option | Description | Selected |
|--------|-------------|----------|
| Just flag the envelope, no measurement guidance | Describes O(K×B) memory + 3× scan cost as known tradeoffs. | ✓ |
| Flag envelope + call out absence of benchmarks | Explicitly note gap. | |
| Flag envelope + add new benchmark in Phase 14 | SampleByFillQueryBenchmark scope expansion. | |
| Flag envelope + open separate issue for bench | Link to follow-up issue. | |

**User's choice:** Just flag the envelope, no measurement guidance.

---

## Claude's Discretion

- M-7 exact fix mechanism (null `this.base` vs. `isClosed` guard) — pick lower-risk after reading factory hierarchy.
- Exact placement of new test methods — alphabetical per CLAUDE.md.
- Helper method extraction in `FillRecord` dispatch — only if zero per-row method-call overhead.

## Deferred Ideas

- M-5 architectural fix (merge-sort wrapper / circuit-breaker) — future phase.
- M-6 architectural fix (keysMap populated during buildChain) — future phase.
- JMH benchmark harness for SAMPLE BY FILL — future phase.
- Tighten `testFillPrevRejectNoArg` fuzzy 4-message assertion — low value, descoped.
- Minor findings Mn-1..Mn-10, Mn-12 — separate cleanup pass or case-by-case.
