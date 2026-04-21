# Phase 16: Fix multi-key FILL(PREV) with inline FUNCTION grouping keys - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `16-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-04-21
**Phase:** 16-fix-multi-key-fill-prev-with-inline-function-grouping-keys
**Areas discussed:** Fix approach, Classifier detection mechanism, Non-PREV regression coverage, Defensive assertion landing

---

## Gray Area Selection

| Option | Description | Selected |
|---|---|---|
| Fix approach (Option 1 vs 2) | Classifier fix local to generateFill vs upstream canonicalization in SqlOptimiser.rewriteSampleBy | ✓ |
| Classifier detection mechanism | How the widened third arm recognizes a non-aggregate FUNCTION as a key column | ✓ |
| Non-PREV regression coverage | Whether FILL(NULL)/FILL(constant) multi-key inline-function tests are in scope | ✓ |
| Defensive assertion landing | How the Phase 15 draft assertion lands (same commit / follow-up / separate plan) + exact form | ✓ |

**User's choice:** all four.

---

## Fix approach

| Option | Description | Selected |
|---|---|---|
| Option 1: classifier fix (Recommended) | Widen third arm in SqlCodeGenerator.generateFill. Narrow, reversible, 1-2 plans. | ✓ |
| Option 2: upstream canonicalization | Lift inline-function grouping expressions to virtual column projections in SqlOptimiser.rewriteSampleBy. 3-4 plans, ripple risk. | |
| Option 1 now, Option 2 as future cleanup | Ship Option 1, file follow-up todo for Option 2. | |

**User's choice:** Option 1.

**Clarification raised by user:** "What is the upstream here? SampleBy logic for fast path? All SampleBy logic? Even above?"

**Claude's response:** Option 2's upstream is specifically `SqlOptimiser.rewriteSampleBy` (the SAMPLE BY rewrite that produces the GROUP BY fast-path model). It affects every fast-path SAMPLE BY with a non-literal grouping key (fill or no-fill) — not the whole SQL optimizer, not everything above rewriteSampleBy. Plan text would shift across SqlOptimiserTest / ExplainPlanTest / SampleByTest / SampleByNanoTimestampTest expectations.

**Follow-up question from user:** "What are the 'extra' affected sample by? Would it be actually helpful as they may be affected by the similar bug?"

**Claude's response:** Non-FILL SAMPLE BY with inline-function grouping keys (e.g. `SELECT ts, interval(lo, hi) k, count() FROM t SAMPLE BY 1h`) has no known latent bug on this path — the GROUP BY engine handles function-valued keys natively, the classifier is the only place that mis-partitions them, and the classifier only runs for FILL. Option 2's benefit is structural (invariant narrowing) not additional-bug-fixing. Option 1 plus a strong defensive assertion gets most of the benefit without the plan-text churn.

**User confirmed Option 1.**

---

## Classifier detection mechanism

| Option | Description | Selected |
|---|---|---|
| (c) !isGroupBy + alias assert (Recommended) | `(FUNCTION \|\| OPERATION) && !isGroupBy(ast.token)` + `-ea` assert that alias resolves to factory key | ✓ |
| (a) !isGroupBy only | `(FUNCTION \|\| OPERATION) && !isGroupBy(ast.token)` continue, no positive alias check | |
| (b) Alias-in-key-set positive check | Positively assert alias resolves to key-set via keyColIndices. Biggest refactor. | |

**User's choice:** (c).

**Rationale captured:** `isGroupBy` is the canonical predicate (20+ uses in SqlOptimiser.java), already available via `functionParser.getFunctionFactoryCache()` which is accessible inside `generateFill`. The `-ea` alias assert is belt-and-braces; `groupByMetadata.getColumnIndexQuiet(alias)` is already used a few lines later to populate `factoryColToUserFillIdx` so the lookup is free.

### OPERATION coverage sub-question

| Option | Description | Selected |
|---|---|---|
| Yes, cover FUNCTION and OPERATION (Recommended) | Same root cause; rewriteSampleBy at :8066 admits OPERATION into maybeKeyed. Add `a \|\| b` to regression suite. | ✓ |
| No, FUNCTION only | Stick to interval/concat/cast (all FUNCTION). | |

**User's choice:** FUNCTION and OPERATION both.

---

## Non-PREV regression coverage

| Option | Description | Selected |
|---|---|---|
| PREV + NULL, 1 variant (Recommended) | FILL(PREV) × {interval, concat, cast, \|\|} + FILL(NULL) × 1 (cast). Skip FILL(const). 5 new tests. | ✓ |
| PREV only | 4 FILL(PREV) variants only. Defensive assertion catches FILL(NULL)/const drift. | |
| Full matrix: PREV, NULL, const × 4 key shapes | 12 tests. Most thorough pinning. Heavy suite, overlap with Phase 11/14 FILL(NULL) tests. | |

**User's choice:** PREV + NULL, 1 variant.

**Rationale:** FILL(PREV) is the primary correctness failure mode (cartesian drop). FILL(NULL) exhibits the same classifier bug but the symptom is nulls instead of dropped rows — one representative test pins that the fix is fill-mode-independent. FILL(const) currently fails loudly with a SqlException from `aggNonKeyCount` mismatch and the classifier fix restores correctness by default.

---

## Defensive assertion landing

| Option | Description | Selected |
|---|---|---|
| (I), same commit as fix (Recommended) | Assert `ast.type == FUNCTION && isGroupBy(ast.token)` at aggregate-arm entry. Locks three-way partition. Same-commit landing per Phase 15 style. | ✓ |
| (I), follow-up commit in same plan | Same invariant, separate commit for cleaner review history. | |
| (II), same commit as fix | Post-loop invariant that every non-timestamp non-aggregate factory column maps to -1 in factoryColToUserFillIdx. More global but more code. | |
| (I) + (II), same commit | Both asserts. Overlapping invariants, belt-and-braces. | |

**User's choice:** (I), same commit as fix.

**Rationale:** QuestDB surefire runs `-ea` per `core/pom.xml:37`, so CI catches drift immediately. (I) is tighter and closer to the actual fire site than (II); the three-way partition is the invariant that matters post-fix.

---

## Final check

| Option | Description | Selected |
|---|---|---|
| I'm ready for context | All four selected areas resolved. | ✓ |
| Plan/commit breakdown | Discuss whether 1 plan or split. | |
| Cursor-side wiring verification scope | Is keyColIndices verification an explicit task or fallout from plan-phase research? | |
| Explore more gray areas | Something else still underspecified. | |

**User's choice:** Ready.

---

## Claude's Discretion

- Exact test fixtures (DDL, literal interval/concat/cast/|| values) inside SampleByFillTest — planner picks smallest diff matching the empirical probe.
- `-ea` alias assert placement inside vs outside the new `continue` branch — cleanliness call.
- Plan count — single plan preferred per Phase 14/15 file-clustered convention; planner may split if the diff warrants.

## Deferred Ideas

- Option 2 (upstream canonicalization in `SqlOptimiser.rewriteSampleBy`) — captured as possible future hardening phase if classifier drifts again despite D-05.
- FILL(constant) multi-key inline-function regression test — add ad-hoc if a real-world report surfaces.
