# Phase 1: Correctness - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-13
**Phase:** 01-correctness
**Areas discussed:** Sort algorithm fix, DirectArray state fix, Validation strategy, setEmpty() approach

---

## Sort Algorithm Fix

| Option | Description | Selected |
|--------|-------------|----------|
| Reuse GroupByDoubleList sort | GroupByDoubleList already has a correct dual-pivot quicksort with Dutch National Flag. Extract and reuse. | ✓ |
| Add three-way partition to existing Lomuto | Minimal change: add Dutch National Flag fallback when equal elements detected. | |
| Replace with quickselect entirely | For single-percentile: O(n) quickselect. For multi: sort once. Biggest change. | |

**User's choice:** Reuse GroupByDoubleList sort (Recommended)
**Notes:** Feeds directly into QUAL-01 (extract shared utility) in Phase 4.

---

## DirectArray State Fix

| Option | Description | Selected |
|--------|-------------|----------|
| Re-apply every call | ~30ns/call overhead. setType + setDimLen + applyShape on every non-null path. Simple and defensive. | ✓ |
| Move init, keep guard | Same cost but cleaner structure. Only allocate DirectArray once. | |
| Track dirty flag | More complex, saves ~30ns when not dirty. Risk of missed flag updates. | |

**User's choice:** Re-apply every call (Recommended)
**Notes:** User asked about overhead. Analysis showed ~30ns per call — negligible vs quickSelect work per group. On 1M groups = ~30ms total overhead.

---

## Validation Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Factory + runtime | Constness in factory (compile-time), range via getPercentileMultiplier (runtime). Matches ApproxPercentile. | ✓ |
| Factory only | All validation at creation. Can't catch invalid bind variables. | |
| You decide | Claude picks based on patterns. | |

**User's choice:** Factory + runtime (Recommended)
**Notes:** None.

---

## setEmpty() Approach

| Option | Description | Selected |
|--------|-------------|----------|
| Store 0L | Matches ApproxPercentile pattern exactly. Minimal change. | ✓ |
| You decide | Claude follows ApproxPercentile pattern. | |

**User's choice:** Store 0L (Recommended)
**Notes:** None.

---

## Claude's Discretion

- Exact error message wording for validation failures
- Whether to add constness check to group-by factories (not strictly required)

## Deferred Ideas

None — discussion stayed within phase scope.
