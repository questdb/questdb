# Phase 13: Migrate FILL(PREV) snapshots to rowId — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-17
**Phase:** 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
**Areas discussed:** Approach choice (rowId vs value buffering), Investigation depth, chain.clear() sequencing, Retro-fallback scope, SEED-002 defects scope, Test cherry-pick timing, ALIGN TO FIRST OBSERVATION scope

---

## Pre-discussion finding

Scouting the source branch `sm_fill_prev_fast_all_types` and sibling
`sm_fill_prev_keysmap_driven` surfaced that the roadmap's stated
approach (rowId) was **researched GO** but the actual implementation
on `sm_fill_prev_keysmap_driven` **abandoned rowId** (commit
`fe487c06a9`) and reverted to value buffering. This contradiction
between research verdict and implementation outcome was presented to
the user as the primary gray area before any other discussion.

---

## Approach choice (rowId vs value buffering)

| Option | Description | Selected |
|--------|-------------|----------|
| Borrow value buffering from keysmap_driven tip | Port sm_fill_prev_keysmap_driven @ 1424bd5216. Proven stable; abandons rowId per source branch's implementation verdict. | |
| Try rowId ourselves | Implement rowId + chain.clear() per research 4ebfa3243c. Research said GO; implementation may hit same issues source branch did. | ✓ |
| Investigate rowId abandonment first | Spawn researcher to read fe487c06a9 in depth before picking. | |
| Hybrid: f43a3d7057 base + selective keysmap_driven deltas | Start from value buffering, adopt deltas as needed. | |

**User's choice:** Try rowId ourselves (original roadmap).

**User directive:** "no value buffering. It must be rowId either from current sorted cursor or maybe another wrapper. Double-check first if it's really the problem, if confirmed - analyze other sorting options from existing mechanism in questdb, search for similar cases where group by or another unordered result is wrapped with some order"

**Notes:** User treats rowId as a principled architectural choice. Value buffering is explicitly ruled out, even as a mid-phase pivot. If SortedRecordCursor proves unsuitable, alternative sort-wrap mechanisms in QuestDB are to be surveyed — not fallback to value buffering.

---

## Investigation depth (before implementing)

| Option | Description | Selected |
|--------|-------------|----------|
| Read fe487c06a9 diff in full first | 1-2 hour investigation. Identify WHY chain.clear() wasn't sufficient. | ✓ |
| Just implement; debug as issues surface | Apply chain.clear(), implement rowId. Risk repeating source branch's path. | |
| Build instrumentation first | Add assertions in SortedRecordCursor for chain-offset inconsistency detection. | |

**User's choice:** Read fe487c06a9 diff in full first.

**Notes:** Investigation deliverable (per CONTEXT D-02) is a `13-INVESTIGATION.md` artifact with verdict:
- (a) chain.clear() sufficient — proceed
- (b) root cause identified + proposed additional fix — proceed if low-cost
- (c) SortedRecordCursor structurally unsuitable — survey alternatives in QuestDB codebase

---

## chain.clear() sequencing

| Option | Description | Selected |
|--------|-------------|----------|
| Ship as independent first commit of phase 13 | Load-bearing for rowId correctness. Backportable, independently revertable. | ✓ |
| Fold into fill-cursor rewrite commit | Single commit, harder to isolate if fix needs backport. | |
| Ship as separate PR before this phase | Slows phase 13 start; gets fix to master fastest. | |

**User's choice:** Ship as independent first commit of phase 13.

---

## SEED-001 disposition clarification

Before the retro-fallback question, the user asked why deletion of retro-fallback would discard SEED-001. Orchestrator clarified:

- **WR-01, WR-02, WR-03** are defects *inside* retro-fallback code. If retro-fallback is deleted, they become obsolete (code is gone).
- **WR-04** (precise chain-rejection position) is in `generateFill` grammar code, not retro-fallback. Survives.
- **Defect 3** (insufficient fill values grammar) is in `generateFill`. Survives.

SEED-001's own Re-sorting Checklist spells this out. The seed is not discarded wholesale; only the retro-fallback-dependent items drop.

---

## Retro-fallback scope (post-clarification)

| Option | Description | Selected |
|--------|-------------|----------|
| Delete retro-fallback if all types unlocked; ship WR-04 + Defect 3 in this phase | WR-01/02/03 obsolete; WR-04 + Defect 3 bundled since we're touching generateFill anyway. | ✓ |
| Delete retro-fallback if all types unlocked; defer WR-04 + Defect 3 | Minimal phase 13 surface; grammar items ship in small follow-up. | |
| Keep retro-fallback as belt-and-braces; ship all 5 SEED-001 items | Safety-first; rowId may have edge cases. | |
| Defer retro-fallback decision to mid-phase | Reassess once rowId works against 13 goal-state tests. | |

**User's choice:** Delete retro-fallback if all types unlocked; ship WR-04 + Defect 3 in this phase.

---

## SEED-002 cursor defects scope

| Option | Description | Selected |
|--------|-------------|----------|
| Verify absorbed by rowId, restore testSampleByFillNeedFix | Defect 2 likely absorbed; Defect 1 investigated. Follow-up phase if Defect 1 survives. | ✓ (with directive) |
| Defect 2 only; Defect 1 filed separately | Smaller scope; assumes Defect 1 is factory-plumbing and independent. | |
| Punt both to separate investigation phase | Smallest phase 13 surface. | |

**User's choice:** Option 1 BUT fully fix it.

**User directive:** "option 1 but fully fix it, end of this phase should be a full solution"

**Notes:** Phase 13 must end with BOTH defects resolved. Defect 1 investigation is mid-phase; if it turns out to be factory-plumbing-shaped, fix it here — do not punt to follow-up. `testSampleByFillNeedFix` restored to master's 3-row form in both assertions.

---

## Test cherry-pick timing

| Option | Description | Selected |
|--------|-------------|----------|
| After rowId rewrite, before retro-fallback cleanup | Progressive commits, clean bisection. | ✓ |
| Before rowId rewrite — red tests as pressure | TDD flavor; risk: red tests in intermediate commits. | |
| Bundle with retro-fallback cleanup | Less intermediate noise, less progressive signal. | |

**User's choice:** After rowId rewrite, before retro-fallback cleanup.

---

## ALIGN TO FIRST OBSERVATION scope

| Option | Description | Selected |
|--------|-------------|----------|
| Out of scope — SEED-003 handles later | FO is orthogonal to rowId; keep phase 13 focused. | ✓ |
| In scope — bundle with rowId | Same source branch; ~1185 LoC + 17 tests. Overloads phase. | |

**User's choice:** Out of scope — keep SEED-003 for later.

---

## Plan-B if investigation rules out rowId

| Option | Description | Selected |
|--------|-------------|----------|
| Pivot to value buffering, continue phase 13 | Ship chain.clear() standalone, implement value buffering from keysmap_driven. | |
| Stop at investigation, ship only chain.clear(), file new phase for value buffering | Minimal phase 13 if rowId unviable; new phase for value buffering. | |
| Abandon phase 13, pivot milestone | Re-evaluate whole approach at milestone level. | |

**User's choice:** None of the above — value buffering is never the answer.

**User directive:** "no value buffering. It must be rowId either from current sorted cursor or maybe another wrapper. Double-check first if it's really the problem, if confirmed - analyze other sorting options from existing mechanism in questdb, search for similar cases where group by or another unordered result is wrapped with some order"

**Translation for planner:** If SortedRecordCursor doesn't work, the investigation continues into other rowId-capable sort/wrap mechanisms in QuestDB. Value buffering is not a pivot option. Architectural escalation is the last resort if no rowId vehicle exists anywhere.

---

## Claude's Discretion

- Commit 2 structure (single rowId rewrite vs split scaffolding+deletion) — planner's call.
- Exact SqlException wording for WR-04 and Defect 3 — implementation time.
- Vehicle choice (SortedRecordCursor vs alternative) — decided by D-02 investigation.

## Deferred Ideas

- Value buffering — explicitly rejected.
- ALIGN TO FIRST OBSERVATION — SEED-003.
- PREV numeric widening — deferred from Phase 12 D-09.
- FILL(LINEAR) on fast path — roadmap level.
- Old `SampleByFillPrev*` cursor removal — separate phase.
- Two-token stride on fast path — roadmap level.
