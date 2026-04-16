# Phase 12: Replace safety-net reclassification with legacy fallback — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-16
**Phase:** 12-replace-safety-net-reclassification-with-legacy-fallback-and
**Areas discussed:** Grammar of FILL(PREV, PREV(...)), Legacy-fallback trigger mechanism, Optimizer gate inference scope, assertSql → assertQueryNoLeakCheck conversion scope

---

## Grammar of FILL(PREV, PREV(...))

### Bare PREV mixed with per-column list

| Option | Description | Selected |
|--------|-------------|----------|
| Keep & document | Bare PREV in per-column list means self-prev for that slot. Matches testFillPrevCrossColumnKeyed. Add grammar-spec comment in generateFill. | ✓ |
| Reject — bare PREV only alone | Error 'PREV must be the only fill value, use PREV(col) in per-column list' when size>1 and any slot is bare PREV. Would break testFillPrevCrossColumnKeyed. | |

**User's choice:** Keep & document.
**Notes:** Preserves existing test coverage and semantically-reasonable behavior. Requires one comment in generateFill near line 3417-3420.

### FILL(PREV(ts)) — designated timestamp column

| Option | Description | Selected |
|--------|-------------|----------|
| Reject with positioned error | SqlException at rhs.position: 'PREV cannot reference the designated timestamp column' | ✓ |
| Accept as no-op | Document as no-op; fill row still emits bucket timestamp. | |

**User's choice:** Reject with positioned error.

### FILL(PREV(a)) — self-reference

| Option | Description | Selected |
|--------|-------------|----------|
| Alias to FILL_PREV_SELF | When srcColIdx == col, set fillModes[col] = FILL_PREV_SELF. Same runtime behavior, one snapshot slot instead of two. | ✓ |
| Reject with positioned error | SqlException: 'PREV(a) references column a itself, use bare PREV for self-prev' | |
| Keep as-is | Accept the indirection. | |

**User's choice:** Alias to FILL_PREV_SELF.

### Chain handling (PREV(b) where b = PREV(a))

| Option | Description | Selected |
|--------|-------------|----------|
| Document as undefined, add regression test | Current loop only adds one hop. Defer correct chain semantics to future phase. | |
| Fix transitively now | Walk mode graph until fixed-point. Expands scope beyond phase 12. | |
| Reject chains at compile time | If PREV(b) references b and fillModes[b] is itself cross-col PREV, raise SqlException. | ✓ |

**User's choice:** Reject chains at compile time.

### Malformed PREV shapes — function arg, multi-arg, no-arg

| Option | Description | Selected |
|--------|-------------|----------|
| Single unified error | Detect any FUNCTION-shaped PREV that isn't (paramCount==1 && rhs.type==LITERAL), raise 'PREV argument must be a single column name' at fillExpr.position. | ✓ |
| Shape-specific errors | Three distinct messages for each malformed shape. | |

**User's choice:** Single unified error.
**Notes:** User pointed out paramCount > 2 was missing from the initial question; clarified to cover all three malformed shapes under one rule.

### FILL(PREV($1)) — bind variable

| Option | Description | Selected |
|--------|-------------|----------|
| Keep current 'column not found' | Existing positioned error is clear enough for rare case. | — (moot) |
| Add bind-var-specific error | Detect rhs.type==BIND_VARIABLE with dedicated message. | — (moot) |

**User's choice:** Question dropped — covered by unified rejection rule since BIND_VARIABLE is not LITERAL (verified at ExpressionNode.java:46,50).

### Source/target type mismatch

| Option | Description | Selected |
|--------|-------------|----------|
| Require tag match | ColumnType.tagOf(targetType) == ColumnType.tagOf(sourceType), else SqlException. Catches the garbage-output case. | ✓ |
| Require assignable | Allow numeric widening (INT→LONG, FLOAT→DOUBLE). Requires explicit getter conversion. | |
| Leave silent | Accept undefined output for mismatched types. | |

**User's choice:** Require tag match. Numeric widening deferred.

---

## Legacy-fallback trigger mechanism

### Approach for the codegen-side safety check

User re-framed the problem: "why do we need to care about aggregate types when groupBy already handles them?"

After investigation, established that:
- The fill cursor's PREV snapshot buffer is a `long[]` (8 bytes per slot) — constrains which types can be snapshotted.
- Types unsupported: UUID/LONG128/LONG256/DECIMAL128/256, STRING/VARCHAR/SYMBOL/BINARY/arrays, INTERVAL.
- Design docs (fill-fast-path-overview.md §9, fill-fast-path-design-review_v2.md §3, §8, §9) explicitly mandate "unsupported types fall back to old path", never "fail loud".

User then asked: "was it described in the previous planning materials, look through all the phases and all artefacts".

Scan results confirmed:
- Phase 7 CONTEXT designed two-layer defense with safety net throwing SqlException (per threat model T-07-03).
- Phase 11 changed the safety net to silently reclassify as FILL_KEY — this is the regression.
- Phase 12 CONTEXT.md (original draft) proposed legacy fallback.
- External design docs (`/Users/sminaev/projects/questdb/plans/fill-fast-path-overview.md` and `fill-fast-path-design-review_v2.md`) explicitly say fallback is the correct behavior; "fail loud" would be an undocumented scope reduction.

User asked whether SqlException would surface to the user. Traced through the code: under "pure fail-loud", the exception propagates through `generateSelectGroupBy` to the user. Concrete example queries enumerated (`sum(price * qty)` with DECIMAL128, `first(substr(city, 0, 3))`, UUID aggregates in CTE contexts) — all legitimate queries master handles correctly, which would become user-facing errors.

| Option | Description | Selected |
|--------|-------------|----------|
| Fall back to legacy (stashed model retro-fallback) | Stash sampleByNode in rewriteSampleBy, try/catch in generateSelectGroupBy, restore + generateSampleBy on catch. User never sees error for master-supported queries. ~30 LOC. | ✓ |
| Fail loud with error | SqlException surfaces to user. Simplest code but scope regression not mentioned in any design doc. | |
| Fail loud for bugs only, fall back otherwise | Combined approach — retro-fallback in production, assert in tests. | |

**User's choice:** Legacy fallback.
**Notes:** Decision anchored in external design docs; "fail loud" would have been a user-facing regression.

---

## Optimizer gate inference scope

### How much to tighten the gate

User initially asked how strict the gate needs to be if it's the single source of truth. Investigation revealed:
- `QueryColumn.getColumnType()` returns output type at rewriteSampleBy time — but only for base-table columns. Aggregate output types aren't resolved until `rewriteSelectClause` runs AFTER `rewriteSampleBy`.
- Existing `isUnsupportedPrevAggType` uses argument type (base-table column), applying implicit aggregate rules.
- Pipeline restructuring (moving rewriteSelectClause earlier) would be risky.

User asked whether the optimizer runs multiple passes. Investigation: no, single-pass. Pipeline restructuring ruled out.

Since retro-fallback now handles residue, gate stays a performance optimization, not a correctness mechanism.

| Option | Description | Selected |
|--------|-------------|----------|
| Tier 1 only — mandatory gaps | Cross-col PREV(alias) resolution + LONG128/INTERVAL in isUnsupportedPrevType. ~15 LOC. | ✓ |
| Tier 1 + simple aggregate rules | Add type-promotion inference for sum/avg/first/last/min/max/count with LITERAL args. ~30 LOC. | |
| Tier 1 + expression walker | Also walk simple arithmetic/cast expressions. ~60 LOC. | |

**User's choice:** Tier 1 only.
**Notes:** Retro-fallback makes deeper gate inference unnecessary. Complex expressions that would need inference are rare in real workloads.

---

## assertSql → assertQueryNoLeakCheck conversion scope

### Selection criterion

| Option | Description | Selected |
|--------|-------------|----------|
| Rule-based: convert all fast-path tests | Rule: 'if the test exercises the new fast-path fill cursor and supportsRandomAccess=false or size()=-1 is the correct expected value, convert.' Skip legacy-plan, mid-test-DDL, storage tests. | ✓ |
| Ad-hoc: list specific tests in PLAN | Enumerate test names. More planning work; less ambiguity. | |
| Minimal: only tests that assert random-access/size | Smallest diff; most stay assertSql. | |

**User's choice:** Rule-based.

---

## Claude's Discretion

- Exception vs sentinel for the fallback signal mechanism — both work; plan decides based on what's cleanest to thread through the three call sites.
- Error-message wording for rejected grammar shapes — use positioned `SqlException.$(position, "message").put(...)` style; exact wording at implementation time.

## Deferred Ideas

- Numeric-widening conversion for `PREV(src)` / target type mismatches — deferred.
- Tier 2 aggregate-rule inference at the gate — revisit if real workloads show high fallback rate.
- CASE / function-call expression walker at the gate — same rationale.
- PREV for STRING/SYMBOL/VARCHAR — tracked separately on `sm_fill_prev_fast_all_types` branch.
- `FILL(LINEAR)` on fast path — per project roadmap.
- Old cursor path removal — postponed.
- `ALIGN TO FIRST OBSERVATION` on fast path — postponed.
- Two-token stride syntax on fast path — postponed.
