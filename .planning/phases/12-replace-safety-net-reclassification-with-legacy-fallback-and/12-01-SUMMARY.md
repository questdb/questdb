---
phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
plan: 01
subsystem: sql
tags: [sample-by, fill-prev, optimizer-gate, fast-path, retro-fallback]

# Dependency graph
requires:
  - phase: 11-hardening-review-findings-fixes-and-missing-test-coverage
    provides: current safety-net shape in SqlCodeGenerator, existing Tier 1 gate, PREV(key_col) runtime path that plan 12-01 preserves
provides:
  - FallbackToLegacyException.java (singleton checked-exception signal)
  - QueryModel.stashedSampleByNode field + getter/setter + clear() reset
  - IQueryModel get/setStashedSampleByNode interface methods
  - QueryModelWrapper delegation for the new getter and UnsupportedOperationException setter
  - SqlOptimiser.rewriteSampleBy writes the stash before clearing sampleBy
  - SqlOptimiser.isUnsupportedPrevType rejects LONG128 and INTERVAL
  - SqlOptimiser.hasPrevWithUnsupportedType resolves PREV(alias) aggregate sources and blocks unsupported arg types
affects:
  - 12-03 (needs FallbackToLegacyException and stashedSampleByNode for retro-fallback at codegen)
  - 12-04 (grammar and negative tests in SampleByFillTest)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Singleton checked-exception for zero-allocation control-flow fallback (fillInStackTrace override)"
    - "Stash-and-restore on shared QueryModel for pre-rewrite state (mirror of fillOffset field shape)"
    - "Tier 1 optimizer gate with cross-column alias resolution against model/nested alias maps"

key-files:
  created:
    - core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java
  modified:
    - core/src/main/java/io/questdb/griffin/model/QueryModel.java
    - core/src/main/java/io/questdb/griffin/model/IQueryModel.java
    - core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java
    - core/src/main/java/io/questdb/griffin/SqlOptimiser.java

key-decisions:
  - "FallbackToLegacyException extends SqlException (same package, default constructor accessible via super()) rather than falling back to plain Exception. Keeps generateFill's throws clause unchanged."
  - "Singleton INSTANCE with fillInStackTrace() override - no per-throw allocation, no stack walk. Exception carries no position/message because it is never surfaced to users."
  - "stashedSampleByNode nulled inside clear() (not inside clearSampleBy()) in the sample-by/fill cluster so the state is reset even if callers bypass clearSampleBy."
  - "Tier 1 cross-column gate preserves PREV(key_col): LITERAL-source branch passes through regardless of the key column type, because the runtime reads key values from keysMapRecord rather than from the prev snapshot."

patterns-established:
  - "Sentinel exception + mutable-model-stash pattern for codegen-to-optimizer retro-fallback"
  - "Strict alphabetical insertion discipline across field, clear(), getter, setter, and interface method slots"

requirements-completed: [PTSF-02, PTSF-04, CONTEXT-item-1, CONTEXT-item-2, D-02]

# Metrics
duration: 12min
completed: 2026-04-17
---

# Phase 12 Plan 01: Scaffolding - Fallback Exception, Stash Field, Tier 1 Gate Summary

**Singleton FallbackToLegacyException + stashedSampleByNode plumbing on QueryModel/IQueryModel/QueryModelWrapper + LONG128/INTERVAL gap closure + cross-column PREV(alias) aggregate-source resolution in the Tier 1 optimizer gate**

## Performance

- **Duration:** ~12 min
- **Started:** 2026-04-17T10:18:18Z
- **Completed:** 2026-04-17T10:31:00Z
- **Tasks:** 3
- **Files modified:** 4 (+1 created)

## Accomplishments

- New `FallbackToLegacyException` singleton ready for codegen-to-caller fallback (consumers land in plan 12-03).
- `QueryModel.stashedSampleByNode` field wired through the field block, `clear()`, getter/setter, interface methods, and the wrapper.
- `SqlOptimiser.rewriteSampleBy` now preserves the original stride node on the nested model via `setStashedSampleByNode(sampleBy)` immediately before `setSampleBy(null)`.
- Tier 1 gate catches `PREV` on `LONG128` and `INTERVAL` aggregates, closing the asymmetry against `SqlCodeGenerator.isFastPathPrevSupportedType`.
- Cross-column `PREV(alias)` now resolves the alias against the output model. When the alias refers to an aggregate column on a LITERAL source argument, the gate walks to the base column's type on the nested model and blocks unsupported source types. `PREV(key_col)` and expression-arg sources pass through as intended.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add FallbackToLegacyException and QueryModel.stashedSampleByNode plumbing** - `1370c9528a` (feat)
2. **Task 2: Stash original sampleByNode inside rewriteSampleBy** - `26f2bc985e` (feat)
3. **Task 3: Tier 1 optimizer gate - LONG128/INTERVAL and cross-col PREV(alias) resolution** - `2ba6c4d1a9` (feat)

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java` (NEW) - Singleton checked exception extending `SqlException`; `fillInStackTrace()` is overridden to return `this` so no stack is walked on the rare fallback path.
- `core/src/main/java/io/questdb/griffin/model/QueryModel.java` - Added `stashedSampleByNode` field between `standaloneUnnest` and `tableId` (line 185), `stashedSampleByNode = null;` reset in the sample-by/fill cluster inside `clear()` (line 471), `getStashedSampleByNode()` between `getShowKind` and `getTableId`, `setStashedSampleByNode(ExpressionNode)` between `setStandaloneUnnest` and `setTableId`.
- `core/src/main/java/io/questdb/griffin/model/IQueryModel.java` - Added `ExpressionNode getStashedSampleByNode();` between `getShowKind` and `getTableId`, `void setStashedSampleByNode(ExpressionNode);` between `setStandaloneUnnest` and `setTableId`.
- `core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java` - Delegated getter to `delegate.getStashedSampleByNode()`; setter throws `UnsupportedOperationException` matching the wrapper's read-only contract.
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` - Stash write `nested.setStashedSampleByNode(sampleBy);` in `rewriteSampleBy` between the `setFillOffset` block and `setSampleBy(null)` (line 8441). Tier 1 widen: `LONG128` and `INTERVAL` added to the `isUnsupportedPrevType` switch. Cross-col resolution: replaced the 4-line PREV(alias) skip with a resolver that walks `model.getAliasToColumnMap()` then `nested.getAliasToColumnMap()` when the source AST is a FUNCTION-on-LITERAL aggregate.

## Decisions Made

- **Sentinel exception shape.** `extends SqlException` chosen over plain `Exception`. The parent class has a default (implicit) constructor accessible from the same package, so `super()` compiles cleanly. Staying inside the `SqlException` hierarchy means callers do not need a new `throws` on any method that already throws `SqlException`.
- **Singleton + no stack trace.** `FallbackToLegacyException.INSTANCE` is the only allocated instance; `fillInStackTrace()` returns `this` to skip the JVM's expensive stack walk. The exception is never surfaced to users so carrying no position/message is intentional.
- **Stash reset in `clear()` not `clearSampleBy()`.** `clear()` is the authoritative reset the pool uses when recycling a QueryModel. Adding the null there protects against callers that touch only `clearSampleBy()` or that never call `clearSampleBy()` at all.
- **Preserve PREV(key_col).** When `srcAst.type == LITERAL` (the source is a key or timestamp column in the output model), the new Tier 1 gate intentionally does NOT call `isUnsupportedPrevType`. Key columns are handled by a dedicated runtime path that reads `keysMapRecord` rather than the prev snapshot, so type constraints do not apply - see CONTEXT.md Accepted Shapes D-05 / PREV(key_col). The comment block in-place documents this.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] LITERAL-source PREV(alias) branch was blocking PREV(key_col) on SYMBOL / VARCHAR keys**

- **Found during:** Task 3 (first test run: `testFillPrevOfSymbolKeyColumn` and `testFillPrevOfVarcharKeyColumn` regressed).
- **Issue:** The plan's literal `<action>` code for Task 3 resolved `srcAst.type == LITERAL` aliases on the nested model and returned `true` (block) whenever the resolved type was in `isUnsupportedPrevType`. But `PREV(k)` where `k` is a SYMBOL or VARCHAR key column is an accepted fast-path shape - the runtime reads key values directly from `keysMapRecord`, bypassing the prev snapshot, so type is irrelevant. Applying the gate's type rule to key columns regressed two existing green tests and broke the documented CONTEXT.md D-05 accepted shape.
- **Fix:** Dropped the LITERAL-source branch from the gate. When the alias resolves to a LITERAL (key or timestamp column), the gate passes through; only the FUNCTION-on-LITERAL (aggregate) branch keeps its type check. An in-place comment documents the reasoning. 12-RESEARCH.md Pitfall 4 had already flagged that the two tests must continue to pass.
- **Files modified:** `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (lines 520-550 inside `hasPrevWithUnsupportedType`).
- **Verification:** `mvn -pl core -Dtest=SampleByFillTest test` - 52/52 green. `mvn -pl core -Dtest=SampleByTest test` - 302/302 green.
- **Committed in:** `2ba6c4d1a9` (Task 3 commit; the deviation was fixed before the commit landed so the commit already carries the corrected gate shape).

---

**Total deviations:** 1 auto-fixed (1 bug).
**Impact on plan:** The deviation preserves a pre-existing accepted grammar shape that the plan's `<action>` text would have regressed. No scope creep - the correction is smaller code (one branch removed) than the plan spec. The plan's `must_haves.truths` statement "hasPrevWithUnsupportedType resolves PREV(alias) against the nested model and blocks when the source column type is unsupported" still holds for aggregate sources, which is the intent of Tier 1; the key-column branch was always supposed to pass through per CONTEXT.md D-05 and Pitfall 4.

## Issues Encountered

- **Plan's reference to a package-private `SqlException(int position)` constructor.** The plan's action text suggested `super(0)` against a `SqlException(int position)` constructor and offered a plain `Exception` fallback. In practice `SqlException` only has the implicit default constructor (no explicit `(int)` form anywhere in the file). Used `super()` with no arguments; the class is in the same package, so the default constructor is accessible. No fallback to `extends Exception` required.
- **`QueryModelWrapper` was not called out in Task 1's files-list.** The interface addition required two new methods on `QueryModelWrapper` as well, since it implements `IQueryModel`. Added the getter delegation and setter UnsupportedOperationException in the same commit as the other Task 1 changes. Mentioning this in the plan would have saved one iteration.

## User Setup Required

None - scaffolding-only plan. No external services, no runtime config.

## Next Phase Readiness

- `FallbackToLegacyException.INSTANCE` is importable from anywhere inside `io.questdb.griffin`. Plan 12-03 can throw it from `SqlCodeGenerator.generateFill` and catch it at the three `generateSelectGroupBy` call sites.
- `QueryModel.getStashedSampleByNode()` returns the original stride node for any SAMPLE BY FILL query that passed through `rewriteSampleBy`. Plan 12-03's catch-site handler can restore it via `nested.setSampleBy(...)` and re-dispatch to `generateSampleBy`.
- Tier 1 gate is strictly tighter than before for aggregate PREV sources and unchanged for key-column PREV. No further gate changes planned for phase 12 (per CONTEXT.md D-02).

## Self-Check: PASSED

- FOUND: core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java
- FOUND: core/src/main/java/io/questdb/griffin/model/QueryModel.java (modified)
- FOUND: core/src/main/java/io/questdb/griffin/model/IQueryModel.java (modified)
- FOUND: core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java (modified)
- FOUND: core/src/main/java/io/questdb/griffin/SqlOptimiser.java (modified)
- FOUND: 1370c9528a (Task 1)
- FOUND: 26f2bc985e (Task 2)
- FOUND: 2ba6c4d1a9 (Task 3)

---

*Phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and*
*Completed: 2026-04-17*
