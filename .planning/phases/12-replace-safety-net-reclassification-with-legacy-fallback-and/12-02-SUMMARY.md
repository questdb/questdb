---
phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
plan: 02
subsystem: sql
tags: [sample-by, fill-prev, explain-plan, alphabetization, defensive-guard]

# Dependency graph
requires:
  - phase: 11-hardening-review-findings-fixes-and-missing-test-coverage
    provides: current SampleByFillRecordCursorFactory shape with 36 FillRecord getters, FQN type usages, and fill= plan emission only for hasPrevFill
provides:
  - SampleByFillRecordCursorFactory.hasExplicitTo demotion line after the hasExplicitTo/maxTimestamp assignment when maxTimestamp == LONG_NULL
  - SampleByFillRecordCursorFactory.toPlan emits exactly one of fill=null, fill=prev, or fill=value per invocation
  - SampleByFillRecordCursorFactory.hasAnyConstantFill private helper on the outer class
  - SampleByFillRecordCursorFactory outer class fillModes field, wired from constructor
  - FillRecord getters in strict alphabetical order (36 methods)
  - SampleByFillCursor private members alphabetized within kind (static vs instance)
  - readColumnAsLongBits moved from interleaved instance methods block into the static block
  - FillRecord signatures use plain class names instead of FQN paths
  - Import block alphabetized within each package group
affects:
  - 12-04 (assertPlanNoLeakCheck sites in SampleByFillTest and ExplainPlanTest need the new fill= attribute in their expected plan text)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Defensive runtime-null guard after object-identity singleton comparison (catch bind variables and null-returning functions)"
    - "Unconditional plan attribute emission for user-visible mode distinctions (fill=null vs fill=value vs fill=prev vs no fill attr)"
    - "NullConstant instanceof check as the 'real constant supplied' distinguisher across the constantFills list"

key-files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java

key-decisions:
  - "hasAnyConstantFill predicate uses 'f != null && !(f instanceof NullConstant)' as the 'real user constant supplied' distinguisher. NullConstant.NULL is the fill-slot sentinel for 'no constant here' (set by generateFill when the slot is FILL_KEY, FILL_PREV_SELF, or cross-col PREV). A user-supplied FILL(<expr>) where <expr> is 'null' also resolves to NullConstant, so it legitimately registers as no real constant and toPlan falls through to the else branch and emits fill=null. This keeps FILL(NULL) visually indistinguishable from FILL() in the plan, which matches user intent."
  - "fillModes field added to the outer SampleByFillRecordCursorFactory class. The plan's action text calls for hasAnyConstantFill to iterate fillModes, but the field only existed on the inner SampleByFillCursor before this plan. Wiring it through the outer constructor (already a parameter) and retaining it as a final field on the outer class is a minimal, surgical extension consistent with the existing constantFills field shape."
  - "readColumnAsLongBits static method placed immediately after the three private static final int constants at the top of SampleByFillCursor. This is the cleanest 'static group' placement per CLAUDE.md 'kind-grouped' rule, even though FillRangeRecordCursor has no static methods at all (not a contradictory precedent, just a different shape)."

patterns-established:
  - "fillModes as outer-class field for toPlan observability alongside inner-cursor runtime use"
  - "Strict alphabetical method ordering within FillRecord (36 getters) and SampleByFillCursor private instance methods (8 methods)"

requirements-completed: [CONTEXT-item-4, CONTEXT-item-15, CONTEXT-item-16, CONTEXT-item-18, CONTEXT-item-19, CONTEXT-item-20]

# Metrics
duration: 14min
completed: 2026-04-17
---

# Phase 12 Plan 02: SampleByFillRecordCursorFactory file-local cleanups Summary

**Defensive LONG_NULL guard in hasExplicitTo, unconditional fill= in toPlan, alphabetization of FillRecord getters and SampleByFillCursor members, FQN-to-plain-import replacement, and import-block alphabetization - all inside SampleByFillRecordCursorFactory.java**

## Performance

- **Duration:** ~14 min
- **Started:** 2026-04-17T10:34:57Z
- **Completed:** 2026-04-17T10:49:14Z
- **Tasks:** 3
- **Files modified:** 1

## Accomplishments

- `SampleByFillCursor.initialize()` now demotes `hasExplicitTo` to `false` when the runtime-evaluated TO expression returns `LONG_NULL`. The previous object-identity check against `TimestampConstantNull` missed `TO null::timestamp`, bind variables, and functions returning null at runtime; those paths now terminate fill emission correctly instead of running up to `Long.MAX_VALUE`.
- `SampleByFillRecordCursorFactory.toPlan` emits exactly one of `fill=null`, `fill=prev`, or `fill=value` for every invocation. The new `hasAnyConstantFill()` private helper distinguishes "real constant supplied" from "NULL placeholder" by checking `instanceof NullConstant`.
- The `fillModes` `IntList` is now an outer-class field, wired through the existing constructor parameter, so `hasAnyConstantFill` can iterate it from `toPlan`.
- FillRecord's 36 getters are in strict alphabetical order; pure mechanical reorder, no body edits.
- SampleByFillCursor's private members now group by kind: 3 static fields, 1 static method (`readColumnAsLongBits`), final instance fields, mutable instance fields, constructor, 8 public @Override methods (already alphabetical), 8 private instance methods in strict alphabetical order (`emitNextFillRow`, `fillMode`, `hasKeyPrev`, `initialize`, `of`, `prevValue`, `savePrevValues`, `updatePerKeyPrev`), followed by the two inner classes `FillRecord` and `FillTimestampConstant`.
- FillRecord signatures use plain class names (`ArrayView`, `BinarySequence`, `Decimal128`, `Decimal256`, `Long256`, `CharSink`, `Utf8Sequence`) matching the sibling `FillRangeRecordCursorFactory` style.
- Import block is alphabetical within each package group; `GeoHashes` moved after `ColumnType`, `Decimals` moved before `IntList`.

## Task Commits

Each task was committed atomically:

1. **Task 1: hasExplicitTo LONG_NULL guard + toPlan fill= unconditional** - `3fac476764` (feat)
2. **Task 2: Alphabetize FillRecord getters (pure reorder)** - `21913b42d6` (refactor)
3. **Task 3: Alphabetize SampleByFillCursor members, FQN to plain imports, import block sort** - `7fae166407` (refactor)

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` - All three sub-changes landed in this single file per the plan's deliberate single-file scope.

## Decisions Made

- **`hasAnyConstantFill` predicate shape.** Used `f != null && !(f instanceof NullConstant)` as the "real user-supplied constant" distinguisher. Every `constantFills[col]` slot is either `NullConstant.NULL` (for FILL_KEY, FILL_PREV_SELF, cross-col PREV, or user-supplied `FILL(NULL)`) or a user-supplied `ConstantFunction` (for `FILL(<literal>)`). The instanceof check is O(1), zero-allocation, and explicit. A simpler `f != NullConstant.NULL` identity check would be slightly faster, but SqlCodeGenerator does not consistently supply the exact singleton - `FILL(NULL)` resolves through the SQL literal parser, which may build a fresh NullConstant instance; instanceof is the safe portable form.
- **`fillModes` field on outer class.** The plan's action text required `hasAnyConstantFill` to iterate `fillModes` and correlate with `constantFills`. The outer class already stores `constantFills` as a `private final` field but was not storing `fillModes`. Added the field in the alphabetical slot between `cursor` and `fromFunc`, assigned it from the already-existing constructor parameter. No new constructor argument, no API change.
- **`readColumnAsLongBits` placement.** Placed immediately after the three `private static final int` slot constants at the top of `SampleByFillCursor`, before instance fields. This matches CLAUDE.md's "grouped by kind (static vs instance) and visibility, sorted alphabetically" rule - there is only one private static method and it sorts alphabetically in its group of one.
- **Constructor ordering inside `SampleByFillCursor.this`-assignment block.** Left as-is - the constructor body is an ordered sequence of assignments, not a member-declaration block. CLAUDE.md's alphabetization rule applies to member declarations, not to statement ordering inside method bodies.
- **Did NOT reorder the `SampleByFillCursor.this`-field assignments inside the outer constructor.** Outer-class constructor mirrors the same pragma: statements, not declarations.

## Deviations from Plan

### Auto-fixed Issues

None - all three tasks executed exactly as the plan specified. The plan's action text for Task 1 referenced `fillModes` on the outer class as if it already existed. The plan's `<action>` block for Task 1 says `iterate fillModes` in the hasAnyConstantFill body, but the only `fillModes` field was on `SampleByFillCursor` (inner). I added the outer-class `fillModes` field as a minimal mechanical supporting change (not a functional deviation). This is consistent with the plan's must_haves.truths which only specify behavior, not storage layout.

---

**Total deviations:** 0.
**Impact on plan:** None - every acceptance criterion met.

## Issues Encountered

- **`testExplainFillRange` now fails** (SampleByFillTest:~3800-3830). Expected - the test asserts the plan text omits `fill=null` for a FILL(NULL) range query. After Task 1, the plan now includes `fill: null`. The plan text explicitly documents this as expected: "Test failures from downstream `assertPlanNoLeakCheck` assertions are expected - plan 04 updates them." No other tests regressed.
- **`test case-insensitive vs case-sensitive ordering rule for getIPv4 vs getInt`.** The plan's Task 2 action text says "case-insensitive ASCII comparison" but then explains the ordering via case-sensitive code points (`P`=0x50 < `n`=0x6E). Used the plan's explicit target sequence verbatim, as the plan instructed ("Copy this ordering from RESEARCH.md verbatim - do not re-derive"). Internal rule-text inconsistency noted, explicit target sequence is authoritative.

## User Setup Required

None.

## Downstream Test Impact (for plan 04)

The following `assertPlanNoLeakCheck` call sites will need refreshed expected plan text to account for the new `fill=null | fill=prev | fill=value` attribute:

- `SampleByFillTest.testExplainFillRange` - confirmed failing after Task 1. Expected plan text needs a `fill: null` line between the `stride:` and the `Sort` child. Exactly one other `assertPlanNoLeakCheck` site in this test class mentions `fill: prev`, and the 50 remaining tests in the 52-test suite use `assertQueryNoLeakCheck` or `assertSql` and are unaffected.
- Any `ExplainPlanTest.java` assertions that cover SAMPLE BY FILL(...) - the plan calls out this file in CONTEXT.md item 20 but plan 02 does not touch the test class.

## Imports Added (FQN → plain conversion)

Per sub-change B of Task 3:

```java
import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
```

Plus the `NullConstant` import added in Task 1:

```java
import io.questdb.griffin.engine.functions.constants.NullConstant;
```

## Known Stubs

None - all changes are real functional code (Task 1) or pure mechanical reorders (Tasks 2 and 3).

## Self-Check: PASSED

- FOUND: core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java (modified)
- FOUND: 3fac476764 (Task 1)
- FOUND: 21913b42d6 (Task 2)
- FOUND: 7fae166407 (Task 3)
- VERIFIED: `grep -c 'sink\.attr("fill")' ... = 3` exactly (fill=null, fill=prev, fill=value)
- VERIFIED: `grep -n "if (maxTimestamp == Numbers.LONG_NULL) hasExplicitTo = false" ...` returns exactly one line (at line 553)
- VERIFIED: FillRecord contains 36 getters in strict alphabetical order (getArray through getVarcharSize)
- VERIFIED: `grep -c "io\.questdb\." ...` (body only, excluding imports) = 0
- VERIFIED: Import block is alphabetical; `Decimals` precedes `IntList`; `CairoConfiguration` precedes `ColumnType` precedes `GeoHashes`
- VERIFIED: `mvn -pl core compile` exits 0
- VERIFIED: `mvn -pl core -Dtest=SampleByFillTest test` = 51/52 tests pass; the single failing test (`testExplainFillRange`) is an `assertPlanNoLeakCheck` site refreshed in plan 04 per CONTEXT item 20
- VERIFIED: No `// ===` or `// ---` banner comments in the file

---

*Phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and*
*Completed: 2026-04-17*
