---
phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
reviewed: 2026-04-20T17:00:00Z
depth: standard
files_reviewed: 8
files_reviewed_list:
  - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
  - core/src/main/java/io/questdb/griffin/SqlOptimiser.java
  - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
  - core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java
  - core/src/test/java/io/questdb/test/griffin/RecordCursorMemoryUsageTest.java
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java
findings:
  critical: 0
  warning: 2
  info: 6
  total: 8
status: issues_found
---

# Phase 14: Code Review Report

**Reviewed:** 2026-04-20T17:00:00Z
**Depth:** standard
**Files Reviewed:** 8
**Status:** issues_found

## Summary

The phase ships four grouped fixes (M-1, M-2, M-3, M-4, M-7, M-8, M-9, Mn-13)
plus the D-18 RCM chain-walk assertion and the D-16 ghost-test rename. The
code is correct in all cases reviewed, and the regression tests exercise the
intended branches. No critical bugs, no security issues, no data-loss risks.

Two warnings concern a latent native-memory leak in
`SortedRecordCursorFactory` that the M-7 fix does not fully close on its own
(the `rankMaps` return value of `SortKeyEncoder.createRankMaps` is not freed
if the subsequent allocation of `SortedRecordCursor` throws), and a minor
contract-exposure issue in the cursor's constructor catch regarding the
inner `rankMaps` ownership. Both are edge cases under JVM OOM conditions,
not normal query-compile paths, and none of them regress the M-7 fix.

Info-level items cover convention drift: non-alphabetical field ordering in
`SortedRecordCursorFactory` after the M-7 fix, a dead `keyColIndices` field
in `SampleByFillCursor`, dead code in `generateFill` that was flagged but
left to a future cleanup pass (Mn-12), a boolean local that does not start
with `is`/`has`, and a subtle narrowing-cast pattern in `FillRecord.getByte`
/ `getShort` that can silently overflow a user-supplied `FILL(...)` constant
(pre-existing, matches `IntFunction` behavior). Finally, the `SampleByFillCursor`
reference to `prevRecord` is only safe because `SortedRecordCursor.getRecordB()`
is stable post-`buildChain` — the code comments this assumption explicitly,
so this is a documentation/fragility note, not a bug.

## Warnings

### WR-01: `rankMaps` not freed if constructor throws after `SortKeyEncoder.createRankMaps` succeeds

**File:** `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java:74`

**Issue:** On line 74,

```java
this.cursor = new SortedRecordCursor(chain, comparator, SortKeyEncoder.createRankMaps(metadata, sortColumnFilter));
```

Java evaluates the arguments left-to-right, so `SortKeyEncoder.createRankMaps`
runs first and allocates an `ObjList<DirectIntList>` with native-backed
`DirectIntList` entries. Then `new SortedRecordCursor(...)` runs. If
`SortedRecordCursor`'s constructor throws between evaluation of the third
argument and successful completion of the constructor body (e.g., JVM OOM
during `new SortedRecordCursor(...)`, or any future change that adds a
throwing step to the cursor constructor), the returned `rankMaps` become
unreachable.

The catch block at lines 75-82 frees `chain` but does not free the
`rankMaps` because it has no reference to them — they were a transient
argument. Each `DirectIntList` holds native memory, so this leaks native
memory under the OOM path.

The M-7 fix correctly handles the `base` field, but does not close this
orphaned-rankMaps hole. It is a distinct, narrower issue that surfaces
only under the same constructor-throw path M-7 guards against.

**Fix:** Hoist the `createRankMaps` result into a local before passing it
to `new SortedRecordCursor(...)`, and free it in the catch block:

```java
RecordTreeChain chain = null;
ObjList<DirectIntList> rankMaps = null;
try {
    chain = new RecordTreeChain(...);
    rankMaps = SortKeyEncoder.createRankMaps(metadata, sortColumnFilter);
    this.base = base;
    this.cursor = new SortedRecordCursor(chain, comparator, rankMaps);
    // ownership transferred to cursor
    rankMaps = null;
} catch (Throwable th) {
    this.base = null;
    Misc.freeObjList(rankMaps);
    Misc.free(chain);
    close();
    throw th;
}
```

`SortedLightRecordCursorFactory.java:62` has the same code shape; the
same fix applies there too.

---

### WR-02: `SampleByFillCursor.of()` comment overstates the contract of `Function.init()`

**File:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:181-185`

**Issue:**

```java
try {
    cursor.of(baseCursor, executionContext);
    return cursor;
} catch (Throwable th) {
    // Defensive: cursor.of() currently only calls Function.init() and toTop(),
    // which do not throw on the happy path; kept to maintain the cursor-contract.
    cursor.close();
    throw th;
}
```

`Function.init()` can throw `SqlException` — it is part of its declared
signature and is specifically how runtime-constant validation surfaces
errors (e.g., a FROM bind variable whose value fails a bound check).
Phase 12/13 tests already exercise `toFunc` returning `LONG_NULL`; a
similar flow for a bind variable that fails to coerce could throw
`SqlException` from `fromFunc.init` or `toFunc.init`. The comment claims
these do "not throw on the happy path" which is the normal case but
implies the catch is theoretical. The catch is load-bearing, not
defensive. Misleading comment creates pressure for a future maintainer
to remove the catch as dead code.

Also note `cursor.close()` inside the catch only nulls `baseCursor`
(see `SampleByFillCursor.close` at line 337-339); it does not undo
`Function.init` side effects. This is acceptable because `Function.init`
is idempotent on the same context, but worth a short note rather than
the current `// Defensive: ...` wording.

**Fix:** Replace the comment with one that reflects the actual contract:

```java
} catch (Throwable th) {
    // Function.init() can raise SqlException for runtime-constant
    // validation errors; release cursor state so the caller's frame can
    // propagate the exception cleanly. close() nulls baseCursor; init
    // side effects on the FROM/TO functions are idempotent.
    cursor.close();
    throw th;
}
```

## Info

### IN-01: Non-alphabetical field order in `SortedRecordCursorFactory` after M-7 fix

**File:** `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java:42-44`

**Issue:** Fields currently declared as

```java
private final SortedRecordCursor cursor;
private final ListColumnFilter sortColumnFilter;
private RecordCursorFactory base;
```

Alphabetical would be `base`, `cursor`, `sortColumnFilter`. CLAUDE.md
mandates alphabetical ordering within visibility+kind groups. The M-7
fix (de-finalized `base`) placed the field at the end rather than
reinserting it in the alphabetical position. Low risk of functional
regression; violates convention.

**Fix:** Move `base` to the top of the field block:

```java
private RecordCursorFactory base;
private final SortedRecordCursor cursor;
private final ListColumnFilter sortColumnFilter;
```

---

### IN-02: Dead `keyColIndices` field in `SampleByFillCursor`

**File:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:263, 322`

**Issue:** `private final IntList keyColIndices` at line 263 is assigned
at line 322 and read only in the constructor's loop at line 331-333 to
initialize `outputColToKeyPos`. After construction, the field is never
read again — it is dead. The CONTEXT.md already lists this under Mn-12
as a deferred cleanup, but the field keeps a reference to the caller-owned
IntList alive for the lifetime of the cursor, which is a small
housekeeping concern.

**Fix:** Drop the field; use the constructor parameter directly:

```java
// in constructor (replace lines 322, 331-333):
this.outputColToKeyPos = new int[columnCount];
Arrays.fill(outputColToKeyPos, -1);
for (int i = 0, n = keyColIndices.size(); i < n; i++) {
    outputColToKeyPos[keyColIndices.getQuick(i)] = keyPosOffset + i;
}
// no this.keyColIndices assignment; no field declaration
```

---

### IN-03: `keyPosOffset` could be a local constant

**File:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:264, 328, 332`

**Issue:** `private final int keyPosOffset` is assigned to `3` in the
constructor and used exactly once (line 332) in the same constructor.
It has no callers outside `new SampleByFillCursor(...)` and its value
is documented as "fixed 3-LONG value header". Keeping it as a field
clutters the cursor's state without serving runtime behavior.

**Fix:** Replace the field with either a local variable or a class-level
`private static final int` constant (matching the existing
`HAS_PREV_SLOT`, `KEY_INDEX_SLOT`, `PREV_ROWID_SLOT` constants at
lines 251-253).

---

### IN-04: Dead branch in `generateFill` at `SqlCodeGenerator.java:3297`

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3297-3300`

**Issue:**

```java
if (fillValues.size() == 0 || (fillValues.size() == 1 && isNoneKeyword(fillValues.getQuick(0).getName()))) {
    Misc.freeObjList(fillValues);
    return groupByFactory;
}
```

The loop at lines 3282-3295 already returns early whenever any expression
has `isNoneKeyword(expr.token)`. By the time execution reaches line 3297,
no element of `fillValues` can be a NONE placeholder. The
`isNoneKeyword(fillValues.getQuick(0).getName())` branch therefore cannot
fire. The `fillValues.size() == 0` branch is also unreachable because
`fillValues` size tracks `fillValuesExprs.size()` and we bailed out
earlier if `fillValuesExprs == null`.

**Fix:** Remove the dead guard or tighten its comment to document why
it is a defensive belt-and-suspenders (the current behavior is
structurally redundant given the loop's early return).

---

### IN-05: Convention drift on local boolean `needsExactTypeMatch`

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3554`

**Issue:** `final boolean needsExactTypeMatch = ...` uses a `needs`
prefix rather than `is`/`has`. CLAUDE.md says "always use the is... or
has... prefix, as appropriate". This is a new local introduced by
Phase 14 Plan 01 (M-9). Semantically `hasExactTypeMatchRequirement` or
`isExactTypeMatchRequired` would match the convention; `needsX` does
not.

**Fix:** Rename to `isExactTypeMatchRequired` or
`hasExactTypeMatchRequirement`. Matches the sibling
`isTypeCompatible` on the next line.

---

### IN-06: Silent narrowing cast in `FillRecord.getByte` and `getShort`

**File:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:765, 1097`

**Issue:** The FILL_CONSTANT branch for `getByte` and `getShort` calls
`.getInt(null)` then casts to the target width:

```java
if (mode == FILL_CONSTANT) return (byte) constantFills.getQuick(col).getInt(null);
// ...
if (mode == FILL_CONSTANT) return (short) constantFills.getQuick(col).getInt(null);
```

A user-supplied `FILL(300)` with a BYTE aggregate column silently wraps
to `(byte) 300 = 44`, and `FILL(70000)` with a SHORT column wraps to
`(short) 70000 = 4464`. The legacy `FillRangeRecordCursorFactory.getByte`
(at `FillRangeRecordCursorFactory.java:421`) uses `getFillFunction(col).getByte(null)`
directly, which would raise `UnsupportedOperationException` (IntFunction
default) and surface as a compile-time type error rather than runtime
truncation.

The pragmatic reason for the `.getInt(null)` indirection is that
`IntConstant.getByte(Record)` throws — the constant parser of a
`FILL(42)` literal does not auto-narrow to a BYTE/SHORT-typed function,
so calling `.getByte(null)` would fail with an unsupported-operation
exception. The cast avoids that at the cost of silent overflow.

Preferred long-term path: coerce the user-supplied constant to the
target column's type at codegen time (similar to `coerceRuntimeConstantType`
used for FROM/TO bounds on lines 3353, 3358), so the constant Function
stored in `constantFillFuncs` implements `getByte` / `getShort` natively.
Then `FillRecord.getByte` / `getShort` could call `.getByte(null)` /
`.getShort(null)` without casting. Out of scope for Phase 14 (pre-existing
behavior inherited from the legacy fill factory); left as a future
cleanup item.

**Fix:** Track as a future improvement; no change needed in Phase 14.
If/when addressed, pattern is `functionParser` + `coerceRuntimeConstantType`
(see how fromFunc/toFunc are narrowed at `SqlCodeGenerator.java:3352-3358`).

---

_Reviewed: 2026-04-20T17:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
