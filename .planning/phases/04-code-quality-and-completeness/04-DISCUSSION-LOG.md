# Phase 4: Code Quality and Completeness - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-13
**Phase:** 04-code-quality-and-completeness
**Areas discussed:** Shared sort utility, Window alias strategy, toPlan format, Test migration scope, Sort utility naming/package, Missing group-by factories list, ORDER BY error test, GroupByDoubleList.sort() delegation, Quantile alias test coverage, assertQueryNoLeakCheck migration details

---

## Shared Sort Utility

| Option | Description | Selected |
|--------|-------------|----------|
| Static utility class | New class like DoubleMemorySort with static methods accepting (long ptr, long left, long right). Window factories call it directly on raw pointers. GroupByDoubleList.sort() delegates to it internally. | ✓ |
| Extend GroupByDoubleList | Add public static sort methods to GroupByDoubleList. Keeps sort logic in one existing file. | |

**User's choice:** Static utility class (Recommended)
**Notes:** None.

---

## Shared Sort Utility — Scope

| Option | Description | Selected |
|--------|-------------|----------|
| quickSort only | Extract just quickSort + partition + swap for window factories. quickSelect stays in GroupByDoubleList/GroupByLongList. | ✓ |
| Both quickSort and quickSelect | Extract everything into the utility. More consolidation but changes GroupByDoubleList/GroupByLongList internals. | |

**User's choice:** quickSort only (Recommended)
**Notes:** None.

---

## Window Alias Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Thin delegating factories | Create QuantileDisc*WindowFunctionFactory etc. that override getSignature() and delegate newInstance(). 4 new files. | ✓ |
| Register same factory twice | Modify Percentile* factories to support multiple names. | |

**User's choice:** Thin delegating factories (Recommended)
**Notes:** Matches how existing group-by aliases work.

---

## toPlan Format

| Option | Description | Selected |
|--------|-------------|----------|
| Show both args | percentile_disc(value,0.5) — shows data column and percentile value. | ✓ |
| Keep current (data arg only) | percentile_disc(value) — consistent with approx_percentile. | |

**User's choice:** Show both args (Recommended)
**Notes:** None.

---

## Test Migration Scope

| Option | Description | Selected |
|--------|-------------|----------|
| Migrate all 26 | Every assertSql call becomes assertQueryNoLeakCheck. Consistent. | ✓ |
| Migrate selectively | Only migrate data-correctness calls. | |

**User's choice:** Migrate all 26 (Recommended)
**Notes:** None.

---

## Sort Utility Naming/Package

| Option | Description | Selected |
|--------|-------------|----------|
| DoubleSort in io.questdb.std | Follows existing pattern: LongSort, IntGroupSort. | ✓ |
| MemoryDoubleSort in io.questdb.std | More descriptive name, still in std package. | |

**User's choice:** DoubleSort in io.questdb.std (Recommended)
**Notes:** None.

---

## Missing Group-By Factories List

| Option | Description | Selected |
|--------|-------------|----------|
| Register all 10 | All 10 factory files exist and compile. All should be callable from SQL. | ✓ |
| Review each individually | Walk through each factory to confirm. | |

**User's choice:** Register all 10 (Recommended)
**Notes:** 5 percentile + 5 quantile alias factories.

---

## ORDER BY Error Test

| Option | Description | Selected |
|--------|-------------|----------|
| All 4 window functions | Test percentile_disc, percentile_cont, multi-percentile_disc, multi-percentile_cont with ORDER BY. Also test ROWS/RANGE frames. | ✓ |
| One representative test | Just percentile_disc with ORDER BY. | |

**User's choice:** All 4 window functions (Recommended)
**Notes:** None.

---

## GroupByDoubleList.sort() Delegation

| Option | Description | Selected |
|--------|-------------|----------|
| Delegate to DoubleSort | GroupByDoubleList.sort() becomes a one-liner calling DoubleSort.sort(dataPtr(), 0, size-1). Full dedup. | ✓ |
| Keep separate | GroupByDoubleList keeps its own sort. 8→1 window dedup only. | |

**User's choice:** Delegate to DoubleSort (Recommended)
**Notes:** None.

---

## Quantile Alias Test Coverage

| Option | Description | Selected |
|--------|-------------|----------|
| Parsing + result equivalence | Verify quantile_disc produces same result as percentile_disc for both group-by and window. One test per alias pair. | ✓ |
| Parsing only | Just verify SQL recognition. | |

**User's choice:** Parsing + result equivalence (Recommended)
**Notes:** None.

---

## assertQueryNoLeakCheck Migration Details

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, all 26 | All 26 assertSql calls are data-correctness. Error-paths already use assertException(). Clean split. | ✓ |
| Spot-check first | Read a few assertSql calls to verify before bulk migration. | |

**User's choice:** Yes, all 26 (Recommended)
**Notes:** Verified that error-path tests already use assertException() (4 calls), so all assertSql calls are safe to migrate.

---

## Claude's Discretion

- Exact internal structure of DoubleSort (insertion sort threshold, recursion limit)
- Whether to port swap/partition as separate public methods or keep them private
- Test data setup for alias equivalence tests
- Whether to add quantile alias error-path tests or rely on delegation

## Deferred Ideas

None — discussion stayed within phase scope.
