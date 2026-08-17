# Composite 9A — Day-Run Interval Cursor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the monotonic partition×interval walk in both interval frame cursors with a day-run, cell-major walk, so every cell of a day sees every interval — deleting the `multipleSubDayIntervalsOverMultiCellDayUnsupported` gate and all nine of its throw sites.

**Architecture:** A day is a contiguous run `[runLo, runHi)` of partitions sharing one partition timestamp (both cursors already rely on this adjacency). Within a run the walk inverts to cell-major: each cell restarts at the run's first interval. The global interval resume point after a run is the *minimum* index any cell reached (forward; maximum for backward), so an interval reaching into the next day stays live. A plain table's run holds exactly one cell, so the inner loop runs once and the walk reduces to today's — plain byte-identity is structural, not tested-for.

**Tech Stack:** Java 25 (`JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`), Maven offline (`mvn -o -pl core`), JUnit 4, `QDB_TEST_TMPDIR=/dev/shm`.

## Global Constraints

- **Cardinal rule:** composite behaves exactly like its plain twin, or fails LOUDLY. No silent path.
- **Invariant 1:** plain-table behaviour is byte-identical. Plain frame emission order must not change.
- **Invariant 6:** a refusal fires at the statement that caused it.
- Every new test must be shown to FAIL with its fix reverted, and the result recorded in the commit message. Use `cp`/restore for negative controls — **never `git stash` or `git checkout`** in this worktree (it holds uncommitted unrelated work; a prior `git stash` control in this project produced a false pass).
- A backward-scan test must use a **single sort key**, project **only `ts`**, and **assert the plan**. A multi-key `ORDER BY ts DESC` silently plans as a sort over a forward scan, and an outer `ORDER BY` lets the optimiser drop an inner one. Three tests in this project passed against a defective build before this was checked.
- `io.questdb.test.griffin.**` must be green before any PR. Four errors are known and pre-existing: two `CurrentDataIDFunctionFactoryTest`, `SampleByConfigTest`, `SampleByNanoTimestampConfigTest` — all port-9000 collisions, all unrelated.
- Performance is measured and recorded per operation, never gating (project decision, 2026-08-17).
- Do not run long suites in the background inside a subagent — all three wave-0 implementers stalled that way. Run them in the foreground.

---

### Task 1: Day-run state and helpers in the shared base

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/AbstractIntervalPartitionFrameCursor.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeDayRunUnitTest.java` (create)

**Interfaces:**
- Consumes: `reader.getPartitionTimestampByIndex(int)`, `partitionLo`, `partitionHi`, `intervalsLo`, `intervalsHi` (existing protected fields).
- Produces: protected fields `runLo`, `runHi`, `runIntervalLo`, `runResume`; methods `beginForwardRun()`, `beginBackwardRun()`, `forwardRunEnd(int)`, `backwardRunStart(int)`. Tasks 2–4 rely on exactly these names.

- [ ] **Step 1: Write the failing test**

```java
package io.questdb.test.cairo;

import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Day-run bounds are the foundation of the 9A walk: a run is the maximal set of partitions sharing
 * one partition timestamp. Asserted directly against a reader rather than through a query, because
 * every branch of both cursors depends on these two numbers being right at the edges — first run,
 * last run, single-cell run, and a run that would extend past the culled partition bound.
 */
public class CompositeDayRunUnitTest extends AbstractCairoTest {

    @Test
    public void testRunBoundsOverMixedCellCounts() throws Exception {
        assertMemoryLeak(() -> {
            // day 1: 1 cell, day 2: 3 cells, day 3: 2 cells
            execute("create table c (ts timestamp, exch symbol, px double)"
                    + " timestamp(ts) partition by day, exch layout plain wal");
            execute("insert into c values"
                    + " ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + " ('2023-01-02T01:00:00.000000Z','E0',2.0),"
                    + " ('2023-01-02T02:00:00.000000Z','E1',3.0),"
                    + " ('2023-01-02T03:00:00.000000Z','E2',4.0),"
                    + " ('2023-01-03T01:00:00.000000Z','E0',5.0),"
                    + " ('2023-01-03T02:00:00.000000Z','E1',6.0)");
            drainWalQueue();

            try (TableReader reader = getReader("c")) {
                Assert.assertEquals(6, reader.getPartitionCount());
                // (index -> expected run end) for a forward walk over the whole table
                assertForwardRun(reader, 0, 6, 1);   // day 1, single cell
                assertForwardRun(reader, 1, 6, 4);   // day 2, three cells
                assertForwardRun(reader, 2, 6, 4);   // mid-run start still ends at 4
                assertForwardRun(reader, 4, 6, 6);   // day 3, two cells, ends at bound
                // a culled bound must clamp the run
                assertForwardRun(reader, 1, 3, 3);
                // backward
                assertBackwardRun(reader, 5, 0, 4);  // day 3 starts at 4
                assertBackwardRun(reader, 3, 0, 1);  // day 2 starts at 1
                assertBackwardRun(reader, 0, 0, 0);  // day 1, single cell
                assertBackwardRun(reader, 3, 2, 2);  // culled bound clamps
            }
        });
    }

    private static void assertForwardRun(TableReader reader, int from, int hiBound, int expected) {
        long ts = reader.getPartitionTimestampByIndex(from);
        int end = from + 1;
        while (end < hiBound && reader.getPartitionTimestampByIndex(end) == ts) {
            end++;
        }
        Assert.assertEquals("forward run end from " + from, expected, end);
    }

    private static void assertBackwardRun(TableReader reader, int from, int loBound, int expected) {
        long ts = reader.getPartitionTimestampByIndex(from);
        int start = from;
        while (start > loBound && reader.getPartitionTimestampByIndex(start - 1) == ts) {
            start--;
        }
        Assert.assertEquals("backward run start from " + from, expected, start);
    }
}
```

> **Note for the implementer:** this test deliberately re-implements the run scan inline rather than
> calling the new helpers. It is a *characterisation* test of the reader's partition ordering — the
> fact the whole design rests on. Step 3 makes the helpers match it. If you find yourself changing
> this test to match your helper, stop: the helper is wrong, or the ordering assumption is.

- [ ] **Step 2: Run it to make sure it passes against today's reader**

```bash
cd /home/nick/claude/wt/oss/composite-partitioning
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 QDB_TEST_TMPDIR=/dev/shm
mvn -o -pl core test -Dtest=CompositeDayRunUnitTest
```

Expected: PASS (1/1). This one is green from the start on purpose — it pins the ordering assumption
before anything depends on it. If it FAILS, the adjacency premise is false and **this whole plan is
wrong**: stop and escalate rather than adapting the test.

- [ ] **Step 3: Add the run state and helpers**

In `AbstractIntervalPartitionFrameCursor`, add beside the existing protected fields (after
`protected int partitionLo;`):

```java
    // 9A day-run state. A "run" is the maximal set of partitions sharing one partition timestamp --
    // i.e. all cells of one day. For a PLAIN table every run is exactly one partition, which is what
    // makes the cell-major inner walk in both concrete cursors reduce to the pre-9A walk there.
    protected int runHi = -1;
    protected int runIntervalLo;
    protected int runLo = -1;
    protected int runResume;
```

Add the four helpers (place them beside `isCellAllowed`, alphabetical order as in this file):

```java
    /**
     * First partition index of the day-run containing {@code partitionIndex}, clamped at
     * {@code loBound}. Backward counterpart of {@link #forwardRunEnd(int, int)}.
     */
    protected int backwardRunStart(int partitionIndex, int loBound) {
        final long ts = reader.getPartitionTimestampByIndex(partitionIndex);
        int start = partitionIndex;
        while (start > loBound && reader.getPartitionTimestampByIndex(start - 1) == ts) {
            start--;
        }
        return start;
    }

    /**
     * One past the last partition index of the day-run containing {@code partitionIndex}, clamped at
     * {@code hiBound}. O(cells-in-day), called once per run rather than per frame.
     */
    protected int forwardRunEnd(int partitionIndex, int hiBound) {
        final long ts = reader.getPartitionTimestampByIndex(partitionIndex);
        int end = partitionIndex + 1;
        while (end < hiBound && reader.getPartitionTimestampByIndex(end) == ts) {
            end++;
        }
        return end;
    }

    /**
     * Opens the day-run starting at {@code partitionLo}. Every cell of the run will be walked from
     * {@code runIntervalLo}; {@code runResume} accumulates the MINIMUM interval index the run's cells
     * reach, which becomes the global {@code intervalsLo} once the run completes. The minimum, not the
     * last cell's index: an interval that reaches past this day must stay live for the next one, and
     * taking the last cell's index would retire it early and silently drop rows.
     */
    protected void beginForwardRun() {
        runLo = partitionLo;
        runHi = forwardRunEnd(partitionLo, partitionHi);
        runIntervalLo = intervalsLo;
        runResume = intervalsHi;
    }

    /**
     * Backward mirror of {@link #beginForwardRun()}. The run is opened from its TOP
     * ({@code partitionHi - 1}) and {@code runResume} accumulates the MAXIMUM interval index reached,
     * for the same reason inverted.
     */
    protected void beginBackwardRun() {
        runHi = partitionHi;
        runLo = backwardRunStart(partitionHi - 1, partitionLo);
        runIntervalLo = intervalsHi;
        runResume = intervalsLo;
    }
```

Extend `toTop()` — every resumption point depends on this being reset:

```java
    @Override
    public void toTop() {
        parquetTimestampFinder.clear();
        nativeTimestampFinder.clear();
        intervalsLo = initialIntervalsLo;
        intervalsHi = initialIntervalsHi;
        partitionLo = initialPartitionLo;
        partitionHi = initialPartitionHi;
        sizeSoFar = 0;
        // 9A: -1/-1 means "no run open"; both concrete cursors open one lazily on the next call.
        runLo = -1;
        runHi = -1;
        runIntervalLo = 0;
        runResume = 0;
    }
```

- [ ] **Step 4: Extend the test to exercise the helpers directly**

Append to `CompositeDayRunUnitTest`:

```java
    @Test
    public void testHelpersAgreeWithInlineScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double)"
                    + " timestamp(ts) partition by day, exch layout plain wal");
            execute("insert into c values"
                    + " ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + " ('2023-01-02T01:00:00.000000Z','E0',2.0),"
                    + " ('2023-01-02T02:00:00.000000Z','E1',3.0),"
                    + " ('2023-01-02T03:00:00.000000Z','E2',4.0)");
            drainWalQueue();
            try (TableReader reader = getReader("c")) {
                TestDayRunProbe probe = new TestDayRunProbe(reader);
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    int expectedEnd = i + 1;
                    while (expectedEnd < n
                            && reader.getPartitionTimestampByIndex(expectedEnd)
                            == reader.getPartitionTimestampByIndex(i)) {
                        expectedEnd++;
                    }
                    Assert.assertEquals("forwardRunEnd(" + i + ")", expectedEnd, probe.forwardEnd(i, n));
                }
            }
        });
    }
```

Add `TestDayRunProbe` as a package-private class in the same file's package that extends
`AbstractIntervalPartitionFrameCursor` only far enough to expose the two helpers:

```java
package io.questdb.test.cairo;

import io.questdb.cairo.TableReader;

/**
 * Exposes AbstractIntervalPartitionFrameCursor's protected run helpers to the unit test. Exists
 * because the helpers are pure functions of the reader's partition ordering and deserve a direct
 * test, not only the indirect coverage they get through the two concrete cursors.
 */
final class TestDayRunProbe {
    // See task brief: the implementer decides between a test-only subclass and making the two
    // helpers package-visible for test. Prefer whichever needs no production visibility widening.
}
```

> **Implementer decision, stated so it is not discovered mid-task:** `forwardRunEnd`/`backwardRunStart`
> are `protected` on an abstract class whose constructor needs a `CairoConfiguration` and an interval
> model. If a test-only subclass turns out to need more scaffolding than the test is worth, delete
> `testHelpersAgreeWithInlineScan` and `TestDayRunProbe` and rely on `testRunBoundsOverMixedCellCounts`
> plus the end-to-end coverage in Tasks 2–4. Do not widen production visibility to make a unit test
> convenient. Record which you chose in the report.

- [ ] **Step 5: Compile and run**

```bash
mvn -o -pl core test -Dtest=CompositeDayRunUnitTest
```

Expected: PASS. Nothing else in the tree uses the new fields yet, so no other suite can move.

- [ ] **Step 6: Confirm nothing regressed**

```bash
mvn -o -pl core test -Dtest='Composite*'
```

Expected: 429/429 pass, 0 failures, 0 errors. This is the pre-9A baseline — record the exact number
in the report, because Tasks 2–4 are judged against it.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/AbstractIntervalPartitionFrameCursor.java \
        core/src/test/java/io/questdb/test/cairo/CompositeDayRunUnitTest.java
git commit -m "feat(composite): add day-run state and helpers for the 9A cursor walk

Adds runLo/runHi/runIntervalLo/runResume and the four helpers both interval
cursors will walk with, plus a characterisation test pinning the assumption the
whole design rests on: a day's cells are CONTIGUOUS in partition-index order.

No cursor uses these yet, so behaviour is unchanged -- Composite* 429/429,
identical to the pre-task baseline."
```

---

### Task 2: Forward `next()` — day-run cell-major walk

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/IntervalFwdPartitionFrameCursor.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeMultiIntervalTest.java` (extend)

**Interfaces:**
- Consumes: Task 1's `beginForwardRun()`, `runLo`, `runHi`, `runIntervalLo`, `runResume`.
- Produces: the forward walk shape Task 3 mirrors in `calculateSize()` and Task 4 mirrors backward.

- [ ] **Step 1: Write the failing test**

Add to `CompositeMultiIntervalTest` — this is the shape the gate exists to refuse:

```java
    /**
     * The shape 9A exists to support: TWO sub-day intervals over ONE multi-cell day. Pre-9A this threw
     * "does not yet support multiple sub-day time intervals over a single multi-cell day"; the rows the
     * gate protected are the ones asserted here.
     */
    @Test
    public void testTwoSubDayIntervalsOverOneMultiCellDay() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // E0 holds 01:00 and 07:00; E1 holds 03:00 and 09:00 -- so BOTH cells have rows in BOTH
            // intervals, which is exactly what a monotonic walk cannot deliver.
            insertBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T07:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T09:00:00.000000Z','E1',4.0)");
            assertTwinEqual("select ts from %s where ts in ('2023-01-02T00:30:00.000000Z','2023-01-02T03:30:00.000000Z')"
                    + " or ts in ('2023-01-02T06:30:00.000000Z','2023-01-02T09:30:00.000000Z') order by ts");
        });
    }

    /**
     * Same shape, backward. Single sort key, projects only ts, and asserts the plan -- a multi-key
     * ORDER BY ts DESC plans as a sort over a FORWARD scan and would test nothing.
     */
    @Test
    public void testTwoSubDayIntervalsOverOneMultiCellDayBackward() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T07:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T09:00:00.000000Z','E1',4.0)");
            assertBackwardTwinEqual("select ts from %s where ts in ('2023-01-02T00:30:00.000000Z','2023-01-02T03:30:00.000000Z')"
                    + " or ts in ('2023-01-02T06:30:00.000000Z','2023-01-02T09:30:00.000000Z') order by ts desc");
        });
    }
```

> Use the existing helpers in `AbstractCompositeTwinTest` (`assertTwinEqual`, `assertBackwardTwinEqual`)
> — they already enforce the single-sort-key, project-only-`ts`, assert-the-plan discipline. If
> `CompositeMultiIntervalTest` does not already extend it, make it do so rather than re-deriving the
> helpers.

- [ ] **Step 2: Run it to verify it fails**

```bash
mvn -o -pl core test -Dtest=CompositeMultiIntervalTest
```

Expected: both new tests FAIL with `CairoException` containing "does not yet support multiple sub-day
time intervals over a single multi-cell day". That message *is* the gate — seeing it confirms the
test reaches the shape rather than passing for an unrelated reason. Record the failure text.

- [ ] **Step 3: Restructure `next()`**

Replace the `while (intervalsLo < intervalsHi && partitionLo < partitionHi)` loop header and add the
run bookkeeping at the top of the body:

```java
    @Override
    public PartitionFrame next(long skipTarget) {
        // order of logical operations is important
        // we are not calculating partition ranges when intervals are empty
        while (partitionLo < partitionHi && (intervalsLo < intervalsHi || runLo >= 0)) {
            // 9A: open a day-run the first time, and whenever the previous one completed. Every cell of
            // the run is walked from runIntervalLo, so each cell sees EVERY interval -- the monotonic
            // constraint that produced this file's three silent-wrong-answer defects is gone. A PLAIN
            // table's run is one partition, so the whole run mechanism collapses to a no-op there.
            if (partitionLo >= runHi) {
                beginForwardRun();
            }
            if (intervalsLo >= intervalsHi) {
                // this cell has consumed every interval -- move to the next cell of the run
                advanceForwardCell();
                continue;
            }
            ...
```

Add the shared cell-advance, which is the single place the run's resume point is maintained:

```java
    /**
     * Ends the current cell and moves to the next one, folding this cell's reached interval index into
     * the run's resume point. When the run completes, {@code intervalsLo} becomes that resume point --
     * the MINIMUM index any cell reached, so an interval that extends past this day survives for the
     * next one. Taking the last cell's index instead would retire such an interval early and silently
     * drop its rows, which is precisely the defect class 9A exists to end.
     */
    private void advanceForwardCell() {
        if (intervalsLo < runResume) {
            runResume = intervalsLo;
        }
        partitionLimit = 0;
        partitionLo++;
        if (partitionLo >= runHi) {
            // run complete -- publish the resume point and let the next iteration open a new run
            intervalsLo = runResume;
        } else {
            // next cell of the SAME day restarts at this run's first interval
            intervalsLo = runIntervalLo;
        }
    }
```

Now replace each branch. Every branch that today advances `partitionLo` becomes `advanceForwardCell()`;
every branch that today advances `intervalsLo` stays as it is; every gate throw is **deleted**:

| Today (line) | Condition | Replacement |
|---|---|---|
| 205-209 | `!isCellAllowed(partitionLo)` | `advanceForwardCell(); continue;` |
| 231-234 | `partitionTimestampLoApprox > intervalHi && !hasSameDaySiblingAhead(...)` | **delete the sibling clause** — becomes `if (partitionTimestampLoApprox > intervalHi) { intervalsLo++; continue; }`. The fall-through existed only to reach the sibling logic; with per-cell intervals the approximate check is sound again. |
| 238-242 | `partitionTimestampHiApprox < intervalLo` | `advanceForwardCell(); continue;` |
| 259-265 | `partitionTimestampLoExact > intervalHi` | `intervalsLo++; continue;` — delete the `retireIntervalOrVisitSibling` call entirely |
| 268-272 | `partitionTimestampHiExact < intervalLo` | `advanceForwardCell(); continue;` — the cell's max ts is below this interval's lo, so it is below every later interval's lo too: this **cell** is exhausted |
| 311-314 | `hi == rowCount` (whole partition) | `advanceForwardCell();` then `return frame;` — cell exhausted, interval stays live via `runResume` |
| 315-349 | fragment **with** same-day sibling | **delete this entire branch**, including its gate throw. The fragment case is now uniform. |
| 350-355 | fragment, no sibling | keep as the sole fragment branch: `partitionLimit = hi; intervalsLo++;` then `return frame;` |
| 359-366 | empty frame | `partitionLimit = hi; intervalsLo++;` — delete the `retireIntervalOrVisitSibling` call |
| 367-370 | `rowCount == 0` | `advanceForwardCell(); continue;` |

Then delete the now-unused `retireIntervalOrVisitSibling(long)` and `hasSameDaySiblingAhead(int,int)`
methods, and extend `toTop()`:

```java
    @Override
    public void toTop() {
        super.toTop();
        partitionLimit = 0;
    }
```

(`super.toTop()` already resets the run fields as of Task 1 — no change needed here, but confirm it
compiles and that `runHi` is `-1` so the first `next()` opens a run.)

> **The loop condition changed and that is load-bearing.** `partitionLo < partitionHi && (intervalsLo <
> intervalsHi || runLo >= 0)` — the `runLo >= 0` disjunct lets a cell that exhausted its intervals
> still reach `advanceForwardCell()`, which is what moves the walk to the next cell of the run. With
> the original condition the loop would exit the moment the FIRST cell finished its intervals, and
> every later cell of that day would be dropped. Get this wrong and the tests in Step 1 still pass
> (both cells have rows in both intervals) while `CompositeIntervalSiblingCellTest` fails.

- [ ] **Step 4: Run the new tests**

```bash
mvn -o -pl core test -Dtest=CompositeMultiIntervalTest
```

Expected: PASS, including the two new ones.

- [ ] **Step 5: Run the whole 9A regression net**

```bash
mvn -o -pl core test -Dtest='CompositeInterval*,CompositeMultiIntervalTest,CompositeReadShapes*,CompositeFactoryCoverageTest'
```

Expected: all pass. These eleven suites were built specifically to catch a rewrite of this cursor. Any
failure here is the rewrite being wrong, not the test being stale — do not adjust a test to fit.

- [ ] **Step 6: Prove plain is untouched**

```bash
mvn -o -pl core test -Dtest='IntervalFwd*,IntervalBwd*,*IntervalTest,SampleByTest,LatestByTest'
```

Expected: all pass. Then confirm structurally, and paste the output into the report:

```bash
git diff core/src/main/java/io/questdb/cairo/IntervalFwdPartitionFrameCursor.java | grep -c '^[-+]'
```

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/IntervalFwdPartitionFrameCursor.java \
        core/src/test/java/io/questdb/test/cairo/CompositeMultiIntervalTest.java
git commit -m "feat(composite): forward interval cursor walks day-runs cell-major

Each cell of a day now restarts at the run's first interval, so every cell sees
every interval. Deletes five gate throw sites, retireIntervalOrVisitSibling and
hasSameDaySiblingAhead from this file.

Negative control: both new tests fail before this change with 'does not yet
support multiple sub-day time intervals over a single multi-cell day'.

Plain unchanged by construction -- a plain day-run is one partition, so the
cell-major inner walk runs once and reduces to the pre-9A walk."
```

---

### Task 3: Forward `calculateSize()` — same walk, no frames

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/IntervalFwdPartitionFrameCursor.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeIntervalAggregateTest.java` (extend)

**Interfaces:**
- Consumes: Task 2's branch structure. `calculateSize()` must agree with `next()` on every branch.

> **Context the implementer needs:** `calculateSize()` operates on *local copies* (`intervalsLo1`,
> `partitionLo1`, …) and must not mutate cursor state, so it needs its own local run variables rather
> than the shared fields. Note also that its "no residual limit" sentinel is `-1`, while `next()`'s is
> `0`. This asymmetry is pre-existing — preserve it, do not unify it in this task.
>
> A prior commit message in this project falsely claimed `calculateSize()` is reached by composite
> `count()`. It is **not** — measured, and corrected in the code comments. It is kept mirrored anyway
> because counting by iteration is an obvious future optimisation, and a delegation added later would
> silently reintroduce the dropped-rows defect. Treat it as futureproofing that must be *correct*, not
> as a live path, and do not write a test that claims to prove it live.

- [ ] **Step 1: Write the test**

```java
    /**
     * calculateSize() must agree with next() on the shape 9A unlocks. This does NOT prove composite
     * count() reaches calculateSize() -- it does not (measured). It proves the mirrored walk is right
     * for the day it is switched on.
     */
    @Test
    public void testCountMatchesTwinOverTwoSubDayIntervals() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T07:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T09:00:00.000000Z','E1',4.0)");
            assertTwinEqual("select count() from %s where ts in ('2023-01-02T00:30:00.000000Z','2023-01-02T03:30:00.000000Z')"
                    + " or ts in ('2023-01-02T06:30:00.000000Z','2023-01-02T09:30:00.000000Z')");
        });
    }
```

- [ ] **Step 2: Run it**

```bash
mvn -o -pl core test -Dtest=CompositeIntervalAggregateTest
```

Expected: PASS already, via the `next()` path fixed in Task 2. Record that it passes *before* the
Step 3 change — this test does not gate Task 3, it guards against Task 3 breaking the live path.

- [ ] **Step 3: Apply the same restructure to `calculateSize()`**

Add locals mirroring the run fields at the top of the method, beside the existing local copies:

```java
        int runHi1 = -1;
        int runIntervalLo1 = 0;
        int runResume1 = 0;
```

Loop header and run bookkeeping:

```java
        while (partitionLo1 < partitionHi1 && (intervalsLo1 < intervalsHi1 || runHi1 >= 0)) {
            if (partitionLo1 >= runHi1) {
                runHi1 = forwardRunEnd(partitionLo1, partitionHi1);
                runIntervalLo1 = intervalsLo1;
                runResume1 = intervalsHi1;
            }
            if (intervalsLo1 >= intervalsHi1) {
                if (intervalsLo1 < runResume1) {
                    runResume1 = intervalsLo1;
                }
                partitionLimit1 = -1;
                partitionLo1++;
                intervalsLo1 = partitionLo1 >= runHi1 ? runResume1 : runIntervalLo1;
                continue;
            }
```

Apply the identical branch table from Task 2, substituting the local names and the `-1` sentinel. The
"advance cell" sequence in this method is:

```java
                if (intervalsLo1 < runResume1) {
                    runResume1 = intervalsLo1;
                }
                partitionLimit1 = -1;
                partitionLo1++;
                intervalsLo1 = partitionLo1 >= runHi1 ? runResume1 : runIntervalLo1;
                continue;
```

Delete both of this method's gate throws (lines 162-165 and 178-181 pre-change) and its calls to
`hasSameDaySiblingAhead` (lines 94, 114, 177 pre-change), collapsing the fragment branches exactly as
Task 2 did for `next()`.

- [ ] **Step 4: Run**

```bash
mvn -o -pl core test -Dtest='CompositeIntervalAggregate*,CompositeInterval*,CompositeMultiIntervalTest'
```

Expected: all pass.

- [ ] **Step 5: Prove the two walks agree**

Add to `CompositeIntervalCursorUnitTest` a direct comparison — this is the only way to test
`calculateSize()`'s composite path, since no query reaches it:

```java
    /**
     * next() and calculateSize() are two hand-maintained copies of one walk. Nothing in production
     * reaches calculateSize() for a composite table today, so only a direct comparison can catch them
     * drifting -- and drift is exactly how a future count() optimisation would reintroduce dropped rows.
     */
    @Test
    public void testCalculateSizeAgreesWithNextOverMultiCellDays() throws Exception {
        // Drive both over the same cursor state for a table with 3 cells on one day and two
        // sub-day intervals; sum next()'s frame sizes and compare with calculateSize()'s counter.
    }
```

> **Implementer:** fill this in against the existing helpers in `CompositeIntervalCursorUnitTest`,
> which already constructs cursors directly. The assertion is `sum of (rowHi - rowLo) over all frames
> from next()` equals `counter` from `calculateSize()` on a freshly `toTop()`'d cursor. Include at
> least: one multi-cell day with two sub-day intervals, one interval spanning a day boundary, and one
> excluded cell via `setAllowedCellKeys`.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/IntervalFwdPartitionFrameCursor.java \
        core/src/test/java/io/questdb/test/cairo/CompositeIntervalAggregateTest.java \
        core/src/test/java/io/questdb/test/cairo/CompositeIntervalCursorUnitTest.java
git commit -m "feat(composite): mirror the day-run walk into forward calculateSize()

Deletes this method's two gate throws and its sibling-lookahead. Adds a direct
next()-vs-calculateSize() agreement test, the only way to cover this path: no
composite query reaches calculateSize() today (measured), and the mirror exists
so a future count() optimisation cannot silently reintroduce dropped rows."
```

---

### Task 4: Backward cursor — both methods

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/IntervalBwdPartitionFrameCursor.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeIntervalSiblingCellTest.java` (extend)

**Interfaces:**
- Consumes: Task 1's `beginBackwardRun()`, `backwardRunStart(int,int)`.

> **Why this is its own task:** the backward cursor is not a syntactic mirror. It walks
> `partitionHi - 1` downward and `intervalsHi - 1` downward, its residual sentinel is `-1` in *both*
> methods, and its `limitHi` bookkeeping (`partitionLimit1 == -1 ? rowCount - 1 : partitionLimit1 - 1`)
> has no forward counterpart. This cursor was also found returning **zero rows** for
> `ORDER BY ts DESC` with a timestamp filter when only the forward fix had been applied — the reason
> this plan does not treat "mirror it" as mechanical.

- [ ] **Step 1: Write the failing tests**

```java
    /**
     * Backward twin of the 9A shape. Single sort key, projects only ts, asserts the plan -- see the
     * plan's global constraints for why all three are mandatory here.
     */
    @Test
    public void testBackwardTwoSubDayIntervalsOverOneMultiCellDay() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T07:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T09:00:00.000000Z','E1',4.0)");
            assertBackwardTwinEqual("select ts from %s where ts in ('2023-01-02T00:30:00.000000Z','2023-01-02T03:30:00.000000Z')"
                    + " or ts in ('2023-01-02T06:30:00.000000Z','2023-01-02T09:30:00.000000Z') order by ts desc");
        });
    }

    /**
     * An interval spanning a day boundary must stay live across the run -- the case runResume's
     * MAXIMUM (backward) exists for. A last-cell resume point would retire it at the day edge.
     */
    @Test
    public void testBackwardIntervalSpanningDayBoundaryOverMultiCellDays() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertBoth("('2023-01-02T22:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T23:00:00.000000Z','E1',2.0),"
                    + "('2023-01-03T01:00:00.000000Z','E0',3.0),"
                    + "('2023-01-03T02:00:00.000000Z','E1',4.0)");
            assertBackwardTwinEqual("select ts from %s where ts between '2023-01-02T21:00:00.000000Z'"
                    + " and '2023-01-03T03:00:00.000000Z' order by ts desc");
        });
    }
```

- [ ] **Step 2: Run to verify failure**

```bash
mvn -o -pl core test -Dtest=CompositeIntervalSiblingCellTest
```

Expected: the first FAILS with the gate message. The second may pass already — record which, and if
it passes, say so plainly rather than implying both were red.

- [ ] **Step 3: Restructure `next()`**

Loop header and run bookkeeping (note: run opens from the TOP, resume is a MAXIMUM):

```java
        while (partitionLo < partitionHi && (intervalsLo < intervalsHi || runLo >= 0)) {
            if (partitionHi <= runLo) {
                beginBackwardRun();
            }
            if (intervalsLo >= intervalsHi) {
                retreatBackwardCell();
                continue;
            }
```

```java
    /**
     * Backward mirror of the forward cursor's advanceForwardCell(). Folds this cell's reached interval
     * bound into the run's resume point as a MAXIMUM: walking downward, an interval that reaches BELOW
     * this day must stay live for the next (earlier) day, so the run resumes at the highest bound any
     * cell reached.
     */
    private void retreatBackwardCell() {
        if (intervalsHi > runResume) {
            runResume = intervalsHi;
        }
        partitionLimit = -1;
        partitionHi--;
        if (partitionHi <= runLo) {
            intervalsHi = runResume;
        } else {
            intervalsHi = runIntervalLo;
        }
    }
```

Branch replacements — every branch that retreats `partitionHi1`/`partitionHi` becomes
`retreatBackwardCell()`; every branch that retreats `intervalsHi` stays; both gate throws go:

| Today (line) | Condition | Replacement |
|---|---|---|
| 82-86 / next's twin | `!isCellAllowed(currentPartition)` | `retreatBackwardCell(); continue;` |
| 100-104 | `partitionTimestampLoApprox > intervalHi` | `retreatBackwardCell(); continue;` |
| 109-113 | `partitionTimestampHiApprox < intervalLo && !hasSameDaySiblingBelow(...)` | delete the sibling clause: `if (partitionTimestampHiApprox < intervalLo) { partitionLimit = limitHi + 1; intervalsHi = currentInterval; continue; }` |
| 122-135 | `partitionTimestampHiExact < intervalLo` | `partitionLimit = limitHi + 1; intervalsHi = currentInterval; continue;` — delete the sibling block and its gate throw |
| 153-156 | `lo == 0` (whole partition) | `retreatBackwardCell();` |
| 157-170 | fragment **with** same-day sibling | **delete this entire branch and its gate throw** |
| 171-175 | fragment, no sibling | keep as the sole fragment branch |
| 180-184 | `rowCount == 0` | `retreatBackwardCell();` |

Delete `hasSameDaySiblingBelow(int,int)` once unused.

- [ ] **Step 4: Apply the same to backward `calculateSize()`**

Same substitution with local variables (`runLo1`, `runIntervalLo1`, `runResume1`), mirroring Task 3.

- [ ] **Step 5: Run backward and the full net**

```bash
mvn -o -pl core test -Dtest='CompositeIntervalSiblingCellTest,CompositeInterval*,CompositeMultiIntervalTest,CompositeReadShapes*,CompositeFactoryCoverageTest'
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/IntervalBwdPartitionFrameCursor.java \
        core/src/test/java/io/questdb/test/cairo/CompositeIntervalSiblingCellTest.java
git commit -m "feat(composite): backward interval cursor walks day-runs cell-major

Mirrors Tasks 2-3 downward: run opens from the top, resume point is a MAXIMUM so
an interval reaching below the day stays live. Deletes four gate throw sites and
hasSameDaySiblingBelow.

Negative control: testBackwardTwoSubDayIntervalsOverOneMultiCellDay fails before
this change with the gate message."
```

---

### Task 5: Delete the gate, flip the classification, enrol in the fuzz

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/AbstractIntervalPartitionFrameCursor.java` (delete `multipleSubDayIntervalsOverMultiCellDayUnsupported`)
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java` (GATED → SUPPORTED)
- Modify: `docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md` (audit key count 38 → 37)
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeUnsupportedOpsTest.java` (remove the gate's case)

- [ ] **Step 1: Delete the gate method and confirm no references remain**

```bash
grep -rn "multipleSubDayIntervalsOverMultiCellDay" core/src/ docs/
```

Expected after deletion: matches only in `docs/` (historical record) and none in `core/src/main`.
Any remaining `core/src` match is a throw site Tasks 2-4 missed — fix it before continuing.

- [ ] **Step 2: Flip the classification**

In `CompositeFuzzRunner`, move the multi-sub-day-interval shape from `GATED` to `SUPPORTED`. This
**auto-enrols it in the differential fuzz** — that is the mechanism, not a side effect. A gate lifted
without this flip is a gate lifted without coverage, and `CompositeFuzzOpCoverageTest` fails on any
unclassified operation.

- [ ] **Step 3: Update the tests that assert the refusal**

`CompositeUnsupportedOpsTest` asserts this gate refuses. Remove that case — do not leave it asserting
a message that no longer exists, and do not weaken it to a try/catch that passes either way.

- [ ] **Step 4: Run the fuzz**

```bash
mvn -o -pl core test -Dtest='CompositeFuzz*'
```

Expected: all pass, with the newly-enrolled shape exercised. Record the seed count actually run.

- [ ] **Step 5: Update the audit key count**

The closure index's appendix lists every refusal string. Remove this one and change the count from 38
to 37 in the prose. Then verify:

```bash
# regenerate and diff the audit block exactly as the closure index documents
```

Expected: empty diff at 37 keys.

- [ ] **Step 6: Full composite + griffin**

```bash
mvn -o -pl core test -Dtest='Composite*'
mvn -o -pl core test -Dtest='io.questdb.test.griffin.**'
```

Expected: `Composite*` all green (count will exceed 429 — record the new number). griffin: 0 failures,
exactly the 4 known port-9000 errors.

- [ ] **Step 7: Measure, do not gate**

Record composite-vs-plain timing for a multi-interval query over a multi-cell day, and for a
single-interval query (the pre-9A supported shape, to show the restructure did not cost the common
case). Performance is recorded, never gating — but an unexplained large regression is a correctness
smell worth reporting.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(composite): delete the multi-sub-day-interval gate

All nine throw sites are gone with the walk that needed them. The shape flips
GATED -> SUPPORTED in the fuzz classification, which enrols it in the
differential fuzz by construction. Audit keys 38 -> 37."
```

---

## Self-Review

**Spec coverage.** §3 (day-run design) → Tasks 1-4. §5's classification flip and the fuzz enrolment →
Task 5. §5's regression net is run in Tasks 2, 4 and 5. Gate 1 in §1's table is deleted in Task 5.
Gates 2-5 are explicitly out of scope for 9A (they are 9B/9C/9D, scheduled after sub-projects 2 and 3)
— no task here touches them.

**Not covered, deliberately:** §5 lists "the fuzz generates no indexed tables and no parquet cells" as
harness gaps. Those belong to 9B/9C and 9D respectively; adding them here would be building for a
tranche that is two sub-projects away.

**Placeholder scan.** Two steps hand judgment to the implementer rather than giving code: Task 1
Step 4 (`TestDayRunProbe` — whether a test-only subclass is worth its scaffolding) and Task 3 Step 5
(the next-vs-calculateSize comparison body). Both are marked as decisions with the criterion stated
and the fallback named. Both are genuine judgment calls about test construction against helpers I
cannot see from here, not missing content — but they are the two places a reviewer should look hardest.

**Type consistency.** `beginForwardRun()`/`beginBackwardRun()` take no arguments and read the cursor's
fields; `forwardRunEnd(int,int)`/`backwardRunStart(int,int)` take explicit bounds so `calculateSize()`
can call them with its locals. Task 3 and Task 4 Step 4 both depend on that two-argument form — it is
why the helpers are not written to read `partitionHi` directly.

**Known risk, stated rather than discovered.** The loop-condition change in Task 2 Step 3
(`|| runLo >= 0`) is the single most dangerous line in this plan: getting it wrong leaves the new
tests passing while dropping every cell after the first in a day. Task 2 Step 5 runs
`CompositeIntervalSiblingCellTest` specifically because that is the suite which would catch it. If
that suite goes red, suspect the loop condition before anything else.

**The `-1`/`0` sentinel asymmetry** between `next()` and `calculateSize()` is pre-existing and
preserved. It is a real trap for the implementer, so it is called out in Task 3's context block rather
than left to be discovered. Unifying it is a reasonable follow-up and explicitly **not** part of this
plan.
