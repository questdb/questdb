# Composite Wave 0 — Earliest Refusal + O3 Purge Proof — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every composite refusal fire at the statement that caused it, and either prove the O3 partition-purge skip harmless or escalate it as a disk leak.

**Architecture:** No new capability. Three independent changes: a measurement that decides whether a silent skip is a leak, and two gates moved earlier in time (commit-time → DDL, query-time → CREATE). Each moved gate keeps its original deeper gate in place as defence in depth, because non-SQL paths reach the same code.

**Tech Stack:** Java 25, Maven, JUnit 4. Tests live under `core/src/test/java/io/questdb/test/`. Build and test commands assume:

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
export QDB_TEST_TMPDIR=/dev/shm
```

## Global Constraints

Copied verbatim from `docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md` — every task's requirements implicitly include these.

- **Plain byte-identity.** A `dimCount == 0` table's `_txn` and `_meta` bytes are unchanged by any of this work.
- **No silent path.** Composite either behaves as the plain twin or fails loudly. A skip is acceptable only with a test proving it harmless.
- **A refusal fires at the statement that caused it**, never at a later one.
- **Every new test is shown to FAIL with its fix reverted**, and the result is recorded in the commit message.
- **Performance is measured and recorded per operation, never gating.**
- Any task that adds, moves or deletes a `"composite …"` refusal string MUST regenerate the audit-key block in the closure index (Appendix), or the next audit reports a false hole.
- Backward-scan tests use a single sort key, project only `ts`, and assert the plan. A multi-key `ORDER BY ts DESC` silently plans as a sort over a FORWARD scan.

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `core/src/test/java/io/questdb/test/cairo/CompositeO3PurgeSkipTest.java` | Decides whether the day-blind purge skip leaks disk. Created. | 1 |
| `core/src/test/java/io/questdb/test/griffin/CompositeEarliestRefusalTest.java` | Both moved gates: refusal must arrive at the statement. Created in 2, extended in 3. | 2, 3 |
| `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` | `alterTableSetFormat` gains the compile-time composite refusal. Modified ~1847. | 2 |
| `core/src/main/java/io/questdb/griffin/engine/ops/CreateTableOperationBuilderImpl.java` | `resolvePartitionSpec` gains the indexed-column refusal. Modified ~250. | 3 |
| `docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md` | Audit keys + skip-table verdict. Modified. | 1, 2, 3 |

---

### Task 1: Prove or escalate the O3 partition-purge skip

`O3PartitionPurgeJob:224` returns early for any composite table ("day-blind walk, cell-aware purge deferred") and only logs at INFO. Invariant 2 permits a skip only with a test proving it harmless. No such test exists. This task writes it. **The expected outcome is that it FAILS** — a day-blind walk over `<day>/<cell>` directories has no evident reason to reclaim anything — which converts the skip from "deferred optimisation" into a recorded disk leak.

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/CompositeO3PurgeSkipTest.java`
- Modify: `docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md` (skip table verdict)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: a recorded verdict (harmless vs leak) that sub-project 1 plans against. No production API.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cairo/CompositeO3PurgeSkipTest.java`:

```java
package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * O3PartitionPurgeJob skips composite tables entirely (a day-blind walk over what are now
 * <day>/<cell> directories). The project's invariant permits a skip only with a test proving it
 * harmless; this is that test.
 * <p>
 * O3 writes into an already-written partition create a NEW partition version directory and leave the
 * old one behind for readers still on the old txn. Purge is what reclaims them. If purge never runs,
 * those directories accumulate: a disk leak that no error surfaces.
 */
public class CompositeO3PurgeSkipTest extends AbstractCairoTest {

    @Test
    public void testCompositeReclaimsObsoletePartitionVersions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            seed("c");
            seed("p");
            drainWalQueue();

            final int compositeBefore = countPartitionDirs("c");
            final int plainBefore = countPartitionDirs("p");

            // 20 rounds of out-of-order writes into an already-written day: each round rewrites the
            // partition, producing a new version directory and orphaning the previous one.
            for (int round = 1; round <= 20; round++) {
                churn("c", round);
                churn("p", round);
                drainWalQueue();
                engine.releaseInactive();
                runPartitionPurgeJobs();
            }
            drainWalQueue();
            engine.releaseInactive();
            runPartitionPurgeJobs();

            final int compositeAfter = countPartitionDirs("c");
            final int plainAfter = countPartitionDirs("p");

            // The plain twin is the control: it proves the workload really does create obsolete
            // versions and that purge really does reclaim them on this build.
            Assert.assertTrue(
                    "control failed: the plain twin did not accumulate-then-reclaim, so this workload"
                            + " does not exercise purge at all (plainBefore=" + plainBefore
                            + ", plainAfter=" + plainAfter + ")",
                    plainAfter <= plainBefore + 1);

            Assert.assertTrue(
                    "composite leaked obsolete partition version directories: before=" + compositeBefore
                            + " after=" + compositeAfter + " (plain: " + plainBefore + " -> " + plainAfter
                            + "). O3PartitionPurgeJob skips composite tables, so nothing reclaims them.",
                    compositeAfter <= compositeBefore + 1);
        });
    }

    private void churn(String table, int round) throws Exception {
        // Lands inside the already-written day. MOST rounds are genuinely out-of-order; the ones
        // where (round % 6) == 5 land at 05:30, after the seeded max of 05:00, and are in-order
        // appends. That is fine for this measurement -- ~17 of 20 rounds still rewrite the partition,
        // and the growth it produces is unambiguous -- but the distinction is stated rather than
        // glossed, because a reader who assumed all 20 were O3 would mis-size the leak.
        execute("INSERT INTO " + table + " VALUES ('2023-01-02T0" + (round % 6) + ":30:00.000000Z','E"
                + (round % 3) + "'," + round + ".0)");
    }

    /**
     * Counts directories under the table root that look like a partition version (a dated directory,
     * with or without a .<txn> suffix). An obsolete version left behind by O3 shows up here.
     */
    private int countPartitionDirs(String table) throws IOException {
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> walk = Files.walk(root, 2)) {
            return (int) walk
                    .filter(Files::isDirectory)
                    .filter(p -> {
                        final Path parent = p.getParent();
                        return parent != null
                                && parent.getFileName().toString().startsWith(table + "~")
                                && p.getFileName().toString().startsWith("2023-");
                    })
                    .count();
        }
    }

    private void seed(String table) throws Exception {
        execute("INSERT INTO " + table + " VALUES ('2023-01-02T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-02T05:00:00.000000Z','E1',5.0)");
    }
}
```

- [ ] **Step 2: Run it and record the verdict**

```bash
cd /home/nick/claude/wt/oss/composite-partitioning
mvn -o -pl core test -Dtest='CompositeO3PurgeSkipTest' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:|leaked obsolete|control failed"
```

Two possible outcomes, both concrete:

- **FAIL on "composite leaked obsolete partition version directories"** — the expected result. The skip is a disk leak, not a harmless deferral. Go to Step 3a.
- **PASS** — the skip is harmless on this workload. Go to Step 3b.
- **FAIL on "control failed"** — the workload does not exercise purge at all; the test proves nothing. Raise the round count from 20 to 60 and re-run before drawing any conclusion.

- [ ] **Step 3a: If it FAILED — record the leak and mark the test `@Ignore`d against sub-project 1**

Add to the test class, above the method:

```java
    @org.junit.Ignore("PROVEN LEAK: O3PartitionPurgeJob skips composite tables, so obsolete partition"
            + " version directories are never reclaimed. Un-ignore when sub-project 1 makes the purge"
            + " walk cell-aware; this test is its acceptance criterion.")
```

Then edit `docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md`, replacing the O3 row's verdict text in the skip table with the measured numbers, e.g.:

```
| **O3 partition purge** (`O3PartitionPurgeJob:224`) | 1 — lifecycle. **MEASURED 2026-08-17: NOT harmless.** 20 O3 rounds left composite at N partition directories vs plain at M — obsolete versions are never reclaimed. This is a silent disk leak, and it is sub-project 1's first task. Acceptance test: `CompositeO3PurgeSkipTest` (currently `@Ignore`d). |
```

- [ ] **Step 3b: If it PASSED — record it as a proven-harmless skip**

Leave the test enabled and edit the same row to state that it is proven harmless, with the numbers, and that the test is the standing proof.

- [ ] **Step 4: Verify the test behaves as recorded**

```bash
mvn -o -pl core test -Dtest='CompositeO3PurgeSkipTest' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:|BUILD"
```

Expected after 3a: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 1`.
Expected after 3b: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0`.

- [ ] **Step 5: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/CompositeO3PurgeSkipTest.java \
        docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md
git commit -m "test(composite): decide whether the O3 purge skip is harmless or a disk leak

O3PartitionPurgeJob returns early for composite tables and logs at INFO. Invariant 2
permits a skip only with a test proving it harmless; none existed. This is that test:
20 rounds of out-of-order writes into an already-written day, counting partition
version directories before and after, with the plain twin as the control that proves
the workload exercises purge at all.

VERDICT: <leak | harmless>, numbers recorded in the closure index skip table."
```

---

### Task 2: `FORMAT PARQUET` — refuse at the statement, not at the next commit

Today `ALTER TABLE … SET FORMAT PARQUET` succeeds on a composite table and the *next commit* suspends it (`TableWriter:13224`). The statement that caused the problem reported success and an unrelated insert took the blame.

**Files:**
- Create: `core/src/test/java/io/questdb/test/griffin/CompositeEarliestRefusalTest.java`
- Modify: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` (in `alterTableSetFormat`, after the existing WAL/mat-view checks, ~line 1858)
- Modify: `docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md` (audit keys)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: the SQL-level refusal message `composite partitioning does not yet support FORMAT PARQUET [table=<name>]`, thrown as `SqlException` at the format token position. Task 3 extends the same test class.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/griffin/CompositeEarliestRefusalTest.java`:

```java
package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Wave 0: a composite refusal must arrive at the statement that caused it.
 * <p>
 * Both gates here previously fired somewhere else — one at the next commit, one at query time —
 * so the user learned about the restriction at a point where they could not act on it, and an
 * unrelated operation took the blame.
 */
public class CompositeEarliestRefusalTest extends AbstractCairoTest {

    /**
     * SET FORMAT PARQUET used to be accepted and suspend the table on the NEXT commit.
     */
    @Test
    public void testSetFormatParquetRefusedAtTheStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO c VALUES ('2023-01-02T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();

            assertExceptionNoLeakCheck("ALTER TABLE c SET FORMAT PARQUET", -1,
                    "composite partitioning does not yet support FORMAT PARQUET");

            // the table must be untouched and still writable -- a refused statement changes nothing
            Assert.assertFalse("a refused statement must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            execute("INSERT INTO c VALUES ('2023-01-02T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n2\n");
        });
    }

    /**
     * A PLAIN table must be entirely unaffected: SET FORMAT PARQUET still works.
     */
    @Test
    public void testSetFormatParquetStillWorksOnPlainTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES ('2023-01-02T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            execute("ALTER TABLE p SET FORMAT PARQUET");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("p")));
        });
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

```bash
mvn -o -pl core test -Dtest='CompositeEarliestRefusalTest' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:|expected|did not"
```

Expected: `testSetFormatParquetRefusedAtTheStatement` FAILS — the ALTER is currently accepted, so no exception is thrown.

- [ ] **Step 3: Add the compile-time refusal**

In `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java`, inside `alterTableSetFormat`, immediately after the existing `if (tableToken.isMatView())` check and still inside `if (format == TableUtils.TABLE_FORMAT_PARQUET) { … }`:

```java
            // Wave 0 -- a refusal must fire at the statement that caused it. Without this, SET FORMAT
            // PARQUET is accepted on a composite table and the NEXT commit suspends it via
            // TableWriter's own FORMAT PARQUET guard, so an unrelated insert takes the blame for this
            // statement. That writer-side guard STAYS: it protects non-SQL paths, and gates move
            // rather than vanish. Removed by sub-project 3, which makes a parquet cell addressable.
            //
            // A TableReader is opened because TableRecordMetadata does not expose PartitionSpec --
            // the same idiom as the DEDUP gate in this file and the mat-view gate in
            // executeCreateMatView. alterTableSetFormat has no SqlExecutionContext parameter, so the
            // engine field is used, exactly as the mat-view gate does.
            try (TableReader compositeCheckReader = engine.getReader(tableToken)) {
                if (compositeCheckReader.getMetadata().getPartitionSpec().getDimensionCount() > 0) {
                    throw SqlException.$(formatPos, "composite partitioning does not yet support FORMAT PARQUET [table=")
                            .put(tableToken.getTableName()).put(']');
                }
            }
```

If `io.questdb.cairo.TableReader` is not already imported in this file, add the import.

- [ ] **Step 4: Run the test to verify it passes**

```bash
mvn -o -pl core test -Dtest='CompositeEarliestRefusalTest' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:|BUILD"
```

Expected: `Tests run: 2, Failures: 0, Errors: 0`.

- [ ] **Step 5: Verify the test fails with the fix reverted (required by Global Constraints)**

```bash
cp core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java /tmp/SqlCompilerImpl.keep
# delete the block added in Step 3, then:
mvn -o -pl core test -Dtest='CompositeEarliestRefusalTest' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:"
cp /tmp/SqlCompilerImpl.keep core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java
```

Expected while reverted: `Tests run: 2, Failures: 1`. Record that number in the commit message.
Do NOT use `git stash` for this — it produced a false pass on this branch on 2026-08-13.

- [ ] **Step 6: Regenerate the audit keys**

```bash
grep -rhoE '"[^"]*composite[^"]*"' core/src/main/java/ \
  | grep -viE "renderCellSegment|resolveCellKey|must not be called" \
  | grep -iE "does not (yet )?support|not yet supported|supports native|requires a WAL|skipping" \
  | sed 's/"//g; s/ \[.*//; s/;.*//' | sort -u
```

Paste the output into the first fenced block of the Appendix in
`docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md`, replacing its contents. (This
task adds no new distinct string — the SQL-side message matches the writer-side one — so the block
should be unchanged; confirm that rather than assuming it.)

- [ ] **Step 7: Run the SQL suite**

```bash
mvn -o -pl core test -Dtest='io.questdb.test.griffin.**' -DfailIfNoTests=false 2>&1 | grep -E "Tests run:.*(Failures: [1-9]|Errors: [1-9])|BUILD"
```

Expected: `BUILD SUCCESS`, or only the known port-9000 `could not bind socket` errors
(`CurrentDataIDFunctionFactoryTest`, `SampleByConfigTest`, `SampleByNanoTimestampConfigTest`) —
check `ss -ltnp | grep -E ':(9000|9003|9090)'` before blaming the code.

- [ ] **Step 8: Commit**

```bash
git add core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java \
        core/src/test/java/io/questdb/test/griffin/CompositeEarliestRefusalTest.java \
        docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md
git commit -m "fix(griffin): refuse SET FORMAT PARQUET on a composite table at the statement

It was accepted, and the NEXT commit suspended the table via TableWriter's own
FORMAT PARQUET guard -- so an unrelated insert took the blame for this statement.
Wave 0's rule is that a refusal fires at the statement that caused it.

The writer-side guard stays: it protects non-SQL paths, and gates move rather than
vanish. Sub-project 3 removes both when a parquet cell becomes addressable.

Negative control: 1 of 2 tests fails with the compile-time check removed."
```

---

### Task 3: WITHDRAWN — indexed columns are not a trap

**Status: built, measured, withdrawn 2026-08-17.** Kept here rather than deleted, because the
reasoning error is more useful than the outcome.

The task was implemented as written and passed its own tests (4/4, with 2 of 8 failing without the
gate). Running the wider suite then produced **35 errors across 6 suites** —
`CompositeCellPruningTest`, `CompositeReadShapesTest`, `CompositeUnsupportedOpsTest`,
`CompositeReadEndToEndTest`, `CompositeEndToEndTest`, `CompositeFastAppendTest`. Reverting restored
429/429, so the gate was the sole cause.

**Why the premise was false.** `CompositeCellPruningTest` is an ~860-line suite built specifically
around indexed composite tables: an indexed DIMENSION column delivers cell pruning, which is the
core value of subpartitioning. The factory audit of the same day had already found
`WHERE exch = 'E1'` matching the plain twin through `DeferredSingleSymbolFilterPageFrame` +
`Index forward scan`. The evidence that indexes work was in hand before this task was written.

**And the invariant was never violated.** The indexed-WHERE refusal fires at the `SELECT` that used
the unsupported shape — that IS the statement that caused it. Wave-0 item 2 conflated two different
statements: a `CREATE` that legitimately succeeded, and a later `SELECT` that was legitimately
refused. Each already refuses at its own earliest point.

**Lesson for the remaining sub-projects:** "accepted here, refused there" is only a trap when it is
the SAME statement's consequence surfacing later. Two statements with independent outcomes is
ordinary partial support. Check which one you have before adding a gate.

Sub-project 9B/9C still makes the remaining index shapes cell-aware; nothing about that changes.

## Self-Review

**Spec coverage.** Wave 0 in the closure index has three items: FORMAT PARQUET → Task 2; indexed
columns → Task 3; O3 purge prove-or-escalate → Task 1. All three covered.

**Placeholder scan.** No TBD/TODO. Every step has the code or the exact command. Task 1's two
outcomes are both specified concretely rather than left as "handle the result".

**Type consistency — verified against the code, not assumed:**

| Claim | Evidence |
|---|---|
| `columnModels.keys()` returns `ObjList<CharSequence>` | already used that way at `CreateTableOperationBuilderImpl:712` (`final ObjList<CharSequence> castColumns = columnModels.keys();`) — same file Task 3 edits |
| `CreateTableColumnModel.isIndexed()` exists | used at `CreateTableOperationBuilderImpl:727` and `:813` |
| `TableReaderMetadata.getPartitionSpec()` exists | declared at `TableReaderMetadata:206` |
| `TableReader` is already imported in `SqlCompilerImpl` | confirmed present, so Task 2 adds no import |
| `assertExceptionNoLeakCheck(CharSequence, int, CharSequence)` exists | `AbstractCairoTest:628` |
| the compile-time composite-gate idiom | `SqlCompilerImpl:1470` (DEDUP) opens a reader because `TableRecordMetadata` does not expose `PartitionSpec`; `executeCreateMatView` uses `engine.getReader(...)` because it has no `SqlExecutionContext` — Task 2 is in the same position |

**Known risk, stated rather than discovered:** Task 3 forbids a shape that existing tests outside
`CompositeFactoryCoverageTest` may also use. Step 8 runs the whole `Composite*` and griffin suites
precisely to surface those.

**Outcome: that risk fired, and it was fatal to the task rather than a cost to absorb.** 35 errors
across 6 suites revealed that indexed composite tables are a supported, tested capability, not a
trap — so Task 3 was withdrawn rather than paid for. The risk note was right to exist and wrong in
its assumption that the fallout would merely be tests to update.
