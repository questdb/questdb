# Composite Supported-Surface Verification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a seeded differential fuzz harness proving a composite table is indistinguishable from its plain twin, make gates first-class fuzz outcomes, and close the mat-view silent-wrong risk.

**Architecture:** A composite-owned `CompositeFuzzRunner` composes QuestDB's existing `FuzzTransactionGenerator` and `Fuzz*Operation` types, but owns table creation (composite subject + plain reference), the apply loop, gate expectations, anti-vacuity counters and fault injection. Deterministic matrix tests cover what randomness reaches unreliably. One production change: a mat-view composite gate.

**Tech Stack:** Java 25, JUnit 4, Maven (offline), QuestDB core test fixtures (`AbstractCairoTest`), `io.questdb.test.fuzz` generator package.

**Spec:** `docs/superpowers/specs/2026-08-11-composite-supported-surface-verification-design.md`

## Global Constraints

- Worktree `/home/nick/claude/wt/oss/composite-partitioning`, branch `feat/composite-partitioning`.
- Every command: `export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64` and `export QDB_TEST_TMPDIR=/dev/shm`. Use `mvn -o`.
- **Never** `git checkout`/`git stash` in this worktree — it holds 160+ unpushed commits.
- Port **9003** is held by a local QuestDB. `ExpParquetExportTest#testParquetExportReadOnlyHttp` and `#testParquetExportDisabledReadOnlyInstance` will ERROR with `could not bind socket`. Environmental — do not kill that process, do not "fix" those tests.
- Flag defaults stay: `cairo.wal.composite.fastappend.enabled=true`, `cairo.wal.composite.fastappend.max.open.cells=64`.
- This sub-project changes **no production behaviour except Task 9's mat-view gate**. Any other production edit is out of scope — raise it, don't do it.
- One narrow exception, already sanctioned: Task 4 may add `@TestOnly` counter accessors to `TableWriter` **if and only if** no existing composite counter exposes what it needs. Accessors only — no behaviour, no new counters on the hot path.
- No probe/instrumentation may survive a commit: `grep -rn "PROBE-" core/src` must be empty before every commit.
- Every guard test needs a negative control proving it fails when the thing it guards is broken.

---

### Task 1: Harness skeleton — subject, reference, one applied transaction list

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java`
- Create: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzTest.java`

**Interfaces:**
- Produces: `CompositeFuzzRunner.of(CairoEngine, Rnd)`, `.createTables(String base)`, `.applyToBoth(ObjList<FuzzTransaction>)`, `.assertTwinEqual()`. Tasks 2–5 extend this class; Task 8 adds fault injection to `applyToBoth`.

- [ ] **Step 1: Write the failing test**

```java
package io.questdb.test.cairo.fuzz;

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CompositeFuzzTest extends AbstractCairoTest {

    @Test
    public void testFixedSeedTwinEquality() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(1234L, 5678L));
            runner.createTables("fuzz1");
            runner.applyGeneratedTransactions(200, 20);
            runner.assertTwinEqual();
        });
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `mvn -o -pl core surefire:test -Dtest='CompositeFuzzTest' -DfailIfNoTests=false`
Expected: FAIL — `cannot find symbol: CompositeFuzzRunner`.

- [ ] **Step 3: Implement the minimal runner**

`CompositeFuzzRunner` builds one column model and emits two DDLs from it, so subject and reference can never drift:

```java
package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzTransaction;

public class CompositeFuzzRunner {
    private final CairoEngine engine;
    private final Rnd rnd;
    private String compositeName;
    private String plainName;

    private CompositeFuzzRunner(CairoEngine engine, Rnd rnd) {
        this.engine = engine;
        this.rnd = rnd;
    }

    public static CompositeFuzzRunner of(CairoEngine engine, Rnd rnd) {
        return new CompositeFuzzRunner(engine, rnd);
    }

    public String compositeName() {
        return compositeName;
    }

    public String plainName() {
        return plainName;
    }

    /**
     * ONE column model, TWO DDLs. The reference differs from the subject only in the partition
     * clause, so a divergence can never come from the schema.
     */
    public void createTables(String base) throws SqlException {
        this.compositeName = base + "_composite";
        this.plainName = base + "_plain";
        final String cols = "(ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE, qty LONG)";
        AbstractCairoTest.execute("CREATE TABLE " + compositeName + " " + cols
                + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        AbstractCairoTest.execute("CREATE TABLE " + plainName + " " + cols
                + " TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    public void applyGeneratedTransactions(int rowCount, int transactionCount) throws Exception {
        ObjList<FuzzTransaction> transactions = generate(rowCount, transactionCount);
        applyToBoth(transactions);
    }

    public void assertTwinEqual() throws SqlException {
        AbstractCairoTest.assertSqlCursors(
                "SELECT * FROM " + plainName + " ORDER BY ts",
                "SELECT * FROM " + compositeName + " ORDER BY ts"
        );
    }
}
```

Implement `generate(...)` by delegating to `FuzzTransactionGenerator.generateSet(...)` against the **plain** table's metadata (the schemas are identical, and the plain reader is never itself under test), and `applyToBoth(...)` by applying each `FuzzTransaction`'s operations to both tables through `engine.getTableWriterAPI(...)`, then `AbstractCairoTest.drainWalQueue()`.

- [ ] **Step 4: Run to verify it passes**

Run: `mvn -o -pl core surefire:test -Dtest='CompositeFuzzTest' -DfailIfNoTests=false`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

- [ ] **Step 5: Prove it is not vacuous**

Temporarily change the subject DDL's `PARTITION BY DAY, exch` to `PARTITION BY DAY` (making both tables plain). The test must still pass — confirming the comparison works — then restore. Next, temporarily corrupt the comparison by ordering the subject `ORDER BY ts DESC`; the test must FAIL. Restore both. **Do not commit either mutation.**

- [ ] **Step 6: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/fuzz/
git commit -m "test(composite): differential fuzz harness skeleton (subject + plain twin)"
```

---

### Task 2: Randomized composite axes

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java`
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzTest.java`

**Interfaces:**
- Produces: `CompositeFuzzRunner.axes()` returning the resolved `Axes` (dimension count/kinds, layout, clustering, cardinality, flag), used by Task 4's failure messages and Task 7's matrix.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testAxesVaryAcrossSeeds() throws Exception {
        assertMemoryLeak(() -> {
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (int i = 0; i < 12; i++) {
                CompositeFuzzRunner r = CompositeFuzzRunner.of(engine, new Rnd(i, i * 7L));
                r.createTables("axes" + i);
                seen.add(r.axes().toString());
            }
            org.junit.Assert.assertTrue("axes must vary across seeds, saw " + seen, seen.size() > 3);
        });
    }
```

- [ ] **Step 2: Run to verify it fails**

Expected: FAIL — `cannot find symbol: axes()`.

- [ ] **Step 3: Implement the axes**

Add an `Axes` value class resolved from `rnd` in `createTables`, and build the subject DDL from it:

```java
    public static final class Axes {
        public final int dimCount;          // 1..3
        public final String[] dimClauses;   // e.g. "exch", "hash(sym, 32)", "truncate(sym, 3)"
        public final boolean hivelayout;    // false => LAYOUT PLAIN
        public final boolean clustered;     // ORDER BY sym
        public final int cardinality;       // distinct dimension values to generate
        public final boolean fastAppend;    // cairo.wal.composite.fastappend.enabled

        @Override
        public String toString() {
            return "dims=" + String.join(",", dimClauses)
                    + " layout=" + (hivelayout ? "HIVE" : "PLAIN")
                    + " clustered=" + clustered
                    + " cardinality=" + cardinality
                    + " fastAppend=" + fastAppend;
        }
    }
```

Cardinality is drawn from `{3, 16, 64, 96}` — small, medium, at the open-cell cap, above it. `fastAppend` is applied with `AbstractCairoTest.setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, …)` before table creation. The subject DDL becomes `PARTITION BY DAY, <dimClauses joined by ", ">` plus `ORDER BY sym` when clustered and `LAYOUT PLAIN` when not Hive.

- [ ] **Step 4: Run to verify it passes**

Run: `mvn -o -pl core surefire:test -Dtest='CompositeFuzzTest' -DfailIfNoTests=false`
Expected: `Tests run: 2, Failures: 0`

- [ ] **Step 5: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/fuzz/
git commit -m "test(composite): randomized composite axes (dims, layout, clustering, cardinality, flag)"
```

---

### Task 3: The full comparison oracle

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java`

**Interfaces:**
- Consumes: Task 1's `assertTwinEqual()`.
- Produces: `assertTwinEqual()` covering all seven shapes from spec §4.4.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testAllShapesCompared() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(99L, 42L));
            runner.createTables("shapes");
            runner.applyGeneratedTransactions(500, 30);
            runner.assertTwinEqual();
            org.junit.Assert.assertEquals("all seven shapes must be compared",
                    7, runner.comparedShapeCount());
        });
    }
```

- [ ] **Step 2: Run to verify it fails**

Expected: FAIL — `cannot find symbol: comparedShapeCount()`.

- [ ] **Step 3: Implement the seven comparisons**

Each shape increments the counter once, even when it issues two queries. The seven, in order:
(1) full scan, compared forward AND backward; (2) `count(*)`/`min(ts)`/`max(ts)`; (3) `LATEST ON ts PARTITION BY sym`; (4) `SAMPLE BY` with a keyed aggregate; (5) dimension-filtered `=` and `IN`, using one value known present and one known absent; (6) a timestamp interval crossing a partition boundary; (7) a window-join with the table as slave. Composite-only sanity — `table_partitions()` row count equals distinct `(day, cell)` pairs and every named directory exists — is asserted separately, not compared to the twin.

- [ ] **Step 4: Run to verify it passes**

Expected: `Tests run: 3, Failures: 0`

- [ ] **Step 5: Commit**

```bash
git commit -am "test(composite): full comparison oracle across all seven read shapes"
```

---

### Task 4: Anti-vacuity counters and floors

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java`
- Modify (if the existing composite counters are insufficient): `core/src/main/java/io/questdb/cairo/TableWriter.java` — `@TestOnly` accessors **only**

**Interfaces:**
- Produces: `assertExercised()` — fails the run when a floor is unmet.

This is the task that decides whether the whole harness is worth anything. A fuzz that never routes a second cell passes while testing nothing.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testRunMustProveItExercisedComposite() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(7L, 7L));
            runner.createTables("exercised");
            runner.applyGeneratedTransactions(800, 40);
            runner.assertTwinEqual();
            runner.assertExercised();   // must throw if the run was vacuous
        });
    }

    @Test
    public void testFloorsFailAVacuousRun() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(7L, 7L));
            runner.createTables("vacuous");
            // no transactions applied at all -> nothing routed
            try {
                runner.assertExercised();
                org.junit.Assert.fail("expected the anti-vacuity floors to reject an unexercised run");
            } catch (AssertionError expected) {
                io.questdb.test.tools.TestUtils.assertContains(expected.getMessage(), "distinct cellKeys");
            }
        });
    }
```

The second test is the negative control: it proves the floors can fail.

- [ ] **Step 2: Run to verify both fail**

Expected: FAIL — `cannot find symbol: assertExercised()`.

- [ ] **Step 3: Implement counters and floors**

Floors, per spec §4.5: distinct routed cellKeys ≥ 2 (≥ 1 when the axes chose a single-cell shape); composite O3 merge commits ≥ 1; fast-append commits ≥ 1 when the flag is on; rows landing in a non-last partition ≥ 1; gated operations attempted ≥ 1 (wired in Task 5). Read distinct cellKeys from `table_partitions()` on the subject; read path counters from the existing composite counters, adding `@TestOnly` accessors only where none exists. Every failure message names the seed and `axes().toString()`.

- [ ] **Step 4: Run to verify both pass**

Expected: `Tests run: 5, Failures: 0`

- [ ] **Step 5: Commit**

```bash
git commit -am "test(composite): anti-vacuity floors — a fuzz run must prove it routed cells"
```

---

### Task 5: Gates as first-class outcomes

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java`

**Interfaces:**
- Produces: `CompositeFuzzRunner.Support` enum and the classification map consumed by Task 6's guard.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testGatedOperationThrowsAndLeavesNoDamage() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(11L, 13L));
            runner.createTables("gated");
            runner.applyGeneratedTransactions(400, 20);
            runner.assertTwinEqual();

            long before = runner.compositeRowCount();
            runner.applyGatedOperation("ALTER TABLE " + runner.compositeName() + " DROP COLUMN qty");
            org.junit.Assert.assertEquals("a rejected op must not change row count",
                    before, runner.compositeRowCount());
            runner.assertTwinEqual();   // and must leave the table twin-equal
        });
    }
```

- [ ] **Step 2: Run to verify it fails**

Expected: FAIL — `cannot find symbol: applyGatedOperation`.

- [ ] **Step 3: Implement the classification and rejection protocol**

```java
    public enum Support { SUPPORTED, GATED }

    /**
     * Applies a statement expected to be refused for a composite table, then asserts the refusal
     * left NO damage: still readable, row count unchanged, still twin-equal. A gate that throws
     * after partially mutating _txn or the directory tree passes every existing test and fails here.
     */
    public void applyGatedOperation(String sql) throws Exception {
        gatedAttempted++;
        try {
            AbstractCairoTest.execute(sql);
            throw new AssertionError("expected a composite gate to reject: " + sql);
        } catch (io.questdb.cairo.CairoException e) {
            io.questdb.test.tools.TestUtils.assertContains(e.getFlyweightMessage(), "composite");
        }
    }
```

The classification map keys each `Fuzz*Operation` class to a `Support` value, per spec §5.1. The reference table skips any operation classified `GATED`, keeping the twin aligned.

- [ ] **Step 4: Run to verify it passes**

Expected: `Tests run: 6, Failures: 0`

- [ ] **Step 5: Commit**

```bash
git commit -am "test(composite): gated ops must throw AND leave the table undamaged"
```

---

### Task 6: Operation-coverage guard

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzOpCoverageTest.java`

- [ ] **Step 1: Write the test**

It enumerates the `Fuzz*Operation` implementations in `io.questdb.test.fuzz` and fails when one is absent from Task 5's classification map, naming the class and stating that the decision is supported-vs-gated. Enumerate by scanning the package directory on the test classpath rather than a hard-coded list — a hard-coded list would defeat the purpose.

- [ ] **Step 2: Run to verify it passes**

Expected: `Tests run: 1, Failures: 0`

- [ ] **Step 3: Negative control**

Temporarily remove one entry from the classification map; the guard must FAIL naming that class. Restore. **Do not commit the removal.**

- [ ] **Step 4: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzOpCoverageTest.java
git commit -m "test(composite): fail when a new fuzz operation is left unclassified"
```

---

### Task 7: Deterministic matrix completion

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/CompositeLayoutPlainTest.java`
- Modify: existing composite tests only where a gap is found

- [ ] **Step 1: Write the tests**

Per spec §6: `LAYOUT PLAIN` routing, on-disk names and `SHOW CREATE TABLE` round-trip (today only 2–3 files touch PLAIN versus 14 for HIVE); expression dimensions end-to-end against a plain twin including a value that changes bucket; fast-append flag **off** parity; the 64-cell cap boundary and eviction at 96 asserting non-truncating close; day-roll with multiple live cells; a never-routed empty composite table on the read side.

- [ ] **Step 2: Run**

Run: `mvn -o -pl core surefire:test -Dtest='CompositeLayoutPlainTest,Composite*' -DfailIfNoTests=false`
Expected: all green; total composite tests rises above the current 315.

- [ ] **Step 3: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/CompositeLayoutPlainTest.java
git commit -m "test(composite): deterministic matrix — LAYOUT PLAIN, expression dims, flag-off, cap boundary"
```

---

### Task 8: Crash and power-loss injection

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java`
- Create: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzCrashTest.java`

- [ ] **Step 1: Write the failing test**

A run with fault injection enabled selects a random commit, fails a write at it, reopens the engine, replays, and asserts twin equality. Crash points sampled: `_txn` commit, `_cv` commit, cell column append, cell segment open, WAL apply mid-drain. Reuse the `FilesFacade` fault-injection approach already proven in `CompositeFastAppendCrashTest`.

- [ ] **Step 2: Run to verify it fails**

Expected: FAIL — the injection hook does not exist yet.

- [ ] **Step 3: Implement**

Gate on `-Dcomposite.fuzz.crash=true`; when off, the harness behaves exactly as Tasks 1–5. Each recovery must land on one of exactly two acceptable states — the transaction applied in full, or not at all.

- [ ] **Step 4: Run both arms**

```bash
mvn -o -pl core surefire:test -Dtest='CompositeFuzzCrashTest' -Dcomposite.fuzz.crash=true -DfailIfNoTests=false
mvn -o -pl core surefire:test -Dtest='CompositeFuzzCrashTest' -DfailIfNoTests=false
```
Expected: both green; the second is a no-injection sanity run.

- [ ] **Step 5: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/fuzz/
git commit -m "test(composite): crash/power-loss injection in the differential fuzz (flag-gated)"
```

---

### Task 9: Materialized-view gate — the one production change

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` (~`:4576–4588`)
- Create: `core/src/test/java/io/questdb/test/griffin/CompositeMatViewGateTest.java`

**Interfaces:**
- Consumes: nothing. Sub-project 7 removes this gate once mat views over composite are proven.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testMatViewOverCompositeBaseIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("INSERT INTO base VALUES ('2024-01-01T00:00:00Z', 'BTC', 1.0)");
            drainWalQueue();
            try {
                execute("CREATE MATERIALIZED VIEW mv AS ("
                        + "SELECT ts, avg(px) AS ap FROM base SAMPLE BY 1h) PARTITION BY DAY");
                Assert.fail("expected rejection of a composite base");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite");
            }
        });
    }

    @Test
    public void testMatViewOverPlainBaseStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base2 (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mv2 AS ("
                    + "SELECT ts, avg(px) AS ap FROM base2 SAMPLE BY 1h) PARTITION BY DAY");
        });
    }
```

The second test is the positive control: the gate must not over-reach onto plain bases.

- [ ] **Step 2: Run to verify the first fails**

Expected: the composite case does NOT throw (no gate exists yet), so the test fails on `Assert.fail`.

- [ ] **Step 3: Add the gate**

In the existing base-table validation block, alongside `base table must be a WAL table` and `live views are not allowed as base tables in V1`, reject a base whose partition spec has dimensions, throwing `SqlException` at `op.getBaseTableNamePosition()` so the error carries a caret position like its neighbours. Message must contain `composite`.

- [ ] **Step 4: Run to verify both pass**

Run: `mvn -o -pl core surefire:test -Dtest='CompositeMatViewGateTest' -DfailIfNoTests=false`
Expected: `Tests run: 2, Failures: 0`

- [ ] **Step 5: Regression-check the mat-view suites**

Run: `mvn -o -pl core surefire:test -Dtest='*MatView*' -DfailIfNoTests=false`
Expected: no new failures.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java core/src/test/java/io/questdb/test/griffin/CompositeMatViewGateTest.java
git commit -m "fix(griffin): reject materialized views over a composite base (temporary gate)"
```

---

### Task 10: The two silent skips

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/CompositeSilentSkipTest.java`

- [ ] **Step 1: Write the tests**

Split-fragment squash and symbol-capacity autoscale are silently skipped for composite. Each test asserts the skip happens **and** that the table remains correct and twin-equal afterwards — a silent skip is acceptable only if provably harmless.

- [ ] **Step 2: Run**

Expected: `Tests run: 2, Failures: 0`

- [ ] **Step 3: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/CompositeSilentSkipTest.java
git commit -m "test(composite): prove the two silent skips are harmless"
```

---

### Task 11: CI wiring and full regression

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzTest.java` (PR profile: fixed seeds, bounded counts)
- Create: `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzNightlyTest.java` (random seeds, larger counts, crash on)

- [ ] **Step 1: Bound the PR profile**

`CompositeFuzzTest` uses a small fixed seed set and bounded row/transaction counts; target under two minutes. QuestDB CI cost has roughly doubled over twelve months, so this stays deliberately bounded.

- [ ] **Step 2: Time it**

```bash
time mvn -o -pl core surefire:test -Dtest='CompositeFuzzTest' -DfailIfNoTests=false
```
Expected: under 2 minutes wall clock. If over, reduce counts — not seeds.

- [ ] **Step 3: Full regression**

```bash
mvn -o -pl core surefire:test \
  -Dtest='Composite*,*Parquet*,O3*,Wal*,Commit*,TxReaderTest,TxWriterTest,ShowPartitionsTest,CoveringIndexBlockApplySealTest,LiveView*,*MatView*' \
  -DfailIfNoTests=false 2>&1 | tail -5
```
Expected: 0 failures; exactly 2 errors, both the known `:9003` port binds.

- [ ] **Step 4: Verify no instrumentation survived**

```bash
grep -rn "PROBE-" core/src --include=*.java; echo "exit=$? (1 = clean)"
git status --short
```

- [ ] **Step 5: Commit**

```bash
git commit -am "test(composite): PR and nightly fuzz profiles"
```

---

## Definition of Done

- [ ] `CompositeFuzzTest` green with fixed seeds, under two minutes.
- [ ] Anti-vacuity floors proven capable of failing (Task 4's negative control).
- [ ] `CompositeFuzzOpCoverageTest` proven capable of failing (Task 6's negative control).
- [ ] Every gated operation asserted to throw **and** leave the table twin-equal.
- [ ] Crash-injection arm green under `-Dcomposite.fuzz.crash=true`.
- [ ] Mat-view gate rejects composite bases, plain bases unaffected.
- [ ] Both silent skips proven harmless.
- [ ] Full regression: 0 failures, only the 2 known port-bind errors.
- [ ] `grep -rn "PROBE-" core/src` empty; working tree clean.
