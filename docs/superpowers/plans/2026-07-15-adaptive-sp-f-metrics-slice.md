# SP-F Metrics Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the adaptive durable-frontier lag, epoch cadence, and recovery events as Prometheus metrics + two `wal_tables()` columns, so the Prove-it track (SP-C/SP-D) can interpret its results and ops can alert on the durable frontier falling behind.

**Architecture:** Extend the existing `WalMetrics` (the established home for `wal_apply_seq_txn`/`writer_txn`) with one global gauge + two counters. Push the gauge delta from inside `SeqTxnTracker.setLocalDurableSeqTxn` (mirrors how `addSeqTxn` is already pushed). Increment the counters at the two event sites (epoch advance, recovery). Add two per-table columns to `wal_tables()` for drill-down. Durable-frontier lag is *computed* (`wal_apply_seq_txn − wal_apply_local_durable_seq_txn`), never a third push-site. No per-table Prometheus labels.

**Tech Stack:** Java (QuestDB core), JDK25, JUnit4 (`AbstractCairoTest`), Prometheus metrics (`io.questdb.metrics`).

## Global Constraints

- Worktree `~/claude/wt/oss/adaptive-commit`, branch `nw_adaptive_commit`. OSS core only.
- JDK25: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64`.
- Test/build command (single class): `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='<Class>#<method>' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`. Read the authoritative report at `core/target/surefire-reports/<FQCN>.txt`.
- Metric registered names: `wal_apply_local_durable_seq_txn`, `wal_adaptive_epoch_advances`, `wal_adaptive_recovery_events`. Prometheus scrape tags add the `questdb_` prefix (tests read `questdb_wal_...` via `TestUtils.getMetricValue(engine, tag)`).
- Adaptive table in tests: `node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive")` then a `... wal` table; drive apply with `drainWalQueue()`.
- Test style: exact-value assertions where deterministic; assert the metric against the tracker's own getter to avoid brittle magic numbers (mirrors `SeqTxnMetricsTest`).
- DRY: do NOT create a new metrics class; reuse `WalMetrics`. YAGNI: no lag gauge, no per-table labels.

## File Structure

- `core/src/main/java/io/questdb/cairo/wal/WalMetrics.java` — +1 `LongGauge`, +2 `Counter`, their updater methods, register in ctor, reset in `clear()`. (Owner of all adaptive metric objects.)
- `core/src/main/java/io/questdb/cairo/wal/seq/SeqTxnTracker.java` — push the local-durable gauge delta inside `setLocalDurableSeqTxn` (line 308). (Single push-site for the frontier gauge.)
- `core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java` — increment epoch-advances counter in `maybeAdvanceDurableEpoch` (line 677). (Epoch event site.)
- `core/src/main/java/io/questdb/cairo/RecoveryCoordinator.java` — increment recovery-events counter in `recoverTable` success path (after line 199). (Recovery event site.)
- `core/src/main/java/io/questdb/griffin/engine/functions/catalogue/WalTableListFunctionFactory.java` — +2 columns (`localDurableSeqTxn`, `lastEpochTs`). (Per-table drill-down.)
- `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveMetricsTest.java` — NEW: Tasks 1 & 2 tests.
- `core/src/test/java/io/questdb/test/cairo/RecoveryCoordinatorTest.java` — Task 3 test (existing recovery harness).
- `core/src/test/java/io/questdb/test/griffin/WalTableListFunctionFactoryTest.java` — Task 4 test (or the existing `wal_tables()` test class).

---

### Task 1: `wal_apply_local_durable_seq_txn` gauge (durable-frontier)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/WalMetrics.java`
- Modify: `core/src/main/java/io/questdb/cairo/wal/seq/SeqTxnTracker.java:308-315`
- Test: `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveMetricsTest.java` (create)

**Interfaces:**
- Produces: `WalMetrics.addLocalDurableSeqTxn(long txnDelta)`; metric `wal_apply_local_durable_seq_txn`.
- Consumes: existing `SeqTxnTracker.metrics` (a `Metrics`, already used for `metrics.walMetrics().addSeqTxn(...)`).

- [ ] **Step 1: Write the failing test** — create `AdaptiveMetricsTest.java`:

```java
package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdaptiveMetricsTest extends AbstractCairoTest {

    @Test
    public void testLocalDurableSeqTxnGaugeAdvancesUnderAdaptive() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values (0, 1)");
            execute("insert into x values (1000000, 2)");
            execute("insert into x values (2000000, 3)");
            drainWalQueue();

            SeqTxnTracker tracker = engine.getTableSequencerAPI()
                    .getTxnTracker(engine.verifyTableName("x"));
            long localDurable = tracker.getLocalDurableSeqTxn();
            assertTrue("adaptive commits should make the frontier durable", localDurable > 0);
            // Global gauge == this single table's local-durable frontier.
            assertEquals(localDurable,
                    TestUtils.getMetricValue(engine, "questdb_wal_apply_local_durable_seq_txn"));
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='AdaptiveMetricsTest#testLocalDurableSeqTxnGaugeAdvancesUnderAdaptive' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `getMetricValue` returns 0 (metric `questdb_wal_apply_local_durable_seq_txn` not registered / never pushed) while `localDurable > 0`.

- [ ] **Step 3: Implement — add the gauge to `WalMetrics.java`**

Add the field (with the other gauges, ~line 39):
```java
    private final LongGauge localDurableSeqTxnGauge;
```
Register in the constructor (after the `writerTxnGauge` line, ~line 50):
```java
        this.localDurableSeqTxnGauge = metricsRegistry.newAtomicLongGauge("wal_apply_local_durable_seq_txn");
```
Add the adder method (next to `addSeqTxn`, ~line 68):
```java
    public void addLocalDurableSeqTxn(long txnDelta) {
        localDurableSeqTxnGauge.add(txnDelta);
    }
```
Reset in `clear()` (with the other gauges, ~line 80):
```java
        localDurableSeqTxnGauge.setValue(0);
```

- [ ] **Step 4: Implement — push the delta from `SeqTxnTracker.setLocalDurableSeqTxn`**

Replace the body at `SeqTxnTracker.java:308-315`:
```java
    public void setLocalDurableSeqTxn(long seqTxn) {
        // Monotone CAS-free update: ADAPTIVE commits publish strictly increasing seqTxns per table
        // (the sequencer is single-writer per table), so a plain volatile write suffices.
        // Guard against any ordering anomaly with a max() check.
        if (seqTxn > localDurableSeqTxn) {
            // Push the delta to the global durable-frontier gauge, clamping the -1 initial to 0
            // (mirrors the addSeqTxn(newSeqTxn - Math.max(0, stxn)) pattern above).
            metrics.walMetrics().addLocalDurableSeqTxn(seqTxn - Math.max(0, localDurableSeqTxn));
            localDurableSeqTxn = seqTxn;
        }
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='AdaptiveMetricsTest#testLocalDurableSeqTxnGaugeAdvancesUnderAdaptive' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/wal/WalMetrics.java \
        core/src/main/java/io/questdb/cairo/wal/seq/SeqTxnTracker.java \
        core/src/test/java/io/questdb/test/cairo/wal/AdaptiveMetricsTest.java
git commit -m "feat(metrics): wal_apply_local_durable_seq_txn gauge (adaptive durable frontier)"
```

---

### Task 2: `wal_adaptive_epoch_advances` counter (epoch cadence)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/WalMetrics.java`
- Modify: `core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java:677` (`maybeAdvanceDurableEpoch`)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveMetricsTest.java`

**Interfaces:**
- Produces: `WalMetrics.incrementEpochAdvances()`; metric `wal_adaptive_epoch_advances`.
- Consumes: `ApplyWal2TableJob.metrics` (the `WalMetrics` field, line 97, `= engine.getMetrics().walMetrics()`).

- [ ] **Step 1: Write the failing test** — add to `AdaptiveMetricsTest.java`:

```java
    @Test
    public void testEpochAdvancesCounterIncrements() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, "0"); // advance every batch
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            long before = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_epoch_advances");
            execute("insert into x values (0, 1)");
            drainWalQueue();
            long after = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_epoch_advances");
            assertTrue("each adaptive apply batch advances the durable epoch", after > before);
        });
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='AdaptiveMetricsTest#testEpochAdvancesCounterIncrements' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `after == before == 0` (counter not registered/incremented).

- [ ] **Step 3: Implement — add the counter to `WalMetrics.java`**

Field (~line 35):
```java
    private final Counter epochAdvancesCounter;
```
Register in ctor (~line 50):
```java
        this.epochAdvancesCounter = metricsRegistry.newCounter("wal_adaptive_epoch_advances");
```
Method (~line 72):
```java
    public void incrementEpochAdvances() {
        epochAdvancesCounter.inc();
    }
```
Reset in `clear()`:
```java
        epochAdvancesCounter.reset();
```

- [ ] **Step 4: Implement — increment on epoch publish in `ApplyWal2TableJob.maybeAdvanceDurableEpoch`**

At `ApplyWal2TableJob.java`, immediately after `tracker.setDurableEpochSeqTxn(epochSeqTxn);` (line 776 — the point the epoch is published), add:
```java
            metrics.incrementEpochAdvances();
```
(`metrics` is the `WalMetrics` field on `ApplyWal2TableJob`, line 97.)

- [ ] **Step 5: Run test to verify it passes**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='AdaptiveMetricsTest#testEpochAdvancesCounterIncrements' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/wal/WalMetrics.java \
        core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java \
        core/src/test/java/io/questdb/test/cairo/wal/AdaptiveMetricsTest.java
git commit -m "feat(metrics): wal_adaptive_epoch_advances counter (epoch cadence)"
```

---

### Task 3: `wal_adaptive_recovery_events` counter (recovery detector)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/WalMetrics.java`
- Modify: `core/src/main/java/io/questdb/cairo/RecoveryCoordinator.java:198-199` (`recoverTable` success path)
- Test: `core/src/test/java/io/questdb/test/cairo/RecoveryCoordinatorTest.java`

**Interfaces:**
- Produces: `WalMetrics.incrementRecoveryEvents()`; metric `wal_adaptive_recovery_events`.
- Consumes: `RecoveryCoordinator.engine` (line 73) → `engine.getMetrics().walMetrics()`.

- [ ] **Step 1: Write the failing test** — add a method to `RecoveryCoordinatorTest.java`, mirroring the existing recovery scenario `testRecoverSkipsEpochAheadOfRestoredTxn` but for a *successful* rewind (a table whose valid durable epoch is BEHIND the live `_txn`, so `recoverTable` restores it). Reuse this file's existing helpers (`copyTableFile`, `epochArtifactExists`). Assert the metric after `recover()`:

```java
    @Test
    public void testRecoverIncrementsRecoveryEventsMetric() throws Exception {
        assertMemoryLeak(() -> {
            // Arrange: an adaptive WAL table advanced to seqTxn=6, with a valid durable epoch
            // captured at seqTxn=3 (epoch BEHIND live _txn) — the happy-path rewind case.
            // (Mirror the arrange block of testRecoverSkipsEpochAheadOfRestoredTxn, but DO NOT
            //  restore the older _txn over the live one: leave live _txn at 6 and the epoch trio at 3,
            //  which is the normal "rewind to epoch" recovery input.)
            long before = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_recovery_events");

            new io.questdb.cairo.RecoveryCoordinator(engine).recover();

            long after = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_recovery_events");
            assertTrue("a table recovered (rewound) at boot must increment the counter", after > before);
        });
    }
```

> Implementer note: read `testRecoverSkipsEpochAheadOfRestoredTxn` and the sibling happy-path recovery test in this file for the exact table/epoch setup calls; the assertion above is the only new behavior. Add `import io.questdb.test.tools.TestUtils;` and `import static org.junit.Assert.assertTrue;` if absent.

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='RecoveryCoordinatorTest#testRecoverIncrementsRecoveryEventsMetric' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `after == before` (counter absent / not incremented).

- [ ] **Step 3: Implement — add the counter to `WalMetrics.java`**

Field:
```java
    private final Counter recoveryEventsCounter;
```
Ctor:
```java
        this.recoveryEventsCounter = metricsRegistry.newCounter("wal_adaptive_recovery_events");
```
Method:
```java
    public void incrementRecoveryEvents() {
        recoveryEventsCounter.inc();
    }
```
`clear()`:
```java
        recoveryEventsCounter.reset();
```

- [ ] **Step 4: Implement — increment on successful recovery in `RecoveryCoordinator.recoverTable`**

In `recoverTable`, immediately after the two restore calls at lines 198-199 (`restoreFile(... TXN_FILE_NAME)` / `restoreFile(... COLUMN_VERSION_FILE_NAME)` — the point a table has actually been rewound to its epoch), add:
```java
        engine.getMetrics().walMetrics().incrementRecoveryEvents();
```

- [ ] **Step 5: Run test to verify it passes**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='RecoveryCoordinatorTest#testRecoverIncrementsRecoveryEventsMetric' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS. Also re-run the full class to confirm no regression:
`... -Dtest='RecoveryCoordinatorTest' ...` → all green.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/wal/WalMetrics.java \
        core/src/main/java/io/questdb/cairo/RecoveryCoordinator.java \
        core/src/test/java/io/questdb/test/cairo/RecoveryCoordinatorTest.java
git commit -m "feat(metrics): wal_adaptive_recovery_events counter (recovery detector)"
```

---

### Task 4: `wal_tables()` columns `localDurableSeqTxn` + `lastEpochTs`

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/engine/functions/catalogue/WalTableListFunctionFactory.java`
- Test: `core/src/test/java/io/questdb/test/griffin/WalTableListFunctionFactoryTest.java`

**Interfaces:**
- Consumes: `SeqTxnTracker.getLocalDurableSeqTxn()` (LONG), `SeqTxnTracker.getLastEpochTs()` (long ms → TIMESTAMP).
- Produces: two new trailing `wal_tables()` columns.

- [ ] **Step 1: Write the failing test** — add to `WalTableListFunctionFactoryTest.java`:

```java
    @Test
    public void testWalTablesExposesLocalDurableAndLastEpochTs() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values (0, 1)");
            drainWalQueue();
            // Both columns exist and localDurableSeqTxn is positive for an adaptive table post-apply.
            assertSql(
                    "hasLocalDurable\n" +
                            "true\n",
                    "select localDurableSeqTxn > 0 as hasLocalDurable from wal_tables() where name = 'x'"
            );
            // lastEpochTs column is selectable as a TIMESTAMP (no error).
            assertSql(
                    "count\n" +
                            "1\n",
                    "select count() as count from wal_tables() where name = 'x' and lastEpochTs is not null or name = 'x'"
            );
        });
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='WalTableListFunctionFactoryTest#testWalTablesExposesLocalDurableAndLastEpochTs' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `Invalid column: localDurableSeqTxn`.

- [ ] **Step 3: Implement — add the two columns in `WalTableListFunctionFactory.java`**

(a) Column index fields (with `durableEpochSeqTxnColumn` etc., ~line 71):
```java
    private static final int lastEpochTsColumn;
    private static final int localDurableSeqTxnColumn;
```
(b) Record fields (in `TableListRecord`, with `durableEpochSeqTxn` etc., ~line 207):
```java
                private long lastEpochTs;
                private long localDurableSeqTxn;
```
(c) `getLong` cases (after the `recoveryIncarnationColumn` case, ~line 253) — TIMESTAMP is a long, returned via `getLong`:
```java
                    if (col == localDurableSeqTxnColumn) {
                        return localDurableSeqTxn;
                    }
                    if (col == lastEpochTsColumn) {
                        return lastEpochTs;
                    }
```
(d) In `switchTo`, initialised branch (after `recoveryIncarnation = ...`, ~line 305):
```java
                            localDurableSeqTxn = seqTxnTracker.getLocalDurableSeqTxn();
                            lastEpochTs = seqTxnTracker.getLastEpochTs();
```
and the not-initialised default branch (after `recoveryIncarnation = 0;`, ~line 321):
```java
                        localDurableSeqTxn = 0;
                        lastEpochTs = 0;
```
(e) Column registration (after the `recoveryIncarnation` registration, ~line 398):
```java
        metadata.add(new TableColumnMetadata("localDurableSeqTxn", ColumnType.LONG));
        localDurableSeqTxnColumn = metadata.getColumnCount() - 1;
        metadata.add(new TableColumnMetadata("lastEpochTs", ColumnType.TIMESTAMP));
        lastEpochTsColumn = metadata.getColumnCount() - 1;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -f pom.xml -pl core test -Dtest='WalTableListFunctionFactoryTest#testWalTablesExposesLocalDurableAndLastEpochTs' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS. Re-run the full class (`-Dtest='WalTableListFunctionFactoryTest'`) — the existing 7 tests must stay green (column-count/order assertions may need the two new trailing columns added; update them if present).

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/io/questdb/griffin/engine/functions/catalogue/WalTableListFunctionFactory.java \
        core/src/test/java/io/questdb/test/griffin/WalTableListFunctionFactoryTest.java
git commit -m "feat(metrics): wal_tables() localDurableSeqTxn + lastEpochTs columns"
```

---

## Self-Review

**1. Spec coverage.** SP-F spec Design-A (three Prometheus metrics via WalMetrics) → Tasks 1-3. Design-B (two `wal_tables()` columns) → Task 4. "Lag computed not pushed" → honored (no lag gauge; both operands exposed). "No per-table labels" → honored (global gauges + SQL drill-down). Acceptance (scrapeable metrics + queryable columns) → covered. The late user-docs slice is explicitly out of this plan (roadmap-separated). No gaps.

**2. Placeholder scan.** No TBD/TODO. Every code step shows complete code. The Task 3 test references an existing sibling test for the *arrange* block (a real, readable codebase test — not a plan placeholder) and gives the exact new assertion; this is the one spot an implementer must read neighboring code, flagged explicitly.

**3. Type consistency.** `WalMetrics`: `addLocalDurableSeqTxn(long)`, `incrementEpochAdvances()`, `incrementRecoveryEvents()` — same names used at every call site (SeqTxnTracker, ApplyWal2TableJob via `metrics.`, RecoveryCoordinator via `engine.getMetrics().walMetrics().`). Metric names identical between registration and the `questdb_`-prefixed test tags. `wal_tables()` column constants (`localDurableSeqTxnColumn`, `lastEpochTsColumn`) consistent across declaration, `getLong`, `switchTo`, and registration. Consistent.
