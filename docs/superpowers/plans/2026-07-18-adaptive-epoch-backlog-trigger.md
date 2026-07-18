# Adaptive durable-epoch time-OR-backlog trigger — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the adaptive durable epoch fire on `interval-elapsed OR un-epoched-row-backlog ≥ cap`, and lengthen the default interval to 60 s, so cadence is decoupled from the WAL-retention / recovery bound.

**Architecture:** A new global config `cairo.adaptive.epoch.max.rows` bounds the un-epoched applied-row backlog. `SeqTxnTracker` gains an apply-worker-local `rowsSinceEpoch` counter; `ApplyWal2TableJob` feeds it each applied batch, adds a `backlogHit` term to the existing cadence gate, and resets it when an epoch publishes. Nothing about the epoch record, its ordering (INV-5), recovery, or read-visibility changes — only *when* `advance()` is called.

**Tech Stack:** Java (JDK25), QuestDB core, JUnit + `AbstractCairoTest` fluent house style.

## Global Constraints

- Target: OSS core, worktree `~/claude/wt/oss/adaptive-commit`, branch `nw_adaptive_commit`.
- JDK25. Build/test module: `core`.
- Test house style: `extends AbstractCairoTest`; `setProperty(...)`, `execute(...)`, `drainWalQueue()`, `assertMemoryLeak(...)`; assert epoch state via `engine.getTableSequencerAPI().getTxnTracker(token).getDurableEpochSeqTxn()`.
- **Untouched:** INV-5 epoch ordering in `advance()`, `RecoveryCoordinator`, read-visibility (reads gate on `localDurableSeqTxn`, never the epoch). The change only affects the epoch *trigger condition* and the interval *default*.
- Cap semantics: `cairo.adaptive.epoch.max.rows` default `5_000_000`; **`≤ 0` disables the cap** (interval-only). The pre-existing **`cairo.adaptive.epoch.interval.ms < 0` disables epochs entirely** and is evaluated first, so it overrides the cap.
- Counter is apply-worker-local (single-threaded per table): plain `long`, no `volatile`, mirroring the existing `lastEpochTs` field.
- Reset lives on the `advance()` success path only (after the epoch publishes), never in its early-return guards.

---

## File Structure

- `core/.../io/questdb/PropertyKey.java` — declare the new config key.
- `core/.../io/questdb/cairo/CairoConfiguration.java` — interface default `getAdaptiveEpochMaxRows()`.
- `core/.../io/questdb/cairo/CairoConfigurationWrapper.java` — delegate the new getter.
- `core/.../io/questdb/PropServerConfiguration.java` — read the key (field + getter) + change interval default `1000`→`60000`.
- `core/.../io/questdb/cairo/wal/seq/SeqTxnTracker.java` — `rowsSinceEpoch` counter (add/get/reset).
- `core/.../io/questdb/cairo/wal/ApplyWal2TableJob.java` — feed counter, extend gate, reset on advance.
- `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveEpochTriggerTest.java` — new behavior test (Task 2).

Two tasks: **Task 1** is the config knob + default (independently testable via the config getters); **Task 2** is the trigger wiring (independently testable via epoch-firing behavior, and consumes Task 1's getter).

---

### Task 1: Config knob `cairo.adaptive.epoch.max.rows` + interval default 60 s

**Files:**
- Modify: `core/src/main/java/io/questdb/PropertyKey.java:53`
- Modify: `core/src/main/java/io/questdb/cairo/CairoConfiguration.java:218-220`
- Modify: `core/src/main/java/io/questdb/cairo/CairoConfigurationWrapper.java:111-114`
- Modify: `core/src/main/java/io/questdb/PropServerConfiguration.java:1567` (interval default + new read) and `:3888-3890` (new getter) and the `adaptiveEpochIntervalMs` field declaration
- Test: `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveEpochTriggerTest.java` (defaults test method; the class is created here and extended in Task 2)

**Interfaces:**
- Produces: `long CairoConfiguration.getAdaptiveEpochMaxRows()` (default `5_000_000`); `PropServerConfiguration` reads `cairo.adaptive.epoch.max.rows` (default `5_000_000`) and now defaults `cairo.adaptive.epoch.interval.ms` to `60000`.

- [ ] **Step 1: Write the failing test** — create `AdaptiveEpochTriggerTest` with just the config-defaults method:

```java
/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AdaptiveEpochTriggerTest extends AbstractCairoTest {

    @Test
    public void testAdaptiveEpochConfigDefaults() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(60000L, engine.getConfiguration().getAdaptiveEpochIntervalMs());
            Assert.assertEquals(5_000_000L, engine.getConfiguration().getAdaptiveEpochMaxRows());
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/claude/wt/oss/adaptive-commit && mvn -q -pl core test -Dtest=AdaptiveEpochTriggerTest#testAdaptiveEpochConfigDefaults`
Expected: FAIL to COMPILE — `getAdaptiveEpochMaxRows()` is undefined (and, if it compiled, the interval assertion would fail at 1000).

- [ ] **Step 3: Add the PropertyKey** — in `PropertyKey.java`, immediately after `CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS(...)` (line 53):

```java
    // Max rows applied to a table since its last durable epoch before an epoch is FORCED, independent of
    // the interval above. Bounds post-epoch WAL retention (the WalPurgeJob floor IS the epoch) + recovery
    // replay, so the interval can be long. Default 5_000_000; <= 0 disables the cap. See ApplyWal2TableJob.
    CAIRO_ADAPTIVE_EPOCH_MAX_ROWS("cairo.adaptive.epoch.max.rows"),
```

- [ ] **Step 4: Add the CairoConfiguration interface default** — in `CairoConfiguration.java`, immediately after the `getAdaptiveEpochIntervalMs()` default (ends line 220):

```java
    /**
     * The maximum rows applied to a table since its last durable epoch before an epoch is FORCED,
     * independent of {@link #getAdaptiveEpochIntervalMs()}. Bounds both post-epoch WAL retention (the
     * {@code WalPurgeJob} floor is the epoch) and post-crash recovery-replay lag, so the interval can be
     * long without either growing unbounded. {@code <= 0} disables the cap (interval-only cadence).
     *
     * @return the per-table un-epoched applied-row cap; {@code <= 0} disables it
     */
    default long getAdaptiveEpochMaxRows() {
        return 5_000_000;
    }
```

Note: the interface `getAdaptiveEpochIntervalMs()` default stays `1000` (test fallback); only the shipped `PropServerConfiguration` default changes.

- [ ] **Step 5: Delegate in CairoConfigurationWrapper** — in `CairoConfigurationWrapper.java`, immediately after the `getAdaptiveEpochIntervalMs()` override (ends line 114):

```java
    @Override
    public long getAdaptiveEpochMaxRows() {
        return getDelegate().getAdaptiveEpochMaxRows();
    }
```

- [ ] **Step 6: Wire PropServerConfiguration** — three edits:

(a) Declare the field beside the existing one. Locate it: `rg -n "long adaptiveEpochIntervalMs;" core/src/main/java/io/questdb/PropServerConfiguration.java`, then add directly below:

```java
        private long adaptiveEpochMaxRows;
```

(b) At line 1567, change the interval default and add the new read directly after it:

```java
            this.adaptiveEpochIntervalMs = getMillis(properties, env, PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 60000);
            this.adaptiveEpochMaxRows = getLong(properties, env, PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 5_000_000);
```

(c) After the `getAdaptiveEpochIntervalMs()` override (ends line 3890), add the getter:

```java
        @Override
        public long getAdaptiveEpochMaxRows() {
            return adaptiveEpochMaxRows;
        }
```

- [ ] **Step 7: Run the defaults test to verify it passes**

Run: `mvn -q -pl core test -Dtest=AdaptiveEpochTriggerTest#testAdaptiveEpochConfigDefaults`
Expected: PASS.

- [ ] **Step 8: Regression — the interval-default change perturbs nothing**

Run: `mvn -q -pl core test -Dtest=AdaptiveWalDurabilityTest,AdaptiveEpochCrashTest,AdaptiveMetricsTest,PerTableCommitModeTest`
Expected: PASS (these set the interval explicitly, so the default change is inert). If any FAILS because it relied on the old `1000` default firing a *non-first-batch* epoch, the fix is to add `setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);` to that test's setup — restoring its original cadence — then re-run.

- [ ] **Step 9: Commit**

```bash
git add core/src/main/java/io/questdb/PropertyKey.java \
        core/src/main/java/io/questdb/cairo/CairoConfiguration.java \
        core/src/main/java/io/questdb/cairo/CairoConfigurationWrapper.java \
        core/src/main/java/io/questdb/PropServerConfiguration.java \
        core/src/test/java/io/questdb/test/cairo/wal/AdaptiveEpochTriggerTest.java
git commit -m "feat(adaptive): add cairo.adaptive.epoch.max.rows; default interval 1s->60s"
```

---

### Task 2: Time-OR-backlog epoch trigger

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/seq/SeqTxnTracker.java:69` (field), `:158` (accessors)
- Modify: `core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java:595-597` (call site), `:677` (signature), `:697-703` (gate), `:790-792` (reset)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/AdaptiveEpochTriggerTest.java` (add behavior methods)

**Interfaces:**
- Consumes: `CairoConfiguration.getAdaptiveEpochMaxRows()` (Task 1).
- Produces (on `SeqTxnTracker`): `long getRowsSinceEpoch()`, `void addRowsSinceEpoch(long rows)`, `void resetRowsSinceEpoch()`.

- [ ] **Step 1: Write the failing behavior tests** — add these four methods (and the private helper) to `AdaptiveEpochTriggerTest`:

```java
    // Long interval so, after the mandatory first-batch epoch, ONLY the row cap can fire within a
    // sub-second test — independent of wall-clock advancement.
    private static final int LONG_INTERVAL_MS = 3_600_000;

    @Test
    public void testBacklogCapForcesEpoch() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 500);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floorAfterFirst = durableEpoch("x");
            Assert.assertTrue("first-batch epoch should advance the floor", floorAfterFirst > 0);

            // 600 rows (> cap 500) with an effectively infinite interval -> only the cap can fire.
            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floorAfterCap = durableEpoch("x");
            Assert.assertTrue(
                    "backlog cap should force an epoch: floorAfterCap=" + floorAfterCap
                            + " must exceed floorAfterFirst=" + floorAfterFirst,
                    floorAfterCap > floorAfterFirst
            );
        });
    }

    @Test
    public void testCapDisabledNoEpochOnBacklog() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0); // cap disabled
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floorAfterFirst = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floorAfterBacklog = durableEpoch("x");
            Assert.assertEquals(
                    "cap disabled + long interval must not fire a second epoch",
                    floorAfterFirst, floorAfterBacklog
            );
        });
    }

    @Test
    public void testBacklogCounterResetsAfterEpoch() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 500);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor1 = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floor2 = durableEpoch("x");
            Assert.assertTrue("cap should fire epoch #2", floor2 > floor1);

            // 300 < cap, counter reset after epoch #2 -> no epoch (else cumulative 600+300 would fire).
            execute("insert into x select timestamp_sequence(200000000000L, 1000000L), x from long_sequence(300)");
            drainWalQueue();
            long floor3 = durableEpoch("x");
            Assert.assertEquals("counter reset: 300 < cap must not fire", floor2, floor3);

            // 300 more -> 600 since epoch #2 >= cap -> epoch #3.
            execute("insert into x select timestamp_sequence(300000000000L, 1000000L), x from long_sequence(300)");
            drainWalQueue();
            long floor4 = durableEpoch("x");
            Assert.assertTrue("crossing the cap again should fire epoch #3", floor4 > floor2);
        });
    }

    @Test
    public void testTimePathStillFiresWhenCapHuge() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0); // every batch (time path)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1_000_000_000L); // cap never hit
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor1 = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor2 = durableEpoch("x");
            Assert.assertTrue("interval=0 must still epoch every batch (time path intact)", floor2 > floor1);
        });
    }

    private static long durableEpoch(String tableName) {
        TableToken token = engine.verifyTableName(tableName);
        return engine.getTableSequencerAPI().getTxnTracker(token).getDurableEpochSeqTxn();
    }
```

- [ ] **Step 2: Run the behavior tests to verify they fail**

Run: `mvn -q -pl core test -Dtest=AdaptiveEpochTriggerTest#testBacklogCapForcesEpoch+testBacklogCounterResetsAfterEpoch`
Expected: FAIL to COMPILE — `addRowsSinceEpoch` / `getRowsSinceEpoch` / `resetRowsSinceEpoch` are undefined on `SeqTxnTracker`. (After the tracker methods exist but before the gate is wired, `testBacklogCapForcesEpoch` FAILS its assertion — the cap never fires.)

- [ ] **Step 3: Add the counter to SeqTxnTracker** — add the field directly after `lastEpochTs` (line 69):

```java
    // Rows applied to this table since its last durable epoch (adaptive backlog gate). Reset to 0 in
    // ApplyWal2TableJob.advance() when an epoch publishes. Read+written ONLY by the apply worker that
    // holds the table writer (single-threaded per table) -> plain long, no CAS (mirrors lastEpochTs).
    private long rowsSinceEpoch = 0;
```

Add the accessors directly after `getLastEpochTs()` (ends line 158):

```java
    /** Rows applied since the last durable epoch (adaptive backlog gate). Apply-worker-only. */
    public long getRowsSinceEpoch() {
        return rowsSinceEpoch;
    }

    /** Adds to the un-epoched applied-row count (adaptive backlog gate). Apply-worker-only. */
    public void addRowsSinceEpoch(long rows) {
        rowsSinceEpoch += rows;
    }

    /** Resets the un-epoched applied-row count; called when an epoch publishes. Apply-worker-only. */
    public void resetRowsSinceEpoch() {
        rowsSinceEpoch = 0;
    }
```

- [ ] **Step 4: Feed the counter and extend the gate in ApplyWal2TableJob**

(a) At the call site (lines 595-597), pass the batch row count `rowsAdded` (declared at line 426, accumulated at line 566):

```java
                    if (totalTransactionCount > 0) {
                        maybeAdvanceDurableEpoch(tableToken, writer, rowsAdded);
                    }
```

(b) Change the method signature (line 677):

```java
    private void maybeAdvanceDurableEpoch(TableToken tableToken, TableWriter writer, long rowsApplied) {
```

(c) Replace the tracker-fetch + cadence gate (lines 697-703) with the fed counter + time-OR-backlog gate:

```java
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
        // Feed the backlog counter for THIS batch BEFORE the gate, so the just-applied rows count toward it.
        tracker.addRowsSinceEpoch(rowsApplied);
        final long nowMs = microClock.getTicks() / 1000L;
        final long lastEpochTs = tracker.getLastEpochTs();
        final long maxRows = config.getAdaptiveEpochMaxRows();
        // Fire on the first batch (lastEpochTs == 0), once intervalMs has elapsed, OR once the un-epoched
        // applied-row backlog reaches the cap (maxRows > 0). The cap bounds WAL retention + recovery replay
        // so the interval can be long; maxRows <= 0 disables it (interval-only). A negative interval already
        // returned above, so an operator opt-out of epochs also opts out of the cap.
        final boolean timeElapsed = lastEpochTs == 0 || (nowMs - lastEpochTs) >= intervalMs;
        final boolean backlogHit = maxRows > 0 && tracker.getRowsSinceEpoch() >= maxRows;
        if (!timeElapsed && !backlogHit) {
            return;
        }
```

- [ ] **Step 5: Reset the counter on the advance success path** — in `advance()`, after `tracker.setLastEpochTs(nowMs);` (line 792):

```java
        // Restart the backlog count now the epoch is published (success path only; the demote/slot-busy
        // early returns above intentionally leave it intact so a skipped epoch retries on the next batch).
        tracker.resetRowsSinceEpoch();
```

- [ ] **Step 6: Run the full behavior test class to verify it passes**

Run: `mvn -q -pl core test -Dtest=AdaptiveEpochTriggerTest`
Expected: PASS (all five methods, including Task 1's defaults method).

- [ ] **Step 7: Regression — the epoch/apply/recovery suites are unaffected**

Run: `mvn -q -pl core test -Dtest=AdaptiveWalDurabilityTest,AdaptiveEpochCrashTest,AdaptiveMetricsTest,RecoveryCoordinatorTest,WalPurgeJobTest`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/wal/seq/SeqTxnTracker.java \
        core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java \
        core/src/test/java/io/questdb/test/cairo/wal/AdaptiveEpochTriggerTest.java
git commit -m "feat(adaptive): fire durable epoch on row-backlog cap, not just interval"
```

---

## Self-Review

**Spec coverage:**
- Trigger `time OR backlog` — Task 2 Step 4c. ✓
- `cairo.adaptive.epoch.max.rows`, default 5M, `≤0` disables — Task 1 (config), Task 2 (`backlogHit = maxRows > 0 && …`), test `testCapDisabledNoEpochOnBacklog`. ✓
- Interval default 60 s — Task 1 Step 6b + `testAdaptiveEpochConfigDefaults`. ✓
- Monotonic apply-worker-local counter, reset on success — Task 2 Steps 3, 5; comment + `testBacklogCounterResetsAfterEpoch`. ✓
- Negative-interval overrides cap (evaluated first) — preserved (the `intervalMs < 0` return at line 694 is untouched, above the new code); noted in gate comment. ✓
- INV-5 / recovery / read-gating untouched — no edits to `advance()` ordering (only an append after the final publish) or `RecoveryCoordinator`. ✓
- Behavior matrix (quiet=time, burst/catch-up=backlog) — covered by `testBacklogCapForcesEpoch` (backlog) + `testTimePathStillFiresWhenCapHuge` (time). ✓

**Placeholder scan:** none — every code step has complete code; the one "locate the field" is a concrete `rg` command, not a hand-wave.

**Type consistency:** `getAdaptiveEpochMaxRows()` returns `long` in interface, wrapper, and PropServer getter; `rowsSinceEpoch` is `long`; `addRowsSinceEpoch(long)` / `getRowsSinceEpoch()` / `resetRowsSinceEpoch()` names match between Task 2 Step 3 (definition) and Steps 4-5 (use); `maybeAdvanceDurableEpoch(TableToken, TableWriter, long)` signature matches its call site. `durableEpoch(...)` helper defined once (Task 2 Step 1), used by all behavior methods. ✓
