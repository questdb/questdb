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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.lv.LiveViewRefreshStats.BASE_METADATA_DRIFT_RECOMPILE_COUNT;

/**
 * A shutdown, a DROP or an invalidation trips a live view's refresh breaker while the view recovers:
 * in the recovery a base schema change asks for, in the one a mid-drain fault asks for, or in the
 * re-derive from the applied base a lost base WAL segment ends in once it spent the flush-retry budget.
 * The recovery hands the cancellation back as its error, and none of the three is a refresh fault, as
 * a cancellation the turn itself meets is not: nothing is charged, no back-off is armed, no last resort
 * runs and the view is not invalidated - a shutdown leaves it valid on disk, and an invalidation keeps
 * its own reason. A view the cancellation leaves running owes the window-state debt, so the next turn
 * that drives it recovers before it drains or flushes.
 * <p>
 * The breaker trips where a concurrent one would: from the refresh-fault observer, once the turn's
 * fault reached the handler and before the recovery scans; from the hook ahead of the re-derive's
 * replay, which then meets the breaker when it opens its first cursor; or from the open of a base
 * partition's column file, which the replay's scan makes once it has wiped and started feeding the
 * runtime. A shutdown is the engine's closing flag, which the breaker reads. Turns run one at a time,
 * and the flag goes down before the next one.
 */
public class LiveViewRecoveryCancellationTest extends AbstractLiveViewTxTest {
    private static final String DEDUP_CLAUSE = "DEDUP UPSERT KEYS(created_at, account_id)";
    // Long enough for a thread to start and reach its first statement on a loaded CI host; only a broken
    // drop ever waits it out.
    private static final long DROP_TRIP_WAIT_NANOS = 60_000_000_000L;
    // VIEW_SQL and RECOMPUTE_SQL with a filter no row of the fixture passes: every amount is positive.
    private static final String EMPTY_RECOMPUTE_SQL = """
            (
                SELECT created_at, account_id,
                    sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum,
                    count(account_id) OVER (PARTITION BY account_id, bucket ORDER BY created_at
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_count
                FROM (
                    SELECT created_at, account_id, amount,
                        timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket
                    FROM tx
                    WHERE amount < 0
                )
            ) ORDER BY 2, 1
            """;
    private static final String EMPTY_VIEW_SQL = "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
            + "count(account_id) OVER w AS cumulative_count "
            + "FROM tx WHERE amount < 0 WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')";
    private static final String INVALIDATION_REASON = "invalidated while the view recovered";
    private static final String RECOVERY_CANCELLED_LINE = "live view refresh cancelled during recovery [view=lv, dropped=";
    private static final String REDERIVE_CANCELLED_LINE = "live view re-derive from the applied base cancelled [view=lv, dropped=";
    // Where the breaker stops the re-derive in assertShutdownDuringTheRederiveKeepsTheViewValid: the rebuild it
    // recompiles for a base schema change, the open of its replay's first cursor, or its replay's second page frame.
    private static final int STAGE_DRIFT = 2;
    private static final int STAGE_PROBE = 0;
    private static final int STAGE_SCAN = 1;
    private static final LogCapture capture = new LogCapture();

    @Before
    public void setUpCancellation() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @After
    public void tearDownCancellation() {
        engine.setClosing(false);
        capture.stop();
        setCurrentMicros(-1);
    }

    @Test
    public void testAFailedDriftRecoveryIsStillCharged() throws Exception {
        // The boundary of the cancellation's arm: a base ADD COLUMN drifts the view's plan, the drift
        // recovery finds no timeline and rebuilds, and the rebuild's scan of the base fails on a file it
        // cannot open. That is the recovery failing, not a cancellation, so the turn is charged and paced.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture(DEDUP_CLAUSE);
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                removeTimeline();
                execute("ALTER TABLE tx ADD COLUMN note INT");
                drainWalQueue();
                engine.releaseInactive();
                fault.armAppliedScan();
                advanceClockToNextRefreshPass();
                job.run();
                Assert.assertTrue("the drift recovery's rebuild must have been failed", fault.hasAppliedScanFired());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals(1, instance.getFlushRetryCount());
                Assert.assertNotEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
                Assert.assertNotEquals(Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
                Assert.assertTrue("a failed recovery leaves the debt", instance.isWindowStateDirty());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences("live view refresh failed [view=lv"));
                capture.assertNotLogged(RECOVERY_CANCELLED_LINE);
            }
        });
    }

    @Test
    public void testAMidDrainRecoveryThatMeetsASchemaChangeIsStillCharged() throws Exception {
        // A drain fails between two commits after feeding the first, and a base ADD COLUMN lands while the
        // mid-drain recovery looks for its timeline, which is gone. The rebuild it falls back to meets a stale
        // plan, an error that is no CairoException, let alone a cancellation: the turn is charged and paced,
        // and the gate of the next turn recompiles the plan and recovers the view.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(instance.getLastFlushTimeUs());
                insert("('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
                drainWalQueue();
                removeTimeline();
                final AtomicBoolean isAltered = new AtomicBoolean();
                fault.armRuntimeRestore(() -> {
                    try {
                        execute("ALTER TABLE tx ADD COLUMN note INT");
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    drainWalQueue();
                    isAltered.set(true);
                });
                fault.arm(2);
                job.run();
                Assert.assertTrue(fault.hasFired());
                Assert.assertTrue("the schema change must land in the recovery", isAltered.get());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals(1, instance.getFlushRetryCount());
                Assert.assertNotEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
                Assert.assertNotEquals(Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
                Assert.assertTrue("a failed recovery leaves the debt", instance.isWindowStateDirty());
                capture.drain();
                capture.assertOnlyOnce("live view window-state recompute failed \\[view=lv, cause=mid-drain refresh failure, "
                        + "error=[^\\n]*cached query plan cannot be used because table schema has changed");
                Assert.assertEquals(1, capture.countOccurrences("live view refresh failed [view=lv"));
                capture.assertNotLogged(RECOVERY_CANCELLED_LINE);

                driveRefreshToQuiescence(job);
                Assert.assertFalse(instance.isInvalid());
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARederiveThatFailsOnANonCairoErrorStillInvalidatesTheView() throws Exception {
        // The boundary of the re-derive's cancellation arm: the re-derive fails on an error that is no
        // CairoException - a fault in its own code, which the hook ahead of its replay stands in for. That is
        // not a cancellation, so the spent budget invalidates the view as it does after any failed re-derive.
        assertMemoryLeak(() -> {
            createRederiveFixture();
            final LiveViewInstance instance = instance();
            final AtomicBoolean isFailed = new AtomicBoolean();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                spendTheFlushRetryBudgetButOne(job, instance);
                hooks(job).setSimulateBaseApplyDuringRederive(() -> {
                    isFailed.set(true);
                    throw new IllegalStateException("simulated re-derive fault");
                });
                advanceClockToNextRefreshPass();
                job.run();
            }
            assertRederiveFailureInvalidated(instance, isFailed);
        });
    }

    @Test
    public void testARederivesDriftRetryThatFailsOnANonCairoErrorStillInvalidatesTheView() throws Exception {
        // A base ADD COLUMN lands during the re-derive, whose replay then meets a stale plan, and the rebuild
        // it recompiles the plan for fails on an error that is no CairoException, in its publication tail.
        // That is not a cancellation, so the spent budget invalidates the view.
        assertMemoryLeak(() -> {
            createRederiveFixture();
            final LiveViewInstance instance = instance();
            final AtomicBoolean isFailed = new AtomicBoolean();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                spendTheFlushRetryBudgetButOne(job, instance);
                hooks(job).setSimulateBaseApplyDuringRederive(() -> {
                    try {
                        execute("ALTER TABLE tx ADD COLUMN note INT");
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    drainWalQueue();
                    hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                        isFailed.set(true);
                        throw new IllegalStateException("simulated re-derive fault");
                    });
                });
                advanceClockToNextRefreshPass();
                job.run();
            }
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences("live view re-derive found the base metadata changed, recompiling once [view=lv"));
            assertRederiveFailureInvalidated(instance, isFailed);
        });
    }

    @Test
    public void testCancelledRederiveOverAnUnflushedLeadLosesNoRow() throws Exception {
        // A lead-eligible view holds two drained commits' rows in its un-flushed lead when the base loses its
        // WAL. A shutdown cancels the re-derive the spent budget runs, which dropped the lead's row count
        // ahead of its replay. Then a pass with FLUSH EVERY due, driven by a stale notification of the
        // lead's own commit, so the pass has nothing to drain: its gate restores the runtime first, so the
        // view drains the lost commits again, meets the lost segment and re-derives every row, rather than
        // flushing an empty lead past the rows the lead held.
        assertMemoryLeak(() -> assertCancelledRederiveOverAnUnflushedLeadLosesNoRow(false));
    }

    @Test
    public void testCancelledRederiveOverAnUnflushedLeadLosesNoRowAtTheDefaultCadence() throws Exception {
        // testCancelledRederiveOverAnUnflushedLeadLosesNoRow at the server's default checkpoint cadence, which
        // seals the first boundary only, with a second flushed commit ahead of the lead. The gate's restore
        // then has to replay the lost WAL above the root, which no retry gets past, so the gate rebuilds the
        // view from the applied base, which commits every row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> assertCancelledRederiveOverAnUnflushedLeadLosesNoRow(true));
    }

    @Test
    public void testDropDuringADriftRecoveryIsNotCharged() throws Exception {
        // A DROP LIVE VIEW on another thread trips the breaker while a base ADD COLUMN's drift recovery runs,
        // at the edge of a one-failure budget. The cancelled recovery charges nothing and starts no
        // invalidation over the view the drop is tearing down, and the drop completes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, 1);
        assertMemoryLeak(() -> {
            createFixture(DEDUP_CLAUSE);
            final LiveViewInstance instance = instance();
            final AtomicReference<Thread> dropper = new AtomicReference<>();
            final AtomicReference<Throwable> dropError = new AtomicReference<>();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                removeTimeline();
                execute("ALTER TABLE tx ADD COLUMN note INT");
                drainWalQueue();
                hooks(job).setRefreshFaultObserver(t -> {
                    if (t instanceof TableReferenceOutOfDateException && dropper.get() == null) {
                        dropper.set(startDropAndAwaitTheTrip(instance, dropError));
                    }
                });
                advanceClockToNextRefreshPass();
                job.run();
            }
            Assert.assertNotNull("the drift must have reached the handler", dropper.get());
            Assert.assertEquals(0, instance.getFlushRetryCount());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
            assertDroppedNotInvalidated(instance, dropper.get(), dropError, RECOVERY_CANCELLED_LINE);
        });
    }

    @Test
    public void testDropDuringTheRederiveAfterBaseWalLossIsNotAnInvalidation() throws Exception {
        // A DROP LIVE VIEW on another thread trips the breaker ahead of the re-derive's replay. The cancelled
        // re-derive starts no invalidation over the view the drop is tearing down, and the drop completes.
        assertMemoryLeak(() -> {
            createRederiveFixture();
            final LiveViewInstance instance = instance();
            final AtomicReference<Thread> dropper = new AtomicReference<>();
            final AtomicReference<Throwable> dropError = new AtomicReference<>();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                spendTheFlushRetryBudgetButOne(job, instance);
                hooks(job).setSimulateBaseApplyDuringRederive(() -> dropper.set(startDropAndAwaitTheTrip(instance, dropError)));
                advanceClockToNextRefreshPass();
                job.run();
            }
            Assert.assertNotNull("the re-derive must have run", dropper.get());
            Assert.assertEquals("the lost segment that spent the budget is charged",
                    configuration.getLiveViewFlushRetryMax(), instance.getFlushRetryCount());
            assertDroppedNotInvalidated(instance, dropper.get(), dropError, REDERIVE_CANCELLED_LINE);
        });
    }

    @Test
    public void testInvalidationDuringADriftRecoveryKeepsItsReason() throws Exception {
        // An invalidation trips the breaker while a base ADD COLUMN's drift recovery runs, at the edge of a
        // one-failure budget. The cancelled recovery is not charged, so the invalidation's own reason stands,
        // in memory and on disk.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, 1);
        assertMemoryLeak(() -> {
            createFixture(DEDUP_CLAUSE);
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                removeTimeline();
                execute("ALTER TABLE tx ADD COLUMN note INT");
                drainWalQueue();
                final AtomicBoolean isTripped = tripOnTheDrift(job, () -> engine.invalidateLiveView(instance, INVALIDATION_REASON));
                advanceClockToNextRefreshPass();
                job.run();
                Assert.assertTrue("the drift must have reached the handler", isTripped.get());
            }
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals(INVALIDATION_REASON, instance.getInvalidationReason());
            Assert.assertEquals(0, instance.getFlushRetryCount());
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences(RECOVERY_CANCELLED_LINE + "false, invalid=true]"));
            capture.assertNotLogged("budget exhausted");
            shutdown();
            engine.buildViewGraphs();
            assertLiveViewsRow("lv\tinvalid\t" + INVALIDATION_REASON + '\n');
        });
    }

    @Test
    public void testInvalidationDuringTheRederiveAfterBaseWalLossKeepsItsReason() throws Exception {
        // An invalidation trips the breaker ahead of the re-derive's replay. The cancelled re-derive does not
        // invalidate the view again, so the invalidation's own reason stands, in memory and on disk.
        assertMemoryLeak(() -> {
            createRederiveFixture();
            final LiveViewInstance instance = instance();
            final AtomicBoolean isTripped = new AtomicBoolean();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                spendTheFlushRetryBudgetButOne(job, instance);
                hooks(job).setSimulateBaseApplyDuringRederive(() -> {
                    engine.invalidateLiveView(instance, INVALIDATION_REASON);
                    isTripped.set(true);
                });
                advanceClockToNextRefreshPass();
                job.run();
            }
            Assert.assertTrue("the re-derive must have run", isTripped.get());
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals(INVALIDATION_REASON, instance.getInvalidationReason());
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences(REDERIVE_CANCELLED_LINE + "false, invalid=true]"));
            capture.assertNotLogged("live view refresh budget exhausted, invalidating");
            capture.assertNotLogged("live view could not re-derive from the applied base after base WAL loss");
            shutdown();
            engine.buildViewGraphs();
            assertLiveViewsRow("lv\tinvalid\t" + INVALIDATION_REASON + '\n');
        });
    }

    @Test
    public void testShutdownAtTheRederivesFirstCursorKeepsTheViewValid() throws Exception {
        // The breaker stops the re-derive's replay as it opens its first cursor, ahead of anything the replay
        // changes, after the re-derive dropped the lead's row count.
        final LiveViewBaseAmountOpenFault fault = new LiveViewBaseAmountOpenFault();
        assertMemoryLeak(fault, () -> assertShutdownDuringTheRederiveKeepsTheViewValid(fault, STAGE_PROBE));
    }

    @Test
    public void testShutdownDuringADriftRecoveryAtTheEndOfTheBudgetKeepsTheViewValid() throws Exception {
        // A shutdown cancels a base ADD COLUMN's drift recovery at the edge of a one-failure budget. Charged,
        // it would spend the budget and invalidate the view durably on the way down; it is not, and the
        // restart recovers the view.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, 1);
        assertMemoryLeak(() -> {
            createFixture(DEDUP_CLAUSE);
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                runTheDriftTurnUnderShutdown(job);
            }
            Assert.assertFalse("[reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
            Assert.assertEquals(0, instance.getFlushRetryCount());
            capture.drain();
            capture.assertNotLogged("budget exhausted");
            shutdown();
            engine.buildViewGraphs();
            assertLiveViewsRow("lv\tactive\t\n");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(instance().isInvalid());
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testShutdownDuringADriftRecoveryPastTheDurationBudgetKeepsTheViewValid() throws Exception {
        // A drain fault that fed nothing started a flush-retry streak 61 s ago, past the default 60 s duration
        // budget, so any fault the next turn is charged for spends the budget. A shutdown cancels that turn's
        // drift recovery, which is not charged: the streak stands as it was, and the view stays valid.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture(DEDUP_CLAUSE);
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                removeTimeline();
                insert("('2026-01-02T09:00:00.000000Z', 'acct-1', 16.0)");
                fault.arm(0);
                advanceClockToNextRefreshPass();
                job.run();
                Assert.assertTrue("the drain must have been failed", fault.hasFired());
                Assert.assertFalse("the drain must have fed nothing", instance.isWindowStateDirty());
                Assert.assertEquals(1, instance.getFlushRetryCount());
                final long streakStartUs = instance.getFlushRetryStartUs();
                Assert.assertNotEquals(Numbers.LONG_NULL, streakStartUs);

                execute("ALTER TABLE tx ADD COLUMN note INT");
                drainWalQueue();
                final AtomicBoolean isTripped = tripOnTheDrift(job, () -> engine.setClosing(true));
                setCurrentMicros(streakStartUs + 61_000_000L);
                try {
                    job.run();
                } finally {
                    engine.setClosing(false);
                }
                Assert.assertTrue("the drift must have reached the handler", isTripped.get());
                Assert.assertFalse("[reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
                Assert.assertFalse(instance.hasPendingInvalidationReason());
                Assert.assertEquals(1, instance.getFlushRetryCount());
                Assert.assertEquals(streakStartUs, instance.getFlushRetryStartUs());
                Assert.assertTrue(instance.isWindowStateDirty());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences(RECOVERY_CANCELLED_LINE + "false, invalid=false]"));
                capture.assertNotLogged("budget exhausted");

                driveRefreshToQuiescence(job);
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                Assert.assertFalse(instance.isWindowStateDirty());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testShutdownDuringADriftRecoveryRebuildIsNotCharged() throws Exception {
        // A base ADD COLUMN drifts a deduplicating view's plan, its drift recovery finds no timeline and
        // rebuilds the view from the applied base, and a shutdown trips the breaker before the rebuild
        // scans. Nothing is charged, nothing is paced, and the debt the drift left stands, so once the flag
        // is down the gate rebuilds the view before anything drains.
        assertMemoryLeak(() -> {
            createFixture(DEDUP_CLAUSE);
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final long faultsBefore = instance.getRefreshFaultCount();
                runTheDriftTurnUnderShutdown(job);
                Assert.assertEquals(faultsBefore + 1, instance.getRefreshFaultCount());
                assertUncharged(instance);
                Assert.assertTrue(instance.isWindowStateDirty());
                capture.drain();
                capture.assertOnlyOnce("live view window-state recompute failed \\[view=lv, cause=base table metadata change, error=[^\\n]*cancelled by user");
                Assert.assertEquals(1, capture.countOccurrences(RECOVERY_CANCELLED_LINE + "false, invalid=false]"));
                capture.assertNotLogged("live view refresh failed");
                capture.assertNotLogged("budget exhausted");

                driveRefreshToQuiescence(job);
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences("live view recomputed window state from applied base [view=lv"));
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testShutdownDuringADriftRecoveryRestoreIsNotCharged() throws Exception {
        // As testShutdownDuringADriftRecoveryRebuildIsNotCharged, but the view keeps its timeline at the
        // server's default cadence, which seals the first boundary only, so the drift recovery restores the
        // runtime from it, and the breaker stops the restore's replay of the base WAL above the root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createO3GapFixture();
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("ALTER TABLE tx ADD COLUMN note INT");
                drainWalQueue();
                final AtomicBoolean isTripped = tripOnTheDrift(job, () -> engine.setClosing(true));
                advanceClockToNextRefreshPass();
                try {
                    job.run();
                } finally {
                    engine.setClosing(false);
                }
                Assert.assertTrue("the drift must have reached the handler", isTripped.get());
                assertUncharged(instance);
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences(RECOVERY_CANCELLED_LINE + "false, invalid=false]"));
                capture.assertNotLogged("live view cannot restore its runtime from the checkpoint timeline");
                capture.assertNotLogged("live view window-state recompute failed");
                capture.assertNotLogged("live view refresh failed");

                driveRefreshToQuiescence(job);
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testShutdownDuringAMidDrainRecoveryIsNotCharged() throws Exception {
        // A drain fails between two commits after feeding the first, the mid-drain recovery finds no timeline
        // and rebuilds the view, and a shutdown trips the breaker before the rebuild scans. Nothing is charged
        // and the debt stands for the next turn's gate.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(instance.getLastFlushTimeUs());
                insert("('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
                drainWalQueue();
                removeTimeline();
                final AtomicBoolean isTripped = new AtomicBoolean();
                hooks(job).setRefreshFaultObserver(t -> tripByShutdown(isTripped));
                fault.arm(2);
                final long faultsBefore = instance.getRefreshFaultCount();
                try {
                    job.run();
                } finally {
                    engine.setClosing(false);
                }
                Assert.assertTrue(fault.hasFired());
                Assert.assertTrue(isTripped.get());
                Assert.assertEquals(faultsBefore + 1, instance.getRefreshFaultCount());
                Assert.assertFalse(instance.isInvalid());
                assertUncharged(instance);
                Assert.assertTrue(instance.isWindowStateDirty());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences(RECOVERY_CANCELLED_LINE + "false, invalid=false]"));
                capture.assertNotLogged("live view refresh failed");

                driveRefreshToQuiescence(job);
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testShutdownDuringAnEmptyViewsDriftRecoveryIsNotCharged() throws Exception {
        // An active view whose filter passes no base row holds no row and never sealed a timeline. A base ADD
        // COLUMN drifts its plan, its drift recovery rebuilds it from the applied base, and a shutdown trips
        // the breaker, which the rebuild meets as it opens its first cursor although it would find no row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL " + DEDUP_CLAUSE);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + EMPTY_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insert("('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)");
                driveRefreshToQuiescence(job);
                insert("('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)");
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                Assert.assertFalse("a view with no row seals no timeline", timelineExists(instance));
                assertNoRefreshFaults("lv");
                assertEmptyViewMatchesRecompute();

                runTheDriftTurnUnderShutdown(job);
                assertUncharged(instance);
                Assert.assertFalse(instance.isInvalid());
                Assert.assertTrue(instance.isWindowStateDirty());
                capture.drain();
                capture.assertOnlyOnce("live view window-state recompute failed \\[view=lv, cause=base table metadata change, error=[^\\n]*cancelled by user");
                Assert.assertEquals(1, capture.countOccurrences(RECOVERY_CANCELLED_LINE + "false, invalid=false]"));
                capture.assertNotLogged("live view refresh failed");

                driveRefreshToQuiescence(job);
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences("live view recomputed window state from applied base [view=lv"));
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                assertEmptyViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testShutdownDuringTheRederivesDriftRetryKeepsTheViewValid() throws Exception {
        // A base ADD COLUMN lands during the re-derive, whose replay then meets a stale plan, and the breaker
        // stops the rebuild the re-derive recompiles the plan for.
        final LiveViewBaseAmountOpenFault fault = new LiveViewBaseAmountOpenFault();
        assertMemoryLeak(fault, () -> assertShutdownDuringTheRederiveKeepsTheViewValid(fault, STAGE_DRIFT));
    }

    @Test
    public void testShutdownDuringTheRederivesScanKeepsTheViewValid() throws Exception {
        // The breaker stops the re-derive's replay on its second page frame, after the replay wiped the
        // runtime and fed it the first day's rows.
        final LiveViewBaseAmountOpenFault fault = new LiveViewBaseAmountOpenFault();
        assertMemoryLeak(fault, () -> assertShutdownDuringTheRederiveKeepsTheViewValid(fault, STAGE_SCAN));
    }

    private static void assertDroppedNotInvalidated(
            LiveViewInstance instance,
            Thread dropper,
            AtomicReference<Throwable> dropError,
            String cancelledLine
    ) throws InterruptedException {
        // Read as the turn left the instance, before the drop's teardown is waited for.
        final boolean isInvalid = instance.isInvalid();
        final CharSequence reason = instance.getInvalidationReason();
        final boolean isDropped = instance.isDropped();
        dropper.join(DROP_TRIP_WAIT_NANOS / 1_000_000L);
        Assert.assertFalse("the drop must complete", dropper.isAlive());
        Assert.assertNull("the drop must not fail", dropError.get());
        Assert.assertTrue(isDropped);
        Assert.assertFalse("a cancellation must not invalidate a view a drop is tearing down", isInvalid);
        Assert.assertNull(reason);
        capture.drain();
        Assert.assertEquals(1, capture.countOccurrences(cancelledLine + "true, invalid=false]"));
        capture.assertNotLogged("live view refresh budget exhausted, invalidating");
        capture.assertNotLogged("could not persist live view invalidation");
        Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
        Assert.assertNull(engine.getTableTokenIfExists("lv"));
    }

    private static void assertRederiveFailureInvalidated(LiveViewInstance instance, AtomicBoolean isFailed) {
        Assert.assertTrue("the re-derive must have failed", isFailed.get());
        Assert.assertTrue(instance.isInvalid());
        TestUtils.assertEquals("flush retry budget exhausted", instance.getInvalidationReason());
        capture.drain();
        capture.assertOnlyOnce("live view could not re-derive from the applied base after base WAL loss \\[view=lv, "
                + "error=[^\\n]*simulated re-derive fault");
        Assert.assertEquals(1, capture.countOccurrences("live view refresh budget exhausted, invalidating [view=lv"));
        capture.assertNotLogged(REDERIVE_CANCELLED_LINE);
    }

    private static void assertUncharged(LiveViewInstance instance) {
        Assert.assertFalse("[reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
        Assert.assertFalse(instance.hasPendingInvalidationReason());
        Assert.assertEquals("a cancellation must not count toward the flush-retry budget", 0, instance.getFlushRetryCount());
        Assert.assertEquals("a cancellation must not start a flush-retry streak", Numbers.LONG_NULL, instance.getFlushRetryStartUs());
        Assert.assertEquals("a cancellation must not arm the refresh-retry back-off", Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
    }

    // Starts DROP LIVE VIEW lv on a thread of its own and waits until the drop has tripped the view's
    // refresh breaker, which it does before it waits for the turn in flight to release the refresh latch.
    private static Thread startDropAndAwaitTheTrip(LiveViewInstance instance, AtomicReference<Throwable> dropError) {
        final Thread dropper = new Thread(() -> {
            try {
                engine.dropLiveView("lv", AllowAllSecurityContext.INSTANCE);
            } catch (Throwable th) {
                dropError.set(th);
            } finally {
                clearWorkerThreadLocals();
            }
        }, "lv-dropper");
        dropper.start();
        final long deadlineNanos = System.nanoTime() + DROP_TRIP_WAIT_NANOS;
        while (!instance.getRefreshCancelledFlag().get()) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError("the drop never tripped the refresh breaker");
            }
            Os.pause();
        }
        return dropper;
    }

    private static boolean timelineExists(LiveViewInstance instance) {
        try (
                Path dir = new Path().of(configuration.getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            return configuration.getFilesFacade().exists(timelinePath.$());
        }
    }

    // What ServerMain's signalClose() sets: the engine's closing flag, which every live view's refresh
    // breaker reads.
    private static void tripByShutdown(AtomicBoolean isTripped) {
        engine.setClosing(true);
        isTripped.set(true);
    }

    // Runs trip once, from the refresh-fault observer, when the turn's fault is a base schema change.
    private static AtomicBoolean tripOnTheDrift(LiveViewRefreshJob job, Runnable trip) {
        final AtomicBoolean isTripped = new AtomicBoolean();
        hooks(job).setRefreshFaultObserver(t -> {
            if (t instanceof TableReferenceOutOfDateException && isTripped.compareAndSet(false, true)) {
                trip.run();
            }
        });
        return isTripped;
    }

    private void assertCancelledRederiveOverAnUnflushedLeadLosesNoRow(boolean isDefaultCadence) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT, pg SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // FLUSH EVERY 1s with the clock held still: the first drain flushes (its last flush is unset), later
        // drains publish the lead in RAM only. START FROM NOW over the empty base seeds nothing.
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m START FROM NOW AS "
                + "SELECT ts, x, count(*) OVER (PARTITION BY pg ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base");
        final TableToken baseToken = engine.verifyTableName("base");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:01.000000Z', 10), "
                    + "('2026-04-01T00:00:02.000000Z', 20), ('2026-04-01T00:00:03.000000Z', 30)");
            drainWalQueue();
            drainJob(job);
            if (isDefaultCadence) {
                // A second flush, which seals no root at the default cadence.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:04.000000Z', 40)");
                drainWalQueue();
                setCurrentMicros(currentMicros + 1_000_000L);
                drainJob(job);
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:05.000000Z', 50)");
            } else {
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:04.000000Z', 40), ('2026-04-01T00:00:05.000000Z', 50)");
            }
            drainWalQueue();
            drainJob(job);
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertTrue("the view must hold an un-flushed lead", instance.getLeadRowCount() > 0);
            final long leadSeqTxn = instance.getRefreshedUpToSeqTxn();
            Assert.assertEquals(instance.getLastProcessedSeqTxn() + 1, leadSeqTxn);
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:06.000000Z', 60)");
            drainWalQueue();
            // Every commit so far is in the base's first WAL, which the base loses.
            engine.releaseInactive();
            removeBaseWal("base");

            spendTheFlushRetryBudgetButOne(job, instance);
            final AtomicBoolean isTripped = new AtomicBoolean();
            hooks(job).setSimulateBaseApplyDuringRederive(() -> tripByShutdown(isTripped));
            advanceClockToNextRefreshPass();
            try {
                job.run();
            } finally {
                engine.setClosing(false);
            }
            Assert.assertTrue("the re-derive must have met the breaker", isTripped.get());
            Assert.assertFalse("[reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
            Assert.assertEquals("the re-derive dropped the lead's row count ahead of its replay", 0, instance.getLeadRowCount());
            Assert.assertTrue("the cancelled re-derive leaves the debt", instance.isWindowStateDirty());
            Assert.assertEquals(leadSeqTxn, instance.getRefreshedUpToSeqTxn());
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences(REDERIVE_CANCELLED_LINE + "false, invalid=false]"));

            // FLUSH EVERY is due, and a stale notification of the lead's own commit drives a pass with no
            // commit above the refresh cursor.
            final long restoresBefore = instance.getCheckpointRuntimeRestores();
            setCurrentMicros(currentMicros + 2_000_000L);
            engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, leadSeqTxn);
            job.run();
            drainWalQueue();
            capture.drain();
            if (isDefaultCadence) {
                // The restore needs the lost WAL above the root, and rebuilds from the applied base instead.
                Assert.assertEquals(restoresBefore, instance.getCheckpointRuntimeRestores());
                capture.assertNotLogged("live view restored its runtime from the checkpoint timeline");
                Assert.assertEquals(1, capture.countOccurrences(
                        "live view could not restore its runtime from the checkpoint timeline, rebuilding from the applied base [view=lv, cause=mid-drain refresh failure"));
                Assert.assertEquals(1, capture.countOccurrences("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure"));
            } else {
                Assert.assertEquals("the gate restores before anything flushes", restoresBefore + 1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals(1, capture.countOccurrences("live view re-derived from the applied base after base WAL loss [view=lv"));
            }
            Assert.assertFalse(instance.isInvalid());
            Assert.assertFalse(instance.isWindowStateDirty());
            Assert.assertEquals(0, instance.getFlushRetryCount());
            Assert.assertEquals(engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn(), instance.getLastProcessedSeqTxn());
            final StringSink sink = new StringSink();
            printDiskOnly("lv", "SELECT ts, x, rn FROM lv ORDER BY ts", sink);
            TestUtils.assertEquals("""
                    ts\tx\trn
                    2026-04-01T00:00:01.000000Z\t10\t1
                    2026-04-01T00:00:02.000000Z\t20\t2
                    2026-04-01T00:00:03.000000Z\t30\t3
                    2026-04-01T00:00:04.000000Z\t40\t4
                    2026-04-01T00:00:05.000000Z\t50\t5
                    2026-04-01T00:00:06.000000Z\t60\t6
                    """, sink);
        }
    }

    private void assertEmptyViewMatchesRecompute() throws Exception {
        assertQuery("SELECT count() FROM lv")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n0\n");
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, EMPTY_RECOMPUTE_SQL, "(lv) ORDER BY 2, 1", LOG, true);
    }

    private void assertLiveViewsRow(String expectedRow) throws Exception {
        assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_name\tview_status\tinvalidation_reason\n" + expectedRow);
    }

    // The base lost the WAL segment of a commit the view has yet to drain, so every drain the flush-retry
    // budget allows fails, and the turn that spends the budget re-derives the view from the applied base. A
    // shutdown stops the re-derive at the stage given. The view stays valid, owing the window-state debt, and
    // nothing more is charged. The next pass restores the runtime from the timeline the cancelled replay
    // left in place before it drains, the drain meets the lost segment again, and the spent budget
    // re-derives the view.
    private void assertShutdownDuringTheRederiveKeepsTheViewValid(LiveViewBaseAmountOpenFault fault, int stage) throws Exception {
        createRederiveFixture();
        fault.of(engine.verifyTableName("tx").getDirName());
        final LiveViewInstance instance = instance();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            spendTheFlushRetryBudgetButOne(job, instance);
            final AtomicBoolean isTripped = new AtomicBoolean();
            final AtomicBoolean isDirtyAtTrip = new AtomicBoolean();
            hooks(job).setSimulateBaseApplyDuringRederive(() -> {
                switch (stage) {
                    case STAGE_PROBE -> tripByShutdown(isTripped);
                    case STAGE_SCAN -> fault.runAtFirstOpen("2026-01-02", () -> {
                        isDirtyAtTrip.set(instance.isWindowStateDirty());
                        tripByShutdown(isTripped);
                    });
                    default -> {
                        try {
                            execute("ALTER TABLE tx ADD COLUMN note INT");
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                        drainWalQueue();
                        tripByShutdown(isTripped);
                    }
                }
            });
            // The replay's reader then opens the base's partitions afresh, which the scan's trip needs.
            engine.releaseAllReaders();
            final long streakStartUs = instance.getFlushRetryStartUs();
            final long backoffUs = instance.getRefreshRetryNotBeforeUs();
            advanceClockToNextRefreshPass();
            try {
                job.run();
            } finally {
                engine.setClosing(false);
            }
            Assert.assertTrue("the re-derive must have met the breaker", isTripped.get());
            if (stage == STAGE_SCAN) {
                Assert.assertTrue("the trip must land after the replay wiped the runtime", isDirtyAtTrip.get());
            }
            Assert.assertFalse("[reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
            Assert.assertFalse(instance.hasPendingInvalidationReason());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals("the lost segment that spent the budget is charged",
                    configuration.getLiveViewFlushRetryMax(), instance.getFlushRetryCount());
            Assert.assertEquals(streakStartUs, instance.getFlushRetryStartUs());
            Assert.assertEquals("the cancellation arms no back-off", backoffUs, instance.getRefreshRetryNotBeforeUs());
            Assert.assertTrue("the cancelled re-derive leaves the debt", instance.isWindowStateDirty());
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences(REDERIVE_CANCELLED_LINE + "false, invalid=false]"));
            capture.assertNotLogged("live view could not re-derive from the applied base after base WAL loss");
            capture.assertNotLogged("live view refresh budget exhausted, invalidating");
            Assert.assertEquals(0, job.getStatsForTest().get(BASE_METADATA_DRIFT_RECOMPILE_COUNT));
            if (stage == STAGE_DRIFT) {
                Assert.assertEquals(1, capture.countOccurrences("live view re-derive found the base metadata changed, recompiling once [view=lv"));
                capture.assertOnlyOnce("live view window-state recompute failed \\[view=lv, cause=base table metadata change, error=[^\\n]*cancelled by user");
            } else {
                capture.assertNotLogged("live view re-derive found the base metadata changed");
            }

            // One pass with the flag down.
            final long restoresBefore = instance.getCheckpointRuntimeRestores();
            advanceClockToNextRefreshPass();
            job.run();
            capture.drain();
            Assert.assertEquals("the gate restores before the drain", restoresBefore + 1, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals(1, capture.countOccurrences(
                    "live view restored its runtime from the checkpoint timeline [view=lv, cause=mid-drain refresh failure"));
            Assert.assertEquals(1, capture.countOccurrences("live view re-derived from the applied base after base WAL loss [view=lv"));
            // The cancelled step recompiled the plan at the base's new metadata already.
            capture.assertNotLogged("live view re-derived from the applied base after base WAL loss and a base metadata change");
            Assert.assertEquals(0, job.getStatsForTest().get(BASE_METADATA_DRIFT_RECOMPILE_COUNT));
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals(0, instance.getFlushRetryCount());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
            Assert.assertFalse(instance.isWindowStateDirty());
            final long baseHead = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("tx")).getWriterTxn();
            Assert.assertEquals(stage == STAGE_DRIFT ? 6 : 5, baseHead);
            Assert.assertEquals(baseHead, instance.getLastProcessedSeqTxn());
            assertViewMatchesRecompute();
        }
        shutdown();
        engine.buildViewGraphs();
        assertLiveViewsRow("lv\tactive\t\n");
        assertViewMatchesRecompute();
    }

    // createFixture's view, then a commit on 2026-01-02 the base applies and the view has yet to drain, a
    // stop, the base's WAL gone, and the start: what a restore that captured the base table and not its WAL
    // leaves behind.
    private void createRederiveFixture() throws Exception {
        createFixture("");
        insert("('2026-01-02T09:00:00.000000Z', 'acct-1', 16.0)");
        shutdown();
        removeBaseWal("tx");
        engine.buildViewGraphs();
    }

    // A base ADD COLUMN on a view that removed its timeline, and one turn that meets the drift with a shutdown
    // tripping the breaker as the fault reaches the handler.
    private void runTheDriftTurnUnderShutdown(LiveViewRefreshJob job) throws Exception {
        if (timelineExists(instance())) {
            removeTimeline();
        }
        execute("ALTER TABLE tx ADD COLUMN note INT");
        drainWalQueue();
        final AtomicBoolean isTripped = tripOnTheDrift(job, () -> engine.setClosing(true));
        advanceClockToNextRefreshPass();
        try {
            job.run();
        } finally {
            engine.setClosing(false);
        }
        hooks(job).setRefreshFaultObserver(null);
        Assert.assertTrue("the drift must have reached the handler", isTripped.get());
    }
}
