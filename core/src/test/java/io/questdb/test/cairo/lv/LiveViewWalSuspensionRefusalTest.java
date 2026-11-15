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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * A refresh turn that passed the hold of a paused view before an operator's SUSPEND WAL (or the
 * view's listing in cairo.wal.apply.suspended.tables) landed meets, with
 * cairo.wal.apply.suspended.write.denied on, the write gate's refusal at its next write to the view's
 * own WAL. Wherever it meets it - a lead flush, a coupled drain, a seed turn, an out-of-order repair,
 * the restart's restore and rebuild, the recoveries of a fault and the last resorts of a spent
 * flush-retry budget - the refusal is a pause: nothing is charged, no recovery falls back, the view
 * is not invalidated, and the debt the turn left waits for the first turn after RESUME WAL.
 * <p>
 * The suspension lands through the real gate: a hook ahead of the turn's latch runs the statement, or
 * a one-shot observer runs what the statement runs right before the next commit to the view's WAL.
 */
public class LiveViewWalSuspensionRefusalTest extends AbstractLiveViewTxTest {
    private static final String PAUSE_LINE = "live view refresh paused, its WAL is suspended [view=lv";
    private static final String RESTART_REBUILD_RETRY_LINE = "live view restart rebuild hit a retryable fault, retrying [view=lv";
    private static final LogCapture capture = new LogCapture();

    @After
    public void tearDownSuspension() {
        CairoEngine.setRoleSwitchMintObserver(null);
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpSuspension() {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testDropOfAViewPausedWithAParkedRepair() throws Exception {
        // DROP LIVE VIEW of a paused view that holds a repair parked on its turn budget, with the repair's
        // pinned base snapshot and uncommitted replacement. The DROP discards the repair, frees what it held
        // and unregisters the view, and a view created again under the same name holds the recompute.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                parkARepair(job);
                final LiveViewInstance instance = instance();
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                Assert.assertFalse(job.processNotificationsForTest());
                Assert.assertNotNull("the pause keeps the parked repair", instance.getSuspendedRepair());

                execute("DROP LIVE VIEW lv");
                Assert.assertTrue(instance.isDropped());
                Assert.assertNull("the DROP discards the parked repair", instance.getSuspendedRepair());
                Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
                assertQuery("SELECT count() FROM live_views()")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n0\n");
                drainJob(job);

                execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testFaultInTheTurnSuspendLandsInIsHandledAsBefore() throws Exception {
        // With write denial off, a turn meets a mid-drain fault, and SUSPEND WAL lands while the turn handles
        // it. The fault is no refusal, so it is handled as without the suspension - the runtime restored from
        // the timeline in that turn, the fault charged to the duration budget only - and from the next pass
        // the pause holds the view, past the retry back-off, until RESUME WAL.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "false");
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isArmed = new AtomicBoolean(true);
                hooks(job).setRefreshFaultObserver(t -> {
                    if (isArmed.compareAndSet(true, false)) {
                        try {
                            execute("ALTER LIVE VIEW lv SUSPEND WAL");
                        } catch (SqlException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                // The first commit lands in the un-flushed lead, and the next two coalesce behind it into
                // one pass, which fails reading the third after feeding the second.
                setCurrentMicros(instance.getLastFlushTimeUs());
                insert("('2026-01-01T09:40:00.000000Z', 'acct-1', 16.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-01T09:50:00.000000Z', 'acct-2', 32.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-01T10:00:00.000000Z', 'acct-1', 64.0)");
                drainWalQueue();
                fault.arm(2);
                drainJob(job);
                Assert.assertTrue(fault.hasFired());
                Assert.assertFalse("the fault observer must have suspended the view", isArmed.get());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals("a recovered fault is charged to the duration budget only", 0, instance.getFlushRetryCount());
                Assert.assertNotEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
                capture.drain();
                capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=mid-drain refresh failure");
                capture.assertLogged("live view refresh failed, window state recovered, retrying [view=lv");
                capture.assertNotLogged(PAUSE_LINE);

                Assert.assertFalse(drivePass(job));
                Assert.assertFalse(drivePass(job));
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                resumeAndConverge(job);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
            }
        });
    }

    @Test
    public void testParkedRepairWaitsOutThePause() throws Exception {
        // A repair parked on its turn budget when SUSPEND WAL lands stays parked through the pause, with its
        // pinned base snapshot and uncommitted replacement: no turn continues it, the job reports no work and
        // the view's WAL does not move. After RESUME WAL the repair finishes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                parkARepair(job);
                final LiveViewInstance instance = instance();
                final Object repair = instance.getSuspendedRepair();
                final long seqTxn = engine.getTableSequencerAPI().getTxnTracker(lvToken()).getSeqTxn();
                execute("ALTER LIVE VIEW lv SUSPEND WAL");
                for (int pass = 0; pass < 3; pass++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    Assert.assertFalse("a paused view's parked repair must not be continued", job.processNotificationsForTest());
                }
                Assert.assertSame(repair, instance.getSuspendedRepair());
                Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().getTxnTracker(lvToken()).getSeqTxn());
                Assert.assertEquals(0, instance.getRefreshFaultCount());

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveRefreshToQuiescence(job);
                Assert.assertNull(instance.getSuspendedRepair());
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testRefusalAtACoupledDrainCommitIsAPause() throws Exception {
        // A deduplicating base's coupled drain feeds two commits' rows, and the write gate refuses its commit:
        // SUSPEND WAL landed right before it. A pause, uncharged, and the fed rows stay owed: after RESUME
        // the gate restores the runtime before the drain, so no accumulator counts a row twice.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createFixture("DEDUP UPSERT KEYS(created_at, account_id)");
            final LiveViewInstance instance = instance();
            final long processed = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insert("('2026-01-02T09:00:00.000000Z', 'acct-1', 16.0), ('2026-01-02T09:10:00.000000Z', 'acct-2', 32.0)");
                final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                setCurrentMicros(currentMicros + 1_000_000L);
                job.run();
                Assert.assertFalse("the drain's commit must meet the suspension", isArmed.get());
                assertPausedUncharged(instance, 1);
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertTrue("the drain fed rows before its commit", instance.isWindowStateDirty());
                Assert.assertEquals(processed, instance.getLastProcessedSeqTxn());
                Assert.assertFalse(drivePass(job));
                Assert.assertFalse(drivePass(job));
                resumeAndConverge(job);
            }
        });
    }

    @Test
    public void testRefusalAtAParkedRepairsCommitDiscardsTheRepair() throws Exception {
        // A repair continued from its park reaches its commit, and the write gate refuses it: SUSPEND WAL
        // landed right before it. The refusal discards the repair, as any fault of a turn does, and pauses
        // the view uncharged. After RESUME WAL the view repairs the late row again and ends on the recompute.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                parkARepair(job);
                final LiveViewInstance instance = instance();
                final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && isArmed.get(); pass++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    job.processNotificationsForTest();
                }
                Assert.assertFalse("the repair's commit must meet the suspension", isArmed.get());
                Assert.assertNull("a refused repair is discarded", instance.getSuspendedRepair());
                assertPausedUncharged(instance, 1);
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertFalse(drivePass(job));
                Assert.assertFalse(drivePass(job));
                resumeAndConverge(job);
            }
        });
    }

    @Test
    public void testRefusalAtASeedCommitIsAPause() throws Exception {
        // A seed turn feeds its rows and the write gate refuses its commit: SUSPEND WAL landed right before
        // it. A pause, uncharged, which re-arms the sweep's resume, so after RESUME WAL the sweep restores its
        // window state before it feeds a row again and completes on the recompute.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tx (created_at, account_id, amount) VALUES
                    ('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0),
                    ('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0),
                    ('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0),
                    ('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0),
                    ('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0),
                    ('2026-01-03T09:10:00.000000Z', 'acct-2', 32.0)""");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = instance();
                for (int i = 0; i < 100 && instance.getSeedDataOffset() < 2; i++) {
                    job.run();
                    drainWalQueue();
                }
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
                Assert.assertTrue(instance.isSeedResumeAttempted());
                final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                for (int i = 0; i < 100 && isArmed.get(); i++) {
                    job.run();
                }
                Assert.assertFalse("the seed turn's commit must meet the suspension", isArmed.get());
                assertPausedUncharged(instance, 1);
                Assert.assertFalse("the refusal re-arms the sweep's resume", instance.isSeedResumeAttempted());
                Assert.assertFalse(drivePass(job));
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());

                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRefusalAtTheLeadFlushAcquireIsAPause() throws Exception {
        // SUSPEND WAL lands after a turn passed the pause's hold. The turn drains a commit into the lead and
        // the write gate refuses the writer the lead's flush acquires. A pause: the view stays valid, nothing
        // is charged, and the rows the drain fed stay owed to the first turn after RESUME WAL.
        assertMemoryLeak(() -> {
            createFixture("");
            final LiveViewInstance instance = instance();
            final long processed = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isArmed = suspendBeforeNextLatch(job);
                insert("('2026-01-02T09:00:00.000000Z', 'acct-1', 16.0)");
                setCurrentMicros(currentMicros + 1_000_000L);
                job.run();
                Assert.assertFalse("the suspension must land in the turn", isArmed.get());
                assertPausedUncharged(instance, 1);
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertTrue("the drain fed the commit's row before the refusal", instance.isWindowStateDirty());
                Assert.assertEquals(processed, instance.getLastProcessedSeqTxn());
                Assert.assertFalse(drivePass(job));
                Assert.assertFalse(drivePass(job));
                assertPausedUncharged(instance, 1);
                resumeAndConverge(job);
            }
        });
    }

    @Test
    public void testRefusalAtTheRestartRebuildsCommitIsAPause() throws Exception {
        // A restart finds no timeline and rebuilds the view from the applied base, and the write gate refuses
        // that rebuild's commit: SUSPEND WAL landed right before it. The rebuild hands the refusal on as a
        // fault a retry gets past, rather than stopping the view, and the turn pauses uncharged with the
        // window-state debt standing. After RESUME WAL the gate rebuilds, and a later restart restores.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createFixture("");
            removeTimeline();
            shutdown();
            engine.buildViewGraphs();
            final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int pass = 0; pass < 8 && isArmed.get(); pass++) {
                    drivePass(job);
                }
                Assert.assertFalse("the restart rebuild's commit must meet the suspension", isArmed.get());
                final LiveViewInstance instance = instance();
                assertPausedUncharged(instance, 1);
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertTrue(instance.isWindowStateDirty());
                capture.assertOnlyOnce(Pattern.quote(RESTART_REBUILD_RETRY_LINE));
                Assert.assertFalse(drivePass(job));
                Assert.assertFalse(drivePass(job));
                resumeAndConverge(job);
            }
            shutdown();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testRefusalOfADriftRecoveryRebuildIsAPause() throws Exception {
        // A base ADD COLUMN drifts the view's plan, the drift recovery finds no timeline and rebuilds the
        // view from the applied base, and the write gate refuses that rebuild's commit: SUSPEND WAL landed
        // right before it. The recovery hands the refusal back, and the turn pauses uncharged with the
        // window-state debt the drift left. After RESUME WAL the view converges.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createFixture("DEDUP UPSERT KEYS(created_at, account_id)");
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                removeTimeline();
                execute("ALTER TABLE tx ADD COLUMN note INT");
                drainWalQueue();
                final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                for (int pass = 0; pass < 8 && isArmed.get(); pass++) {
                    drivePass(job);
                }
                Assert.assertFalse("the drift recovery's rebuild must meet the suspension", isArmed.get());
                assertPausedUncharged(instance, 1);
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertFalse(drivePass(job));
                resumeAndConverge(job);
            }
        });
    }

    @Test
    public void testRefusalOfAFlushThatFedNothingLeavesNoDebt() throws Exception {
        // An earlier turn drained the commit into the lead, so the turn SUSPEND WAL lands in only flushes, and
        // the write gate refuses the writer that flush acquires. The turn fed no row, so it owes nothing: the
        // lead stays published and readable, and after RESUME WAL the flush lands it with no runtime restore.
        assertMemoryLeak(() -> {
            createFixture("");
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insert("('2026-01-02T09:00:00.000000Z', 'acct-1', 16.0)");
                instance.setLastFlushTimeUs(currentMicros);
                job.run();
                Assert.assertEquals("the drain must publish a lead", 1, instance.getLeadRowCount());
                final AtomicBoolean isArmed = suspendBeforeNextLatch(job);
                setCurrentMicros(currentMicros + 1_000_000L);
                job.run();
                Assert.assertFalse("the suspension must land in the flush's turn", isArmed.get());
                assertPausedUncharged(instance, 1);
                Assert.assertFalse("a turn that fed nothing owes nothing", instance.isWindowStateDirty());
                Assert.assertEquals(1, instance.getLeadRowCount());
                assertViewMatchesRecompute();
                Assert.assertFalse(drivePass(job));

                capture.drain();
                final int restores = capture.countOccurrences("live view restored its runtime from the checkpoint timeline");
                execute("ALTER LIVE VIEW lv RESUME WAL");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(0, instance.getLeadRowCount());
                assertViewMatchesRecompute();
                capture.drain();
                Assert.assertEquals("no runtime restore after a pause that left no debt", restores,
                        capture.countOccurrences("live view restored its runtime from the checkpoint timeline"));
                Assert.assertEquals("no fault after the refusal", 1, instance.getRefreshFaultCount());
            }
        });
    }

    @Test
    public void testRefusalOfAMidDrainRecoveryRebuildIsAPause() throws Exception {
        // A drain fails between two commits after feeding the first, the mid-drain recovery finds no
        // timeline and rebuilds the view, and the write gate refuses that rebuild's commit: SUSPEND WAL landed
        // right before it. Neither the fault nor the refusal is charged: the turn pauses with the debt the
        // recovery left, and the view converges after RESUME WAL.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
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
                final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                fault.arm(2);
                drainJob(job);
                Assert.assertTrue(fault.hasFired());
                Assert.assertFalse("the recovery's rebuild must meet the suspension", isArmed.get());
                assertPausedUncharged(instance, 1);
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertFalse(drivePass(job));
                resumeAndConverge(job);
            }
        });
    }

    @Test
    public void testRefusalOfARepairInAMidDrainRestoresReplayIsAPause() throws Exception {
        // A drain fails between two commits, and the mid-drain recovery restores the runtime from the
        // timeline. Its replay of the gap above the root meets a commit out of order within itself and hands
        // off to the out-of-order repair, whose commit the write gate refuses: SUSPEND WAL landed right before
        // it. The restore hands the refusal back rather than fall back to the whole-view rebuild, and the
        // turn pauses uncharged. The view converges after RESUME WAL.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createO3GapFixture();
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(instance.getLastFlushTimeUs());
                insert("('2026-01-04T09:00:00.000000Z', 'acct-1', 1.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:10:00.000000Z', 'acct-1', 2.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:20:00.000000Z', 'acct-1', 4.0)");
                drainWalQueue();
                final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                fault.arm(2);
                for (int pass = 0; pass < 8 && isArmed.get(); pass++) {
                    drainJob(job);
                    advanceClockToNextRefreshPass();
                }
                Assert.assertTrue(fault.hasFired());
                Assert.assertFalse("the restore's repair must meet the suspension", isArmed.get());
                capture.drain();
                capture.assertLoggedRE("live view O3 replay \\[view=lv, lateRowTs=");
                capture.assertLogged("live view could not restore its runtime from the checkpoint timeline, retrying [view=lv, cause=mid-drain refresh failure");
                capture.assertNotLogged("live view could not restore its runtime from the checkpoint timeline, rebuilding from the applied base");
                assertPausedUncharged(instance, 1);
                Assert.assertEquals(0, instance.getCheckpointRebuildAttempts());
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertFalse(drivePass(job));
                resumeAndConverge(job);
            }
        });
    }

    @Test
    public void testRefusalOfARepairInTheRestartRestoresReplayIsAPause() throws Exception {
        // A restart restores the view from its timeline, and the replay of the gap above the root meets a
        // commit out of order within itself and hands off to the out-of-order repair, whose commit the write
        // gate refuses: SUSPEND WAL landed right before it. The restore hands the refusal on as a fault a retry
        // gets past rather than fall back to the whole-view rebuild, and the turn pauses uncharged. The view
        // converges after RESUME WAL.
        assertMemoryLeak(() -> {
            createO3GapFixture();
            shutdown();
            engine.buildViewGraphs();
            final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int pass = 0; pass < 8 && isArmed.get(); pass++) {
                    drivePass(job);
                }
                Assert.assertFalse("the restore's repair must meet the suspension", isArmed.get());
                final LiveViewInstance instance = instance();
                capture.drain();
                capture.assertLoggedRE("live view O3 replay \\[view=lv, lateRowTs=");
                capture.assertLogged("could not restore live view from checkpoint timeline, retrying [view=lv");
                capture.assertNotLogged("could not restore live view from checkpoint timeline, rebuilding derived state");
                assertPausedUncharged(instance, 1);
                Assert.assertEquals(0, instance.getCheckpointRebuildAttempts());
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertFalse(drivePass(job));
                resumeAndConverge(job);
            }
        });
    }

    @Test
    public void testRefusalOfTheGatesRebuildIsAPause() throws Exception {
        // An earlier mid-drain fault left the window-state debt: its recovery found no timeline and its rebuild
        // failed. The gate of the next turn rebuilds again, and the write gate refuses that rebuild's commit:
        // SUSPEND WAL landed right before it. The gate hands the refusal on, the turn pauses without a charge
        // of its own, and the debt stands until the gate of the first turn after RESUME WAL rebuilds.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
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
                engine.releaseInactive();
                fault.arm(2);
                fault.armAppliedScan();
                drainJob(job);
                Assert.assertTrue(fault.hasFired());
                Assert.assertTrue("the recovery's rebuild must have failed", fault.hasAppliedScanFired());
                Assert.assertTrue("the failed recovery leaves the debt", instance.isWindowStateDirty());
                final int retries = instance.getFlushRetryCount();
                final long streakStartUs = instance.getFlushRetryStartUs();
                final long faults = instance.getRefreshFaultCount();
                capture.drain();
                capture.assertNotLogged(PAUSE_LINE);

                final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                advanceClockToNextRefreshPass();
                drainJob(job);
                Assert.assertFalse("the gate's rebuild must meet the suspension", isArmed.get());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals("the refusal is not charged", retries, instance.getFlushRetryCount());
                Assert.assertEquals(streakStartUs, instance.getFlushRetryStartUs());
                Assert.assertEquals(faults + 1, instance.getRefreshFaultCount());
                Assert.assertTrue("the debt stands until a recovery settles it", instance.isWindowStateDirty());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences(PAUSE_LINE));
                capture.assertNotLogged("budget exhausted");
                Assert.assertFalse(drivePass(job));
                Assert.assertFalse(drivePass(job));
                resumeAndConverge(job);
            }
        });
    }

    @Test
    public void testRefusalOfTheRebuildBehindASpentRestoreBudgetIsAPause() throws Exception {
        // Every open of the view's timeline fails on an error a retry may get past, so every restore the
        // flush-retry budget allows fails, and the turn that spends the budget takes the whole-view rebuild.
        // The write gate refuses that rebuild's commit: SUSPEND WAL landed right before it. A pause, not the
        // invalidation a failed last resort ends in; after the fault clears and RESUME WAL, the view converges.
        // Without the suspension the same rebuild heals the view: only the refusal is a pause.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            for (int suspended = 0; suspended < 2; suspended++) {
                final boolean isSuspended = suspended == 1;
                final String label = "[suspended=" + isSuspended + ']';
                setCurrentMicros(0);
                createFixture("");
                fault.of(engine.verifyTableName("tx").getDirName());
                shutdown();
                engine.buildViewGraphs();
                fault.armTimelineOpenUntilDisarmed(13);
                final LiveViewInstance instance = instance();
                final int maxRetry = configuration.getLiveViewFlushRetryMax();
                capture.drain();
                final int pausesBefore = capture.countOccurrences(PAUSE_LINE);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    Assert.assertTrue(label + ": the restart's restore must have been failed", fault.getTimelineOpenFailures() > 0);
                    insert("('2026-01-02T09:00:00.000000Z', 'acct-1', 16.0)");
                    for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && instance.getRefreshFaultCount() < maxRetry - 1; pass++) {
                        advanceClockToNextRefreshPass();
                        drainJob(job);
                    }
                    Assert.assertEquals(label, maxRetry - 1, instance.getRefreshFaultCount());
                    Assert.assertFalse(label, instance.isInvalid());

                    final AtomicBoolean isArmed = isSuspended ? suspendAtNextLiveViewCommit() : new AtomicBoolean();
                    advanceClockToNextRefreshPass();
                    drainJob(job);
                    Assert.assertFalse(label + ": the last resort's rebuild must meet the suspension", isArmed.get());
                    Assert.assertFalse(label + ": the last resort must not invalidate the view [reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
                    Assert.assertFalse(label, instance.hasPendingInvalidationReason());
                    Assert.assertFalse(label, instance.isCheckpointRecoveryBlocked());
                    capture.drain();
                    capture.assertLogged("live view refresh budget exhausted retrying a failed restore, rebuilding from the applied base [view=lv");
                    capture.assertNotLogged("live view refresh budget exhausted, invalidating");
                    Assert.assertEquals(label, pausesBefore + (isSuspended ? 1 : 0), capture.countOccurrences(PAUSE_LINE));
                    if (isSuspended) {
                        Assert.assertTrue(label + ": the debt stands for the turn after RESUME", instance.isWindowStateDirty());
                        Assert.assertFalse(label, drivePass(job));
                    } else {
                        Assert.assertFalse(label + ": the rebuild settles the debt", instance.isWindowStateDirty());
                        assertViewMatchesRecompute();
                    }
                }
                fault.disarmAll();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    resumeAndConverge(job);
                }
                CairoEngine.setRoleSwitchMintObserver(null);
                execute("DROP LIVE VIEW lv");
                execute("DROP TABLE tx");
            }
        });
    }

    @Test
    public void testRefusalOfTheRederiveAfterBaseWalLossIsAPause() throws Exception {
        // The base lost the WAL segment of a commit the view has yet to drain, so every drain the flush-retry
        // budget allows fails, and the turn that spends the budget re-derives the view from the applied base.
        // The write gate refuses the re-derive's commit - SUSPEND WAL landed right before it - or, when a base
        // schema change lands during the re-derive, the commit of the rebuild it recompiles for that change.
        // Each refusal is a pause, not the invalidation a failed re-derive ends in, and after RESUME WAL the view
        // re-derives and converges. A re-derive that fails otherwise - the scan of the applied base fails -
        // still invalidates the view: only the refusal is a pause.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            for (int variant = 0; variant < 4; variant++) {
                final boolean isDrifted = (variant & 1) == 1;
                final boolean isRefused = variant < 2;
                final String label = "[drift=" + isDrifted + ", refused=" + isRefused + ']';
                setCurrentMicros(0);
                createFixture("");
                fault.of(engine.verifyTableName("tx").getDirName());
                // Applied to the base, not drained by the view.
                insert("('2026-01-02T09:00:00.000000Z', 'acct-1', 16.0)");
                shutdown();
                removeBaseWal("tx");
                engine.buildViewGraphs();
                capture.drain();
                final int pausesBefore = capture.countOccurrences(PAUSE_LINE);
                final int recompilesBefore = capture.countOccurrences("live view re-derive found the base metadata changed, recompiling once [view=lv");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    final AtomicBoolean isFailureArmed = new AtomicBoolean();
                    if (isDrifted) {
                        hooks(job).setSimulateBaseApplyDuringRederive(() -> {
                            try {
                                execute("ALTER TABLE tx ADD COLUMN note INT");
                                drainWalQueue();
                            } catch (SqlException e) {
                                throw new AssertionError(e);
                            }
                            if (isRefused) {
                                suspendAtNextLiveViewCommit();
                            } else {
                                engine.releaseInactive();
                                fault.armAppliedScan();
                            }
                            isFailureArmed.set(true);
                        });
                    } else if (isRefused) {
                        suspendAtNextLiveViewCommit();
                        isFailureArmed.set(true);
                    } else {
                        fault.armAppliedScan();
                        isFailureArmed.set(true);
                    }
                    final LiveViewInstance instance = instance();
                    for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !instance.isInvalid() && countPauses() == pausesBefore; pass++) {
                        advanceClockToNextRefreshPass();
                        drainJob(job);
                    }
                    Assert.assertTrue(label + ": the re-derive must have met the armed failure", isFailureArmed.get());
                    capture.drain();
                    capture.assertNotLogged("live view re-derived from the applied base after base WAL loss");
                    Assert.assertEquals(label, recompilesBefore + (isDrifted ? 1 : 0),
                            capture.countOccurrences("live view re-derive found the base metadata changed, recompiling once [view=lv"));
                    if (!isRefused) {
                        Assert.assertTrue(label + ": the applied scan must have been failed", fault.hasAppliedScanFired());
                        Assert.assertTrue(label + ": a re-derive that fails must invalidate the view", instance.isInvalid());
                        TestUtils.assertEquals("flush retry budget exhausted", instance.getInvalidationReason());
                        Assert.assertEquals(label, pausesBefore, capture.countOccurrences(PAUSE_LINE));
                    } else {
                        Assert.assertFalse(label + ": a refused re-derive must not invalidate the view [reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
                        Assert.assertFalse(label, instance.hasPendingInvalidationReason());
                        Assert.assertFalse(label, instance.isCheckpointRecoveryBlocked());
                        Assert.assertEquals(label, pausesBefore + 1, capture.countOccurrences(PAUSE_LINE));
                        capture.assertNotLogged("live view refresh budget exhausted, invalidating");
                        Assert.assertFalse(label, drivePass(job));

                        hooks(job).setSimulateBaseApplyDuringRederive(null);
                        execute("ALTER LIVE VIEW lv RESUME WAL");
                        final long baseHead = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("tx")).getWriterTxn();
                        for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && instance.getLastProcessedSeqTxn() < baseHead; pass++) {
                            advanceClockToNextRefreshPass();
                            drainJob(job);
                        }
                        driveRefreshToQuiescence(job);
                        Assert.assertFalse(label, instance.isInvalid());
                        assertViewMatchesRecompute();
                    }
                }
                fault.disarmAll();
                CairoEngine.setRoleSwitchMintObserver(null);
                execute("DROP LIVE VIEW lv");
                execute("DROP TABLE tx");
            }
        });
    }

    @Test
    public void testRefusalRacedByResumeOnAViewPastItsRetryBudgetIsStillAPause() throws Exception {
        // A fault 61 s earlier started a flush-retry streak, so the retry's turn would spend the duration budget
        // on any fault it is charged for. Its commit meets the write gate's refusal, and while the turn handles
        // it the suspension ends: RESUME WAL, or write denial reloaded off with the view still suspended. The
        // refusal is still a pause - only the view's own suspension raises it - so nothing is charged, no last
        // resort runs and the view stays valid. After RESUME the next pass drives the view; with write denial
        // reloaded off the view stays paused until RESUME WAL.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int variant = 0; variant < 2; variant++) {
                    final boolean isResumed = variant == 0;
                    final String label = isResumed ? "[RESUME WAL]" : "[write denial reloaded off]";
                    setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
                    // A drain fault that fed nothing is charged once and starts the streak.
                    insert("('2026-01-0" + (2 + variant) + "T09:00:00.000000Z', 'acct-1', 16.0)");
                    fault.arm(0);
                    setCurrentMicros(currentMicros + 1_000_000L);
                    job.run();
                    Assert.assertTrue(label, fault.hasFired());
                    Assert.assertEquals(label, 1, instance.getFlushRetryCount());
                    final long streakStartUs = instance.getFlushRetryStartUs();
                    Assert.assertNotEquals(label, Numbers.LONG_NULL, streakStartUs);
                    final long rebuildAttempts = instance.getCheckpointRebuildAttempts();

                    final AtomicBoolean isLifted = new AtomicBoolean(true);
                    hooks(job).setRefreshFaultObserver(t -> {
                        if (isLifted.compareAndSet(true, false)) {
                            if (isResumed) {
                                try {
                                    execute("ALTER LIVE VIEW lv RESUME WAL");
                                } catch (SqlException e) {
                                    throw new AssertionError(e);
                                }
                            } else {
                                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "false");
                            }
                        }
                    });
                    final AtomicBoolean isArmed = suspendAtNextLiveViewCommit();
                    setCurrentMicros(streakStartUs + 61_000_000L);
                    job.run();
                    hooks(job).setRefreshFaultObserver(null);
                    Assert.assertFalse(label + ": the retry's commit must meet the suspension", isArmed.get());
                    Assert.assertFalse(label + ": the suspension must end while the turn handles the refusal", isLifted.get());
                    Assert.assertFalse(label + " [reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
                    Assert.assertFalse(label, instance.hasPendingInvalidationReason());
                    Assert.assertEquals(label, 1, instance.getFlushRetryCount());
                    Assert.assertEquals(label, streakStartUs, instance.getFlushRetryStartUs());
                    Assert.assertEquals(label, rebuildAttempts, instance.getCheckpointRebuildAttempts());
                    capture.drain();
                    capture.assertNotLogged("budget exhausted");
                    Assert.assertEquals(label, variant + 1, capture.countOccurrences(PAUSE_LINE));

                    if (isResumed) {
                        Assert.assertTrue(label + ": the resumed view must be driven", drivePass(job));
                    } else {
                        Assert.assertFalse(label + ": the view stays suspended", drivePass(job));
                        Assert.assertFalse(label, drivePass(job));
                        execute("ALTER LIVE VIEW lv RESUME WAL");
                    }
                    driveRefreshToQuiescence(job);
                    assertViewMatchesRecompute();
                    Assert.assertFalse(label, instance.isInvalid());
                    Assert.assertEquals(label + ": the turn that got past the commits ends the streak", 0, instance.getFlushRetryCount());
                }
            }
        });
    }

    private static void assertPausedLogs(int expectedPauses) {
        capture.drain();
        capture.assertNotLogged("live view refresh failed");
        capture.assertNotLogged("budget exhausted");
        capture.assertNotLogged("live view restart applied-base rebuild failed");
        Assert.assertEquals("pause lines", expectedPauses, capture.countOccurrences(PAUSE_LINE));
    }

    private static void assertPausedUncharged(LiveViewInstance instance, int expectedPauses) {
        Assert.assertFalse("a refusal must not invalidate the view [reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
        Assert.assertFalse("a refusal must not stash an invalidation", instance.hasPendingInvalidationReason());
        Assert.assertEquals("a refusal must not count toward the flush-retry budget", 0, instance.getFlushRetryCount());
        Assert.assertEquals("a refusal must not start a flush-retry streak", Numbers.LONG_NULL, instance.getFlushRetryStartUs());
        Assert.assertEquals("a refusal must not arm the refresh-retry back-off", Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
        assertPausedLogs(expectedPauses);
    }

    private static int countPauses() {
        capture.drain();
        return capture.countOccurrences(PAUSE_LINE);
    }

    // One pass with the clock moved past the refresh-retry back-off and FLUSH EVERY.
    private static boolean drivePass(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + 61_000_000L);
        drainWalQueue();
        final boolean isWork = drainJob(job);
        drainWalQueue();
        return isWork;
    }

    private static TableToken lvToken() {
        return engine.verifyTableName("lv");
    }

    private static String repairRow(int day, int hour, String account) {
        return "('2026-01-0" + day + "T0" + hour + ":00:00.000000Z', '" + account + "', 1.0)";
    }

    // What ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL', 'manual halt' runs, once, right before the next
    // commit to a live view's WAL: inside the commit's fence, after the commit acquired its writer.
    private static AtomicBoolean suspendAtNextLiveViewCommit() {
        final TableToken token = lvToken();
        final AtomicBoolean isArmed = new AtomicBoolean(true);
        CairoEngine.setRoleSwitchMintObserver(() -> {
            if (isArmed.compareAndSet(true, false)) {
                engine.addWalApplySuspended(token);
                engine.getTableSequencerAPI().suspendTable(token, ErrorTag.DISK_FULL, "manual halt");
            }
        });
        return isArmed;
    }

    // Runs ALTER LIVE VIEW lv SUSPEND WAL once, ahead of the next turn's latch: after every hold the turn
    // passes on its way in.
    private static AtomicBoolean suspendBeforeNextLatch(LiveViewRefreshJob job) {
        final AtomicBoolean isArmed = new AtomicBoolean(true);
        hooks(job).setSimulatePeerTurnBeforeRefreshLatch(() -> {
            if (isArmed.compareAndSet(true, false)) {
                try {
                    execute("ALTER LIVE VIEW lv SUSPEND WAL WITH 'DISK FULL', 'manual halt'");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            }
        });
        return isArmed;
    }

    // tx partitioned by hour with three days of rows and a view refreshed over them, head rows on the fifth
    // day, then a correction on the second whose repair parks on its turn budget, which
    // cairo.live.view.checkpoint.repair.replay.max.rows = 1 makes one row.
    private void parkARepair(LiveViewRefreshJob job) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int hour = 1; hour <= 4; hour++) {
                rows.append(rows.length() > 0 ? ", " : "").append(repairRow(day, hour, "acct-1"));
            }
            rows.append(", ").append(repairRow(day, 1, "acct-2"));
        }
        insert(rows.toString());
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
        driveRefreshToQuiescence(job);
        insert(repairRow(5, 1, "acct-1") + ", " + repairRow(5, 2, "acct-2"));
        driveRefreshToQuiescence(job);
        insert(repairRow(2, 5, "acct-1"));
        driveUntilParked(job, "lv");
    }

    // RESUME WAL, then a base commit and a drive: the view continues from where the pause left it.
    private void resumeAndConverge(LiveViewRefreshJob job) throws Exception {
        execute("ALTER LIVE VIEW lv RESUME WAL");
        driveRefreshToQuiescence(job);
        insert("('2026-01-02T10:00:00.000000Z', 'acct-1', 1000.0)");
        drivePass(job);
        driveRefreshToQuiescence(job);
        assertViewMatchesRecompute();
        Assert.assertFalse(instance().isInvalid());
        Assert.assertFalse(instance().isWindowStateDirty());
    }
}
