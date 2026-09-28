/*+*****************************************************************************
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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The per-view backoff that paces a live view's refresh retries after a faulting turn.
 * <p>
 * A refresh turn that faults leaves the view in front of the commits it could not drain, so the
 * fallback scan finds it lagging on the very next pass. Without a backoff the refresh worker
 * re-drives it at once - a faulting turn reports work, so {@code Worker.loopLegacy} never naps -
 * and the flush-retry count budget ({@code cairo.live.view.flush.retry.max}, 5 by default) runs
 * out a few milliseconds after the first fault. A fault that lasts longer than that - an EMFILE
 * under load, an EIO on network storage - then invalidates the view for good.
 * <p>
 * The cases that measure the pacing drive the job the way an idle refresh worker does: one
 * {@code run()} per millisecond of the simulated clock. A worker spins faster than that, and the
 * pacing does not depend on it: the view is due at its deadline, not after some number of passes.
 * The schedule itself is the one {@link AbstractLiveViewTest#refreshRetryStreakMicros} spells out.
 */
public class LiveViewRefreshRetryBackoffTest extends AbstractLiveViewTest {
    private static final String CREATE_VIEW_SQL = "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
            + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
            + "count(account_id) OVER w AS cumulative_count "
            + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')";
    private static final String DEDUP_UPSERT_KEYS = "DEDUP UPSERT KEYS(created_at, account_id)";
    private static final String EIGHTH_ROW_COMMIT = "INSERT INTO tx (created_at, account_id, amount) VALUES "
            + "('2026-01-02T09:50:00.000000Z', 'acct-1', 128.0)";
    private static final String EIGHTH_ROW_OUTPUT = "2026-01-02T09:50:00.000000Z\tacct-1\t172.0\t4\n";
    private static final int ERRNO_EIO = 5;
    private static final int ERRNO_EMFILE = 24;
    private static final String[] FOUR_ROWS = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)"
    };
    private static final String FOUR_ROWS_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            """;
    private static final String SEVEN_ROWS_OUTPUT = FOUR_ROWS_OUTPUT + """
            2026-01-02T09:20:00.000000Z\tacct-2\t16.0\t1
            2026-01-02T09:30:00.000000Z\tacct-1\t44.0\t3
            2026-01-02T09:40:00.000000Z\tacct-2\t80.0\t2
            """;
    private static final String THREE_ROWS_COMMIT = "INSERT INTO tx (created_at, account_id, amount) VALUES "
            + "('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0), "
            + "('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0), "
            + "('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)";
    // Longer than the time the default count budget lasts under the schedule (1.5s), and far
    // inside the default duration budget (60s).
    private static final long RECOVERED_FAULT_MICROS = 2_000_000;
    // How long the stalled-apply case watches the job after the view's deadline: 200 apply-lag
    // back-off windows of 5ms each.
    private static final long STALLED_APPLY_MICROS = 1_000_000;
    // Well past the 3ms a fault used to get, and shorter than the time the default count budget
    // lasts under the refresh-retry schedule (100 + 200 + 400 + 800 ms = 1.5s).
    private static final long TRANSIENT_FAULT_MICROS = 1_000_000;
    private static final String VIEW_ROWS_QUERY = "SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv";
    // An idle refresh worker re-runs its job well within a millisecond; see the class comment.
    private static final long WORKER_TICK_MICROS = 1_000;
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpClock() {
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testABaseCommitDuringTheBackoffDoesNotHoldADedupViewBehindAStalledApply() throws Exception {
        // The deduplicating-base variant of the case below, with the base's WAL apply stalled. Such a
        // view reads the applied base whenever the raw WAL cannot be proven to match it.
        assertAHeldBackTargetDoesNotHoldAnAppliedBaseViewBehindAStalledApply(false);
    }

    @Test
    public void testABaseCommitDuringTheBackoffDoesNotHoldAReplicaViewBehindAStalledApply() throws Exception {
        // The same on a read-only replica, where every view reads the applied base: the refresh job
        // there prefers the applied base (prefersAppliedBaseRefresh), which the job below stands in
        // for over a base without dedup keys.
        assertAHeldBackTargetDoesNotHoldAnAppliedBaseViewBehindAStalledApply(true);
    }

    @Test
    public void testABaseCommitDuringTheBackoffNeitherBypassesNorLosesTheRetry() throws Exception {
        // Any worker takes a base-table notification, so a commit that lands while the view backs off
        // reaches refreshInstance on whichever worker dequeues it. It must not re-drive the view early,
        // which would bring the unpaced retry back for any view whose base keeps ingesting, and it
        // must not be lost either: the worker consumes the notification, and once the deadline passes
        // the fallback scan must drive the view as far as the notification asked. The base does not
        // apply the new commit here, so a scan that went only as far as the base's applied head would
        // leave it out. Two workers, so the notification and the scan run on different jobs, and the
        // scan is sharded between them.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance instance = createViewOverFourRows(fault);
            final TableToken baseToken = engine.verifyTableName("tx");
            final long durableSeqTxn = instance.getLastProcessedSeqTxn();
            try (
                    LiveViewRefreshJob job0 = new LiveViewRefreshJob(0, 2, engine, 1);
                    LiveViewRefreshJob job1 = new LiveViewRefreshJob(1, 2, engine, 1)
            ) {
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                final long firstFaultUs = currentMicros;
                fault.arm(0);
                Assert.assertTrue("the faulting turn is work", job0.run());
                Assert.assertTrue("the drain's first read must have been failed", fault.hasFired());
                final long notBeforeUs = instance.getRefreshRetryNotBeforeUs();
                Assert.assertEquals(firstFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, notBeforeUs);

                // A commit lands mid-backoff, and the other worker dequeues its notification. The
                // base's WAL apply does not run, so its applied head stays one commit behind.
                setCurrentMicros(firstFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS / 2);
                execute(EIGHTH_ROW_COMMIT);
                final long baseSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                Assert.assertEquals(baseSeqTxn - 1, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
                fault.arm(0);
                job1.run();
                Assert.assertFalse("a notification must not re-drive the view before its deadline", fault.hasFired());
                Assert.assertEquals(durableSeqTxn, instance.getLastProcessedSeqTxn());
                Assert.assertEquals("the notification's target stays owed", baseSeqTxn, instance.getRefreshRetryDeferredSeqTxn());
                Assert.assertFalse("the scan finds the view backing off: no work", job0.run());
                Assert.assertFalse("the scan finds the view backing off: no work", job1.run());

                // One microsecond short of the deadline still holds.
                setCurrentMicros(notBeforeUs - 1);
                Assert.assertFalse(job0.run());
                Assert.assertFalse(job1.run());
                Assert.assertFalse(fault.hasFired());
                Assert.assertEquals(durableSeqTxn, instance.getLastProcessedSeqTxn());

                // At the deadline the fallback scan re-drives the view, with no notification left.
                fault.disarm();
                setCurrentMicros(notBeforeUs);
                final boolean didWork0 = job0.run();
                final boolean didWork1 = job1.run();
                Assert.assertTrue("the owning worker's scan must re-drive the view at its deadline", didWork0 || didWork1);
                Assert.assertEquals(
                        "the scan must drive the view as far as the held-back notification asked",
                        baseSeqTxn,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertEquals(baseSeqTxn - 1, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
                Assert.assertEquals("the success ends the backoff", Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
                Assert.assertEquals(
                        "the drive that served the held-back target retires it",
                        Numbers.LONG_NULL,
                        instance.getRefreshRetryDeferredSeqTxn()
                );
            }
            Assert.assertFalse(instance.isInvalid());
            drainWalQueue();
            assertViewRows(SEVEN_ROWS_OUTPUT + EIGHTH_ROW_OUTPUT);
        });
    }

    @Test
    public void testAHeldBackTargetPastAnUnappliedMatViewRebuildLetsTheJobGoQuiet() throws Exception {
        // A drive to the held-back target can also stop short of it without faulting and without
        // deferring on the base's apply lag. Over a materialized view, the raw-WAL drain stops at the
        // TRUNCATE a full refresh commits, and only the mat view's own apply, which invalidates the
        // view when it reaches that TRUNCATE, ends the stop. A target at or above the TRUNCATE sent
        // every later scan pass into the same stop, and the job reported work on each one for as
        // long as the mat view's apply stayed behind - with its WAL suspended, indefinitely.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW tx_hourly AS ("
                    + "SELECT created_at, account_id, sum(amount) AS total FROM tx SAMPLE BY 1h"
                    + ") PARTITION BY DAY");
            drainWalAndMatViewQueues(engine);
            final TableToken mvToken = engine.verifyTableName("tx_hourly");
            // The view drains the mat view's WAL, so the fault fails a read of that WAL.
            fault.of(mvToken.getDirName());
            // Over the still-empty mat view, so the view consumes its commits incrementally.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT created_at, account_id, sum(total) OVER ("
                    + "PARTITION BY account_id ORDER BY created_at ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
                    + ") AS moving_total FROM tx_hourly");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            final String fourBucketsOutput = """
                    created_at\taccount_id\tmoving_total
                    2026-01-01T00:00:00.000000Z\tacct-1\t1.0
                    2026-01-01T01:00:00.000000Z\tacct-1\t3.0
                    2026-01-01T02:00:00.000000Z\tacct-1\t6.0
                    2026-01-01T03:00:00.000000Z\tacct-1\t10.0
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO tx (created_at, account_id, amount) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'acct-1', 1.0),
                        ('2026-01-01T01:00:00.000000Z', 'acct-1', 2.0),
                        ('2026-01-01T02:00:00.000000Z', 'acct-1', 3.0)
                        """);
                drainWalAndMatViewQueues(engine);
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");

                // The mat view commits and applies one more bucket, and the turn its notification
                // drives fails the drain's first read of the mat view's WAL.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-01T03:00:00.000000Z', 'acct-1', 4.0)");
                drainWalAndMatViewQueues(engine);
                final long firstFaultUs = currentMicros;
                fault.arm(0);
                Assert.assertTrue("the faulting turn is work", job.run());
                Assert.assertTrue("the drain's first read must have been failed", fault.hasFired());
                fault.disarm();
                final long notBeforeUs = instance.getRefreshRetryNotBeforeUs();
                Assert.assertEquals(firstFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, notBeforeUs);

                // During the backoff the mat view is rebuilt over a truncated base. Only the mat
                // view's refresh runs, so its WAL holds the TRUNCATE and the rebuilt rows, and its
                // apply stays behind them from here on. The worker consumes the notification, whose
                // target stays owed.
                execute("TRUNCATE TABLE tx");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-01T05:00:00.000000Z', 'acct-1', 100.0)");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW tx_hourly FULL");
                drainMatViewQueue(engine);
                final long mvSeqTxn = engine.getTableSequencerAPI().lastTxn(mvToken);
                final long mvAppliedSeqTxn = engine.getTableSequencerAPI().getTxnTracker(mvToken).getWriterTxn();
                Assert.assertTrue("the rebuild must be committed and not applied", mvAppliedSeqTxn < mvSeqTxn);
                job.run();
                Assert.assertEquals("the notification's target stays owed", mvSeqTxn, instance.getRefreshRetryDeferredSeqTxn());

                // The fault has cleared. From the deadline on, the job passes once per millisecond of
                // the simulated clock, while the mat view's apply stays behind the TRUNCATE.
                int workingRuns = 0;
                for (long nowUs = notBeforeUs; nowUs < notBeforeUs + STALLED_APPLY_MICROS; nowUs += WORKER_TICK_MICROS) {
                    setCurrentMicros(nowUs);
                    if (job.run()) {
                        workingRuns++;
                    }
                }
                Assert.assertEquals(mvAppliedSeqTxn, engine.getTableSequencerAPI().getTxnTracker(mvToken).getWriterTxn());
                Assert.assertFalse(instance.isInvalid());
                // The view serves every bucket the mat view committed below the TRUNCATE.
                assertQuery("SELECT created_at, account_id, moving_total FROM lv")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .expectSize()
                        .returns(fourBucketsOutput);
                // One drive that serves the bucket below the TRUNCATE and stops there, and one that
                // gets no further and retires the target. Nothing else is work.
                Assert.assertEquals(
                        "the job must go quiet once a drive stops short of the held-back target [workingRuns=" + workingRuns + ']',
                        2,
                        workingRuns
                );
                Assert.assertEquals(
                        "a drive that got no further retired the held-back target",
                        Numbers.LONG_NULL,
                        instance.getRefreshRetryDeferredSeqTxn()
                );
                Assert.assertEquals("no turn faulted after the fault cleared", 1, instance.getRefreshFaultCount());

                // The mat view's apply reaches the TRUNCATE, which invalidates the view.
                drainWalQueue();
                Assert.assertTrue(instance.isInvalid());
                Assert.assertFalse("an invalid view is no work", job.run());
            }
            drainWalQueue();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status\tinvalidation_reason
                            lv\tinvalid\tbase materialized view was rebuilt
                            """);
            assertQuery("SELECT created_at, account_id, moving_total FROM lv")
                    .noLeakCheck()
                    .timestamp("created_at")
                    .expectSize()
                    .returns(fourBucketsOutput);
        });
    }

    @Test
    public void testAHeldBackTargetPastAnUnappliedRetypeLetsTheJobGoQuiet() throws Exception {
        // Another drive that stops short of the held-back target without faulting and without
        // deferring: the target lies past a commit written after a referenced base column was
        // retyped, and the base has applied neither. The drain meets the drifted segment, and the
        // recompile-and-recover path answers it without arming the backoff; the recompile adopts
        // the applied metadata, which does not carry the retype yet, so a drive to the target meets
        // the same drift again. A target that outlived that drive sent every later scan pass into
        // the same recovery, reporting work each time, and kept the view from serving even the
        // commit the base had applied.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance instance = createViewOverFourRows(fault);
            final TableToken baseToken = engine.verifyTableName("tx");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                final long appliedSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                final long firstFaultUs = currentMicros;
                fault.arm(0);
                Assert.assertTrue("the faulting turn is work", job.run());
                Assert.assertTrue("the drain's first read must have been failed", fault.hasFired());
                fault.disarm();
                final long notBeforeUs = instance.getRefreshRetryNotBeforeUs();
                Assert.assertEquals(firstFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, notBeforeUs);

                // During the backoff the column the view sums is retyped, and a row is written after
                // the retype. The base's WAL apply stays behind both from here on. The worker
                // consumes the notification, whose target stays owed.
                execute("ALTER TABLE tx ALTER COLUMN amount TYPE FLOAT");
                execute(EIGHTH_ROW_COMMIT);
                final long baseSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                Assert.assertEquals(appliedSeqTxn + 2, baseSeqTxn);
                job.run();
                Assert.assertEquals("the notification's target stays owed", baseSeqTxn, instance.getRefreshRetryDeferredSeqTxn());

                // The fault has cleared. From the deadline on, the job passes once per millisecond of
                // the simulated clock, while the base's apply stays behind the retype.
                int workingRuns = 0;
                for (long nowUs = notBeforeUs; nowUs < notBeforeUs + STALLED_APPLY_MICROS; nowUs += WORKER_TICK_MICROS) {
                    setCurrentMicros(nowUs);
                    if (job.run()) {
                        workingRuns++;
                    }
                }
                Assert.assertEquals(appliedSeqTxn, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals(
                        "the view must serve every commit the base has applied [workingRuns=" + workingRuns + ']',
                        appliedSeqTxn,
                        instance.getLastProcessedSeqTxn()
                );
                // One drive to the held-back target, which meets the drift and gets no further, so it
                // retires the target, and one drive to the base's applied head. Nothing else is work.
                Assert.assertEquals("the job must go quiet once the view serves the applied head", 2, workingRuns);
                Assert.assertEquals(
                        "a drive that got no further retired the held-back target",
                        Numbers.LONG_NULL,
                        instance.getRefreshRetryDeferredSeqTxn()
                );
                Assert.assertEquals("the drive to the applied head zeroes the streak", 0, instance.getFlushRetryCount());

                // The base's apply lands the retype, which invalidates the view.
                drainWalQueue();
                Assert.assertTrue(instance.isInvalid());
                Assert.assertFalse("an invalid view is no work", job.run());
            }
            drainWalQueue();
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testAMidDrainFaultTheRecoveryAnswersIsPacedByTheSameBackoff() throws Exception {
        // A fault that strikes after the turn fed a row leaves the view's accumulators ahead of its
        // durable output, and the recovery restores them from the checkpoint timeline. Such a fault
        // is charged to the duration budget alone, and those turns shared the unpaced retry: each one
        // re-ran the restore on the worker's very next pass, for as long as the 60s budget lasted. So
        // the backoff paces them as well. The fault outlasts the time the count budget lasts under the
        // schedule, which shows the count takes no part.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance instance = createViewOverFourRows(fault);
            final TableToken baseToken = engine.verifyTableName("tx");
            final long durableSeqTxn = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // As in LiveViewRuntimeRestoreTest: the first commit gets a refresh task of its own, the
                // next two coalesce behind it and drain in one pass, and the clock stays on the view's
                // last flush, so the first commit is an un-flushed lead when the read of the third fails.
                final long firstFaultUs = instance.getLastFlushTimeUs();
                setCurrentMicros(firstFaultUs);
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                drainWalQueue();
                execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
                drainWalQueue();
                final long baseSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                final LongList faultingTurnsUs = new LongList();
                fault.arm(2);
                job.run();
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertEquals("the recovery must have restored the runtime", 1, instance.getCheckpointRuntimeRestores());
                faultingTurnsUs.add(firstFaultUs);
                // Every turn after it re-drains the three commits from the durable output, feeds the
                // first and fails the read of the second.
                long nowUs = firstFaultUs + WORKER_TICK_MICROS;
                for (; nowUs < firstFaultUs + RECOVERED_FAULT_MICROS; nowUs += WORKER_TICK_MICROS) {
                    tick(job, fault, 1, nowUs, faultingTurnsUs);
                    Assert.assertFalse(instance.isInvalid());
                    Assert.assertEquals("a recovered fault leaves the count alone", 0, instance.getFlushRetryCount());
                    Assert.assertEquals("the restore puts the view back where it stood", durableSeqTxn, instance.getLastProcessedSeqTxn());
                }
                // 0, 100, 300, 700 and 1500ms: one more faulting turn than the count budget allows.
                assertFaultingTurns(faultingTurnsUs, firstFaultUs, 5);
                Assert.assertEquals("every faulting turn restored", 5, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals("the first recovered fault starts the duration clock", firstFaultUs, instance.getFlushRetryStartUs());

                fault.disarm();
                final long dueUs = firstFaultUs + refreshRetryStreakMicros(6);
                long recoveredAtUs = Numbers.LONG_NULL;
                for (; nowUs < dueUs + REFRESH_RETRY_BACKOFF_MAX_MICROS; nowUs += WORKER_TICK_MICROS) {
                    tick(job, fault, -1, nowUs, faultingTurnsUs);
                    if (instance.getLastProcessedSeqTxn() == baseSeqTxn) {
                        recoveredAtUs = nowUs;
                        break;
                    }
                }
                Assert.assertEquals("the view must be retried at its deadline, not before and not later", dueUs - firstFaultUs, recoveredAtUs - firstFaultUs);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
            }
            Assert.assertFalse(instance.isInvalid());
            drainWalQueue();
            assertViewRows(SEVEN_ROWS_OUTPUT);
            capture.drain();
            // One log line per faulting turn; the fifth reports the 1.5s the duration budget measured.
            capture.assertLogged("live view refresh failed, window state recovered, retrying [view=lv, retryCount=0, elapsedUs=1500000, ");
            capture.assertNotLogged("elapsedUs=1500001");
        });
    }

    @Test
    public void testAPreFeedFaultThatNeverClearsStillExhaustsTheCountBudget() throws Exception {
        // The backoff paces the retries; it must not stretch them into an endless wait. The count
        // budget still counts every faulting turn, so a fault that never clears invalidates the
        // view on its fifth turn - 1.5s after the first under the default budget, not 4ms. A commit
        // that lands during the first wait, with the base's apply stalled, leaves a held-back
        // target: every faulting turn keeps it owed to the next deadline, and the invalidation
        // retires it.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance instance = createViewOverFourRows(fault);
            final TableToken baseToken = engine.verifyTableName("tx");
            final int maxRetry = engine.getConfiguration().getLiveViewFlushRetryMax();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                final long firstFaultUs = currentMicros;
                final LongList faultingTurnsUs = new LongList();
                tick(job, fault, 0, firstFaultUs, faultingTurnsUs);
                Assert.assertEquals(1, faultingTurnsUs.size());

                setCurrentMicros(firstFaultUs + WORKER_TICK_MICROS);
                execute(EIGHTH_ROW_COMMIT);
                Assert.assertTrue("the worker consumes the notification", job.run());
                final long targetSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                Assert.assertEquals(targetSeqTxn, instance.getRefreshRetryDeferredSeqTxn());

                final long horizonUs = firstFaultUs + refreshRetryStreakMicros(maxRetry) + REFRESH_RETRY_BACKOFF_MAX_MICROS;
                for (long nowUs = firstFaultUs + 2 * WORKER_TICK_MICROS; nowUs < horizonUs && !instance.isInvalid(); nowUs += WORKER_TICK_MICROS) {
                    tick(job, fault, 0, nowUs, faultingTurnsUs);
                    if (!instance.isInvalid()) {
                        Assert.assertEquals("a faulting turn keeps the target owed", targetSeqTxn, instance.getRefreshRetryDeferredSeqTxn());
                    }
                }
                Assert.assertTrue("a fault that never clears must invalidate the view", instance.isInvalid());
                assertFaultingTurns(faultingTurnsUs, firstFaultUs, maxRetry);
            }
            Assert.assertEquals("flush retry budget exhausted", instance.getInvalidationReason());
            Assert.assertEquals("an invalid view waits for nothing", Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
            Assert.assertEquals("an invalid view is owed nothing", Numbers.LONG_NULL, instance.getRefreshRetryDeferredSeqTxn());
            drainWalQueue();
            assertViewRows(FOUR_ROWS_OUTPUT);
        });
    }

    @Test
    public void testARecreatedViewDoesNotInheritTheDroppedViewsBackoff() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance dropped = createViewOverFourRows(fault);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                final long faultUs = currentMicros;
                fault.arm(0);
                job.run();
                Assert.assertTrue(fault.hasFired());
                Assert.assertEquals(faultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, dropped.getRefreshRetryNotBeforeUs());
                fault.disarm();
                // A commit lands during the backoff, and its notification leaves a target owed.
                execute(EIGHTH_ROW_COMMIT);
                Assert.assertTrue("the worker consumes the notification", job.run());
                Assert.assertEquals(
                        engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tx")),
                        dropped.getRefreshRetryDeferredSeqTxn()
                );
                drainWalQueue();

                execute("DROP LIVE VIEW lv");
                Assert.assertEquals("a dropped view waits for nothing", Numbers.LONG_NULL, dropped.getRefreshRetryNotBeforeUs());
                Assert.assertEquals("a dropped view is owed nothing", Numbers.LONG_NULL, dropped.getRefreshRetryDeferredSeqTxn());
                execute(CREATE_VIEW_SQL);
                final LiveViewInstance recreated = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(recreated);
                Assert.assertNotSame(dropped, recreated);
                Assert.assertEquals(Numbers.LONG_NULL, recreated.getRefreshRetryNotBeforeUs());
                Assert.assertEquals(Numbers.LONG_NULL, recreated.getRefreshRetryDeferredSeqTxn());
                // Still inside the dropped view's backoff, on a clock nothing advances: the new view
                // seeds straight away.
                driveSeedToCompletion(job, "lv");
                Assert.assertEquals(faultUs, currentMicros);
                driveRefreshToQuiescence(job);
                Assert.assertFalse(recreated.isInvalid());
                Assert.assertEquals(0, recreated.getRefreshFaultCount());
            }
            assertViewRows(SEVEN_ROWS_OUTPUT + EIGHTH_ROW_OUTPUT);
        });
    }

    @Test
    public void testARestartedViewDoesNotInheritTheBackoff() throws Exception {
        // The backoff is runtime state, like the flush-retry streak it paces: a restart rebuilds
        // the view's instance, and the view refreshes straight away.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance before = createViewOverFourRows(fault);
            final TableToken baseToken = engine.verifyTableName("tx");
            final long faultUs;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                faultUs = currentMicros;
                fault.arm(0);
                job.run();
                Assert.assertTrue(fault.hasFired());
                Assert.assertEquals(faultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, before.getRefreshRetryNotBeforeUs());
            }
            fault.disarm();

            engine.getLiveViewRegistry().clear();
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseInactive();
            engine.buildViewGraphs();
            final LiveViewInstance after = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(after);
            Assert.assertNotSame(before, after);
            Assert.assertEquals(Numbers.LONG_NULL, after.getRefreshRetryNotBeforeUs());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Still inside the backoff the previous instance armed, on a clock nothing advances.
                drainJob(job);
                Assert.assertEquals(faultUs, currentMicros);
                Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(baseToken), after.getLastProcessedSeqTxn());
            }
            Assert.assertFalse(after.isInvalid());
            drainWalQueue();
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testASuccessfulTurnResetsTheBackoffAndAHealthyViewIsNeverHeldBack() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance instance = createViewOverFourRows(fault);
            // A second view, over a base the fault does not touch.
            execute("CREATE TABLE pay (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute(CREATE_VIEW_SQL.replace("lv", "pay_lv").replace("FROM tx", "FROM pay"));
            final TableToken baseToken = engine.verifyTableName("tx");
            final TableToken healthyBaseToken = engine.verifyTableName("pay");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "pay_lv");
                driveRefreshToQuiescence(job);
                final LiveViewInstance healthy = engine.getLiveViewRegistry().getViewInstance("pay_lv");
                Assert.assertNotNull(healthy);

                // Two faulting turns in a row build the streak up to a 200ms wait.
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                final long firstFaultUs = currentMicros;
                fault.arm(0);
                job.run();
                Assert.assertTrue(fault.hasFired());
                Assert.assertEquals(firstFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, instance.getRefreshRetryNotBeforeUs());
                setCurrentMicros(firstFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS);
                fault.arm(0);
                job.run();
                Assert.assertTrue(fault.hasFired());
                final long notBeforeUs = instance.getRefreshRetryNotBeforeUs();
                Assert.assertEquals(firstFaultUs + 3 * REFRESH_RETRY_BACKOFF_BASE_MICROS, notBeforeUs);
                fault.disarm();

                // The healthy view refreshes on the pass its base commits, while lv backs off.
                setCurrentMicros(firstFaultUs + 2 * REFRESH_RETRY_BACKOFF_BASE_MICROS);
                execute("INSERT INTO pay (created_at, account_id, amount) VALUES ('2026-01-01T09:00:00.000000Z', 'acct-9', 5.0)");
                drainWalQueue();
                Assert.assertTrue(job.run());
                Assert.assertEquals(
                        "a view with no fault must not be held back by another view's backoff",
                        engine.getTableSequencerAPI().lastTxn(healthyBaseToken),
                        healthy.getRefreshedUpToSeqTxn()
                );
                Assert.assertEquals(Numbers.LONG_NULL, healthy.getRefreshRetryNotBeforeUs());
                Assert.assertTrue("lv still backs off", instance.getLastProcessedSeqTxn() < engine.getTableSequencerAPI().lastTxn(baseToken));

                // lv recovers at its deadline, and the success ends the backoff.
                setCurrentMicros(notBeforeUs);
                Assert.assertTrue(job.run());
                Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(baseToken), instance.getLastProcessedSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());

                // The next fault waits the shortest backoff again, not where the last streak left off.
                execute(EIGHTH_ROW_COMMIT);
                drainWalQueue();
                final long nextFaultUs = notBeforeUs + REFRESH_RETRY_BACKOFF_BASE_MICROS;
                setCurrentMicros(nextFaultUs);
                fault.arm(0);
                job.run();
                Assert.assertTrue(fault.hasFired());
                Assert.assertEquals(nextFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, instance.getRefreshRetryNotBeforeUs());
                Assert.assertEquals("a new streak", 1, instance.getFlushRetryCount());
                fault.disarm();
                setCurrentMicros(nextFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS);
                driveRefreshToQuiescence(job);
                Assert.assertFalse(instance.isInvalid());
                Assert.assertFalse(healthy.isInvalid());
            }
            drainWalQueue();
            assertViewRows(SEVEN_ROWS_OUTPUT + EIGHTH_ROW_OUTPUT);
        });
    }

    @Test
    public void testATransientPreFeedFaultIsPacedByBackoffAndTheViewConverges() throws Exception {
        assertATransientPreFeedFaultIsPacedAndTheViewConverges("");
    }

    @Test
    public void testATransientPreFeedFaultOverADedupBaseIsPacedByBackoffAndTheViewConverges() throws Exception {
        // A deduplicating base runs the coupled cycle, which FLUSH EVERY gates; a turn that faulted
        // never stamps the flush time, so only the backoff paces the retries there too. The commits
        // hold no duplicate, so the drain takes the raw-WAL path and meets the same fault.
        assertATransientPreFeedFaultIsPacedAndTheViewConverges(DEDUP_UPSERT_KEYS);
    }

    @Test
    public void testAWallClockStepBackDoesNotHoldTheViewForTheStep() throws Exception {
        // The backoff deadline is wall-clock time, the same clock the duration budget measures. A
        // clock that steps back after a fault must not hold the view for the length of the step.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        setCurrentMicros(3_600_000_000L);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance instance = createViewOverFourRows(fault);
            final TableToken baseToken = engine.verifyTableName("tx");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                final long faultUs = currentMicros;
                fault.arm(0);
                job.run();
                Assert.assertTrue(fault.hasFired());
                Assert.assertEquals(faultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, instance.getRefreshRetryNotBeforeUs());
                fault.disarm();

                // A step back no longer than the longest wait the backoff arms is indistinguishable
                // from a wait that is still running, and holds the view.
                setCurrentMicros(faultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS - REFRESH_RETRY_BACKOFF_MAX_MICROS);
                Assert.assertFalse(job.run());
                Assert.assertTrue(instance.getRefreshedUpToSeqTxn() < engine.getTableSequencerAPI().lastTxn(baseToken));
                // A longer one cannot be a wait the backoff armed: the view is due, and the pass
                // drains the commits into its lead. FLUSH EVERY measures the flush against the same
                // stepped-back clock, so the flush itself waits for the clock to catch up.
                setCurrentMicros(faultUs - 60_000_000L);
                Assert.assertTrue(job.run());
                Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(baseToken), instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
                setCurrentMicros(faultUs);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(baseToken), instance.getLastProcessedSeqTxn());
            }
            Assert.assertFalse(instance.isInvalid());
            drainWalQueue();
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    private static void assertFaultingTurns(LongList faultingTurnsUs, long firstFaultUs, int expectedTurns) {
        final StringBuilder expected = new StringBuilder();
        final StringBuilder actual = new StringBuilder();
        for (int i = 0; i < expectedTurns; i++) {
            expected.append(refreshRetryStreakMicros(i + 1)).append(' ');
        }
        for (int i = 0, n = faultingTurnsUs.size(); i < n; i++) {
            actual.append(faultingTurnsUs.getQuick(i) - firstFaultUs).append(' ');
        }
        Assert.assertEquals(
                "each faulting turn must wait out its backoff exactly: no earlier, and no later than the next worker tick",
                expected.toString(),
                actual.toString()
        );
    }

    private static LiveViewRefreshJob newRefreshJob(int workerId, int workerCount, boolean isReplica) {
        if (!isReplica) {
            return new LiveViewRefreshJob(workerId, workerCount, engine, 1);
        }
        return new LiveViewRefreshJob(workerId, workerCount, engine, 1) {
            @Override
            protected boolean prefersAppliedBaseRefresh() {
                return true;
            }
        };
    }

    /**
     * A view that reads the applied base, with the base's WAL apply stalled one commit behind a
     * notification the backoff held back. The applied-base drain waits until the base has applied
     * everything it was asked to reach, so the held-back target is owed one drive, as the
     * notification itself was: the base has not applied that far, the drive defers on the apply
     * lag, and from then on the view follows the base's applied head. A held-back target that
     * outlived that deferral kept every later pass asking for the unapplied commit: the view stayed
     * behind commits the base had already applied, and the job reported work each time the
     * apply-lag back-off ran out, for as long as the apply stayed behind.
     *
     * @param isReplica false for a deduplicating base on the primary's refresh job, true for a
     *                  base without dedup keys under a refresh job that prefers the applied base
     */
    private void assertAHeldBackTargetDoesNotHoldAnAppliedBaseViewBehindAStalledApply(boolean isReplica) throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance instance = createViewOverFourRows(fault, isReplica ? "" : DEDUP_UPSERT_KEYS);
            final TableToken baseToken = engine.verifyTableName("tx");
            final long durableSeqTxn = instance.getLastProcessedSeqTxn();
            try (
                    LiveViewRefreshJob job0 = newRefreshJob(0, 2, isReplica);
                    LiveViewRefreshJob job1 = newRefreshJob(1, 2, isReplica)
            ) {
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                final long appliedSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                final long firstFaultUs = currentMicros;
                if (isReplica) {
                    // The applied-base drain reads the base table, not its WAL, so fail the first
                    // column file of the base its reader opens. Releasing the pooled readers makes
                    // the drain open a fresh one.
                    engine.releaseAllReaders();
                    fault.armAppliedScan();
                } else {
                    // The commits hold no duplicate, so this drain takes the raw-WAL path.
                    fault.arm(0);
                }
                Assert.assertTrue("the faulting turn is work", job0.run());
                Assert.assertTrue(
                        "the drain's first read must have been failed",
                        isReplica ? fault.hasAppliedScanFired() : fault.hasFired()
                );
                final long notBeforeUs = instance.getRefreshRetryNotBeforeUs();
                Assert.assertEquals(firstFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS, notBeforeUs);
                fault.disarm();

                // A commit lands mid-backoff, and the other worker consumes its notification. The
                // base's WAL apply stays stalled from here on, so the base never applies it.
                setCurrentMicros(firstFaultUs + REFRESH_RETRY_BACKOFF_BASE_MICROS / 2);
                execute(EIGHTH_ROW_COMMIT);
                final long baseSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                Assert.assertEquals(appliedSeqTxn + 1, baseSeqTxn);
                job1.run();
                Assert.assertEquals(durableSeqTxn, instance.getLastProcessedSeqTxn());
                Assert.assertEquals("the notification's target stays owed", baseSeqTxn, instance.getRefreshRetryDeferredSeqTxn());

                // The fault has cleared. From the deadline on, both workers pass once per millisecond
                // of the simulated clock, while the base's apply stays one commit behind.
                int workingRuns = 0;
                for (long nowUs = notBeforeUs; nowUs < notBeforeUs + STALLED_APPLY_MICROS; nowUs += WORKER_TICK_MICROS) {
                    setCurrentMicros(nowUs);
                    if (job0.run()) {
                        workingRuns++;
                    }
                    if (job1.run()) {
                        workingRuns++;
                    }
                }
                Assert.assertEquals(appliedSeqTxn, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
                Assert.assertEquals(
                        "the view must serve every commit the base has applied [workingRuns=" + workingRuns + ']',
                        appliedSeqTxn,
                        instance.getLastProcessedSeqTxn()
                );
                // One drive to the held-back target, which defers on the apply lag, and one drive to
                // the base's applied head once the apply-lag back-off runs out. Nothing else is work.
                Assert.assertEquals("the job must go quiet once the view serves the applied head", 2, workingRuns);
                Assert.assertEquals(
                        "the apply-lag deferral consumed the held-back target",
                        Numbers.LONG_NULL,
                        instance.getRefreshRetryDeferredSeqTxn()
                );
                Assert.assertEquals(Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
                Assert.assertEquals("no turn faulted after the fault cleared", 1, instance.getRefreshFaultCount());
            }
            Assert.assertFalse(instance.isInvalid());
            // The apply resumes, and the view catches up with the last commit.
            try (LiveViewRefreshJob job = newRefreshJob(0, 1, isReplica)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(baseToken), instance.getLastProcessedSeqTxn());
            drainWalQueue();
            assertViewRows(SEVEN_ROWS_OUTPUT + EIGHTH_ROW_OUTPUT);
        });
    }

    /**
     * The fault fails the first base WAL column the drain opens, before the turn feeds a row, so
     * each faulting turn is charged to the count budget. It holds for a full second and then clears.
     */
    private void assertATransientPreFeedFaultIsPacedAndTheViewConverges(String dedupClause) throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(fault.facade(), () -> {
            final LiveViewInstance instance = createViewOverFourRows(fault, dedupClause);
            final TableToken baseToken = engine.verifyTableName("tx");
            final long durableSeqTxn = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute(THREE_ROWS_COMMIT);
                drainWalQueue();
                final long baseSeqTxn = engine.getTableSequencerAPI().lastTxn(baseToken);
                final long firstFaultUs = currentMicros;
                final long faultClearsUs = firstFaultUs + TRANSIENT_FAULT_MICROS;
                final LongList faultingTurnsUs = new LongList();
                long nowUs = firstFaultUs;
                for (; nowUs < faultClearsUs; nowUs += WORKER_TICK_MICROS) {
                    tick(job, fault, 0, nowUs, faultingTurnsUs);
                    Assert.assertFalse(
                            "a transient pre-feed fault must not invalidate the view [faultingTurns=" + faultingTurnsUs.size()
                                    + ", elapsedMicros=" + (nowUs - firstFaultUs) + ", reason=" + instance.getInvalidationReason() + ']',
                            instance.isInvalid()
                    );
                    Assert.assertEquals("no faulting turn may move the view", durableSeqTxn, instance.getLastProcessedSeqTxn());
                }
                // Four faulting turns inside the second: 0, 100, 300 and 700ms.
                assertFaultingTurns(faultingTurnsUs, firstFaultUs, 4);

                // The fault has cleared. Nothing new commits, so only the fallback scan can pick
                // the view back up, and it must do so at the deadline the fourth fault armed.
                fault.disarm();
                final long dueUs = firstFaultUs + refreshRetryStreakMicros(5);
                long recoveredAtUs = Numbers.LONG_NULL;
                for (; nowUs < dueUs + REFRESH_RETRY_BACKOFF_MAX_MICROS; nowUs += WORKER_TICK_MICROS) {
                    tick(job, fault, -1, nowUs, faultingTurnsUs);
                    if (instance.getLastProcessedSeqTxn() == baseSeqTxn) {
                        recoveredAtUs = nowUs;
                        break;
                    }
                }
                Assert.assertEquals("the view must converge once the fault clears", baseSeqTxn, instance.getLastProcessedSeqTxn());
                Assert.assertEquals("the view must be retried at its deadline, not before and not later", dueUs - firstFaultUs, recoveredAtUs - firstFaultUs);
                Assert.assertEquals("the retry that got past the fault zeroes the streak", 0, instance.getFlushRetryCount());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
            }
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals(4, instance.getRefreshFaultCount());
            drainWalQueue();
            assertViewRows(SEVEN_ROWS_OUTPUT);
            capture.drain();
            capture.assertNotLogged("live view refresh budget exhausted");
        });
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery(VIEW_ROWS_QUERY)
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
    }

    private LiveViewInstance createViewOverFourRows(LiveViewMidDrainFault fault) throws Exception {
        return createViewOverFourRows(fault, "");
    }

    private LiveViewInstance createViewOverFourRows(LiveViewMidDrainFault fault, String dedupClause) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL " + dedupClause);
        execute(CREATE_VIEW_SQL);
        fault.of(engine.verifyTableName("tx").getDirName());
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (String values : FOUR_ROWS) {
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + values);
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        }
        assertNoRefreshFaults("lv");
        drainWalQueue();
        assertViewRows(FOUR_ROWS_OUTPUT);
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        return instance;
    }

    /**
     * One idle-worker pass at {@code nowUs}. With {@code faultSkip} of zero or more, the pass fails
     * the base WAL column open that follows {@code faultSkip} others: zero fails the drain's first
     * read, before it feeds a row. Records the pass when it faulted, and otherwise requires it to
     * have been idle, unless it is the pass that recovers the view: a view waiting out its backoff
     * is no work, so the worker naps instead of spinning.
     */
    private void tick(LiveViewRefreshJob job, LiveViewMidDrainFault fault, int faultSkip, long nowUs, LongList faultingTurnsUs) {
        setCurrentMicros(nowUs);
        if (faultSkip >= 0) {
            fault.arm(faultSkip);
        }
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        final long processedBefore = instance.getLastProcessedSeqTxn();
        final boolean didWork = job.run();
        if (faultSkip >= 0 && fault.hasFired()) {
            faultingTurnsUs.add(nowUs);
            return;
        }
        if (instance.getLastProcessedSeqTxn() == processedBefore) {
            Assert.assertFalse("a view waiting out its backoff must not report work [nowUs=" + nowUs + ']', didWork);
        }
    }
}
