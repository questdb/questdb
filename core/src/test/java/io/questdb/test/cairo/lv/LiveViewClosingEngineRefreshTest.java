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
import io.questdb.cairo.lv.LiveViewCheckpointRepairSession;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A refresh worker starts no live view turn while the engine closes. ServerMain raises the engine's
 * closing flag (signalClose) before the worker pools halt, and from then on every check of a view's
 * refresh breaker trips: a turn would start a drain, a seed sweep, a repair or a recovery only for the
 * breaker to cancel it, and a cancelled turn reports work without arming the refresh-retry back-off, so
 * the workers would drive the view again on every pass. A cancelled drain can also leave rows it fed in
 * the accumulators, ahead of the lead and of the durable output, and a later turn that flushed the lead
 * would seal them as the root of a commit they have run past. The restart recovers each view from what
 * is durable instead.
 * <p>
 * The flag is engine.setClosing(true), what signalClose() sets. It goes up ahead of a pass, inside the
 * turn in flight, or between a turn's read of it ahead of the refresh latch and its taking the latch. A
 * burst is {@link #BURST_RUNS} job.run() calls. A test whose cancelled turn fed rows lowers the flag and
 * restarts the views, as a process does, rather than drive the same instance on.
 */
public class LiveViewClosingEngineRefreshTest extends AbstractLiveViewTxTest {
    private static final int BURST_RUNS = 64;
    // c1: one row, the commit whose task a run dequeues; c2: four rows; c3: six rows.
    private static final String C1 = "('2026-01-02T09:00:00.000000Z', 'acct-1', 16.0)";
    private static final String C2 = "('2026-01-02T09:10:00.000000Z', 'acct-1', 32.0), ('2026-01-02T09:11:00.000000Z', 'acct-2', 64.0), "
            + "('2026-01-02T09:12:00.000000Z', 'acct-1', 128.0), ('2026-01-02T09:13:00.000000Z', 'acct-2', 256.0)";
    private static final String C3 = "('2026-01-02T09:20:00.000000Z', 'acct-1', 512.0), ('2026-01-02T09:21:00.000000Z', 'acct-2', 1024.0), "
            + "('2026-01-02T09:22:00.000000Z', 'acct-1', 2048.0), ('2026-01-02T09:23:00.000000Z', 'acct-2', 4096.0), "
            + "('2026-01-02T09:24:00.000000Z', 'acct-1', 8192.0), ('2026-01-02T09:25:00.000000Z', 'acct-2', 16384.0)";
    // What handleRefreshFailure logs when the breaker cancels the turn itself, with any dropped and invalid flags.
    private static final String CANCELLED_LINE = "live view refresh cancelled [view=lv";
    private static final String DEDUP_CLAUSE = "DEDUP UPSERT KEYS(created_at, account_id)";
    private static final String RECOVERY_CANCELLED_LINE = "live view refresh cancelled during recovery [view=lv, dropped=false, invalid=false]";
    private static final String REDERIVE_CANCELLED_LINE = "live view re-derive from the applied base cancelled [view=lv, dropped=false, invalid=false]";
    private static final String RESTORE_DECLINED_LINE = "live view cannot restore its runtime from the checkpoint timeline";
    // Long enough for a thread to start and reach its first statement on a loaded CI host; only a broken
    // handshake ever waits it out.
    private static final long THREAD_WAIT_SECONDS = 60;
    private static final LogCapture capture = new LogCapture();

    @Before
    public void setUpClosingEngine() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @After
    public void tearDownClosingEngine() {
        engine.setClosing(false);
        capture.stop();
        setCurrentMicros(-1);
    }

    @Test
    public void testALeadDueDuringTheShutdownIsRedrainedAfterTheRestart() throws Exception {
        // The one turn the shutdown stops that would complete: a lead drained before the flag rose whose FLUSH
        // EVERY falls due while the engine closes. It stays in RAM, the base WAL above the durable watermark
        // survives a purge, and the restart drains the lead's commit once.
        configureWalPurge();
        assertMemoryLeak(() -> {
            createFixture("");
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewTestRefreshHooks hooks = hooks(job);
                // Hold the clock at the last flush: the drain lands in the lead, and no flush is due.
                setCurrentMicros(instance.getLastFlushTimeUs());
                insert(C1);
                drainJob(job);
                Assert.assertEquals(1, instance.getLeadRowCount());
                Assert.assertEquals(4, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(5, instance.getRefreshedUpToSeqTxn());
                final long lvSeqTxn = lvSeqTxn();
                final LongList ladder = snapshotCheckpointLadder(instance);
                Assert.assertEquals("the fixture seals a root per commit", 4, ladder.size() / 2);
                final int latchAttempts = hooks.getRefreshLatchAttempts();

                engine.setClosing(true);
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                Assert.assertEquals(0, burst(job));
                Assert.assertEquals(latchAttempts, hooks.getRefreshLatchAttempts());
                Assert.assertEquals(4, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(5, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals(1, instance.getLeadRowCount());
                Assert.assertEquals(lvSeqTxn, lvSeqTxn());
                Assert.assertEquals(ladder, snapshotCheckpointLadder(instance));
            }
            engine.setClosing(false);
            // One segment a commit. The view's durable watermark 4 holds the segment of commit 5, and the older
            // of the timeline's two durable slots, the root of commit 3, the segment of commit 4.
            Assert.assertEquals("the purge must keep only the segments above the floor", 5 - 3, purgeBaseWalAndCountSegments("tx"));
            restartLiveViewsAndRecover();
            assertQuery("SELECT count() FROM lv WHERE created_at = '2026-01-02T09:00:00.000000Z'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
            assertRows("2026-01-02T09:00:00.000000Z", "2026-01-02T09:00:00.000000Z\tacct-1\t16.0\t1\n");
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAPeerTurnTheShutdownCancelledLeavesNothingToFlush() throws Exception {
        // Two refresh workers over one view. This worker dequeues c1's task and reads the flag down ahead of the
        // refresh latch. Before it takes the latch, its peer drains c1 into the lead, then drains c2 as FLUSH
        // EVERY falls due, and the flag rises as the drain opens c3: the breaker cancels the peer's turn with
        // c2 fed and releases the latch. This worker's turn, at c1's seqTxn, would skip the drain, flush the
        // lead and seal the accumulators holding c2 as the root of c1, which the restart would replay c2 over.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (
                    LiveViewRefreshJob peer = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)
            ) {
                final LiveViewTestRefreshHooks hooks = hooks(job);
                setCurrentMicros(instance.getLastFlushTimeUs());
                insert(C1);
                final long lvSeqTxn = lvSeqTxn();
                final LongList ladder = snapshotCheckpointLadder(instance);
                Assert.assertEquals(4, ladder.size() / 2);
                hooks.setSimulatePeerTurnBeforeRefreshLatch(() -> {
                    try {
                        execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + C2);
                        execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + C3);
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    // This job holds c1's task, so the peer's scan drives the view at the base's applied head.
                    Assert.assertTrue(peer.run());
                    Assert.assertEquals(4, instance.getLastProcessedSeqTxn());
                    Assert.assertEquals(5, instance.getRefreshedUpToSeqTxn());
                    Assert.assertEquals(1, instance.getLeadRowCount());
                    drainWalQueue();
                    fault.runAtWalRead(1, () -> engine.setClosing(true));
                    setCurrentMicros(currentMicros + 1_000_000L);
                    Assert.assertTrue(peer.run());
                });
                Assert.assertTrue("the run dequeued c1's task", job.run());
                Assert.assertTrue("the flag must have risen inside the peer's drain", engine.isClosing());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences(CANCELLED_LINE + ", dropped=false, invalid=false]"));
                Assert.assertEquals("the peer's drain fed c2", ts("2026-01-02T09:13:00.000000Z"), instance.getLatestSeenTs());
                Assert.assertEquals("the peer's cancellation is the only fault", 1, instance.getRefreshFaultCount());
                Assert.assertEquals(4, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(5, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals(1, instance.getLeadRowCount());
                Assert.assertEquals(lvSeqTxn, lvSeqTxn());
                Assert.assertEquals("no root may be sealed over the cancelled drain", ladder, snapshotCheckpointLadder(instance));
                Assert.assertEquals(1, hooks.getRefreshLatchAttempts());
            }
            engine.setClosing(false);
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
            assertRows("2026-01-02T09:10:00.000000Z", "2026-01-02T09:10:00.000000Z\tacct-1\t48.0\t2\n");
        });
    }

    @Test
    public void testAShutdownCancelledDrainIsNotSealedByTheNextPass() throws Exception {
        // One worker. Its run drains c1, the task of c1's own commit, and the task the refreshed notification
        // re-enqueues has it drain c2 and meet the flag as it opens c3: the breaker cancels the turn with c2
        // fed. FLUSH EVERY then falls due, and a pass of the scan, at the base's applied head of c1, would
        // flush the lead and seal the accumulators holding c2 as the root of c1.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewTestRefreshHooks hooks = hooks(job);
                setCurrentMicros(instance.getLastFlushTimeUs());
                insert(C1);
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + C2);
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + C3);
                final long lvSeqTxn = lvSeqTxn();
                final LongList ladder = snapshotCheckpointLadder(instance);
                Assert.assertEquals(4, ladder.size() / 2);
                fault.runAtWalRead(2, () -> engine.setClosing(true));
                Assert.assertTrue(job.run());
                Assert.assertTrue("the flag must have risen inside the drain", engine.isClosing());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences(CANCELLED_LINE + ", dropped=false, invalid=false]"));
                Assert.assertEquals("the drain fed c2", ts("2026-01-02T09:13:00.000000Z"), instance.getLatestSeenTs());
                Assert.assertEquals(4, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(5, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals(1, instance.getLeadRowCount());
                Assert.assertEquals("both turns took the latch before the flag rose", 2, hooks.getRefreshLatchAttempts());

                setCurrentMicros(currentMicros + 1_000_000L);
                Assert.assertEquals(0, burst(job));
                Assert.assertEquals(2, hooks.getRefreshLatchAttempts());
                Assert.assertEquals(4, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(1, instance.getLeadRowCount());
                Assert.assertEquals(lvSeqTxn, lvSeqTxn());
                Assert.assertEquals("no root may be sealed over the cancelled drain", ladder, snapshotCheckpointLadder(instance));
            }
            engine.setClosing(false);
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
            assertRows("2026-01-02T09:10:00.000000Z", "2026-01-02T09:10:00.000000Z\tacct-1\t48.0\t2\n");
        });
    }

    @Test
    public void testAShutdownDuringTheResumeFrontierStateFoldRestoresAfterTheRestart() throws Exception {
        // An anchored view over a deduplicating base whose newest day departs in two corrections:
        // the first takes a localized head miss that re-versions the new day's roots from that day's
        // start, and the second resumes from the root at 00:01 and replays nothing, so it derives
        // the state at the view's new frontier 23:58 ahead of its commit, from the root at 23:57 and
        // the base row above it. The shutdown rises as that derivation starts, and the fold's breaker
        // check cancels the repair, which commits nothing. The restart restores the timeline the
        // first correction left, repeats the repair and seals the frontier's state, so the late row
        // at 23:59 adds to the day's running sum.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, sum(x) OVER w s FROM base "
                    + "WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitBase(job, "('2026-01-01T23:57:00.000000Z', 'a', 1.5)");
                commitBase(job, "('2026-01-01T23:58:00.000000Z', 'a', 2.5), ('2026-01-02T00:01:00.000000Z', 'a', 4.5)");
                commitBase(job, "('2026-01-02T00:02:00.000000Z', 'a', 8.5)");
                commitBase(job, "('2026-01-02T00:01:00.000000Z', 'a', -4.5)");
                final LongList ladder = snapshotCheckpointLadder(instance);
                Assert.assertEquals("roots at 23:57, 00:01 and 00:02", 3, ladder.size() / 2);
                final long generationBefore = publishedCheckpointGeneration(instance);
                final long processedBefore = instance.getLastProcessedSeqTxn();
                final long faultsBefore = instance.getRefreshFaultCount();
                final AtomicBoolean isClosing = new AtomicBoolean();
                hooks(job).setSimulateResumeFrontierStateFoldStart(() -> {
                    engine.setClosing(true);
                    isClosing.set(true);
                });
                execute("INSERT INTO base VALUES ('2026-01-02T00:02:00.000000Z', 'a', -8.5)");
                drainWalQueue();
                // A pass at a time, up to the turn the shutdown cancels.
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !isClosing.get(); pass++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    job.processNotificationsForTest();
                }
                Assert.assertTrue("the resume reaches its derivation", isClosing.get());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences(CANCELLED_LINE + ", dropped=false, invalid=false]"));
                Assert.assertEquals("the cancelled turn counts as a fault", faultsBefore + 1, instance.getRefreshFaultCount());
                Assert.assertFalse("a cancellation leaves the view valid", instance.isInvalid());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.RESUME_FRONTIER_STATE_RESTORE_COUNT));
                Assert.assertEquals("nothing publishes", generationBefore, publishedCheckpointGeneration(instance));
                Assert.assertEquals(ladder, snapshotCheckpointLadder(instance));
                Assert.assertEquals("the correction stays unconsumed", processedBefore, instance.getLastProcessedSeqTxn());
                Assert.assertEquals("no turn starts while the engine closes", 0, burst(job));
            }
            engine.setClosing(false);
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            final LiveViewInstance restarted = instance();
            assertCheckpointLadder(restarted, false, """
                    2026-01-01T23:57:00.000000Z -> 1
                    2026-01-01T23:58:00.000000Z -> 2
                    """);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commitBase(job, "('2026-01-01T23:59:00.000000Z', 'a', 1.0)");
                commitBase(job, "('2026-01-02T00:03:00.000000Z', 'a', 3.0)");
            }
            assertQuery("SELECT ts, sym, x, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\tx\ts
                            2026-01-01T23:57:00.000000Z\ta\t1.5\t1.5
                            2026-01-01T23:58:00.000000Z\ta\t2.5\t4.0
                            2026-01-01T23:59:00.000000Z\ta\t1.0\t5.0
                            2026-01-02T00:03:00.000000Z\ta\t3.0\t3.0
                            """);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym, timestamp_floor('d', ts) ORDER BY ts) s FROM base WHERE x > 0 ORDER BY ts, sym",
                    "(lv) ORDER BY ts, sym",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testATurnTheShutdownCatchesBeforeItsLatchIsRefused() throws Exception {
        // The flag rises after the turn read it down ahead of the refresh latch, and before the turn takes the
        // latch: the turn is refused under it, so the breaker cancels nothing.
        assertMemoryLeak(() -> {
            createFixture("");
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewTestRefreshHooks hooks = hooks(job);
                insert(C1);
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                final long faults = instance.getRefreshFaultCount();
                final long processed = instance.getLastProcessedSeqTxn();
                final long refreshedUpTo = instance.getRefreshedUpToSeqTxn();
                final long lead = instance.getLeadRowCount();
                hooks.setSimulatePeerTurnBeforeRefreshLatch(() -> engine.setClosing(true));
                Assert.assertTrue("the run dequeued c1's task", job.run());
                Assert.assertTrue(engine.isClosing());
                Assert.assertEquals(1, hooks.getRefreshLatchAttempts());
                Assert.assertEquals(faults, instance.getRefreshFaultCount());
                Assert.assertEquals(processed, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(refreshedUpTo, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals(lead, instance.getLeadRowCount());
                Assert.assertTrue("the refused turn must release the latch", instance.tryLockForRefresh());
                instance.unlockAfterRefresh();

                Assert.assertEquals(0, burst(job, BURST_RUNS - 1));
                Assert.assertEquals(1, hooks.getRefreshLatchAttempts());
                Assert.assertEquals(faults, instance.getRefreshFaultCount());
                capture.drain();
                capture.assertNotLogged(CANCELLED_LINE);

                engine.setClosing(false);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testDropDuringShutdownDiscardsAParkedRepair() throws Exception {
        // DROP LIVE VIEW lands on a view whose repair stayed parked through the shutdown: it discards the repair
        // and unregisters the view, and a view created again under the same name holds the recompute.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                holdAParkedRepairThroughAShutdown(job);
                final LiveViewInstance instance = instance();
                engine.dropLiveView("lv", AllowAllSecurityContext.INSTANCE);
                Assert.assertTrue(instance.isDropped());
                Assert.assertNull("the DROP discards the parked repair", instance.getSuspendedRepair());
                Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
                assertQuery("SELECT count() FROM live_views()")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n0\n");

                engine.setClosing(false);
                execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testJobCloseDiscardsARepairParkedThroughAShutdown() throws Exception {
        // The pool halt closes the refresh job with the flag still up, and the job discards the repair it
        // parked. A job of the next start repairs the late row again.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                holdAParkedRepairThroughAShutdown(job);
            }
            Assert.assertTrue(engine.isClosing());
            Assert.assertNull("the job's close discards the repair it parked", instance().getSuspendedRepair());

            engine.setClosing(false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertNull(instance().getSuspendedRepair());
            Assert.assertFalse(instance().isInvalid());
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testRefreshWorkerReadsAShutdownAnotherThreadRaises() throws Exception {
        // The closing thread raises the flag while a refresh worker stands between its read of the flag ahead of
        // the refresh latch and its taking the latch. The worker reads the flag up under the latch.
        assertMemoryLeak(() -> {
            createFixture("");
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewTestRefreshHooks hooks = hooks(job);
                insert(C1);
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                final long faults = instance.getRefreshFaultCount();
                final long processed = instance.getLastProcessedSeqTxn();
                final long refreshedUpTo = instance.getRefreshedUpToSeqTxn();
                final long lead = instance.getLeadRowCount();
                final CountDownLatch isBetweenTheReads = new CountDownLatch(1);
                final CountDownLatch isFlagRaised = new CountDownLatch(1);
                hooks.setSimulatePeerTurnBeforeRefreshLatch(() -> {
                    isBetweenTheReads.countDown();
                    try {
                        Assert.assertTrue("the flag was never raised", isFlagRaised.await(THREAD_WAIT_SECONDS, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });
                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final AtomicInteger workRuns = new AtomicInteger();
                final Thread worker = new Thread(() -> {
                    try {
                        workRuns.set(burst(job));
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        clearWorkerThreadLocals();
                    }
                }, "lv-refresh-worker");
                worker.start();
                try {
                    Assert.assertTrue("the worker never reached its latch", isBetweenTheReads.await(THREAD_WAIT_SECONDS, TimeUnit.SECONDS));
                    engine.setClosing(true);
                } finally {
                    isFlagRaised.countDown();
                    worker.join(TimeUnit.SECONDS.toMillis(THREAD_WAIT_SECONDS));
                }
                Assert.assertFalse("the worker must finish its burst", worker.isAlive());
                Assert.assertTrue(errors.toString(), errors.isEmpty());
                Assert.assertEquals("only the run that dequeued c1's task reports work", 1, workRuns.get());
                Assert.assertEquals(1, hooks.getRefreshLatchAttempts());
                Assert.assertEquals(faults, instance.getRefreshFaultCount());
                Assert.assertEquals(processed, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(refreshedUpTo, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals(lead, instance.getLeadRowCount());
                capture.drain();
                capture.assertNotLogged(CANCELLED_LINE);

                engine.setClosing(false);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testShutdownLeavesAParkedRepairParked() throws Exception {
        // A repair parked on its turn budget. While the engine closes, the passes that would continue it start
        // no turn, so the repair stays parked as it is; once the flag goes down the next pass continues it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                holdAParkedRepairThroughAShutdown(job);
                engine.setClosing(false);
                driveRefreshToQuiescence(job);
                Assert.assertNull(instance().getSuspendedRepair());
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testShutdownStartsNoRestartRecoveryOverAKeptTimeline() throws Exception {
        assertMemoryLeak(() -> assertShutdownStartsNoRestartRecovery(true));
    }

    @Test
    public void testShutdownStartsNoRestartRecoveryOverARemovedTimeline() throws Exception {
        assertMemoryLeak(() -> assertShutdownStartsNoRestartRecovery(false));
    }

    @Test
    public void testShutdownStartsNoSeedSweep() throws Exception {
        // A view created over existing rows is SEEDING until its sweep covers them. While the engine closes no
        // sweep starts; once the flag goes down the sweep seeds the view.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
            insert("('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0), ('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
            final LiveViewInstance instance = instance();
            final long lvSeqTxn = lvSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewTestRefreshHooks hooks = hooks(job);
                engine.setClosing(true);
                Assert.assertEquals(0, burst(job));
                Assert.assertEquals(0, hooks.getRefreshLatchAttempts());
                Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
                Assert.assertEquals(0, instance.getRefreshFaultCount());
                Assert.assertEquals(lvSeqTxn, lvSeqTxn());

                engine.setClosing(false);
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testShutdownStartsNoTurnAfterACancelledRederive() throws Exception {
        // The base lost the WAL segment of a commit the view has yet to drain, the drain that spends the
        // flush-retry budget re-derives the view from the applied base, and a shutdown cancels the re-derive at
        // its first cursor: the view stays valid and owes the window-state debt. Every later pass of the
        // closing window would restore the runtime, meet the lost segment, charge it and have the re-derive
        // cancelled again: none starts.
        assertMemoryLeak(() -> {
            createFixture("");
            insert(C1);
            shutdown();
            removeBaseWal("tx");
            engine.buildViewGraphs();
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewTestRefreshHooks hooks = hooks(job);
                spendTheFlushRetryBudgetButOne(job, instance);
                final AtomicBoolean isTripped = new AtomicBoolean();
                hooks.setSimulateBaseApplyDuringRederive(() -> {
                    engine.setClosing(true);
                    isTripped.set(true);
                });
                advanceClockToNextRefreshPass();
                job.run();
                Assert.assertTrue("the re-derive must have met the breaker", isTripped.get());
                Assert.assertFalse("[reason=" + instance.getInvalidationReason() + ']', instance.isInvalid());
                Assert.assertTrue("the cancelled re-derive leaves the debt", instance.isWindowStateDirty());
                Assert.assertEquals("the lost segment that spent the budget is charged",
                        configuration.getLiveViewFlushRetryMax(), instance.getFlushRetryCount());
                capture.drain();
                Assert.assertEquals(1, capture.countOccurrences(REDERIVE_CANCELLED_LINE));
                final long faults = instance.getRefreshFaultCount();
                final long processed = instance.getLastProcessedSeqTxn();
                final int latchAttempts = hooks.getRefreshLatchAttempts();
                final int restores = capture.countOccurrences("live view restored its runtime from the checkpoint timeline");
                final StringSink diskRows = new StringSink();
                printDiskOnly("lv", "SELECT * FROM lv ORDER BY 2, 1", diskRows);

                for (int b = 0; b < 3; b++) {
                    advanceClockToNextRefreshPass();
                    Assert.assertEquals(0, burst(job));
                }
                Assert.assertEquals(latchAttempts, hooks.getRefreshLatchAttempts());
                Assert.assertEquals(configuration.getLiveViewFlushRetryMax(), instance.getFlushRetryCount());
                Assert.assertEquals(faults, instance.getRefreshFaultCount());
                Assert.assertEquals(processed, instance.getLastProcessedSeqTxn());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertTrue(instance.isWindowStateDirty());
                capture.drain();
                Assert.assertEquals(restores, capture.countOccurrences("live view restored its runtime from the checkpoint timeline"));
                Assert.assertEquals(1, capture.countOccurrences(REDERIVE_CANCELLED_LINE));
                capture.assertNotLogged("live view could not re-derive");
                capture.assertNotLogged("live view refresh budget exhausted, invalidating");
                final StringSink diskRowsAfter = new StringSink();
                printDiskOnly("lv", "SELECT * FROM lv ORDER BY 2, 1", diskRowsAfter);
                TestUtils.assertEquals(diskRows, diskRowsAfter);
            }
            engine.setClosing(false);
            shutdown();
            engine.buildViewGraphs();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\nlv\tactive\t\n");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertFalse(instance().isInvalid());
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testShutdownStartsNoTurnAfterAMidDrainRecoveryIsCancelled() throws Exception {
        // A drain fails between two commits after feeding the first, the mid-drain recovery finds no timeline and
        // rebuilds, and a shutdown trips the breaker before the rebuild scans: the view owes the window-state
        // debt. Every later pass of the closing window would start that recovery again for the breaker to
        // cancel: none starts, and once the flag goes down the next turn's gate settles the debt.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createFixture("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewTestRefreshHooks hooks = hooks(job);
                setCurrentMicros(instance.getLastFlushTimeUs());
                insert("('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
                drainWalQueue();
                removeTimeline();
                final AtomicBoolean isTripped = new AtomicBoolean();
                hooks.setRefreshFaultObserver(t -> {
                    if (isTripped.compareAndSet(false, true)) {
                        engine.setClosing(true);
                    }
                });
                fault.arm(2);
                final long faults = instance.getRefreshFaultCount();
                final long processed = instance.getLastProcessedSeqTxn();
                // Turn 1 drains the first commit, whose task the first insert queued; the task the refreshed
                // notification re-enqueues has turn 2 drain the other two, and it fails between them.
                Assert.assertTrue(job.run());
                Assert.assertTrue(fault.hasFired());
                Assert.assertTrue(isTripped.get());
                Assert.assertTrue(engine.isClosing());
                Assert.assertEquals("both turns took the latch before the flag rose", 2, hooks.getRefreshLatchAttempts());
                assertRecoveryCancelledOnce(instance, faults + 1, processed);

                for (int b = 0; b < 2; b++) {
                    Assert.assertEquals(0, burst(job));
                    Assert.assertEquals(2, hooks.getRefreshLatchAttempts());
                    assertRecoveryCancelledOnce(instance, faults + 1, processed);
                }

                hooks.setRefreshFaultObserver(null);
                engine.setClosing(false);
                driveRefreshToQuiescence(job);
                capture.drain();
                Assert.assertEquals(2, capture.countOccurrences(RESTORE_DECLINED_LINE));
                Assert.assertEquals(1, capture.countOccurrences("live view recomputed window state from applied base [view=lv"));
                Assert.assertEquals(faults + 1, instance.getRefreshFaultCount());
                Assert.assertFalse(instance.isWindowStateDirty());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testShutdownStartsNoTurnForAQueuedCommit() throws Exception {
        assertMemoryLeak(() -> assertShutdownStartsNoTurnForAQueuedCommit(""));
    }

    @Test
    public void testShutdownStartsNoTurnForAQueuedCommitOverADedupBase() throws Exception {
        assertMemoryLeak(() -> assertShutdownStartsNoTurnForAQueuedCommit(DEDUP_CLAUSE));
    }

    private static void assertRecoveryCancelledOnce(LiveViewInstance instance, long faults, long processed) {
        Assert.assertEquals(faults, instance.getRefreshFaultCount());
        Assert.assertFalse(instance.isInvalid());
        Assert.assertEquals("a cancellation must not count toward the flush-retry budget", 0, instance.getFlushRetryCount());
        Assert.assertEquals("a cancellation must not start a flush-retry streak", Numbers.LONG_NULL, instance.getFlushRetryStartUs());
        Assert.assertEquals("a cancellation must not arm the refresh-retry back-off", Numbers.LONG_NULL, instance.getRefreshRetryNotBeforeUs());
        Assert.assertTrue("the cancelled recovery leaves the debt", instance.isWindowStateDirty());
        Assert.assertEquals(processed, instance.getLastProcessedSeqTxn());
        capture.drain();
        Assert.assertEquals(1, capture.countOccurrences(RESTORE_DECLINED_LINE));
        Assert.assertEquals(1, capture.countOccurrences("live view window-state recompute failed [view=lv"));
        Assert.assertEquals(1, capture.countOccurrences(RECOVERY_CANCELLED_LINE));
        Assert.assertEquals(0, capture.countOccurrences(CANCELLED_LINE));
    }

    private static int burst(LiveViewRefreshJob job) {
        return burst(job, BURST_RUNS);
    }

    private static int burst(LiveViewRefreshJob job, int runs) {
        int workRuns = 0;
        for (int i = 0; i < runs; i++) {
            if (job.run()) {
                workRuns++;
            }
        }
        return workRuns;
    }

    private static long lvSeqTxn() {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("lv"));
    }

    private void commitBase(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO base VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // The rows of the view at createdAt.
    private void assertRows(String createdAt, String rows) throws Exception {
        assertQuery("SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv WHERE created_at = '" + createdAt + "'")
                .noLeakCheck()
                .timestamp("created_at")
                .returns("created_at\taccount_id\tcumulative_sum\tcumulative_count\n" + rows);
    }

    // After a restart the view's first turn restores it from its timeline, or rebuilds it from the applied base when
    // the timeline is gone. While the engine closes no such turn starts, and the single-shot attempt stays unmade.
    private void assertShutdownStartsNoRestartRecovery(boolean isTimelineKept) throws Exception {
        createFixture("");
        if (!isTimelineKept) {
            removeTimeline();
        }
        shutdown();
        engine.buildViewGraphs();
        final LiveViewInstance instance = instance();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            final LiveViewTestRefreshHooks hooks = hooks(job);
            engine.setClosing(true);
            advanceClockToNextRefreshPass();
            Assert.assertEquals(0, burst(job));
            Assert.assertFalse("the single-shot restart recovery must stay unmade", instance.isCheckpointRestoreAttempted());
            Assert.assertEquals(0, hooks.getRefreshLatchAttempts());
            Assert.assertEquals(0, instance.getRefreshFaultCount());
            Assert.assertFalse(instance.isWindowStateDirty());
            capture.drain();
            capture.assertNotLogged("restored live view from checkpoint timeline [view=lv");
            capture.assertNotLogged("live view restart rebuilding from applied base [view=lv");

            engine.setClosing(false);
            driveRefreshToQuiescence(job);
            if (isTimelineKept) {
                assertRestoredFromTimeline("lv");
            } else {
                assertRebuiltFromAppliedBase("lv");
            }
            insert(C1);
            driveRefreshToQuiescence(job);
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        }
    }

    // A commit whose task the notification path dequeues while the engine closes: the dequeue is the run's work,
    // and it starts no turn.
    private void assertShutdownStartsNoTurnForAQueuedCommit(String dedupClause) throws Exception {
        createFixture(dedupClause);
        final LiveViewInstance instance = instance();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            final LiveViewTestRefreshHooks hooks = hooks(job);
            insert(C1);
            // Past FLUSH EVERY: a turn would drain the commit and flush it.
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            final long faults = instance.getRefreshFaultCount();
            final long processed = instance.getLastProcessedSeqTxn();
            final long refreshedUpTo = instance.getRefreshedUpToSeqTxn();
            final long lead = instance.getLeadRowCount();
            engine.setClosing(true);
            Assert.assertTrue("the run dequeued c1's task", job.run());
            Assert.assertEquals(0, hooks.getRefreshLatchAttempts());
            Assert.assertEquals(faults, instance.getRefreshFaultCount());
            Assert.assertEquals(processed, instance.getLastProcessedSeqTxn());
            Assert.assertEquals(refreshedUpTo, instance.getRefreshedUpToSeqTxn());
            Assert.assertEquals(lead, instance.getLeadRowCount());
            Assert.assertEquals(0, burst(job, BURST_RUNS - 1));
            Assert.assertEquals(0, hooks.getRefreshLatchAttempts());
            Assert.assertEquals(faults, instance.getRefreshFaultCount());
            capture.drain();
            capture.assertNotLogged(CANCELLED_LINE);

            engine.setClosing(false);
            driveRefreshToQuiescence(job);
            Assert.assertEquals(faults, instance.getRefreshFaultCount());
            assertViewMatchesRecompute();
        }
    }

    // parkARepair, then the flag and three passes, each of which reports no work and leaves the repair parked as it
    // was, with the view's WAL, its faults and the job's latch attempts where the repair left them.
    private void holdAParkedRepairThroughAShutdown(LiveViewRefreshJob job) throws Exception {
        final LiveViewTestRefreshHooks hooks = hooks(job);
        parkARepair(job);
        final LiveViewInstance instance = instance();
        final LiveViewCheckpointRepairSession session = instance.getSuspendedRepair();
        Assert.assertNotNull(session);
        final long lvSeqTxn = lvSeqTxn();
        final int latchAttempts = hooks.getRefreshLatchAttempts();
        engine.setClosing(true);
        for (int pass = 0; pass < 3; pass++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            Assert.assertFalse(job.processNotificationsForTest());
            Assert.assertSame("the shutdown must leave the repair parked", session, instance.getSuspendedRepair());
            Assert.assertEquals(lvSeqTxn, lvSeqTxn());
            Assert.assertEquals(0, instance.getRefreshFaultCount());
            Assert.assertEquals(latchAttempts, hooks.getRefreshLatchAttempts());
        }
    }
}
