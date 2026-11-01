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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Coverage for the route a live view's one restart recovery attempt records.
 * <p>
 * A restart has two ways to end up with correct rows and only one of them is the
 * checkpoint timeline. {@code tryRestoreFromTimeline} catches every {@link Throwable}
 * and falls through to the applied-base rebuild, which throws the published roots away
 * and recomputes the entire window from the base table. The rebuild produces the same
 * rows, faults no refresh cycle, fails no seal and reports the same
 * {@code isCheckpointRestoreSucceeded()} - so every oracle a restart test would
 * otherwise reach for is green over it, and a restore that silently stopped working
 * looks exactly like one that works.
 * <p>
 * The cases below pin that pairing directly: the fallback case asserts the success flag
 * is {@code true} <b>while</b> the route is {@code fallback_rebuild}, which is the
 * demonstration that the flag cannot carry a restore assertion on its own. The witness
 * is what carries it.
 */
public class LiveViewCheckpointRestoreRouteTest extends AbstractLiveViewTest {
    private static final int ERRNO_EMFILE = 24;
    // The gate's restore declines for a view with no timeline, and it falls back to the rebuild.
    private static final String GATE_ABSENT_TIMELINE_REBUILD_LINE = "live view cannot restore its runtime from the checkpoint timeline, "
            + "rebuilding from the applied base [view=lv, cause=mid-drain refresh failure, reason=timeline is absent]";
    // The line the restart's applied-base rebuild logs when it hands a fault a retry may get past
    // to the flush-retry budget instead of invalidating the view.
    private static final String RESTART_REBUILD_RETRY_LINE = "live view restart rebuild hit a retryable fault, retrying [view=lv";
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so a handful of rows leaves a real ladder to
        // restart off rather than a single root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testAFailedRebuildRecordsABlockedRoute() throws Exception {
        // The timeline is gone, so the restart has nothing to restore, and the rebuild
        // that covers for that cannot read the base either. The view then has no derived
        // state at all - the disposition no other case can reach, because a rebuild that
        // works covers for every restore that does not.
        final AtomicBoolean failBaseColumnReads = new AtomicBoolean();
        // The errno the next errno() read reports, set by the open that failed; 0 when none.
        final AtomicInteger pendingErrno = new AtomicInteger();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                final int errno = pendingErrno.getAndSet(0);
                return errno != 0 ? errno : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                // Partition column reads only. The WAL segment copies carry the same
                // file name and the apply that reads them must keep working, or the
                // fixture never gets its rows into the base table. The file reads as
                // gone, which no retry gets past.
                if (failBaseColumnReads.get()
                        && Utf8s.endsWithAscii(name, "amount.d")
                        && !Utf8s.containsAscii(name, "wal")) {
                    pendingErrno.set(CairoException.ERRNO_FILE_DOES_NOT_EXIST);
                    return -1;
                }
                return super.openRO(name);
            }
        }, () -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 30; second += 10) {
                    insertAccount(job, timestamp(second), "acct-1", second + 1.0);
                }
            }

            removeTimeline();
            failBaseColumnReads.set(true);
            try {
                restartCycle();
            } finally {
                failBaseColumnReads.set(false);
            }

            final LiveViewInstance instance = instance();
            assertRoute(LiveViewCheckpointRecoveryPhase.ROUTE_BLOCKED, instance);
            Assert.assertFalse(
                    "a blocked attempt resolved no derived state, so it must not report success",
                    instance.isCheckpointRestoreSucceeded()
            );
            Assert.assertEquals(
                    "the attempt must have started exactly one rebuild before blocking",
                    1L,
                    instance.getCheckpointRebuildAttempts()
            );
            Assert.assertEquals(
                    "a blocked attempt names no root",
                    Numbers.LONG_NULL,
                    instance.getCheckpointRestoreCheckpointId()
            );
        });
    }

    @Test
    public void testABaseSchemaChangeDuringTheRestartRebuildKeepsTheViewActive() throws Exception {
        // ADD COLUMN touches no column the view references, so it leaves the view valid, but it
        // moves the base metadata version past the plan the restart compiled. Landing it between
        // that compile and the rebuild's base reader open makes the rebuild meet a stale plan,
        // which only a recompile can answer. The restart must hand the drift to the recovery
        // that recompiles rather than take the view out of service.
        final AtomicBoolean isSchemaChangeArmed = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                // The restart checks for the timeline after it compiled the plan and before the
                // rebuild opens the base. Gate on the compile too, so an earlier look at the
                // timeline - the catalogue load's - cannot land the change before it.
                if (isSchemaChangeArmed.get()
                        && Utf8s.endsWithAscii(path, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)
                        && isRestartPlanCompiled()) {
                    isSchemaChangeArmed.set(false);
                    try {
                        execute("ALTER TABLE tx ADD COLUMN unrelated TIMESTAMP_NS");
                        drainWalQueue();
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.exists(path);
            }
        }, () -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 30; second += 10) {
                    insertAccount(job, timestamp(second), second % 20 == 0 ? "acct-1" : "acct-2", second + 1.0);
                }
                assertViewMatchesRecompute();
            }

            // The missing timeline sends the restart down the applied-base rebuild, the route
            // whose failure used to invalidate the view.
            removeTimeline();
            isSchemaChangeArmed.set(true);
            restartCycle();
            Assert.assertFalse("the schema change never landed inside the restart", isSchemaChangeArmed.get());

            final LiveViewInstance instance = instance();
            Assert.assertFalse(
                    "a base schema change to a column the view does not reference must not invalidate it"
                            + " [reason=" + instance.getInvalidationReason() + ']',
                    instance.isInvalid()
            );
            Assert.assertEquals(
                    "the restart must start the rebuild the schema change interrupts, and must not fall"
                            + " back to a second one over the same stale plan",
                    1L,
                    instance.getCheckpointRebuildAttempts()
            );
            Assert.assertEquals(
                    "the drift must reach the refresh failure handler once, whose recovery recompiles",
                    1L,
                    instance.getRefreshFaultCount()
            );
            assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\nactive\n");
            assertViewMatchesRecompute();

            // The recovered runtime must keep the running totals: a cold one would answer this
            // row's own amount.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('" + timestamp(40) + "', 'acct-1', 100.0)");
                drainWalQueue();
                driveRefreshToQuiescence(resumed);
                assertViewMatchesRecompute();
            }
            Assert.assertFalse(instance().isInvalid());
        });
    }

    @Test
    public void testAFaultTheRetryGetsPastDuringTheRestartRebuildKeepsTheViewActive() throws Exception {
        // testAFailedRebuildRecordsABlockedRoute with the base column read failing once on EMFILE,
        // a fault a retry may get past, rather than on a file that is gone. The restart must hand
        // the fault to the flush-retry accounting, rather than invalidate the view durably on a
        // fault that clears. The fault strikes the probe, before the rebuild owes the window
        // state, so the faulted turn charges it and leaves the window-state debt, which the gate
        // of the turn the next base commit gives the view settles with a rebuild.
        final AtomicBoolean isScanFaultArmed = new AtomicBoolean();
        final AtomicBoolean hasScanFaultFired = new AtomicBoolean();
        // The errno the next errno() read reports, set by the open that failed; 0 when none.
        final AtomicInteger pendingErrno = new AtomicInteger();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                final int errno = pendingErrno.getAndSet(0);
                return errno != 0 ? errno : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                if (isScanFaultArmed.get()
                        && Utf8s.endsWithAscii(name, "amount.d")
                        && !Utf8s.containsAscii(name, "wal")
                        && isScanFaultArmed.compareAndSet(true, false)) {
                    hasScanFaultFired.set(true);
                    pendingErrno.set(ERRNO_EMFILE);
                    return -1;
                }
                return super.openRO(name);
            }
        }, () -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 30; second += 10) {
                    insertAccount(job, timestamp(second), second % 20 == 0 ? "acct-1" : "acct-2", second + 1.0);
                }
                assertViewMatchesRecompute();
            }

            removeTimeline();
            engine.releaseInactive();
            isScanFaultArmed.set(true);
            restartCycle();
            Assert.assertTrue("the restart rebuild's scan must have been failed", hasScanFaultFired.get());

            final LiveViewInstance instance = instance();
            Assert.assertFalse(
                    "a fault a retry gets past must not invalidate the view [reason=" + instance.getInvalidationReason() + ']',
                    instance.isInvalid()
            );
            Assert.assertFalse(instance.hasPendingInvalidationReason());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            // Nothing drives a view with no commits in front of it, so the debt stands.
            Assert.assertTrue(instance.isWindowStateDirty());
            Assert.assertEquals("the one scan fault is the one refresh fault", 1L, instance.getRefreshFaultCount());
            Assert.assertEquals(1L, instance.getCheckpointRebuildAttempts());
            // A rebuild that hands its fault on records no restart route, as a drift's does.
            assertRoute(LiveViewCheckpointRecoveryPhase.ROUTE_NONE, instance);
            assertViewMatchesRecompute();
            capture.drain();
            capture.assertOnlyOnce(Pattern.quote(RESTART_REBUILD_RETRY_LINE));
            capture.assertNotLogged("live view restart applied-base rebuild failed [view=");
            // No restore ran, so the log must not report one as failed.
            capture.assertNotLogged("could not restore live view from checkpoint timeline");
            // The fault came before the rebuild owed the window state, so the faulted turn charged it
            // rather than recover in place.
            capture.assertLogged("live view refresh failed [view=lv, retryCount=1");
            capture.assertNotLogged("live view refresh failed, window state recovered");
            capture.assertNotLogged(GATE_ABSENT_TIMELINE_REBUILD_LINE);

            // The commit's turn runs the gate, which rebuilds with the restatement guard armed. The
            // rebuild pins the base's applied head, so it consumes the commit, and the turn records
            // the success that ends the retry streak.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                insertAccount(resumed, timestamp(40), "acct-1", 100.0);
            }
            Assert.assertFalse(instance.isInvalid());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isWindowStateDirty());
            Assert.assertEquals(1L, instance.getRefreshFaultCount());
            Assert.assertEquals("the gate's rebuild is no restart rebuild attempt", 1L, instance.getCheckpointRebuildAttempts());
            // The gate's recovery records no restart route either.
            assertRoute(LiveViewCheckpointRecoveryPhase.ROUTE_NONE, instance);
            Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
            Assert.assertEquals(0, instance.getFlushRetryCount());
            assertViewMatchesRecompute();
            capture.drain();
            capture.assertOnlyOnce(Pattern.quote(GATE_ABSENT_TIMELINE_REBUILD_LINE));

            // The gate's rebuild sealed a fresh timeline, which the next restart restores from.
            restartCycle();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance().isInvalid());
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAFaultTheRetryGetsPastMidwayThroughTheRestartRebuildKeepsTheViewActive() throws Exception {
        // testAFaultTheRetryGetsPastDuringTheRestartRebuildKeepsTheViewActive with the fault on the
        // second hour's column file. By then the probe has found a row, the rebuild has wiped the
        // accumulators and owes the window state, and its replacement writer holds the first hour's
        // rows uncommitted. The faulted turn's failure handling recovers the window state in place,
        // with a rebuild that runs the restatement guard, unlike the upgrade's.
        final AtomicBoolean isScanFaultArmed = new AtomicBoolean();
        final AtomicBoolean hasScanFaultFired = new AtomicBoolean();
        final AtomicBoolean isWindowStateDirtyAtFault = new AtomicBoolean();
        // The base column opens the fault let through before it fired.
        final AtomicInteger scanOpensBeforeFault = new AtomicInteger();
        // The errno the next errno() read reports, set by the open that failed; 0 when none.
        final AtomicInteger pendingErrno = new AtomicInteger();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                final int errno = pendingErrno.getAndSet(0);
                return errno != 0 ? errno : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                if (isScanFaultArmed.get()
                        && Utf8s.endsWithAscii(name, "amount.d")
                        && !Utf8s.containsAscii(name, "wal")) {
                    if (Utf8s.containsAscii(name, "2026-01-01T09")) {
                        scanOpensBeforeFault.incrementAndGet();
                    } else if (isScanFaultArmed.compareAndSet(true, false)) {
                        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                        isWindowStateDirtyAtFault.set(instance != null && instance.isWindowStateDirty());
                        hasScanFaultFired.set(true);
                        pendingErrno.set(ERRNO_EMFILE);
                        return -1;
                    }
                }
                return super.openRO(name);
            }
        }, () -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, "2026-01-01T09:00:00.000000Z", "acct-1", 1.0);
                insertAccount(job, "2026-01-01T09:00:10.000000Z", "acct-2", 11.0);
                insertAccount(job, "2026-01-01T10:00:00.000000Z", "acct-1", 21.0);
                insertAccount(job, "2026-01-01T10:00:10.000000Z", "acct-2", 31.0);
                assertViewMatchesRecompute();
            }

            removeTimeline();
            engine.releaseInactive();
            isScanFaultArmed.set(true);
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob restarted = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(restarted);
                guard = restarted.rebuildRestatementGuardForTest();
                Assert.assertTrue("the restart rebuild's scan must have been failed", hasScanFaultFired.get());
                Assert.assertTrue("the scan must have opened the first hour before the fault", scanOpensBeforeFault.get() > 0);
                Assert.assertTrue("the fault must strike after the rebuild owes the window state", isWindowStateDirtyAtFault.get());
                // Read before the invalidation check, so a run that invalidates the view still shows
                // that the abandoned replacement rolled back: the first hour alone would replace
                // every row.
                assertViewMatchesRecompute();

                final LiveViewInstance instance = instance();
                Assert.assertFalse(
                        "a fault a retry gets past must not invalidate the view [reason=" + instance.getInvalidationReason() + ']',
                        instance.isInvalid()
                );
                Assert.assertFalse(instance.hasPendingInvalidationReason());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals("the one scan fault is the one refresh fault", 1L, instance.getRefreshFaultCount());
                Assert.assertEquals("the recovery's rebuild is no restart rebuild attempt", 1L, instance.getCheckpointRebuildAttempts());
                // The in-place recovery records no restart route, as a drift's does.
                assertRoute(LiveViewCheckpointRecoveryPhase.ROUTE_NONE, instance);
                // The recovery's rebuild ran the guard, which found every row the view retains.
                Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
                Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
                Assert.assertEquals(4, guard.getReproducedRows());
                capture.drain();
                capture.assertOnlyOnce(Pattern.quote(RESTART_REBUILD_RETRY_LINE));
                capture.assertNotLogged("live view restart applied-base rebuild failed [view=");
                capture.assertNotLogged("could not restore live view from checkpoint timeline");
                // The faulted turn recovered the window state itself, rather than charge the fault
                // and leave the recovery to a later turn's gate.
                capture.assertLogged("live view refresh failed, window state recovered, retrying [view=lv");
                capture.assertNotLogged("live view refresh failed [view=lv");
                capture.assertOnlyOnce(Pattern.quote(GATE_ABSENT_TIMELINE_REBUILD_LINE));

                // A commit drains over the recovered runtime, and the turn that gets past it ends the
                // retry streak.
                insertAccount(restarted, "2026-01-01T10:00:20.000000Z", "acct-1", 100.0);
                assertViewMatchesRecompute();
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals(1L, instance.getRefreshFaultCount());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
                Assert.assertEquals(0, instance.getFlushRetryCount());
            }

            // The recovery's rebuild sealed a fresh timeline, which the next restart restores from.
            restartCycle();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance().isInvalid());
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testAnAbsentTimelineRecordsAFallbackRebuildThatStillReportsSuccess() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 30; second += 10) {
                    insertAccount(job, timestamp(second), second % 20 == 0 ? "acct-1" : "acct-2", second + 1.0);
                }
                assertViewMatchesRecompute();
            }

            // The roots are still on disk; only the ladder that addresses them is gone,
            // which is what sends the restart down the applied-base rebuild.
            removeTimeline();
            restartCycle();

            final LiveViewInstance instance = instance();
            assertRebuiltFromAppliedBase("lv");
            Assert.assertTrue(
                    "the rebuild resolves the derived state too, which is exactly why the success flag"
                            + " cannot carry a restore assertion on its own",
                    instance.isCheckpointRestoreSucceeded()
            );
            Assert.assertTrue(
                    "the rebuild retires the timeline before it replays",
                    instance.getCheckpointTimelineResets() > 0
            );
            Assert.assertEquals(
                    "a rebuild names no root: it restored from none",
                    Numbers.LONG_NULL,
                    instance.getCheckpointRestoreGeneration()
            );
            // The rows a naive restart oracle would have stopped at. They are correct
            // either way, which is the point.
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");
        });
    }

    @Test
    public void testARestartOffPublishedRootsRecordsTheRootItRestoredFrom() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            final long publishedGeneration;
            final long newestCheckpointId;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                for (int second = 0; second <= 30; second += 10) {
                    insertAccount(job, timestamp(second), second % 20 == 0 ? "acct-1" : "acct-2", second + 1.0);
                }
                assertViewMatchesRecompute();
                publishedGeneration = newestGeneration();
                newestCheckpointId = newestCheckpointId();
            }

            restartCycle();

            // Captured before the restart, because the first post-restart flush reseals
            // over the lineage: the witness is stamped at restore time and survives it,
            // the ladder does not.
            assertRestoredFromTimeline("lv");
            final LiveViewInstance instance = instance();
            Assert.assertEquals(
                    "the restart must name the root the pre-restart writer left as the newest boundary",
                    newestCheckpointId,
                    instance.getCheckpointRestoreCheckpointId()
            );
            Assert.assertEquals(
                    "the restart must restore under the generation the previous process published",
                    publishedGeneration,
                    instance.getCheckpointRestoreGeneration()
            );
            assertViewMatchesRecompute();
            assertNoRefreshFaults("lv");

            // A restore that dropped the accumulators would answer this row's own amount
            // rather than the running total, and the witness above would still be green.
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                insertAccount(resumed, timestamp(40), "acct-1", 100.0);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    private static void assertRoute(int expected, LiveViewInstance instance) {
        Assert.assertEquals(
                "live view 'lv' took the wrong restart recovery route",
                LiveViewCheckpointRecoveryPhase.routeName(expected),
                LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
        );
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static String timestamp(int secondOfDay) {
        return "2026-01-01T09:" + String.format("%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT created_at, account_id, "
                        + "sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                        + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum "
                        + "FROM (SELECT created_at, account_id, amount, " + bucket + " AS bucket FROM tx)"
                        + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
    }

    private void createTargetView() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    private void insertAccount(LiveViewRefreshJob job, String timestamp, String account, double amount)
            throws Exception {
        execute("INSERT INTO tx VALUES ('" + timestamp + "', '" + account + "', " + amount + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private boolean isRestartPlanCompiled() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        return instance != null && instance.isCheckpointRestoreAttempted() && instance.getCompiledPlan() != null;
    }

    private long newestCheckpointId() {
        final LiveViewInstance instance = instance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            store.of(dir);
            timeline.of(dir);
            try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue(
                        "the view must have sealed a boundary to restart off",
                        timeline.last(pin.getTimelineRootRef(), newest)
                );
                return newest.checkpointId;
            }
        }
    }

    private long newestGeneration() {
        final LiveViewInstance instance = instance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration)
        ) {
            store.of(dir);
            try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                return pin.getGeneration();
            }
        }
    }

    private void removeTimeline() {
        try (
                Path dir = checkpointsDir(instance());
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            Assert.assertTrue(
                    "the fixture must have published a timeline to remove",
                    configuration.getFilesFacade().removeQuiet(timelinePath.$())
            );
        }
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }
}
