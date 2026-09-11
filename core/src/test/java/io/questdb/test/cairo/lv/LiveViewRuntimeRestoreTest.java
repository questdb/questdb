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

import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The restore from the checkpoint timeline that a refreshing view runs in place of a whole-view
 * rebuild, when a base schema change or a mid-drain failure has cost it its accumulators.
 * <p>
 * Both recoveries leave the view's durable output correct and only its runtime wrong: a drift
 * freed the compiled factory, and a mid-drain failure fed rows the turn never committed. They used
 * to answer that by recomputing every retained row from the applied base and replacing the whole
 * output - a restatement the rebuild restatement guard refuses once the base has lost rows the
 * view retains, which stopped the view until a restart. The restore is the restart's own recovery
 * run in place: recompile, restore the newest compatible root, replay the base WAL above it up to
 * the applied watermark. It rewrites no output, so it has nothing to restate.
 * <p>
 * The view here keeps the checkpoint cadence at its default, which seals the first boundary when
 * the first row lands and nothing after it for the length of any case. So the newest root sits
 * well below the durable frontier, and every restore has base WAL to replay above it: a restore
 * that brought back the root alone would leave acct-1's day-two accumulation short, which the
 * expected rows would catch.
 * <p>
 * Every case ends on explicit rows and on a counter that tells the restore from the rebuild.
 * The rows alone could not: over a base that still holds every row, the rebuild reproduces them
 * exactly, so a restore that silently fell back would pass a row comparison.
 */
public class LiveViewRuntimeRestoreTest extends AbstractLiveViewCheckpointCompatTest {
    private static final String[] FOUR_ROWS = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)"
    };
    private static final String SEVEN_ROWS_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-02T09:20:00.000000Z\tacct-2\t16.0\t1
            2026-01-02T09:30:00.000000Z\tacct-1\t44.0\t3
            2026-01-02T09:40:00.000000Z\tacct-2\t80.0\t2
            """;
    private static final String[] SIX_ROWS = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)",
            "('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0)",
            "('2026-01-03T09:10:00.000000Z', 'acct-2', 32.0)"
    };
    // ANCHOR DAILY resets each account's accumulators at midnight.
    private static final String SIX_ROWS_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    private static final String RESTORED = "live view restored its runtime from the checkpoint timeline";
    private static final String VIEW_ROWS_QUERY = "SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv";
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
    public void testABaseSchemaChangeRestoresTheRuntimeFromTheTimeline() throws Exception {
        assertMemoryLeak(() -> {
            // A deduplicating base, because its drain reads the applied base through the
            // compiled factory, and that is where a base metadata change surfaces as drift.
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(SIX_ROWS);
            Assert.assertEquals("the default cadence seals the first boundary only", 1, countSealedBoundaries("lv"));

            // A schema change the view survives, then a commit the base collapses into one row:
            // the collapse routes the drain through the applied base, whose reader the view's
            // compiled plan now predates.
            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            // The restore replayed the five rows above the first root, and nothing rebuilt.
            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=base table metadata change, .*replayedRows=5]");
            capture.assertNotLogged("live view recomputed window state from applied base");
            capture.assertNotLogged("could not restore its runtime");
            Assert.assertEquals(
                    "no whole-view rebuild may have run",
                    LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                    guard.getAbstention()
            );
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());

            // The commit that met the drift is materialized by the recompiled runtime, on top of
            // the day-three accumulation the restore put back.
            assertViewRows(SIX_ROWS_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");

            // The ladder the restore stood on is the one a restart reads, and it agrees.
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(SIX_ROWS_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");
        });
    }

    @Test
    public void testADriftWhoseRecoveryFailsLeavesTheDebtForTheNextTurn() throws Exception {
        final String[] baseDir = new String[1];
        final AtomicBoolean failTimelineOpen = new AtomicBoolean();
        final AtomicBoolean failBaseColumnOpen = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // The rebuild's first read of a base partition column - its probe, which runs
                // before it wipes the runtime.
                if (failBaseColumnOpen.get()
                        && baseDir[0] != null
                        && Utf8s.containsAscii(name, baseDir[0])
                        && !Utf8s.containsAscii(name, "wal")
                        && Utf8s.endsWithAscii(name, ".d")) {
                    failBaseColumnOpen.set(false);
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                // The restore's first open of the timeline, which maps its superblock.
                if (failTimelineOpen.get() && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)) {
                    failTimelineOpen.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            baseDir[0] = engine.verifyTableName("tx").getDirName();
            insertAndRefresh(SIX_ROWS);

            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            // Both recoveries of the drift turn fail. The drift freed the factory before either
            // ran, and the rebuild's probe fails before its wipe, so neither recovery marks the
            // runtime it leaves behind: the drift itself has to. A later turn that drained
            // through that runtime would count acct-1's day three from nothing - 64.0 over one
            // row instead of 80.0 over two.
            failTimelineOpen.set(true);
            failBaseColumnOpen.set(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertFalse("the restore's timeline read must have been failed", failTimelineOpen.get());
            Assert.assertFalse("the rebuild's probe must have been failed", failBaseColumnOpen.get());
            assertViewRows(SIX_ROWS_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");

            capture.drain();
            capture.assertLogged("live view could not restore its runtime from the checkpoint timeline");
            capture.assertLogged("live view window-state recompute failed");
            // The next turn's gate took the debt and restored, now that nothing fails.
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=");
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertFalse(instance.isInvalid());
        });
    }

    @Test
    public void testAMidDrainFailureOverABaseThatLostADayKeepsTheViewRunning() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            // The incremental path walks past the DROP PARTITION and keeps the day's rows. A
            // whole-view rebuild from here would drop them, and the restatement guard would
            // refuse it on the history floor and stop the view.
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                insertThreeAndFailMidDrain(job, fault);
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, .*replayedRows=[1-9]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED, guard.getAbstention());
            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse("the view must keep refreshing", instance.isCheckpointRecoveryBlocked());
            assertRestoredInProcess(instance, 1);
            // The dropped day stays, and the three commits the fault interrupted land on top of
            // accumulators that neither lost nor double-counted a row.
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testAMidDrainFailureRestoresTheRuntimeAndDerivesTheLeadAgain() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            final long durableSeqTxn = instance("lv").getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertThreeAndFailMidDrain(job, fault);
                // The clock has not moved, so nothing has flushed since the fault: the
                // recovery dropped the lead the failed turn stood on, and the turn after it
                // derived all three commits into the lead again, over the restored runtime.
                final LiveViewInstance recovering = instance("lv");
                Assert.assertEquals(durableSeqTxn, recovering.getLastProcessedSeqTxn());
                Assert.assertEquals(durableSeqTxn + 3, recovering.getRefreshedUpToSeqTxn());
                Assert.assertEquals(3, recovering.getLeadRowCount());
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, .*replayedRows=3]");
            capture.assertNotLogged("live view recomputed window state from applied base");
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the mid-drain fault is the one fault", 1, instance.getRefreshFaultCount());
            Assert.assertEquals("a restore that recovered charges no retry", 0, instance.getFlushRetryCount());
            Assert.assertEquals(
                    "the view must have refreshed and flushed past every commit",
                    instance.getLastProcessedSeqTxn(),
                    instance.getRefreshedUpToSeqTxn()
            );
            // Row 09:30 is the one the failed turn had already fed: a runtime left as the turn
            // left it would count it twice, 76.0 over four rows.
            assertViewRows(SEVEN_ROWS_OUTPUT);

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testARestoreBehindALiveRepairMarkerFallsBackToTheRebuild() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            // What a prefix-preserving repair leaves while its truncated head is not yet
            // re-sealed: the superblock still names the discarded head, so no restore may read
            // the timeline under it.
            writeRepairMarker(instance("lv"));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertThreeAndFailMidDrain(job, fault);
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=mid-drain refresh failure, reason=prefix preservation repair marker present]");
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
            Assert.assertTrue("the rebuild retires the timeline under the marker", instance.getCheckpointTimelineResets() > 0);
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertFalse(
                        "the retire takes the marker with the timeline",
                        LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir)
                );
            }
            // The base holds every row, so the rebuild is compared and reproduces them.
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testARestoreThatCannotReproduceTheViewFallsBackToTheRebuild() throws Exception {
        assertMemoryLeak(() -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            // The fourth commit carries a duplicate the base collapses into its last row. The
            // view's drain reads the applied base and emits one row for it; a replay of the raw
            // WAL above the first root feeds both.
            insertAndRefresh(
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
                    "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
                    "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
                    "('2026-01-02T09:10:00.000000Z', 'acct-1', 7.0), ('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)",
                    "('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0)"
            );
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
                    """;
            assertViewRows(viewRows);

            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            // The restore's own check refuses a replay that feeds five rows above the first root
            // where the view holds four, and the rebuild covers for it - from a base that still
            // holds every row, so it is compared and goes ahead.
            capture.drain();
            capture.assertLogged("live view could not restore its runtime from the checkpoint timeline");
            capture.assertLogged("does not match durable materialization");
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=base table metadata change]");
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isWindowStateDirty());
            assertViewRows(viewRows + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");
        });
    }

    /**
     * Asserts the view's accumulators came back from its own timeline while it was refreshing,
     * rather than from a rebuild: the runtime restores counted, no timeline retired, no restart
     * rebuild started, and no debt left over.
     */
    private static void assertRestoredInProcess(LiveViewInstance instance, long expectedRestores) {
        Assert.assertEquals(
                "the view must have restored its runtime from the timeline while refreshing",
                expectedRestores,
                instance.getCheckpointRuntimeRestores()
        );
        Assert.assertEquals("a restore retires no timeline", 0, instance.getCheckpointTimelineResets());
        Assert.assertEquals("a restore starts no rebuild", 0, instance.getCheckpointRebuildAttempts());
        Assert.assertFalse("a restore settles the window-state debt", instance.isWindowStateDirty());
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery(VIEW_ROWS_QUERY)
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
    }

    private void createBase(String dedupClause) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL " + dedupClause);
    }

    private void createView() throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    /**
     * One commit per argument, each refreshed before the next, so the view's watermark sits on
     * the last commit and its first root on the first.
     */
    private void insertAndRefresh(String... commits) throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (String values : commits) {
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + values);
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        }
        assertNoRefreshFaults("lv");
    }

    /**
     * Commits three rows and has the refresh fail between feeding the second and reading the
     * third: the first gets a refresh task of its own, the next two coalesce behind it and drain
     * in one pass, and the fault fails that pass's read of the third commit's timestamp column.
     * <p>
     * The clock stays on the view's last flush throughout, so the first commit is an un-flushed
     * lead when the fault lands - the lead the recovery has to drop - and whatever the recovery
     * leaves is still unflushed when this returns. The caller drives the flush.
     */
    private void insertThreeAndFailMidDrain(LiveViewRefreshJob job, LiveViewMidDrainFault fault) throws Exception {
        setCurrentMicros(instance("lv").getLastFlushTimeUs());
        execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
        drainWalQueue();
        execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
        execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
        drainWalQueue();
        fault.arm(2);
        drainJob(job);
        Assert.assertTrue("the mid-drain segment read must have been failed exactly once", fault.hasFired());
    }

    private long newestGeneration(LiveViewInstance instance) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin()
        ) {
            return pin.getGeneration();
        }
    }

    private void restart() {
        engine.buildViewGraphs();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
    }

    private void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }

    private void writeRepairMarker(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            LiveViewCheckpointRepairMarker.write(
                    engine.getConfiguration(),
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    newestGeneration(instance),
                    ts("2026-01-02T00:00:00.000000Z")
            );
        }
    }
}
