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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * The restatement guard in front of the whole-view rebuild from the applied base: what it
 * refuses, what it lets through, and what a refused rebuild leaves behind.
 * <p>
 * Every refusal case starts the same way. A base partition the view has already derived
 * rows from is dropped, and the incremental path walks past the DROP PARTITION as it always
 * has - the view keeps its rows for that day, which is the frozen-prefix contract. Then a
 * route into the whole-view rebuild opens: a restart with no timeline, a restart behind a
 * live repair marker, a base schema change, a lost base WAL segment. Before the guard, each
 * of them recomputed the view from the surviving base rows and replaced its output - the
 * dropped day's rows gone, silently, with the view valid throughout. Now each of them stops
 * the view instead, and the case asserts that everything the rebuild would have replaced is
 * still there.
 * <p>
 * The two checks are witnessed apart. Dropping the OLDEST day moves the base's earliest row
 * above the view's, which the history floor sees before the rebuild reads a row. Dropping a
 * MIDDLE day leaves the base's earliest row where it was, so only the scan's row count can
 * see it. {@link LiveViewRebuildRestatementGuard#getVerdict()} names which one fired.
 * <p>
 * The pass cases matter as much: a rebuild over a base that still holds every row has to go
 * ahead exactly as before, and so does one whose backlog legitimately removes a row - both
 * are rebuilds that heal, and a guard that stopped them would trade one silent failure for a
 * loud one nobody asked for.
 */
public class LiveViewRebuildRestatementGuardTest extends AbstractLiveViewCheckpointCompatTest {
    // What the view holds once the fixture's six rows are in. ANCHOR DAILY resets each
    // account's accumulators at midnight, so day 2 carries acct-1 to 12.0 / 2.
    private static final String ALL_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    private static final String VIEW_ROWS_QUERY = "SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv";
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so the timeline a refusal must preserve holds a
        // ladder rather than a single root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testABaseSchemaChangeRebuildIsRefusedAndTheRestartResumesFromTheTimeline() throws Exception {
        assertMemoryLeak(() -> {
            // A deduplicating base, because its drain reads the applied base through the
            // compiled factory, and that is where a base metadata change surfaces as drift. The
            // view has no filter, so dedup cannot drop an output row and the guard compares.
            seedSixRows("DEDUP UPSERT KEYS(created_at, account_id)");
            dropPartitionAndRefresh("2026-01-01");
            final int boundariesBefore = countSealedBoundaries("lv");
            final long generationBefore = newestGeneration(instance("lv"));
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();

            // A schema change the view survives: the column is one it never reads. The raw WAL
            // drain resolves columns by name and would absorb it, so the commit after it carries
            // two rows the base collapses into one. That dedup is what routes the drain through
            // the applied base, whose reader the view's compiled plan now predates: the drain
            // meets the drift, recompiles and asks for the whole-view rebuild.
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

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view recomputed window state from applied base");
            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "base table metadata change");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "the view holds rows from 2026-01-01T09:00:00.000000Z but the base table's earliest row is at 2026-01-02T09:00:00.000000Z"
            );

            // Nothing moved: not the rows, not the watermark, not the timeline. The timeline is the
            // one thing that has to survive, because it is the way back - the same generation,
            // addressing the same ladder, rather than the retire a whole-view rebuild owes.
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertEquals(
                    "a refused rebuild must not retire the timeline a restart restores from",
                    generationBefore,
                    newestGeneration(instance)
            );
            Assert.assertEquals(boundariesBefore, countSealedBoundaries("lv"));
            assertLiveViewsReportsTheBlock();

            // The block is not durable. A restart takes the ordinary restore off the preserved
            // ladder - no rebuild, so no refusal - and the view resumes with the day the base
            // lost still in it, consuming the commit that arrived while it was stopped.
            shutdown();
            restart();
            final LiveViewInstance resumed = instance("lv");
            Assert.assertFalse(resumed.isCheckpointRecoveryBlocked());
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(ALL_ROWS + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");
        });
    }

    @Test
    public void testALostBaseWalRederiveIsRefusedRatherThanInvalidated() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            // The base moves on while the view does not see it: a commit and the loss of the
            // oldest day, both applied to the base table only. This is the lag a backup
            // captures, and its restore brings back the table without the WAL that carried it.
            execute("INSERT INTO tx VALUES ('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();
            shutdown();
            removeBaseWal();

            final LiveViewRebuildRestatementGuard guard = restart();

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view re-derived from the applied base after base WAL loss");
            final LiveViewInstance instance = instance("lv");
            // The re-derive is the last step before a durable invalidation. A refusal is not a
            // failure, so the view stops without being invalidated, and the restart the operator
            // runs after bringing the WAL back takes it up again.
            assertRebuildBlocked(instance, "base WAL segment missing");
            // The backlog the re-derive folds in is exactly what cannot be read. A base that is
            // neither a materialized view nor deduplicating under a filter has no commit there
            // that could remove a row legitimately, so the guard compares anyway - and refuses
            // on the floor, before the re-derive reads a row.
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
            assertLiveViewsReportsTheBlock();
        });
    }

    @Test
    public void testARebuildOverACompleteBasePassesTheGuard() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            shutdown();
            removeTimeline();

            final LiveViewRebuildRestatementGuard guard = restart();

            // The rebuild that heals a lost timeline over a base that still has every row goes
            // ahead exactly as it did before the guard - compared, and found to reproduce every
            // row the view held.
            assertRebuiltFromAppliedBase("lv");
            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
            Assert.assertEquals(6, guard.getDurableRows());
            Assert.assertEquals(6, guard.getReproducedRows());
            assertNoRefreshFaults("lv");
            assertViewRows(ALL_ROWS);
            capture.drain();
            capture.assertNotLogged("live view rebuild from the applied base refused");
        });
    }

    @Test
    public void testARestartBehindARepairMarkerRefusesARebuildThatWouldDropAMiddleDay() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            // The MIDDLE day: the base's earliest row stays where it was, so the history floor
            // has nothing to see and only the recompute's own row count can.
            dropPartitionAndRefresh("2026-01-02");
            // A crash in the middle of a prefix-preserving repair leaves this marker, and the
            // restart that finds it live rebuilds rather than trust the timeline under it.
            writeRepairMarker(instance("lv"));
            final long generationBefore = newestGeneration(instance("lv"));
            final int boundariesBefore = countSealedBoundaries("lv");
            shutdown();

            final LiveViewRebuildRestatementGuard guard = restart();

            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "prefix preservation repair marker present");
            Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute()));
            Assert.assertEquals(1, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_ROW_SHORTFALL, guard.getVerdict());
            Assert.assertEquals(4, guard.getReproducedRows());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "the rebuild reproduces 4 of the 6 rows the view holds up to 2026-01-03T09:10:00.000000Z"
            );
            // The refusal came after the scan, with the replacement staged in the view's WAL
            // writer: closing the writer rolled it back, and the retire the rebuild owed never
            // ran, so the marker and the timeline under it are exactly as the crash left them.
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(generationBefore, newestGeneration(instance));
            Assert.assertEquals(boundariesBefore, countSealedBoundaries("lv"));
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertTrue(
                        "the marker must survive for the next restart to meet",
                        LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir)
                );
            }
            assertNoRefreshFaults("lv");
            capture.drain();
            capture.assertNotLogged("live view O3 head-miss replay completed");
        });
    }

    @Test
    public void testARestartWithNoTimelineRefusesARebuildThatWouldDropTheOldestDay() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            assertViewRows(ALL_ROWS);
            final long processedBefore = instance("lv").getLastProcessedSeqTxn();
            shutdown();
            // A restart with no timeline to restore from, which is what a directory reset for its
            // format, a history-epoch replacement or a timeline an earlier failure retired leaves.
            removeTimeline();

            final LiveViewRebuildRestatementGuard guard = restart();

            // Refused before the rebuild read a row: the view's first row is older than anything
            // the base still holds, which two transaction-file reads are enough to see.
            capture.drain();
            capture.assertLogged("live view restart rebuilding from applied base");
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view O3 head-miss replay completed");
            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "timeline is absent");
            Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute()));
            Assert.assertFalse(
                    "a refused rebuild resolved no derived state, so it must not report success",
                    instance.isCheckpointRestoreSucceeded()
            );
            Assert.assertEquals(1, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            Assert.assertEquals("the floor needs no scan", 0, guard.getReproducedRows());
            assertViewRows(ALL_ROWS);
            assertLiveViewsReportsTheBlock();

            // Refresh is stopped, not merely the restore: a base commit moves neither the rows
            // nor the watermark, and nothing faults.
            execute("INSERT INTO tx VALUES ('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(processedBefore, instance("lv").getLastProcessedSeqTxn());
            assertNoRefreshFaults("lv");

            // Nothing on disk records the block, and nothing needs to: the next restart asks the
            // same question of the same evidence.
            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "timeline is absent");
            assertViewRows(ALL_ROWS);

            // The operator's exit is a deliberate recomputation from what the base holds today.
            execute("DROP LIVE VIEW lv");
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
                    2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
                    2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2
                    """);
        });
    }

    @Test
    public void testATurnedOffGuardLetsTheRebuildFollowTheBase() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            shutdown();
            removeTimeline();
            // The escape hatch: an operator who would rather the view track the base's retention
            // than stop gets the rebuild every release before the guard ran.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_REBUILD_RESTATEMENT_GUARD_ENABLED, "false");

            final LiveViewRebuildRestatementGuard guard = restart();

            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_DISABLED, guard.getAbstention());
            // The restatement the guard exists to refuse, now asked for: the dropped day's rows
            // are gone from the view.
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
                    2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
                    """);
        });
    }

    @Test
    public void testABacklogThatLegitimatelyRemovesARowIsNotRefused() throws Exception {
        assertMemoryLeak(() -> {
            // A deduplicating base under a filtering view: a replacement that fails the filter
            // takes the replaced row out of the view, and incremental refresh would propagate
            // exactly that. So a rebuild whose snapshot holds such a replacement the view has not
            // consumed yet reproduces fewer rows, and is right to.
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts "
                    + "RANGE BETWEEN '9' MINUTE PRECEDING AND CURRENT ROW) AS v FROM base WHERE i > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                execute("INSERT INTO base (ts, sym, i) VALUES "
                        + "('2026-01-01T00:01:00.000000Z', 'a', 297), "
                        + "('2026-01-01T00:05:00.000000Z', 'a', 500), "
                        + "('2026-01-01T00:09:00.000000Z', 'a', 900)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            // The replacement: applied to the base, never refreshed into the view.
            execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:01:00.000000Z', 'a', -108)");
            drainWalQueue();
            shutdown();
            removeTimeline();

            final LiveViewRebuildRestatementGuard guard = restart();

            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertEquals(
                    "a backlog commit that can legitimately remove an output row must stand the guard down",
                    LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                    guard.getAbstention()
            );
            // An unchecked rebuild over rows the view could lose says so, so a restatement found
            // later has a line explaining why nothing stopped it.
            capture.drain();
            capture.assertLogged("live view rebuild from the applied base runs without the restatement guard");
            assertQuery("SELECT ts, sym, i FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsym\ti
                            2026-01-01T00:05:00.000000Z\ta\t500
                            2026-01-01T00:09:00.000000Z\ta\t900
                            """);
        });
    }

    @Test
    public void testGuardAbstentionsCompareNothing() {
        final LiveViewRebuildRestatementGuard guard = new LiveViewRebuildRestatementGuard();
        Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED, guard.getAbstention());
        final int[] abstentions = {
                LiveViewRebuildRestatementGuard.ABSTAIN_DISABLED,
                LiveViewRebuildRestatementGuard.ABSTAIN_NOTHING_RETAINED,
                LiveViewRebuildRestatementGuard.ABSTAIN_SNAPSHOT_BEHIND,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_MAY_REMOVE,
                LiveViewRebuildRestatementGuard.ABSTAIN_BACKLOG_UNREADABLE
        };
        for (int abstention : abstentions) {
            // Armed over evidence both checks would refuse, then stood down: nothing it held
            // survives the disarm, and nothing it is shown afterwards counts.
            guard.arm(10, ts("2026-01-01T00:00:00.000000Z"), ts("2026-01-02T00:00:00.000000Z"), 0, 0);
            guard.disarm(abstention);
            guard.observe(ts("2026-01-01T12:00:00.000000Z"));
            Assert.assertEquals(abstention, guard.getAbstention());
            Assert.assertFalse(guard.isHistoryFloorBreached());
            Assert.assertFalse(guard.isRowShortfall());
            Assert.assertEquals(0, guard.getReproducedRows());
            Assert.assertNotEquals("not evaluated", LiveViewRebuildRestatementGuard.abstentionName(abstention));
        }
    }

    @Test
    public void testGuardHistoryFloorIsStrictAndCoversAnEmptyBase() {
        final LiveViewRebuildRestatementGuard guard = new LiveViewRebuildRestatementGuard();
        final long viewMin = ts("2026-01-01T09:00:00.000000Z");
        final long viewMax = ts("2026-01-03T09:00:00.000000Z");

        // A base row AT the view's earliest timestamp may be the one that produced it.
        guard.arm(6, viewMin, viewMax, 4, viewMin);
        Assert.assertFalse(guard.isHistoryFloorBreached());
        guard.arm(6, viewMin, viewMax, 4, viewMin - 1);
        Assert.assertFalse(guard.isHistoryFloorBreached());
        guard.arm(6, viewMin, viewMax, 4, viewMin + 1);
        Assert.assertTrue(guard.isHistoryFloorBreached());

        // An empty base has no earliest row to compare against, and every row the view holds is
        // below its floor - whatever the minimum the reader reports for no rows.
        guard.arm(6, viewMin, viewMax, 0, Long.MIN_VALUE);
        Assert.assertTrue(guard.isHistoryFloorBreached());
        guard.refuse(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR);
        final StringSink sink = new StringSink();
        guard.appendEvidence(sink, MicrosTimestampDriver.INSTANCE);
        TestUtils.assertEquals("the view holds rows from 2026-01-01T09:00:00.000000Z but the base table holds no rows", sink);
    }

    @Test
    public void testGuardRowShortfallCountsOnlyRowsAtOrBelowTheFrontier() {
        final LiveViewRebuildRestatementGuard guard = new LiveViewRebuildRestatementGuard();
        final long viewMax = ts("2026-01-03T09:00:00.000000Z");
        guard.arm(3, ts("2026-01-01T09:00:00.000000Z"), viewMax, 3, ts("2026-01-01T09:00:00.000000Z"));

        guard.observe(ts("2026-01-01T09:00:00.000000Z"));
        guard.observe(viewMax);
        // Above the frontier: a row from a base transaction the view had not consumed. It adds
        // to the recompute, not to what the recompute reproduces of the view.
        guard.observe(viewMax + 1);
        Assert.assertEquals(2, guard.getReproducedRows());
        Assert.assertTrue(guard.isRowShortfall());
        guard.refuse(LiveViewRebuildRestatementGuard.VERDICT_ROW_SHORTFALL);
        final StringSink sink = new StringSink();
        guard.appendEvidence(sink, MicrosTimestampDriver.INSTANCE);
        TestUtils.assertEquals("the rebuild reproduces 2 of the 3 rows the view holds up to 2026-01-03T09:00:00.000000Z", sink);

        guard.observe(viewMax);
        Assert.assertFalse("a recompute that reproduces every row is no shortfall", guard.isRowShortfall());
    }

    /**
     * Asserts the disposition every refusal leaves: a stopped view that is not a durable
     * invalidation, blocked on the rebuild phase with a reason naming the route that asked for
     * the rebuild and the operator's ways out.
     */
    private static void assertRebuildBlocked(LiveViewInstance instance, String cause) {
        Assert.assertTrue("the view must be stopped", instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse("a rebuild block is not a format block", instance.isCheckpointFormatBlocked());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_BLOCKED, instance.getCheckpointRecoveryPhase());
        Assert.assertFalse("a refused rebuild must not write _lv.s.invalid", instance.isInvalid());
        final String reason = instance.getCheckpointRecoveryReason();
        TestUtils.assertContains(reason, "rebuilding the view from its base table would drop rows it retains [cause=" + cause + "]");
        TestUtils.assertContains(reason, "DROP and re-create the view");
        TestUtils.assertContains(reason, "cairo.live.view.rebuild.restatement.guard.enabled=false");
    }

    private void assertLiveViewsReportsTheBlock() throws Exception {
        assertQuery("SELECT view_status, checkpoint_recovery_phase, "
                + "invalidation_reason = checkpoint_recovery_reason AS reason_mirrored "
                + "FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_status\tcheckpoint_recovery_phase\treason_mirrored
                        invalid\trebuild_blocked\ttrue
                        """);
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery(VIEW_ROWS_QUERY)
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
    }

    private void createView() throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    /**
     * Drops one base partition the view has already derived rows from, and lets the view walk
     * past the DROP PARTITION the way it always has: it keeps those rows.
     */
    private void dropPartitionAndRefresh(String day) throws Exception {
        execute("ALTER TABLE tx DROP PARTITION LIST '" + day + "'");
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
        assertViewRows(ALL_ROWS);
        assertNoRefreshFaults("lv");
    }

    private long newestGeneration(LiveViewInstance instance) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin()
        ) {
            return pin.getGeneration();
        }
    }

    /**
     * Removes every WAL directory of the base table, which is what a restore that captured the
     * applied table and not its WAL leaves behind.
     */
    private void removeBaseWal() {
        final TableToken baseToken = engine.verifyTableName("tx");
        final File baseDir = new File(engine.getConfiguration().getDbRoot(), baseToken.getDirName());
        final File[] walDirs = baseDir.listFiles(f -> f.isDirectory() && f.getName().startsWith(WalUtils.WAL_NAME_BASE));
        Assert.assertNotNull(walDirs);
        Assert.assertTrue("the base must have a WAL to lose", walDirs.length > 0);
        for (File walDir : walDirs) {
            try (Path p = new Path()) {
                p.of(walDir.getAbsolutePath());
                Assert.assertTrue("could not remove " + walDir, engine.getConfiguration().getFilesFacade().rmdir(p));
            }
        }
    }

    /**
     * Removes the view's {@code _timeline}, leaving the segments under it for the catalogue
     * load's orphan sweep. The restart then finds no timeline and asks for the rebuild.
     */
    private void removeTimeline() {
        final File timeline = new File(checkpointsRootByDirName(), LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        Assert.assertTrue("the fixture must have published a timeline to remove", timeline.delete());
    }

    private File checkpointsRootByDirName() {
        final TableToken viewToken = engine.verifyTableName("lv");
        return new File(
                new File(engine.getConfiguration().getDbRoot(), viewToken.getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
    }

    /**
     * Rebuilds the view registry from disk and drives the first refresh turns, which is where a
     * restart runs its recovery. Returns what the last whole-view rebuild's guard found.
     */
    private LiveViewRebuildRestatementGuard restart() {
        engine.buildViewGraphs();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
            // A plain heap object the job keeps no native resource in, so it stays readable
            // after the job closes.
            return job.rebuildRestatementGuardForTest();
        }
    }

    /**
     * Six rows over three days, one commit each, so the timeline holds one boundary per row and
     * every base partition holds rows the view has derived output from.
     */
    private void seedSixRows(String dedupClause) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL " + dedupClause);
        createView();
        final String[] rows = {
                "'2026-01-01T09:00:00.000000Z', 'acct-1', 1.0",
                "'2026-01-01T09:10:00.000000Z', 'acct-2', 2.0",
                "'2026-01-02T09:00:00.000000Z', 'acct-1', 4.0",
                "'2026-01-02T09:10:00.000000Z', 'acct-1', 8.0",
                "'2026-01-03T09:00:00.000000Z', 'acct-1', 16.0",
                "'2026-01-03T09:10:00.000000Z', 'acct-2', 32.0"
        };
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (String row : rows) {
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES (" + row + ")");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        }
        assertViewRows(ALL_ROWS);
        assertNoRefreshFaults("lv");
        Assert.assertEquals("one boundary per commit", 6, countSealedBoundaries("lv"));
    }

    /**
     * Releases everything that maps the view's and the base's files, the way a stopped process
     * would, so the next {@link #restart()} starts from disk.
     */
    private void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }

    /**
     * Stamps the durable marker a prefix-preserving repair writes before it truncates, over the
     * generation on disk, so the restart reads it as a repair that crashed rather than one a
     * later seal made stale.
     */
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
