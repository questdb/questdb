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
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Numbers;
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
 * live repair marker, a base schema change the view cannot restore its accumulators past in
 * place, a lost base WAL segment. Before the guard, each of them recomputed the view from the
 * surviving base rows and replaced its output - the dropped day's rows gone, silently, with
 * the view valid throughout. Now each of them stops the view instead, and the case asserts
 * that everything the rebuild would have replaced is still there.
 * <p>
 * A base schema change that can restore in place never gets that far: it puts the
 * accumulators back from the view's own timeline and keeps refreshing, with the dropped day
 * still in the view. That case is here too, because it is the refusal the restore removes;
 * the restore itself is {@link LiveViewRuntimeRestoreTest}'s subject.
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
    // Commits made after the fixture's six rows, in the order the cases below make them, and the
    // view row each one produces on top of ALL_ROWS. They extend day three, so acct-1 and acct-2
    // keep accumulating from 16.0 and 32.0.
    private static final String[] ROWS_AHEAD = {
            "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)",
            "('2026-01-03T10:10:00.000000Z', 'acct-2', 128.0)",
            "('2026-01-03T10:20:00.000000Z', 'acct-1', 256.0)",
            "('2026-01-03T10:30:00.000000Z', 'acct-2', 512.0)"
    };
    private static final String[] ROWS_AHEAD_OUTPUT = {
            "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n",
            "2026-01-03T10:10:00.000000Z\tacct-2\t160.0\t2\n",
            "2026-01-03T10:20:00.000000Z\tacct-1\t336.0\t3\n",
            "2026-01-03T10:30:00.000000Z\tacct-2\t672.0\t3\n"
    };
    // The two running doors into the whole-view rebuild, as their recoveries name themselves in
    // the log lines and the operator reasons a deferral or a refusal publishes.
    private static final String DRIFT_CAUSE = "base table metadata change";
    private static final String MID_DRAIN_CAUSE = "mid-drain refresh failure";
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
    public void testABaseSchemaChangeBehindALiveRepairMarkerIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            // A deduplicating base, because its drain reads the applied base through the
            // compiled factory, and that is where a base metadata change surfaces as drift. The
            // view has no filter, so dedup cannot drop an output row and the guard compares.
            seedSixRows("DEDUP UPSERT KEYS(created_at, account_id)");
            dropPartitionAndRefresh("2026-01-01");
            // A repair whose truncated head is not yet re-sealed. It is what keeps the drift's
            // own recovery - restoring the accumulators from the timeline in place - off the
            // timeline, and so what sends it to the whole-view rebuild.
            writeRepairMarker(instance("lv"));
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
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=base table metadata change, reason=prefix preservation repair marker present]");
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view recomputed window state from applied base");
            final LiveViewInstance instance = instance("lv");
            assertRebuildBlocked(instance, "base table metadata change");
            Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "the view holds rows from 2026-01-01T09:00:00.000000Z but the base table's earliest row is at 2026-01-02T09:00:00.000000Z"
            );

            // Nothing moved: not the rows, not the watermark, not the timeline, not the marker.
            assertViewRows(ALL_ROWS);
            Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertEquals(
                    "a refused rebuild must not retire the timeline a restart restores from",
                    generationBefore,
                    newestGeneration(instance)
            );
            Assert.assertEquals(boundariesBefore, countSealedBoundaries("lv"));
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir));
            }
            assertLiveViewsReportsTheBlock();

            // The block is not durable, and neither is it lifted by one: the restart runs the
            // recovery again, meets the same marker, and the same evidence refuses its rebuild.
            shutdown();
            restart();
            assertRebuildBlocked(instance("lv"), "prefix preservation repair marker present");
            assertViewRows(ALL_ROWS);
        });
    }

    @Test
    public void testABaseSchemaChangeRestoresInsteadOfRebuildingAndKeepsTheDayTheBaseLost() throws Exception {
        assertMemoryLeak(() -> {
            // The same drift as above, with nothing standing over the timeline. The recovery
            // restores the accumulators the recompile lost from the view's own newest root
            // rather than rebuilding the view from what its base holds today, so it never asks
            // the question the guard would have answered with a refusal.
            seedSixRows("DEDUP UPSERT KEYS(created_at, account_id)");
            dropPartitionAndRefresh("2026-01-01");

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
            capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=base table metadata change");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            capture.assertNotLogged("live view recomputed window state from applied base");
            Assert.assertEquals(
                    "no whole-view rebuild may have been asked for",
                    LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                    guard.getAbstention()
            );
            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse("the view must keep refreshing", instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals(0, instance.getCheckpointTimelineResets());
            Assert.assertFalse(instance.isInvalid());
            // The day the base lost is still in the view, and the commit that met the drift is
            // materialized on top of the accumulation the restore put back - with no restart.
            final String resumedRows = ALL_ROWS + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n";
            assertViewRows(resumedRows);

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(resumedRows);
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
    public void testALostBaseWalRederiveBehindTheViewsLeadComparesAndRefuses() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            dropPartitionAndRefresh("2026-01-01");
            final LiveViewInstance instance = instance("lv");
            final long processedBefore = instance.getLastProcessedSeqTxn();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // An un-flushed lead that runs one commit past the base's apply: the first commit
                // is applied before the view drains it, the second is not. The base's applied head
                // then sits strictly between the view's flushed watermark and its lead, which is
                // what lets the re-derive below run at all, and it pins that head.
                setCurrentMicros(instance.getLastFlushTimeUs());
                execute("INSERT INTO tx VALUES " + ROWS_AHEAD[0]);
                drainWalQueue();
                drainJob(job);
                execute("INSERT INTO tx VALUES " + ROWS_AHEAD[1]);
                drainJob(job);
                Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(processedBefore + 2, instance.getRefreshedUpToSeqTxn());
                // A third commit whose WAL segment is lost before anything applies or drains it.
                commitThroughASecondWalAndLoseIt(ROWS_AHEAD[2]);

                // The drain fails on the lost segment until the retry budget runs out, and the
                // re-derive that follows pins the base's applied head, one commit behind the lead.
                // That used to stand the guard down as a snapshot behind the view. The lead is not
                // in the view's table, though, and the re-derive drops it, so the snapshot holds
                // every commit the table does and the guard compares.
                final int drainsToExhaustTheBudget = engine.getConfiguration().getLiveViewFlushRetryMax() + 1;
                for (int i = 0; i < drainsToExhaustTheBudget; i++) {
                    failDrainOnTheLostSegment(job, processedBefore + 3);
                }
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view re-derived from the applied base after base WAL loss");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            capture.assertNotLogged("live view rebuild from the applied base waits for the base table");
            assertRebuildBlocked(instance, "base WAL segment missing");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            Assert.assertEquals(6, guard.getDurableRows());
            Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
            Assert.assertFalse("a refused re-derive must not invalidate the view", instance.isInvalid());
            // A view stopped at a running door keeps serving its in-memory lead.
            assertViewRows(ALL_ROWS + rowsAheadOutput(2));
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
    public void testARebuildAheadOfTheBaseApplyWaitsForItAndHealsACompleteBase() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                // Keeps the mid-drain recovery's restore off the timeline, so it asks for the
                // whole-view rebuild.
                writeRepairMarker(instance);
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                // Pinned where the base had applied, the rebuild would hold the view's table, which
                // has the flushed commit's row, against a snapshot that lacks it: seven rows held
                // against six reproduced, and a refusal of a rebuild that restates nothing. The
                // previous code stood the guard down there instead and ran the rebuild unchecked.
                assertRebuildDeferred(job, instance, baseApplied);
                assertViewRows(ALL_ROWS + rowsAheadOutput(2));

                // One turn after the base applies the four commits. The deferred recovery's
                // rebuild pins a snapshot holding every commit the view's table has output of, so
                // the guard compares - and finds every row the view holds reproduced.
                drainWalQueue();
                drainJob(job);
                final LiveViewRebuildRestatementGuard guard = job.rebuildRestatementGuardForTest();
                Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
                Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
                Assert.assertEquals(7, guard.getDurableRows());
                Assert.assertEquals(7, guard.getReproducedRows());
                // The cost of the wait: the rebuild commits at the base's head, four commits past
                // the point the base had applied when the fault landed, so it materializes the
                // lead's commit and the two the fault interrupted itself rather than leaving them
                // to the next drain.
                Assert.assertEquals(baseApplied + 4, instance.getLastProcessedSeqTxn());
                Assert.assertFalse(instance.isWindowStateDirty());
                // The rebuild ran, so the view no longer waits for anything, and says so.
                assertLiveViewsReportsNoRecovery(instance);
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isInvalid());
            assertViewRows(ALL_ROWS + rowsAheadOutput(4));
        });
    }

    @Test
    public void testARebuildAheadOfTheBaseApplyWaitsForItAndRefusesToDropTheOldestDay() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            dropPartitionAndRefresh("2026-01-01");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            final long generationBefore;
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                writeRepairMarker(instance);
                generationBefore = newestGeneration(instance);
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                // Pinned where the base had applied, the rebuild would have stood the guard down and
                // replaced the view with what the surviving days produce. It waits instead.
                assertRebuildDeferred(job, instance, baseApplied);
                final String deferralReason = instance.getCheckpointRecoveryReason();
                assertViewRows(ALL_ROWS + rowsAheadOutput(2));

                // The back-off only paces the retries. Once it has elapsed, a later commit's
                // notification brings the view back with the base still behind: the window-state
                // gate takes the debt, the restore declines again, and the rebuild defers again -
                // through the refresh turn's own apply-lag arm this time, with no fault counted and
                // no second log line for the same target.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                engine.getLiveViewStateStore().notifyBaseTableCommit(engine.verifyTableName("tx"), baseApplied + 4);
                drainJob(job);
                capture.drain();
                // The retry reached the gate: the restore declined a second time.
                capture.assertLoggedRE("(?s)live view cannot restore its runtime from the checkpoint timeline.*"
                        + "live view cannot restore its runtime from the checkpoint timeline");
                capture.assertOnlyOnce("live view rebuild from the applied base waits for the base table to apply what the view consumed");
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(baseApplied + 1, instance.getApplyLagDeferTargetSeqTxn());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                Assert.assertEquals(
                        LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );
                Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());
                assertViewRows(ALL_ROWS + rowsAheadOutput(2));
                // Still waiting, and still saying so with the reason the first deferral published:
                // a retry on the same target builds nothing new.
                Assert.assertTrue(instance.isCheckpointRebuildDeferred());
                Assert.assertSame(deferralReason, instance.getCheckpointRecoveryReason());

                drainWalQueue();
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            assertRebuildBlocked(instance, "mid-drain refresh failure");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            TestUtils.assertContains(
                    instance.getCheckpointRecoveryReason(),
                    "the view holds rows from 2026-01-01T09:00:00.000000Z but the base table's earliest row is at 2026-01-02T09:00:00.000000Z"
            );
            Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());
            Assert.assertEquals(generationBefore, newestGeneration(instance));
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertTrue(LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir));
            }
            assertLiveViewsReportsTheBlock();
            // A view stopped at a running door keeps serving its in-memory lead.
            assertViewRows(ALL_ROWS + rowsAheadOutput(2));
        });
    }

    @Test
    public void testARebuildWaitingOnASuspendedBaseApplyReportsTheWaitUntilTheApplyResumes() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final TableToken baseToken = engine.verifyTableName("tx");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                writeRepairMarker(instance);
                // The base's WAL apply stops with the view's table holding output of a commit it
                // never applied - an operator's SUSPEND WAL here, a failed apply in the wild. Nothing
                // on the view's side bounds the wait that follows: a deferral charges no retry, so
                // only the apply ends it.
                execute("ALTER TABLE tx SUSPEND WAL");
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                assertRebuildDeferred(job, instance, baseApplied);
                final String deferralReason = instance.getCheckpointRecoveryReason();

                // Retry after retry, the view waits and keeps saying what it waits for. Before the
                // phase existed, this stretch showed only as a lag behind the base.
                for (int i = 0; i < 3; i++) {
                    drainWalQueue();
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseApplied + 4);
                    drainJob(job);
                }
                Assert.assertTrue(engine.isWalApplySuspended(baseToken));
                Assert.assertEquals(baseApplied, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
                capture.drain();
                capture.assertOnlyOnce("live view rebuild from the applied base waits for the base table to apply what the view consumed");
                Assert.assertTrue(instance.isCheckpointRebuildDeferred());
                Assert.assertSame(deferralReason, instance.getCheckpointRecoveryReason());
                // Three retries, one wait. base_apply_wait_micros measures from the deferral that
                // opened it, not from the last retry, which is what makes a suspended base look
                // different from a base that is merely a window behind.
                assertQuery("SELECT base_apply_wait_seqtxn, base_apply_wait_micros "
                        + "FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("base_apply_wait_seqtxn\tbase_apply_wait_micros\n"
                                + (baseApplied + 1) + "\t" + (3 * CLOCK_ADVANCE_MICROS) + "\n");
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals(0, instance.getFlushRetryCount());
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());
                assertViewRows(ALL_ROWS + rowsAheadOutput(2));

                // The apply resumes and lands the four commits, which ends the wait whatever the
                // rebuild then does. Here its first run fails on the applied base's scan: the view
                // owes the recovery still, and is charged for the failure, but no longer reports a
                // wait the base has already satisfied.
                execute("ALTER TABLE tx RESUME WAL");
                drainWalQueue();
                // A fresh base reader, so the rebuild's scan opens the column the fault fails. One
                // pass of the job rather than a drain, because the turn after the failure heals.
                engine.releaseInactive();
                fault.armAppliedScan();
                job.run();
                Assert.assertTrue("the rebuild's scan must have been failed once", fault.hasAppliedScanFired());
                capture.drain();
                capture.assertLogged("live view window-state recompute failed [view=lv, cause=mid-drain refresh failure");
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(2, instance.getRefreshFaultCount());
                Assert.assertEquals(1, instance.getFlushRetryCount());
                assertLiveViewsReportsNoRecovery(instance);

                // The next turn's rebuild pins a snapshot holding all four commits, compares,
                // finds every row the view holds, and heals.
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            assertLiveViewsReportsNoRecovery(instance);
            Assert.assertFalse(instance.isWindowStateDirty());
            Assert.assertEquals(baseApplied + 4, instance.getLastProcessedSeqTxn());
            assertViewRows(ALL_ROWS + rowsAheadOutput(4));
        });
    }

    @Test
    public void testARetryThatRestoresFromTheTimelineEndsTheWaitBeforeTheBaseApplies() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final TableToken baseToken = engine.verifyTableName("tx");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                writeRepairMarker(instance);
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                assertRebuildDeferred(job, instance, baseApplied);

                // What kept the recovery off the timeline goes away while the base is still behind.
                // A retry tries the restore before the rebuild, so this one restores in place, which
                // needs nothing from the base's apply, and the wait ends without the apply landing.
                try (Path dir = checkpointsDir(instance)) {
                    LiveViewCheckpointRepairMarker.clear(engine.getConfiguration().getFilesFacade(), dir);
                }
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseApplied + 4);
                drainJob(job);

                capture.drain();
                capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=mid-drain refresh failure");
                capture.assertNotLogged("live view recomputed window state from applied base");
                Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals(
                        "no whole-view rebuild may have run",
                        LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );
                Assert.assertEquals(baseApplied, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
                Assert.assertFalse(instance.isWindowStateDirty());
                assertLiveViewsReportsNoRecovery(instance);

                driveRefreshToQuiescence(job);
            }

            Assert.assertEquals("the mid-drain fault is the one fault", 1, instance.getRefreshFaultCount());
            assertLiveViewsReportsNoRecovery(instance);
            assertViewRows(ALL_ROWS + rowsAheadOutput(4));
        });
    }

    @Test
    public void testADroppedViewStopsWaitingForTheRebuildItWasDeferring() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            final TableToken baseToken = engine.verifyTableName("tx");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                writeRepairMarker(instance);
                failMidDrainAheadOfTheBaseApply(job, fault, 1);
                assertRebuildDeferred(job, instance, baseApplied);
            }

            // The operator drops the view rather than waiting the base's apply out - the exit the
            // suspended-base case shows an operator needs, taken. DROP LIVE VIEW fences the
            // refresh worker and closes the instance on the SQL thread, and tryCloseIfDropped's
            // two clears run there under the refresh latch. Nothing else on that path clears
            // either one: close() clears neither, and no refresh turn runs again.
            //
            // Unlike the invalidation the two clears mirror, this one cannot be read through
            // live_views() - the row is gone before the clear could be reported - so the drop's
            // disposition is read off the instance, which the caller still holds.
            execute("DROP LIVE VIEW lv");
            Assert.assertTrue(instance.isDropped());
            Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, instance.getCheckpointRecoveryPhase());
            Assert.assertNull(instance.getCheckpointRecoveryReason());
            Assert.assertFalse(instance.isCheckpointRebuildDeferred());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            // A deferred rebuild is an apply-lag wait as well, so the drop ends both halves.
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferTargetSeqTxn());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferSinceUs());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferUntilUs());

            // The base is left holding the commits the view never consumed, and is a table like
            // any other once the view that lagged it is gone: its apply lands them, and nothing
            // is waiting on it.
            drainWalQueue();
            Assert.assertEquals(baseApplied + 4, engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn());
            Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
        });
    }

    @Test
    public void testABaseSchemaChangeAheadOfTheBaseApplyDefersTheRebuildUntilTheApplyInvalidatesTheView() throws Exception {
        assertMemoryLeak(() -> {
            seedSixRows("");
            final TableToken baseToken = engine.verifyTableName("tx");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                flushAheadOfTheBaseApply(job);
                // Keeps the drift's own recovery - restoring the accumulators from the timeline in
                // place - off the timeline, so it falls back to the whole-view rebuild.
                writeRepairMarker(instance);
                // The retype and the row that carries it into a fresh WAL segment are sequenced but
                // not applied. The sequencer notifies live views at COMMIT time, so the raw-WAL
                // drain reaches a segment whose 'amount' is no longer the DOUBLE the compiled
                // projection strides and bails with the drift, while the base's apply - and the
                // invalidation the retype earns there - is still behind the commit the view has
                // already flushed. That is what puts a drift in front of a rebuild the view's own
                // coordinate is ahead of; every other route to a drift reads the applied base and
                // so waits for the apply before it can drift at all.
                execute("ALTER TABLE tx ALTER COLUMN amount TYPE FLOAT");
                execute("INSERT INTO tx VALUES " + ROWS_AHEAD[1]);
                drainJob(job);

                // The drift door's own deferral arm. The rebuild it asked for would pin the base's
                // applied head, behind the commit the view's table already holds output of, so it
                // waits instead - and the wait reaches the back-off rather than escaping the turn's
                // failure handling, which has no other arm that would catch it.
                assertRebuildDeferred(job, instance, DRIFT_CAUSE, baseApplied);
                final String deferralReason = instance.getCheckpointRecoveryReason();
                Assert.assertFalse("the apply has not landed the retype yet", instance.isInvalid());
                assertViewRows(ALL_ROWS + rowsAheadOutput(1));

                // A retry once the back-off has elapsed, with the base still behind: the gate takes
                // the debt the drift left on the instance, the restore declines again, and the
                // rebuild defers again on the same target. The gate recovers any carried debt under
                // the mid-drain cause, so the retry's own line would name that one - there is no
                // second line, because the target has not moved.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseApplied + 3);
                drainJob(job);
                capture.drain();
                capture.assertOnlyOnce("live view rebuild from the applied base waits for the base table to apply what the view consumed");
                Assert.assertTrue(instance.isCheckpointRebuildDeferred());
                Assert.assertSame(deferralReason, instance.getCheckpointRecoveryReason());
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
                Assert.assertEquals("a deferral charges no retry", 0, instance.getFlushRetryCount());
                Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(
                        LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );

                // The apply lands the retype, which invalidates the view on the referenced column
                // it changed. That is where this drift was always going to end: the rebuild the
                // wait was for never runs, and a view that has stopped refreshing waits for
                // nothing, so both the phase and the two apply-wait columns clear.
                drainWalQueue();
                drainJob(job);
            }

            capture.drain();
            capture.assertNotLogged("live view recomputed window state from applied base");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertContains(instance.getInvalidationReason(), "change column type operation [column=amount]");
            Assert.assertFalse(instance.isCheckpointRebuildDeferred());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, instance.getCheckpointRecoveryPhase());
            Assert.assertNull(instance.getCheckpointRecoveryReason());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferTargetSeqTxn());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferSinceUs());
            assertQuery("SELECT view_status, checkpoint_recovery_phase, checkpoint_recovery_reason, "
                    + "base_apply_wait_seqtxn, base_apply_wait_micros "
                    + "FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_status\tcheckpoint_recovery_phase\tcheckpoint_recovery_reason\tbase_apply_wait_seqtxn\tbase_apply_wait_micros
                            invalid\t\t\tnull\tnull
                            """);
            // An invalidated view stays queryable, and the drift never let a row of the drifted
            // segment through: the view holds what it held before the retype was sequenced.
            assertViewRows(ALL_ROWS + rowsAheadOutput(1));
        });
    }

    @Test
    public void testARebuildBehindTheViewsLeadComparesAtOnceAndRefuses() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            seedSixRows("");
            fault.of(engine.verifyTableName("tx").getDirName());
            dropPartitionAndRefresh("2026-01-01");
            final LiveViewInstance instance = instance("lv");
            final long baseApplied = instance.getLastProcessedSeqTxn();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                writeRepairMarker(instance);
                // The view drains the first of three unapplied commits into its lead, so the lead
                // runs past the base's apply while its table does not. The rebuild pins where the
                // base had applied, which holds every commit the table has output of, and drops the
                // lead: the guard compares without waiting for anything.
                failMidDrainAheadOfTheBaseApply(job, fault, 0);
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains");
            capture.assertNotLogged("live view rebuild from the applied base waits for the base table");
            capture.assertNotLogged("live view rebuild from the applied base runs without the restatement guard");
            assertRebuildBlocked(instance, "mid-drain refresh failure");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            Assert.assertEquals(6, guard.getDurableRows());
            Assert.assertEquals(baseApplied, instance.getLastProcessedSeqTxn());
            assertViewRows(ALL_ROWS + rowsAheadOutput(1));
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

    /**
     * The operator text a deferred rebuild publishes, waiting for the base to apply
     * {@code rebuildSeqTxn} on behalf of the recovery {@code cause} names.
     */
    private static String deferralReason(String cause, long rebuildSeqTxn) {
        return "rebuilding the view from its base table waits for the base table to apply what the view consumed "
                + "[cause=" + cause + ", baseTable=tx, rebuildSeqTxn=" + rebuildSeqTxn + "]: the view's table "
                + "holds output of base commits the base table has not applied yet, and nothing has moved. Refresh "
                + "resumes on its own once the base table applies seqTxn " + rebuildSeqTxn + "; a base table whose WAL "
                + "apply is suspended (see wal_tables()) keeps the view waiting until the apply resumes";
    }

    /**
     * The view rows the first {@code count} of {@link #ROWS_AHEAD} produce, in order.
     */
    private static String rowsAheadOutput(int count) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(ROWS_AHEAD_OUTPUT[i]);
        }
        return sb.toString();
    }

    /**
     * Asserts a view whose deferred rebuild ran, or whose recovery otherwise finished, reports no
     * recovery at all: both recovery columns NULL beside an {@code active} status.
     */
    private void assertLiveViewsReportsNoRecovery(LiveViewInstance instance) throws Exception {
        Assert.assertFalse(instance.isCheckpointRebuildDeferred());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.NONE, instance.getCheckpointRecoveryPhase());
        Assert.assertNull(instance.getCheckpointRecoveryReason());
        // The wait the rebuild was in goes with the phase: a view that owes nothing waits for
        // nothing, so the two base_apply_wait_* columns read NULL beside the two recovery ones.
        Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferSinceUs());
        Assert.assertEquals(Numbers.LONG_NULL, instance.getApplyLagDeferTargetSeqTxn());
        assertQuery("SELECT view_status, checkpoint_recovery_phase, invalidation_reason, checkpoint_recovery_reason, "
                + "base_apply_wait_seqtxn, base_apply_wait_micros "
                + "FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("""
                        view_status\tcheckpoint_recovery_phase\tinvalidation_reason\tcheckpoint_recovery_reason\tbase_apply_wait_seqtxn\tbase_apply_wait_micros
                        active\t\t\t\tnull\tnull
                        """);
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

    /**
     * Asserts the whole-view rebuild a mid-drain recovery asked for waited for the base's apply
     * instead of running: nothing pinned a snapshot, nothing was refused, rebuilt or charged to the
     * retry budget, and the window-state debt stands on the instance behind an apply-lag back-off
     * that names the commit the view flushed past the base's applied head. The wait is reported:
     * the view is not stopped, so {@code live_views()} keeps it {@code active} and says what it
     * waits for through the two recovery columns alone.
     */
    private void assertRebuildDeferred(LiveViewRefreshJob job, LiveViewInstance instance, long baseApplied) throws Exception {
        assertRebuildDeferred(job, instance, MID_DRAIN_CAUSE, baseApplied);
    }

    /**
     * The same, for a deferral a named recovery asked for. Both running doors reach the rebuild
     * the same way, so both report the same pair of columns under the same phase; only the cause
     * the reason and the log line carry tells them apart.
     */
    private void assertRebuildDeferred(
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            String cause,
            long baseApplied
    ) throws Exception {
        capture.drain();
        capture.assertLogged("live view rebuild from the applied base waits for the base table to apply what the view consumed "
                + "[view=lv, cause=" + cause + ", rebuildSeqTxn=" + (baseApplied + 1)
                + ", appliedSeqTxn=" + baseApplied + "]");
        capture.assertNotLogged("live view recomputed window state from applied base");
        Assert.assertEquals(
                "no whole-view rebuild may have pinned a snapshot",
                LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                job.rebuildRestatementGuardForTest().getAbstention()
        );
        Assert.assertTrue("the recovery the rebuild owes must carry to a later turn", instance.isWindowStateDirty());
        Assert.assertEquals(baseApplied + 1, instance.getApplyLagDeferTargetSeqTxn());
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse(instance.isInvalid());
        Assert.assertEquals("the fault that asked for the recovery is the one fault", 1, instance.getRefreshFaultCount());
        Assert.assertEquals("a deferral charges no retry", 0, instance.getFlushRetryCount());
        Assert.assertEquals(baseApplied + 1, instance.getLastProcessedSeqTxn());

        Assert.assertTrue("the wait must be reported", instance.isCheckpointRebuildDeferred());
        Assert.assertEquals(LiveViewCheckpointRecoveryPhase.REBUILD_DEFERRED, instance.getCheckpointRecoveryPhase());
        Assert.assertEquals(deferralReason(cause, baseApplied + 1), instance.getCheckpointRecoveryReason());
        // A deferred rebuild is an apply-lag wait like any other, so it reports through the two
        // base_apply_wait_* columns as well: the seqTxn the phase's reason names, and a duration
        // the frozen test clock pins to the stamp the deferral took.
        Assert.assertEquals(currentMicros, instance.getApplyLagDeferSinceUs());
        assertQuery("SELECT view_status, checkpoint_recovery_phase, invalidation_reason, checkpoint_recovery_reason, "
                + "base_apply_wait_seqtxn, base_apply_wait_micros "
                + "FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tcheckpoint_recovery_phase\tinvalidation_reason\tcheckpoint_recovery_reason\t"
                        + "base_apply_wait_seqtxn\tbase_apply_wait_micros\n"
                        + "active\trebuild_deferred\t\t" + deferralReason(cause, baseApplied + 1) + "\t"
                        + (baseApplied + 1) + "\t0\n");
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

    /**
     * Commits {@link #ROWS_AHEAD} {@code first} to {@code first + 2}, which the base table does not
     * apply yet, and has the view fail mid-drain over them. A view over a base without dedup keys
     * drains the raw WAL, so it runs ahead of the base's own apply: the first commit gets a refresh
     * task of its own and lands in the view's un-flushed lead, the next two coalesce behind it, and
     * the fault fails that pass's read of the third commit after the second has been fed. The
     * recovery that follows owes the view its accumulators while the base has applied none of the
     * three.
     * <p>
     * The clock stays on the view's last flush, so the lead is not flushed; the caller drives
     * everything after the fault.
     */
    private void failMidDrainAheadOfTheBaseApply(LiveViewRefreshJob job, LiveViewMidDrainFault fault, int first) throws Exception {
        setCurrentMicros(instance("lv").getLastFlushTimeUs());
        for (int i = first; i < first + 3; i++) {
            execute("INSERT INTO tx VALUES " + ROWS_AHEAD[i]);
        }
        fault.arm(2);
        drainJob(job);
        Assert.assertTrue("the mid-drain segment read must have been failed exactly once", fault.hasFired());
    }

    /**
     * Commits {@link #ROWS_AHEAD}'s first row, which the base table does not apply, and has the view
     * drain it from raw WAL and flush it, with the clock past {@code FLUSH EVERY}. The view's table
     * then holds output of a commit the base has not applied, which is what puts the view's own
     * coordinate past the base's applied head.
     */
    private void flushAheadOfTheBaseApply(LiveViewRefreshJob job) throws Exception {
        final LiveViewInstance instance = instance("lv");
        final long baseApplied = instance.getLastProcessedSeqTxn();
        setCurrentMicros(instance.getLastFlushTimeUs() + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO tx VALUES " + ROWS_AHEAD[0]);
        drainJob(job);
        Assert.assertEquals(
                "the view must have flushed a commit the base has not applied",
                baseApplied + 1,
                instance.getLastProcessedSeqTxn()
        );
        Assert.assertEquals(baseApplied, engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("tx")).getWriterTxn());
    }

    /**
     * Re-publishes the base's head commit and drives the refresh, which is what a later commit
     * notification looks like to the view: it drains from its lead up to the lost segment and fails
     * there. The fallback scan would not retry the drain, because it drives a view only as far as
     * the base has applied, and here the base's apply is at or behind the lead.
     */
    private void failDrainOnTheLostSegment(LiveViewRefreshJob job, long baseHead) {
        engine.getLiveViewStateStore().notifyBaseTableCommit(engine.verifyTableName("tx"), baseHead);
        drainJob(job);
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
     * Commits one row to the base through a WAL of its own - the insert takes a second WAL writer
     * while the test holds the first - and then removes that WAL, so this commit alone is lost to
     * everything that would read it: the view's drain and the base's own apply. Every earlier
     * commit stays readable in the first WAL.
     */
    private void commitThroughASecondWalAndLoseIt(String values) throws Exception {
        final TableToken baseToken = engine.verifyTableName("tx");
        try (WalWriter held = engine.getWalWriter(baseToken)) {
            Assert.assertEquals("every earlier commit must sit in the first WAL", 1, held.getWalId());
            execute("INSERT INTO tx VALUES " + values);
        }
        engine.releaseInactive();
        final File secondWal = new File(
                new File(engine.getConfiguration().getDbRoot(), baseToken.getDirName()),
                WalUtils.WAL_NAME_BASE + 2
        );
        Assert.assertTrue("the insert must have taken a second WAL", secondWal.isDirectory());
        try (Path p = new Path()) {
            p.of(secondWal.getAbsolutePath());
            Assert.assertTrue("could not remove " + secondWal, engine.getConfiguration().getFilesFacade().rmdir(p));
        }
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
