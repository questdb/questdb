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
 * <p>
 * Two of the cases - the ones that end in a parked repair - cover what a restore owes the turn it
 * runs in rather than what it brings back. A replay that meets an unresolved out-of-order commit
 * hands off to the out-of-order repair, and a localized repair there can park on the refresh
 * turn's budget - at which point it owns the runtime, and the turn has to end on it rather than
 * drain through accumulators the parked replay is standing half-way through. The refresh turn
 * checks for that twice, once after the restart restore and once after the running one, and the
 * two cases take one door each.
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
    // Day four's first row, the one the restart case leaves in the base unconsumed so the drain
    // the parked repair's check suppresses has work waiting behind it.
    private static final String DAY_FOUR_FIRST_ROW_OUTPUT =
            "2026-01-04T09:00:00.000000Z\tacct-1\t1.0\t1\n";
    private static final String DAY_FOUR_OUTPUT = DAY_FOUR_FIRST_ROW_OUTPUT
            + "2026-01-04T09:10:00.000000Z\tacct-1\t3.0\t2\n"
            + "2026-01-04T09:20:00.000000Z\tacct-1\t7.0\t3\n";
    // One commit per entry, and the last of them is the whole point: its rows are not in
    // timestamp order, every one of them sits above the frontier the commit before it left, and
    // two of them collide on the base's dedup keys.
    //
    // The collision is what routes the drain through the applied base rather than the raw WAL,
    // and the applied base's reader yields rows in timestamp order - so the view consumes the
    // commit with no out-of-order repair, and the default cadence seals no root over it. The raw
    // WAL under it still holds those rows in the order they arrived, which is what a later
    // restore's replay of the gap reads.
    private static final String[] O3_IN_THE_REPLAY_GAP = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-03T09:00:00.000000Z', 'acct-1', 8.0), ('2026-01-03T09:10:00.000000Z', 'acct-1', 16.0), "
                    + "('2026-01-03T09:20:00.000000Z', 'acct-1', 32.0)",
            "('2026-01-03T09:50:00.000000Z', 'acct-1', 64.0), ('2026-01-03T09:50:00.000000Z', 'acct-1', 65.0), "
                    + "('2026-01-03T09:40:00.000000Z', 'acct-1', 128.0)"
    };
    private static final String O3_GAP_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-03T09:00:00.000000Z\tacct-1\t8.0\t1
            2026-01-03T09:10:00.000000Z\tacct-1\t24.0\t2
            2026-01-03T09:20:00.000000Z\tacct-1\t56.0\t3
            2026-01-03T09:40:00.000000Z\tacct-1\t184.0\t4
            2026-01-03T09:50:00.000000Z\tacct-1\t249.0\t5
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
    public void testAGateRestoreWhoseParkedRepairEndsTheTurn() throws Exception {
        // The same disposition reached from the running door. The gate every turn opens at once
        // the view owes its accumulators a recovery runs the same restore, over the same replay
        // gap, and parks the same repair - so it ends its turn the same way.
        //
        // Both recoveries of the failing turn have to fail for the debt to reach a turn of its
        // own: a restore that succeeded would settle it, and so would the rebuild behind it. The
        // failures are one-shot, so the gate turn that follows them runs against an intact tree.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(O3_IN_THE_REPLAY_GAP);
            assertViewRows(O3_GAP_OUTPUT);
            final LiveViewInstance instance = instance("lv");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Commits the collapse above left provably clean, so the view drains them through
                // the raw WAL and the fault can strike between two of them. The first goes in on
                // its own turn; the next two coalesce behind it and drain in one pass, which is
                // what puts the failure after a row this turn has already fed.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:00:00.000000Z', 'acct-1', 1.0)");
                drainWalQueue();
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:10:00.000000Z', 'acct-1', 2.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:20:00.000000Z', 'acct-1', 4.0)");
                drainWalQueue();
                runOnePass(job);
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);

                fault.arm(1);
                fault.armTimelineOpen();
                fault.armAppliedScan();
                runOnePass(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertFalse("the recovery's restore must have been failed", fault.isTimelineOpenArmed());
                Assert.assertTrue("the recovery's rebuild must have been failed", fault.hasAppliedScanFired());
                Assert.assertTrue(
                        "the failed recovery must leave the window-state debt for the next turn",
                        instance.isWindowStateDirty()
                );
                Assert.assertNull("nothing may park while both recoveries fail", instance.getSuspendedRepair());
                Assert.assertEquals(
                        "the failed restore must have brought nothing back",
                        0,
                        instance.getCheckpointRuntimeRestores()
                );
                final long watermarkBeforeTheGate = instance.getLastProcessedSeqTxn();

                // The gate turn. Its restore runs now that nothing fails, meets the same
                // out-of-order commit in the replay gap, and parks the repair it hands off to.
                runOnePass(job);
                Assert.assertNotNull(
                        "the gate's restore must leave the repair it handed off to parked on the view",
                        instance.getSuspendedRepair()
                );
                capture.drain();
                capture.assertLoggedRE("live view O3 replay \\[view=lv, lateRowTs=");
                capture.assertLoggedRE("live view O3 repair yielded on its turn budget \\[view=lv, turns=1,");
                Assert.assertEquals(
                        "the repair must have come out of the gate's own in-process restore",
                        1,
                        instance.getCheckpointRuntimeRestores()
                );
                Assert.assertEquals(
                        "the parked repair owns the runtime, so the gate must not have let the drain run",
                        watermarkBeforeTheGate,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertTrue(
                        "the debt belongs to the repair until it finishes",
                        instance.isWindowStateDirty()
                );
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);

                driveRefreshToQuiescence(job);
            }

            // The repair finishes across the turns after it and the view converges on every row,
            // the three commits the fault interrupted included.
            Assert.assertNull(instance.getSuspendedRepair());
            Assert.assertFalse(instance.isWindowStateDirty());
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals("the injected mid-drain failure is the one fault", 1, instance.getRefreshFaultCount());
            assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_OUTPUT);
        });
    }

    @Test
    public void testARestartRestoreWhoseParkedRepairEndsTheTurn() throws Exception {
        // A restore's replay walks the base WAL above the root it came back on, and that WAL is
        // raw: a commit whose own rows are not in timestamp order corrupts the accumulators if it
        // is fed in WAL order, so the replay hands off to the out-of-order repair. A localized
        // repair there parks on the refresh turn's budget like any other, and it owns the runtime
        // from that point - so the turn has to end on it. The drain below it would otherwise feed
        // rows through accumulators the parked replay is standing half-way through.
        //
        // The base deduplicates, which is what puts such a commit in the gap at all. Its drain
        // reads the applied base, whose reader yields rows in timestamp order, so a commit that is
        // out of order only within itself and entirely above the frontier is consumed with no
        // repair and no root sealed over it. The raw WAL under it still holds the rows unsorted.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(O3_IN_THE_REPLAY_GAP);
            Assert.assertEquals("the default cadence seals the first boundary only", 1, countSealedBoundaries("lv"));
            assertViewRows(O3_GAP_OUTPUT);
            final long gapWatermark = instance("lv").getLastProcessedSeqTxn();

            // One commit the view does not consume, so the drain the check suppresses has work of
            // its own waiting behind it.
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:00:00.000000Z', 'acct-1', 1.0)");
            drainWalQueue();

            shutdown();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                runOnePass(job);
                final LiveViewInstance instance = instance("lv");
                Assert.assertNotNull(
                        "the restart restore must leave the repair it handed off to parked on the view",
                        instance.getSuspendedRepair()
                );
                capture.drain();
                capture.assertLoggedRE("live view O3 replay \\[view=lv, lateRowTs=");
                capture.assertLoggedRE("live view O3 repair yielded on its turn budget \\[view=lv, turns=1,");
                // Nothing below the check ran: the watermark still names the commit the restart
                // read off disk, and the commit waiting above it is not in the view.
                Assert.assertEquals(
                        "the parked repair owns the runtime, so the turn must not have drained over it",
                        gapWatermark,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertEquals("a park is not a fault", 0, instance.getRefreshFaultCount());
                Assert.assertEquals(
                        "the repair must have come out of the restart's own restore",
                        0,
                        instance.getCheckpointRuntimeRestores()
                );
                Assert.assertEquals("the restore must not have fallen back to a rebuild", 0, instance.getCheckpointRebuildAttempts());
                assertViewRows(O3_GAP_OUTPUT);

                driveRefreshToQuiescence(job);
                Assert.assertNull(instance.getSuspendedRepair());
                Assert.assertFalse(instance.isInvalid());
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);
            }

            // The ladder the repair left behind is the one the next restart reads, and it needs no
            // repair of its own.
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);
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

    /**
     * Advances the clock and runs exactly one refresh turn, so a caller can read the state that
     * turn left rather than the state the turns after it converged on.
     */
    private void runOnePass(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        drainWalQueue();
        job.run();
        drainWalQueue();
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
