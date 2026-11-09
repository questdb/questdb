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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A refresh cycle that consumes base commits and puts no row through the window - the
 * view's filter rejected them all - still moves the view's head checkpoint on the
 * duration cadence, so the base table's WAL is released while nothing matches.
 * <p>
 * Every case drives the clock by hand: each commit is refreshed {@link #STEP_MICROS}
 * after the previous one, past the views' 100ms FLUSH EVERY, against a 1s duration
 * cadence. The expected seqTxns follow from that arithmetic: a head written at commit
 * {@code k} is next due at commit {@code k + 4}, 1.2s later.
 */
public class LiveViewNoRowCheckpointTest extends AbstractLiveViewTest {
    private static final long CADENCE_MICROS = 1_000_000;
    // The root ladder departTheNewestRow leaves: the day the base lost, 00:01, and the new
    // frontier 00:02.
    private static final String DEPARTURE_LADDER = """
            2025-12-31T12:00:00.000000Z -> 1
            2026-01-01T00:01:00.000000Z -> 2
            2026-01-01T00:02:00.000000Z -> 3
            """;
    // The rows departTheNewestRow leaves: three rows of a, each summing itself and the two before.
    private static final String DEPARTURE_ROWS = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            2026-01-01T00:02:00.000000Z\ta\t2.5\t4.5
            """;
    // Past the cadence, so a cycle refreshed this long after the last head write is due.
    private static final long DEPARTURE_STEP_MICROS = 1_200_000;
    // Roots sealed below a departure so the catalogue spans more than one 64-entry leaf.
    private static final int MULTI_LEAF_ROOTS = 30;
    private static final long START_MICROS = 1_000_000_000_000L;
    private static final long STEP_MICROS = 300_000;

    @Test
    public void testAFailedConsumedAdvanceHoldsTheHeadAppliedBase() throws Exception {
        assertAFailedConsumedAdvanceHoldsTheHead(DrainPath.APPLIED_BASE);
    }

    @Test
    public void testAFailedConsumedAdvanceHoldsTheHeadLead() throws Exception {
        assertAFailedConsumedAdvanceHoldsTheHead(DrainPath.LEAD);
    }

    @Test
    public void testAFailedConsumedAdvanceHoldsTheHeadRawWal() throws Exception {
        assertAFailedConsumedAdvanceHoldsTheHead(DrainPath.RAW_WAL);
    }

    @Test
    public void testAViewOwedARecoveryDoesNotSealWithoutRows() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                commit(job, minute(3), "a", -1.0);
                commit(job, minute(4), "a", -1.0);
                final LiveViewInstance instance = instance();
                // The seal is due at seqTxn 5, over a runtime a recovery is owed: freezing it
                // would publish state the durable output does not describe.
                LiveViewTestRefreshHooks.install(job).setSimulateNoRowHeadCheckpointStart(
                        () -> instance.setWindowStateDirty(true)
                );
                commit(job, minute(5), "a", -1.0);
                Assert.assertEquals(5, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);

                // The next turn restores the runtime from the timeline before it drains, and
                // then seals it.
                commit(job, minute(6), "a", -1.0);
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                assertTimeline(2, 2, 6);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAViewThatNeverEmittedHoldsNoBaseWal() throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                for (int i = 1; i <= 6; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                final LiveViewInstance instance = instance();
                Assert.assertEquals(6, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(0, purgeBaseWalAndCountSegments());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDedupBaseAppliedReadAdvancesTheHead() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                final long cleanCycles = instance.getDedupRawWalCleanCycles();
                // Each rejected commit repeats its row, which the dedup keys collapse. The
                // applied base then differs from the raw WAL, so every one of these cycles
                // reads the applied base.
                for (int i = 3; i <= 13; i++) {
                    execute("INSERT INTO base VALUES ('" + minute(i) + "', 'a', -1.0), ('" + minute(i) + "', 'a', -1.0)");
                    refresh(job);
                }
                Assert.assertEquals(cleanCycles, instance.getDedupRawWalCleanCycles());
                assertRejectedStretchAdvancedTheHead(instance);
            }
        });
    }

    @Test
    public void testDedupBaseRawWalCycleAdvancesTheHead() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                final long cleanCycles = instance.getDedupRawWalCleanCycles();
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                // Nothing collapsed, so every rejected commit took the coupled raw-WAL drain.
                Assert.assertEquals(cleanCycles + 11, instance.getDedupRawWalCleanCycles());
                assertRejectedStretchAdvancedTheHead(instance);
            }
        });
    }

    @Test
    public void testDepartureOfUnsealedNewestRowsThenNoRowStretchKeepsTheTimeline() throws Exception {
        assertDepartureThenNoRowStretch(DepartureShape.UNSEALED, DrainPath.RAW_WAL, "-4.5", FirstEvent.RESTART);
    }

    @Test
    public void testDepartureStateDropsWithoutLeaks() throws Exception {
        // The view drops in the state a departure and the cycles without rows after it leave:
        // the head on the root at the newest row it holds, below the newest timestamp it has seen.
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "-4.5");
                for (int i = 6; i <= 8; i++) {
                    commitRejected(job, DrainPath.RAW_WAL, i, DEPARTURE_STEP_MICROS);
                }
                Assert.assertEquals(8, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                execute("DROP LIVE VIEW lv");
                drainJob(job);
                Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
            }
        });
    }

    @Test
    public void testDepartureThatEmptiesTheViewSealsNothingWithoutRows() throws Exception {
        // The view's one row departs, and the repair leaves the view with no row and no timeline.
        // The newest timestamp the view has seen stays at the departed row, where no row of the
        // view sits: each cycle without rows declines the seal there rather than open a history
        // at a timestamp no restart could restore from.
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5, DEPARTURE_STEP_MICROS);
                commit(job, minute(1), "a", -1.5, DEPARTURE_STEP_MICROS);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(ts(minute(1)), instance.getLatestSeenTs());
                final long declinesBefore = sealAboveFrontierDeclines(job);
                for (int i = 3; i <= 8; i++) {
                    commit(job, minute(i), "a", -1.0, DEPARTURE_STEP_MICROS);
                }
                // No head, so every cycle asks for a first root, and each one declines it.
                Assert.assertEquals(6, sealAboveFrontierDeclines(job) - declinesBefore);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                assertNoTimeline(instance);
                assertQuery("SELECT ts, sym, x, s FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tsym\tx\ts\n");
            }
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(9), "a", -1.0, DEPARTURE_STEP_MICROS);
                final LiveViewInstance instance = instance();
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_status\nactive\n");
                assertQuery("SELECT ts, sym, x, s FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tsym\tx\ts\n");
                commit(job, minute(10), "a", 4.0, DEPARTURE_STEP_MICROS);
                assertCheckpointLadder(instance, false, "2026-01-01T00:10:00.000000Z -> 1\n");
                assertQuery("SELECT ts, sym, x, s FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tsym\tx\ts
                                2026-01-01T00:10:00.000000Z\ta\t4.0\t4.0
                                """);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDepartureThenAFailedNoRowPublicationRetries() throws Exception {
        // The first publication of the stretch after a departure fails, which counts as a failed
        // seal does and leaves the head and the purge floor where they were. The head keeps its
        // write time, so the next cycle is due again and publishes.
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "-4.5");
                final long floorBefore = instance.getCheckpointTimelineWalPurgeFloor();
                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                commitRejected(job, DrainPath.RAW_WAL, 6, DEPARTURE_STEP_MICROS);
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(floorBefore, instance.getCheckpointTimelineWalPurgeFloor());
                job.setCheckpointTimelineTestFailureStage(0);
                commitRejected(job, DrainPath.RAW_WAL, 7, DEPARTURE_STEP_MICROS);
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(7, instance.getHeadCheckpointBaseSeqTxn());
                for (int i = 8; i <= 11; i++) {
                    commitRejected(job, DrainPath.RAW_WAL, i, DEPARTURE_STEP_MICROS);
                }
                assertDepartureStretchAdvancedTheHead(job, instance);
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
            }
            assertDepartureStateSurvivesTwoRestarts(DrainPath.RAW_WAL, 12, DEPARTURE_ROWS);
        });
    }

    @Test
    public void testDepartureThenNoRowStretchKeepsPurgedWalOrderedBehindTheHead() throws Exception {
        // The purge runs between the consumed watermark's advance and the head's, on the first
        // cycle of the stretch: the head still names the departure's commit, so the purge keeps
        // every commit above it, and the restarts replay what they need from them.
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "-4.5");
                Assert.assertEquals(5, instance.getCheckpointTimelineWalPurgeFloor());
                final AtomicLong segmentsAtTheHook = new AtomicLong(-1);
                hooks(job).setSimulateNoRowHeadCheckpointStart(() -> {
                    Assert.assertEquals(6, instance.getStateReader().getLvConsumedSeqTxn());
                    Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                    segmentsAtTheHook.set(purgeBaseWalAndCountSegments());
                });
                commitRejected(job, DrainPath.RAW_WAL, 6, DEPARTURE_STEP_MICROS);
                hooks(job).setSimulateNoRowHeadCheckpointStart(null);
                Assert.assertEquals("the purge keeps the commit above the head's", 6 - 5, segmentsAtTheHook.get());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                for (int i = 7; i <= 11; i++) {
                    commitRejected(job, DrainPath.RAW_WAL, i, DEPARTURE_STEP_MICROS);
                }
                assertDepartureStretchAdvancedTheHead(job, instance);
            }
            assertDepartureStateSurvivesTwoRestarts(DrainPath.RAW_WAL, 12, DEPARTURE_ROWS);
        });
    }

    @Test
    public void testDepartureThenNoRowStretchKeepsTheTimelineAcrossRestartsAppliedBase() throws Exception {
        assertDepartureThenNoRowStretch(DepartureShape.ONE_ROOT, DrainPath.APPLIED_BASE, "-4.5", FirstEvent.RESTART);
    }

    @Test
    public void testDepartureThenNoRowStretchKeepsTheTimelineAcrossRestartsLead() throws Exception {
        assertDepartureThenNoRowStretch(DepartureShape.ONE_ROOT, DrainPath.LEAD, null, FirstEvent.RESTART);
    }

    @Test
    public void testDepartureThenNoRowStretchKeepsTheTimelineAcrossRestartsRawWal() throws Exception {
        assertDepartureThenNoRowStretch(DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "-4.5", FirstEvent.RESTART);
    }

    @Test
    public void testDepartureThenNoRowStretchKeepsTheNextAnchoredSealIncremental() throws Exception {
        // A departure over an anchored view: the resume truncates the root the departed row
        // sealed and seals the new frontier, imaging every key. The stretch of rejected commits
        // after it carries that root's baselines forward and seals nothing at the departed row,
        // so the next row's seal images its own key alone.
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, amount DOUBLE, fee DECIMAL(38,2)) TIMESTAMP(ts) PARTITION BY DAY WAL "
                    + "DEDUP UPSERT KEYS(ts, sym)");
            createAnchoredViewOverBase();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                seedAccounts(job);
                final LiveViewInstance instance = instance();
                execute("INSERT INTO base VALUES ('2026-01-01T11:00:20.000000Z', 'acct-1', 2.0, 1.00::decimal(38,2)), "
                        + "('2026-01-01T11:00:21.000000Z', 'acct-1', 3.0, 1.00::decimal(38,2))");
                refresh(job, DEPARTURE_STEP_MICROS);
                assertCheckpointLadder(instance, false, """
                        2026-01-01T11:00:08.000000Z -> 8
                        2026-01-01T11:00:21.000000Z -> 10
                        """);
                execute("INSERT INTO base VALUES ('2026-01-01T11:00:21.000000Z', 'acct-1', -1.0, 1.00::decimal(38,2))");
                refresh(job, DEPARTURE_STEP_MICROS);
                final String ladder = """
                        2026-01-01T11:00:08.000000Z -> 8
                        2026-01-01T11:00:20.000000Z -> 9
                        """;
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals(ts("2026-01-01T11:00:20.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(ts("2026-01-01T11:00:21.000000Z"), instance.getLatestSeenTs());
                for (int i = 4; i <= 7; i++) {
                    execute("INSERT INTO base VALUES ('2026-01-01T11:00:3" + i + ".000000Z', 'acct-2', -1.0, 1.00::decimal(38,2))");
                    refresh(job, DEPARTURE_STEP_MICROS);
                    Assert.assertEquals(i, instance.getHeadCheckpointBaseSeqTxn());
                }
                assertCheckpointLadder(instance, false, ladder);

                final long windowCaptures = instance.getCheckpointCaptureLedgerForTest().getWindowCaptures();
                final long windowIncremental = instance.getCheckpointCaptureLedgerForTest().getWindowIncrementalCaptures();
                final long windowImaged = instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged();
                final long functionCaptures = instance.getCheckpointCaptureLedgerForTest().getFunctionCaptures();
                final long functionIncremental = instance.getCheckpointCaptureLedgerForTest().getFunctionIncrementalCaptures();
                final long functionImaged = instance.getCheckpointCaptureLedgerForTest().getFunctionKeysImaged();
                execute("INSERT INTO base VALUES ('2026-01-01T11:00:50.000000Z', 'acct-3', 1.0, 1.25::decimal(38,2))");
                refresh(job, DEPARTURE_STEP_MICROS);
                assertCheckpointLadder(instance, false, ladder + "2026-01-01T11:00:50.000000Z -> 10\n");
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowCaptures() - windowCaptures);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowIncrementalCaptures() - windowIncremental);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged() - windowImaged);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionCaptures() - functionCaptures);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionIncrementalCaptures() - functionIncremental);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionKeysImaged() - functionImaged);
                assertAnchoredViewMatchesRecompute();
            }
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertRestoredFromTimeline("lv");
            assertAnchoredViewMatchesRecompute();
        });
    }

    @Test
    public void testDepartureThenNoRowStretchKeepsTheTimelineWithJitDisabled() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        assertDepartureThenNoRowStretch(DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "-4.5", FirstEvent.RESTART);
    }

    @Test
    public void testDepartureThenNoRowStretchSurvivesASchemaChangeThenARestart() throws Exception {
        assertDepartureThenNoRowStretch(DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "-4.5", FirstEvent.SCHEMA_CHANGE);
    }

    @Test
    public void testDepartureThenPublicationsFailingPastThePauseOpenNoHistoryAboveTheFrontier() throws Exception {
        // Three failed publications of the stretch after a departure pause the seal: the head stays
        // on the root at the frontier. The fault outlasts the pause, so the first publication after
        // it fails, which retires the timeline and clears the head, with the newest timestamp the
        // view has seen still at the departed row. Past the cooldown each cycle without rows
        // declines a first root there, above the newest row the view holds. The next matching row
        // opens a fresh history at its own timestamp, which the restart restores from.
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "-4.5");
                final long resetsBefore = instance.getCheckpointTimelineResets();
                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                for (int i = 6; i <= 8; i++) {
                    commitRejected(job, DrainPath.RAW_WAL, i, DEPARTURE_STEP_MICROS);
                }
                Assert.assertEquals(3, instance.getCheckpointSealFailures());
                Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));

                setCurrentMicros(currentMicros + Micros.MINUTE_MICROS + Micros.SECOND_MICROS);
                commitRejected(job, DrainPath.RAW_WAL, 9, DEPARTURE_STEP_MICROS);
                job.setCheckpointTimelineTestFailureStage(0);
                Assert.assertEquals(4, instance.getCheckpointSealFailures());
                Assert.assertEquals("the fourth failure retires", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros + 2 * Micros.MINUTE_MICROS - Micros.SECOND_MICROS));
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros + 2 * Micros.MINUTE_MICROS + Micros.SECOND_MICROS));
                assertNoTimeline(instance);

                setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
                final long declinesBefore = sealAboveFrontierDeclines(job);
                for (int i = 10; i <= 12; i++) {
                    commitRejected(job, DrainPath.RAW_WAL, i, DEPARTURE_STEP_MICROS);
                    Assert.assertEquals(i - 9, sealAboveFrontierDeclines(job) - declinesBefore);
                }
                Assert.assertEquals(4, instance.getCheckpointSealFailures());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                assertNoTimeline(instance);

                commit(job, minute(13), "a", 4.0, DEPARTURE_STEP_MICROS);
                Assert.assertEquals(ts(minute(13)), instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, "2026-01-01T00:13:00.000000Z -> 4\n");
            }
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertRestoredFromTimeline("lv");
            assertDepartedViewRows(DEPARTURE_ROWS + "2026-01-01T00:13:00.000000Z\ta\t4.0\t8.0\n");
        });
    }

    @Test
    public void testRowlessPublicationFaultSpanningThreeCyclesPausesOnTheAppliedBaseDrain() throws Exception {
        assertRowlessPublicationFaultSpanningThreeCyclesPauses(DrainPath.APPLIED_BASE);
    }

    @Test
    public void testRowlessPublicationFaultSpanningThreeCyclesPausesOnTheRawWalDrain() throws Exception {
        assertRowlessPublicationFaultSpanningThreeCyclesPauses(DrainPath.RAW_WAL);
    }

    @Test
    public void testDepartureToNullThenNoRowStretchKeepsTheTimeline() throws Exception {
        assertDepartureThenNoRowStretch(DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "NULL", FirstEvent.RESTART);
    }

    @Test
    public void testDepartureToZeroThenNoRowStretchKeepsTheTimeline() throws Exception {
        assertDepartureThenNoRowStretch(DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "0.0", FirstEvent.RESTART);
    }

    @Test
    public void testDepartureThenNoRowCyclesReleaseBaseWal() throws Exception {
        // One commit seals two rows under one root at the newer of them, and an upsert then moves
        // that row out of the filter. The resume leaves the view's head at the row below it, with
        // no root above: the stretch of rejected commits after it then moves the head's base
        // coordinate and the generation's on the duration cadence, as it does after any row, and
        // puts no root above the newest row the view holds, which a restart restores from.
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        final long stepMicros = 1_200_000;
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5, stepMicros);
                execute("INSERT INTO base VALUES ('" + minute(2) + "', 'a', 2.5), ('" + minute(3) + "', 'a', 4.5)");
                refresh(job, stepMicros);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(ts(minute(3)), instance.getHeadCheckpointMaxTs());
                commit(job, minute(3), "a", -4.5, stepMicros);
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                for (int i = 4; i <= 9; i++) {
                    commit(job, minute(i), "a", -1.0, stepMicros);
                }
                Assert.assertEquals(9, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(9, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                assertCheckpointLadder(instance, false, """
                        2026-01-01T00:01:00.000000Z -> 1
                        2026-01-01T00:02:00.000000Z -> 2
                        """);
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                assertQuery("SELECT checkpoint_timeline_normalized_base_seqtxn FROM live_views() WHERE view_name = 'lv'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("checkpoint_timeline_normalized_base_seqtxn\n9\n");
                // The older of the two superblock slots, which the publication at seqTxn 9 left at 8.
                Assert.assertEquals(8, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 9 - 8, purgeBaseWalAndCountSegments());
                assertViewMatchesRecompute();
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t1.5
                    2026-01-01T00:02:00.000000Z\ta\t2.5\t4.0
                    """;
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The restore puts the newest timestamp seen back at the newest row, and the
                // root there is the newest, so every cycle without rows publishes.
                for (int i = 10; i <= 12; i++) {
                    commit(job, minute(i), "a", -1.0, stepMicros);
                }
                assertRestoredFromTimeline("lv");
                final LiveViewInstance instance = instance();
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(12, instance.getHeadCheckpointBaseSeqTxn());
                assertDepartedViewRows(rows);
            }
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertRestoredFromTimeline("lv");
            assertDepartedViewRows(rows);
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testFailedNoRowPublicationKeepsTheHeadAndRetries() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                final LiveViewInstance instance = instance();

                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                for (int i = 2; i <= 4; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                // Due at seqTxn 5, and the publication fails: the head and the floor stay.
                commit(job, minute(5), "a", -1.0);
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(1, 1, 1);
                Assert.assertFalse(instance.isInvalid());

                // The head kept its old write time, so the next cycle is still due and retries.
                job.setCheckpointTimelineTestFailureStage(0);
                commit(job, minute(6), "a", -1.0);
                Assert.assertEquals(1, instance.getCheckpointSealFailures());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 1, 6);

                // Three failures in a row pause the publication, as three failed seals do: the
                // cadence is due again at seqTxn 10, and 10 to 12 fail. The head, both of the
                // view's floors and the timeline stay, so a restart in the pause restores.
                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                for (int i = 7; i <= 12; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(4, instance.getCheckpointSealFailures());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 1, 6);
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));

                // The pause holds the next publication off, though the cadence is due.
                commit(job, minute(13), "a", -1.0);
                Assert.assertEquals(4, instance.getCheckpointSealFailures());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());

                // The fault outlasts the pause: the first publication after it fails, which
                // retires the timeline and releases both of the view's floors with it.
                setCurrentMicros(currentMicros + Micros.MINUTE_MICROS + Micros.SECOND_MICROS);
                commit(job, minute(14), "a", -1.0);
                Assert.assertEquals(5, instance.getCheckpointSealFailures());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getCheckpointTimelineWalPurgeFloor());
                try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                }

                // The two-minute cooldown holds the next seal off, rows or not.
                job.setCheckpointTimelineTestFailureStage(0);
                commit(job, minute(15), "a", -1.0);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());

                // Past it, a cycle without rows opens a fresh history at the runtime frontier.
                setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
                commit(job, minute(16), "a", -1.0);
                Assert.assertEquals(16, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(16, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(ts(minute(1)), instance.getHeadCheckpointMaxTs());
                assertTimeline(1, 1, 16);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(17), "a", 3.5);
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testFilteredViewReleasesBaseWalWhileNoRowMatches() throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertRejectedStretchAdvancedTheHead(instance);
                assertViewMatchesRecompute();
                Assert.assertEquals(
                        "the purge must keep only the segments above the floor",
                        13 - 9,
                        purgeBaseWalAndCountSegments()
                );
            }
        });
    }

    @Test
    public void testFirstMatchingRowAfterTheStretchMovesThePurgeFloor() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertRejectedStretchAdvancedTheHead(instance);

                // The first matching row once the cadence has elapsed seals a root of its own.
                // The floor is the older slot's coordinate, which the stretch has moved on to
                // seqTxn 13, rather than the coordinate of the seal before the stretch.
                setCurrentMicros(currentMicros + CADENCE_MICROS);
                commit(job, minute(14), "a", 3.5);
                Assert.assertEquals(14, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(13, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(ts(minute(14)), instance.getHeadCheckpointMaxTs());
                assertTimeline(5, 3, 14);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testLateRowTheFilterRejectsWhoseSplicePublicationFailsRetiresAndReStampsNothing() throws Exception {
        // The splice of the late row's zero-row resume fails after its metadata publication, so
        // no generation names the repair commit: the resume retires the timeline, and the head
        // must stay cleared rather than be re-stamped over a timeline that is gone.
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5, DEPARTURE_STEP_MICROS);
                execute("INSERT INTO base (ts, sym, x) VALUES ('" + minute(2) + "', 'a', 2.5), ('" + minute(3) + "', 'a', 4.5)");
                refresh(job, STEP_MICROS);
                commit(job, minute(3), "a", -4.5, DEPARTURE_STEP_MICROS);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
                final long resetsBefore = instance.getCheckpointTimelineResets();

                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                commitLateRejected(job, DrainPath.RAW_WAL, lateTs(0), "-1.0", DEPARTURE_STEP_MICROS);
                job.setCheckpointTimelineTestFailureStage(0);
                Assert.assertEquals(resetsBefore + 1, instance.getCheckpointTimelineResets());
                assertNoTimeline(instance);
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(0, spliceZeroRowKeeps(job));
                assertViewMatchesRecompute();

                commitLateRejected(job, DrainPath.RAW_WAL, lateTs(1), "-1.0", DEPARTURE_STEP_MICROS);
                Assert.assertEquals(0, spliceZeroRowKeeps(job));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureKeepTheCheckpointDirectoryBounded() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.UNSEALED, DrainPath.RAW_WAL, "-4.5");
                // The departure's sweep left its proposal for the first late commit's splice to carry.
                Assert.assertFalse(deadCatalogueEntries(instance).isEmpty());
                assertLateRejectedStretchKeepsTheCheckpointDirectoryBounded(job, instance, 6, true);
                assertDepartedViewRows(DEPARTURE_ROWS);
            }
            assertRestartRestoresTheDepartedRows(DEPARTURE_ROWS);
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureKeepAMultiLeafCatalogueBounded() throws Exception {
        // Enough roots below the departure that the catalogue spans more than one 64-entry leaf, so a
        // splice carries proposals out of leaves other than the one its registration rewrites.
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                for (int i = 1; i <= MULTI_LEAF_ROOTS; i++) {
                    commit(job, "2026-01-01T00:01:" + (i < 10 ? "0" : "") + i + ".000000Z", "a", 1.5, DEPARTURE_STEP_MICROS);
                }
                execute("INSERT INTO base (ts, sym, x) VALUES ('" + minute(2) + "', 'a', 2.5), ('" + minute(3) + "', 'a', 4.5)");
                refresh(job, STEP_MICROS);
                commit(job, minute(3), "a", -4.5, DEPARTURE_STEP_MICROS);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(MULTI_LEAF_ROOTS + 2, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertTrue("the catalogue must span more than one leaf", catalogueSize(instance) > 64);
                assertLateRejectedStretchKeepsTheCheckpointDirectoryBounded(job, instance, MULTI_LEAF_ROOTS + 3, false);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureMoveTheHeadAppliedBase() throws Exception {
        assertLateRowsTheFilterRejectsAfterADepartureMoveTheHead(DepartureShape.UNSEALED, DrainPath.APPLIED_BASE, "-1.0");
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureMoveTheHeadAtNull() throws Exception {
        assertLateRowsTheFilterRejectsAfterADepartureMoveTheHead(DepartureShape.UNSEALED, DrainPath.RAW_WAL, "NULL");
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureMoveTheHeadAtZero() throws Exception {
        assertLateRowsTheFilterRejectsAfterADepartureMoveTheHead(DepartureShape.UNSEALED, DrainPath.RAW_WAL, "0.0");
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureMoveTheHeadInNanoseconds() throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP_NS, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, "2026-01-01T00:01:00.000000000Z", "a", 1.5, DEPARTURE_STEP_MICROS);
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-01T00:02:00.000000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000000Z', 'a', 4.5)");
                refresh(job, STEP_MICROS);
                commit(job, "2026-01-01T00:03:00.000000000Z", "a", -4.5, DEPARTURE_STEP_MICROS);
                final LiveViewInstance instance = instance();
                final String ladder = """
                        2026-01-01T00:01:00.000000000Z -> 1
                        2026-01-01T00:02:00.000000000Z -> 2
                        """;
                assertCheckpointLadder(instance, true, ladder);
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
                // One nanosecond above the head root, then one below the departed row.
                commit(job, "2026-01-01T00:02:00.000000001Z", "b", -1.0, DEPARTURE_STEP_MICROS);
                Assert.assertEquals(4, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, spliceZeroRowKeeps(job));
                commit(job, "2026-01-01T00:02:59.999999999Z", "b", -1.0, DEPARTURE_STEP_MICROS);
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(2, spliceZeroRowKeeps(job));
                assertCheckpointLadder(instance, true, ladder);
                assertTimelineBase(2, 5);
                Assert.assertEquals(4, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 5 - 4, purgeBaseWalAndCountSegments());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureMoveTheHeadLead() throws Exception {
        assertLateRowsTheFilterRejectsAfterADepartureMoveTheHead(DepartureShape.UNSEALED, DrainPath.LEAD, "-1.0");
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureMoveTheHeadOneRoot() throws Exception {
        assertLateRowsTheFilterRejectsAfterADepartureMoveTheHead(DepartureShape.ONE_ROOT, DrainPath.RAW_WAL, "-1.0");
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureMoveTheHeadRawWal() throws Exception {
        assertLateRowsTheFilterRejectsAfterADepartureMoveTheHead(DepartureShape.UNSEALED, DrainPath.RAW_WAL, "-1.0");
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureMoveTheHeadWithThePurgeCadenceOff() throws Exception {
        // At interval zero nothing sweeps while the process runs, so every splice's superseded
        // catalogue segment stays on disk and in the catalogue, yet the head still moves: the
        // re-stamp does not depend on the sweep. A restart's reconciliation unlinks the files, and
        // the first seal of the next process carries their entries out.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_PURGE_INTERVAL, 0);
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.UNSEALED, DrainPath.RAW_WAL, "-4.5");
                final int filesBefore = checkpointFileCount(instance);
                final long entriesBefore = catalogueSize(instance);
                Assert.assertTrue(deadCatalogueEntries(instance).isEmpty());
                for (int i = 1; i <= 6; i++) {
                    commitLateRejected(job, DrainPath.RAW_WAL, lateTs(i), "-1.0", DEPARTURE_STEP_MICROS);
                    Assert.assertEquals(5 + i, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(i, spliceZeroRowKeeps(job));
                    Assert.assertEquals(filesBefore + i, checkpointFileCount(instance));
                    Assert.assertEquals(entriesBefore + i, catalogueSize(instance));
                    Assert.assertTrue(deadCatalogueEntries(instance).isEmpty());
                }
                Assert.assertEquals("the purge must keep only the segments above the floor", 11 - 10, purgeBaseWalAndCountSegments());
            }
            final long entriesBeforeRestart = catalogueSize(instance());
            restartCycle();
            // The boot reconciliation unlinks every superseded segment but the one the older
            // superblock slot still protects, and rewrites no catalogue.
            Assert.assertEquals(1, uncollectedSegmentCount(instance()));
            Assert.assertEquals(entriesBeforeRestart, catalogueSize(instance()));
            Assert.assertFalse(deadCatalogueEntries(instance()).isEmpty());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                Assert.assertEquals(11, instance().getHeadCheckpointBaseSeqTxn());
                // A row the filter keeps, in order: its seal is this process's first append, which
                // reconciles the directory and carries the entries the reconciliation proposes.
                commit(job, minute(4), "a", 5.0, DEPARTURE_STEP_MICROS);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(ts(minute(4)), instance.getHeadCheckpointMaxTs());
                Assert.assertTrue(deadCatalogueEntries(instance).isEmpty());
                assertCatalogueMatchesDisk(instance);
                assertDepartedViewRows(DEPARTURE_ROWS + "2026-01-01T00:04:00.000000Z\ta\t5.0\t9.0\n");
            }
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureOfAnAnchoredViewMoveTheHead() throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createAnchoredFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5, DEPARTURE_STEP_MICROS);
                execute("INSERT INTO base (ts, sym, x) VALUES ('" + minute(2) + "', 'a', 2.5), ('" + minute(3) + "', 'a', 4.5)");
                refresh(job, STEP_MICROS);
                replaceBaseRange(ts(minute(3)), ts(minute(3)) + 1, Double.NaN);
                refresh(job, DEPARTURE_STEP_MICROS);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(3)), instance.getLatestSeenTs());
                for (int k = 4; k <= 9; k++) {
                    commitLateRejected(job, DrainPath.LEAD, lateTs(k - 4), "-1.0", DEPARTURE_STEP_MICROS);
                    Assert.assertEquals(k, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(k - 3, spliceZeroRowKeeps(job));
                }
                assertTimelineBase(2, 9);
                Assert.assertEquals(8, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 9 - 8, purgeBaseWalAndCountSegments());
                assertAnchoredFilteredViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureReleaseBaseWalAtTheDefaultRollover() throws Exception {
        // The default WAL segment rollover: the five fixture rows and the first twenty late commits
        // of 10_000 rows fill the first segment past 200_000 rows, so the last five commits sit in
        // the segment open after it. A head that follows the commits lets the purge take the closed
        // segment whole.
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 0);
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.UNSEALED, DrainPath.RAW_WAL, "-4.5");
                for (int i = 0; i < 25; i++) {
                    execute("INSERT INTO base SELECT ('" + minute(2) + "'::timestamp + " + i * 10_000 + " + x)::timestamp, 'b', -1.0 FROM long_sequence(10_000)");
                    refresh(job, DEPARTURE_STEP_MICROS);
                    Assert.assertEquals(6 + i, instance.getHeadCheckpointBaseSeqTxn());
                }
                Assert.assertEquals(25, spliceZeroRowKeeps(job));
                Assert.assertEquals("the purge must keep only the open segment", 1, purgeBaseWalAndCountSegments());
                assertDepartedViewRows(DEPARTURE_ROWS);
            }
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsAfterADepartureSweepAtThePurgeInterval() throws Exception {
        // Every third seal sweeps, re-stamps included. In between, each splice leaves one more
        // superseded catalogue segment on disk, and the sweep takes them back to the one the
        // older superblock slot protects; the splice after the sweep carries its proposal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_PURGE_INTERVAL, 3);
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.UNSEALED, DrainPath.RAW_WAL, "-4.5");
                instance.resetSealsSincePurge();
                int filesAtSweep = -1;
                Set<Long> deadAtSweep = null;
                for (int i = 1; i <= 9; i++) {
                    commitLateRejected(job, DrainPath.RAW_WAL, lateTs(i), "-1.0", DEPARTURE_STEP_MICROS);
                    Assert.assertEquals(5 + i, instance.getHeadCheckpointBaseSeqTxn());
                    if (deadAtSweep != null) {
                        assertNoneCatalogued(instance, deadAtSweep);
                        deadAtSweep = null;
                    }
                    final int files = checkpointFileCount(instance);
                    switch (i % 3) {
                        case 0 -> {
                            Assert.assertEquals(1, uncollectedSegmentCount(instance));
                            if (filesAtSweep < 0) {
                                filesAtSweep = files;
                            } else {
                                Assert.assertEquals(filesAtSweep, files);
                            }
                            deadAtSweep = deadCatalogueEntries(instance);
                            Assert.assertFalse(deadAtSweep.isEmpty());
                        }
                        case 1 -> {
                            if (filesAtSweep >= 0) {
                                Assert.assertEquals(2, uncollectedSegmentCount(instance));
                                Assert.assertEquals(filesAtSweep + 1, files);
                            }
                        }
                        default -> {
                            if (filesAtSweep >= 0) {
                                Assert.assertEquals(3, uncollectedSegmentCount(instance));
                                Assert.assertEquals(filesAtSweep + 2, files);
                            }
                        }
                    }
                }
                assertCatalogueMatchesDisk(instance);
                assertDepartedViewRows(DEPARTURE_ROWS);
            }
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsAtTheEdgesOfTheDepartedGap() throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.UNSEALED, DrainPath.RAW_WAL, "-4.5");
                // One microsecond above the head root, then one below the newest timestamp seen.
                commitLateRejected(job, DrainPath.RAW_WAL, "2026-01-01T00:02:00.000001Z", "-1.0", DEPARTURE_STEP_MICROS);
                assertLateCommitStampedTheHead(job, instance, 6, 1);
                commitLateRejected(job, DrainPath.RAW_WAL, "2026-01-01T00:02:59.999999Z", "-1.0", DEPARTURE_STEP_MICROS);
                assertLateCommitStampedTheHead(job, instance, 7, 2);
                // The departed row upserted again, twice in one commit: the applied-base drain reads a
                // commit at the newest timestamp seen as an overlap.
                execute("INSERT INTO base (ts, sym, x) VALUES ('" + minute(3) + "', 'a', -7.0), ('" + minute(3) + "', 'a', -7.0)");
                refresh(job, DEPARTURE_STEP_MICROS);
                assertLateCommitStampedTheHead(job, instance, 8, 3);
                // A row of b at the newest timestamp seen, a key the base does not hold yet: the
                // commit collapses nothing, the raw-WAL drain reads it in order, and the cycle
                // without rows publishes.
                commitLateRejected(job, DrainPath.RAW_WAL, minute(3), "-7.0", DEPARTURE_STEP_MICROS);
                assertLateCommitStampedTheHead(job, instance, 9, 3);
                // On the head root's own timestamp: the resume anchors below the head root and
                // replays 00:02 a, whose closing seal re-stamps that root.
                commitLateRejected(job, DrainPath.RAW_WAL, minute(2), "-1.0", DEPARTURE_STEP_MICROS);
                assertLateCommitStampedTheHead(job, instance, 10, 3);
                // Above the newest timestamp seen: in order, and the cycle without rows publishes.
                commitLateRejected(job, DrainPath.RAW_WAL, "2026-01-01T00:03:00.000001Z", "-1.0", DEPARTURE_STEP_MICROS);
                assertLateCommitStampedTheHead(job, instance, 11, 3);
                assertCheckpointLadder(instance, false, DEPARTURE_LADDER);
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, sealAboveFrontierDeclines(job));
                Assert.assertEquals("the purge must keep only the segments above the floor", 11 - 10, purgeBaseWalAndCountSegments());
                assertDepartedViewRows(DEPARTURE_ROWS);
            }
            assertRestartRestoresTheDepartedRows(DEPARTURE_ROWS);
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsBelowADepartedRangeFrameMoveTheHeadAtTheDefaultCadence() throws Exception {
        // The item's trigger as stated: a one-minute RANGE frame over a deduplicating base, every
        // row at the view's newest timestamps upserted out of the filter, then rows of another
        // symbol the filter rejects below the departed timestamps, each one six minutes after the
        // last, past the default five-minute cadence.
        configureWalPurge();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + HEAD_MISS_DEPARTURE_VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                execute("INSERT INTO base VALUES " + minuteRowsOfA(0, 30, "1.0"));
                refreshToQuiescence(job, DURATION_CADENCE_ELAPSED_MICROS);
                execute("INSERT INTO base VALUES " + minuteRowsOfA(31, 59, "1.0"));
                refreshToQuiescence(job, DURATION_CADENCE_ELAPSED_MICROS);
                execute("INSERT INTO base VALUES ('2026-01-01T00:57:00.000000Z', 'a', 5.0), "
                        + "('2026-01-01T00:58:00.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', -1.0)");
                refreshToQuiescence(job, DURATION_CADENCE_ELAPSED_MICROS);
                final LiveViewInstance instance = instance();
                Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts("2026-01-01T00:59:00.000000Z"), instance.getLatestSeenTs());
                int files = -1;
                long entries = -1;
                long retirementBytes = -1;
                for (int i = 0; i < 6; i++) {
                    execute("INSERT INTO base VALUES ('2026-01-01T00:58:1" + i + ".000000Z', 'b', -1.0)");
                    refreshToQuiescence(job, DURATION_CADENCE_ELAPSED_MICROS);
                    Assert.assertEquals(4 + i, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), instance.getHeadCheckpointMaxTs());
                    Assert.assertEquals(i + 1, spliceZeroRowKeeps(job));
                    if (i == 1) {
                        files = checkpointFileCount(instance);
                        entries = catalogueSize(instance);
                        retirementBytes = retirementQueueBytes(instance);
                    } else if (i > 1) {
                        Assert.assertEquals(files, checkpointFileCount(instance));
                        Assert.assertEquals(entries, catalogueSize(instance));
                        Assert.assertEquals(retirementBytes, retirementQueueBytes(instance));
                    }
                }
                Assert.assertEquals("the purge must keep only the segments above the floor", 1, purgeBaseWalAndCountSegments());
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + HEAD_MISS_DEPARTURE_VIEW_SQL + ") ORDER BY ts, sym",
                        "(lv) ORDER BY ts, sym",
                        LOG,
                        true
                );
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testLateRowsTheFilterRejectsInsideASealPauseMoveTheHead() throws Exception {
        // The seal pause arms just ahead of the resume's head seal, as a third seal failure in a
        // row would. A re-stamp writes nothing, so it passes the pause and leaves it armed.
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, DepartureShape.UNSEALED, DrainPath.RAW_WAL, "-4.5");
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> instance.setSealCooldownUntilUs(currentMicros + 10 * Micros.MINUTE_MICROS)
                );
                commitLateRejected(job, DrainPath.RAW_WAL, lateTs(0), "-1.0", DEPARTURE_STEP_MICROS);
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, spliceZeroRowKeeps(job));
                commitLateRejected(job, DrainPath.RAW_WAL, lateTs(1), "-1.0", DEPARTURE_STEP_MICROS);
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));
                Assert.assertEquals(7, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(2, spliceZeroRowKeeps(job));
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals("the purge must keep only the segments above the floor", 7 - 6, purgeBaseWalAndCountSegments());
                assertCheckpointLadder(instance, false, DEPARTURE_LADDER);
                assertDepartedViewRows(DEPARTURE_ROWS);
            }
        });
    }

    @Test
    public void testNextSealAfterNoRowPublicationIsIncrementalFused() throws Exception {
        assertNextSealAfterNoRowPublicationIsIncremental(true);
    }

    @Test
    public void testNextSealAfterNoRowPublicationIsIncrementalUnfused() throws Exception {
        assertNextSealAfterNoRowPublicationIsIncremental(false);
    }

    @Test
    public void testNoRowPublicationAtTheDefaultCadence() throws Exception {
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                final long stepMicros = Micros.MINUTE_MICROS;
                commit(job, minute(1), "a", 1.5, stepMicros);
                final LiveViewInstance instance = instance();
                for (int i = 2; i <= 5; i++) {
                    commit(job, minute(i), "a", -1.0, stepMicros);
                }
                // Four minutes in: inside the five-minute default.
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);
                commit(job, minute(6), "a", -1.0, stepMicros);
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 1, 6);
            }
        });
    }

    @Test
    public void testNoRowPublicationCarriesOnlyTheBaselinesOfTheGenerationItReplaces() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                seedAccounts(job);
                final LiveViewInstance instance = instance();
                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull(window);
                final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
                Assert.assertEquals(1, window.getCheckpointBaselineGeneration());
                final LongList baselines = new LongList();
                int sealedFunctions = 0;
                for (int i = 0, n = functions.size(); i < n; i++) {
                    final long baseline = functions.getQuick(i).getCheckpointBaselineGeneration();
                    baselines.add(baseline);
                    if (baseline == 1) {
                        sealedFunctions++;
                    }
                }
                Assert.assertTrue("the seal must have stamped a function baseline", sealedFunctions > 0);

                final long headMaxTs = instance.getHeadCheckpointMaxTs();
                final long headRootId = instance.getHeadCheckpointRootId();
                final long tableId = instance.getLiveViewToken().getTableId();
                final long lifecycleIdentity = instance.getLifecycleIdentity();
                final ObjList<WindowFunction> noFunctions = new ObjList<>();
                try (
                        LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path dir = checkpointsDir(instance)
                ) {
                    // Over the generation the runtime's baselines name: they move.
                    LiveViewCheckpointTimelineStoreWriter.WatermarkResult result = writer.publishWatermark(
                            dir, tableId, 0, lifecycleIdentity, 1, headMaxTs, headRootId, functions, window, true
                    );
                    Assert.assertEquals(2, result.getGeneration());
                    Assert.assertEquals(2, window.getCheckpointBaselineGeneration());
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        Assert.assertEquals(
                                baselines.getQuick(i) == 1 ? 2 : baselines.getQuick(i),
                                functions.getQuick(i).getCheckpointBaselineGeneration()
                        );
                    }

                    // With no runtime named, nothing moves.
                    result = writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 1, headMaxTs, headRootId, noFunctions, null, true);
                    Assert.assertEquals(3, result.getGeneration());
                    Assert.assertEquals(2, window.getCheckpointBaselineGeneration());

                    // Over a generation the baselines do not name: they stay where they are.
                    result = writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 1, headMaxTs, headRootId, functions, window, true);
                    Assert.assertEquals(4, result.getGeneration());
                    Assert.assertEquals(2, window.getCheckpointBaselineGeneration());
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        Assert.assertEquals(
                                baselines.getQuick(i) == 1 ? 2 : baselines.getQuick(i),
                                functions.getQuick(i).getCheckpointBaselineGeneration()
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testNoRowPublicationFollowsTheAdaptiveCadence() throws Exception {
        // An anchored view's seals image incrementally, so a correction tightens its cadence.
        assertNoRowPublicationCadenceAfterACorrection(true, true);
    }

    @Test
    public void testNoRowPublicationKeepsConfiguredCadenceForUnanchoredViews() throws Exception {
        // An unanchored view images its whole map per seal: it records the correction but keeps
        // the configured cadence, so its head stays at the repair's seqTxn until the hour.
        assertNoRowPublicationCadenceAfterACorrection(true, false);
    }

    @Test
    public void testNoRowPublicationFollowsTheConfiguredCadenceWithoutAdaptation() throws Exception {
        assertNoRowPublicationCadenceAfterACorrection(false, false);
    }

    @Test
    public void testNoRowPublicationRefusesWhatItCannotVouchFor() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", -1.0);
                final LiveViewInstance instance = instance();
                final long headMaxTs = instance.getHeadCheckpointMaxTs();
                final long headRootId = instance.getHeadCheckpointRootId();
                final long tableId = instance.getLiveViewToken().getTableId();
                final long lifecycleIdentity = instance.getLifecycleIdentity();
                final ObjList<WindowFunction> noFunctions = new ObjList<>();
                try (
                        LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration);
                        Path dir = checkpointsDir(instance);
                        Path emptyDir = new Path().of(configuration.getDbRoot()).concat("lv_no_timeline")
                ) {
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 2, headMaxTs, headRootId, noFunctions, null, false),
                            "replica must not publish a live view checkpoint timeline"
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 2, headMaxTs, headRootId + 1, noFunctions, null, true),
                            "live view checkpoint watermark names a root that is not the newest [maxTimestamp="
                                    + headMaxTs + ", checkpointId=" + (headRootId + 1) + ']'
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 2, headMaxTs + 1, headRootId, noFunctions, null, true),
                            "live view checkpoint watermark names a root that is not the newest [maxTimestamp="
                                    + (headMaxTs + 1) + ", checkpointId=" + headRootId + ']'
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 0, lifecycleIdentity, 0, headMaxTs, headRootId, noFunctions, null, true),
                            "live view checkpoint generation watermarks must not move backwards [storedBase=1, nextBase=0]"
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId + 1, 0, lifecycleIdentity, 2, headMaxTs, headRootId, noFunctions, null, true),
                            "live view checkpoint watermark definition identity mismatch"
                    );
                    assertRefused(
                            () -> writer.publishWatermark(dir, tableId, 1, lifecycleIdentity, 2, headMaxTs, headRootId, noFunctions, null, true),
                            "live view checkpoint watermark definition identity mismatch"
                    );
                    Assert.assertEquals(0, configuration.getFilesFacade().mkdirs(emptyDir.slash(), configuration.getMkDirMode()));
                    assertRefused(
                            () -> writer.publishWatermark(emptyDir, tableId, 0, lifecycleIdentity, 2, headMaxTs, headRootId, noFunctions, null, true),
                            "cannot advance the base watermark of a live view checkpoint timeline with no valid generation"
                    );
                    // Every refusal left the published generation alone.
                    try (LiveViewCheckpointSuperblock superblock = new LiveViewCheckpointSuperblock(configuration)) {
                        superblock.of(dir);
                        Assert.assertEquals(1, superblock.generation);
                        Assert.assertEquals(1, superblock.normalizedBaseSeqTxn);
                    }
                }

                // The view goes on as if nothing had been tried.
                for (int i = 3; i <= 5; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(2, 1, 5);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testOutOfOrderRowAfterNoRowPublicationsMatchesRecompute() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertRejectedStretchAdvancedTheHead(instance());

                // Between the two roots, below the head the stretch republished.
                commit(job, "2026-01-01T00:01:30.000000Z", "a", 9.5);
                assertViewMatchesRecompute();
                // And one the filter rejects, below the frontier too.
                commit(job, "2026-01-01T00:01:40.000000Z", "a", -9.5);
                assertViewMatchesRecompute();
                // The repair of that last commit splices the timeline, and its newest root
                // already sits at the frontier, so it re-stamps the head without a root
                // identity. The cadence still moves the head, naming the root by timestamp.
                final LiveViewInstance instance = instance();
                Assert.assertEquals(15, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
                for (int i = 16; i <= 18; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(15, instance.getHeadCheckpointBaseSeqTxn());
                commit(job, minute(19), "a", -1.0);
                Assert.assertEquals(19, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(Numbers.LONG_NULL, instance.getHeadCheckpointRootId());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(15, instance.getCheckpointTimelineWalPurgeFloor());
                commit(job, minute(20), "a", -1.0);
                commit(job, minute(21), "a", 4.5);
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(22), "a", 5.5);
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRestartAfterNoRowPublicationsRestoresFromTheTimeline() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                Assert.assertEquals(1, instance().getHeadCheckpointBaseSeqTxn());
            }

            // The restore replays seqTxn 2, so the runtime stands above the restored root, and
            // the head this process never wrote seals at the first cycle without rows.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(3), "a", -1.0);
                assertRestoredFromTimeline("lv");
                final LiveViewInstance instance = instance();
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 2, 3);
                commit(job, minute(4), "a", -1.0);
                Assert.assertEquals(3, instance.getHeadCheckpointBaseSeqTxn());
            }

            // The restore replays seqTxn 4, which the filter rejects, so the runtime is the
            // restored root, and the first cycle without rows publishes it at once.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(5), "a", -1.0);
                assertRestoredFromTimeline("lv");
                final LiveViewInstance instance = instance();
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(3, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(3, 2, 5);
                commit(job, minute(6), "a", 3.5);
                assertViewMatchesRecompute();
            }

            // A restore from that generation replays only seqTxn 6.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(7), "a", 4.5);
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRowNumberViewAdvancesTheHead() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // row_number() keeps no incremental baseline of its own.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, x, row_number() OVER w rn FROM base WHERE x > 0 " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "b", 2.5);
                for (int i = 3; i <= 13; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertRejectedStretchAdvancedTheHead(instance());
                assertRowNumberViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(14), "a", 3.5);
                commit(job, minute(15), "b", 4.5);
                assertRestoredFromTimeline("lv");
                assertRowNumberViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testRuntimeAtTheHeadRootAdvancesWithoutANewRoot() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                final LiveViewInstance instance = instance();
                final long headRootId = instance.getHeadCheckpointRootId();
                Assert.assertNotEquals(Numbers.LONG_NULL, headRootId);

                for (int i = 2; i <= 4; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                // 900ms since the seal: inside the cadence.
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);

                commit(job, minute(5), "a", -1.0);
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 1, 5);

                for (int i = 6; i <= 9; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(9, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(5, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(3, 1, 9);
                // Still the one root, and the runtime still stands on it.
                Assert.assertEquals(headRootId, instance.getHeadCheckpointRootId());
                Assert.assertEquals(ts(minute(1)), instance.getHeadCheckpointMaxTs());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheCheckpointDirectoryStaysBoundedOverNoRowPublications() throws Exception {
        configureCadence(0);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                final LiveViewInstance instance = instance();
                // A zero cadence publishes on every cycle without rows.
                for (int i = 2; i <= 11; i++) {
                    commit(job, minute(i), "a", -1.0);
                    Assert.assertEquals(i, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(i - 1, instance.getCheckpointTimelineWalPurgeFloor());
                }
                assertTimeline(11, 1, 11);
                final int filesAfterTen = checkpointFileCount(instance);
                final long catalogueAfterTen = catalogueSize(instance);
                for (int i = 12; i <= 41; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                assertTimeline(41, 1, 41);
                Assert.assertEquals(
                        "every publication supersedes a catalogue segment, which the sweep must reclaim",
                        filesAfterTen,
                        checkpointFileCount(instance)
                );
                Assert.assertEquals(
                        "the publications must carry the swept segments out of the catalogue",
                        catalogueAfterTen,
                        catalogueSize(instance)
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTiedRowsHoldTheHeadUntilARowAboveIt() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                // On the head's own timestamp: the head root does not hold it, and no
                // boundary above the head can.
                commit(job, minute(1), "b", 2.5);
                final LiveViewInstance instance = instance();
                for (int i = 3; i <= 8; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);
            }

            // The restore replays the tied row from seqTxn 2, which only holds while the
            // generation still starts its replay there.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, minute(9), "a", -1.0);
                assertRestoredFromTimeline("lv");
                final LiveViewInstance instance = instance();
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);
                assertViewMatchesRecompute();

                // The first row above it seals both, replacing the head root.
                commit(job, minute(10), "a", 3.5);
                Assert.assertEquals(10, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(10)), instance.getHeadCheckpointMaxTs());
                assertTimeline(2, 1, 10);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testZeroRowResumeAtTheHeadThenNoRowStretchReleasesBaseWal() throws Exception {
        // The newest row departs unsealed above the head's root, so the resume anchors on that
        // root, emits nothing and re-versions no root. Its splice publishes at the departure's
        // commit, and the head goes back on the root there. The runtime then holds the head root's
        // state with nothing fed since, and the stretch of rejected commits moves the head's base
        // coordinate on the duration cadence, with no root above the newest row the view holds.
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            final String ladder = """
                    2025-12-31T12:00:00.000000Z -> 1
                    2026-01-01T00:01:00.000000Z -> 2
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commitTheDayTheBaseLoses(job);
                commit(job, minute(1), "a", 1.5, DEPARTURE_STEP_MICROS);
                // 300ms after the root at 00:01, and 600ms after it the departure: inside the
                // cadence, so no root seals 00:02.
                commit(job, minute(2), "a", 2.5, STEP_MICROS);
                commit(job, minute(2), "a", -2.5, STEP_MICROS);
                final LiveViewInstance instance = instance();
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals(ts(minute(1)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(ts(minute(2)), instance.getLatestSeenTs());
                Assert.assertEquals(Long.MAX_VALUE, instance.getMinSeenTsSinceCheckpoint());
                final long generationBefore = publishedCheckpointGeneration(instance);
                // The resume re-stamped the head at seqTxn 5, 1.2s before seqTxn 6: each cycle is due.
                for (int i = 6; i <= 11; i++) {
                    commitRejected(job, DrainPath.RAW_WAL, i, DEPARTURE_STEP_MICROS);
                }
                Assert.assertEquals(generationBefore + 6, publishedCheckpointGeneration(instance));
                assertCheckpointLadder(instance, false, ladder);
                Assert.assertEquals(ts(minute(1)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(11, instance.getHeadCheckpointBaseSeqTxn());
                assertTimelineBase(2, 11);
                Assert.assertEquals(10, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals("the purge must keep only the segments above the floor", 11 - 10, purgeBaseWalAndCountSegments());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, sealAboveFrontierDeclines(job));
            }
            assertDepartureStateSurvivesTwoRestarts(
                    DrainPath.RAW_WAL,
                    12,
                    """
                            ts\tsym\tx\ts
                            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                            """
            );
        });
    }

    @Test
    public void testUnmatchedRowsAfterAnUnsealedRowSealItAtTheCadence() throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createFilteredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                final LiveViewInstance instance = instance();
                for (int i = 3; i <= 4; i++) {
                    commit(job, minute(i), "a", -1.0);
                }
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);
                // Due at seqTxn 5: the row seqTxn 2 put through the window gets a root of its
                // own, at the runtime frontier.
                commit(job, minute(5), "a", -1.0);
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                assertTimeline(2, 2, 5);
                assertViewMatchesRecompute();
            }
        });
    }

    private static void assertNoTimeline(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance); Path timelinePath = new Path()) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            Assert.assertFalse("the view keeps no timeline", configuration.getFilesFacade().exists(timelinePath.$()));
        }
    }

    private static void assertRefused(Runnable publication, String message) {
        try {
            publication.run();
            Assert.fail("the publication must be refused: " + message);
        } catch (CairoException e) {
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static void configureCadence(long durationMicros) {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, durationMicros);
    }

    private static LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        return instance;
    }

    private static String minute(int minute) {
        return "2026-01-01T00:" + (minute < 10 ? "0" : "") + minute + ":00.000000Z";
    }

    private static void assertNoneCatalogued(LiveViewInstance instance, Set<Long> segmentIds) {
        final Set<Long> catalogue = catalogue(instance);
        for (long segmentId : segmentIds) {
            Assert.assertFalse(
                    "an entry the last sweep proposed must leave the catalogue with the next publication, segmentId=" + segmentId,
                    catalogue.contains(segmentId)
            );
        }
    }

    // A timestamp between the root departTheNewestRow leaves at 00:02 and the departed row at 00:03.
    private static String lateTs(int i) {
        return "2026-01-01T00:02:" + (10 + i) + ".000000Z";
    }

    private static long sealAboveFrontierDeclines(LiveViewRefreshJob job) {
        return job.getStatsForTest().get(LiveViewRefreshStats.SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT);
    }

    private static long spliceZeroRowKeeps(LiveViewRefreshJob job) {
        return job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_ZERO_ROW_KEEP_COUNT);
    }

    private static void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * The consumed watermark's write fails once, on the cycle the cadence comes due. The
     * head must not move past what the state file holds, and the next cycle, whose write
     * goes through, moves it.
     */
    private void assertAFailedConsumedAdvanceHoldsTheHead(DrainPath drainPath) throws Exception {
        configureCadence(CADENCE_MICROS);
        final AtomicInteger stateWriteFailures = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME) && stateWriteFailures.getAndDecrement() > 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            if (drainPath == DrainPath.LEAD) {
                createFilteredView();
            } else {
                createDedupBaseView();
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5);
                commit(job, minute(2), "a", 2.5);
                commitRejected(job, drainPath, 3);
                commitRejected(job, drainPath, 4);
                final LiveViewInstance instance = instance();
                final long cleanCycles = instance.getDedupRawWalCleanCycles();

                stateWriteFailures.set(1);
                commitRejected(job, drainPath, 5);
                Assert.assertEquals("the one write the fault fails", -1, stateWriteFailures.get());
                Assert.assertEquals(5, instance.getLastProcessedSeqTxn());
                Assert.assertEquals(4, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);

                commitRejected(job, drainPath, 6);
                Assert.assertEquals(6, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                assertTimeline(2, 2, 6);
                Assert.assertEquals(
                        drainPath == DrainPath.RAW_WAL ? cleanCycles + 2 : cleanCycles,
                        instance.getDedupRawWalCleanCycles()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * What a late commit leaves when its resume emitted nothing over a root at the frontier, or
     * when a cycle without rows or a closing seal stamped the head: the head stands on the root at
     * 00:02 and names the commit, and the older superblock slot the commit before it.
     */
    private void assertLateCommitStampedTheHead(LiveViewRefreshJob job, LiveViewInstance instance, long seqTxn, long zeroRowKeeps) throws Exception {
        Assert.assertEquals(seqTxn, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(zeroRowKeeps, spliceZeroRowKeeps(job));
        assertTimelineBase(3, seqTxn);
        Assert.assertEquals(seqTxn - 1, instance.getCheckpointTimelineWalPurgeFloor());
    }

    /**
     * Late rows the filter rejects, from {@code firstSeqTxn} on, each one's resume emitting nothing
     * over the head root. Every splice registers the catalogue segment the publication before it
     * wrote and carries out the entries the last sweep proposed, and every re-stamp sweeps: from the
     * second commit on (the third over more than one catalogue leaf), the directory holds the same
     * files, catalogue entries and retirement queue bytes. When {@code isOneLeaf}, the catalogue
     * fits one leaf, so each splice supersedes exactly
     * the one catalogue segment the commit before it wrote: one superseded segment the older slot
     * protects stays on disk, and one entry whose file the sweep unlinked waits for the next splice.
     */
    private void assertLateRejectedStretchKeepsTheCheckpointDirectoryBounded(
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            long firstSeqTxn,
            boolean isOneLeaf
    ) throws Exception {
        // A splice path-copies only the catalogue leaves it touches, so over more than one leaf
        // the segment the departure's seal wrote can keep a live leaf page past the first late
        // commit, whose sweep then has nothing to unlink: the stretch settles one commit later.
        final int steadyFrom = isOneLeaf ? 1 : 2;
        int files = -1;
        long entries = -1;
        long retirementBytes = -1;
        Set<Long> deadBefore = null;
        for (int i = 0; i < 30; i++) {
            commitLateRejected(job, DrainPath.RAW_WAL, lateTs(i), "-1.0", DEPARTURE_STEP_MICROS);
            Assert.assertEquals(firstSeqTxn + i, instance.getHeadCheckpointBaseSeqTxn());
            Assert.assertEquals(i + 1, spliceZeroRowKeeps(job));
            if (deadBefore != null) {
                assertNoneCatalogued(instance, deadBefore);
            }
            final Set<Long> dead = deadCatalogueEntries(instance);
            if (isOneLeaf && i > 0) {
                Assert.assertEquals(1, uncollectedSegmentCount(instance));
                Assert.assertEquals(1, dead.size());
            }
            if (i == steadyFrom) {
                files = checkpointFileCount(instance);
                entries = catalogueSize(instance);
                retirementBytes = retirementQueueBytes(instance);
            } else if (i > steadyFrom) {
                Assert.assertEquals(files, checkpointFileCount(instance));
                Assert.assertEquals(entries, catalogueSize(instance));
                Assert.assertEquals(retirementBytes, retirementQueueBytes(instance));
            }
            deadBefore = dead;
        }
        assertCatalogueMatchesDisk(instance);
        Assert.assertEquals(0, instance.getCheckpointSealFailures());
    }

    /**
     * The departure {@link #departTheNewestRow} builds, then six late rows the filter rejects, at
     * 00:02:10 to 00:02:15, between the head root and the departed row, each refreshed 1.2s after
     * the one before. Each one's resume anchors on the head root, emits nothing and splices at its
     * commit, so the head goes back on that root there: the base WAL below the commit goes, and a
     * restart restores the generation the head names.
     */
    private void assertLateRowsTheFilterRejectsAfterADepartureMoveTheHead(
            DepartureShape shape,
            DrainPath drainPath,
            String lateX
    ) throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            if (drainPath == DrainPath.LEAD) {
                createFilteredView();
            } else {
                createDedupBaseView();
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, shape, drainPath, "-4.5");
                Assert.assertEquals(0, spliceZeroRowKeeps(job));
                final long generationBefore = publishedCheckpointGeneration(instance);
                for (int k = 6; k <= 11; k++) {
                    commitLateRejected(job, drainPath, lateTs(k - 6), lateX, DEPARTURE_STEP_MICROS);
                    assertLateCommitStampedTheHead(job, instance, k, k - 5);
                    // The splice is the commit's one publication: the re-stamp publishes nothing.
                    Assert.assertEquals(generationBefore + k - 5, publishedCheckpointGeneration(instance));
                }
                assertCheckpointLadder(instance, false, DEPARTURE_LADDER);
                Assert.assertEquals(11, instance.getStateReader().getLvConsumedSeqTxn());
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                Assert.assertEquals(0, sealAboveFrontierDeclines(job));
                Assert.assertEquals("the purge must keep only the segments above the floor", 11 - 10, purgeBaseWalAndCountSegments());
                assertDepartedViewRows(DEPARTURE_ROWS);
            }
            // The restore stamps the head at the generation the last splice published, where the
            // re-stamp had already put it.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertRestoredFromTimeline("lv");
            Assert.assertEquals(11, instance().getHeadCheckpointBaseSeqTxn());
            assertDepartedViewRows(DEPARTURE_ROWS);
            assertDepartureStateSurvivesTwoRestarts(drainPath, 12, DEPARTURE_ROWS);
        });
    }

    private void assertDepartedViewRows(String rows) throws Exception {
        assertQuery("SELECT ts, sym, x, s FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(rows);
    }

    /**
     * Two restarts around three rejected commits from {@code firstSeqTxn}. Each restart puts the
     * head and the newest timestamp seen at the newest row the view holds, where the newest root
     * sits, so every cycle without rows publishes: the first at once, for a head this process
     * restored rather than wrote, the others 1.2s after the one before. The second restart
     * restores from the timeline the first left.
     */
    private void assertDepartureStateSurvivesTwoRestarts(DrainPath drainPath, int firstSeqTxn, String rows) throws Exception {
        restartCycle();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            for (int i = firstSeqTxn; i < firstSeqTxn + 3; i++) {
                commitRejected(job, drainPath, i, DEPARTURE_STEP_MICROS);
                Assert.assertEquals(i, instance().getHeadCheckpointBaseSeqTxn());
            }
            assertRestoredFromTimeline("lv");
            final LiveViewInstance instance = instance();
            Assert.assertEquals(0, instance.getCheckpointSealFailures());
            Assert.assertEquals(0, sealAboveFrontierDeclines(job));
            assertDepartedViewRows(rows);
        }
        assertRestartRestoresTheDepartedRows(rows);
    }

    /**
     * The departure {@link #departTheNewestRow} builds, then the rejected commits 6 to 11, each
     * refreshed 1.2s after the one before and so each due, then {@code firstEvent} and the
     * restarts after it.
     */
    private void assertDepartureThenNoRowStretch(
            DepartureShape shape,
            DrainPath drainPath,
            String departingX,
            FirstEvent firstEvent
    ) throws Exception {
        configureWalPurge();
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            if (drainPath == DrainPath.LEAD) {
                createFilteredView();
            } else {
                createDedupBaseView();
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = departTheNewestRow(job, shape, drainPath, departingX);
                final long generationBefore = publishedCheckpointGeneration(instance);
                for (int i = 6; i <= 11; i++) {
                    commitRejected(job, drainPath, i, DEPARTURE_STEP_MICROS);
                }
                Assert.assertEquals("one publication per cycle", generationBefore + 6, publishedCheckpointGeneration(instance));
                assertDepartureStretchAdvancedTheHead(job, instance);
                Assert.assertEquals(0, instance.getCheckpointSealFailures());
                if (firstEvent == FirstEvent.SCHEMA_CHANGE) {
                    final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
                    final long rebuildAttemptsBefore = instance.getCheckpointRebuildAttempts();
                    final long resetsBefore = instance.getCheckpointTimelineResets();
                    execute("ALTER TABLE base ADD COLUMN y INT");
                    refresh(job, DEPARTURE_STEP_MICROS);
                    // The drift recovery restores the runtime from the timeline in place.
                    Assert.assertEquals(runtimeRestoresBefore + 1, instance.getCheckpointRuntimeRestores());
                    Assert.assertEquals(rebuildAttemptsBefore, instance.getCheckpointRebuildAttempts());
                    for (int i = 13; i <= 15; i++) {
                        commitRejected(job, drainPath, i, DEPARTURE_STEP_MICROS);
                    }
                    Assert.assertEquals(0, instance.getCheckpointSealFailures());
                    Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                    Assert.assertEquals(15, instance.getHeadCheckpointBaseSeqTxn());
                    Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
                    assertCheckpointLadder(instance, false, DEPARTURE_LADDER);
                }
            }
            if (firstEvent == FirstEvent.RESTART) {
                assertDepartureStateSurvivesTwoRestarts(drainPath, 12, DEPARTURE_ROWS);
            } else {
                assertRestartRestoresTheDepartedRows(DEPARTURE_ROWS);
            }
        });
    }

    /**
     * What the rejected commits 6 to 11 after {@link #departTheNewestRow} leave: each cycle was
     * due, so each published the root at the newest row the view holds at its own commit, and
     * none sealed a root above it.
     */
    private void assertDepartureStretchAdvancedTheHead(LiveViewRefreshJob job, LiveViewInstance instance) throws Exception {
        assertCheckpointLadder(instance, false, DEPARTURE_LADDER);
        Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(11, instance.getStateReader().getLvConsumedSeqTxn());
        Assert.assertEquals(11, instance.getHeadCheckpointBaseSeqTxn());
        assertTimelineBase(3, 11);
        // The older of the two superblock slots.
        Assert.assertEquals(10, instance.getCheckpointTimelineWalPurgeFloor());
        Assert.assertEquals("the purge must keep only the segments above the floor", 11 - 10, purgeBaseWalAndCountSegments());
        Assert.assertEquals(0, sealAboveFrontierDeclines(job));
    }

    private void assertRestartRestoresTheDepartedRows(String rows) throws Exception {
        restartCycle();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        assertRestoredFromTimeline("lv");
        Assert.assertFalse(instance().isCheckpointRecoveryBlocked());
        assertDepartedViewRows(rows);
    }

    private void assertTimelineBase(long entries, long normalizedBaseSeqTxn) throws Exception {
        assertQuery(
                "SELECT checkpoint_timeline_entries, checkpoint_timeline_normalized_base_seqtxn FROM live_views() WHERE view_name = 'lv'"
        ).noLeakCheck().noRandomAccess().returns(
                "checkpoint_timeline_entries\tcheckpoint_timeline_normalized_base_seqtxn\n" + entries + "\t" + normalizedBaseSeqTxn + "\n"
        );
    }

    private void assertAnchoredViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, " +
                        "sum(amount) OVER (PARTITION BY sym, bucket ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s, " +
                        "sum(fee) OVER (PARTITION BY sym, bucket ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) f " +
                        "FROM (SELECT *, timestamp_floor('1d', ts) bucket FROM base WHERE amount > 0)) ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * What the stretch every case shares leaves behind: seqTxn 1 and 2 matched, 3 to 13
     * did not. The cadence came due at 5, 9 and 13. At 5 the row seqTxn 2 put through the
     * window was not sealed yet, so it got a root of its own; at 9 and 13 the runtime was
     * that root, so they moved the generation's base coordinate alone.
     */
    private void assertNextSealAfterNoRowPublicationIsIncremental(boolean isFused) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createAnchoredView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                seedAccounts(job);
                final LiveViewInstance instance = instance();
                for (int i = 2; i <= 5; i++) {
                    execute("INSERT INTO base VALUES ('2026-01-01T11:00:" + (10 + i) + ".000000Z', 'acct-1', -1.0, 1.00::decimal(38,2))");
                    refresh(job);
                }
                // The runtime was the seed's root, so seqTxn 5 republished it.
                Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(2, 1, 5);

                final long windowCaptures = instance.getCheckpointCaptureLedgerForTest().getWindowCaptures();
                final long windowIncremental = instance.getCheckpointCaptureLedgerForTest().getWindowIncrementalCaptures();
                final long windowImaged = instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged();
                final long functionCaptures = instance.getCheckpointCaptureLedgerForTest().getFunctionCaptures();
                final long functionIncremental = instance.getCheckpointCaptureLedgerForTest().getFunctionIncrementalCaptures();
                final long functionImaged = instance.getCheckpointCaptureLedgerForTest().getFunctionKeysImaged();

                // One key moves, and the cadence is due: the seal images that key alone, on
                // top of the root the publication carried forward.
                setCurrentMicros(currentMicros + CADENCE_MICROS);
                execute("INSERT INTO base VALUES ('2026-01-01T11:00:20.000000Z', 'acct-3', 1.0, 1.25::decimal(38,2))");
                refresh(job);
                Assert.assertEquals(6, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(3, 2, 6);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowCaptures() - windowCaptures);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowIncrementalCaptures() - windowIncremental);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged() - windowImaged);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionCaptures() - functionCaptures);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionIncrementalCaptures() - functionIncremental);
                Assert.assertEquals(1, instance.getCheckpointCaptureLedgerForTest().getFunctionKeysImaged() - functionImaged);
                assertAnchoredViewMatchesRecompute();
            }

            // The incremental root restores with the keys it did not image.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T11:00:21.000000Z', 'acct-4', 1.0, 1.50::decimal(38,2))");
                refresh(job);
                assertRestoredFromTimeline("lv");
                assertAnchoredViewMatchesRecompute();
            }
        });
    }

    /**
     * A late row teaches the adaptive cadence a ten-minute correction depth, which halves to
     * a five-minute cadence under the hour configured. Cycles without rows follow whichever
     * cadence is in force.
     */
    private void assertNoRowPublicationCadenceAfterACorrection(boolean isAdaptive, boolean isAnchored) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, String.valueOf(isAdaptive));
        configureCadence(60 * Micros.MINUTE_MICROS);
        // Only an anchored view's seals image incrementally, so only it is tightened. An unanchored
        // view images its whole key map per seal, so it records the correction but keeps the ceiling.
        final boolean tightens = isAdaptive && isAnchored;
        assertMemoryLeak(() -> {
            if (isAnchored) {
                createAnchoredFilteredView();
            } else {
                createFilteredView();
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(0), "a", 1.0);
                commit(job, minute(20), "a", 2.0);
                commit(job, minute(10), "a", 3.0);
                final LiveViewInstance instance = instance();
                if (tightens) {
                    // depth 10 min halves to 5 min, above the one-minute floor.
                    Assert.assertEquals(
                            5 * Micros.MINUTE_MICROS,
                            instance.getEffectiveCheckpointDurationMicros(60 * Micros.MINUTE_MICROS)
                    );
                } else if (isAdaptive) {
                    // The correction is recorded, but the unanchored view stays at the ceiling.
                    Assert.assertEquals(
                            60 * Micros.MINUTE_MICROS,
                            instance.getEffectiveCheckpointDurationMicros(60 * Micros.MINUTE_MICROS)
                    );
                    Assert.assertEquals(1, instance.getAdaptiveCheckpointCorrectionCount());
                    Assert.assertEquals(10 * Micros.MINUTE_MICROS, instance.getAdaptiveCheckpointLastCorrectionDepthMicros());
                }
                final long repairedBase = instance.getHeadCheckpointBaseSeqTxn();
                Assert.assertEquals(3, repairedBase);

                // Four minutes of rejected commits: inside either cadence.
                for (int i = 21; i <= 24; i++) {
                    commit(job, minute(i), "a", -1.0, Micros.MINUTE_MICROS);
                }
                Assert.assertEquals(repairedBase, instance.getHeadCheckpointBaseSeqTxn());
                // The fifth minute: due under the learned 5-minute cadence alone.
                commit(job, minute(25), "a", -1.0, Micros.MINUTE_MICROS);
                Assert.assertEquals(tightens ? 8 : repairedBase, instance.getHeadCheckpointBaseSeqTxn());
                // An hour on: due under either.
                setCurrentMicros(currentMicros + 60 * Micros.MINUTE_MICROS);
                commit(job, minute(26), "a", -1.0);
                Assert.assertEquals(9, instance.getHeadCheckpointBaseSeqTxn());
                if (isAnchored) {
                    assertAnchoredFilteredViewMatchesRecompute();
                } else {
                    assertViewMatchesRecompute();
                }
            }
        });
    }

    private void assertAnchoredFilteredViewMatchesRecompute() throws Exception {
        // Every test row sits in the 2026-01-01 anchor bucket, so ANCHOR DAILY equals an
        // unbounded-preceding cumulative sum partitioned by sym.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s "
                        + "FROM base WHERE x > 0 ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void assertRejectedStretchAdvancedTheHead(LiveViewInstance instance) throws Exception {
        Assert.assertEquals(13, instance.getStateReader().getLvConsumedSeqTxn());
        Assert.assertEquals(13, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
        // The older of the two superblock slots.
        Assert.assertEquals(9, instance.getCheckpointTimelineWalPurgeFloor());
        assertTimeline(4, 2, 13);
    }

    private void assertRowNumberViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, x, row_number() OVER (PARTITION BY sym, bucket ORDER BY ts) rn " +
                        "FROM (SELECT *, timestamp_floor('1d', ts) bucket FROM base WHERE x > 0)) ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * A root at 00:01 (seqTxn 1), then rows the filter rejects, each past the one-second cadence,
     * whose publications fail three times in a row (2 to 4): that pauses the publication over
     * the intact timeline. The pause holds a fourth (5), though the fault persists and the cadence
     * is due. The fault clears, and the first rejected row after the pause (6) publishes, which a
     * restart restores from.
     */
    private void assertRowlessPublicationFaultSpanningThreeCyclesPauses(DrainPath drainPath) throws Exception {
        configureCadence(CADENCE_MICROS);
        assertMemoryLeak(() -> {
            createDedupBaseView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(START_MICROS);
                commit(job, minute(1), "a", 1.5, DEPARTURE_STEP_MICROS);
                final LiveViewInstance instance = instance();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                assertTimeline(1, 1, 1);
                job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH);
                for (int i = 2; i <= 4; i++) {
                    commitRejected(job, drainPath, i, DEPARTURE_STEP_MICROS);
                    Assert.assertEquals(i - 1, instance.getCheckpointSealFailures());
                }
                Assert.assertEquals("the third failure pauses", resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(1, instance.getCheckpointTimelineWalPurgeFloor());
                Assert.assertEquals(ts(minute(1)), instance.getHeadCheckpointMaxTs());
                assertTimeline(1, 1, 1);
                Assert.assertTrue(instance.isSealOnCooldown(currentMicros));

                commitRejected(job, drainPath, 5, DEPARTURE_STEP_MICROS);
                Assert.assertEquals("the pause holds the publication", 3, instance.getCheckpointSealFailures());
                Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
                assertTimeline(1, 1, 1);

                job.setCheckpointTimelineTestFailureStage(0);
                setCurrentMicros(currentMicros + Micros.MINUTE_MICROS + Micros.SECOND_MICROS);
                commitRejected(job, drainPath, 6, DEPARTURE_STEP_MICROS);
                Assert.assertEquals(3, instance.getCheckpointSealFailures());
                Assert.assertEquals(resetsBefore, instance.getCheckpointTimelineResets());
                Assert.assertEquals("the first cycle after the pause publishes", 6, instance.getHeadCheckpointBaseSeqTxn());
                Assert.assertEquals(ts(minute(1)), instance.getHeadCheckpointMaxTs());
                Assert.assertFalse(instance.isSealOnCooldown(currentMicros));
                assertTimeline(2, 1, 6);
            }
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertRestoredFromTimeline("lv");
            assertViewMatchesRecompute();
        });
    }

    private void assertTimeline(long generation, long entries, long normalizedBaseSeqTxn) throws Exception {
        assertQuery(
                "SELECT checkpoint_timeline_generation, checkpoint_timeline_entries, " +
                        "checkpoint_timeline_normalized_base_seqtxn FROM live_views() WHERE view_name = 'lv'"
        ).noLeakCheck().noRandomAccess().returns(
                "checkpoint_timeline_generation\tcheckpoint_timeline_entries\tcheckpoint_timeline_normalized_base_seqtxn\n" +
                        generation + "\t" + entries + "\t" + normalizedBaseSeqTxn + "\n"
        );
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s " +
                        "FROM base WHERE x > 0 ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void commit(LiveViewRefreshJob job, String ts, String sym, double x) throws Exception {
        commit(job, ts, sym, x, STEP_MICROS);
    }

    private void commit(LiveViewRefreshJob job, String ts, String sym, double x, long stepMicros) throws Exception {
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + ts + "', '" + sym + "', " + x + ")");
        refresh(job, stepMicros);
    }

    // One row of b the filter rejects at ts; twice in one commit on the applied-base drain.
    private void commitLateRejected(LiveViewRefreshJob job, DrainPath drainPath, String ts, String x, long stepMicros) throws Exception {
        final String row = "('" + ts + "', 'b', " + x + ")";
        execute("INSERT INTO base (ts, sym, x) VALUES " + (drainPath == DrainPath.APPLIED_BASE ? row + ", " + row : row));
        refresh(job, stepMicros);
    }

    private void commitRejected(LiveViewRefreshJob job, DrainPath drainPath, int minute) throws Exception {
        commitRejected(job, drainPath, minute, STEP_MICROS);
    }

    private void commitRejected(LiveViewRefreshJob job, DrainPath drainPath, int minute, long stepMicros) throws Exception {
        if (drainPath == DrainPath.APPLIED_BASE) {
            // The dedup keys collapse the repeated row, so the applied base differs from the
            // raw WAL, and the cycle reads the applied base.
            execute("INSERT INTO base (ts, sym, x) VALUES ('" + minute(minute) + "', 'a', -1.0), ('" + minute(minute) + "', 'a', -1.0)");
            refresh(job, stepMicros);
        } else {
            commit(job, minute(minute), "a", -1.0, stepMicros);
        }
    }

    // The one row of a day the base then drops (seqTxn 1 and 2), which the view walks past and
    // keeps: a rebuild from the applied base would lose it, and the restatement guard refuses one.
    private void commitTheDayTheBaseLoses(LiveViewRefreshJob job) throws Exception {
        commit(job, "2025-12-31T12:00:00.000000Z", "a", 0.5, DEPARTURE_STEP_MICROS);
        execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
        refresh(job, DEPARTURE_STEP_MICROS);
    }

    /**
     * Over the day the base loses (seqTxn 1 and 2), a root at 00:01 (3), then 00:02 and 00:03 in
     * one commit (4), which a root at 00:03 seals for {@link DepartureShape#ONE_ROOT} and the
     * cadence leaves unsealed for {@link DepartureShape#UNSEALED}. Then 00:03 departs (5): an
     * upsert to {@code departingX}, or a REPLACE_RANGE deleting it on the lead drain's base. The
     * resume repairing it leaves a root and the head at 00:02, with nothing fed since, and the
     * newest timestamp the view has seen still at 00:03.
     */
    private LiveViewInstance departTheNewestRow(
            LiveViewRefreshJob job,
            DepartureShape shape,
            DrainPath drainPath,
            String departingX
    ) throws Exception {
        setCurrentMicros(START_MICROS);
        commitTheDayTheBaseLoses(job);
        commit(job, minute(1), "a", 1.5, DEPARTURE_STEP_MICROS);
        execute("INSERT INTO base (ts, sym, x) VALUES ('" + minute(2) + "', 'a', 2.5), ('" + minute(3) + "', 'a', 4.5)");
        refresh(job, shape == DepartureShape.ONE_ROOT ? DEPARTURE_STEP_MICROS : STEP_MICROS);
        final LiveViewInstance instance = instance();
        Assert.assertEquals(ts(minute(shape == DepartureShape.ONE_ROOT ? 3 : 1)), instance.getHeadCheckpointMaxTs());
        if (drainPath == DrainPath.LEAD) {
            replaceBaseRange(ts(minute(3)), ts(minute(3)) + 1, Double.NaN);
        } else {
            execute("INSERT INTO base (ts, sym, x) VALUES ('" + minute(3) + "', 'a', " + departingX + ")");
        }
        refresh(job, DEPARTURE_STEP_MICROS);
        assertCheckpointLadder(instance, false, DEPARTURE_LADDER);
        Assert.assertEquals(ts(minute(2)), instance.getHeadCheckpointMaxTs());
        Assert.assertEquals(5, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(ts(minute(3)), instance.getLatestSeenTs());
        Assert.assertEquals(Long.MAX_VALUE, instance.getMinSeenTsSinceCheckpoint());
        assertDepartedViewRows(DEPARTURE_ROWS);
        return instance;
    }

    private void createAnchoredView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, amount DOUBLE, fee DECIMAL(38,2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        createAnchoredViewOverBase();
    }

    private void createAnchoredViewOverBase() throws Exception {
        // The DECIMAL sum keeps a root of its own beside the window's, fused or not.
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                "SELECT ts, sym, sum(amount) OVER w AS s, sum(fee) OVER w AS f FROM base WHERE amount > 0 " +
                "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
    }

    private void createAnchoredFilteredView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                "SELECT ts, sym, x, sum(x) OVER w s FROM base WHERE x > 0 " +
                "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
    }

    private void createDedupBaseView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        createView();
    }

    private void createFilteredView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        createView();
    }

    private void createView() throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s " +
                "FROM base WHERE x > 0");
    }

    private void refresh(LiveViewRefreshJob job) {
        refresh(job, STEP_MICROS);
    }

    private void refresh(LiveViewRefreshJob job, long stepMicros) {
        drainWalQueue();
        setCurrentMicros(currentMicros + stepMicros);
        drainJob(job);
        drainWalQueue();
    }

    private void refreshToQuiescence(LiveViewRefreshJob job, long stepMicros) {
        drainWalQueue();
        setCurrentMicros(currentMicros + stepMicros);
        driveRefreshToQuiescence(job);
    }

    /**
     * Eight accounts in one commit, seqTxn 1, which seals the first root over all of them.
     */
    private void seedAccounts(LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO base SELECT ('2026-01-01T11:00:00.000000Z'::timestamp + x * 1_000_000)::timestamp, " +
                "('acct-' || x)::symbol, x * 1.0, (x * 0.25)::decimal(38,2) FROM long_sequence(8)");
        refresh(job);
        final LiveViewInstance instance = instance();
        Assert.assertEquals(1, instance.getHeadCheckpointBaseSeqTxn());
        Assert.assertEquals(8, instance.getCheckpointCaptureLedgerForTest().getWindowKeysImaged());
        assertTimeline(1, 1, 1);
    }

    private enum DepartureShape {
        // 00:02 and 00:03 sealed under one root at 00:03: the resume's splice re-versions that
        // root, truncates it at the new frontier and seals a root there.
        ONE_ROOT,
        // 00:02 and 00:03 unsealed above the root at 00:01: the resume seals the new frontier.
        UNSEALED
    }

    private enum FirstEvent {
        // A restart, which restores the runtime from the timeline.
        RESTART,
        // A base column added, whose drift recovery restores the runtime in place.
        SCHEMA_CHANGE
    }

    private enum DrainPath {
        // A dedup base whose rejected commits collapse rows: the coupled applied-base drain.
        APPLIED_BASE,
        // A plain base: the lead drain and its cadence flush.
        LEAD,
        // A dedup base whose commits collapse nothing: the coupled raw-WAL drain.
        RAW_WAL
    }
}
