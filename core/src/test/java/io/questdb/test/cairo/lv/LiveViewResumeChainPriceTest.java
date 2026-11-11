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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointCaptureLedger;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRowsBounds;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewResumeChainPrice;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Files;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The price an out-of-order resume pays to keep its checkpoint chain. Every function a seal
 * walks whole - one outside the ANCHOR window, and anchored lag, rank, dense_rank,
 * corr/covar*, EXCLUDE CURRENT ROW calls and row_number past the window's leaf budget - images
 * all its live keys at each chained boundary. A resume keeps its chain only while the keys its
 * boundaries image whole stay within B = max(the resume's estimated base rows,
 * cairo.live.view.checkpoint.repair.scan.max.keys), and checks that at every boundary before
 * it freezes it, with the live maps there: boundary i is refused when
 * max(K_front, K_i) x (R - i) > B - S_i, where K_front is the keys at the frontier, K_i the
 * keys at boundary i, and S_i the keys the boundaries already admitted imaged.
 * <p>
 * The fixture: one root per refreshed commit; hour 0 holds keys s1..sK in one commit, the last
 * at 00:59:59.999999, which is the anchor A; hour 1 holds R in-order commits on s1, each a root;
 * the late commit lands in hour 1 below every one of them. The base partitions by hour and
 * deduplicates, so the late commit takes the whole-range resume from A, and the plan's estimate
 * is every base row of hour 1 (and of the hours above it), counted exactly. Every expected
 * decision comes from that arithmetic, by hand.
 */
public class LiveViewResumeChainPriceTest extends AbstractLiveViewTest {
    private static final String ANCHOR_WINDOW = " WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final String DECLINE_LOG = "live view O3 resume declined the checkpoint chain, truncating instead [view=lv";
    // 2026-01-01T00:00:00Z
    private static final long DAY_ZERO_MICROS = 1_767_225_600_000_000L;
    private static final long HOUR_MICROS = 3_600_000_000L;
    private static final String RANGE_SUM = "sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '1' HOUR PRECEDING AND CURRENT ROW)";
    private static final String ROWS_AVG = "avg(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
    private static final String ROWS_SUM = "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";

    @Test
    public void testAnchoredIncrementalFamiliesAreNotPriced() throws Exception {
        // Every seal of these images only the keys a correction touched, so nothing is imaged
        // whole and the price is never wired: 3 x 4 = 12 > 4 would decline any of them.
        final String[] projections = {
                "sum(x) OVER w s", "count() OVER w s", "avg(x) OVER w s", "max(x) OVER w s", "ksum(x) OVER w s",
                "stddev_samp(x) OVER w s", "row_number() OVER w s", "sum(d) OVER w s", "first_value(x) OVER w s",
                "nth_value(x, 2) OVER w s", "max(d) OVER w s", "avg(x, 'period', 5) OVER w s",
                "avg(x, 'period', 5, y) OVER w s", sums16() + ", avg(x) OVER w s"
        };
        for (boolean isFused : new boolean[]{true, false}) {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
            for (String projection : projections) {
                assertChainDecision(projection + ANCHOR_WINDOW, 4, new int[]{1, 1, 1}, row("01:00:30", "s2"), 1, false);
            }
        }
    }

    @Test
    public void testAnchoredWholeImagedFamiliesArePriced() throws Exception {
        // An ANCHOR window, yet every seal images these whole: lag keeps no dirty set, rank and
        // the bivariate calls keep no baseline, and an EXCLUDE CURRENT ROW call is a ring. K = 4,
        // R = 3: est 12 keeps the chain, est 11 declines it.
        final String[] projections = {
                "lag(x) OVER w s", "lag(x, 2) OVER w s", "lag(d) OVER w s", "rank() OVER w s", "dense_rank() OVER w s",
                "corr(x, y) OVER w s", "covar_pop(x, y) OVER w s", "covar_samp(x, y) OVER w s"
        };
        for (String projection : projections) {
            assertChainDecision(projection + ANCHOR_WINDOW, 4, new int[]{3, 4, 4}, row("01:00:30", "s2"), 1, false);
            assertChainDecision(projection + ANCHOR_WINDOW, 4, new int[]{3, 3, 4}, row("01:00:30", "s2"), 1, true);
        }
        final String[] excluded = {"sum(x)", "avg(x)", "count(x)", "sum(d)", "first_value(x)", "last_value(x)", "nth_value(x, 2)"};
        for (String call : excluded) {
            final String window = " WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW ANCHOR DAILY '00:00')";
            assertChainDecision(call + " OVER w s" + window, 4, new int[]{3, 4, 4}, row("01:00:30", "s2"), 1, false);
            assertChainDecision(call + " OVER w s" + window, 4, new int[]{3, 3, 4}, row("01:00:30", "s2"), 1, true);
        }
    }

    @Test
    public void testBoundaryCountGuardDeclinesFirst() throws Exception {
        // The count guard declines before the price runs, so the price's stat stays put.
        for (int max : new int[]{2, 0}) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, max);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
            final LogCapture capture = new LogCapture();
            try {
                assertMemoryLeak(() -> {
                    createBaseAndView(ROWS_SUM + " s");
                    try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                        buildLadder(job, 4, new int[]{1, 1, 1});
                        final long[] before = counters(job);
                        capture.start();
                        commit(job, row("01:00:30", "s2"));
                        capture.drain();
                        Assert.assertEquals("versioned=0 declines=0 discarded=0 fnCaptures=1 fnKeys=4", repairDelta(before, job, false));
                        capture.assertLogged(DECLINE_LOG + ", boundaries=3, max=" + max + ", anchorMaxTs=");
                    }
                    assertMatchesRecompute(ROWS_SUM + " s");
                    dropViewAndBase();
                });
            } finally {
                capture.stop();
            }
        }
    }

    @Test
    public void testChainImagingMoreKeysThanTheReplayReadsTruncates() throws Exception {
        // K = 4, R = 3, est = 3 + 3 + 4 + 1 = 11: 12 > 11, so the chain is declined before any
        // freeze and the truncate seals one head, a complete image of the 4 keys.
        assertChainDecision(ROWS_SUM + " s", 4, new int[]{3, 3, 4}, row("01:00:30", "s2"), 1, true);
    }

    @Test
    public void testChainPastTheDefaultKeyBudgetTruncates() throws Exception {
        // K = 10,001, R = 10, est 11, default scan.max.keys: 100,010 > 100,000.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 10_001, ones(10));
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2"));
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=10001", repairDelta(before, job, false));
                Assert.assertEquals("the anchor and the truncate's head", 2, colLong("checkpoint_timeline_entries"));
                Assert.assertEquals("resume from anchor", colStr("checkpoint_repair_last_disposition"));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testChainWithinTheDefaultKeyBudgetIsKept() throws Exception {
        // K = 10,000, R = 10, est 11, default scan.max.keys: 100,000 <= 100,000, so the chain is
        // kept, imaging the 10,000 keys at each of the 10 boundaries.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 10_000, ones(10));
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2"));
                Assert.assertEquals("versioned=10 declines=0 discarded=0 fnCaptures=10 fnKeys=100000", repairDelta(before, job, false));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testCompositeKeyCountsDistinctKeys() throws Exception {
        // PARTITION BY sym, k: four (sym, k) pairs over two symbols, so K = 4.
        final String projection = "sum(x) OVER (PARTITION BY sym, k ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) s";
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        for (boolean isKept : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createBaseAndView(projection);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commit(job, row("2026-01-01T00:59:59.999996Z", "s1", 1, "v1", 1.0) + ", " + row("2026-01-01T00:59:59.999997Z", "s1", 2, "v1", 1.0)
                            + ", " + row("2026-01-01T00:59:59.999998Z", "s2", 1, "v1", 1.0) + ", " + row("2026-01-01T00:59:59.999999Z", "s2", 2, "v1", 1.0));
                    commitInOrder(job, new int[]{3, 3, isKept ? 5 : 4});
                    final long[] before = counters(job);
                    // On an existing pair, so K stays 4; est = 3 + 3 + (4 or 5) + 1 = 11 or 12.
                    commit(job, row("2026-01-01T01:00:30.000000Z", "s2", 1, "v1", 1.0));
                    Assert.assertEquals(
                            isKept ? "versioned=3 declines=0 discarded=0 fnCaptures=3 fnKeys=12" : "versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4",
                            repairDelta(before, job, false)
                    );
                }
                assertMatchesRecompute(projection);
                dropViewAndBase();
            });
        }
    }

    @Test
    public void testDeclinedAndChainedResumesPublishTheSameRows() throws Exception {
        // T4's data under a budget that declines and under one that keeps: rows identical to
        // each other and to the recompute.
        final String[] published = new String[2];
        final long[] budgets = {1, 0};
        for (int i = 0; i < 2; i++) {
            final int side = i;
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, budgets[i]);
            assertMemoryLeak(() -> {
                createBaseAndView(ROWS_SUM + " s");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    buildLadder(job, 4, new int[]{3, 3, 4});
                    final long[] before = counters(job);
                    commit(job, row("01:00:30", "s2"));
                    Assert.assertEquals(
                            side == 0 ? "versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4" : "versioned=3 declines=0 discarded=0 fnCaptures=3 fnKeys=12",
                            repairDelta(before, job, false)
                    );
                    Assert.assertEquals(side == 0 ? 2 : 4, colLong("checkpoint_timeline_entries"));
                }
                assertMatchesRecompute(ROWS_SUM + " s");
                published[side] = rowsOf("SELECT * FROM lv ORDER BY ts, sym");
                dropViewAndBase();
            });
        }
        Assert.assertEquals(published[0], published[1]);
    }

    @Test
    public void testDeclinedResumeAfterRestartPricesTheRestoredState() throws Exception {
        // T2's view, restarted before the late row: K is read off the maps the restore rebuilt,
        // 10,001 keys, so the new job declines the chain as the old one would have.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 10_001, ones(10));
            }
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2"));
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=10001", repairDelta(before, job, false));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testDeclinedResumeReplayFaultKeepsTheTimeline() throws Exception {
        // The replay faults after the anchor restore and before it reads a row, so nothing froze
        // and nothing moved: the timeline stays as it was (A + 3), and the next turn declines the
        // chain again and publishes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 4, new int[]{3, 3, 4});
                final LiveViewInstance instance = instance();
                final long[] before = counters(job);
                final AtomicBoolean hasFaulted = new AtomicBoolean();
                final int[] entriesAtFault = {-1};
                hooks(job).setSimulateResumeReplayStart(() -> {
                    hasFaulted.set(true);
                    entriesAtFault[0] = snapshotCheckpointLadder(instance).size() / 2;
                    throw CairoException.critical(0).put("test replay fault");
                });
                commit(job, row("01:00:30", "s2"));
                Assert.assertTrue(hasFaulted.get());
                Assert.assertEquals("the timeline the fault met is the pre-repair one", 4, entriesAtFault[0]);
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4", repairDelta(before, job, false));
                Assert.assertEquals(2, colLong("checkpoint_timeline_entries"));
                assertNoStagedRepair(instance);
            }
            assertRowsMatchRecompute(ROWS_SUM + " s", "");
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testDeclinedResumeRestoresAfterRestart() throws Exception {
        // T4 declines; two restarts restore from the truncated timeline, and a forward row on an
        // untouched key then reads the restored state.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 4, new int[]{3, 3, 4});
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2"));
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4", repairDelta(before, job, false));
            }
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertRestoredFromTimeline("lv");
                assertMatchesRecompute(ROWS_SUM + " s");
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, row("01:05:00", "s3"));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testDeclinedResumeSealFailureRetiresAsTheTruncateDoes() throws Exception {
        // The truncate's closing seal fails: with no root at the frontier, the truncated timeline
        // retires, as every resume holding no capture does. The rows stand; a restart rebuilds
        // them from the applied base.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 4, new int[]{3, 3, 4});
                final LiveViewInstance instance = instance();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                final long[] before = counters(job);
                hooks(job).setSimulateResumeHeadSealStart(
                        () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH)
                );
                commit(job, row("01:00:30", "s2"));
                job.setCheckpointTimelineTestFailureStage(0);
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=0 fnKeys=0", repairDelta(before, job, false));
                Assert.assertEquals("the truncated timeline retires", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertFalse(instance.isInvalid());
            }
            assertMatchesRecompute(ROWS_SUM + " s");
            restartLiveViewsAndRecover();
            assertRebuiltFromAppliedBase("lv");
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testDeclinedResumesMoveTheAnchorOnceTheReplayPaysForIt() throws Exception {
        // T16: every root sits at an hour's last microsecond, so every resume floor is an hour's
        // floor and the estimate counts whole partitions. K = 4, scan.max.keys = 1, R = 1 each
        // step: a step whose estimate is 3 declines (4 > 3) and leaves the anchor where it was, so
        // the next step's replay reads 5 rows, which pays for the chain (4 <= 5), and the chain
        // moves the anchor up one hour. HEAD chained every step (3 rows each, a root kept per
        // step); the PR base truncated every step (3, 5, 7, 9, 11, 13 rows).
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                seedKeys(job, 4);
                commit(job, row("01:59:59.999999", "s1"));
                final LiveViewInstance instance = instance();
                final StringBuilder steps = new StringBuilder();
                for (int j = 2; j <= 7; j++) {
                    final long[] before = counters(job);
                    final long scanBefore = instance.getO3ReplayScanRows();
                    commit(job, row(hh(j - 1) + ":30:00", "s2") + ", " + row(hh(j) + ":59:59.999999", "s1"));
                    final long[] after = counters(job);
                    steps.append("decline=").append(after[0] - before[0])
                            .append(" versioned=").append(after[2] - before[2])
                            .append(" scan=").append(instance.getO3ReplayScanRows() - scanBefore).append('\n');
                }
                Assert.assertEquals(
                        """
                                decline=1 versioned=0 scan=3
                                decline=0 versioned=1 scan=5
                                decline=1 versioned=0 scan=3
                                decline=0 versioned=1 scan=5
                                decline=1 versioned=0 scan=3
                                decline=0 versioned=1 scan=5
                                """,
                        steps.toString()
                );
                Assert.assertEquals(5, colLong("checkpoint_timeline_entries"));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testDeepChainUnderTheBoundaryGuardIsPriced() throws Exception {
        // R = 256, the count guard's maximum: the price still decides, 256 x 4 = 1,024 > 257.
        // R = 257: the count guard declines first, and the price's stat stays put.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        // No directory sweep per seal: it lists a directory every one of the 257 roots grows.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_PURGE_INTERVAL, 0);
        for (int roots : new int[]{256, 257}) {
            assertMemoryLeak(() -> {
                createBaseAndView(ROWS_SUM + " s");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    seedKeys(job, 4);
                    for (int i = 1; i <= roots; i++) {
                        // One refresh pass per commit is enough for an in-order row to seal its root.
                        execute("INSERT INTO base (ts, sym, k, v, x, y, d) VALUES " + row(DAY_ZERO_MICROS + HOUR_MICROS + i * 10_000_000L, "s1"));
                        drainWalQueue();
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainJob(job);
                    }
                    drainWalQueue();
                    Assert.assertEquals(roots + 1, colLong("checkpoint_timeline_entries"));
                    final long[] before = counters(job);
                    commit(job, row(DAY_ZERO_MICROS + HOUR_MICROS + 5_000_000L, "s2"));
                    Assert.assertEquals(
                            roots == 256 ? "versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4" : "versioned=0 declines=0 discarded=0 fnCaptures=1 fnKeys=4",
                            repairDelta(before, job, false)
                    );
                }
                assertMatchesRecompute(ROWS_SUM + " s");
                dropViewAndBase();
            });
        }
    }

    @Test
    public void testDepartureDeclinedByThePriceKeepsItsTimeline() throws Exception {
        // T32: the late commit upserts B1..B3's rows out of WHERE x > 0, so the replay emits no
        // row. est 3; at B1, in freezeRemaining, 3 x 4 = 12 > 3: declined before any freeze. The
        // truncate's anchor is the view's new frontier, so the zero-row resume keeps the
        // truncated timeline and a restart restores from it.
        final String projection = ROWS_SUM + " s";
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(projection, " WHERE x > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 4, new int[]{1, 1, 1});
                final long[] before = counters(job);
                commit(job, row("01:01:00", "s1", -1.0) + ", " + row("01:02:00", "s1", -1.0) + ", " + row("01:03:00", "s1", -1.0));
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=0 fnKeys=0", repairDelta(before, job, false));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.TRUNCATE_ZERO_ROW_KEEP_COUNT));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_ZERO_ROW_KEEP_COUNT));
            }
            assertMatchesRecompute(projection, " WHERE x > 0");
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertMatchesRecompute(projection, " WHERE x > 0");
        });
    }

    @Test
    public void testEveryWholeImagedFunctionAddsItsKeys() throws Exception {
        // A ROWS and a RANGE sum, each over the 4 keys: K = 8, R = 3. m = (7, 8, 8): est 24,
        // 24 <= 24 keeps the chain; m = (7, 7, 8): est 23 declines it.
        final String projection = ROWS_SUM + " s, " + RANGE_SUM + " r";
        assertChainDecision(projection, 4, new int[]{7, 8, 8}, row("01:00:30", "s2"), 1, false);
        assertChainDecision(projection, 4, new int[]{7, 7, 8}, row("01:00:30", "s2"), 1, true);
    }

    @Test
    public void testFilteredRowsStillPayForTheChain() throws Exception {
        // The estimate counts base rows, the filtered-out ones too: m = (3, 4, 4) with every
        // other row below the filter still estimates 12, which pays for 3 x 4.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s", " WHERE x > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                seedKeys(job, 4);
                commit(job, row("01:01:00", "s1", -1.0) + ", " + row("01:01:01", "s1", 1.0) + ", " + row("01:01:02", "s1", 1.0));
                commit(job, row("01:02:00", "s1", -1.0) + ", " + row("01:02:01", "s1", 1.0) + ", " + row("01:02:02", "s1", -1.0) + ", " + row("01:02:03", "s1", 1.0));
                commit(job, row("01:03:00", "s1", -1.0) + ", " + row("01:03:01", "s1", 1.0) + ", " + row("01:03:02", "s1", -1.0) + ", " + row("01:03:03", "s1", 1.0));
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2"));
                Assert.assertEquals("versioned=3 declines=0 discarded=0 fnCaptures=3 fnKeys=12", repairDelta(before, job, false));
            }
            assertMatchesRecompute(ROWS_SUM + " s", " WHERE x > 0");
        });
    }

    @Test
    public void testFrontierKeysDeclineBeforeAnyFreeze() throws Exception {
        // T29: the frontier maps hold 12 keys (n1..n8 arrived with B2), while the replay reaches
        // B1 with 4. K_front floors the projection: max(12, 4) x 3 = 36 > 12 declines at B1,
        // before anything froze. Without the floor B1 would freeze (4 x 3 = 12 <= 12) and B2
        // would decline (4 + 2 x 12 = 28 > 12), discarding a root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                seedKeys(job, 4);
                commit(job, row("01:01:00", "s1"));
                commit(job, newKeys("01:01:30", 1, 8) + ", " + row("01:02:00", "s1"));
                commit(job, row("01:03:00", "s1"));
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2"));
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=12", repairDelta(before, job, false));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testKeyBudgetAtOrBelowZeroIsUnlimited() throws Exception {
        // K = 4, R = 3, est 4: 12 > 4, but a budget at or below zero is unlimited and a huge one
        // is never reached, so the chain is kept.
        for (long budget : new long[]{0, -1, Long.MAX_VALUE}) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, budget);
            assertChainDecision(ROWS_SUM + " s", 4, new int[]{1, 1, 1}, row("01:00:30", "s2"), budget, false);
        }
    }

    @Test
    public void testLateDeclineSealsTheAnchoredWindowWhole() throws Exception {
        // T30: an ANCHOR DAILY sum beside a ROWS avg; only the avg is imaged whole (K_front 4).
        // est 12. B1 (01:10): K = 4, 4 x 3 = 12 <= 12, frozen (S = 4); B2 (01:20): n1..n8 arrived,
        // K = 12, 4 + 2 x 12 = 28 > 12: declined, one root discarded. B1's freeze reset the
        // window's dirty set there, so the closing seal must image the window whole (12 keys),
        // not incrementally against the anchor: an incremental seal would keep A's entry for s1,
        // whose late row at 01:05 sits below B1, and a restart would read s1 from it.
        final String projection = "sum(x) OVER w s, " + ROWS_AVG + " a";
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(projection + ANCHOR_WINDOW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                seedKeys(job, 4);
                commit(job, row("01:10:00", "s4"));
                commit(job, row("01:20:00", "s4"));
                commit(job, row("01:30:00", "s4"));
                final long[] before = counters(job);
                commit(job, row("01:05:00", "s1", 7.0) + ", " + newKeys("01:15:00", 1, 8));
                Assert.assertEquals(
                        "versioned=0 declines=1 discarded=1 fnCaptures=1 fnKeys=12 windowCaptures=1 windowIncremental=0 windowKeys=12",
                        repairDelta(before, job, true)
                );
            }
            assertMatchesRecompute(projection + " WINDOW w AS (PARTITION BY sym ORDER BY ts)", "", projection + ANCHOR_WINDOW);
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                commit(job, row("01:40:00", "s1"));
            }
            // s1: 1.0 in hour 0, 7.0 late, 1.0 now.
            Assert.assertEquals("9.0", rowsOf("SELECT s FROM lv WHERE sym = 's1' ORDER BY ts DESC LIMIT 1").trim().split("\n")[1]);
            assertMatchesRecompute(projection + " WINDOW w AS (PARTITION BY sym ORDER BY ts)", "", projection + ANCHOR_WINDOW);
        });
    }

    @Test
    public void testLateDeclineWhoseTruncateMarkerFailsRetiresAndReseals() throws Exception {
        // T31: T27's late decline, then the truncate's repair marker cannot be written: the
        // truncate retires the timeline before the commit, the commit lands, and the closing seal
        // opens a fresh history. The discarded capture leaves no staged segment or descriptor.
        final AtomicBoolean isMarkerFailing = new AtomicBoolean();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (isMarkerFailing.get() && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(ff, () -> {
                createBaseAndView(ROWS_SUM + " s");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    buildLadder(job, 4, new int[]{1, 1, 1});
                    final LiveViewInstance instance = instance();
                    final long[] before = counters(job);
                    capture.start();
                    isMarkerFailing.set(true);
                    commit(job, row("01:00:30", "s2") + ", " + newKeys("01:01:30", 1, 8));
                    isMarkerFailing.set(false);
                    capture.drain();
                    capture.assertLogged("could not preserve live view checkpoint timeline prefix, retiring [view=lv");
                    capture.assertLogged(DECLINE_LOG + ", boundaries=3, max=256, frozenRoots=1, boundaryKeys=12, frontierKeys=4, imagedKeys=4, keyBudget=12, resumeScanRows=12, anchorMaxTs=");
                    Assert.assertEquals("versioned=0 declines=1 discarded=1 fnCaptures=1 fnKeys=12", repairDelta(before, job, false));
                    Assert.assertEquals("the closing seal opens a fresh history", 1, colLong("checkpoint_timeline_entries"));
                    assertNoStagedRepair(instance);
                    assertCatalogueMatchesDisk(instance);
                }
                assertMatchesRecompute(ROWS_SUM + " s");
                restartLiveViewsAndRecover();
                assertRestoredFromTimeline("lv");
                assertMatchesRecompute(ROWS_SUM + " s");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testLateKeysAboveTheFirstBoundaryDiscardItsRoot() throws Exception {
        // T27: n1..n8 land between B1 and B2. est 12. B1: K = 4, 4 x 3 = 12 <= 12, frozen (S = 4);
        // B2: K = 12, 4 + 2 x 12 = 28 > 12: declined, B1's root discarded. A second late row then
        // finds A and the truncate's head (R = 1), K_front 12, est 13: 1 x 12 <= 13 keeps its
        // chain. A decline does not stop the next repair from chaining.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createBaseAndView(ROWS_SUM + " s");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    buildLadder(job, 4, new int[]{1, 1, 1});
                    long[] before = counters(job);
                    capture.start();
                    commit(job, row("01:00:30", "s2") + ", " + newKeys("01:01:30", 1, 8));
                    capture.drain();
                    capture.assertLogged(DECLINE_LOG + ", boundaries=3, max=256, frozenRoots=1, boundaryKeys=12, frontierKeys=4, imagedKeys=4, keyBudget=12, resumeScanRows=12, anchorMaxTs=2026-01-01T00:59:59.999999Z");
                    Assert.assertEquals("versioned=0 declines=1 discarded=1 fnCaptures=1 fnKeys=12", repairDelta(before, job, false));
                    Assert.assertEquals(2, colLong("checkpoint_timeline_entries"));
                    assertNoStagedRepair(instance());

                    before = counters(job);
                    commit(job, row("01:00:45", "s3"));
                    Assert.assertEquals("versioned=1 declines=0 discarded=0 fnCaptures=1 fnKeys=12", repairDelta(before, job, false));
                }
                assertMatchesRecompute(ROWS_SUM + " s");
                restartLiveViewsAndRecover();
                assertRestoredFromTimeline("lv");
                assertMatchesRecompute(ROWS_SUM + " s");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testLateKeysBelowTheFirstBoundaryArePriced() throws Exception {
        // T26: n1..n9 land below B1, so the replay reaches B1 with 13 keys although the frontier
        // held 4. est 13; B1: 3 x 13 = 39 > 13, declined before any freeze. A price read before
        // the replay (3 x 4 = 12 <= 13) would keep a chain imaging 39 keys.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        final LogCapture capture = new LogCapture();
        try {
            assertMemoryLeak(() -> {
                createBaseAndView(ROWS_SUM + " s");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    buildLadder(job, 4, new int[]{1, 1, 1});
                    final long[] before = counters(job);
                    capture.start();
                    commit(job, row("01:00:30", "s2") + ", " + newKeys("01:00:31", 1, 9));
                    capture.drain();
                    capture.assertLogged(DECLINE_LOG + ", boundaries=3, max=256, frozenRoots=0, boundaryKeys=13, frontierKeys=4, imagedKeys=0, keyBudget=13, resumeScanRows=13, anchorMaxTs=2026-01-01T00:59:59.999999Z");
                    Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=13", repairDelta(before, job, false));
                    Assert.assertEquals(2, colLong("checkpoint_timeline_entries"));
                }
                assertMatchesRecompute(ROWS_SUM + " s");
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testMixedViewPricesOnlyTheWholeImagedFunction() throws Exception {
        // An ANCHOR DAILY sum (a durable projection, never whole) beside a ROWS avg (whole, K = 4).
        // est 12 keeps the chain; est 11 declines it before any freeze, and the closing seal
        // then images the window incrementally against the anchor: only the key the replay
        // touched (s2) and the ones the in-order commits did (s1).
        final String projection = "sum(x) OVER w s, " + ROWS_AVG + " a";
        assertChainDecision(projection + ANCHOR_WINDOW, 4, new int[]{3, 4, 4}, row("01:00:30", "s2"), 1, false);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(projection + ANCHOR_WINDOW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 4, new int[]{3, 3, 4});
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2"));
                Assert.assertEquals(
                        "versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4 windowCaptures=1 windowIncremental=1 windowKeys=2",
                        repairDelta(before, job, true)
                );
                // The ROWS avg keeps no dirty set, and says so.
                final WindowFunction avg = unwrapWindowFunctions(instance()).getQuick(1);
                Assert.assertFalse(avg.isCheckpointDirtyTracked());
                Assert.assertNull(avg.getCheckpointDirtyPartitionMap());
            }
            assertMatchesRecompute(projection + " WINDOW w AS (PARTITION BY sym ORDER BY ts)", "", projection + ANCHOR_WINDOW);
        });
    }

    @Test
    public void testNanosecondBaseIsPricedTheSame() throws Exception {
        // T4 over a TIMESTAMP_NS base: the price reads keys and rows, never a timestamp. The
        // anchor sits at hour 0's last nanosecond, so the resume floor is hour 1's floor and the
        // estimate counts hour 1 whole, as over a microsecond base: 11.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBase("TIMESTAMP_NS", "HOUR");
            createView(ROWS_SUM + " s", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commit(job, row("2026-01-01T00:59:59.999999996Z", "s1", 1, "s1", 1.0) + ", " + row("2026-01-01T00:59:59.999999997Z", "s2", 1, "s2", 1.0)
                        + ", " + row("2026-01-01T00:59:59.999999998Z", "s3", 1, "s3", 1.0) + ", " + row("2026-01-01T00:59:59.999999999Z", "s4", 1, "s4", 1.0));
                commitInOrder(job, new int[]{3, 3, 4});
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2"));
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4", repairDelta(before, job, false));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testNoBoundaryAboveTheAnchorIsNotPriced() throws Exception {
        // checkpoint.rows = 2 leaves the last commit unsealed, and the late row lands above the
        // newest root: no boundary to re-version, so nothing to price.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 2);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                seedKeys(job, 4);
                commit(job, row("01:01:00", "s1"));
                commit(job, row("01:02:00", "s1"));
                commit(job, row("01:03:00", "s1"));
                final long[] before = counters(job);
                commit(job, row("01:02:30", "s2"));
                final long[] after = counters(job);
                Assert.assertEquals("no decline", 0, after[0] - before[0]);
                Assert.assertEquals("no root re-versioned", 0, after[2] - before[2]);
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testReadFaultAfterALateDeclineReplansIt() throws Exception {
        // T31b: T27 whose late commit also carries s3 in hour 2, so est 13. The replay declines
        // at B2 (4 + 2 x 12 = 28 > 13) and then fails to read hour 2. The faulted turn counts no
        // decline; the next turn restores in place, replans, declines at B2 again and publishes.
        final PartitionReadFault ff = new PartitionReadFault();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(ff, () -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 4, new int[]{1, 1, 1});
                final LiveViewInstance instance = instance();
                final long[] before = counters(job);
                execute("INSERT INTO base (ts, sym, k, v, x, y, d) VALUES " + row("01:00:30", "s2") + ", " + newKeys("01:01:30", 1, 8)
                        + ", " + row("02:00:00", "s3"));
                drainWalQueue();
                engine.releaseAllReaders();
                ff.arm(engine.verifyTableName("base"), "2026-01-01T02");
                driveRefreshToQuiescence(job);
                Assert.assertTrue("the replay must have met the read fault", ff.hasFired());
                Assert.assertEquals(1, instance.getRefreshFaultCount());
                Assert.assertEquals("versioned=0 declines=1 discarded=1 fnCaptures=1 fnKeys=12", repairDelta(before, job, false));
                assertNoStagedRepair(instance);
            }
            assertRowsMatchRecompute(ROWS_SUM + " s", "");
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    @Test
    public void testReplayRowsPayForTheChain() throws Exception {
        // K = 4, R = 3, m = (3, 4, 4): est 12, and 12 <= 12 keeps the chain.
        assertChainDecision(ROWS_SUM + " s", 4, new int[]{3, 4, 4}, row("01:00:30", "s2"), 1, false);
    }

    @Test
    public void testRuntimeOnlyRowNumberGrowingBetweenBoundariesIsRepriced() throws Exception {
        // 16 sums fill the window's leaf, so row_number is a runtime-only member imaged whole:
        // fused, its walk reads the anchor map; unfused, its own map. T27's late commit brings
        // n1..n8 between B1 and B2: est 12; B1: K = 4, admitted (S = 4); B2: K = 12,
        // 4 + 2 x 12 = 28 > 12, declined. The closing seal then images the window whole.
        final String projection = sums16() + ", row_number() OVER w rn";
        for (boolean isFused : new boolean[]{true, false}) {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
            assertMemoryLeak(() -> {
                createBaseAndView(projection + ANCHOR_WINDOW);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    buildLadder(job, 4, new int[]{1, 1, 1});
                    final long[] before = counters(job);
                    commit(job, row("01:00:30", "s2") + ", " + newKeys("01:01:30", 1, 8));
                    final long[] after = counters(job);
                    Assert.assertEquals("decline", 1, after[0] - before[0]);
                    Assert.assertEquals("discarded", 1, after[1] - before[1]);
                    Assert.assertEquals("versioned", 0, after[2] - before[2]);
                    Assert.assertEquals("the closing seal images the window whole", 0, after[6] - before[6]);
                    Assert.assertEquals(12, after[7] - before[7]);
                }
                assertMatchesRecompute(projection + " WINDOW w AS (PARTITION BY sym ORDER BY ts)", "", projection + ANCHOR_WINDOW);
                dropViewAndBase();
            });
        }
    }

    @Test
    public void testRuntimeOnlyRowNumberIsPricedByTheKeysItsWalkImages() throws Exception {
        // T23: row_number past the leaf budget is not a durable projection and is imaged whole,
        // by the anchor map when fused and by its own map when not; K = 4 either way. est 11
        // declines, est 12 keeps.
        final String projection = sums16() + ", row_number() OVER w rn";
        for (boolean isFused : new boolean[]{true, false}) {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
            assertChainDecision(projection + ANCHOR_WINDOW, 4, new int[]{3, 3, 4}, row("01:00:30", "s2"), 1, true);
            assertChainDecision(projection + ANCHOR_WINDOW, 4, new int[]{3, 4, 4}, row("01:00:30", "s2"), 1, false);
        }
    }

    @Test
    public void testScalarWholeImagedFunctionIsNeverPricedOut() throws Exception {
        // A stateful function with no partition map seals a scalar root, which images no key, so
        // a runtime whose only whole-imaged function is scalar has nothing to price. CREATE
        // refuses every such window function a live view could hold (row_number() OVER (), an
        // unpartitioned ROWS frame), so this drives the price over a factory holding one directly.
        assertMemoryLeak(() -> {
            final ObjList<Function> functions = new ObjList<>();
            functions.add(new ScalarStateStub());
            try (WindowRecordCursorFactory factory = new WindowRecordCursorFactory(null, new GenericRecordMetadata(), functions, null)) {
                final LiveViewResumeChainPrice price = new LiveViewResumeChainPrice();
                Assert.assertTrue(LiveViewResumeChainPrice.isCheckpointImagedWhole(null, null, factory.getWindowFunctions().getQuick(0)));
                Assert.assertFalse(price.of(null, factory, 1));
                Assert.assertEquals(0, price.getFrontierKeys());
            }
        });
    }

    @Test
    public void testSingleKeyViewNeverDeclinesOnPrice() throws Exception {
        // K = 1: est is at least R + 1 > R x 1, so the chain always pays.
        assertChainDecision(ROWS_SUM + " s", 1, new int[]{1, 1, 1}, row("01:00:30", "s1"), 1, false);
    }

    @Test
    public void testSpreadLateKeysAtTheBudgetKeepTheChain() throws Exception {
        // T28's twin: n5..n13, est 17, budget 30. B1 S = 5; B2 S = 13; B3: K = 17,
        // 13 + 17 = 30 <= 30 kept. A kept chain images at most the budget: 5 + 8 + 17 = 30.
        assertSpreadLateKeys(13, "versioned=3 declines=0 discarded=0 fnCaptures=3 fnKeys=30");
    }

    @Test
    public void testSpreadLateKeysDeclineWhereTheyOutgrowTheBudget() throws Exception {
        // T28: s2 and n1 below B1, n2..n4 below B2, n5..n14 below B3; est 18, scan.max.keys 30.
        // B1: K = 5, 3 x 5 = 15 <= 30 (S = 5); B2: K = 8, 5 + 2 x 8 = 21 <= 30 (S = 13);
        // B3, in freezeRemaining: K = 18, 13 + 18 = 31 > 30, declined, two roots discarded.
        assertSpreadLateKeys(14, "versioned=0 declines=1 discarded=2 fnCaptures=1 fnKeys=18");
    }

    @Test
    public void testKeyBudgetNormalizesTheDisablingValues() {
        // scan.max.keys <= 0 lifts the floor: the budget it gives is unlimited.
        Assert.assertEquals(Long.MAX_VALUE, LiveViewCheckpointRowsBounds.budgetOf(0));
        Assert.assertEquals(Long.MAX_VALUE, LiveViewCheckpointRowsBounds.budgetOf(-1));
        Assert.assertEquals(Long.MAX_VALUE, LiveViewCheckpointRowsBounds.budgetOf(Long.MIN_VALUE));
        Assert.assertEquals(1, LiveViewCheckpointRowsBounds.budgetOf(1));
        Assert.assertEquals(100_000, LiveViewCheckpointRowsBounds.budgetOf(100_000));
    }

    @Test
    public void testWithinBudgetAtAndAroundTheEdges() {
        // M x n > B - S <=> M > floor((B - S) / n) for non-negative integers and n >= 1, with
        // M = max(K_front, K_i). Arguments: S, K_i, K_front, n = R - i, B.
        Assert.assertTrue(LiveViewResumeChainPrice.isWithinBudget(0, 4, 4, 3, 12));
        Assert.assertFalse(LiveViewResumeChainPrice.isWithinBudget(0, 4, 4, 3, 11));
        Assert.assertFalse(LiveViewResumeChainPrice.isWithinBudget(0, 13, 4, 3, 13));
        Assert.assertFalse(LiveViewResumeChainPrice.isWithinBudget(4, 12, 4, 2, 12));
        Assert.assertTrue(LiveViewResumeChainPrice.isWithinBudget(13, 17, 0, 1, 30));
        Assert.assertFalse(LiveViewResumeChainPrice.isWithinBudget(13, 18, 0, 1, 30));
        // K_front floors the projection: K_i alone (4 x 3 = 12) would pass.
        Assert.assertFalse(LiveViewResumeChainPrice.isWithinBudget(0, 4, 12, 3, 12));
        Assert.assertTrue(LiveViewResumeChainPrice.isWithinBudget(0, 0, 0, 256, 0));
        Assert.assertTrue(LiveViewResumeChainPrice.isWithinBudget(30, 0, 0, 1, 30));
        Assert.assertFalse(LiveViewResumeChainPrice.isWithinBudget(30, 1, 0, 1, 30));
        Assert.assertTrue(LiveViewResumeChainPrice.isWithinBudget(0, 1_000_000_000_000L, 0, 256, Long.MAX_VALUE));
        // 256 x (Long.MAX_VALUE / 2) overflows a product; the division form answers it.
        Assert.assertFalse(LiveViewResumeChainPrice.isWithinBudget(0, Long.MAX_VALUE / 2, 0, 256, Long.MAX_VALUE));
        Assert.assertTrue(LiveViewResumeChainPrice.isWithinBudget(Long.MAX_VALUE - 256, 1, 0, 256, Long.MAX_VALUE));
        Assert.assertFalse(LiveViewResumeChainPrice.isWithinBudget(Long.MAX_VALUE - 255, 1, 0, 256, Long.MAX_VALUE));
    }

    @Test
    public void testOverstatedEstimateKeepsTheChain() throws Exception {
        // T24: DAY partitions. Day 1 holds N rows on s1..s4 from its floor D, then A at D+12:00
        // and B1..B3 at D+20:00, 21:00, 22:00; the late row at D+16:00. The anchor's partition is
        // the last, so est = (N + 5) x 10 h / (22 h + 1 us). N = 22: (long) 12.27 = 12 keeps the
        // chain though the replay reads 4 rows; N = 21: (long) 11.82 = 11 declines it.
        for (int n : new int[]{22, 21}) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
            assertMemoryLeak(() -> {
                createBase("TIMESTAMP", "DAY");
                createView(ROWS_SUM + " s", "");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commit(job, row("2025-12-31T12:00:00.000000Z", "s1", 1, "s1", 1.0));
                    final StringBuilder rows = new StringBuilder();
                    for (int i = 0; i < n; i++) {
                        if (i > 0) {
                            rows.append(", ");
                        }
                        rows.append(row(DAY_ZERO_MICROS + 86_400_000_000L + i * 60_000_000L, "s" + (i % 4 + 1)));
                    }
                    commit(job, rows.toString());
                    commit(job, row("2026-01-02T12:00:00.000000Z", "s1", 1, "s1", 1.0));
                    commit(job, row("2026-01-02T20:00:00.000000Z", "s1", 1, "s1", 1.0));
                    commit(job, row("2026-01-02T21:00:00.000000Z", "s1", 1, "s1", 1.0));
                    commit(job, row("2026-01-02T22:00:00.000000Z", "s1", 1, "s1", 1.0));
                    final long[] before = counters(job);
                    commit(job, row("2026-01-02T16:00:00.000000Z", "s2", 1, "s2", 1.0));
                    Assert.assertEquals(
                            n == 22 ? "versioned=3 declines=0 discarded=0 fnCaptures=3 fnKeys=12" : "versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4",
                            repairDelta(before, job, false)
                    );
                }
                assertMatchesRecompute(ROWS_SUM + " s");
                dropViewAndBase();
            });
        }
    }

    @Test
    public void testUnderstatedEstimateDeclinesUntilTheReplayCatchesUp() throws Exception {
        // T25: day 1 holds s1..s4 near its floor, A at D+12:00, 18 rows in D+19:00-20:00, then
        // D+21:00 and D+22:00. The late row at D+16:00: est (long)(26 x 10 h / (22 h + 1 us)) = 11,
        // so 3 x 4 = 12 > 11 declines although the replay reads 21 rows. A second late row at
        // D+17:00 finds A and the truncate's head (R = 1): est (long)(27 x ...) = 12, 4 <= 12 keeps.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createBase("TIMESTAMP", "DAY");
            createView(ROWS_SUM + " s", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commit(job, row("2025-12-31T12:00:00.000000Z", "s1", 1, "s1", 1.0));
                commit(job, row("2026-01-02T00:00:01.000000Z", "s1", 1, "s1", 1.0) + ", " + row("2026-01-02T00:00:02.000000Z", "s2", 1, "s2", 1.0)
                        + ", " + row("2026-01-02T00:00:03.000000Z", "s3", 1, "s3", 1.0) + ", " + row("2026-01-02T00:00:04.000000Z", "s4", 1, "s4", 1.0));
                commit(job, row("2026-01-02T12:00:00.000000Z", "s1", 1, "s1", 1.0));
                final StringBuilder rows = new StringBuilder();
                for (int i = 0; i < 18; i++) {
                    if (i > 0) {
                        rows.append(", ");
                    }
                    rows.append(row(DAY_ZERO_MICROS + 86_400_000_000L + 19 * HOUR_MICROS + (i + 1) * 60_000_000L, "s1"));
                }
                commit(job, rows.toString());
                commit(job, row("2026-01-02T21:00:00.000000Z", "s1", 1, "s1", 1.0));
                commit(job, row("2026-01-02T22:00:00.000000Z", "s1", 1, "s1", 1.0));
                long[] before = counters(job);
                commit(job, row("2026-01-02T16:00:00.000000Z", "s2", 1, "s2", 1.0));
                Assert.assertEquals("versioned=0 declines=1 discarded=0 fnCaptures=1 fnKeys=4", repairDelta(before, job, false));
                before = counters(job);
                commit(job, row("2026-01-02T17:00:00.000000Z", "s3", 1, "s3", 1.0));
                Assert.assertEquals("versioned=1 declines=0 discarded=0 fnCaptures=1 fnKeys=4", repairDelta(before, job, false));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    private static String hh(int hour) {
        return hour < 10 ? "0" + hour : String.valueOf(hour);
    }

    private static LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        return instance;
    }

    // Rows n<from>..n<to>, one a second from the given time in hour 1.
    private static String newKeys(String fromTime, int from, int to) {
        final long fromMicros = ts("2026-01-01T" + fromTime + ".000000Z");
        final StringBuilder sb = new StringBuilder();
        for (int i = from; i <= to; i++) {
            if (i > from) {
                sb.append(", ");
            }
            sb.append(row(fromMicros + (i - from) * 1_000_000L, "n" + i));
        }
        return sb.toString();
    }

    private static int[] ones(int count) {
        final int[] rows = new int[count];
        java.util.Arrays.fill(rows, 1);
        return rows;
    }

    // One base row on 2026-01-01 at the given time of day, x = 1.0.
    private static String row(String timeOfDay, String sym) {
        return row(timeOfDay, sym, 1.0);
    }

    private static String row(String timeOfDay, String sym, double x) {
        return row("2026-01-01T" + timeOfDay + (timeOfDay.length() > 8 ? "Z" : ".000000Z"), sym, 1, sym, x);
    }

    private static String row(long micros, String sym) {
        return "(" + micros + "::timestamp, '" + sym + "', 1, '" + sym + "', 1.0, 2.0, 1.0m)";
    }

    private static String row(String timestamp, String sym, int k, String v, double x) {
        return "('" + timestamp + "', '" + sym + "', " + k + ", '" + v + "', " + x + ", 2.0, " + x + "m)";
    }

    private static String sums16() {
        final StringBuilder sb = new StringBuilder();
        for (int q = 1; q <= 16; q++) {
            if (q > 1) {
                sb.append(", ");
            }
            sb.append("sum(q").append(q).append(") OVER w s").append(q);
        }
        return sb.toString();
    }

    private void assertChainDecision(
            String projection,
            int keys,
            int[] commitRows,
            String lateRows,
            long scanMaxKeys,
            boolean isDeclined
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, scanMaxKeys);
        assertMemoryLeak(() -> {
            createBaseAndView(projection);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, keys, commitRows);
                Assert.assertEquals(commitRows.length + 1, colLong("checkpoint_timeline_entries"));
                final long[] before = counters(job);
                commit(job, lateRows);
                final long[] after = counters(job);
                Assert.assertEquals(projection + ": decline", isDeclined ? 1 : 0, after[0] - before[0]);
                Assert.assertEquals(projection + ": discarded", 0, after[1] - before[1]);
                Assert.assertEquals(projection + ": versioned", isDeclined ? 0 : commitRows.length, after[2] - before[2]);
                Assert.assertEquals("resume from anchor", colStr("checkpoint_repair_last_disposition"));
            }
            final String plain = projection.replace(" ANCHOR DAILY '00:00'", "");
            assertMatchesRecompute(plain, "", projection);
            dropViewAndBase();
        });
    }

    private void assertMatchesRecompute(String projection) throws Exception {
        assertMatchesRecompute(projection, "");
    }

    private void assertMatchesRecompute(String projection, String where) throws Exception {
        assertMatchesRecompute(projection, where, projection);
    }

    // The view's rows against its own SELECT over the base; ANCHOR views hold one day, so their
    // oracle is the SELECT without the ANCHOR clause.
    private void assertMatchesRecompute(String oracleProjection, String where, String viewProjection) throws Exception {
        assertNoRefreshFaults("lv");
        assertRowsMatchRecompute(oracleProjection, where);
    }

    // The rows alone, for a view a test has faulted on purpose and asserted the count of.
    private void assertRowsMatchRecompute(String oracleProjection, String where) throws Exception {
        final String oracle = selectSql(oracleProjection.replace(" ANCHOR DAILY '00:00'", ""), where);
        assertSqlCursors("SELECT * FROM (" + oracle + ") ORDER BY ts, sym", "SELECT * FROM lv ORDER BY ts, sym");
    }

    private void assertNoStagedRepair(LiveViewInstance instance) {
        final File checkpoints = new File(
                new File(configuration.getDbRoot(), instance.getLiveViewToken().getDirName()),
                LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
        );
        final StringBuilder leftovers = new StringBuilder();
        collectTmp(checkpoints, leftovers);
        Assert.assertEquals("no staged segment or repair descriptor may outlive the repair", "", leftovers.toString());
        try (Path dir = new Path().of(checkpoints.getAbsolutePath())) {
            Assert.assertFalse("a published repair owes no marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
    }

    private void assertSpreadLateKeys(int lastNewKey, String expected) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 30);
        assertMemoryLeak(() -> {
            createBaseAndView(ROWS_SUM + " s");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildLadder(job, 4, new int[]{1, 1, 1});
                final long[] before = counters(job);
                commit(job, row("01:00:30", "s2") + ", " + newKeys("01:00:40", 1, 1) + ", " + newKeys("01:01:30", 2, 4)
                        + ", " + newKeys("01:02:30", 5, lastNewKey));
                Assert.assertEquals(expected, repairDelta(before, job, false));
            }
            assertMatchesRecompute(ROWS_SUM + " s");
        });
    }

    // Hour 0: keys s1..sK in one commit, the last at 00:59:59.999999 (root A); then one in-order
    // commit per entry of commitRows on s1 in hour 1, each a root.
    private void buildLadder(LiveViewRefreshJob job, int keys, int[] commitRows) throws Exception {
        driveSeedToCompletion(job, "lv");
        seedKeys(job, keys);
        commitInOrder(job, commitRows);
    }

    private void collectTmp(File dir, StringBuilder sink) {
        final File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                collectTmp(file, sink);
            } else if (file.getName().endsWith(LiveViewCheckpointLayout.TMP_SUFFIX)
                    || file.getName().startsWith(LiveViewCheckpointLayout.REPAIR_DESCRIPTOR_PREFIX)) {
                sink.append(file.getName()).append(' ');
            }
        }
    }

    private long colLong(String column) throws Exception {
        try (RecordCursorFactory factory = select("SELECT " + column + " FROM live_views() WHERE view_name = 'lv'")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private String colStr(String column) throws Exception {
        try (RecordCursorFactory factory = select("SELECT " + column + " FROM live_views() WHERE view_name = 'lv'")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                final CharSequence value = cursor.getRecord().getStrA(0);
                return value == null ? null : value.toString();
            }
        }
    }

    private void commit(LiveViewRefreshJob job, String rows) throws Exception {
        execute("INSERT INTO base (ts, sym, k, v, x, y, d) VALUES " + rows);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // One in-order commit per entry: commit i holds that many rows on s1 from 01:0i:00, one a
    // second, so its root sits at the last of them.
    private void commitInOrder(LiveViewRefreshJob job, int[] commitRows) throws Exception {
        for (int i = 0; i < commitRows.length; i++) {
            final long minuteMicros = DAY_ZERO_MICROS + HOUR_MICROS + (i + 1) * 60_000_000L;
            final StringBuilder rows = new StringBuilder();
            for (int j = 0; j < commitRows[i]; j++) {
                if (j > 0) {
                    rows.append(", ");
                }
                rows.append(row(minuteMicros + j * 1_000_000L, "s1"));
            }
            commit(job, rows.toString());
        }
    }

    private long[] counters(LiveViewRefreshJob job) {
        final LiveViewInstance instance = instance();
        final LiveViewCheckpointCaptureLedger ledger = instance.getCheckpointCaptureLedgerForTest();
        return new long[]{
                job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT),
                job.getStatsForTest().get(LiveViewRefreshStats.RESUME_CHAIN_DISCARDED_ROOT_COUNT),
                instance.getCheckpointRepairRootsVersioned(),
                ledger.getFunctionCaptures(),
                ledger.getFunctionKeysImaged(),
                ledger.getWindowCaptures(),
                ledger.getWindowIncrementalCaptures(),
                ledger.getWindowKeysImaged()
        };
    }

    private void createBase(String timestampType, String partitionBy) throws Exception {
        final StringBuilder q = new StringBuilder();
        for (int i = 1; i <= 16; i++) {
            q.append(", q").append(i).append(" DOUBLE");
        }
        execute("CREATE TABLE base (ts " + timestampType + ", sym SYMBOL, k INT, v VARCHAR, x DOUBLE, y DOUBLE, d DECIMAL(18,2)" + q
                + ") TIMESTAMP(ts) PARTITION BY " + partitionBy + " WAL DEDUP UPSERT KEYS(ts, sym)");
    }

    private void createBaseAndView(String projection) throws Exception {
        createBaseAndView(projection, "");
    }

    private void createBaseAndView(String projection, String where) throws Exception {
        createBase("TIMESTAMP", "HOUR");
        createView(projection, where);
    }

    private void createView(String projection, String where) throws Exception {
        setCurrentMicros(DAY_ZERO_MICROS);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + selectSql(projection, where));
    }

    // Drops the view and its base, so a test running several scenarios starts each one afresh.
    private void dropViewAndBase() throws Exception {
        execute("DROP LIVE VIEW lv");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            drainJob(job);
        }
        execute("DROP TABLE base");
        drainWalQueue();
    }

    private String repairDelta(long[] before, LiveViewRefreshJob job, boolean isWindowReported) {
        final long[] after = counters(job);
        final String delta = "versioned=" + (after[2] - before[2])
                + " declines=" + (after[0] - before[0])
                + " discarded=" + (after[1] - before[1])
                + " fnCaptures=" + (after[3] - before[3])
                + " fnKeys=" + (after[4] - before[4]);
        if (!isWindowReported) {
            return delta;
        }
        return delta + " windowCaptures=" + (after[5] - before[5])
                + " windowIncremental=" + (after[6] - before[6])
                + " windowKeys=" + (after[7] - before[7]);
    }

    private String rowsOf(String sql) throws Exception {
        final io.questdb.std.str.StringSink sink = new io.questdb.std.str.StringSink();
        printSql(sql, sink);
        return sink.toString();
    }

    private void seedKeys(LiveViewRefreshJob job, int keys) throws Exception {
        final long lastMicros = DAY_ZERO_MICROS + HOUR_MICROS - 1;
        execute("INSERT INTO base (ts, sym, k, v, x, y, d) SELECT (" + (lastMicros - keys) + " + x)::timestamp, ('s' || x)::symbol, 1, 's' || x, 1.0, 2.0, 1.0m FROM long_sequence(" + keys + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private String selectSql(String projection, String where) {
        final int windowAt = projection.indexOf(" WINDOW w AS");
        final String calls = windowAt < 0 ? projection : projection.substring(0, windowAt);
        final String window = windowAt < 0 ? "" : projection.substring(windowAt);
        return "SELECT ts, sym, x, " + calls + " FROM base" + where + window;
    }

    // A stateful window function keeping its state in one scalar root, with no partition map.
    private static final class ScalarStateStub extends BaseWindowFunction {
        private ScalarStateStub() {
            super(null);
        }

        @Override
        public String getName() {
            return "scalar_state_stub";
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }
    }

    /**
     * Once armed, fails the first read-only open of a column file in one base partition: the
     * replay's own scan, when the base has applied the commit and no pooled reader is left.
     */
    private static final class PartitionReadFault extends TestFilesFacadeImpl {
        private final AtomicBoolean hasFired = new AtomicBoolean();
        private final AtomicBoolean isArmed = new AtomicBoolean();
        private volatile String partitionPathPart;

        @Override
        public long openRO(LPSZ name) {
            if (isArmed.get()
                    && Utf8s.containsAscii(name, partitionPathPart)
                    && Utf8s.endsWithAscii(name, ".d")
                    && isArmed.compareAndSet(true, false)) {
                hasFired.set(true);
                return -1;
            }
            return super.openRO(name);
        }

        void arm(TableToken baseToken, String partitionName) {
            partitionPathPart = Files.SEPARATOR + baseToken.getDirName() + Files.SEPARATOR + partitionName;
            hasFired.set(false);
            isArmed.set(true);
        }

        boolean hasFired() {
            return hasFired.get();
        }
    }
}
