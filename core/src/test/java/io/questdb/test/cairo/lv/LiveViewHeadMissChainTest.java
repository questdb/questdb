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
import io.questdb.cairo.lv.LiveViewCheckpointCaptureLedger;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.tools.LogCapture;
import org.junit.Assert;
import org.junit.Test;

/**
 * A late row at the start of an anchored view's current segment, below that segment's first
 * checkpoint root, rebuilds from the segment start (a head miss) and splices every root above
 * it. The splice chains from nothing: its first boundary images the replay's keys once, and
 * every boundary above it only the keys the replay touched since the one below. The functions
 * every chained freeze images whole (lag, rank, covar, rings) are priced boundary by boundary
 * against B = max(the base rows the replay reads, scan.max.keys), and a refused chain falls
 * back to the truncate.
 * <p>
 * The fixture (HM): {@code base} partitioned by DAY; one commit 'a' at day0 23:00 (a root
 * below the late row); one commit of N rows on K keys s1..sK at day1 + i us, i = 1..N, plus
 * 'a' at day0 23:30, so a resume from the 23:00 root reads one row more than the rebuild from
 * the day's start; R commits of 'a' at day1 + j x 5 min, one root each; then the late row 'b'
 * at day1 00:00. R + 1 roots sit above the late row. The replay reads day1 whole, so its
 * estimate is exact: N + 1 + R rows. Boundary 0 holds 'b' and the K keys, every later one
 * adds 'a' once and touches it alone. Every expected figure below is that arithmetic, by hand.
 */
public class LiveViewHeadMissChainTest extends AbstractLiveViewTest {
    private static final String ANCHOR_WINDOW = " WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final long SECOND = 1_000_000L;
    private static final long MINUTE = 60 * SECOND;
    private static final long DAY0_2300 = 1_767_308_400_000_000L; // 2026-01-01T23:00:00Z
    private static final long DAY1 = 1_767_312_000_000_000L; // 2026-01-02T00:00:00Z
    private static final long DAY2 = DAY1 + 24 * 60 * MINUTE;
    private static final String DECLINE_LOG = "live view O3 head miss declined the checkpoint splice, truncating instead [view=lv";
    private static final long HOUR = 60 * MINUTE;
    private static final String LAG = "lag(x) OVER w AS l";
    private static final String LAG_ORACLE = "lag(x) OVER (PARTITION BY sym, timestamp_floor('d', ts) ORDER BY ts) AS l";
    private static final String SUM = "sum(x) OVER w AS s";
    private static final String SUM_ORACLE = "sum(x) OVER (PARTITION BY sym, timestamp_floor('d', ts) ORDER BY ts) AS s";

    @Test
    public void testChainedSpliceImagesTheKeysOnce() throws Exception {
        // K = 10,000, 13 roots above the late row. Boundary 0 images 'b' and the K keys,
        // complete: 10,001; each of the 12 'a' boundaries images 'a' alone, incrementally.
        // HEAD imaged all 13 complete: 130,025 keys.
        assertMemoryLeak(() -> {
            createView(SUM, "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildHm(job, 10_000, 10_000, 12, true);
                final long[] before = counters(job);
                late(job, lateRow(DAY1, "b"));
                Assert.assertEquals(
                        "route=range versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=10013 fnCaptures=0 fnKeys=0",
                        delta(before, job)
                );
                Assert.assertEquals(14, colLong("checkpoint_timeline_entries"));
                assertMatchesRecompute(SUM_ORACLE);
                // A second late row between the 00:10 and 00:15 roots resumes from the chained
                // 00:10 root, whose window entry for 'a' the chain staged incrementally.
                late(job, lateRow(DAY1 + 12 * MINUTE, "a"));
                Assert.assertEquals("resume from anchor", colStr("checkpoint_repair_last_disposition"));
                assertMatchesRecompute(SUM_ORACLE);
            }
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertMatchesRecompute(SUM_ORACLE);
        });
    }

    @Test
    public void testClosedSegmentChainIsNotPriced() throws Exception {
        // The closed-segment repair converges below the frontier (finite H) and is never priced,
        // whatever its W: lag images whole at each of the 13 boundaries, 13 x 1,001 + 12 = 13,025,
        // though scan.max.keys = 1 and the replay reads 1,013 rows. The window chains.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createView(LAG, "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildHm(job, 1_000, 1_000, 12, true);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (DAY2 + MINUTE) + "::timestamp, 'a', 1.0)", true);
                final long[] before = counters(job);
                late(job, lateRow(DAY1, "b"));
                Assert.assertEquals(
                        "route=segment versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=1013 fnCaptures=13 fnKeys=13025",
                        delta(before, job)
                );
            }
            assertMatchesRecompute(LAG_ORACLE);
        });
    }

    @Test
    public void testClosedSegmentCopyAsideChain() throws Exception {
        // As the isolated closed-segment repair, through the primary runtime copied aside.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        assertClosedSegmentChain();
    }

    @Test
    public void testClosedSegmentIsolatedChain() throws Exception {
        // A late row in a CLOSED day whose 13 roots sit above it: the converging repair
        // re-versions them as a chain from nothing, 10,001 + 12 keys rather than 13 x 10,002.
        assertClosedSegmentChain();
    }

    @Test
    public void testCopyAsideMembersOweACompleteFreeze() throws Exception {
        // Sixteen sums overflow the fused window's leaf, so s16, n and c are runtime-only members
        // with function roots of their own, walked out of the anchor map. A late row in a closed
        // day whose 13 roots sit above it, repaired through the primary copied aside, with no root
        // yet in the current day: the carryover finds no root at or above H and drops. The chain:
        // window 1,001 + 12 keys, 12 of 13 incremental; each member the same, 3 x 13 captures,
        // 3 x 12 incremental, 3 x 1,013 keys. The overlay restores the window latched, so the seal
        // that closes the repair images the window and the 3 members complete over the frontier's
        // 1,001 keys - 'a' and s1..s1000; 'b' lived only in the replay.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        assertMemoryLeak(() -> {
            setCurrentMicros(0L);
            final StringBuilder ddl = new StringBuilder();
            final StringBuilder projections = new StringBuilder();
            final StringBuilder oracle = new StringBuilder();
            final StringBuilder columns = new StringBuilder();
            for (int i = 1; i <= 16; i++) {
                ddl.append(", q").append(i).append(" DOUBLE");
                projections.append(", sum(q").append(i).append(") OVER w AS s").append(i);
                oracle.append(", sum(q").append(i).append(") OVER (PARTITION BY sym, timestamp_floor('d', ts) ORDER BY ts) AS s").append(i);
                columns.append(", s").append(i);
            }
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL CAPACITY 65536 NOCACHE, x DOUBLE" + ddl + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x" + projections
                    + ", count(*) OVER w AS n, count(sym) OVER w AS c FROM base" + ANCHOR_WINDOW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildHm(job, 1_000, 1_000, 12, true);
                // Day 2's first row, refreshed before the cadence is due: no root in day 2.
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (DAY2 + MINUTE) + "::timestamp, 'a', 1.0)", false);
                Assert.assertEquals(14, colLong("checkpoint_timeline_entries"));
                final LiveViewCheckpointCaptureLedger ledger = instance().getCheckpointCaptureLedgerForTest();
                final long fnIncrementalBefore = ledger.getFunctionIncrementalCaptures();
                final long[] before = counters(job);
                late(job, lateRow(DAY1, "b"));
                Assert.assertEquals(
                        "route=segment versioned=13 declines=0 discarded=0 winCaptures=14 winIncremental=12 winKeys=2014 fnCaptures=42 fnKeys=6042",
                        delta(before, job)
                );
                Assert.assertEquals(36, ledger.getFunctionIncrementalCaptures() - fnIncrementalBefore);
                // The closing seal's batch reaches back to the late row, below the newest spliced
                // root, so its root replaces that one: the 23:00 root, 12 spliced roots and its own.
                Assert.assertEquals(14, colLong("checkpoint_timeline_entries"));
            }
            final String oracleSql = "SELECT ts, sym, x" + oracle
                    + ", count(*) OVER (PARTITION BY sym, timestamp_floor('d', ts) ORDER BY ts) AS n"
                    + ", count(sym) OVER (PARTITION BY sym, timestamp_floor('d', ts) ORDER BY ts) AS c FROM base ORDER BY ts, sym";
            final String viewSql = "SELECT ts, sym, x" + columns + ", n, c FROM lv ORDER BY ts, sym";
            assertNoRefreshFaults("lv");
            assertSqlCursors(oracleSql, viewSql);
            restartLiveViewsAndRecover();
            assertSqlCursors(oracleSql, viewSql);
        });
    }

    @Test
    public void testDeclineInAYieldingTurn() throws Exception {
        // T6c's refused shape (N = 13,011, rows 13,024) with a replay budget that ends the
        // first turn on the row crossing boundary 1: 'b', the 13,011 key rows and 'a' at
        // 00:05 make 13,013 rows, and 'a' at 00:10 is the 13,014th. The refusal lands in a
        // turn that yields; the later turns park and commit with no capture. At 13,013 the
        // first turn ends on 00:05 and the refusal lands on the resumed turn instead, priced
        // by the session's price with the same S.
        for (long maxRows : new long[]{13_014, 13_013}) {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, maxRows);
            assertMemoryLeak(() -> {
                createView(LAG, "");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    buildHm(job, 1_000, 13_011, 12, true);
                    final long[] before = counters(job);
                    final LiveViewInstance instance = instance();
                    execute("INSERT INTO base (ts, sym, x) VALUES " + lateRow(DAY1, "b"));
                    drainWalQueue();
                    driveUntilParked(job, "lv");
                    Assert.assertNotNull(instance.getSuspendedRepair());
                    driveRefreshToQuiescence(job);
                    Assert.assertNull(instance.getSuspendedRepair());
                    Assert.assertEquals(
                            "maxRows=" + maxRows,
                            "route=range versioned=0 declines=1 discarded=1",
                            delta(before, job).substring(0, "route=range versioned=0 declines=1 discarded=1".length())
                    );
                    Assert.assertEquals("the 23:00 root and the truncate's head", 2, colLong("checkpoint_timeline_entries"));
                }
                assertMatchesRecompute(LAG_ORACLE);
                dropViewAndBase();
            });
        }
    }

    @Test
    public void testEstimateStartsAtTheScanFloor() throws Exception {
        // L < C: the key commit carries M rows of 'w' at 00:01 and its K = 1,000 key rows from
        // 01:00, the 'a' roots sit from 01:05, and the late row 'b' is at 00:30. The replay reads
        // [00:00, +inf): M + N + 13 rows. Boundary 0 holds 'w', 'b' and the K keys, each 'a'
        // boundary one key more: B needed = 1,002 + 12 x 1,003 = 13,038.
        //  - M + N = 13,025: rows 13,038, kept;
        //  - M + N = 13,024: rows 13,037, refused at boundary 1 (13,026 <= 13,037; 1,003 x 12 >
        //    13,037 - 1,002);
        //  - an estimate from C or R would read only part of day 1 and refuse at boundary 0.
        assertScanFloorDecision(1_000, 12_025, "", "route=range versioned=13 declines=0 discarded=0");
        assertScanFloorDecision(999, 12_025, "", "route=range versioned=0 declines=1 discarded=1");
        // The 'w' rows rejected by the view's filter: boundary 0 holds 'b' and the K keys, so the
        // chain needs 1,001 + 12 x 1,002 = 13,025, and the estimate still counts the M filtered
        // rows: M = 1,000, N = 12,012 gives 13,025 rows, kept; without them it would be 12,025.
        assertScanFloorDecision(1_000, 12_012, " WHERE sym != 'w'", "route=range versioned=13 declines=0 discarded=0");
    }

    @Test
    public void testKeyBudgetAtOrBelowZeroIsUnlimited() throws Exception {
        // N = K = 1,000: rows 1,013 would refuse boundary 0 (1,001 x 13 > 1,013) at any positive
        // scan.max.keys below 13,013; at 0 and -1 the floor is unlimited and the chain is kept.
        for (long scanMaxKeys : new long[]{0, -1}) {
            assertPriceDecision(LAG, LAG_ORACLE, 1_000, 1_000, 12, true, scanMaxKeys, "route=range versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=1013 fnCaptures=13 fnKeys=13025");
        }
    }

    @Test
    public void testNextCadenceSealAfterAChainedSpliceIsIncremental() throws Exception {
        // The promoted runtime adopts the splice's generation, so the next cadence seal freezes
        // the 3 keys forward rows touched rather than the 10,002 live keys.
        assertMemoryLeak(() -> {
            createView(SUM, "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildHm(job, 10_000, 10_000, 12, true);
                late(job, lateRow(DAY1, "b"));
                final long[] before = counters(job);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES "
                        + "(" + (DAY1 + 61 * MINUTE) + "::timestamp, 's1', 1.0), "
                        + "(" + (DAY1 + 61 * MINUTE + SECOND) + "::timestamp, 's2', 1.0), "
                        + "(" + (DAY1 + 61 * MINUTE + 2 * SECOND) + "::timestamp, 's3', 1.0)", true);
                Assert.assertEquals(
                        "route=none versioned=0 declines=0 discarded=0 winCaptures=1 winIncremental=1 winKeys=3 fnCaptures=0 fnKeys=0",
                        delta(before, job)
                );
            }
            assertMatchesRecompute(SUM_ORACLE);
        });
    }

    @Test
    public void testNoRootBelowTheLateRowStillPricesTheReplayRows() throws Exception {
        // HM0: no root below the late row, so the plan priced no rebuild; the head miss estimates
        // its own replay. scan.max.keys = 1. N = 14,000: rows 14,013 >= 13,025, kept. N = K:
        // rows 1,013, refused at boundary 0, and with no root below R the truncate retires the
        // timeline; the closing seal opens a one-root history.
        assertPriceDecision(LAG, LAG_ORACLE, 1_000, 14_000, 12, false, 1, "route=range versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=1013 fnCaptures=13 fnKeys=13025");
        assertPriceDecision(LAG, LAG_ORACLE, 1_000, 1_000, 12, false, 1, "route=range versioned=0 declines=1 discarded=0");
    }

    @Test
    public void testParkedChainPublishes() throws Exception {
        // T1's chain at K = 1,000 with every 'a' commit carrying 2,500 'a' rows and a replay
        // budget of 2,000 rows, so the head miss parks inside every interval. Window: 13
        // captures, 12 incremental, 1,001 + 12 keys - as unparked.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 2_000);
        assertMemoryLeak(() -> {
            createView(SUM, "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                setCurrentMicros(0L);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + DAY0_2300 + "::timestamp, 'a', 1.0)", true);
                commit(job, keyCommit(1_000, 1_000), true);
                for (int j = 1; j <= 12; j++) {
                    commit(job, "INSERT INTO base (ts, sym, x) SELECT (" + (DAY1 + j * 5 * MINUTE - 2_500) + " + x)::timestamp, 'a'::symbol, 1.0 FROM long_sequence(2500)", true);
                }
                final long[] before = counters(job);
                final LiveViewInstance instance = instance();
                execute("INSERT INTO base (ts, sym, x) VALUES " + lateRow(DAY1, "b"));
                drainWalQueue();
                driveUntilParked(job, "lv");
                driveRefreshToQuiescence(job);
                Assert.assertNull(instance.getSuspendedRepair());
                Assert.assertEquals(
                        "route=range versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=1013 fnCaptures=0 fnKeys=0",
                        delta(before, job)
                );
                assertMatchesRecompute(SUM_ORACLE);
            }
            restartLiveViewsAndRecover();
            assertRestoredFromTimeline("lv");
            assertMatchesRecompute(SUM_ORACLE);
        });
    }

    @Test
    public void testPriceAtTheExactKeyBudget() throws Exception {
        // HM with anchored lag (imaged whole), K = N = 1,000, R + 1 = 13: rows 1,013. The chain
        // images 1,001 + 12 x 1,002 = 13,025 lag keys. scan.max.keys = 13,025 keeps it;
        // 13,024 refuses boundary 1 (S = 1,001, 1,002 x 12 > 12,023), discarding one root;
        // 13,012 refuses boundary 0 (1,001 x 13 > 13,012), discarding none.
        final LogCapture capture = new LogCapture();
        try {
            assertPriceDecision(LAG, LAG_ORACLE, 1_000, 1_000, 12, true, 13_025, "route=range versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=1013 fnCaptures=13 fnKeys=13025");
            capture.start();
            assertPriceDecision(LAG, LAG_ORACLE, 1_000, 1_000, 12, true, 13_024, "route=range versioned=0 declines=1 discarded=1");
            capture.drain();
            capture.assertLogged(DECLINE_LOG + ", boundaries=13, frozenRoots=1, boundaryKeys=1002, imagedKeys=1001, keyBudget=13024, replayScanRows=1013, outputLowTs=2026-01-02T00:00:00.000000Z]");
            assertPriceDecision(LAG, LAG_ORACLE, 1_000, 1_000, 12, true, 13_012, "route=range versioned=0 declines=1 discarded=0");
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testReplayRowsDecideTheBudget() throws Exception {
        // scan.max.keys = 1, so B is the replay's rows, N + 1 + 12. N = 13,012 (13 rows per key
        // and 12 more): rows 13,025, kept. N = 13,011: rows 13,024, refused at boundary 1,
        // discarding one root. N = K: rows 1,013, refused at boundary 0.
        assertPriceDecision(LAG, LAG_ORACLE, 1_000, 13_012, 12, true, 1, "route=range versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=1013 fnCaptures=13 fnKeys=13025");
        assertPriceDecision(LAG, LAG_ORACLE, 1_000, 13_011, 12, true, 1, "route=range versioned=0 declines=1 discarded=1");
        assertPriceDecision(LAG, LAG_ORACLE, 1_000, 1_000, 12, true, 1, "route=range versioned=0 declines=1 discarded=0");
    }

    @Test
    public void testUnionRangeHeadMissAcrossASweep() throws Exception {
        assertUnionRangeHeadMissAcrossASweep("covar_pop(x, x)");
    }

    @Test
    public void testUnionRangeHeadMissAcrossASweepCorr() throws Exception {
        assertUnionRangeHeadMissAcrossASweep("corr(x, x)");
    }

    @Test
    public void testLateBatchOnNewKeysImagesThemOnce() throws Exception {
        // The frontier holds 'a' alone in day 1; the late commit brings 10,000 new keys at the
        // day's start, below every root. Boundary 0 images them with 'a': 10,001, each later
        // boundary 'a' alone: 10,001 + 12. HEAD imaged them at every root: 130,013.
        assertMemoryLeak(() -> {
            createView(SUM, "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                setCurrentMicros(0L);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + DAY0_2300 + "::timestamp, 'a', 1.0)", true);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (DAY1 + 30 * SECOND) + "::timestamp, 'a', 1.0), (" + (DAY0_2300 + 30 * MINUTE) + "::timestamp, 'a', 1.0)", true);
                for (int j = 1; j <= 12; j++) {
                    commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (DAY1 + j * 5 * MINUTE) + "::timestamp, 'a', 1.0)", true);
                }
                final long[] before = counters(job);
                late(job, "SELECT (" + DAY1 + " + x)::timestamp, ('n' || x)::symbol, 1.0 FROM long_sequence(10000)");
                Assert.assertEquals(
                        "route=range versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=10013 fnCaptures=0 fnKeys=0",
                        delta(before, job)
                );
            }
            assertMatchesRecompute(SUM_ORACLE);
        });
    }

    private static LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        return instance;
    }

    // The key commit: N rows on K keys s1..sK at day1 + i us, i = 1..N, and 'a' at day0 23:30.
    private static String keyCommit(int keys, int keyRows) {
        return "INSERT INTO base (ts, sym, x) SELECT (" + DAY1 + " + x)::timestamp, ('s' || ((x - 1) % " + keys + " + 1))::symbol, 1.0 FROM long_sequence(" + keyRows + ")"
                + " UNION ALL SELECT " + (DAY0_2300 + 30 * MINUTE) + "::timestamp, 'a'::symbol, 1.0 FROM long_sequence(1)";
    }

    private static String lateRow(long ts, String sym) {
        return "(" + ts + "::timestamp, '" + sym + "', 1.0)";
    }

    private void assertClosedSegmentChain() throws Exception {
        assertMemoryLeak(() -> {
            createView(SUM, "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildHm(job, 10_000, 10_000, 12, true);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (DAY2 + MINUTE) + "::timestamp, 'a', 1.0)", true);
                final long[] before = counters(job);
                late(job, lateRow(DAY1, "b"));
                Assert.assertEquals(
                        "route=segment versioned=13 declines=0 discarded=0 winCaptures=13 winIncremental=12 winKeys=10013 fnCaptures=0 fnKeys=0",
                        delta(before, job)
                );
                assertMatchesRecompute(SUM_ORACLE);
                // A later late row in the repaired day resumes from a chained root.
                late(job, lateRow(DAY1 + 12 * MINUTE, "a"));
                assertMatchesRecompute(SUM_ORACLE);
            }
            restartLiveViewsAndRecover();
            assertMatchesRecompute(SUM_ORACLE);
        });
    }

    private void assertMatchesRecompute(String oracleProjection) throws Exception {
        assertMatchesRecompute(oracleProjection, "");
    }

    private void assertMatchesRecompute(String oracleProjection, String where) throws Exception {
        assertNoRefreshFaults("lv");
        final String projection = oracleProjection.substring(oracleProjection.lastIndexOf(" AS ") + 4);
        assertSqlCursors(
                "SELECT ts, sym, x, " + oracleProjection + " FROM base" + where + " ORDER BY ts, sym",
                "SELECT ts, sym, x, " + projection + " FROM lv ORDER BY ts, sym"
        );
    }

    private void assertPriceDecision(
            String projection,
            String oracle,
            int keys,
            int keyRows,
            int roots,
            boolean hasRootBelow,
            long scanMaxKeys,
            String expected
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, scanMaxKeys);
        assertMemoryLeak(() -> {
            createView(projection, "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                buildHm(job, keys, keyRows, roots, hasRootBelow);
                final long entriesBefore = colLong("checkpoint_timeline_entries");
                final long[] before = counters(job);
                late(job, lateRow(DAY1, "b"));
                final String actual = delta(before, job);
                Assert.assertEquals("scan.max.keys=" + scanMaxKeys + ", N=" + keyRows, expected, actual.substring(0, Math.min(actual.length(), expected.length())));
                if (expected.contains("declines=1")) {
                    // The truncate at R keeps the roots below it and the closing head; with none
                    // below, the retire leaves the closing seal a one-root history.
                    Assert.assertEquals(hasRootBelow ? 2 : 1, colLong("checkpoint_timeline_entries"));
                } else {
                    Assert.assertEquals(entriesBefore, colLong("checkpoint_timeline_entries"));
                }
            }
            assertMatchesRecompute(oracle);
            dropViewAndBase();
        });
    }

    private void assertScanFloorDecision(int wRows, int keyRows, String where, String expected) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createView(LAG, where);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                setCurrentMicros(0L);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + DAY0_2300 + "::timestamp, 'a', 1.0)", true);
                commit(job, "INSERT INTO base (ts, sym, x) SELECT (" + (DAY1 + MINUTE) + " + x)::timestamp, 'w'::symbol, 1.0 FROM long_sequence(" + wRows + ")"
                        + " UNION ALL SELECT (" + (DAY1 + HOUR) + " + x)::timestamp, ('s' || ((x - 1) % 1000 + 1))::symbol, 1.0 FROM long_sequence(" + keyRows + ")"
                        + " UNION ALL SELECT " + (DAY0_2300 + 30 * MINUTE) + "::timestamp, 'a'::symbol, 1.0 FROM long_sequence(1)", true);
                for (int j = 1; j <= 12; j++) {
                    commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (DAY1 + HOUR + j * 5 * MINUTE) + "::timestamp, 'a', 1.0)", true);
                }
                final long[] before = counters(job);
                late(job, lateRow(DAY1 + 30 * MINUTE, "b"));
                final String actual = delta(before, job);
                Assert.assertEquals("w=" + wRows + ", N=" + keyRows + where, expected, actual.substring(0, Math.min(actual.length(), expected.length())));
            }
            assertMatchesRecompute(LAG_ORACLE, where);
            dropViewAndBase();
        });
    }

    // T16: an hourly anchor, the union range (per-segment repair off) and a frontier sweep that
    // evicts k between two chained boundaries. Roots: 00:20 (f1, f2, k = 5 at 00:10 and f3, one
    // commit), 01:10 a, 02:10 b (its row sweeps every hour-0 key), 03:10 k (x = 5), 03:40 c. The
    // late commit z at 00:15 and d at 03:50 (above the frontier, so H is the end of the table).
    // The bivariate call's map follows the sweep: B2's complete freeze omits k, B3's holds it
    // again with the bytes it had at B1. A second late row, k at 03:20 (x = 9), resumes from the
    // 03:10 root and reads covar ((5 - 7)^2 + (9 - 7)^2) / 2 = 4.0, the recompute's value; a B3
    // root missing k would read 0.0.
    private void assertUnionRangeHeadMissAcrossASweep(String call) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_STALE_PERCENT, 0);
        final long ts0 = 1_767_225_600_000_000L; // 2026-01-01T00:00:00Z
        assertMemoryLeak(() -> {
            setCurrentMicros(0L);
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, " + call
                    + " OVER w AS l FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                final LiveViewInstance instance = instance();
                commit(job, "INSERT INTO base (ts, sym, x) VALUES "
                        + "(" + (ts0 + MINUTE) + "::timestamp, 'f1', 1.0), "
                        + "(" + (ts0 + 2 * MINUTE) + "::timestamp, 'f2', 1.0), "
                        + "(" + (ts0 + 10 * MINUTE) + "::timestamp, 'k', 5.0), "
                        + "(" + (ts0 + 20 * MINUTE) + "::timestamp, 'f3', 1.0)", true);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (ts0 + HOUR + 10 * MINUTE) + "::timestamp, 'a', 1.0)", true);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (ts0 + 2 * HOUR + 10 * MINUTE) + "::timestamp, 'b', 1.0)", true);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (ts0 + 3 * HOUR + 10 * MINUTE) + "::timestamp, 'k', 5.0)", true);
                commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (ts0 + 3 * HOUR + 40 * MINUTE) + "::timestamp, 'c', 1.0)", true);
                Assert.assertEquals(5, colLong("checkpoint_timeline_entries"));
                final long compactionsBefore = instance.getAnchorWindow().getCompactionCount();
                final long[] before = counters(job);
                late(job, lateRow(ts0 + 15 * MINUTE, "z") + ", " + lateRow(ts0 + 3 * HOUR + 50 * MINUTE, "d"));
                // Window: 5 boundaries chained from nothing, 4 incremental (keys 5, then a, b, k
                // and c alone), and the closing seal for d, whose batch reaches back to z, so it
                // replaces the newest root with a complete image of the 4 live keys b, k, c, d.
                // The bivariate call: 5 complete boundary captures of 5, 6, 2, 2 and 3 keys, and
                // the closing seal's 4. HEAD imaged the window complete at every capture too: 22.
                Assert.assertEquals(
                        call,
                        "route=range versioned=5 declines=0 discarded=0 winCaptures=6 winIncremental=4 winKeys=13 fnCaptures=6 fnKeys=22",
                        delta(before, job)
                );
                Assert.assertEquals(2, instance.getAnchorWindow().getCompactionCount() - compactionsBefore);
                late(job, lateRow(ts0 + 3 * HOUR + 20 * MINUTE, "k").replace("1.0)", "9.0)"));
                Assert.assertEquals("resume from anchor", colStr("checkpoint_repair_last_disposition"));
                assertNoRefreshFaults("lv");
            }
            final String oracle = "SELECT ts, sym, x, " + call + " OVER (PARTITION BY sym, timestamp_floor('1h', ts) ORDER BY ts) AS l FROM base";
            assertSqlCursors(oracle + " ORDER BY ts, sym", "SELECT ts, sym, x, l FROM lv ORDER BY ts, sym");
            restartLiveViewsAndRecover();
            assertSqlCursors(oracle + " ORDER BY ts, sym", "SELECT ts, sym, x, l FROM lv ORDER BY ts, sym");
        });
    }

    // HM, or HM0 without the 23:00 root below the late row.
    private void buildHm(LiveViewRefreshJob job, int keys, int keyRows, int roots, boolean hasRootBelow) throws Exception {
        driveSeedToCompletion(job, "lv");
        setCurrentMicros(0L);
        if (hasRootBelow) {
            commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + DAY0_2300 + "::timestamp, 'a', 1.0)", true);
        }
        commit(job, keyCommit(keys, keyRows), true);
        for (int j = 1; j <= roots; j++) {
            commit(job, "INSERT INTO base (ts, sym, x) VALUES (" + (DAY1 + j * 5 * MINUTE) + "::timestamp, 'a', 1.0)", true);
        }
        Assert.assertEquals((hasRootBelow ? 2 : 1) + roots, colLong("checkpoint_timeline_entries"));
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

    // One commit, refreshed alone; isRootDue moves the clock past the 5-minute cadence first.
    private void commit(LiveViewRefreshJob job, String sql, boolean isRootDue) throws Exception {
        setCurrentMicros(currentMicros + (isRootDue ? 5 * MINUTE + SECOND : SECOND));
        execute(sql);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private long[] counters(LiveViewRefreshJob job) {
        final LiveViewInstance instance = instance();
        final LiveViewCheckpointCaptureLedger ledger = instance.getCheckpointCaptureLedgerForTest();
        return new long[]{
                job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_KEY_BUDGET_DECLINE_COUNT),
                job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_DISCARDED_ROOT_COUNT),
                instance.getCheckpointRepairRootsVersioned(),
                ledger.getFunctionCaptures(),
                ledger.getFunctionKeysImaged(),
                ledger.getWindowCaptures(),
                ledger.getWindowIncrementalCaptures(),
                ledger.getWindowKeysImaged(),
                job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD),
                job.getStatsForTest().getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_SEGMENT_REBUILD)
        };
    }

    private void createView(String projection, String where) throws Exception {
        setCurrentMicros(0L);
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL CAPACITY 65536 NOCACHE, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, x, " + projection + " FROM base" + where + ANCHOR_WINDOW);
    }

    private String delta(long[] before, LiveViewRefreshJob job) {
        final long[] after = counters(job);
        final long range = after[8] - before[8];
        final long segment = after[9] - before[9];
        final String route = range == 1 && segment == 0 ? "range" : segment == 1 && range == 0 ? "segment" : range == 0 && segment == 0 ? "none" : "range+" + range + ",segment+" + segment;
        return "route=" + route
                + " versioned=" + (after[2] - before[2])
                + " declines=" + (after[0] - before[0])
                + " discarded=" + (after[1] - before[1])
                + " winCaptures=" + (after[5] - before[5])
                + " winIncremental=" + (after[6] - before[6])
                + " winKeys=" + (after[7] - before[7])
                + " fnCaptures=" + (after[3] - before[3])
                + " fnKeys=" + (after[4] - before[4]);
    }

    private void dropViewAndBase() throws Exception {
        execute("DROP LIVE VIEW lv");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            drainJob(job);
        }
        execute("DROP TABLE base");
        drainWalQueue();
    }

    private void late(LiveViewRefreshJob job, String rowsOrSelect) throws Exception {
        setCurrentMicros(currentMicros + SECOND);
        execute(rowsOrSelect.startsWith("SELECT") ? "INSERT INTO base (ts, sym, x) " + rowsOrSelect : "INSERT INTO base (ts, sym, x) VALUES " + rowsOrSelect);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    @Override
    protected boolean isRefreshJobLoggingGuaranteed() {
        return false;
    }
}
