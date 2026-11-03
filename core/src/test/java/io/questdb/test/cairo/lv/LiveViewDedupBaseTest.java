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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.QueryAssertion;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Correctness suite for live views over a DEDUP-enabled base table. Such a view
 * takes the coupled, applied-reader refresh path: instead of appending the base's
 * raw (pre-dedup) WAL stream, the refresh worker reads the applied, post-dedup base
 * via a {@code TableReader} and routes any timestamp-overlap batch through the O3
 * replay machinery. See {@code LiveViewRefreshJob#drainAppliedBase}.
 * <p>
 * All tests run under the default (V1) sequencer
 * ({@code cairo.default.seq.part.txn.count = 0}), so any accidental reliance on the
 * V2-only {@code TransactionLogCursor.getTxnMinTimestamp()} would throw immediately;
 * the overlap trigger must source min ts from the base WAL-E event file instead.
 */
public class LiveViewDedupBaseTest extends AbstractLiveViewTest {
    // The rows the item's sequence leaves: the day the base lost, the root below the departed
    // row, and the late row between them.
    private static final String DEPARTED_ROW_THEN_LATE_ROW = """
            ts\tsym\tx\ts
            2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
            2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
            2026-01-01T00:01:30.000000Z\ta\t3.0\t5.0
            """;
    private static final String DEPARTURE_SUM = "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
    // One upsert over the head-miss departure fixture: 00:57 to a new value, 00:58 and 00:59 out
    // of the filter.
    private static final String HEAD_MISS_KEPT_ROW_DEPARTURE = "('2026-01-01T00:57:00.000000Z', 'a', 5.0), "
            + "('2026-01-01T00:58:00.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', -1.0)";
    // What HEAD_MISS_KEPT_ROW_DEPARTURE leaves: the kept row's one-minute frame holds 00:56 too.
    private static final String HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS = headMissDepartureRows(true, 56)
            + "2026-01-01T00:57:00.000000Z\ta\t5.0\t6.0\n";
    private static final String SINGLE_DEPARTURE_FORWARD_ROW = "('2026-01-01T00:05:00.000000Z', 'a', 4.5)";
    // What the single-departure shape leaves: a root at the row that became the frontier.
    private static final String SINGLE_DEPARTURE_LADDER = """
            2025-12-31T12:00:00.000000Z -> 1
            2026-01-01T00:01:00.000000Z -> 2
            2026-01-01T00:02:00.000000Z -> 3
            """;

    // Pin the test clock below all test data before each test. A non-SEED view's
    // lower bound is the CREATE wall-clock moment, and the forward-append refresh path
    // drops rows below it. The test data is timestamped in the past, so without a
    // pinned clock every row would be dropped as pre-CREATE.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAdditiveSameTimestampAcrossCommitsStaysCorrect() throws Exception {
        // Many keys share one ts across separate commits: each later commit's minTs
        // equals the frontier. The applied-reader path would over-trigger a replay here;
        // Phase 2a proves the range clean (no dedup) and takes the cheap raw-WAL append
        // instead. Assert both correctness and that the clean raw-WAL path engaged.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('b', 20, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('c', 30, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                // With no dedup in any commit, the additive same-ts commits route through
                // the cheap raw-WAL path (pre-Phase-2a every one of them would have replayed).
                Assert.assertTrue(
                        "additive same-ts commits must take the cheap raw-WAL path, not replay",
                        instance.getDedupRawWalCleanCycles() > 0
                );
            }
            assertQuery("SELECT sym, val, ts FROM lv ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\tval\tts\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\n" +
                            "b\t20\t2026-01-01T00:00:01.000000Z\n" +
                            "c\t30\t2026-01-01T00:00:01.000000Z\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAlterDedupEnableDisableFlipsCadence() throws Exception {
        // Create over a NON-dedup base (lead-eligible), leave an un-flushed lead, then
        // ALTER ... DEDUP ENABLE. The first coupled cycle must reconcile the stale lead
        // by rebuilding from the applied base, and a frontier-ts duplicate must
        // collapse. Then DISABLE and confirm a later forward row still appends.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: flushed to disk (first flush).
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Batch 2 within FLUSH EVERY (clock still 0): stays as an un-flushed lead.
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 25, '2026-01-01T00:00:03.000000Z'), " +
                        "('a', 35, '2026-01-01T00:00:04.000000Z')");
                drainWalQueue();
                drainJob(job);

                // Enable dedup: the view must flip to the coupled path next cycle.
                execute("ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, sym)");
                drainWalQueue();

                // A dedup replacement at the frontier ts=02 (val 20 -> 99). The first
                // coupled cycle resumes from the disk point, sees the pending lead
                // commits at/below the frontier, replays, and drops the stale lead.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 99, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t99\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t25\t2026-01-01T00:00:03.000000Z\t3\n" +
                            "a\t35\t2026-01-01T00:00:04.000000Z\t4\n");

            // Disable dedup and confirm the view returns to normal forward appending.
            // Let the structural DISABLE commit settle in its own cycle before the
            // data insert, so the two do not share a single lead-drain range.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("ALTER TABLE base DEDUP DISABLE");
                drainWalQueue();
                setCurrentMicros(4_000_000L);
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(6_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 50, '2026-01-01T00:00:05.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\n" +
                            "a\t99\t2026-01-01T00:00:02.000000Z\n" +
                            "a\t25\t2026-01-01T00:00:03.000000Z\n" +
                            "a\t35\t2026-01-01T00:00:04.000000Z\n" +
                            "a\t50\t2026-01-01T00:00:05.000000Z\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchoredWindowReplayOverDedupBase() throws Exception {
        // Anchored window (a per-hour cumulative sum that resets at each anchor
        // bucket) over a dedup base. A below-frontier dedup replacement inside the
        // FIRST hour bucket routes through the O3 replay, which must restore the
        // anchor-map state and recompute only that bucket's downstream sums while the
        // SECOND bucket (a different anchor value) stays untouched. This exercises the
        // anchor-map snapshot/restore contract on the coupled dedup path, distinct
        // from the un-anchored cumulative/row_number tests.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT sym, val, ts, " +
                    "sum(val) OVER w AS cum FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Bucket 00:00 -> cum 10, 30; bucket 01:00 (anchor reset) -> cum 30, 70.
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10.0, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20.0, '2026-01-01T00:30:00.000000Z'), " +
                        "('a', 30.0, '2026-01-01T01:00:01.000000Z'), " +
                        "('a', 40.0, '2026-01-01T01:30:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Replace the mid-bucket row ts=00:30 (below the frontier ts=01:30):
                // val 20 -> 200. Only bucket 00:00 recomputes (cum 10, 210); the
                // anchor reset keeps bucket 01:00 at 30, 70.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 200.0, '2026-01-01T00:30:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, cum FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\tcum\n" +
                            "a\t10.0\t2026-01-01T00:00:01.000000Z\t10.0\n" +
                            "a\t200.0\t2026-01-01T00:30:00.000000Z\t210.0\n" +
                            "a\t30.0\t2026-01-01T01:00:01.000000Z\t30.0\n" +
                            "a\t40.0\t2026-01-01T01:30:00.000000Z\t70.0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnUpsertMovingEveryNewestRowOutOfTheFilterOverALostDayBlocksTheRestart() throws Exception {
        // The three newest rows move out of the filter and nothing is emitted in their place, from
        // a rebuild whose runtime holds only the state it warmed from one minute below them. No
        // root sits at the new frontier 00:56, so the repair retires the timeline rather than keep
        // one a restart could not restore. That restart then rebuilds with the restatement guard
        // armed, which refuses the rebuild over the day the base lost: the view stops and keeps
        // its 2025-12-31 row, where a rebuild would drop it. Known limitation: a zero-row
        // end-of-table repair with no restorable root at its frontier retires the timeline.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView("TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitMinuteBatchesOverTheLostDay(job);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                final long resetsBefore = departureView().getCheckpointTimelineResets();
                commitDeparture(
                        job,
                        "('2026-01-01T00:57:00.000000Z', 'a', -1.0), ('2026-01-01T00:58:00.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', -1.0)"
                );
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
                Assert.assertEquals("the repair retires the timeline", resetsBefore + 1, departureView().getCheckpointTimelineResets());
                try (Path dir = departureCheckpointsDir(); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                    Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                }
            }
            final String rows = headMissDepartureRows(true, 56);
            assertDepartureRows(rows);
            for (int restart = 0; restart < 2; restart++) {
                final LogCapture capture = new LogCapture();
                capture.start();
                try {
                    restartLiveViewsAndRecover();
                    capture.drain();
                    capture.assertLogged("live view restart rebuilding from applied base [view=lv, cause=timeline is absent");
                    capture.assertNotLogged("runs without the restatement guard");
                } finally {
                    capture.stop();
                }
                Assert.assertEquals(
                        "rebuild_blocked",
                        LiveViewCheckpointRecoveryPhase.routeName(departureView().getCheckpointRestoreRoute())
                );
                Assert.assertTrue(departureView().isCheckpointRecoveryBlocked());
                assertDepartureRows(rows);
            }
        });
    }

    @Test
    public void testAnUpsertMovingTheNewestRowOfOneKeyTiedAtTheFrontierKeepsTheSplice() throws Exception {
        // Two keys share the newest timestamp and one of them leaves the filter. The other keeps
        // the frontier where the spliced root sits, so the rebuild's output reaches that root and
        // nothing is truncated: the seal only re-stamps it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView("TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                commitDeparture(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, minuteRowsOfA(31, 59, "1.0") + ", ('2026-01-01T00:59:00.000000Z', 'b', 1.0)");
                assertDepartureLadder("""
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:59:00.000000Z -> 62
                        """);
                final long generationBefore = departureGeneration();
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                commitDeparture(job, "('2026-01-01T00:59:00.000000Z', 'a', -1.0)");
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the splice alone, the seal re-stamps", generationBefore + 1, departureGeneration());
                assertDepartureLadder("""
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:59:00.000000Z -> 61
                        """);
            }
            final String rows = headMissDepartureRows(true, 58) + "2026-01-01T00:59:00.000000Z\tb\t1.0\t1.0\n";
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartureRestored(rows);
            }
        });
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOfOneDayOutOfTheFilterRestoresRetainedRows() throws Exception {
        // The departed rows are all the next day holds, so the kept row, the new frontier, sits on
        // the day below them: the truncate at the frontier drops the root of the emptied day.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView("TIMESTAMP");
            final StringSink firstBatch = new StringSink();
            final StringSink secondBatch = new StringSink();
            final StringSink rows = new StringSink();
            rows.put("ts\tsym\tx\ts\n2025-12-31T23:55:00.000000Z\ta\t0.5\t0.5\n");
            for (int minute = 0; minute <= 59; minute++) {
                final String ts = "2026-01-01T23:" + (minute < 10 ? "0" : "") + minute + ":00.000000Z";
                final StringSink batch = minute <= 30 ? firstBatch : secondBatch;
                batch.put(batch.length() > 0 ? ", " : "").put("('").put(ts).put("', 'a', 1.0)");
                if (minute < 59) {
                    rows.put(ts).put("\ta\t1.0\t").put(minute == 0 ? "1.0" : "2.0").put('\n');
                }
            }
            rows.put("2026-01-01T23:59:00.000000Z\ta\t5.0\t6.0\n");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                commitDeparture(job, firstBatch.toString());
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, secondBatch + ", ('2026-01-02T00:00:00.000000Z', 'a', 1.0), ('2026-01-02T00:01:00.000000Z', 'a', 1.0)");
                assertDepartureLadder("""
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-02T00:01:00.000000Z -> 63
                        """);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                commitDeparture(
                        job,
                        "('2026-01-01T23:59:00.000000Z', 'a', 5.0), ('2026-01-02T00:00:00.000000Z', 'a', -1.0), ('2026-01-02T00:01:00.000000Z', 'a', -1.0)"
                );
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                assertDepartureLadder("""
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T23:59:00.000000Z -> 61
                        """);
            }
            assertDepartureRows(rows.toString());
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartureRestored(rows.toString());
            }
        });
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOutOfTheFilterAtNullRestoresRetainedRows() throws Exception {
        assertNewestRowsLeavingTheFilterRestore("NULL");
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOutOfTheFilterAtTheEpochRestoresRetainedRows() throws Exception {
        // The minute shape in the first hour of the epoch, so the truncate at the new frontier
        // floors a few minutes above the smallest designated timestamp.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView("TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, minuteRowsOfA(0, 30, "1.0").replace("2026-01-01", "1970-01-01"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, minuteRowsOfA(31, 59, "1.0").replace("2026-01-01", "1970-01-01"));
                assertDepartureLadder("""
                        1970-01-01T00:30:00.000000Z -> 31
                        1970-01-01T00:59:00.000000Z -> 60
                        """);
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                commitDeparture(
                        job,
                        "('1970-01-01T00:57:00.000000Z', 'a', 5.0), ('1970-01-01T00:58:00.000000Z', 'a', -1.0), ('1970-01-01T00:59:00.000000Z', 'a', -1.0)"
                );
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                assertDepartureLadder("""
                        1970-01-01T00:30:00.000000Z -> 31
                        1970-01-01T00:57:00.000000Z -> 58
                        """);
            }
            final String rows = (headMissDepartureRows(false, 56) + "2026-01-01T00:57:00.000000Z\ta\t5.0\t6.0\n")
                    .replace("2026-01-01", "1970-01-01");
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartureRestored(rows);
            }
        });
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOutOfTheFilterAtZeroRestoresRetainedRows() throws Exception {
        assertNewestRowsLeavingTheFilterRestore("0.0");
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOutOfTheFilterOverTenThousandKeysRestores() throws Exception {
        // Ten thousand keys hold a row each an hour below the departure, so the seal at the new
        // frontier freezes a runtime over that many keys' expired frames. The base lost nothing,
        // so the view gives what a recompute gives.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView("TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
                execute("INSERT INTO base SELECT '2025-12-31T23:56:00.000000Z'::timestamp + x, 'k' || x, 1.0 FROM long_sequence(10_000)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                commitDeparture(job, minuteRowsOfA(0, 30, "1.0"));
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, minuteRowsOfA(31, 59, "1.0"));
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                commitDeparture(
                        job,
                        "('2026-01-01T00:57:00.000000Z', 'a', 5.0), ('2026-01-01T00:58:00.000000Z', 'a', -1.0), ('2026-01-01T00:59:00.000000Z', 'a', -1.0)"
                );
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                final LongList ladder = snapshotCheckpointLadder(departureView());
                Assert.assertEquals(ts("2026-01-01T00:57:00.000000Z"), ladder.getQuick(ladder.size() - 2));
                Assert.assertEquals(10_059, ladder.getQuick(ladder.size() - 1));
            }
            assertHeadMissDepartureMatchesRecompute();
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertRestoredFromTimeline("lv");
                assertHeadMissDepartureMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOutOfTheFilterRestoresRetainedRows() throws Exception {
        assertNewestRowsLeavingTheFilterRestore("-1.0");
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOutOfTheFilterThenABaseColumnAddedRestoresTheRuntimeInPlace() throws Exception {
        // An unreferenced ADD COLUMN between the departure and the next late row drifts the
        // compiled plan, and the drift recovery restores the runtime from the root the departure
        // sealed at the new frontier. A root above the frontier would fail that restore's row
        // count and rebuild the view from a base that lost a day of its rows.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(() -> {
                createHeadMissDepartureView("TIMESTAMP");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitMinuteBatchesOverTheLostDay(job);
                    commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                    execute("ALTER TABLE base ADD COLUMN extra INT");
                    drainWalQueue();
                    commitDeparture(job, "('2026-01-01T00:56:30.000000Z', 'a', 3.0, NULL)");
                }
                final LiveViewInstance instance = departureView();
                capture.drain();
                capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=base table metadata change");
                Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals("the restore must not fall back to a rebuild", 0, instance.getCheckpointRebuildAttempts());
                Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                // The late row's frame holds 00:56, and the kept row's holds the late row too.
                final String rows = headMissDepartureRows(true, 56) + """
                        2026-01-01T00:56:30.000000Z\ta\t3.0\t4.0
                        2026-01-01T00:57:00.000000Z\ta\t5.0\t9.0
                        """;
                assertDepartureRows(rows);
                for (int restart = 0; restart < 2; restart++) {
                    restartLiveViewsAndRecover();
                    assertDepartureRestored(rows);
                }
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOutOfTheFilterThenABaseTruncateRestores() throws Exception {
        // TRUNCATE removes the base rows, which the view walks past keeping its own: a restart
        // after it restores them from the root the departure sealed at the new frontier.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView("TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitMinuteBatchesOverTheLostDay(job);
                commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                execute("TRUNCATE TABLE base");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            assertDepartureRows(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartureRestored(HEAD_MISS_KEPT_ROW_DEPARTURE_ROWS);
            }
        });
    }

    @Test
    public void testAnUpsertMovingTheNewestRowsOutOfTheFilterThenDropLiveViewReleasesEverything() throws Exception {
        // The view goes right after the departure that truncated its timeline and sealed the new
        // frontier: the drop leaves nothing open, and the base stays usable, holding the minute
        // rows the upsert rewrote rather than removed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView("TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitMinuteBatchesOverTheLostDay(job);
                commitDeparture(job, HEAD_MISS_KEPT_ROW_DEPARTURE);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                execute("DROP LIVE VIEW lv");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
            assertQuery("SELECT count() FROM base")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n60\n");
        });
    }

    @Test
    public void testAnUpsertMovingTheRowAtTheLargestMicrosecondTimestampOutOfTheFilterRestores() throws Exception {
        // The departed row sits at the largest designated timestamp a microsecond table takes,
        // so the truncate at the new frontier floors at that timestamp exactly.
        assertRowAtTheLargestTimestampLeavingTheFilterRestores(
                "TIMESTAMP",
                "9999-12-30T23:55:00.000000Z",
                "9999-12-31T23:",
                ":00.000000Z",
                "9999-12-31T23:59:59.999998Z",
                "9999-12-31T23:59:59.999999Z"
        );
    }

    @Test
    public void testAnUpsertMovingTheRowAtTheLargestNanosecondTimestampOutOfTheFilterRestores() throws Exception {
        assertRowAtTheLargestTimestampLeavingTheFilterRestores(
                "TIMESTAMP_NS",
                "2261-12-30T23:55:00.000000000Z",
                "2261-12-31T23:",
                ":00.000000000Z",
                "2261-12-31T23:59:59.999999998Z",
                "2261-12-31T23:59:59.999999999Z"
        );
    }

    @Test(timeout = 120_000)
    public void testAppliedBaseDrainDefersOnApplyLagInsteadOfDeadlocking() throws Exception {
        // Cooperative apply-lag handoff on the coupled dedup path (regression for
        // M-1). When the base sequencer head advances past the applied reader, the
        // drainAppliedBase gate must NOT block-spin in waitForApply waiting for the
        // apply: on the single-threaded refresh/drain model the same worker has to
        // advance that apply, so spinning deadlocks (and with the flush clock frozen
        // the retry budget never trips); with a live clock a sustained-lag streak
        // instead ticks the flush-retry budget to exhaustion and durably invalidates
        // the view. ensureBaseApplied now peeks the applied seqTxn BEFORE pinning the
        // scan reader and throws a cooperative signal that unwinds the cycle untouched
        // - no watermark advance, no retry-budget tick, no invalidation - and the next
        // tick converges once the base apply lands.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Seed and fully refresh an initial row so the coupled path has an
                // established frontier and watermarks.
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                final long processedBefore = instance.getLastProcessedSeqTxn();
                final long latestSeenBefore = instance.getLatestSeenTs();
                final int retryBefore = instance.getFlushRetryCount();

                // Commit a second row to the base sequencer but do NOT apply it
                // (skip drainWalQueue): the sequencer head advances past the applied
                // reader, so isRangeProvablyClean fails and the coupled
                // drainAppliedBase gate observes the apply lag. Advance the clock past
                // FLUSH EVERY so the flush-due gate lets this cycle reach the drain.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('b', 20, '2026-01-01T00:00:02.000000Z')");

                // With the old block-spin this drain never returns (the frozen clock
                // keeps the retry budget from tripping). It must return, and it must
                // leave the view exactly as it was.
                drainJob(job);

                Assert.assertFalse("apply lag must not invalidate the coupled view", instance.isInvalid());
                Assert.assertEquals("watermark must not advance on apply lag",
                        processedBefore, instance.getLastProcessedSeqTxn());
                Assert.assertEquals("frontier must not advance on apply lag",
                        latestSeenBefore, instance.getLatestSeenTs());
                Assert.assertEquals("apply lag must not tick the flush-retry budget",
                        retryBefore, instance.getFlushRetryCount());

                // Apply the base commit, advance the clock past the apply-lag
                // back-off, then let the next tick converge the drain.
                drainWalQueue();
                setCurrentMicros(currentMicros + 1_000_000);
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse("view must stay valid after the drain converges", instance.isInvalid());
            }
            // Both rows land in ts order with a gapless row_number sequence.
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "b\t20\t2026-01-01T00:00:02.000000Z\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testApplyLagBackOffThrottlesRedrainWithinWindow() throws Exception {
        // The apply-lag back-off floor must THROTTLE re-draining, not just permit
        // convergence. When a coupled dedup drain defers on base apply lag it arms a
        // short back-off (LiveViewRefreshJob.refreshInstance); a refresh tick that
        // enters the view inside that window must skip the whole cycle at the pre-latch
        // guard rather than re-enter drainAppliedBase, re-observe the same lag, and burn
        // a full window recompute every tick until apply lands. The companion
        // testAppliedBaseDrainDefersOnApplyLagInsteadOfDeadlocking proves the
        // cooperative unwind returns and leaves the view untouched; this proves the
        // throttle itself engages. Because the fallback scan gates on the APPLIED base
        // point (getWriterTxn), which does not advance during apply lag, a re-entry has
        // to come from a fresh notification (a new committed-but-unapplied base commit);
        // this test posts one inside the window. Deleting either the arm or the
        // pre-latch guard fails an assertion below.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Establish the coupled frontier and watermarks with a fully applied
                // first row.
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Commit a second row to the base sequencer but do NOT apply it (skip
                // drainWalQueue): the commit posts a refresh notification, and the
                // coupled drainAppliedBase gate then observes the apply lag. Freeze the
                // clock at a known point past FLUSH EVERY so the flush-due gate lets this
                // cycle reach the drain.
                final long deferArmedAtUs = 2_000_000L;
                final long backOffUs = 5_000L; // APPLY_LAG_DEFER_BACKOFF_US
                setCurrentMicros(deferArmedAtUs);
                execute("INSERT INTO base (sym, val, ts) VALUES ('b', 20, '2026-01-01T00:00:02.000000Z')");
                drainJob(job);

                // The cooperative unwind armed the back-off floor at now + backOffUs.
                // Deleting the arm leaves the floor LONG_NULL, so this fails.
                Assert.assertEquals("apply-lag defer must arm a back-off floor",
                        deferArmedAtUs + backOffUs, instance.getApplyLagDeferUntilUs());

                // Step the clock forward but stay strictly INSIDE the back-off window,
                // then commit (again without applying) a third row so its notification
                // re-enters the view this tick. The re-entry must be throttled: the
                // pre-latch guard skips the cycle, so the floor is left exactly where it
                // was. Deleting the guard lets the tick re-enter the drain, re-observe
                // the lag, and re-arm the floor to (now + backOffUs) = a strictly LATER
                // value, so the assertion fails.
                setCurrentMicros(deferArmedAtUs + 2_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('c', 30, '2026-01-01T00:00:03.000000Z')");
                drainJob(job);
                Assert.assertEquals("a tick inside the back-off window must not re-drain (floor unchanged)",
                        deferArmedAtUs + backOffUs, instance.getApplyLagDeferUntilUs());
                Assert.assertFalse("a throttled tick must not invalidate the view", instance.isInvalid());

                // Apply the base commits, step past the back-off floor, and let the next
                // tick converge the drain.
                drainWalQueue();
                setCurrentMicros(deferArmedAtUs + backOffUs + 1_000_000L);
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse("view must stay valid after the drain converges", instance.isInvalid());
                // recordRefreshSuccess must clear the back-off floor once the drain
                // converges. Deleting that clear leaves the stale floor here, so this fails.
                Assert.assertEquals("a converged refresh must clear the back-off floor",
                        Numbers.LONG_NULL, instance.getApplyLagDeferUntilUs());
            }
            // All three rows land in ts order with a gapless row_number sequence.
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "b\t20\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "c\t30\t2026-01-01T00:00:03.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testApplyLagBackOffClearsEarlyWhenBaseAppliesBeforeFloor() throws Exception {
        // The apply-lag back-off floor is only an anti-spin bound; the real
        // precondition is the base applying past the seqTxn that forced the defer.
        // When apply catches up while the clock is still strictly BELOW the floor,
        // the pre-latch guard in LiveViewRefreshJob.refreshInstance must re-check
        // getWriterTxn against the armed target, clear the floor, and drain this
        // tick rather than stall until the wall-clock floor elapses - which a
        // frozen test clock never crosses. The companion
        // testApplyLagBackOffThrottlesRedrainWithinWindow proves the throttle
        // engages while apply still lags; this proves the early clear fires the
        // moment apply catches up. Reverting the early-clear re-check leaves the
        // pre-latch guard a plain clock comparison, so under the frozen clock the
        // deferred row never converges and the assertions below fail.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Establish the coupled frontier with a fully applied first row.
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                final long processedAfterFirst = instance.getLastProcessedSeqTxn();

                // Commit a second row to the sequencer but do NOT apply it: the
                // coupled drainAppliedBase gate observes the apply lag and the
                // cooperative unwind arms the back-off floor at now + backOffUs.
                final long deferArmedAtUs = 2_000_000L;
                final long backOffUs = 5_000L; // APPLY_LAG_DEFER_BACKOFF_US
                setCurrentMicros(deferArmedAtUs);
                execute("INSERT INTO base (sym, val, ts) VALUES ('b', 20, '2026-01-01T00:00:02.000000Z')");
                drainJob(job);
                Assert.assertEquals("apply-lag defer must arm a back-off floor",
                        deferArmedAtUs + backOffUs, instance.getApplyLagDeferUntilUs());
                Assert.assertEquals("watermark must not advance while deferred",
                        processedAfterFirst, instance.getLastProcessedSeqTxn());

                // Apply the base commit so getWriterTxn passes the armed target,
                // but keep the clock frozen strictly BELOW the floor. The re-entry
                // now finds apply caught up: the pre-latch guard must clear the
                // floor early and drain this tick.
                drainWalQueue();
                Assert.assertTrue("clock must stay below the floor to isolate the early clear",
                        engine.getConfiguration().getMicrosecondClock().getTicks() < deferArmedAtUs + backOffUs);
                drainJob(job);
                drainWalQueue();

                Assert.assertFalse("early-clear convergence must not invalidate the view", instance.isInvalid());
                Assert.assertEquals("apply catching up below the floor must clear it early",
                        Numbers.LONG_NULL, instance.getApplyLagDeferUntilUs());
                Assert.assertTrue("the deferred row must be processed after the early clear",
                        instance.getLastProcessedSeqTxn() > processedAfterFirst);
            }
            // Both rows land in ts order with a gapless row_number sequence.
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "b\t20\t2026-01-01T00:00:02.000000Z\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBaseTruncateFreezesDerivedPrefix() throws Exception {
        // A base TRUNCATE below the frontier is a data-shaped non-DATA commit
        // (walId>0, isDataType=false): the WAL-E walk excludes it from batchMinTs, so
        // no history-rewriting replay fires and the LV's derived prefix stays frozen.
        // A following in-order commit still appends.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Remove all base history below the frontier.
                setCurrentMicros(2_000_000L);
                execute("TRUNCATE TABLE base");
                drainWalQueue();
                drainJob(job); // frozen: no replay, no change to the derived prefix
                drainWalQueue();

                // A later forward row still appends on top of the frozen prefix.
                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\n" +
                            "a\t30\t2026-01-01T00:00:03.000000Z\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testChangeDedupKeysWhileDedupEnabled() throws Exception {
        // Changing the dedup UPSERT keys on an already-dedup base (no DISABLE first) is a
        // structural op that does NOT invalidate the view - SET_DEDUP_ENABLE carries no
        // mat-view/live-view invalidation reason - and the refresh cadence stays coupled
        // to the applied, post-dedup base. After switching the key set, a below-frontier
        // UPSERT that now collides under the NEW keys must collapse in the view; under the
        // OLD keys the same row would have been a new insert. That the row collapses (not
        // appends) proves the coupled path reads the re-keyed dedup result, not the raw
        // WAL stream.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('b', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse("view must start valid",
                        engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());

                // Switch the dedup keys in place: (ts, sym) -> (ts, val). No DISABLE
                // needed; the change must not invalidate the view.
                execute("ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, val)");
                drainWalQueue();
                Assert.assertFalse("changing dedup keys must not invalidate the LV",
                        engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());

                // A below-frontier UPSERT keyed (ts=01, val=10) now collides with
                // ('a', 10, 01) and replaces its sym; under the OLD (ts, sym) keys it
                // would have been a distinct row. The coupled O3 replay must collapse it.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('c', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse("view must stay valid after the re-keyed replace",
                        engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "c\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "b\t20\t2026-01-01T00:00:02.000000Z\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateRejectsUnsnapshottableAnchorKeyOverDedupBase() throws Exception {
        // Replay is the normal correction path for a dedup base, and replay restores
        // window state from a checkpoint whose anchor map is keyed by the PARTITION BY
        // columns. A key type the snapshot codec cannot persist (here UUID) must be
        // rejected at CREATE rather than pass and then serve wrong results at the first
        // replay. The reject is enforced by the per-function supportsCheckpointState() check,
        // which folds in LiveViewSnapshotKeyCodec.isAllTypesSupported over the same
        // partition keys and runs for every view. This test pins the observable contract:
        // such a view does not silently create over a dedup base.
        assertMemoryLeak(() -> {
            // sym drives the dedup keys; u (UUID) is the unsupported anchor partition key.
            execute("CREATE TABLE base (sym SYMBOL, u UUID, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT u, val, ts, " +
                        "sum(val) OVER w AS cum FROM base " +
                        "WINDOW w AS (PARTITION BY u ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))");
                Assert.fail("expected SqlException for an unsnapshottable anchor key over a DEDUP base");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("incremental snapshot is not supported")
                );
            }
            Assert.assertNull(
                    "the rejected view must not have been created",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
        });
    }

    @Test
    public void testDeepDedupRecomputesDownstreamWindow() throws Exception {
        // Replace the OLDEST row (below the head checkpoint): the head-miss full
        // rebuild must reflect the new value and recompute every downstream cumulative
        // sum in the same partition frame.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT sym, val, ts, " +
                    "sum(val) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS cum FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10.0, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20.0, '2026-01-01T00:00:02.000000Z'), " +
                        "('a', 30.0, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Replace ts=01: val 10.0 -> 100.0. cum must become 100/120/150.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 100.0, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, cum FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\tcum\n" +
                            "a\t100.0\t2026-01-01T00:00:01.000000Z\t100.0\n" +
                            "a\t20.0\t2026-01-01T00:00:02.000000Z\t120.0\n" +
                            "a\t30.0\t2026-01-01T00:00:03.000000Z\t150.0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDepartedNewestRowAloneInItsDayRestores() throws Exception {
        // The departed row was the view's only row of its day, so the replacement leaves that
        // day's partition of the view empty and the new frontier falls on the day below it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T23:59:00.000000Z', 'a', 2.5), ('2026-01-02T00:01:00.000000Z', 'a', 9.5)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-02T00:01:00.000000Z -> 4
                        """);
                commitDeparture(job, "('2026-01-02T00:01:00.000000Z', 'a', -9.5)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T23:59:00.000000Z -> 3
                        """);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    2026-01-01T23:59:00.000000Z\ta\t2.5\t4.5
                    """;
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            assertDepartureRestoresTwice(rows, "('2026-01-02T00:05:00.000000Z', 'a', 4.5)", """
                    2026-01-02T00:05:00.000000Z\ta\t4.5\t8.5
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowAndALateRowInOneCommitRestoreRetainedRows() throws Exception {
        // One commit carries a late row below the newest root and the upsert that moves that
        // root's row out of the filter. The resume replays the late row alone, so its output ends
        // below the root its splice kept, and that root holds the state at the late row. The
        // resume truncates the spliced timeline at the late row and seals a root there: splice,
        // truncate and seal are three generations.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                final long generationBefore = departureGeneration();
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 3.0), ('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:01:30.000000Z -> 3
                        """);
                Assert.assertEquals(generationBefore + 3, departureGeneration());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            assertDepartureRows(DEPARTED_ROW_THEN_LATE_ROW);
            assertNoRefreshFaults("lv");
            assertDepartureRestoresTwice(DEPARTED_ROW_THEN_LATE_ROW, "('2026-01-01T00:03:00.000000Z', 'a', 4.0)", """
                    2026-01-01T00:03:00.000000Z\ta\t4.0\t8.5
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowAtTheEpochRestores() throws Exception {
        // The smallest frontier a designated timestamp can hold: the view's output ends at the
        // epoch itself once the row one microsecond above it departs. The truncate floors one
        // above that frontier, and drops the root on that microsecond and nothing below it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('1970-01-01T00:00:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('1970-01-01T00:00:00.000001Z', 'a', 2.5)");
                assertDepartureLadder("""
                        1970-01-01T00:00:00.000000Z -> 1
                        1970-01-01T00:00:00.000001Z -> 2
                        """);
                commitDeparture(job, "('1970-01-01T00:00:00.000001Z', 'a', -2.5)");
                assertDepartureLadder("""
                        1970-01-01T00:00:00.000000Z -> 1
                        """);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            final String rows = """
                    ts\tsym\tx\ts
                    1970-01-01T00:00:00.000000Z\ta\t1.5\t1.5
                    """;
            assertDepartureRows(rows);
            assertDepartureRestoresTwice(rows, "('1970-01-01T00:00:00.000002Z', 'a', 4.0)", """
                    1970-01-01T00:00:00.000002Z\ta\t4.0\t5.5
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowInAnInMemoryViewRestoresRetainedRows() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> assertDepartedNewestRowThenALateRowRestore("-2.5", "IN MEMORY 1h ", false));
    }

    @Test
    public void testDepartedNewestRowOfOneKeyTiedAtTheFrontierKeepsTheSplice() throws Exception {
        // The newest root holds two keys on one timestamp, and only one of them departs. The
        // other keeps the view's frontier on that root, so the splice it re-versioned stays
        // where it is and the head seal only re-stamps it: nothing is truncated.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:02:00.000000Z', 'b', 5.0)");
                final long generationBefore = departureGeneration();
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 3
                        """);
                Assert.assertEquals("the splice alone publishes", generationBefore + 1, departureGeneration());
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    2026-01-01T00:02:00.000000Z\tb\t5.0\t5.0
                    """;
            assertDepartureRows(rows);
            assertDepartureRestoresTwice(rows, "('2026-01-01T00:03:00.000000Z', 'a', 4.0)", """
                    2026-01-01T00:03:00.000000Z\ta\t4.0\t6.0
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowOfTenThousandKeysRestores() throws Exception {
        // The seal at the new frontier follows a truncate, so it images every key the runtime
        // holds rather than the keys a dropped root's dirty set names. A forward row on an
        // untouched key after the restart reads that key's restored state back.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                execute("INSERT INTO base SELECT ('2026-01-01T00:00:00.000000Z'::timestamp + x * 1_000_000)::timestamp, "
                        + "('k' || x)::symbol, 1.0 FROM long_sequence(10_000)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                commitDeparture(job, "('2026-01-01T03:00:00.000000Z', 'n1', 2.0), ('2026-01-01T03:01:00.000000Z', 'n2', 4.0)");
                assertDepartureLadder("""
                        2026-01-01T02:46:40.000000Z -> 10000
                        2026-01-01T03:01:00.000000Z -> 10002
                        """);
                commitDeparture(job, "('2026-01-01T03:01:00.000000Z', 'n2', -4.0)");
                assertDepartureLadder("""
                        2026-01-01T02:46:40.000000Z -> 10000
                        2026-01-01T03:00:00.000000Z -> 10001
                        """);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            assertDepartureMatchesRecompute();
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertRestoredFromTimeline("lv");
                Assert.assertFalse(departureView().isCheckpointRecoveryBlocked());
                assertDepartureMatchesRecompute();
                if (restart == 0) {
                    try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                        commitDeparture(job, "('2026-01-01T03:05:00.000000Z', 'k1', 2.0), ('2026-01-01T03:05:00.000000Z', 'n1', 3.0), "
                                + "('2026-01-01T03:05:00.000000Z', 'k10000', 5.0)");
                    }
                    assertQuery("SELECT ts, sym, x, s FROM lv WHERE ts = '2026-01-01T03:05:00.000000Z' ORDER BY sym")
                            .noLeakCheck()
                            .returns("""
                                    ts\tsym\tx\ts
                                    2026-01-01T03:05:00.000000Z\tk1\t2.0\t3.0
                                    2026-01-01T03:05:00.000000Z\tk10000\t5.0\t6.0
                                    2026-01-01T03:05:00.000000Z\tn1\t3.0\t5.0
                                    """);
                    assertDepartureMatchesRecompute();
                }
            }
        });
    }

    @Test
    public void testDepartedNewestRowOnARootTheReplayStopsAtRestampsTheHead() throws Exception {
        // One commit carries a late row and the upsert that moves the newest of three roots out
        // of the filter. The replay stops on the middle root, which the splice re-versions at
        // the new frontier, so the truncate leaves that root as its head: the seal re-stamps it
        // and mints no checkpoint id. Splice and truncate are two generations.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', 4.5)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 3
                        2026-01-01T00:03:00.000000Z -> 4
                        """);
                final long generationBefore = departureGeneration();
                Assert.assertEquals(4, departureNextCheckpointId());
                commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 3.0), ('2026-01-01T00:03:00.000000Z', 'a', -4.5)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:02:00.000000Z -> 4
                        """);
                Assert.assertEquals("[0,1,2]", departureCheckpointIds().toString());
                Assert.assertEquals("the re-stamp must mint no checkpoint id", 4, departureNextCheckpointId());
                Assert.assertEquals(generationBefore + 2, departureGeneration());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(ts("2026-01-01T00:02:00.000000Z"), departureView().getHeadCheckpointMaxTs());
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    2026-01-01T00:01:30.000000Z\ta\t3.0\t5.0
                    2026-01-01T00:02:00.000000Z\ta\t2.5\t7.0
                    """;
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            assertDepartureRestoresTwice(rows, "('2026-01-01T00:04:00.000000Z', 'a', 1.0)", """
                    2026-01-01T00:04:00.000000Z\ta\t1.0\t6.5
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowRestoresARangeFrame() throws Exception {
        assertSingleDepartureRestores(
                "sum(x) OVER (PARTITION BY sym ORDER BY ts RANGE BETWEEN '10' MINUTE PRECEDING AND CURRENT ROW) s FROM base WHERE x > 0",
                "0.5", "1.5", "4.0",
                "8.5"
        );
    }

    @Test
    public void testDepartedNewestRowRestoresARowNumber() throws Exception {
        // A live view takes an unbounded window only under an anchor, which resets it per day.
        assertSingleDepartureRestores(
                "row_number() OVER w s FROM base WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')",
                "1", "1", "2",
                "3"
        );
    }

    @Test
    public void testDepartedNewestRowRestoresACount() throws Exception {
        assertSingleDepartureRestores(
                "count(*) OVER w s FROM base WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')",
                "1", "1", "2",
                "3"
        );
    }

    @Test
    public void testDepartedNewestRowRestoresACumulativeSum() throws Exception {
        assertSingleDepartureRestores(
                "sum(x) OVER w s FROM base WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))",
                "0.5", "1.5", "4.0",
                "8.5"
        );
    }

    @Test
    public void testDepartedNewestRowRestoresADailyAnchor() throws Exception {
        assertSingleDepartureRestores(
                "sum(x) OVER w s FROM base WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')",
                "0.5", "1.5", "4.0",
                "8.5"
        );
    }

    @Test
    public void testDepartedNewestRowRestoresAMax() throws Exception {
        // The departed row held the key's maximum, which the root at the new frontier no longer
        // carries: the forward row after the restart is below it.
        assertSingleDepartureRestores(
                "max(x) OVER w s FROM base WHERE x > 0 WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')",
                "0.5", "1.5", "2.5",
                "4.5"
        );
    }

    @Test
    public void testDepartedNewestRowRestoresAProjectedWindowValue() throws Exception {
        // The view stores an expression over the window value, which the replay writes through
        // the projection and the seal at the new frontier freezes beneath it.
        assertSingleDepartureRestores(
                DEPARTURE_SUM + " * 2 s FROM base WHERE x > 0",
                "1.0", "4.0", "9.0",
                "17.0"
        );
    }

    @Test
    public void testDepartedNewestRowRestoresAtTheDefaultCadence() throws Exception {
        // The duration cadence seals every in-order commit here, as one row per seal does above:
        // the clock moves past it before each one.
        assertMemoryLeak(() -> assertDepartedNewestRowThenALateRowRestore("-2.5", "", true));
    }

    @Test
    public void testDepartedNewestRowRestoresRetainedRowsWhenUpsertedToNegative() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> assertDepartedNewestRowThenALateRowRestore("-2.5", "", false));
    }

    @Test
    public void testDepartedNewestRowRestoresRetainedRowsWhenUpsertedToNull() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> assertDepartedNewestRowThenALateRowRestore("NULL", "", false));
    }

    @Test
    public void testDepartedNewestRowRestoresRetainedRowsWhenUpsertedToZero() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> assertDepartedNewestRowThenALateRowRestore("0.0", "", false));
    }

    @Test
    public void testDepartedNewestRowRestoresRetainedRowsWithJitDisabled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_MODE, SqlJitMode.toString(SqlJitMode.JIT_MODE_DISABLED));
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> assertDepartedNewestRowThenALateRowRestore("-2.5", "", false));
    }

    @Test
    public void testDepartedNewestRowThenALateRowOfAnotherKeyRestoresRetainedRows() throws Exception {
        // The late row after the departure is another key's, and the root it resumes from is
        // the one the departure left at the frontier.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                commitDeparture(job, "('2026-01-01T00:01:45.000000Z', 'b', 7.0)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:01:45.000000Z -> 3
                        """);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                    2026-01-01T00:01:45.000000Z\tb\t7.0\t7.0
                    """;
            assertDepartureRows(rows);
            assertDepartureRestoresTwice(rows, "('2026-01-01T00:03:00.000000Z', 'b', 1.0)", """
                    2026-01-01T00:03:00.000000Z\tb\t1.0\t8.0
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowWithAnUnsealedRowBelowRestoresRetainedRows() throws Exception {
        // One commit sealed two rows under one root, at the newer of them. That one departs, so
        // the resume replays the older one, and its output ends below the root the splice kept:
        // one departure is enough, no late row needed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            assertSingleDepartureOfTheViewRestores("0.5", "2.0", "4.5", "8.5");
        });
    }

    @Test
    public void testDepartedNewestRowWithAnUnsealedRowBelowRestoresInNanoseconds() throws Exception {
        // The departed row sits one nanosecond above the row that becomes the frontier, so the
        // truncate must floor one nanosecond above it: a floor a microsecond up would keep the
        // root of the departed row.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP_NS", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000000Z', 'a', 2.5), ('2026-01-01T00:02:00.000000001Z', 'a', 4.5)");
                assertDepartureLadderNanos("""
                        2025-12-31T12:00:00.000000000Z -> 1
                        2026-01-01T00:01:00.000000000Z -> 2
                        2026-01-01T00:02:00.000000001Z -> 4
                        """);
                commitDeparture(job, "('2026-01-01T00:02:00.000000001Z', 'a', -4.5)");
                assertDepartureLadderNanos("""
                        2025-12-31T12:00:00.000000000Z -> 1
                        2026-01-01T00:01:00.000000000Z -> 2
                        2026-01-01T00:02:00.000000000Z -> 3
                        """);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2025-12-31T12:00:00.000000000Z\ta\t0.5\t0.5
                    2026-01-01T00:01:00.000000000Z\ta\t1.5\t2.0
                    2026-01-01T00:02:00.000000000Z\ta\t2.5\t4.5
                    """;
            assertDepartureRows(rows);
            assertDepartureRestoresTwice(rows, "('2026-01-01T00:03:00.000000000Z', 'a', 4.5)", """
                    2026-01-01T00:03:00.000000000Z\ta\t4.5\t8.5
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowsOfTwoCorrectionsRestoreRetainedRows() throws Exception {
        // The first departure leaves the root it removed the row of in place, re-versioned with
        // the row below it, because a root above it still holds a row. The second departure
        // removes that row: its resume anchors on the rowless root and replays nothing, so the
        // new frontier sits below the anchor itself, and the truncate drops the anchor too.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 4.5)");
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 8.5)");
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -4.5)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:03:00.000000Z -> 3
                        2026-01-01T00:04:00.000000Z -> 4
                        """);
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                final long generationBefore = departureGeneration();
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -8.5)");
                assertDepartureLadder(SINGLE_DEPARTURE_LADDER);
                Assert.assertEquals(generationBefore + 3, departureGeneration());
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            final String rows = singleDepartureRows("0.5", "2.0", "4.5");
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            assertDepartureRestoresTwice(rows, SINGLE_DEPARTURE_FORWARD_ROW, """
                    2026-01-01T00:05:00.000000Z\ta\t4.5\t8.5
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowsPastAChainBudgetOfOneRestore() throws Exception {
        // A chain budget of one boundary declines the first departure's chain, which crosses two,
        // and the truncate it falls back to keeps the ladder below its anchor. The second
        // departure crosses one boundary, chains, and its output ends below the root it kept.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitTheDayTheBaseLoses(job);
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 4.5)");
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', 8.5)");
                commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -4.5)");
                assertDepartureLadder("""
                        2025-12-31T12:00:00.000000Z -> 1
                        2026-01-01T00:01:00.000000Z -> 2
                        2026-01-01T00:04:00.000000Z -> 4
                        """);
                commitDeparture(job, "('2026-01-01T00:04:00.000000Z', 'a', -8.5)");
                assertDepartureLadder(SINGLE_DEPARTURE_LADDER);
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            final String rows = singleDepartureRows("0.5", "2.0", "4.5");
            assertDepartureRows(rows);
            assertDepartureRestoresTwice(rows, SINGLE_DEPARTURE_FORWARD_ROW, """
                    2026-01-01T00:05:00.000000Z\ta\t4.5\t8.5
                    """);
        });
    }

    @Test
    public void testDepartedNewestRowWithTheChainDeclinedAndNoRowToReplayRetiresTheTimeline() throws Exception {
        // With the chain declined, the resume truncates at its anchor before the commit, and its
        // replay emits no row: the newest row departed with nothing between it and the anchor.
        // No seal closes the truncated prefix, so the timeline retires, as every such resume did
        // before the chain, and a restart rebuilds the view from its base, which lost nothing.
        // No splice kept a root above the frontier, so nothing is truncated after the commit.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
                assertDepartureLadder("""
                        2026-01-01T00:01:00.000000Z -> 1
                        2026-01-01T00:02:00.000000Z -> 2
                        """);
                final LiveViewInstance instance = departureView();
                final long resetsBefore = instance.getCheckpointTimelineResets();
                commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', -2.5)");
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals(
                        "the truncated prefix goes with the seal it never got",
                        resetsBefore + 1,
                        instance.getCheckpointTimelineResets()
                );
                try (Path dir = departureCheckpointsDir(); Path timelinePath = new Path()) {
                    LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
                    Assert.assertFalse(configuration.getFilesFacade().exists(timelinePath.$()));
                    Assert.assertFalse(LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                }
            }
            final String rows = """
                    ts\tsym\tx\ts
                    2026-01-01T00:01:00.000000Z\ta\t1.5\t1.5
                    """;
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            restartLiveViewsAndRecover();
            assertRebuiltFromAppliedBase("lv");
            Assert.assertFalse(departureView().isCheckpointRecoveryBlocked());
            assertDepartureRows(rows);
            assertDepartureMatchesRecompute();
        });
    }

    @Test
    public void testDepartedNewestRowWithTheChainDeclinedRestores() throws Exception {
        // With the chain declined, the resume truncates at its anchor before the commit and
        // seals the new frontier, as every resume did before the chain: nothing above the
        // frontier survives, so there is nothing left to truncate after the commit.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitSingleDeparture(job);
                Assert.assertEquals(0, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            }
            final String rows = singleDepartureRows("0.5", "2.0", "4.5");
            assertDepartureRows(rows);
            assertDepartureRestoresTwice(rows, SINGLE_DEPARTURE_FORWARD_ROW, """
                    2026-01-01T00:05:00.000000Z\ta\t4.5\t8.5
                    """);
        });
    }

    @Test
    public void testDepartureThenABaseColumnAddedRestoresTheRuntimeInPlace() throws Exception {
        // An unreferenced ADD COLUMN between the departure and the next late row drifts the
        // compiled plan, and the drift recovery restores the runtime from the timeline the
        // departure left. A root above the frontier would fail that restore's row count and
        // rebuild the view from a base that lost a day of its rows.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(() -> {
                createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveSeedToCompletion(job, "lv");
                    commitSingleDeparture(job);
                    execute("ALTER TABLE base ADD COLUMN extra INT");
                    drainWalQueue();
                    commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 3.0, NULL)");
                    assertDepartureLadder("""
                            2025-12-31T12:00:00.000000Z -> 1
                            2026-01-01T00:01:00.000000Z -> 2
                            2026-01-01T00:02:00.000000Z -> 4
                            """);
                }
                final LiveViewInstance instance = departureView();
                capture.drain();
                capture.assertLogged("live view restored its runtime from the checkpoint timeline [view=lv, cause=base table metadata change");
                Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals("the restore must not fall back to a rebuild", 0, instance.getCheckpointRebuildAttempts());
                Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                final String rows = """
                        ts\tsym\tx\ts
                        2025-12-31T12:00:00.000000Z\ta\t0.5\t0.5
                        2026-01-01T00:01:00.000000Z\ta\t1.5\t2.0
                        2026-01-01T00:01:30.000000Z\ta\t3.0\t5.0
                        2026-01-01T00:02:00.000000Z\ta\t2.5\t7.0
                        """;
                assertDepartureRows(rows);
                assertDepartureRestoresTwice(rows, "('2026-01-01T00:05:00.000000Z', 'a', 4.5, NULL)", """
                        2026-01-01T00:05:00.000000Z\ta\t4.5\t10.0
                        """);
            });
        } finally {
            capture.stop();
        }
    }

    @Test
    public void testDepartureThenABaseTruncateRestores() throws Exception {
        // TRUNCATE is base data removal, which the view walks past keeping its rows. A restart
        // after it must restore them from the timeline: the base it would rebuild from is empty.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitSingleDeparture(job);
                execute("TRUNCATE TABLE base");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            final String rows = singleDepartureRows("0.5", "2.0", "4.5");
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            assertDepartureRestoresTwice(rows, SINGLE_DEPARTURE_FORWARD_ROW, """
                    2026-01-01T00:05:00.000000Z\ta\t4.5\t8.5
                    """);
        });
    }

    @Test
    public void testDepartureThenAMidDrainFaultRestoresTheRuntimeInPlace() throws Exception {
        // The forward drain after the departure takes two commits in one pass and fails on the
        // second, with the first one's row already in the accumulators, and the recovery
        // restores the runtime in place from the timeline the departure left.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault("ts", "x", "sym");
        assertMemoryLeak(fault.facade(), () -> {
            createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", "");
            fault.of(engine.verifyTableName("base").getDirName());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitSingleDeparture(job);
                // A row the filter rejects, then two provably clean commits. The first goes in
                // on its own turn, with no row to seal; the next two coalesce behind it and drain
                // through the raw WAL in one pass, which puts the failure after a row that pass
                // has already fed, above the root the departure left at the frontier.
                execute("INSERT INTO base VALUES ('2026-01-01T00:03:30.000000Z', 'a', -1.0)");
                drainWalQueue();
                execute("INSERT INTO base VALUES ('2026-01-01T00:04:00.000000Z', 'a', 8.0)");
                execute("INSERT INTO base VALUES ('2026-01-01T00:05:00.000000Z', 'a', 4.5)");
                drainWalQueue();
                runOneDeparturePass(job);
                assertDepartureRows(singleDepartureRows("0.5", "2.0", "4.5"));
                assertDepartureLadder(SINGLE_DEPARTURE_LADDER);
                fault.arm(1);
                runOneDeparturePass(job);
                Assert.assertTrue("the drain must have met the fault", fault.hasFired());
                driveRefreshToQuiescence(job);
            }
            final LiveViewInstance instance = departureView();
            Assert.assertEquals(1, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals("the restore must not fall back to a rebuild", 0, instance.getCheckpointRebuildAttempts());
            Assert.assertEquals("the fault is the one fault", 1, instance.getRefreshFaultCount());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            final String rows = singleDepartureRows("0.5", "2.0", "4.5") + """
                    2026-01-01T00:04:00.000000Z\ta\t8.0\t12.0
                    2026-01-01T00:05:00.000000Z\ta\t4.5\t15.0
                    """;
            assertDepartureRows(rows);
            assertDepartureRestoresTwice(rows, "('2026-01-01T00:06:00.000000Z', 'a', 1.0)", """
                    2026-01-01T00:06:00.000000Z\ta\t1.0\t13.5
                    """);
        });
    }

    @Test
    public void testFilteredLowestRowReplacementIsDeleted() throws Exception {
        // A below-frontier dedup replacement flips the LOWEST result row's value so it
        // fails the WHERE filter; the row must be DELETED (the recompute over the
        // applied base no longer has it). Regression for the o3HeadMissReplay boundary:
        // the rebuild's first row is above the filtered ts, so replayMinTs jumps past
        // it and a replayMinTs-only REPLACE_RANGE would leave the stale row frozen.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT sym, val, ts, " +
                    "first_value(val) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS fv " +
                    "FROM base WHERE val > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z'), " +
                        "('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Replace the lowest row ts=01: val 10 -> -5. It now fails WHERE val>0,
                // so the recompute drops it and the view must too.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', -5, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, fv FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\tfv\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\t20\n" +
                            "a\t30\t2026-01-01T00:00:03.000000Z\t20\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFrontierTimestampReplacementReflectedNoDuplicate() throws Exception {
        // A dedup UPSERT replaces the most-recent row at exactly the frontier ts. The
        // raw-WAL cross-commit trigger (txnMinTs < latestSeen, strict) would miss this
        // equality case and append a duplicate; the applied-reader path routes it to a
        // replay that rewrites the row in place. Assert the replaced value with no
        // duplicate row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z'), " +
                        "('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 99, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t99\t2026-01-01T00:00:03.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFusedSymbolJoinMatchesOrdinaryJoinAfterLeadTextReplayedAway() throws Exception {
        // An upsert replays away a lead-only text, so the view's symbol cache keeps the id it
        // interned for that text, and the next new text takes the id after it while the view's
        // table commits that text at the id before it. The in-memory tier then holds that text
        // under two keys, and its rows carry the key that keyOf() does not return. A fused
        // SYMBOL-key join that probes the view must still match those rows by text, as the
        // ordinary join does.
        assertMemoryLeak(() -> {
            replayLeadTextAway();
            // dim holds one text against the view's eleven keys, the shape that fills the
            // translation cache from the build side, were the view's dictionary eligible. The
            // queries return no SYMBOL column: the view's keyOf() gives y's other key, which the
            // query assertion's symbol table checks would report on their own.
            execute("CREATE TABLE dim (sym SYMBOL, w INT)");
            execute("INSERT INTO dim VALUES ('y', 1)");
            final String innerSql = "SELECT d.w, count(*) n, sum(l.val) s FROM lv l JOIN dim d ON l.sym = d.sym ORDER BY d.w";
            final String innerExpected = """
                    w\tn\ts
                    1\t1\t4
                    """;
            final String leftSql = "SELECT l.val, count(d.w) m, count(*) n FROM lv l LEFT JOIN dim d ON l.sym = d.sym ORDER BY l.val";
            final String leftExpected = """
                    val\tm\tn
                    1\t0\t9
                    3\t0\t1
                    4\t1\t1
                    """;
            for (boolean isFused : new boolean[]{false, true}) {
                try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)
                        .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
                    context.setParallelGroupByEnabled(true);
                    context.setParallelHashJoinGroupByEnabled(isFused);
                    for (String[] query : new String[][]{{innerSql, innerExpected}, {leftSql, leftExpected}}) {
                        final QueryAssertion assertion = assertQuery(query[0])
                                .withContext(context)
                                .noLeakCheck()
                                .expectSize();
                        if (isFused) {
                            // The view probes, through the in-memory tier.
                            assertion.withPlanContaining("Async Hash Join Group By", "symbolKeyJoin: true", "inMemory: true");
                        } else {
                            assertion.withPlanNotContaining("Hash Join Group By");
                        }
                        assertion.returns(query[1]);
                    }
                }
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFusedSymbolJoinMatchesOrdinaryJoinWhenViewBuildsAfterLeadTextReplayedAway() throws Exception {
        // The view's in-memory tier holds y under two keys, see replayLeadTextAway(), and its y
        // row carries the key that keyOf() does not return. A fused SYMBOL-key join that builds
        // the view would keep that key and look y up through keyOf(), so it would miss the row.
        // The planner keeps the ordinary join for such a build, which matches the view's keys by
        // their texts. A LEFT join builds its right input, and an INNER join builds the input
        // with fewer table rows: dim2 holds more rows than the view's table, so the fused plan
        // would swap the inputs of lv JOIN dim2 to build the view.
        assertMemoryLeak(() -> {
            replayLeadTextAway();
            execute("CREATE TABLE dim (sym SYMBOL, v INT, w INT)");
            execute("INSERT INTO dim VALUES ('y', 4, 1)");
            execute("CREATE TABLE dim2 (sym SYMBOL, w INT)");
            execute("""
                    INSERT INTO dim2
                    SELECT CASE WHEN x = 1 THEN 'y' ELSE 'z' || x END, CASE WHEN x = 1 THEN 1 ELSE 2 END
                    FROM long_sequence(30)
                    """);
            final String matchOnly = """
                    w\tm\ts
                    1\t1\t4
                    """;
            final String matchAndMiss = """
                    w\tm\ts
                    1\t1\t4
                    2\t0\tnull
                    """;
            final String[][] symbolKeyQueries = {
                    {"SELECT d.w, count(l.val) m, sum(l.val) s FROM dim d LEFT JOIN lv l ON d.sym = l.sym ORDER BY d.w", matchOnly},
                    // A composite key stages the SYMBOL pair through the key sinks instead.
                    {"SELECT d.w, count(l.val) m, sum(l.val) s FROM dim d LEFT JOIN lv l ON d.sym = l.sym AND d.v = l.val ORDER BY d.w", matchOnly},
                    // A filter over the view stays the build's input, with the view beneath it.
                    {"SELECT d.w, count(l.val) m, sum(l.val) s FROM dim d LEFT JOIN (SELECT sym, val FROM lv WHERE val > 0) l ON d.sym = l.sym ORDER BY d.w", matchOnly},
                    {"SELECT d.w, count(l.val) m, sum(l.val) s FROM lv l JOIN dim2 d ON l.sym = d.sym ORDER BY d.w", matchOnly},
                    {"SELECT d.w, count(l.val) m, sum(l.val) s FROM dim2 d LEFT JOIN lv l ON d.sym = l.sym ORDER BY d.w", matchAndMiss},
                    {"SELECT d.w, count(l.val) m, sum(l.val) s FROM dim2 d JOIN lv l ON d.sym = l.sym ORDER BY d.w", matchOnly}
            };
            // An INT key compares no dictionary keys, so the fused plan still builds the view.
            final String intKeySql = "SELECT d.w, count(l.val) m, sum(l.val) s FROM dim d LEFT JOIN lv l ON d.w = l.val ORDER BY d.w";
            final String intKeyExpected = """
                    w\tm\ts
                    1\t9\t9
                    """;
            for (boolean isFused : new boolean[]{false, true}) {
                for (int workerCount : new int[]{1, 4}) {
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, workerCount)
                            .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
                        context.setParallelGroupByEnabled(true);
                        context.setParallelHashJoinGroupByEnabled(isFused);
                        for (String[] query : symbolKeyQueries) {
                            // The view builds through the in-memory tier, and the join stays ordinary.
                            assertQuery(query[0])
                                    .withContext(context)
                                    .noLeakCheck()
                                    .expectSize()
                                    .withPlanContaining("LiveView", "inMemory: true")
                                    .withPlanNotContaining("Hash Join Group By")
                                    .returns(query[1]);
                        }
                        final QueryAssertion assertion = assertQuery(intKeySql)
                                .withContext(context)
                                .noLeakCheck()
                                .expectSize();
                        if (isFused) {
                            assertion.withPlanContaining("Async Hash Join Group By", "Build\n            LiveView", "inMemory: true");
                        } else {
                            assertion.withPlanNotContaining("Hash Join Group By");
                        }
                        assertion.returns(intKeyExpected);
                    }
                }
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testIntraCommitEqualTimestampCollapse() throws Exception {
        // A single commit carries two rows with identical (ts, keys) at a brand-new ts
        // strictly above the frontier. The raw-WAL path would append both (equal ts
        // does not set the intra-commit out-of-order flag); the applied-reader path
        // sees the collapsed single row. One output row with the last value proves the
        // forward path reads the post-dedup base, not raw WAL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 30, '2026-01-01T00:00:03.000000Z'), " +
                        "('a', 40, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t40\t2026-01-01T00:00:03.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRealDedupFallsBackToAppliedReplay() throws Exception {
        // Phase 2a routing discriminator. A forward (additive) commit over a warm signal
        // takes the cheap raw-WAL path; the next commit is a real dedup replacement, which
        // advances the divergence watermark so the gate fails and that cycle falls back to
        // the applied-reader replay. Assert the clean-cycle counter advances on the forward
        // commit but NOT on the dedup commit, and that the replaced value is reflected.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: initial rows (the first cycle warms the signal).
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Batch 2: a forward row strictly above the frontier, no dedup. The signal
                // is warm and the range is clean -> cheap raw-WAL append.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                long cleanAfterForward = instance.getDedupRawWalCleanCycles();
                Assert.assertTrue(
                        "forward additive commit must take the cheap raw-WAL path",
                        cleanAfterForward > 0
                );

                // Batch 3: a dedup replacement at existing ts=02 (val 20 -> 99). The batch
                // dedups, so the divergence watermark advances past the range's lower bound
                // and the gate fails -> fall back to the applied-reader replay. The clean-
                // cycle counter must not move.
                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 99, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "dedup commit must fall back to the applied-reader path, not raw-WAL",
                        cleanAfterForward,
                        instance.getDedupRawWalCleanCycles()
                );
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t99\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t30\t2026-01-01T00:00:03.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRebuildOverTtlEvictedDedupBase() throws Exception {
        // A dedup base denies the raw-WAL insert-only proof, and a TTL base evicts
        // whole partitions at apply time. When the checkpoint gap holds both a
        // below-frontier dedup replacement and a TTL eviction, the restart-restore
        // rebuild incorporates the gap from the applied base as it now stands: the
        // replacement's value (in a surviving partition) is reflected post-dedup, and
        // the surviving region matches a complete recomputation of the view SELECT
        // over the surviving base rows. Rows the view already derived from the
        // TTL-evicted partition are retained - eviction retires settled base data
        // below the view's floor and never retracts derived rows, restart or not.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) " +
                    "TIMESTAMP(ts) PARTITION BY DAY TTL 1 DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '9' MINUTE PRECEDING AND CURRENT ROW) AS v FROM base WHERE i > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Day-1 and day-2 rows, applied and refreshed. The pinned clock sits
                // far below the data max, so TTL's min(maxTimestamp, wallClock)
                // reference keeps every partition alive.
                execute("INSERT INTO base (ts, sym, i) VALUES " +
                        "('2026-01-01T00:01:00.000000Z', 'a', 100), " +
                        "('2026-01-02T00:01:00.000000Z', 'a', 10), " +
                        "('2026-01-02T00:05:00.000000Z', 'a', 20)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // A below-frontier dedup replacement (10 -> 555), refreshed, so the
                // head checkpoint covers a correction that was never provably
                // insert-only.
                setCurrentMicros(currentMicros + 2_000_000L);
                execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-02T00:01:00.000000Z', 'a', 555)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // A zero-row cycle that advances the applied watermark PAST the head
                // checkpoint, so a later restart takes the checkpoint-lags-applied
                // branch: a filtered-out row (i <= 0) in an old partition the low
                // clock keeps TTL away from.
                setCurrentMicros(currentMicros + 2_000_000L);
                execute("INSERT INTO base (ts, sym, i) VALUES ('2025-12-31T00:00:00.000000Z', 'z', -1)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // The gap, applied to the base but never refreshed into the LV, so the
                // restart-restore is what must incorporate it:
                // - an intra-commit equal-ts collapse at day-2 00:07: the base keeps
                //   the last row (999), and with no out-of-order commit behind it,
                //   only the post-dedup applied base names the survivor - a raw
                //   pre-dedup replay would derive both rows;
                // - a day-3 row whose apply advances the TTL reference and evicts the
                //   day-1 (and 2025-12-31) partitions.
                setCurrentMicros(currentMicros + 2_000_000L);
                execute("INSERT INTO base (ts, sym, i) VALUES " +
                        "('2026-01-02T00:07:00.000000Z', 'a', 30), " +
                        "('2026-01-02T00:07:00.000000Z', 'a', 999)");
                drainWalQueue();
                setCurrentMicros(MicrosTimestampDriver.floor("2026-01-05T00:00:00.000000Z"));
                execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-03T00:01:00.000000Z', 'a', 400)");
                drainWalQueue();
            }

            // TTL kept only day-2 and day-3, and the collapse kept one 00:07 row.
            assertQuery("SELECT ts, sym, i FROM base ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                    ts\tsym\ti
                    2026-01-02T00:01:00.000000Z\ta\t555
                    2026-01-02T00:05:00.000000Z\ta\t20
                    2026-01-02T00:07:00.000000Z\ta\t999
                    2026-01-03T00:01:00.000000Z\ta\t400
                    """);

            // Restart: drop the in-memory registry and rebuild from on-disk state, then
            // refresh. The rebuild recomputes the whole view from the applied base.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertNoRefreshFaults("lv");

            // The view retains the day-1 row it derived before the eviction, reflects
            // the below-frontier replacement post-dedup (555, not 10, no duplicate),
            // derives exactly one row from the collapsed 00:07 commit (999, never 30),
            // and appends the day-3 row.
            assertQuery("SELECT ts, sym, i, v FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("""
                    ts\tsym\ti\tv
                    2026-01-01T00:01:00.000000Z\ta\t100\t100.0
                    2026-01-02T00:01:00.000000Z\ta\t555\t555.0
                    2026-01-02T00:05:00.000000Z\ta\t20\t575.0
                    2026-01-02T00:07:00.000000Z\ta\t999\t1574.0
                    2026-01-03T00:01:00.000000Z\ta\t400\t400.0
                    """);
            // The surviving region equals a complete recomputation of the view SELECT
            // over the base as it now stands (the 9-minute frame never crosses a day
            // boundary, so the retained day-1 row influences no surviving value).
            assertQuery("SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '9' MINUTE PRECEDING AND CURRENT ROW) AS v FROM base WHERE i > 0")
                    .noLeakCheck().timestamp("ts").noRandomAccess().returns("""
                            ts\tsym\ti\tv
                            2026-01-02T00:01:00.000000Z\ta\t555\t555.0
                            2026-01-02T00:05:00.000000Z\ta\t20\t575.0
                            2026-01-02T00:07:00.000000Z\ta\t999\t1574.0
                            2026-01-03T00:01:00.000000Z\ta\t400\t400.0
                            """);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRebuildPurgesBelowFrontierDedupReplacement() throws Exception {
        // A below-frontier dedup replacement that pushes a row out of the view's WHERE
        // filter, incorporated by the restart-restore rebuild (not the incremental drain).
        // The rebuild recomputes the whole view from the applied base, so it must delete
        // the stale pre-replacement LV row even though the recompute's lowest surviving
        // row sits above it. Regression for LiveViewFuzzTest#testFuzzDedup
        // (seed 661975787194049L/1784178587678L, RANGE_SUM variant).
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '9' MINUTE PRECEDING AND CURRENT ROW) AS v FROM base WHERE i > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Emit ts=00:01 (i=297) below the frontier, plus higher-ts rows. The refresh
                // seals a head checkpoint at this applied point.
                execute("INSERT INTO base (ts, sym, i) VALUES " +
                        "('2026-01-01T00:01:00.000000Z', 'a', 297), " +
                        "('2026-01-01T00:05:00.000000Z', 'a', 500), " +
                        "('2026-01-01T00:09:00.000000Z', 'a', 900)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // A zero-row cycle that advances the applied watermark PAST the head
                // checkpoint, so a later restart takes the checkpoint-lags-applied rebuild
                // branch: insert a future band and drop it (the LV never emits it).
                setCurrentMicros(currentMicros + 2_000_000L);
                execute("INSERT INTO base (ts, sym, i) VALUES ('2030-01-01T00:00:00.000000Z', 'z', 1)");
                drainWalQueue();
                execute("ALTER TABLE base DROP PARTITION LIST '2030-01-01'");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // Below-frontier dedup replacement of ts=00:01: i 297 -> -108, which now
                // fails WHERE i > 0. Apply it to the base but DON'T refresh the LV, so the
                // restart-restore rebuild is what must incorporate it.
                setCurrentMicros(currentMicros + 2_000_000L);
                execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:01:00.000000Z', 'a', -108)");
                drainWalQueue();
            }

            // Restart: drop the in-memory registry and rebuild from on-disk state, then
            // refresh. The restore rebuilds the whole view from the applied base (which now
            // holds i=-108 at 00:01) and must purge the stale i=297 row.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, sym, i FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\ti\n" +
                            "2026-01-01T00:05:00.000000Z\ta\t500\n" +
                            "2026-01-01T00:09:00.000000Z\ta\t900\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartWithDedupCollapseInCheckpointGap() throws Exception {
        // The head checkpoint is written on a cadence, so it can lag the applied point. When
        // the gap holds an intra-commit equal-ts collapse (Gap B), a raw-WAL
        // replay-to-applied would advance the restored accumulators over BOTH pre-dedup
        // rows, diverging from the post-dedup disk. The dedup restart path must instead
        // rebuild from the applied base. The discriminating assertion is the cumulative
        // sum of a row appended AFTER restart: 120 if reconciled correctly, 150 if the
        // accumulators drifted over the pre-dedup stream.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS SELECT sym, val, ts, " +
                    "sum(val) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS cum FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1 -> applied + first head checkpoint (at the applied point).
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10.0, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20.0, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Batch 2 (the gap): a single commit with two equal-ts rows above the
                // frontier collapses to one (val 40). Applied advances; the checkpoint does
                // not (neither row nor duration cadence is met), so head < applied.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 30.0, '2026-01-01T00:00:03.000000Z'), " +
                        "('a', 40.0, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertTrue(
                    "test must create a checkpoint-cadence gap (head < applied)",
                    instance.getHeadCheckpointLvSeqTxn() < instance.getAppliedWatermark()
            );

            // Simulated restart: drop the in-memory registry and rebuild it from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(restored);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Restore closes the checkpoint-to-applied gap over the applied base, then a
                // forward row at ts=04 appends. cum(04) = 10 + 20 + 40 + 50 = 120.
                setCurrentMicros(1_000_000L);
                drainJob(job); // restore-from-head (dedup gap) rebuild
                drainWalQueue();
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 50.0, '2026-01-01T00:00:04.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            assertQuery("SELECT sym, val, ts, cum FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\tcum\n" +
                            "a\t10.0\t2026-01-01T00:00:01.000000Z\t10.0\n" +
                            "a\t20.0\t2026-01-01T00:00:02.000000Z\t30.0\n" +
                            "a\t40.0\t2026-01-01T00:00:03.000000Z\t70.0\n" +
                            "a\t50.0\t2026-01-01T00:00:04.000000Z\t120.0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSymbolOutputServedOverDedupBase() throws Exception {
        // A SYMBOL output column over a dedup base still refreshes correctly: the
        // coupled forward append eager-interns symbols into the tier's LV-space id
        // set, and a frontier replacement collapses in place.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, tag SYMBOL, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m START FROM NOW AS " +
                    "SELECT sym, tag, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, tag, ts) VALUES " +
                        "('a', 'x', '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 'y', '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Replace the frontier row's tag: 'y' -> 'z'.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, tag, ts) VALUES ('a', 'z', '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, tag, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\ttag\tts\trn\n" +
                            "a\tx\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\tz\t2026-01-01T00:00:02.000000Z\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testTransientBaseMetadataFaultKeepsTheDedupRoute() throws Exception {
        // isDedupBase decides the routing for a whole refresh cycle, so a metadata read that
        // FAILS must not be answered "not a dedup base". A wrong "no" routes the cycle down the
        // raw-WAL path, whose cross-commit trigger (txnMinTs < latestSeen, strict) misses an
        // equality-at-the-frontier replacement - the exact case
        // testFrontierTimestampReplacementReflectedNoDuplicate pins - so the replacement lands as
        // an additive duplicate row and gets committed with recordRefreshSuccess().
        //
        // The fault is one-shot and lands on the base's _meta open that isDedupBase makes:
        // releaseAllReaders() empties the metadata pool, so the borrow rebuilds the tenant from
        // the file rather than reusing a pooled one. That models the transient metadata read
        // failures TableReaderMetadataTenantImpl raises over a perfectly healthy base
        // ("Transaction read timeout" / "Metadata read timeout" under metadata churn), which is
        // why the following cycle - reading the same healthy base - then succeeds.
        //
        // errno 13 (EACCES) is deliberately not one of Files.isErrnoFileCannotRead()'s errnos:
        // one of those would make TableUtils.handleMetadataLoadException spin to the deadline
        // instead of throwing, and this suite runs on a simulated-year spin timeout.
        final AtomicBoolean metaOpenFaultArmed = new AtomicBoolean();
        final AtomicBoolean metaOpenFaultServed = new AtomicBoolean();
        final AtomicReference<String> baseMetaPathTail = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return metaOpenFaultServed.compareAndSet(true, false) ? 13 : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                return serveFault(name) ? -1 : super.openRO(name);
            }

            @Override
            public long openRONoCache(LPSZ name) {
                return serveFault(name) ? -1 : super.openRONoCache(name);
            }

            private boolean serveFault(LPSZ name) {
                final String tail = baseMetaPathTail.get();
                if (tail != null && metaOpenFaultArmed.get() && Utf8s.endsWithAscii(name, tail)) {
                    metaOpenFaultArmed.set(false);
                    metaOpenFaultServed.set(true);
                    return true;
                }
                return false;
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z'), " +
                        "('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                final TableToken baseToken = instance.getDefinition().getBaseTableToken();

                // The dedup UPSERT replaces the row sitting at exactly the frontier ts.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 99, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();

                baseMetaPathTail.set(baseToken.getDirName() + Files.SEPARATOR + TableUtils.META_FILE_NAME);
                engine.releaseAllReaders();
                metaOpenFaultArmed.set(true);
                drainJob(job);
                Assert.assertFalse(
                        "the one-shot base metadata fault was never served, so this case tests nothing",
                        metaOpenFaultArmed.get()
                );

                // Fault gone. The retry reads the same healthy base and must converge on it.
                driveRefreshToQuiescence(job);
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t99\t2026-01-01T00:00:03.000000Z\t3\n");
            Assert.assertTrue(
                    "an unreadable base metadata must reach handleRefreshFailure, not be answered"
                            + " \"not a dedup base\" and route the cycle down the raw-WAL path",
                    instance.getRefreshFaultCount() > 0
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testTruncateInRangeFallsBackToAppliedReader() throws Exception {
        // A data-shaped non-DATA op (TRUNCATE) diverges the applied base from the raw WAL,
        // so its seqTxn advances the divergence watermark and any range covering it fails
        // the clean gate -> applied-reader fallback. Warm the signal with forward commits
        // (clean-cycle counter grows), then TRUNCATE and assert the counter does not move
        // across that cycle, and that the derived prefix stays frozen (no history rewrite).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Forward row above the frontier: clean cycle, cheap raw-WAL append.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                long cleanBeforeTruncate = instance.getDedupRawWalCleanCycles();
                Assert.assertTrue(
                        "forward commit must take the cheap raw-WAL path",
                        cleanBeforeTruncate > 0
                );

                // TRUNCATE removes applied history the raw append would keep -> divergence.
                setCurrentMicros(4_000_000L);
                execute("TRUNCATE TABLE base");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "TRUNCATE must fall back to the applied-reader path, not raw-WAL",
                        cleanBeforeTruncate,
                        instance.getDedupRawWalCleanCycles()
                );
            }
            assertQuery("SELECT sym, val, ts FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    private static Path departureCheckpointsDir() {
        return new Path().of(configuration.getDbRoot())
                .concat(departureView().getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static LiveViewInstance departureView() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    // The rows the single-departure shape leaves the view holding, with the window values the
    // view's own function gives them.
    private static String singleDepartureRows(String s1, String s2, String s3) {
        return "ts\tsym\tx\ts\n"
                + "2025-12-31T12:00:00.000000Z\ta\t0.5\t" + s1 + "\n"
                + "2026-01-01T00:01:00.000000Z\ta\t1.5\t" + s2 + "\n"
                + "2026-01-01T00:02:00.000000Z\ta\t2.5\t" + s3 + "\n";
    }

    private void assertHeadMissDepartureMatchesRecompute() throws Exception {
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

    private void assertDepartureLadder(String expected) {
        assertCheckpointLadder(departureView(), false, expected);
    }

    /**
     * The minute shape over the lost day: the three newest rows go in one upsert, the oldest of
     * them to a new value and the two others to {@code departedValue}, which the filter rejects.
     * The rebuild splices [00:57, +inf) and emits the kept row, below the root the cadence sealed
     * at 00:59: the repair truncates at the new frontier 00:57 and seals a root there, and each
     * restart restores from it, over the day a rebuild would drop.
     */
    private void assertNewestRowsLeavingTheFilterRestore(String departedValue) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView("TIMESTAMP");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitMinuteBatchesOverTheLostDay(job);
                final long generationBefore = departureGeneration();
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                commitDeparture(
                        job,
                        "('2026-01-01T00:57:00.000000Z', 'a', 5.0), ('2026-01-01T00:58:00.000000Z', 'a', " + departedValue
                                + "), ('2026-01-01T00:59:00.000000Z', 'a', " + departedValue + ")"
                );
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the splice, the truncate and the seal", generationBefore + 3, departureGeneration());
                assertDepartureLadder("""
                        2025-12-31T23:55:00.000000Z -> 1
                        2026-01-01T00:57:00.000000Z -> 59
                        """);
            }
            final String rows = headMissDepartureRows(true, 56) + "2026-01-01T00:57:00.000000Z\ta\t5.0\t6.0\n";
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartureRestored(rows);
            }
        });
    }

    /**
     * The newest two rows sit one step apart at the top of the designated timestamp range, the
     * newer of them at its largest value, above a row a minute from 23:00 to 23:58 and a root a
     * day below. One upsert gives the older of the two a new value and moves the newer out of
     * the filter. The rebuild's output ends one step below the root the cadence sealed at the
     * largest timestamp, so the repair truncates the timeline at that timestamp and seals a root
     * one step below it, which each restart restores from.
     *
     * @param hourPrefix   the last hour of the range, up to its minute
     * @param minuteSuffix what follows the minute of a row at a whole minute
     */
    private void assertRowAtTheLargestTimestampLeavingTheFilterRestores(
            String timestampType,
            String dayBelowTs,
            String hourPrefix,
            String minuteSuffix,
            String frontierTs,
            String largestTs
    ) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createHeadMissDepartureView(timestampType);
            final boolean isNanos = "TIMESTAMP_NS".equals(timestampType);
            final StringBuilder firstBatch = new StringBuilder();
            final StringBuilder secondBatch = new StringBuilder();
            final StringBuilder minuteRows = new StringBuilder();
            for (int minute = 0; minute <= 58; minute++) {
                final String ts = hourPrefix + (minute < 10 ? "0" : "") + minute + minuteSuffix;
                final StringBuilder batch = minute <= 30 ? firstBatch : secondBatch;
                batch.append(batch.length() > 0 ? ", " : "").append("('").append(ts).append("', 'a', 1.0)");
                minuteRows.append(ts).append("\ta\t1.0\t").append(minute == 0 ? "1.0" : "2.0").append('\n');
            }
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitDeparture(job, "('" + dayBelowTs + "', 'a', 0.5)");
                commitDeparture(job, firstBatch.toString());
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                commitDeparture(job, secondBatch + ", ('" + frontierTs + "', 'a', 1.0), ('" + largestTs + "', 'a', 1.0)");
                assertCheckpointLadder(departureView(), isNanos, dayBelowTs + " -> 1\n" + largestTs + " -> 62\n");
                final long generationBefore = departureGeneration();
                final long headMissRepairsBefore = countHeadMissRepairs(job);
                commitDeparture(job, "('" + frontierTs + "', 'a', 5.0), ('" + largestTs + "', 'a', -1.0)");
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT));
                Assert.assertEquals("the splice, the truncate and the seal", generationBefore + 3, departureGeneration());
                assertCheckpointLadder(departureView(), isNanos, dayBelowTs + " -> 1\n" + frontierTs + " -> 61\n");
            }
            // The row a day below sits outside every one-minute frame above it, and the kept row's
            // frame holds itself alone: 23:58 sits more than a minute below it.
            final String rows = "ts\tsym\tx\ts\n"
                    + dayBelowTs + "\ta\t0.5\t0.5\n"
                    + minuteRows
                    + frontierTs + "\ta\t5.0\t5.0\n";
            assertDepartureRows(rows);
            assertNoRefreshFaults("lv");
            for (int restart = 0; restart < 2; restart++) {
                restartLiveViewsAndRecover();
                assertDepartureRestored(rows);
            }
        });
    }

    private void assertDepartureLadderNanos(String expected) {
        assertCheckpointLadder(departureView(), true, expected);
    }

    private void assertDepartureMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "SELECT ts, sym, x, " + DEPARTURE_SUM + " s FROM base WHERE x > 0 ORDER BY ts, sym",
                "(lv) ORDER BY ts, sym",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    /**
     * After a restart: the view came back on its timeline, active, holding {@code expectedRows}.
     */
    private void assertDepartureRestored(String expectedRows) throws Exception {
        assertRestoredFromTimeline("lv");
        Assert.assertFalse(departureView().isCheckpointRecoveryBlocked());
        assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\nactive\n");
        assertDepartureRows(expectedRows);
        assertNoRefreshFaults("lv");
    }

    /**
     * Restarts twice, and asserts each restart restores the view from its timeline with its
     * rows. Between the two, {@code forwardRow} folds onto the state the restore put back, and
     * the view must add {@code forwardRowOutput} for it.
     */
    private void assertDepartureRestoresTwice(String rows, String forwardRow, String forwardRowOutput) throws Exception {
        restartLiveViewsAndRecover();
        assertDepartureRestored(rows);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            commitDeparture(job, forwardRow);
        }
        final String grownRows = rows + forwardRowOutput;
        assertDepartureRows(grownRows);
        assertNoRefreshFaults("lv");
        restartLiveViewsAndRecover();
        assertDepartureRestored(grownRows);
    }

    private void assertDepartureRows(String expected) throws Exception {
        assertQuery("SELECT ts, sym, x, s FROM lv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    /**
     * The item's own sequence: two roots above the day the base loses, the newest one's row
     * upserted out of the filter, then a late row between the two roots. {@code departedValue}
     * is what the upsert writes; every value the filter rejects departs the row alike.
     */
    private void assertDepartedNewestRowThenALateRowRestore(
            String departedValue,
            String inMemoryClause,
            boolean isDurationCadence
    ) throws Exception {
        createDepartureView("TIMESTAMP", DEPARTURE_SUM + " s FROM base WHERE x > 0", inMemoryClause);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            commitTheDayTheBaseLoses(job);
            if (isDurationCadence) {
                setCurrentMicros(currentMicros + 6 * Micros.MINUTE_MICROS);
            }
            commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
            if (isDurationCadence) {
                setCurrentMicros(currentMicros + 6 * Micros.MINUTE_MICROS);
            }
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5)");
            assertDepartureLadder("""
                    2025-12-31T12:00:00.000000Z -> 1
                    2026-01-01T00:01:00.000000Z -> 2
                    2026-01-01T00:02:00.000000Z -> 3
                    """);
            // The newest row leaves the view. The resume replays nothing, and the root its splice
            // kept at the departed row holds the state below it: the truncate drops that root.
            commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', " + departedValue + ")");
            assertDepartureLadder("""
                    2025-12-31T12:00:00.000000Z -> 1
                    2026-01-01T00:01:00.000000Z -> 2
                    """);
            Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
            Assert.assertEquals(ts("2026-01-01T00:01:00.000000Z"), departureView().getHeadCheckpointMaxTs());
            // A late row between the two roots resumes from the root at the frontier, with
            // nothing above it.
            commitDeparture(job, "('2026-01-01T00:01:30.000000Z', 'a', 3.0)");
            assertDepartureLadder("""
                    2025-12-31T12:00:00.000000Z -> 1
                    2026-01-01T00:01:00.000000Z -> 2
                    2026-01-01T00:01:30.000000Z -> 3
                    """);
            Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
        }
        assertDepartureRows(DEPARTED_ROW_THEN_LATE_ROW);
        assertNoRefreshFaults("lv");
        assertDepartureRestoresTwice(DEPARTED_ROW_THEN_LATE_ROW, "('2026-01-01T00:03:00.000000Z', 'a', 4.0)", """
                2026-01-01T00:03:00.000000Z\ta\t4.0\t8.5
                """);
    }

    /**
     * Runs {@link #commitSingleDeparture} over the view {@code selectTail} describes, whose
     * window gives the three rows left {@code s1}, {@code s2} and {@code s3}, and the forward row
     * after the first restart {@code forwardS}.
     */
    private void assertSingleDepartureRestores(String selectTail, String s1, String s2, String s3, String forwardS) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createDepartureView("TIMESTAMP", selectTail, "");
            assertSingleDepartureOfTheViewRestores(s1, s2, s3, forwardS);
        });
    }

    private void assertSingleDepartureOfTheViewRestores(String s1, String s2, String s3, String forwardS) throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            commitSingleDeparture(job);
            Assert.assertEquals(1, job.getStatsForTest().get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
        }
        final String rows = singleDepartureRows(s1, s2, s3);
        assertDepartureRows(rows);
        assertNoRefreshFaults("lv");
        assertDepartureRestoresTwice(rows, SINGLE_DEPARTURE_FORWARD_ROW, "2026-01-01T00:05:00.000000Z\ta\t4.5\t" + forwardS + "\n");
    }

    private void commitDeparture(LiveViewRefreshJob job, String values) throws Exception {
        execute("INSERT INTO base VALUES " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // One refresh pass, the way one worker turn takes the notifications waiting for it.
    private void runOneDeparturePass(LiveViewRefreshJob job) {
        advanceClockToNextRefreshPass();
        drainWalQueue();
        job.run();
        drainWalQueue();
    }

    /**
     * The single-departure shape: the day the base loses, a root per commit above it, then one
     * commit that seals two rows under one root, at the newer of them, whose row an upsert then
     * moves out of the filter. The departed row held the key's largest value.
     */
    private void commitSingleDeparture(LiveViewRefreshJob job) throws Exception {
        commitTheDayTheBaseLoses(job);
        commitDeparture(job, "('2026-01-01T00:01:00.000000Z', 'a', 1.5)");
        commitDeparture(job, "('2026-01-01T00:02:00.000000Z', 'a', 2.5), ('2026-01-01T00:03:00.000000Z', 'a', 9.5)");
        assertDepartureLadder("""
                2025-12-31T12:00:00.000000Z -> 1
                2026-01-01T00:01:00.000000Z -> 2
                2026-01-01T00:03:00.000000Z -> 4
                """);
        commitDeparture(job, "('2026-01-01T00:03:00.000000Z', 'a', -9.5)");
        assertDepartureLadder(SINGLE_DEPARTURE_LADDER);
    }

    /**
     * The head-miss departure fixture: the row 2025-12-31T23:55 of a day the base then drops, a
     * row a minute from 00:00 to 00:30 the cadence leaves unsealed, then one from 00:31 to 00:59,
     * which seals a root at 00:59.
     */
    private void commitMinuteBatchesOverTheLostDay(LiveViewRefreshJob job) throws Exception {
        commitDeparture(job, "('2025-12-31T23:55:00.000000Z', 'a', 0.5)");
        execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
        drainWalQueue();
        driveRefreshToQuiescence(job);
        commitDeparture(job, minuteRowsOfA(0, 30, "1.0"));
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        commitDeparture(job, minuteRowsOfA(31, 59, "1.0"));
        assertDepartureLadder("""
                2025-12-31T23:55:00.000000Z -> 1
                2026-01-01T00:59:00.000000Z -> 61
                """);
    }

    /**
     * Commits the one row of a day the base then drops, which the view walks past and keeps:
     * from here on a rebuild from the applied base would lose it.
     */
    private void commitTheDayTheBaseLoses(LiveViewRefreshJob job) throws Exception {
        commitDeparture(job, "('2025-12-31T12:00:00.000000Z', 'a', 0.5)");
        execute("ALTER TABLE base DROP PARTITION LIST '2025-12-31'");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * A view over a deduplicating base that keeps the rows {@code WHERE x > 0}, so an upsert of a
     * value the filter rejects moves a row out of it.
     */
    private void createHeadMissDepartureView(String timestampType) throws Exception {
        execute("CREATE TABLE base (ts " + timestampType + ", sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + HEAD_MISS_DEPARTURE_VIEW_SQL);
    }

    private void createDepartureView(String timestampType, String selectTail, String inMemoryClause) throws Exception {
        execute("CREATE TABLE base (ts " + timestampType + ", sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms " + inMemoryClause + "START FROM BEGINNING AS SELECT ts, sym, x, " + selectTail);
    }

    private LongList departureCheckpointIds() {
        final LongList ids = new LongList();
        try (
                Path dir = departureCheckpointsDir();
                LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration)
        ) {
            store.of(dir);
            reader.of(dir);
            try (LiveViewCheckpointGenerationPin pin = store.pin()) {
                reader.iterateAll(pin.getTimelineRootRef(), entry -> ids.add(entry.checkpointId));
            }
        }
        return ids;
    }

    private long departureGeneration() {
        try (Path dir = departureCheckpointsDir(); LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration)) {
            store.of(dir);
            return store.getSuperblock().generation;
        }
    }

    private long departureNextCheckpointId() {
        try (Path dir = departureCheckpointsDir(); LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration)) {
            store.of(dir);
            return store.getSuperblock().nextCheckpointId;
        }
    }

    // Leaves the view's in-memory tier holding the text y under two keys: an upsert replays away
    // the lead-only text x, so the view's symbol cache keeps the id it interned for x, and y takes
    // the id after it while the view's table commits y at the id before it. The view holds eleven
    // rows: k0 to k8 with val 1, k0 with val 3, and y with val 4.
    private void replayLeadTextAway() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m START FROM NOW AS " +
                "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            // Nine texts, flushed to the view's table.
            execute("""
                    INSERT INTO base (sym, val, ts) VALUES
                        ('k0', 1, '2026-01-01T00:00:01.000000Z'), ('k1', 1, '2026-01-01T00:00:02.000000Z'),
                        ('k2', 1, '2026-01-01T00:00:03.000000Z'), ('k3', 1, '2026-01-01T00:00:04.000000Z'),
                        ('k4', 1, '2026-01-01T00:00:05.000000Z'), ('k5', 1, '2026-01-01T00:00:06.000000Z'),
                        ('k6', 1, '2026-01-01T00:00:07.000000Z'), ('k7', 1, '2026-01-01T00:00:08.000000Z'),
                        ('k8', 1, '2026-01-01T00:00:09.000000Z')
                    """);
            drainWalQueue();
            drainJob(job);
            drainWalQueue();

            // Within FLUSH EVERY (clock still 0): an un-flushed lead with the new text x.
            execute("INSERT INTO base (sym, val, ts) VALUES ('x', 2, '2026-01-01T00:00:10.000000Z')");
            drainWalQueue();
            drainJob(job);

            // Enable dedup and upsert the x row away with a flushed text: the replay commits
            // no x, while the symbol cache keeps the id it gave x.
            execute("ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts)");
            drainWalQueue();
            setCurrentMicros(2_000_000L);
            execute("INSERT INTO base (sym, val, ts) VALUES ('k0', 3, '2026-01-01T00:00:10.000000Z')");
            drainWalQueue();
            drainJob(job);
            drainWalQueue();

            // The new text y: the in-memory tier serves its row.
            setCurrentMicros(4_000_000L);
            execute("INSERT INTO base (sym, val, ts) VALUES ('y', 4, '2026-01-01T00:00:11.000000Z')");
            drainWalQueue();
            drainJob(job);
            drainWalQueue();
        }
    }
}
