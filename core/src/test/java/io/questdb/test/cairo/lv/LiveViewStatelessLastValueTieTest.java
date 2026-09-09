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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code last_value} over a RANGE frame ending at {@code CURRENT ROW}, over rows that
 * share a designated timestamp. Two things meet here, and each is pinned separately.
 * <p>
 * The first is a SQL semantic deviation. The standard defines a RANGE frame's
 * {@code CURRENT ROW} end through the current row's <em>peer group</em>, so the frame
 * runs to the last row sharing the current timestamp and {@code last_value} returns that
 * row's value. QuestDB stops at the current physical row instead, so every row of a tie
 * emits its own argument and the RANGE spelling answers exactly what the ROWS spelling
 * does. {@link #testRangeStatelessLastValueTakesThePhysicalRowNotTheLastPeer()} locks
 * that against the peer-correct oracle, which is spelled here as a whole-partition
 * {@code last_value} keyed on {@code (sym, ts)} - one partition per tie group.
 * <p>
 * The second is what that deviation buys the live-view checkpoint repair, and why
 * correcting it later would not invalidate the repair's high bound. Reading one row is
 * what makes the compiled class {@code isCheckpointStateless()}, which gives the view a
 * zero-width RANGE arm: {@code L = R} and {@code H = changeMaxTs + 1}. Both bounds are
 * timestamps, not row positions, so an out-of-order row landing on a timestamp the view
 * already holds re-emits the <em>complete</em> tie group - every peer, across every key -
 * rather than the changed row alone. That is the interval a peer-correct implementation
 * would need too, since a peer's value can only move the output of rows at its own
 * timestamp. {@link #testTieCorrectionReEmitsTheCompleteTie()} and
 * {@link #testBoundedRangeTieCorrectionReEmitsTheCompleteTie()} prove it on the counters
 * and on the resulting rows, and
 * {@link #testRestartRestoresFromATieBoundaryAndRepairsIntoIt()} repeats the correction
 * against a timeline a restart restored rather than one this process sealed.
 * <p>
 * {@link #testTieSpanningTwoRefreshCyclesSkipsTheSeal()} covers the other side of a tie
 * the checkpoint cadence has to deal with: rows at one timestamp arriving over two
 * refresh cycles, so the second cycle's candidate boundary lands on the timestamp the
 * head already covers and there is nothing to seal.
 */
public class LiveViewStatelessLastValueTieTest extends AbstractLiveViewTest {

    // Frame width the SQL names but the compiled class does not read: last_value over any
    // frame ending at CURRENT ROW lands on the same current-row class, so this view's
    // repair takes the same zero-width bounds as the unbounded spelling below.
    private static final String BOUNDED_RANGE_FRAME = "PARTITION BY sym ORDER BY ts RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW";
    private static final int HISTORY_COMMITS = 12;
    private static final String RANGE_FRAME = "PARTITION BY sym ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
    // Second-of-day the tie group sits on, and the values its rows carry: three for key
    // 'a' - so the tie is a peer group within one partition, which is where the peer
    // semantics are observable - and one for key 'b', which shares the timestamp but not
    // the partition.
    private static final int TIE_SECOND = 50;
    private static final int TIE_A_1 = 51;
    private static final int TIE_A_2 = 52;
    private static final int TIE_A_3 = 53;
    private static final int TIE_B = 105;
    // Value of the out-of-order row every repair case appends to the tie.
    private static final int TIE_CORRECTION = 999;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical root per commit, the densest cadence the view can seal, so a repair
        // has a boundary at the tie's own timestamp to splice.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testBoundedRangeTieCorrectionReEmitsTheCompleteTie() throws Exception {
        // The same repair over a frame whose SQL names a 30-second width. The width never
        // reaches the plan: last_value over a frame ending at CURRENT ROW compiles to the
        // stateless class, which contributes a zero-width arm, so the interval is the one
        // tie group and not the four commits a 30-second look-behind would pull in.
        assertTieCorrectionReEmitsTheCompleteTie(BOUNDED_RANGE_FRAME);
    }

    @Test
    public void testRangeStatelessLastValueTakesThePhysicalRowNotTheLastPeer() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE peers (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO peers (ts, sym, x) VALUES " +
                    row(10, "a", 1) + ", " +
                    row(20, "a", 2) + ", " +
                    row(20, "a", 3) + ", " +
                    row(20, "a", 4) + ", " +
                    row(20, "b", 7) + ", " +
                    row(30, "a", 5) + ", " +
                    row(30, "b", 8));
            drainWalQueue();

            // range_lv is the shape under test; rows_lv is the same call over ROWS framing;
            // peer_lv is the reference answer, one partition per (key, timestamp) peer group
            // read to its last row.
            //
            // The lock: range_lv equals the row's own x everywhere, so it agrees with ROWS
            // framing exactly - two framing modes that the standard makes disagree over a
            // tie - and it differs from peer_lv at the two rows of the tie that are not its
            // physically last. Nothing here is corrected; a peer-semantics change has to
            // restate these rows.
            assertQuery("SELECT ts, sym, x, range_lv, rows_lv, peer_lv FROM (" +
                    "SELECT ts, sym, x, " +
                    "last_value(x) OVER (" + RANGE_FRAME + ") range_lv, " +
                    "last_value(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) rows_lv, " +
                    "last_value(x) OVER (PARTITION BY sym, ts) peer_lv " +
                    "FROM peers) ORDER BY ts, x")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\tx\trange_lv\trows_lv\tpeer_lv\n" +
                            "2026-01-01T00:00:10.000000Z\ta\t1\t1\t1\t1\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t2\t2\t2\t4\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t3\t3\t3\t4\n" +
                            "2026-01-01T00:00:20.000000Z\ta\t4\t4\t4\t4\n" +
                            "2026-01-01T00:00:20.000000Z\tb\t7\t7\t7\t7\n" +
                            "2026-01-01T00:00:30.000000Z\ta\t5\t5\t5\t5\n" +
                            "2026-01-01T00:00:30.000000Z\tb\t8\t8\t8\t8\n");
        });
    }

    @Test
    public void testRestartRestoresFromATieBoundaryAndRepairsIntoIt() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView(RANGE_FRAME);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= HISTORY_COMMITS; commit++) {
                    commitInOrder(job, commit);
                }
                driveRefreshToQuiescence(job);
                assertViewMatchesBase(2 * HISTORY_COMMITS + 2);
            }

            // A restart restores the runtime from the newest logical root and replays
            // the base above it. One of the roots below that head sits on the tie, so
            // the correction after the restart addresses a boundary the restore had to
            // carry through rather than one this process sealed.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesBase(2 * HISTORY_COMMITS + 2);

                commitRows(job, row(TIE_SECOND, "a", TIE_CORRECTION));
                driveRefreshToQuiescence(job);
                assertTieGroup();
                assertViewMatchesBase(2 * HISTORY_COMMITS + 3);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTieCorrectionReEmitsTheCompleteTie() throws Exception {
        assertTieCorrectionReEmitsTheCompleteTie(RANGE_FRAME);
    }

    @Test
    public void testTieSpanningTwoRefreshCyclesSkipsTheSeal() throws Exception {
        // A tie does not have to arrive in one commit. When the second half lands in a
        // later refresh cycle, that cycle's rows carry the timestamp the head boundary
        // already covers, so its cadence seal has no boundary to add: a normal root only
        // ever extends the timeline strictly upwards. That is ordinary data, so the seal
        // is skipped rather than attempted and reported as a failure - which is what it
        // used to be, one CRITICAL log line per such cycle.
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            // Commit index of the tie: one commit per ten seconds, so the tie at
            // TIE_SECOND is the tieCommit-th of them.
            final int tieCommit = TIE_SECOND / 10;
            createBaseAndView(RANGE_FRAME);
            capture.start();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit < tieCommit; commit++) {
                    commitInOrder(job, commit);
                }
                commitRows(job, row(TIE_SECOND, "a", TIE_A_1) + ", " + row(TIE_SECOND, "a", TIE_A_2));
                driveRefreshToQuiescence(job);
                // One boundary per commit at this cadence, the last of them on the tie.
                assertTimelineEntries(tieCommit);

                // The rest of the tie, at the same designated timestamp.
                commitRows(job, row(TIE_SECOND, "a", TIE_A_3) + ", " + row(TIE_SECOND, "b", TIE_B));
                driveRefreshToQuiescence(job);
                assertTimelineEntries(tieCommit);
                assertViewMatchesBase(2 * (tieCommit - 1) + 4);

                // The cadence stayed armed rather than being consumed by the skip: the
                // next cycle above the tie seals, and it seals both cycles' rows.
                commitInOrder(job, tieCommit + 1);
                driveRefreshToQuiescence(job);
                assertTimelineEntries(tieCommit + 1);
                assertViewMatchesBase(2 * tieCommit + 4);
                assertNoRefreshFaults("lv");
            } finally {
                capture.stop();
            }
            capture.assertNotLogged("could not write live view head checkpoint");
        });
    }

    // One VALUES tuple: a row of the base table at the given second-of-day.
    private static String row(int secondOfDay, String sym, long x) {
        return "('" + timestamp(secondOfDay) + "', '" + sym + "', " + x + ')';
    }

    // A 2026-01-01 microsecond literal at the given second-of-day offset. Every row of
    // every case sits inside one calendar day, so the base's DAY partitioning never
    // enters the picture.
    private static String timestamp(int secondOfDay) {
        return String.format("2026-01-01T00:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    /**
     * Drives one view over a history carrying a tie group, corrects that tie with an
     * out-of-order row, and asserts the repair covered the whole of it.
     */
    private void assertTieCorrectionReEmitsTheCompleteTie(String frame) throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView(frame);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int commit = 1; commit <= HISTORY_COMMITS; commit++) {
                    commitInOrder(job, commit);
                }
                driveRefreshToQuiescence(job);
                // Two rows per commit, plus the two extra peers the tie commit carries.
                final int historyRows = 2 * HISTORY_COMMITS + 2;
                assertViewMatchesBase(historyRows);

                // The plan is settled at compile time and the pair is still NULL: a
                // forward-only history has run no repair to report on.
                assertQuery("SELECT checkpoint_repair_plan, checkpoint_repair_last_disposition, " +
                        "checkpoint_repair_last_denial FROM live_views()")
                        .noLeakCheck().noRandomAccess()
                        .returns("checkpoint_repair_plan\tcheckpoint_repair_last_disposition\t" +
                                "checkpoint_repair_last_denial\n" +
                                "range\t\t\n");

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                commitRows(job, row(TIE_SECOND, "a", TIE_CORRECTION));
                driveRefreshToQuiescence(job);

                // L = R, so nothing is warmed up and no anchor is resumed from...
                Assert.assertEquals(
                        "a stateless view replays from R itself rather than resuming an anchor",
                        0,
                        instance.getO3ResumeReplayRows()
                );
                // ...and H = changeMaxTs + 1 is exclusive at the next distinct timestamp,
                // not at the changed row, so the interval is the complete tie: three peers
                // of key 'a', the corrected row, and key 'b's row at the same timestamp.
                // A bound that stopped at the changed row would read and re-emit one row.
                Assert.assertEquals(
                        "the replay must read the complete timestamp tie and nothing above it",
                        5,
                        instance.getO3ReplayScanRows()
                );
                Assert.assertEquals(
                        "the replacement must re-emit every peer of the changed row, not the row alone",
                        5,
                        instance.getO3BoundaryReplayRows()
                );

                assertQuery("SELECT checkpoint_repair_plan, checkpoint_repair_last_disposition, " +
                        "checkpoint_repair_last_denial FROM live_views()")
                        .noLeakCheck().noRandomAccess()
                        .returns("checkpoint_repair_plan\tcheckpoint_repair_last_disposition\t" +
                                "checkpoint_repair_last_denial\n" +
                                "range\tlocalized rebuild\t\n");

                // The tie group holds every peer exactly once, each still emitting its own
                // argument: the correction moved no peer's output, which is the deviation
                // restated at the repaired timestamp. Under peer semantics all four rows of
                // key 'a' would read the corrected value instead - and the repair's
                // interval, which covers them all, would already be wide enough to say so.
                assertTieGroup();
                assertViewMatchesBase(historyRows + 1);
                assertNoRefreshFaults("lv");
            }
        });
    }

    // Logical boundaries the view's checkpoint timeline currently holds.
    private void assertTimelineEntries(long expected) throws Exception {
        assertQuery("SELECT checkpoint_timeline_entries FROM live_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("checkpoint_timeline_entries\n" + expected + "\n");
    }

    // The repaired tie group, every peer exactly once. Ordered by value rather than left
    // to the physical order the out-of-order merge chose within the timestamp.
    private void assertTieGroup() throws Exception {
        // noLeakCheck() throughout: the leak-checking form clears the engine, and with it
        // the live-view registry the case is still driving.
        assertQuery("SELECT sym, l FROM lv WHERE ts = '" + timestamp(TIE_SECOND) + "' ORDER BY sym, l")
                .noLeakCheck()
                .returns("sym\tl\n" +
                        "a\t" + TIE_A_1 + "\n" +
                        "a\t" + TIE_A_2 + "\n" +
                        "a\t" + TIE_A_3 + "\n" +
                        "a\t" + TIE_CORRECTION + "\n" +
                        "b\t" + TIE_B + "\n");
    }

    /**
     * The view against the projection its call reduces to - every row emits its own
     * argument - and against the row count the base holds, so a repair that dropped a peer
     * or duplicated one fails here rather than passing on matching values.
     */
    private void assertViewMatchesBase(int expectedRows) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT ts, sym, x AS l FROM base) ORDER BY 1, 2, 3",
                "(SELECT ts, sym, l FROM lv) ORDER BY 1, 2, 3",
                LOG,
                true
        );
        assertQuery("SELECT count() FROM lv")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + expectedRows + "\n");
    }

    // One in-order commit at the given commit index: one row per key at commit * 10
    // seconds, except at the tie, which carries three rows for key 'a'.
    private void commitInOrder(LiveViewRefreshJob job, int commit) throws Exception {
        final int second = commit * 10;
        if (second == TIE_SECOND) {
            commitRows(job, row(second, "a", TIE_A_1) + ", " + row(second, "a", TIE_A_2)
                    + ", " + row(second, "a", TIE_A_3) + ", " + row(second, "b", TIE_B));
            return;
        }
        commitRows(job, row(second, "a", commit) + ", " + row(second, "b", 100 + commit));
    }

    private void commitRows(LiveViewRefreshJob job, String values) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        execute("INSERT INTO base (ts, sym, x) VALUES " + values);
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private void createBaseAndView(String frame) throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS "
                + "SELECT ts, sym, last_value(x) OVER (" + frame + ") l FROM base");
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }
}
