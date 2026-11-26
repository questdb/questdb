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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.lv.LiveViewRefreshStats.SEGMENT_REPAIR_COUNT;

/**
 * Base row removals - DROP / DETACH PARTITION, TRUNCATE and TTL eviction - that a live view's
 * refresh meets while it lags WAL apply. Each test applies an out-of-order commit (the trigger)
 * and the commits after it before a single refresh, so the refresh replays the trigger against a
 * base that already holds every later commit, and commits its repair at that snapshot.
 * <p>
 * A removal is transparent to a live view: the rows the view emitted for the removed base rows
 * keep their emitted values (LiveViewSmokeTest#testDropPartitionOnBaseIsTransparentToLiveView and
 * its siblings). The repair keeps that contract when it lags. Every view row below the lower of
 * the lowest row the repair re-derives and the trigger's own timestamp keeps its emitted value;
 * the rows from there up equal the view's query recomputed over the repaired base from START FROM.
 * A DATA commit applied ahead of the trigger does not lower that floor, whichever order it and the
 * removal came in, and whether or not the refresh walked past the removal before the trigger. A
 * row such a commit carries into a removed day, below START FROM or outside the view's filter has
 * no view row to replace, and a replace commit over a removed day deletes no base row, so a floor
 * taken from either would only reach rows the removal froze.
 * <p>
 * The same holds when the removed partition shares the view's open anchor segment with partitions
 * that survive it: an hourly TTL base under a daily anchor, or a daily one under an anchor offset
 * from midnight. Those tests evict hour 0 behind a view that holds its rows, then lag over an
 * out-of-order row in hour 1 (the trigger), a late row into the evicted hour, which the apply
 * evicts again, and an in-order row.
 */
public class LiveViewApplyAheadRemovalTest extends AbstractLiveViewTest {

    private static final String ANCHOR_VIEW = "SELECT ts, x, sum(x) OVER w AS rn FROM base " +
            "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))";
    private static final String BASE_DAY2 = """
            ts\tx
            2026-01-02T00:00:01.000000Z\t11
            2026-01-02T00:00:02.000000Z\t12
            2026-01-02T00:00:03.000000Z\t13
            """;
    private static final String BASE_DDL = "CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL";
    private static final String COUNT_VIEW = "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts " +
            "ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base";
    // A daily sum per key, so the open anchor segment spans every hour partition of its day. The
    // view keeps its indexed key, so a repair's walk places each row of the change set.
    private static final String DAILY_ANCHOR_VIEW = "SELECT ts, g, x, sum(x) OVER w AS rn FROM base " +
            "WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '00:00')";
    private static final String DROP_DAY1 = "ALTER TABLE base DROP PARTITION LIST '2026-01-01'";
    // COUNT_VIEW over EMITTED_ROWS.
    private static final String EMITTED_COUNT = """
            ts\tx\trn
            2026-01-01T00:00:01.000000Z\t1\t1
            2026-01-01T00:00:03.000000Z\t3\t2
            2026-01-01T00:00:05.000000Z\t5\t3
            2026-01-02T00:00:01.000000Z\t11\t4
            2026-01-02T00:00:03.000000Z\t13\t5
            """;
    // The rows the view emits before it lags: day 1 at 01/03/05, day 2 at 01/03.
    private static final String EMITTED_ROWS = """
            INSERT INTO base (ts, x) VALUES
            ('2026-01-01T00:00:01.000000Z', 1),
            ('2026-01-01T00:00:03.000000Z', 3),
            ('2026-01-01T00:00:05.000000Z', 5),
            ('2026-01-02T00:00:01.000000Z', 11),
            ('2026-01-02T00:00:03.000000Z', 13)""";
    // A row in day 1 that a view with WHERE x < 100 filters out.
    private static final String FILTERED_DAY1_INSERT = "INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:02.000000Z', 100)";
    // COUNT_VIEW after the repair: the day-1 rows keep their emitted values, and day 2 is
    // re-derived from the base, which no longer holds day 1.
    private static final String FROZEN_DAY1_REDERIVED_DAY2 = """
            ts\tx\trn
            2026-01-01T00:00:01.000000Z\t1\t1
            2026-01-01T00:00:03.000000Z\t3\t2
            2026-01-01T00:00:05.000000Z\t5\t3
            2026-01-02T00:00:01.000000Z\t11\t1
            2026-01-02T00:00:02.000000Z\t12\t2
            2026-01-02T00:00:03.000000Z\t13\t3
            """;
    // The rows of hour 0, hour 1 and hour 2 the view emits over HOUR_TTL_BASE_DDL before it lags.
    private static final String HOUR0_INSERT = """
            INSERT INTO base (ts, x, g) VALUES
            ('2026-01-01T00:10:00.000000Z', 1, 'a'),
            ('2026-01-01T00:20:00.000000Z', 2, 'a'),
            ('2026-01-01T00:30:00.000000Z', 3, 'a')""";
    private static final String HOUR1_INSERT = """
            INSERT INTO base (ts, x, g) VALUES
            ('2026-01-01T01:10:00.000000Z', 10, 'a'),
            ('2026-01-01T01:20:00.000000Z', 20, 'a')""";
    // An out-of-order row in hour 1, below the view's frontier at 02:10: the repair's trigger.
    private static final String HOUR1_TRIGGER = "INSERT INTO base (ts, x, g) VALUES ('2026-01-01T01:15:00.000000Z', 15, 'a')";
    private static final String HOUR2_INSERT = "INSERT INTO base (ts, x, g) VALUES ('2026-01-01T02:10:00.000000Z', 100, 'a')";
    // The in-order row of the lagging pass.
    private static final String HOUR2_IN_ORDER_INSERT = "INSERT INTO base (ts, x, g) VALUES ('2026-01-01T02:30:00.000000Z', 1000, 'a')";
    // The base after the lagging pass: hour 0 is gone, the late row with it.
    private static final String HOUR_TTL_BASE_AFTER = """
            ts\tx
            2026-01-01T01:10:00.000000Z\t10
            2026-01-01T01:15:00.000000Z\t15
            2026-01-01T01:20:00.000000Z\t20
            2026-01-01T02:10:00.000000Z\t100
            2026-01-01T02:30:00.000000Z\t1000
            """;
    // TTL measures retention against the lower of the newest row and the wall clock, which these
    // tests move with the data, so the apply of the 02:10 row evicts hour 0 and keeps hour 1.
    private static final String HOUR_TTL_BASE_DDL = "CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR WAL";
    // DAILY_ANCHOR_VIEW over the rows of hours 0 to 2.
    private static final String HOUR_TTL_EMITTED = """
            ts\tx\trn
            2026-01-01T00:10:00.000000Z\t1\t1.0
            2026-01-01T00:20:00.000000Z\t2\t3.0
            2026-01-01T00:30:00.000000Z\t3\t6.0
            2026-01-01T01:10:00.000000Z\t10\t16.0
            2026-01-01T01:20:00.000000Z\t20\t36.0
            2026-01-01T02:10:00.000000Z\t100\t136.0
            """;
    // DAILY_ANCHOR_VIEW START FROM 00:12 over the rows of hours 0 to 2.
    private static final String HOUR_TTL_EMITTED_FROM_0012 = """
            ts\tx\trn
            2026-01-01T00:20:00.000000Z\t2\t2.0
            2026-01-01T00:30:00.000000Z\t3\t5.0
            2026-01-01T01:10:00.000000Z\t10\t15.0
            2026-01-01T01:20:00.000000Z\t20\t35.0
            2026-01-01T02:10:00.000000Z\t100\t135.0
            """;
    // DAILY_ANCHOR_VIEW after the lagging pass. The rows below the lower of the trigger (01:15) and
    // the lowest row the repair re-derives (01:10) keep their emitted values, and the daily sum is
    // recomputed from 01:10 over the base, which no longer holds hour 0.
    private static final String HOUR_TTL_KEPT = """
            ts\tx\trn
            2026-01-01T00:10:00.000000Z\t1\t1.0
            2026-01-01T00:20:00.000000Z\t2\t3.0
            2026-01-01T00:30:00.000000Z\t3\t6.0
            2026-01-01T01:10:00.000000Z\t10\t10.0
            2026-01-01T01:15:00.000000Z\t15\t25.0
            2026-01-01T01:20:00.000000Z\t20\t45.0
            2026-01-01T02:10:00.000000Z\t100\t145.0
            2026-01-01T02:30:00.000000Z\t1000\t1145.0
            """;
    private static final String LATE_DAY1_INSERT = "INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:02.000000Z', 2)";
    // A late row into the evicted hour 0, between the rows the eviction froze.
    private static final String LATE_HOUR0_INSERT = "INSERT INTO base (ts, x, g) VALUES ('2026-01-01T00:15:00.000000Z', 5, 'a')";
    // RANGE_VIEW over EMITTED_ROWS.
    private static final String RANGE_EMITTED = """
            ts\tx\trn
            2026-01-01T00:00:01.000000Z\t1\t1.0
            2026-01-01T00:00:03.000000Z\t3\t3.0
            2026-01-01T00:00:05.000000Z\t5\t5.0
            2026-01-02T00:00:01.000000Z\t11\t11.0
            2026-01-02T00:00:03.000000Z\t13\t13.0
            """;
    // RANGE_VIEW after the repair: the day-1 rows keep their emitted values, and day 2 is
    // re-derived from the base, which no longer holds day 1.
    private static final String RANGE_FROZEN_DAY1_REDERIVED_DAY2 = """
            ts\tx\trn
            2026-01-01T00:00:01.000000Z\t1\t1.0
            2026-01-01T00:00:03.000000Z\t3\t3.0
            2026-01-01T00:00:05.000000Z\t5\t5.0
            2026-01-02T00:00:01.000000Z\t11\t11.0
            2026-01-02T00:00:02.000000Z\t12\t23.0
            2026-01-02T00:00:03.000000Z\t13\t25.0
            """;
    private static final String RANGE_VIEW = "SELECT ts, x, sum(x) OVER (PARTITION BY g ORDER BY ts " +
            "RANGE BETWEEN '1' SECOND PRECEDING AND CURRENT ROW) AS rn FROM base";
    // An out-of-order row below the view's frontier (day-2 00:00:03): the repair's trigger.
    private static final String TRIGGER = "INSERT INTO base (ts, x) VALUES ('2026-01-02T00:00:02.000000Z', 12)";
    private static final String TTL_BASE_DDL = "CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY TTL 2 DAYS WAL";

    @Before
    public void pinClockBelowTestData() {
        // START FROM NOW resolves the clock at CREATE, so it must sit below the data.
        setCurrentMicros(0L);
    }

    @Test
    public void testDropPartitionRefreshedThenFilteredLateInsertKeepsEmittedRows() throws Exception {
        // The refresh walks past the drop before the trigger, so the apply-ahead range holds
        // only the late row, which re-creates day 1 and fails the view's filter. A view whose
        // repair localizes (RANGE, or ROWS with a finite frame) takes another path that loses
        // these rows as well; this test covers the unlocalized repair.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW + " WHERE x < 100");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(DROP_DAY1);
                refreshAll(job);
                assertLv(EMITTED_COUNT);

                execute(TRIGGER);
                execute(FILTERED_DAY1_INSERT);
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T00:00:02.000000Z\t100
                    2026-01-02T00:00:01.000000Z\t11
                    2026-01-02T00:00:02.000000Z\t12
                    2026-01-02T00:00:03.000000Z\t13
                    """);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropPartitionRefreshedThenReplaceCommitKeepsEmittedRows() throws Exception {
        // The refresh walks past the drop before the trigger, so the apply-ahead range holds
        // only the replace commit, whose range covers the dropped day and deletes no base row.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(DROP_DAY1);
                refreshAll(job);
                assertLv(EMITTED_COUNT);

                execute(TRIGGER);
                pureDelete("2026-01-01T00:00:02.000000Z", "2026-01-02T00:00:00.500000Z");
                refreshAll(job);
            }
            assertBase(BASE_DAY2);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropPartitionRefreshedThenStructuralChangeAheadKeepsEmittedRows() throws Exception {
        // A structural change ahead of the trigger leaves the apply-ahead range unclassified
        // whatever the commits after it hold, so the RANGE view's repair does not localize: it
        // rebuilds from START FROM and keeps the rows the drop froze. The restart recompiles the
        // view against the added column first. Without it the view's plan, compiled before the
        // column existed, would not match the base the repair pins, and the refresh would
        // recover through a full rebuild rather than this repair.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", RANGE_VIEW + " WHERE x < 100");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, RANGE_EMITTED);
                execute(DROP_DAY1);
                refreshAll(job);
                assertLv(RANGE_EMITTED);

                execute(TRIGGER);
                execute("ALTER TABLE base ADD COLUMN y INT");
                execute(FILTERED_DAY1_INSERT);
                drainWalQueue();
            }
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T00:00:02.000000Z\t100
                    2026-01-02T00:00:01.000000Z\t11
                    2026-01-02T00:00:02.000000Z\t12
                    2026-01-02T00:00:03.000000Z\t13
                    """);
            assertLv(RANGE_FROZEN_DAY1_REDERIVED_DAY2);
            assertQuery("SELECT checkpoint_repair_last_disposition, checkpoint_repair_last_denial FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            checkpoint_repair_last_disposition\tcheckpoint_repair_last_denial
                            boundary rebuild\tunclassified apply ahead
                            """);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropPartitionThenFilteredLateInsertKeepsEmittedRows() throws Exception {
        // The late row re-creates the dropped day after the drop, but the view's filter
        // rejects it, so the lowest row the repair re-derives is on day 2.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW + " WHERE x < 100");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute(DROP_DAY1);
                execute(FILTERED_DAY1_INSERT);
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T00:00:02.000000Z\t100
                    2026-01-02T00:00:01.000000Z\t11
                    2026-01-02T00:00:02.000000Z\t12
                    2026-01-02T00:00:03.000000Z\t13
                    """);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropPartitionThenLateInsertBelowStartFromKeepsEmittedRows() throws Exception {
        // The late row re-creates the dropped day below the view's START FROM, so the view
        // never holds it.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "'2026-01-01T00:00:00.500000Z'", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute(DROP_DAY1);
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:00.100000Z', 0)");
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T00:00:00.100000Z\t0
                    2026-01-02T00:00:01.000000Z\t11
                    2026-01-02T00:00:02.000000Z\t12
                    2026-01-02T00:00:03.000000Z\t13
                    """);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropPartitionThenReplaceCommitAboveFilteredPartitionKeepsEmittedRows() throws Exception {
        // An older partition survives the drop, but the view filters out its only row, so the
        // base's oldest partition sits below the rows the drop froze.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW + " WHERE x < 100");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2025-12-31T00:00:05.000000Z', 100)");
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute(DROP_DAY1);
                pureDelete("2026-01-01T00:00:02.000000Z", "2026-01-02T00:00:00.500000Z");
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2025-12-31T00:00:05.000000Z\t100
                    2026-01-02T00:00:01.000000Z\t11
                    2026-01-02T00:00:02.000000Z\t12
                    2026-01-02T00:00:03.000000Z\t13
                    """);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropPartitionThenReplaceCommitKeepsEmittedRows() throws Exception {
        // The replace commit's range covers the dropped day and the start of day 2, and deletes
        // no base row: day 1 is gone and day 2 starts at 00:00:01.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute(DROP_DAY1);
                pureDelete("2026-01-01T00:00:02.000000Z", "2026-01-02T00:00:00.500000Z");
                refreshAll(job);
            }
            assertBase(BASE_DAY2);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDroppedPartitionRecreatedThenReplaceCommitKeepsEmittedRows() throws Exception {
        // A late row re-creates the dropped day, so the base holds day 1 again when the replace
        // commit, which deletes no base row, covers the frozen 03 and 05.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(DROP_DAY1);
                refreshAll(job);
                assertLv(EMITTED_COUNT);
                // Not lagging: the repair of this out-of-order row keeps the day-1 rows the
                // drop froze and re-derives the rest from 07 up.
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:07.000000Z', 7)");
                refreshAll(job);
                assertLv("""
                        ts\tx\trn
                        2026-01-01T00:00:01.000000Z\t1\t1
                        2026-01-01T00:00:03.000000Z\t3\t2
                        2026-01-01T00:00:05.000000Z\t5\t3
                        2026-01-01T00:00:07.000000Z\t7\t1
                        2026-01-02T00:00:01.000000Z\t11\t2
                        2026-01-02T00:00:03.000000Z\t13\t3
                        """);

                execute(TRIGGER);
                pureDelete("2026-01-01T00:00:02.000000Z", "2026-01-01T00:00:06.000000Z");
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T00:00:07.000000Z\t7
                    2026-01-02T00:00:01.000000Z\t11
                    2026-01-02T00:00:02.000000Z\t12
                    2026-01-02T00:00:03.000000Z\t13
                    """);
            assertLv("""
                    ts\tx\trn
                    2026-01-01T00:00:01.000000Z\t1\t1
                    2026-01-01T00:00:03.000000Z\t3\t2
                    2026-01-01T00:00:05.000000Z\t5\t3
                    2026-01-01T00:00:07.000000Z\t7\t1
                    2026-01-02T00:00:01.000000Z\t11\t2
                    2026-01-02T00:00:02.000000Z\t12\t3
                    2026-01-02T00:00:03.000000Z\t13\t4
                    """);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDroppedPartitionRecreatedThenReplaceCommitOverFrozenRowsKeepsThem() throws Exception {
        // As above, but every frozen day-1 row lies inside the replace commit's range. The
        // repair reads the same base, view timestamps and apply-ahead range as it would for a
        // replace commit that deleted base rows 03 and 05 of an intact day 1 (03, 05, 07); here
        // 03 and 05 are rows the drop froze, so they stay.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final String emitted = """
                        ts\tx\trn
                        2026-01-01T00:00:03.000000Z\t3\t1
                        2026-01-01T00:00:05.000000Z\t5\t2
                        2026-01-02T00:00:01.000000Z\t11\t3
                        2026-01-02T00:00:03.000000Z\t13\t4
                        """;
                emit(
                        job,
                        """
                                INSERT INTO base (ts, x) VALUES
                                ('2026-01-01T00:00:03.000000Z', 3),
                                ('2026-01-01T00:00:05.000000Z', 5),
                                ('2026-01-02T00:00:01.000000Z', 11),
                                ('2026-01-02T00:00:03.000000Z', 13)""",
                        emitted
                );
                execute(DROP_DAY1);
                refreshAll(job);
                assertLv(emitted);
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:07.000000Z', 7)");
                refreshAll(job);
                assertLv("""
                        ts\tx\trn
                        2026-01-01T00:00:03.000000Z\t3\t1
                        2026-01-01T00:00:05.000000Z\t5\t2
                        2026-01-01T00:00:07.000000Z\t7\t1
                        2026-01-02T00:00:01.000000Z\t11\t2
                        2026-01-02T00:00:03.000000Z\t13\t3
                        """);

                execute(TRIGGER);
                pureDelete("2026-01-01T00:00:02.000000Z", "2026-01-01T00:00:06.000000Z");
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T00:00:07.000000Z\t7
                    2026-01-02T00:00:01.000000Z\t11
                    2026-01-02T00:00:02.000000Z\t12
                    2026-01-02T00:00:03.000000Z\t13
                    """);
            assertLv("""
                    ts\tx\trn
                    2026-01-01T00:00:03.000000Z\t3\t1
                    2026-01-01T00:00:05.000000Z\t5\t2
                    2026-01-01T00:00:07.000000Z\t7\t1
                    2026-01-02T00:00:01.000000Z\t11\t2
                    2026-01-02T00:00:02.000000Z\t12\t3
                    2026-01-02T00:00:03.000000Z\t13\t4
                    """);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateInsertBelowStartFromThenDropPartitionKeepsEmittedRows() throws Exception {
        // The late row sits below the view's START FROM, under every row the view emitted.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "'2026-01-01T00:00:00.500000Z'", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:00.100000Z', 0)");
                execute(DROP_DAY1);
                refreshAll(job);
            }
            assertBase(BASE_DAY2);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateInsertThenDetachPartitionKeepsEmittedRows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                execute("ALTER TABLE base DETACH PARTITION LIST '2026-01-01'");
                refreshAll(job);
            }
            assertBase(BASE_DAY2);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateInsertThenDropParquetPartitionKeepsEmittedRows() throws Exception {
        // Day 1 and day 2 are Parquet partitions, so the trigger and the late row are
        // out-of-order commits into Parquet.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final String emitted = """
                        ts\tx\trn
                        2026-01-01T00:00:01.000000Z\t1\t1
                        2026-01-01T00:00:03.000000Z\t3\t2
                        2026-01-01T00:00:05.000000Z\t5\t3
                        2026-01-02T00:00:01.000000Z\t11\t4
                        2026-01-02T00:00:03.000000Z\t13\t5
                        2026-01-03T00:00:01.000000Z\t31\t6
                        """;
                emit(
                        job,
                        """
                                INSERT INTO base (ts, x) VALUES
                                ('2026-01-01T00:00:01.000000Z', 1),
                                ('2026-01-01T00:00:03.000000Z', 3),
                                ('2026-01-01T00:00:05.000000Z', 5),
                                ('2026-01-02T00:00:01.000000Z', 11),
                                ('2026-01-02T00:00:03.000000Z', 13),
                                ('2026-01-03T00:00:01.000000Z', 31)""",
                        emitted
                );
                execute("ALTER TABLE base CONVERT PARTITION TO PARQUET LIST '2026-01-01', '2026-01-02'");
                refreshAll(job);
                assertQuery("SELECT name, isParquet FROM table_partitions('base')")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                name\tisParquet
                                2026-01-01\ttrue
                                2026-01-02\ttrue
                                2026-01-03\tfalse
                                """);
                assertLv(emitted);

                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                execute(DROP_DAY1);
                refreshAll(job);
            }
            assertQuery("SELECT name, isParquet FROM table_partitions('base')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tisParquet
                            2026-01-02\ttrue
                            2026-01-03\tfalse
                            """);
            assertBase(BASE_DAY2 + "2026-01-03T00:00:01.000000Z\t31\n");
            assertLv(FROZEN_DAY1_REDERIVED_DAY2 + "2026-01-03T00:00:01.000000Z\t31\t4\n");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateInsertThenDropPartitionAboveFilteredPartitionKeepsEmittedRows() throws Exception {
        // The base's oldest partition survives the drop, but the view filters out its only row,
        // so the lowest row the repair re-derives sits above the dropped day.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW + " WHERE x < 100");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2025-12-31T00:00:05.000000Z', 100)");
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                execute(DROP_DAY1);
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2025-12-31T00:00:05.000000Z\t100
                    2026-01-02T00:00:01.000000Z\t11
                    2026-01-02T00:00:02.000000Z\t12
                    2026-01-02T00:00:03.000000Z\t13
                    """);
            assertLv(FROZEN_DAY1_REDERIVED_DAY2);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateInsertThenDropPartitionKeepsEmittedRowsAnchor() throws Exception {
        assertLateInsertThenDropPartitionKeepsEmittedRows(
                ANCHOR_VIEW,
                """
                        ts\tx\trn
                        2026-01-01T00:00:01.000000Z\t1\t1.0
                        2026-01-01T00:00:03.000000Z\t3\t4.0
                        2026-01-01T00:00:05.000000Z\t5\t9.0
                        2026-01-02T00:00:01.000000Z\t11\t11.0
                        2026-01-02T00:00:03.000000Z\t13\t24.0
                        """,
                """
                        ts\tx\trn
                        2026-01-01T00:00:01.000000Z\t1\t1.0
                        2026-01-01T00:00:03.000000Z\t3\t4.0
                        2026-01-01T00:00:05.000000Z\t5\t9.0
                        2026-01-02T00:00:01.000000Z\t11\t11.0
                        2026-01-02T00:00:02.000000Z\t12\t23.0
                        2026-01-02T00:00:03.000000Z\t13\t36.0
                        """,
                "2026-01-02T00:00:04.000000Z\t14\t50.0\n"
        );
    }

    @Test
    public void testLateInsertThenDropPartitionKeepsEmittedRowsCount() throws Exception {
        assertLateInsertThenDropPartitionKeepsEmittedRows(
                COUNT_VIEW,
                EMITTED_COUNT,
                FROZEN_DAY1_REDERIVED_DAY2,
                "2026-01-02T00:00:04.000000Z\t14\t4\n"
        );
    }

    @Test
    public void testLateInsertThenDropPartitionKeepsEmittedRowsRange() throws Exception {
        assertLateInsertThenDropPartitionKeepsEmittedRows(
                RANGE_VIEW,
                RANGE_EMITTED,
                RANGE_FROZEN_DAY1_REDERIVED_DAY2,
                "2026-01-02T00:00:04.000000Z\t14\t27.0\n"
        );
    }

    @Test
    public void testLateInsertThenTruncateKeepsEmittedRowsBelowTrigger() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                execute("TRUNCATE TABLE base");
                refreshAll(job);
            }
            assertBase("ts\tx\n");
            assertLvBelowTrigger("""
                    ts\tx\trn
                    2026-01-01T00:00:01.000000Z\t1\t1
                    2026-01-01T00:00:03.000000Z\t3\t2
                    2026-01-01T00:00:05.000000Z\t5\t3
                    2026-01-02T00:00:01.000000Z\t11\t4
                    """);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateInsertThenTtlEvictionKeepsEmittedRowsAnchor() throws Exception {
        assertLateInsertThenTtlEvictionKeepsEmittedRows(
                ANCHOR_VIEW,
                """
                        ts\tx\trn
                        2026-01-01T00:00:01.000000Z\t1\t1.0
                        2026-01-01T00:00:03.000000Z\t3\t4.0
                        2026-01-01T00:00:05.000000Z\t5\t9.0
                        2026-01-02T00:00:01.000000Z\t11\t11.0
                        2026-01-02T00:00:03.000000Z\t13\t24.0
                        """,
                """
                        ts\tx\trn
                        2026-01-01T00:00:01.000000Z\t1\t1.0
                        2026-01-01T00:00:03.000000Z\t3\t4.0
                        2026-01-01T00:00:05.000000Z\t5\t9.0
                        2026-01-02T00:00:01.000000Z\t11\t11.0
                        2026-01-02T00:00:02.000000Z\t12\t23.0
                        2026-01-02T00:00:03.000000Z\t13\t36.0
                        2026-01-04T00:00:01.000000Z\t41\t41.0
                        """,
                "removed output floor"
        );
    }

    @Test
    public void testLateInsertThenTtlEvictionKeepsEmittedRowsCount() throws Exception {
        assertLateInsertThenTtlEvictionKeepsEmittedRows(
                COUNT_VIEW,
                EMITTED_COUNT,
                FROZEN_DAY1_REDERIVED_DAY2 + "2026-01-04T00:00:01.000000Z\t41\t4\n",
                null
        );
    }

    @Test
    public void testLateInsertThenTtlEvictionOfADayWithoutViewRowsKeepsTheSegmentRepair() throws Exception {
        // The late row is day 1's only row, so the eviction froze no row the view holds: the
        // repair of day 1's anchor segment, whose floor is the late row, has nothing of the
        // view's to delete below the trigger, and stands.
        assertMemoryLeak(() -> {
            createBaseAndView(TTL_BASE_DDL, "NOW", ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, """
                        INSERT INTO base (ts, x) VALUES
                        ('2026-01-02T00:00:01.000000Z', 11),
                        ('2026-01-02T00:00:03.000000Z', 13)""", """
                        ts\tx\trn
                        2026-01-02T00:00:01.000000Z\t11\t11.0
                        2026-01-02T00:00:03.000000Z\t13\t24.0
                        """);
                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                insertRowEvictingDay1();
                refreshAll(job);
                Assert.assertEquals(1, job.getStatsForTest().get(SEGMENT_REPAIR_COUNT));
            }
            assertBase(BASE_DAY2 + "2026-01-04T00:00:01.000000Z\t41\n");
            assertLv("""
                    ts\tx\trn
                    2026-01-02T00:00:01.000000Z\t11\t11.0
                    2026-01-02T00:00:02.000000Z\t12\t23.0
                    2026-01-02T00:00:03.000000Z\t13\t36.0
                    2026-01-04T00:00:01.000000Z\t41\t41.0
                    """);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateInsertThenTtlEvictionOfADayWithoutViewRowsKeepsTheLocalizedRepair() throws Exception {
        // The view's filter rejected every day-1 row, so the eviction froze no row the view
        // holds: the localized repair, whose floor is the late day-1 row, has nothing of the
        // view's to delete below the trigger, and stands.
        assertMemoryLeak(() -> {
            createBaseAndView(TTL_BASE_DDL, "NOW", RANGE_VIEW + " WHERE x < 100");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, """
                        INSERT INTO base (ts, x) VALUES
                        ('2026-01-01T00:00:01.000000Z', 101),
                        ('2026-01-01T00:00:03.000000Z', 103),
                        ('2026-01-01T00:00:05.000000Z', 105),
                        ('2026-01-02T00:00:01.000000Z', 11),
                        ('2026-01-02T00:00:03.000000Z', 13)""", """
                        ts\tx\trn
                        2026-01-02T00:00:01.000000Z\t11\t11.0
                        2026-01-02T00:00:03.000000Z\t13\t13.0
                        """);
                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                insertRowEvictingDay1();
                refreshAll(job);
            }
            assertBase(BASE_DAY2 + "2026-01-04T00:00:01.000000Z\t41\n");
            assertLv("""
                    ts\tx\trn
                    2026-01-02T00:00:01.000000Z\t11\t11.0
                    2026-01-02T00:00:02.000000Z\t12\t23.0
                    2026-01-02T00:00:03.000000Z\t13\t25.0
                    2026-01-04T00:00:01.000000Z\t41\t41.0
                    """);
            assertRepairOutcome("localized rebuild", "");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateInsertThenTtlEvictionKeepsEmittedRowsRange() throws Exception {
        assertLateInsertThenTtlEvictionKeepsEmittedRows(
                RANGE_VIEW,
                RANGE_EMITTED,
                RANGE_FROZEN_DAY1_REDERIVED_DAY2 + "2026-01-04T00:00:01.000000Z\t41\t41.0\n",
                "removed output floor"
        );
    }

    @Test
    public void testLateInsertThenTtlEvictionKeepsEmittedRowsResume() throws Exception {
        // The first commit holds day-1 00:00:01 alone and refreshes on its own, which seals a
        // root there, under the late day-1 row, so the plan prices a resume from it against
        // the localized rebuild. A resume replaces from the row above its root, which the
        // eviction retired with the rest of day 1.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createBaseAndView(TTL_BASE_DDL, "NOW", RANGE_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:01.000000Z', 1)");
                refreshAll(job);
                emit(job, """
                        INSERT INTO base (ts, x) VALUES
                        ('2026-01-01T00:00:03.000000Z', 3),
                        ('2026-01-01T00:00:05.000000Z', 5),
                        ('2026-01-02T00:00:01.000000Z', 11),
                        ('2026-01-02T00:00:03.000000Z', 13)""", RANGE_EMITTED);
                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                insertRowEvictingDay1();
                refreshAll(job);
            }
            assertBase(BASE_DAY2 + "2026-01-04T00:00:01.000000Z\t41\n");
            assertLv(RANGE_FROZEN_DAY1_REDERIVED_DAY2 + "2026-01-04T00:00:01.000000Z\t41\t41.0\n");
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowBelowEveryFrozenRowOfOpenAnchorSegmentKeepsEmittedRows() throws Exception {
        // The late row at 00:05 sits below every row the eviction of hour 0 froze.
        assertLagOverEvictedHourKeepsEmittedRows(
                HOUR_TTL_BASE_DDL,
                DAILY_ANCHOR_VIEW,
                "INSERT INTO base (ts, x, g) VALUES ('2026-01-01T00:05:00.000000Z', 5, 'a')"
        );
    }

    @Test
    public void testLateRowIntoEvictedDayOfNoonAnchorSegmentKeepsEmittedRows() throws Exception {
        // DAY partitions under ANCHOR DAILY '12:00': the open segment from 2026-01-01 12:00 spans the
        // end of day 1 and the morning of day 2. The clock holds below the data until the lagging
        // pass, whose in-order row's apply evicts day 1, the late row into day 1 included.
        assertMemoryLeak(() -> {
            createBaseAndView(
                    "CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY TTL 1 DAY WAL",
                    "NOW",
                    "SELECT ts, g, x, sum(x) OVER w AS rn FROM base WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR DAILY '12:00')"
            );
            final StringSink frozenRows = new StringSink();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(
                        job,
                        """
                                INSERT INTO base (ts, x, g) VALUES
                                ('2026-01-01T12:10:00.000000Z', 1, 'a'),
                                ('2026-01-01T12:20:00.000000Z', 2, 'a'),
                                ('2026-01-01T12:30:00.000000Z', 3, 'a'),
                                ('2026-01-02T07:10:00.000000Z', 10, 'a'),
                                ('2026-01-02T07:20:00.000000Z', 20, 'a'),
                                ('2026-01-02T08:00:00.000000Z', 100, 'a')""",
                        """
                                ts\tx\trn
                                2026-01-01T12:10:00.000000Z\t1\t1.0
                                2026-01-01T12:20:00.000000Z\t2\t3.0
                                2026-01-01T12:30:00.000000Z\t3\t6.0
                                2026-01-02T07:10:00.000000Z\t10\t16.0
                                2026-01-02T07:20:00.000000Z\t20\t36.0
                                2026-01-02T08:00:00.000000Z\t100\t136.0
                                """
                );
                printSql("SELECT ts, x, rn FROM lv WHERE ts < '2026-01-02T07:10' ORDER BY ts", frozenRows);

                execute("INSERT INTO base (ts, x, g) VALUES ('2026-01-02T07:15:00.000000Z', 15, 'a')");
                execute("INSERT INTO base (ts, x, g) VALUES ('2026-01-01T12:15:00.000000Z', 5, 'a')");
                setCurrentMicros(ts("2026-01-03T01:00:00.000000Z"));
                execute("INSERT INTO base (ts, x, g) VALUES ('2026-01-03T00:30:00.000000Z', 1000, 'a')");
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-02T07:10:00.000000Z\t10
                    2026-01-02T07:15:00.000000Z\t15
                    2026-01-02T07:20:00.000000Z\t20
                    2026-01-02T08:00:00.000000Z\t100
                    2026-01-03T00:30:00.000000Z\t1000
                    """);
            assertLv("""
                    ts\tx\trn
                    2026-01-01T12:10:00.000000Z\t1\t1.0
                    2026-01-01T12:20:00.000000Z\t2\t3.0
                    2026-01-01T12:30:00.000000Z\t3\t6.0
                    2026-01-02T07:10:00.000000Z\t10\t10.0
                    2026-01-02T07:15:00.000000Z\t15\t25.0
                    2026-01-02T07:20:00.000000Z\t20\t45.0
                    2026-01-02T08:00:00.000000Z\t100\t145.0
                    2026-01-03T00:30:00.000000Z\t1000\t1000.0
                    """);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            // The rows below 07:10, the lower of the trigger and the lowest row the repair re-derives,
            // are the ones the view emitted, and the rows from there up are the anchored sum over the
            // base as it stands.
            assertQuery("SELECT ts, x, rn FROM lv WHERE ts < '2026-01-02T07:10' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(frozenRows.toString());
            assertSqlCursors(
                    "SELECT ts, x, sum(x) OVER (PARTITION BY g, timestamp_floor('1d', ts, '1970-01-01T12:00:00.000000Z'::timestamp) ORDER BY ts) AS rn FROM base",
                    "SELECT ts, x, rn FROM lv WHERE ts >= '2026-01-02T07:10'"
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsDiscardedLead() throws Exception {
        // The 02:20 row is an un-flushed lead when the lagging pass lands, so the repair discards it,
        // and the output it does not hold on disk denies the anchored repair its bound.
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "10m", "NOW", DAILY_ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(job, HOUR0_INSERT, HOUR1_INSERT, HOUR_TTL_EMITTED);
                emitHour2Lead(job, HOUR_TTL_EMITTED + "2026-01-01T02:20:00.000000Z\t200\t336.0\n");
                execute(HOUR1_TRIGGER);
                execute(LATE_HOUR0_INSERT);
                execute(HOUR2_IN_ORDER_INSERT);
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T01:10:00.000000Z\t10
                    2026-01-01T01:15:00.000000Z\t15
                    2026-01-01T01:20:00.000000Z\t20
                    2026-01-01T02:10:00.000000Z\t100
                    2026-01-01T02:20:00.000000Z\t200
                    2026-01-01T02:30:00.000000Z\t1000
                    """);
            assertLv("""
                    ts\tx\trn
                    2026-01-01T00:10:00.000000Z\t1\t1.0
                    2026-01-01T00:20:00.000000Z\t2\t3.0
                    2026-01-01T00:30:00.000000Z\t3\t6.0
                    2026-01-01T01:10:00.000000Z\t10\t10.0
                    2026-01-01T01:15:00.000000Z\t15\t25.0
                    2026-01-01T01:20:00.000000Z\t20\t45.0
                    2026-01-01T02:10:00.000000Z\t100\t145.0
                    2026-01-01T02:20:00.000000Z\t200\t345.0
                    2026-01-01T02:30:00.000000Z\t1000\t1345.0
                    """);
            assertRepairOutcome("boundary rebuild", "unflushed output");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsExpressionAnchor() throws Exception {
        assertLagOverEvictedHourKeepsEmittedRows(
                HOUR_TTL_BASE_DDL,
                "SELECT ts, g, x, sum(x) OVER w AS rn FROM base WINDOW w AS (PARTITION BY g ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))",
                LATE_HOUR0_INSERT
        );
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsNanosecondBase() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(
                    "CREATE TABLE base (ts TIMESTAMP_NS, x LONG, g SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR WAL",
                    "NOW",
                    DAILY_ANCHOR_VIEW
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(job, HOUR0_INSERT, HOUR1_INSERT, nanos(HOUR_TTL_EMITTED));
                lagOverEvictedHour(job, HOUR1_TRIGGER, LATE_HOUR0_INSERT, HOUR2_IN_ORDER_INSERT);
            }
            assertBase(nanos(HOUR_TTL_BASE_AFTER));
            assertLv(nanos(HOUR_TTL_KEPT));
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsNonIndexedKey() throws Exception {
        // A key without an index takes no keyed scan, so the walk places each commit wholly in the
        // open segment by its own span rather than row by row.
        assertLagOverEvictedHourKeepsEmittedRows(
                "CREATE TABLE base (ts TIMESTAMP, x LONG, g SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR TTL 1 HOUR WAL",
                DAILY_ANCHOR_VIEW,
                LATE_HOUR0_INSERT
        );
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsOfEveryKey() throws Exception {
        // Keys a and b share hour 0. The trigger and the in-order row are b's, the late row is a's,
        // and the rows of both keys the eviction froze stay.
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "NOW", DAILY_ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(
                        job,
                        """
                                INSERT INTO base (ts, x, g) VALUES
                                ('2026-01-01T00:10:00.000000Z', 1, 'a'),
                                ('2026-01-01T00:20:00.000000Z', 2, 'b'),
                                ('2026-01-01T00:30:00.000000Z', 3, 'a')""",
                        """
                                INSERT INTO base (ts, x, g) VALUES
                                ('2026-01-01T01:10:00.000000Z', 10, 'a'),
                                ('2026-01-01T01:20:00.000000Z', 20, 'b')""",
                        """
                                ts\tx\trn
                                2026-01-01T00:10:00.000000Z\t1\t1.0
                                2026-01-01T00:20:00.000000Z\t2\t2.0
                                2026-01-01T00:30:00.000000Z\t3\t4.0
                                2026-01-01T01:10:00.000000Z\t10\t14.0
                                2026-01-01T01:20:00.000000Z\t20\t22.0
                                2026-01-01T02:10:00.000000Z\t100\t114.0
                                """
                );
                lagOverEvictedHour(
                        job,
                        "INSERT INTO base (ts, x, g) VALUES ('2026-01-01T01:15:00.000000Z', 15, 'b')",
                        LATE_HOUR0_INSERT,
                        "INSERT INTO base (ts, x, g) VALUES ('2026-01-01T02:30:00.000000Z', 1000, 'b')"
                );
            }
            assertBase(HOUR_TTL_BASE_AFTER);
            assertLv("""
                    ts\tx\trn
                    2026-01-01T00:10:00.000000Z\t1\t1.0
                    2026-01-01T00:20:00.000000Z\t2\t2.0
                    2026-01-01T00:30:00.000000Z\t3\t4.0
                    2026-01-01T01:10:00.000000Z\t10\t10.0
                    2026-01-01T01:15:00.000000Z\t15\t15.0
                    2026-01-01T01:20:00.000000Z\t20\t35.0
                    2026-01-01T02:10:00.000000Z\t100\t110.0
                    2026-01-01T02:30:00.000000Z\t1000\t1035.0
                    """);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsOverTheUnionRange() throws Exception {
        // Without the per-segment decomposition the repair plans the change set's union range from
        // the trigger. The open segment's residual has to agree with it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        assertLagOverEvictedHourKeepsEmittedRows(HOUR_TTL_BASE_DDL, DAILY_ANCHOR_VIEW, LATE_HOUR0_INSERT);
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsRestartBeforeRefresh() throws Exception {
        // The base applies the lagging pass before a restart, so the restarted view meets the
        // trigger, the late row and the eviction in its first refresh. A second restart restores
        // what the repair published.
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "NOW", DAILY_ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(job, HOUR0_INSERT, HOUR1_INSERT, HOUR_TTL_EMITTED);
                setCurrentMicros(ts("2026-01-01T02:30:00.000000Z"));
                execute(HOUR1_TRIGGER);
                execute(LATE_HOUR0_INSERT);
                execute(HOUR2_IN_ORDER_INSERT);
                drainWalQueue();
            }
            assertBase(HOUR_TTL_BASE_AFTER);
            restartAndRefresh();
            assertLv(HOUR_TTL_KEPT);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();

            restartAndRefresh();
            assertLv(HOUR_TTL_KEPT);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsResume() throws Exception {
        // The first commit holds 00:10 alone and refreshes on its own, which seals a root there,
        // under the late row, so the plan prices a resume from it. A resume replaces from the row
        // above its root, below the rows the eviction of hour 0 froze, so the plan falls back to the
        // boundary rebuild.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "NOW", DAILY_ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(ts("2026-01-01T00:10:00.000000Z"));
                execute("INSERT INTO base (ts, x, g) VALUES ('2026-01-01T00:10:00.000000Z', 1, 'a')");
                refreshAll(job);
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(ts("2026-01-01T00:10:00.000000Z"), snapshotCheckpointLadder(instance).getQuick(0));
                emitAndEvictHour0(
                        job,
                        """
                                INSERT INTO base (ts, x, g) VALUES
                                ('2026-01-01T00:20:00.000000Z', 2, 'a'),
                                ('2026-01-01T00:30:00.000000Z', 3, 'a')""",
                        HOUR1_INSERT,
                        HOUR_TTL_EMITTED
                );
                lagOverEvictedHour(job, HOUR1_TRIGGER, LATE_HOUR0_INSERT, HOUR2_IN_ORDER_INSERT);
            }
            assertBase(HOUR_TTL_BASE_AFTER);
            assertLv(HOUR_TTL_KEPT);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsStartFromBetweenFrozenRows() throws Exception {
        // START FROM 00:12 sits between the rows of hour 0, below the late row, so the view's
        // boundary clamps neither the late row nor the trigger.
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "'2026-01-01T00:12:00.000000Z'", DAILY_ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(job, HOUR0_INSERT, HOUR1_INSERT, HOUR_TTL_EMITTED_FROM_0012);
                lagOverEvictedHour(job, HOUR1_TRIGGER, LATE_HOUR0_INSERT, HOUR2_IN_ORDER_INSERT);
            }
            assertBase(HOUR_TTL_BASE_AFTER);
            assertLv("""
                    ts\tx\trn
                    2026-01-01T00:20:00.000000Z\t2\t2.0
                    2026-01-01T00:30:00.000000Z\t3\t5.0
                    2026-01-01T01:10:00.000000Z\t10\t10.0
                    2026-01-01T01:15:00.000000Z\t15\t25.0
                    2026-01-01T01:20:00.000000Z\t20\t45.0
                    2026-01-01T02:10:00.000000Z\t100\t145.0
                    2026-01-01T02:30:00.000000Z\t1000\t1145.0
                    """);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowIntoEvictedHourKeepsEmittedRowsWithoutOpenSegmentKeyedReplay() throws Exception {
        // Without the open segment's keyed replay a trigger in the open segment plans the union range
        // from the trigger, as a repair did before the decomposition.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, "false");
        assertLagOverEvictedHourKeepsEmittedRows(HOUR_TTL_BASE_DDL, DAILY_ANCHOR_VIEW, LATE_HOUR0_INSERT);
    }

    @Test
    public void testLateRowIntoEvictedHourOfOpenAnchorSegmentKeepsEmittedRows() throws Exception {
        // Every row from the start of the day sits in the view's open anchor segment, so the floor of
        // the change the repair re-reads is the late row, which the apply evicted again, below the
        // rows the eviction of hour 0 froze.
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "NOW", DAILY_ANCHOR_VIEW);
            final StringSink frozenRows = new StringSink();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(job, HOUR0_INSERT, HOUR1_INSERT, HOUR_TTL_EMITTED);
                printSql("SELECT ts, x, rn FROM lv WHERE ts < '2026-01-01T01:10' ORDER BY ts", frozenRows);
                lagOverEvictedHour(job, HOUR1_TRIGGER, LATE_HOUR0_INSERT, HOUR2_IN_ORDER_INSERT);
            }
            assertBase(HOUR_TTL_BASE_AFTER);
            assertLv(HOUR_TTL_KEPT);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            // The rows below 01:10, the lower of the trigger and the lowest row the repair re-derives,
            // are the ones the view emitted, and the rows from there up are the daily sum over the base
            // as it stands.
            assertQuery("SELECT ts, x, rn FROM lv WHERE ts < '2026-01-01T01:10' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(frozenRows.toString());
            assertSqlCursors(
                    "SELECT ts, x, sum(x) OVER (PARTITION BY g, timestamp_floor('1d', ts) ORDER BY ts) AS rn FROM base",
                    "SELECT ts, x, rn FROM lv WHERE ts >= '2026-01-01T01:10'"
            );

            // A restart restores what the repair published, and an in-order row continues the daily sum
            // the repair recomputed.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                refreshAll(job);
                assertLv(HOUR_TTL_KEPT);
                execute("INSERT INTO base (ts, x, g) VALUES ('2026-01-01T02:40:00.000000Z', 10000, 'a')");
                refreshAll(job);
            }
            assertLv(HOUR_TTL_KEPT + "2026-01-01T02:40:00.000000Z\t10000\t11145.0\n");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLateRowIntoHourTheLaggingPassEvictsKeepsEmittedRows() throws Exception {
        // The clock holds at the view's creation until the lagging pass, so the base keeps hour 0
        // while the view emits it, and the apply of the pass's in-order row evicts hour 0, the late
        // row included.
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "NOW", DAILY_ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute(HOUR0_INSERT);
                refreshAll(job);
                execute(HOUR1_INSERT);
                refreshAll(job);
                emit(job, HOUR2_INSERT, HOUR_TTL_EMITTED);
                assertBase("""
                        ts\tx
                        2026-01-01T00:10:00.000000Z\t1
                        2026-01-01T00:20:00.000000Z\t2
                        2026-01-01T00:30:00.000000Z\t3
                        2026-01-01T01:10:00.000000Z\t10
                        2026-01-01T01:20:00.000000Z\t20
                        2026-01-01T02:10:00.000000Z\t100
                        """);

                execute(HOUR1_TRIGGER);
                execute(LATE_HOUR0_INSERT);
                setCurrentMicros(ts("2026-01-01T02:30:00.000000Z"));
                execute(HOUR2_IN_ORDER_INSERT);
                refreshAll(job);
            }
            assertBase(HOUR_TTL_BASE_AFTER);
            assertLv(HOUR_TTL_KEPT);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceCommitThenTtlEvictionKeepsEmittedRows() throws Exception {
        // The replace commit deletes the base row at day-1 00:00:01, then the apply of the
        // day-4 row evicts the rest of day 1. The query starts above the replace commit's range:
        // this test is about the rows the eviction froze.
        assertMemoryLeak(() -> {
            createBaseAndView(TTL_BASE_DDL, "NOW", COUNT_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                pureDelete("2026-01-01T00:00:00.000000Z", "2026-01-01T00:00:02.000000Z");
                insertRowEvictingDay1();
                refreshAll(job);
            }
            assertBase(BASE_DAY2 + "2026-01-04T00:00:01.000000Z\t41\n");
            assertQuery("SELECT ts, x, rn FROM lv WHERE ts >= '2026-01-01T00:00:02' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\trn
                            2026-01-01T00:00:03.000000Z\t3\t2
                            2026-01-01T00:00:05.000000Z\t5\t3
                            2026-01-02T00:00:01.000000Z\t11\t1
                            2026-01-02T00:00:02.000000Z\t12\t2
                            2026-01-02T00:00:03.000000Z\t13\t3
                            2026-01-04T00:00:01.000000Z\t41\t4
                            """);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testTriggerRowIntoEvictedHourKeepsEmittedRowsBelowIt() throws Exception {
        // The trigger itself lands in the evicted hour at 00:25, above the late row at 00:15, and the
        // apply evicts both. The rows below the trigger keep their emitted values. 00:30 sits above
        // it, and the base no longer holds the row the view emitted it for, so the repair deletes it.
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "NOW", DAILY_ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(job, HOUR0_INSERT, HOUR1_INSERT, HOUR_TTL_EMITTED);
                lagOverEvictedHour(
                        job,
                        "INSERT INTO base (ts, x, g) VALUES ('2026-01-01T00:25:00.000000Z', 15, 'a')",
                        LATE_HOUR0_INSERT,
                        HOUR2_IN_ORDER_INSERT
                );
            }
            assertBase("""
                    ts\tx
                    2026-01-01T01:10:00.000000Z\t10
                    2026-01-01T01:20:00.000000Z\t20
                    2026-01-01T02:10:00.000000Z\t100
                    2026-01-01T02:30:00.000000Z\t1000
                    """);
            assertLv("""
                    ts\tx\trn
                    2026-01-01T00:10:00.000000Z\t1\t1.0
                    2026-01-01T00:20:00.000000Z\t2\t3.0
                    2026-01-01T01:10:00.000000Z\t10\t10.0
                    2026-01-01T01:20:00.000000Z\t20\t30.0
                    2026-01-01T02:10:00.000000Z\t100\t130.0
                    2026-01-01T02:30:00.000000Z\t1000\t1130.0
                    """);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testTriggerStraddlingStartFromReplacesFrozenRowsFromStartFrom() throws Exception {
        // The walk drops the row below START FROM, so the open segment's residual takes its floor from
        // the trigger commit's row at 00:25, above the frozen row at 00:20. The lead denies the
        // anchored repair its bound, and the boundary rebuild still replaces from START FROM.
        assertTriggerStraddlingStartFromReplacesFrozenRows("unflushed output");
    }

    @Test
    public void testTriggerStraddlingStartFromReplacesFrozenRowsFromStartFromOverTheUnionRange() throws Exception {
        // The union range plans from the trigger's own timestamp, which START FROM clamps, so its
        // output floor lands on the view's boundary.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        assertTriggerStraddlingStartFromReplacesFrozenRows("view start floor");
    }

    @Test
    public void testTruncateThenFilteredLateInsertKeepsEmittedRowsBelowTrigger() throws Exception {
        // The late row re-creates the truncated base, but the view's filter rejects it, so the
        // repair re-derives no row at all.
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", COUNT_VIEW + " WHERE x < 100");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, EMITTED_COUNT);
                execute(TRIGGER);
                execute("TRUNCATE TABLE base");
                execute(FILTERED_DAY1_INSERT);
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T00:00:02.000000Z\t100
                    """);
            assertLvBelowTrigger("""
                    ts\tx\trn
                    2026-01-01T00:00:01.000000Z\t1\t1
                    2026-01-01T00:00:03.000000Z\t3\t2
                    2026-01-01T00:00:05.000000Z\t5\t3
                    2026-01-02T00:00:01.000000Z\t11\t4
                    """);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    // The rows a microsecond base prints, at a nanosecond base's precision.
    private static String nanos(String microsRows) {
        return microsRows.replace(".000000Z", ".000000000Z");
    }

    // A REPLACE_RANGE commit with no rows: deletes every base row in [lo, hi).
    private static void pureDelete(String lo, String hi) {
        final TableToken baseToken = engine.verifyTableName("base");
        try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
            walWriter.commitWithParams(ts(lo), ts(hi), WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }

    private void assertBase(String expected) throws Exception {
        assertQuery("SELECT ts, x FROM base ORDER BY ts")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    // The lagging pass over HOUR_TTL_BASE_DDL's rows, with the late row lateRow: the rows the
    // eviction of hour 0 froze keep their emitted values.
    private void assertLagOverEvictedHourKeepsEmittedRows(String baseDdl, String viewSql, String lateRow) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(baseDdl, "NOW", viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(job, HOUR0_INSERT, HOUR1_INSERT, HOUR_TTL_EMITTED);
                lagOverEvictedHour(job, HOUR1_TRIGGER, lateRow, HOUR2_IN_ORDER_INSERT);
            }
            assertBase(HOUR_TTL_BASE_AFTER);
            assertLv(HOUR_TTL_KEPT);
            assertRepairOutcome("boundary rebuild", "removed output floor");
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertLateInsertThenDropPartitionKeepsEmittedRows(
            String viewSql,
            String expectedEmitted,
            String expectedAfterRepair,
            String expectedContinuationRow
    ) throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView(BASE_DDL, "NOW", viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, expectedEmitted);
                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                execute(DROP_DAY1);
                refreshAll(job);
                assertBase(BASE_DAY2);
                assertLv(expectedAfterRepair);
                assertViewActive();

                // The repair leaves the window state of a recompute over the base it pinned, so
                // an in-order row continues from the re-derived day-2 rows.
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-02T00:00:04.000000Z', 14)");
                refreshAll(job);
            }
            assertLv(expectedAfterRepair + expectedContinuationRow);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    // Only DATA commits sit ahead of the trigger, so a RANGE or anchored repair localizes, or
    // resumes from a root, with its output floor at the late day-1 row the eviction retired. The
    // plan falls back to the boundary rebuild there, which keeps the rows the eviction froze:
    // see LiveViewCheckpointRepairPlan#denyRemovedOutputFloor.
    private void assertLateInsertThenTtlEvictionKeepsEmittedRows(
            String viewSql,
            String expectedEmitted,
            String expectedLv,
            String expectedDenial
    ) throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView(TTL_BASE_DDL, "NOW", viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emit(job, EMITTED_ROWS, expectedEmitted);
                execute(TRIGGER);
                execute(LATE_DAY1_INSERT);
                insertRowEvictingDay1();
                refreshAll(job);
            }
            assertBase(BASE_DAY2 + "2026-01-04T00:00:01.000000Z\t41\n");
            assertLv(expectedLv);
            if (expectedDenial != null) {
                assertRepairOutcome("boundary rebuild", expectedDenial);
            }
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertLv(String expected) throws Exception {
        assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    // The view rows below the trigger. With nothing left in the base to re-derive, the repair
    // replaces the view from the trigger's own timestamp up, as
    // LiveViewSmokeTest#testO3ApplyAheadTruncateFallsBackToBoundary pins; the rows below it are
    // the ones the removal froze.
    private void assertLvBelowTrigger(String expected) throws Exception {
        assertQuery("SELECT ts, x, rn FROM lv WHERE ts < '2026-01-02T00:00:02' ORDER BY ts")
                .noLeakCheck()
                .timestamp("ts")
                .returns(expected);
    }

    // The last repair's disposition and the reason it did not localize, empty when nothing
    // denied it.
    private void assertRepairOutcome(String expectedDisposition, String expectedDenial) throws Exception {
        assertQuery("SELECT checkpoint_repair_last_disposition, checkpoint_repair_last_denial FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("checkpoint_repair_last_disposition\tcheckpoint_repair_last_denial\n"
                        + expectedDisposition + "\t" + expectedDenial + "\n");
    }

    // START FROM 00:12, so the view holds hour 0 from 00:20 and seals its first root at 00:30, and an
    // un-flushed lead at 02:20 when the lagging pass lands. The trigger commit holds a row at 00:05,
    // below START FROM, and one at 00:25, under the first root, and the apply evicts both with hour
    // 0. The repair replaces from START FROM, the trigger's own timestamp clamped to the view's
    // boundary, and the base no longer holds the rows the view emitted for 00:20 and 00:30, so they
    // go.
    private void assertTriggerStraddlingStartFromReplacesFrozenRows(String expectedDenial) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(ts("2026-01-01T00:00:00.000000Z"));
            createBaseAndView(HOUR_TTL_BASE_DDL, "10m", "'2026-01-01T00:12:00.000000Z'", DAILY_ANCHOR_VIEW);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                emitAndEvictHour0(job, HOUR0_INSERT, HOUR1_INSERT, HOUR_TTL_EMITTED_FROM_0012);
                emitHour2Lead(job, HOUR_TTL_EMITTED_FROM_0012 + "2026-01-01T02:20:00.000000Z\t200\t335.0\n");
                execute("""
                        INSERT INTO base (ts, x, g) VALUES
                        ('2026-01-01T00:05:00.000000Z', 5, 'a'),
                        ('2026-01-01T00:25:00.000000Z', 15, 'a')""");
                execute(HOUR2_IN_ORDER_INSERT);
                refreshAll(job);
            }
            assertBase("""
                    ts\tx
                    2026-01-01T01:10:00.000000Z\t10
                    2026-01-01T01:20:00.000000Z\t20
                    2026-01-01T02:10:00.000000Z\t100
                    2026-01-01T02:20:00.000000Z\t200
                    2026-01-01T02:30:00.000000Z\t1000
                    """);
            assertLv("""
                    ts\tx\trn
                    2026-01-01T01:10:00.000000Z\t10\t10.0
                    2026-01-01T01:20:00.000000Z\t20\t30.0
                    2026-01-01T02:10:00.000000Z\t100\t130.0
                    2026-01-01T02:20:00.000000Z\t200\t330.0
                    2026-01-01T02:30:00.000000Z\t1000\t1330.0
                    """);
            assertRepairOutcome("boundary rebuild", expectedDenial);
            assertViewActive();
            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertViewActive() throws Exception {
        assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\nactive\n");
        assertNoRefreshFaults("lv");
    }

    private void createBaseAndView(String baseDdl, String startFrom, String viewSql) throws Exception {
        createBaseAndView(baseDdl, "100ms", startFrom, viewSql);
    }

    private void createBaseAndView(String baseDdl, String flushEvery, String startFrom, String viewSql) throws Exception {
        execute(baseDdl);
        execute("CREATE LIVE VIEW lv FLUSH EVERY " + flushEvery + " START FROM " + startFrom + " AS " + viewSql);
    }

    // Inserts the rows the view emits before it lags, refreshes, and checks what it emitted.
    private void emit(LiveViewRefreshJob job, String insertSql, String expectedLv) throws Exception {
        execute(insertSql);
        refreshAll(job);
        assertLv(expectedLv);
    }

    // Emits hour 0, hour 1 and the 02:10 row at a clock that follows them, so the apply of the
    // 02:10 row evicts hour 0 while the view holds its rows.
    private void emitAndEvictHour0(LiveViewRefreshJob job, String hour0Insert, String hour1Insert, String expectedLv) throws Exception {
        setCurrentMicros(ts("2026-01-01T00:30:00.000000Z"));
        execute(hour0Insert);
        refreshAll(job);
        setCurrentMicros(ts("2026-01-01T01:20:00.000000Z"));
        execute(hour1Insert);
        refreshAll(job);
        setCurrentMicros(ts("2026-01-01T02:10:00.000000Z"));
        execute(HOUR2_INSERT);
        refreshAll(job);
        assertLv(expectedLv);
    }

    // Leaves the 02:20 row an un-flushed lead in a FLUSH EVERY 10m view: the refresh drains it a
    // minute after the last flush, at a clock that holds still.
    private void emitHour2Lead(LiveViewRefreshJob job, String expectedLv) throws Exception {
        setCurrentMicros(ts("2026-01-01T02:11:00.000000Z"));
        execute("INSERT INTO base (ts, x, g) VALUES ('2026-01-01T02:20:00.000000Z', 200, 'a')");
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
        assertLv(expectedLv);
    }

    // TTL measures retention against the lower of the newest row and the wall clock, so the
    // clock moves past the data first. The apply of the day-4 row then evicts day 1, which ends
    // more than 2 days before it; day 2 stays.
    private void insertRowEvictingDay1() throws Exception {
        setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
        execute("INSERT INTO base (ts, x) VALUES ('2026-01-04T00:00:01.000000Z', 41)");
    }

    // The lagging pass: the base applies the trigger, the late row and the in-order row at 02:30,
    // which evicts hour 0 again, before one refresh drains them.
    private void lagOverEvictedHour(LiveViewRefreshJob job, String trigger, String lateRow, String inOrderRow) throws Exception {
        setCurrentMicros(ts("2026-01-01T02:30:00.000000Z"));
        execute(trigger);
        execute(lateRow);
        execute(inOrderRow);
        refreshAll(job);
    }

    private void refreshAll(LiveViewRefreshJob job) {
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // Stops every live view the way a process stop does, registers them again from disk and
    // refreshes them to quiescence.
    private void restartAndRefresh() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            refreshAll(job);
        }
    }
}
