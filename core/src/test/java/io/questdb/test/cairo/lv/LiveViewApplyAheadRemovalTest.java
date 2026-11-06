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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import org.junit.Before;
import org.junit.Test;

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
    private static final String LATE_DAY1_INSERT = "INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:02.000000Z', 2)";
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
                        """
        );
    }

    @Test
    public void testLateInsertThenTtlEvictionKeepsEmittedRowsCount() throws Exception {
        assertLateInsertThenTtlEvictionKeepsEmittedRows(
                COUNT_VIEW,
                EMITTED_COUNT,
                FROZEN_DAY1_REDERIVED_DAY2 + "2026-01-04T00:00:01.000000Z\t41\t4\n"
        );
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

    // A RANGE view is left out. With only DATA commits ahead of the trigger its repair localizes,
    // and the localized repair's output floor follows the lowest timestamp those commits carry -
    // the late day-1 row - into the rows the eviction froze. That path is separate from the
    // unlocalized repair these tests cover, and it loses those rows.
    private void assertLateInsertThenTtlEvictionKeepsEmittedRows(
            String viewSql,
            String expectedEmitted,
            String expectedLv
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

    private void assertViewActive() throws Exception {
        assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\nactive\n");
        assertNoRefreshFaults("lv");
    }

    private void createBaseAndView(String baseDdl, String startFrom, String viewSql) throws Exception {
        execute(baseDdl);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM " + startFrom + " AS " + viewSql);
    }

    // Inserts the rows the view emits before it lags, refreshes, and checks what it emitted.
    private void emit(LiveViewRefreshJob job, String insertSql, String expectedLv) throws Exception {
        execute(insertSql);
        refreshAll(job);
        assertLv(expectedLv);
    }

    // TTL measures retention against the lower of the newest row and the wall clock, so the
    // clock moves past the data first. The apply of the day-4 row then evicts day 1, which ends
    // more than 2 days before it; day 2 stays.
    private void insertRowEvictingDay1() throws Exception {
        setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
        execute("INSERT INTO base (ts, x) VALUES ('2026-01-04T00:00:01.000000Z', 41)");
    }

    private void refreshAll(LiveViewRefreshJob job) {
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }
}
