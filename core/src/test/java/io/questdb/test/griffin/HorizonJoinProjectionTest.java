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

package io.questdb.test.griffin;

import io.questdb.MessageBus;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinProjectionRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinProjectionRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class HorizonJoinProjectionTest extends AbstractCairoTest {
    // 50 offsets, one second apart: with two right-hand tables, the most slots per master row the
    // parallel factory takes.
    private static final String MANY_OFFSETS = " RANGE FROM 0s TO 49s STEP 1s AS h";
    // Parallel HORIZON JOIN off, then on. Every row-level assertion runs in both modes and expects
    // the same rows in the same order.
    private static final boolean[] MODES = {false, true};
    // SELECT t.ts, t.sym, h.offset, q.bid ... LIST (-1s, 0s, 1s) over createTradesAndQuotes().
    private static final String ROWS_IN_MASTER_ORDER = """
            ts\tsym\toffset\tbid
            2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
            2026-01-01T00:00:00.000000Z\tA\t0\t10.0
            2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
            2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
            2026-01-01T00:00:00.000000Z\tA\t0\t10.0
            2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
            2026-01-01T00:00:02.000000Z\tA\t-1000000\t10.0
            2026-01-01T00:00:02.000000Z\tA\t0\t10.0
            2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0
            2026-01-01T00:00:02.000000Z\tB\t-1000000\t20.0
            2026-01-01T00:00:02.000000Z\tB\t0\t20.0
            2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0
            2026-01-01T00:00:02.000000Z\tC\t-1000000\tnull
            2026-01-01T00:00:02.000000Z\tC\t0\tnull
            2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
            2026-01-01T00:00:02.000000Z\tC\t-1000000\tnull
            2026-01-01T00:00:02.000000Z\tC\t0\tnull
            2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
            """;

    @Override
    public void setUp() {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testAggregateSubQueryKeepsUnselectedGroupingKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT n FROM (
                            SELECT t.sym, count() AS n FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        ) ORDER BY n
                        """).expectSize().returns("n\n2\n4\n6\n");
                assertQuery("""
                        SELECT count() FROM (
                            SELECT t.sym, h.offset, avg(q.bid) FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("count\n6\n");
                assertQuery("""
                        SELECT count() FROM (
                            SELECT count() FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("count\n1\n");
            }
        });
    }

    @Test
    public void testConstantProjectionAndEarlyClose() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT alloc(1024) AS n FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h LIMIT 2
                        """).noRandomAccess().sizeMayVary().returns("n\n42\n42\n");
                assertQuery("""
                        SELECT count() FROM (
                            SELECT 1 AS n FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("count\n12\n");
            }
        });
    }

    @Test
    public void testDistinctAndExplicitGroupingStillCollapseRows() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String projection : List.of(
                        "SELECT DISTINCT t.sym, h.offset",
                        "SELECT t.sym, h.offset"
                )) {
                    String groupBy = projection.contains("DISTINCT") ? "" : " GROUP BY t.sym, h.offset";
                    assertQuery("SELECT offset / 1_000_000 AS seconds, count() AS n FROM ("
                            + projection + " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h"
                            + groupBy + ") GROUP BY seconds ORDER BY seconds")
                            .expectSize().returns("seconds\tn\n1\t3\n5\t3\n");
                }
            }
        });
    }

    @Test
    public void testEmptyInputs() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("TRUNCATE TABLE TaqQuote");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT count() AS n, count(bid) AS matches FROM (
                            SELECT q.bid FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("n\tmatches\n12\t0\n");
            }
            execute("TRUNCATE TABLE TaqTrade");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT count() FROM (
                            SELECT h.offset FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("count\n0\n");
                assertQuery("""
                        SELECT t.ts, h.offset, q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        """).noRandomAccess().expectSize().timestamp("ts").returns("ts\toffset\tbid\n");
            }
        });
    }

    @Test
    public void testMultiSlaveProjectionAndNullSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT s, count() AS n, sum(bid + ask) AS total FROM (
                            SELECT q.sym AS s, q.bid, r.ask FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (t.sym = q.sym)
                            HORIZON JOIN TaqQuote r ON (t.sym = r.sym) LIST (1s, 5s) AS h
                        ) GROUP BY s ORDER BY s
                        """).expectSize().returns("s\tn\ttotal\n\t4\tnull\nA\t6\t164.0\nB\t2\t92.0\n");
                assertQuery("""
                        SELECT s, count() AS n FROM (
                            SELECT q.sym AS s FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        ) GROUP BY s ORDER BY s
                        """).expectSize().returns("s\tn\n\t4\nA\t6\nB\t2\n");
            }
        });
    }

    @Test
    public void testPlainSelectCreatesTableWithoutTimestampForOtherSpelling() throws Exception {
        // Only the HORIZON JOIN projection resolves every spelling of the designated timestamp. An
        // ordinary virtual SELECT matches the column name exactly, so CREATE TABLE AS SELECT
        // without a TIMESTAMP clause inherits the designated timestamp only from the exact spelling.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String rows = """
                    2026-01-01T00:00:00.000000Z\t11.0
                    2026-01-01T00:00:00.000000Z\t11.0
                    2026-01-01T00:00:02.000000Z\t11.0
                    2026-01-01T00:00:02.000000Z\t21.0
                    2026-01-01T00:00:02.000000Z\t31.0
                    2026-01-01T00:00:02.000000Z\t31.0
                    """;
            execute("CREATE TABLE exact_copy AS (SELECT ts, price + 1 AS p FROM TaqTrade)");
            assertQuery("exact_copy").expectSize().timestamp("ts").returns("ts\tp\n" + rows);
            execute("CREATE TABLE other_copy AS (SELECT TS, price + 1 AS p FROM TaqTrade)");
            assertQuery("other_copy").expectSize().returns("TS\tp\n" + rows);
            assertExceptionNoLeakCheck(
                    "CREATE TABLE other_copy_by_day AS (SELECT TS, price + 1 AS p FROM TaqTrade) PARTITION BY DAY",
                    89,
                    "partitioning is possible only on tables with designated timestamps"
            );
        });
    }

    @Test
    public void testPlainSelectInsertsWithOtherTimestampSpelling() throws Exception {
        // Only the HORIZON JOIN projection resolves every spelling of the designated timestamp. An
        // ordinary virtual SELECT that spells it differently reports no designated timestamp, so
        // INSERT INTO ... SELECT without a column list copies its columns by position. The exact
        // spelling makes the second column the designated timestamp of the SELECT, which does not
        // match the first column of the target.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, ts2 TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z', 1.0),
                        ('2026-01-01T00:00:01Z', '2026-01-01T12:00:00Z', 2.0)
                    """);
            execute("CREATE TABLE tgt (ts TIMESTAMP, src_ts TIMESTAMP, p DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "INSERT INTO tgt SELECT ts2, ts, price + 1 FROM t",
                    12,
                    "designated timestamp of existing table (0) does not match designated timestamp in select query (1)"
            );
            execute("INSERT INTO tgt SELECT ts2, TS, price + 1 FROM t");
            assertQuery("tgt").expectSize().timestamp("ts").returns("""
                    ts\tsrc_ts\tp
                    2026-01-01T12:00:00.000000Z\t2026-01-01T00:00:01.000000Z\t3.0
                    2026-01-02T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\t2.0
                    """);
        });
    }

    @Test
    public void testPlainSelectKeepsTimestampOnlyForExactSpelling() throws Exception {
        // Only the HORIZON JOIN projection resolves every spelling of the designated timestamp, see
        // testProjectionKeepsTimestampInEverySpelling(). An ordinary virtual SELECT keeps the
        // designated timestamp only when the literal matches the column name exactly.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE TaqPrint (Timestamp TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(Timestamp) PARTITION BY DAY");
            execute("INSERT INTO TaqPrint SELECT ts, sym, price FROM TaqTrade");
            final String rows = """
                    2026-01-01T00:00:00.000000Z\t11.0
                    2026-01-01T00:00:00.000000Z\t11.0
                    2026-01-01T00:00:02.000000Z\t11.0
                    2026-01-01T00:00:02.000000Z\t21.0
                    2026-01-01T00:00:02.000000Z\t31.0
                    2026-01-01T00:00:02.000000Z\t31.0
                    """;
            assertQuery("SELECT ts, price + 1 AS p FROM TaqTrade").expectSize().timestamp("ts").returns("ts\tp\n" + rows);
            assertQuery("SELECT Timestamp, price + 1 AS p FROM TaqPrint").expectSize().timestamp("Timestamp")
                    .returns("Timestamp\tp\n" + rows);
            // {select list item, its output column, table and alias}
            final String[][] spellings = {
                    {"TS", "TS", "TaqTrade"},
                    {"t.TS", "TS", "TaqTrade t"},
                    {"timestamp", "timestamp", "TaqPrint"},
            };
            for (String[] spelling : spellings) {
                assertQuery("SELECT " + spelling[0] + ", price + 1 AS p FROM " + spelling[2]).expectSize()
                        .returns(spelling[1] + "\tp\n" + rows);
            }
            // The outer query reads no timestamp, so the sub-query keeps it as a hidden column for
            // the ASOF JOIN, again only under the exact spelling.
            assertQuery("SELECT m.p, x.price FROM (SELECT ts, sym, price + 1 AS p FROM TaqTrade) m ASOF JOIN TaqTrade x ON (sym)")
                    .noRandomAccess().expectSize().returns("""
                            p\tprice
                            11.0\t10.0
                            11.0\t10.0
                            11.0\t10.0
                            21.0\t20.0
                            31.0\t30.0
                            31.0\t30.0
                            """);
            assertQuery("SELECT m.p, x.price FROM (SELECT TS, sym, price + 1 AS p FROM TaqTrade) m ASOF JOIN TaqTrade x ON (sym)")
                    .fails(74, "left side of time series join has no timestamp");
        });
    }

    @Test
    public void testProjectedMarkoutPreservesTrades() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // Keep the direct aggregation as the oracle, then use the report's projection shape.
                String expected = """
                        seconds\tavgMarkoutAll\tnTotal
                        1\t2.5\t6
                        5\t4.5\t6
                        10\t4.5\t6
                        30\t4.5\t6
                        60\t4.5\t6
                        """;
                assertQuery("""
                        SELECT offset / 1_000_000 AS seconds,
                               avg((bid + ask) / 2.0 - t.price) AS avgMarkoutAll, count() AS nTotal
                        FROM (SELECT * FROM TaqTrade WHERE sym IN ('A', 'B', 'C')) t
                        HORIZON JOIN TaqQuote ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h
                        GROUP BY seconds
                        ORDER BY seconds
                        """).expectSize().returns(expected);
                assertQuery("""
                        SELECT offset / 1_000_000 AS seconds,
                               avg((bid + ask) / 2.0 - price) AS avgMarkoutAll, count() AS nTotal
                        FROM (
                            SELECT price, bid, ask, offset
                            FROM (SELECT * FROM TaqTrade WHERE sym IN ('A', 'B', 'C')) t
                            HORIZON JOIN TaqQuote ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h
                        )
                        GROUP BY seconds
                        ORDER BY seconds
                        """).expectSize().returns(expected);
                // The same filter at the join level lets the parallel path steal it.
                assertQuery("""
                        SELECT offset / 1_000_000 AS seconds,
                               avg((bid + ask) / 2.0 - price) AS avgMarkoutAll, count() AS nTotal
                        FROM (
                            SELECT price, bid, ask, offset
                            FROM TaqTrade t
                            HORIZON JOIN TaqQuote ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h
                            WHERE t.sym IN ('A', 'B', 'C')
                        )
                        GROUP BY seconds
                        ORDER BY seconds
                        """).expectSize().withPlanContaining(projectionPlan(isParallel, 5)).returns(expected);
            }
        });
    }

    @Test
    public void testProjectionAsMasterOfAsOfJoin() throws Exception {
        // The projection keeps the master's designated timestamp in ascending order, so a time
        // series join can use it as its left side.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT m.ts, m.sym, m.offset, m.bid, x.ask
                        FROM (
                            SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (0s, 2s) AS h
                        ) m
                        ASOF JOIN TaqQuote x ON (sym)
                        """).noRandomAccess().expectSize().timestamp("ts").returns("""
                        ts\tsym\toffset\tbid\task
                        2026-01-01T00:00:00.000000Z\tA\t0\t10.0\t12.0
                        2026-01-01T00:00:00.000000Z\tA\t2000000\t10.0\t12.0
                        2026-01-01T00:00:00.000000Z\tA\t0\t10.0\t12.0
                        2026-01-01T00:00:00.000000Z\tA\t2000000\t10.0\t12.0
                        2026-01-01T00:00:02.000000Z\tA\t0\t10.0\t12.0
                        2026-01-01T00:00:02.000000Z\tA\t2000000\t14.0\t12.0
                        2026-01-01T00:00:02.000000Z\tB\t0\t20.0\t22.0
                        2026-01-01T00:00:02.000000Z\tB\t2000000\t22.0\t22.0
                        2026-01-01T00:00:02.000000Z\tC\t0\tnull\tnull
                        2026-01-01T00:00:02.000000Z\tC\t2000000\tnull\tnull
                        2026-01-01T00:00:02.000000Z\tC\t0\tnull\tnull
                        2026-01-01T00:00:02.000000Z\tC\t2000000\tnull\tnull
                        """);
            }
        });
    }

    @Test
    public void testProjectionCloseSkipsUnstartedTasks() throws Exception {
        // A LIMIT closes the cursor on the second frame while the queue holds the tasks of the
        // frames that follow. The cursor must cancel those instead of filtering and matching
        // their rows.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        assertMemoryLeak(() -> {
            createRandomTradesAndQuotes(1_500, 6_000, 10);
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            try (RecordCursorFactory factory = select(
                    "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS + " WHERE test_fault() LIMIT 70"
            )) {
                assertProjectionFactory(factory, true);
                for (int i = 0; i < 3; i++) {
                    // A frame holds one trade and its 50 rows. The filter fails on its eleventh
                    // call, which only the task of a later frame can make.
                    TestFaultFunctionFactory.armToFailAfter(10);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        int rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(70, rows);
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                    Assert.assertEquals(0, TestFaultFunctionFactory.faultsTriggered());
                }
            }
        });
    }

    @Test
    public void testProjectionColumnListDoesNotChangeCounts() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String projection : List.of("h.offset", "t.sym, h.offset", "t.price, h.offset", "q.bid, h.offset", "t.price, q.bid, q.ask, h.offset")) {
                    assertQuery("SELECT offset / 1_000_000 AS seconds, count() AS n FROM (SELECT "
                            + projection + " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h)"
                            + " GROUP BY seconds ORDER BY seconds")
                            .expectSize().returns("seconds\tn\n1\t6\n5\t6\n");
                    assertQuery("SELECT count() FROM (SELECT " + projection
                            + " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h)")
                            .noRandomAccess().expectSize().returns("count\n12\n");
                }
            }
        });
    }

    @Test
    public void testProjectionCoveringIndexMasterAboveSlotCap() throws Exception {
        // Past 100 slots per master row the serial factory runs the projection, but it reads its
        // master with random access, which a covering index scan lacks. Such a master stays on the
        // parallel factory. A residual filter puts an async filter, which has random access, on
        // top of the covering index scan, and that master moves to the serial factory.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE plain (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE)
                    TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO plain
                    SELECT '2026-01-01'::TIMESTAMP + x * 1_000_000L, rnd_symbol('A', 'B', 'C'), x::DOUBLE
                    FROM long_sequence(3_000)
                    """);
            execute("INSERT INTO cov SELECT * FROM plain");
            execute("""
                    INSERT INTO quotes
                    SELECT '2026-01-01'::TIMESTAMP + x * 500_000L, rnd_symbol('A', 'B', 'C'), x::DOUBLE
                    FROM long_sequence(6_000)
                    """);
            engine.releaseAllWriters();
            final StringSink expected = new StringSink();
            final StringSink actual = new StringSink();
            for (String keys : new String[]{"t.sym IN ('A', 'B')", "t.sym = 'A'"}) {
                for (String residualFilter : new String[]{"", " AND t.px > 1_000"}) {
                    final String select = "SELECT t.px, h.offset, q.bid FROM ";
                    final String join = " t HORIZON JOIN quotes q ON (sym) RANGE FROM 0s TO 100s STEP 1s AS h WHERE " + keys + residualFilter;
                    // The serial factory does not accept a covering index scan as its master.
                    sqlExecutionContext.setParallelHorizonJoinEnabled(false);
                    try (RecordCursorFactory factory = select(select + "plain" + join)) {
                        assertProjectionFactory(factory, false);
                        printRows(factory, sqlExecutionContext, expected);
                    }
                    sqlExecutionContext.setParallelHorizonJoinEnabled(true);
                    final String sql = select + "cov" + join;
                    try (RecordCursorFactory factory = select(sql)) {
                        assertProjectionFactory(factory, residualFilter.isEmpty());
                        TestUtils.assertContains(explain(engine, sqlExecutionContext, sql), "CoveringIndex on: sym");
                        printRows(factory, sqlExecutionContext, actual);
                        TestUtils.assertEquals(sql, expected, actual);
                    }
                }
            }
        });
    }

    @Test
    public void testProjectionCursorRewindAndSize() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final StringSink firstPass = new StringSink();
            final StringSink secondPass = new StringSink();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // Unfiltered: the size is known up front. Filtered: it is known only after a scan.
                assertRewindAndSize(
                        "SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h",
                        18,
                        true,
                        firstPass,
                        secondPass
                );
                assertRewindAndSize(
                        "SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h WHERE t.price > 15",
                        9,
                        false,
                        firstPass,
                        secondPass
                );
                assertRewindAndSize(
                        "SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h WHERE concat(t.sym, '') = 'A'",
                        9,
                        false,
                        firstPass,
                        secondPass
                );
            }
        });
    }

    @Test
    public void testProjectionDuplicateSlaveTimestamps() throws Exception {
        // ASOF picks the last of several slave rows sharing the matched timestamp, exactly as the
        // ASOF JOIN does.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades VALUES
                        (1, '2026-01-01T00:00:01Z', 'A'),
                        (2, '2026-01-01T00:00:01Z', 'B'),
                        (3, '2026-01-01T00:00:02Z', 'A')
                    """);
            execute("""
                    INSERT INTO quotes VALUES
                        ('2026-01-01T00:00:01Z', 'A', 1),
                        ('2026-01-01T00:00:01Z', 'B', 2),
                        ('2026-01-01T00:00:01Z', 'A', 3),
                        ('2026-01-01T00:00:02Z', 'A', 4),
                        ('2026-01-01T00:00:02Z', 'A', 5)
                    """);
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertMatchesAsOfJoin("trades", "quotes", "ON (sym)", "t.id, q.bid");
                assertMatchesAsOfJoin("trades", "quotes", "", "t.id, q.bid");
            }
        });
    }

    @Test
    public void testProjectionFactoryReopensAfterFunctionFailure() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                try (RecordCursorFactory factory = select("""
                        SELECT alloc(1024) AS n, test_fault() AS is_ok FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h LIMIT 2
                        """)) {
                    assertProjectionFactory(factory, isParallel);
                    TestFaultFunctionFactory.armToFailAfterInits(0);
                    try {
                        factory.getCursor(sqlExecutionContext).close();
                        Assert.fail("expected injected init failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected init failure");
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                    TestFaultFunctionFactory.armToFailAfter(1);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        while (cursor.hasNext()) {
                            cursor.getRecord().getBool(1);
                        }
                        Assert.fail("expected injected read failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected failure");
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                            .noRandomAccess().sizeMayVary().returns("n\tis_ok\n42\ttrue\n42\ttrue\n");
                }
            }
        });
    }

    @Test
    public void testProjectionFilterFailureRecovers() throws Exception {
        // In parallel mode the master filter runs on the reduce side, so the failure travels
        // through the task back to the reading thread.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String sql = """
                    SELECT t.sym, h.offset, q.bid FROM TaqTrade t
                    HORIZON JOIN TaqQuote q ON (sym) LIST (0s, 1s) AS h
                    WHERE test_fault()
                    """;
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                try (RecordCursorFactory factory = select(sql)) {
                    assertProjectionFactory(factory, isParallel);
                    for (int failAfter = 0; failAfter < 6; failAfter++) {
                        TestFaultFunctionFactory.armToFailAfter(failAfter);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected injected failure after " + failAfter + " rows");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected failure");
                        } finally {
                            TestFaultFunctionFactory.disarm();
                        }
                    }
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                            .noRandomAccess().returns("""
                                    sym\toffset\tbid
                                    A\t0\t10.0
                                    A\t1000000\t10.0
                                    A\t0\t10.0
                                    A\t1000000\t10.0
                                    A\t0\t10.0
                                    A\t1000000\t14.0
                                    B\t0\t20.0
                                    B\t1000000\t22.0
                                    C\t0\tnull
                                    C\t1000000\tnull
                                    C\t0\tnull
                                    C\t1000000\tnull
                                    """);
                }
            }
        });
    }

    @Test
    public void testProjectionFilterSubQueryKeepsContextFrameSizes() throws Exception {
        // The parallel factory opens the sub-query of the stolen filter while it prepares the
        // master frames. The frame sizes it picks for the master must not reach that sub-query,
        // or 100 offsets would cut the scan of big into 200 frames of 1,000 rows. Past 100 slots
        // the serial factory leaves the filter, and its sub-query, on the master.
        setProductionPageFrameSizes();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE big (ts TIMESTAMP, sym SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades
                    SELECT '2026-01-01'::TIMESTAMP + x * 100_000_000L, rnd_symbol('A', 'B', 'C'), x::DOUBLE
                    FROM long_sequence(100)
                    """);
            execute("""
                    INSERT INTO quotes
                    SELECT '2026-01-01'::TIMESTAMP + x * 20_000_000L, rnd_symbol('A', 'B', 'C'), x::DOUBLE
                    FROM long_sequence(1_000)
                    """);
            execute("""
                    INSERT INTO big
                    SELECT '2026-01-01'::TIMESTAMP + x * 100_000L, rnd_symbol('A', 'B', 'Z'), rnd_double()
                    FROM long_sequence(200_000)
                    """);
            // A context created at this point takes the production sizes as its defaults.
            try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine, 1)) {
                final StringSink serial = new StringSink();
                final StringSink parallel = new StringSink();
                for (int offsetCount : new int[]{2, 100, 1_000}) {
                    final String sql = "SELECT t.ts, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) RANGE FROM 0s TO "
                            + (offsetCount - 1) + "s STEP 1s AS h WHERE t.sym IN (SELECT sym FROM big WHERE v > 0.5)";
                    context.setParallelHorizonJoinEnabled(false);
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        assertProjectionFactory(factory, false);
                        printRows(factory, context, serial);
                    }
                    context.setParallelHorizonJoinEnabled(true);
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        assertProjectionFactory(factory, offsetCount <= 100);
                        // One task reduces the 100 trades and one scans big, whose 200,000 rows
                        // make a single frame of the default size.
                        Assert.assertEquals(sql, 2, drainAndCountReduceTasks(engine, factory, context));
                        try (RecordCursor ignore = factory.getCursor(context)) {
                            assertProductionPageFrameSizes(context);
                        }
                        printRows(factory, context, parallel);
                    }
                    TestUtils.assertEquals(sql, serial, parallel);
                }

                // A failed open restores the context's frame sizes as well.
                context.setParallelHorizonJoinEnabled(true);
                final String oneHundredOffsets = "SELECT t.ts, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) "
                        + "RANGE FROM 0s TO 99s STEP 1s AS h WHERE t.sym IN (SELECT sym FROM big WHERE v > 0.5)";
                try (RecordCursorFactory factory = engine.select(oneHundredOffsets + " AND test_fault()", context)) {
                    assertProjectionFactory(factory, true);
                    TestFaultFunctionFactory.armToFailAfterInits(0);
                    try {
                        factory.getCursor(context).close();
                        Assert.fail("expected injected init failure");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected init failure");
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                    assertProductionPageFrameSizes(context);

                    // The master's page frame cursor itself refuses to open.
                    execute("ALTER TABLE trades ADD COLUMN extra INT");
                    try {
                        factory.getCursor(context).close();
                        Assert.fail("expected an out-of-date table reference");
                    } catch (TableReferenceOutOfDateException ignore) {
                    }
                    assertProductionPageFrameSizes(context);
                }

                // The context gets back the sizes it had on entry, which a caller may have changed.
                try (RecordCursorFactory factory = engine.select(oneHundredOffsets, context)) {
                    assertProjectionFactory(factory, true);
                    context.changePageFrameSizes(1_000, 50_000);
                    drainAndCountReduceTasks(engine, factory, context);
                    Assert.assertEquals(1_000, context.getPageFrameMinRows());
                    Assert.assertEquals(50_000, context.getPageFrameMaxRows());
                } finally {
                    context.restoreToDefaultPageFrameSizes();
                }
            }
        });
    }

    @Test
    public void testProjectionFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String head = "SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h ";
            final String bcRows = """
                    ts\tsym\toffset\tbid
                    2026-01-01T00:00:02.000000Z\tB\t-1000000\t20.0
                    2026-01-01T00:00:02.000000Z\tB\t0\t20.0
                    2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0
                    2026-01-01T00:00:02.000000Z\tC\t-1000000\tnull
                    2026-01-01T00:00:02.000000Z\tC\t0\tnull
                    2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                    2026-01-01T00:00:02.000000Z\tC\t-1000000\tnull
                    2026-01-01T00:00:02.000000Z\tC\t0\tnull
                    2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                    """;
            final String aRows = """
                    ts\tsym\toffset\tbid
                    2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                    2026-01-01T00:00:00.000000Z\tA\t0\t10.0
                    2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
                    2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                    2026-01-01T00:00:00.000000Z\tA\t0\t10.0
                    2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
                    2026-01-01T00:00:02.000000Z\tA\t-1000000\t10.0
                    2026-01-01T00:00:02.000000Z\tA\t0\t10.0
                    2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0
                    """;
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // JIT-compiled filter.
                assertQuery(head + "WHERE t.price > 15").noRandomAccess().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3)).returns(bcRows);
                // Java filter that is not thread-safe, so each worker gets a clone.
                assertQuery(head + "WHERE concat(t.sym, '') = 'A'").noRandomAccess().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3)).returns(aRows);
                // Bind variable.
                bindVariableService.clear();
                bindVariableService.setStr("sym", "A");
                assertQuery(head + "WHERE t.sym = :sym").noRandomAccess().timestamp("ts").returns(aRows);
                bindVariableService.clear();
                // Nothing passes.
                assertQuery(head + "WHERE t.price > 100").noRandomAccess().timestamp("ts")
                        .returns("ts\tsym\toffset\tbid\n");
                // An interval on the designated timestamp prunes the scan instead of filtering it.
                // The parallel cursor then knows its size from the frames; the serial cursor
                // takes it from the interval scan, which does not report one.
                assertQuery(head + "WHERE t.ts >= '2026-01-01T00:00:01'").noRandomAccess().expectSize(isParallel).timestamp("ts")
                        .returns("""
                                ts\tsym\toffset\tbid
                                2026-01-01T00:00:02.000000Z\tA\t-1000000\t10.0
                                2026-01-01T00:00:02.000000Z\tA\t0\t10.0
                                2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0
                                """ + bcRows.substring(bcRows.indexOf('\n') + 1));
            }
        });
    }

    @Test
    public void testProjectionIndexedFiftySymbolList() throws Exception {
        // A bitmap index serves the 50 symbols with one index scan per key, which yields no page
        // frames, so the serial factory runs the projection even with parallel HORIZON JOIN
        // enabled. A covering index serves the same list with page frames, and the parallel
        // factory runs it. Both return the same rows in the same order.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trades AS (
                        SELECT timestamp_sequence('2026-01-01', 1_000) ts,
                               (x % 50)::SYMBOL sym, 10.0 price
                        FROM long_sequence(10_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("ALTER TABLE trades ALTER COLUMN sym ADD INDEX");
            execute("""
                    CREATE TABLE covered (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price), price DOUBLE)
                    TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO covered SELECT ts, sym, price FROM trades");
            execute("""
                    CREATE TABLE quotes AS (
                        SELECT '2026-01-01'::TIMESTAMP ts, (x % 50)::SYMBOL sym, 10.0 bid, 12.0 ask
                        FROM long_sequence(50)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            engine.releaseAllWriters();
            final StringSink symbols = new StringSink();
            for (int i = 0; i < 50; i++) {
                if (i > 0) {
                    symbols.put(',');
                }
                symbols.put('\'').put(i).put('\'');
            }
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            final StringSink serial = new StringSink();
            final StringSink parallel = new StringSink();
            final ObjList<String> tables = new ObjList<>("trades", "covered");
            for (int i = 0, n = tables.size(); i < n; i++) {
                final String table = tables.getQuick(i);
                final boolean isParallel = table.equals("covered");
                final String from = " FROM (SELECT * FROM " + table + " WHERE sym IN (" + symbols + ")) t"
                        + " HORIZON JOIN quotes ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h";
                assertQuery("SELECT offset / 1_000_000 AS seconds, count() AS n, avg((bid + ask) / 2.0 - price) AS markout FROM ("
                        + "SELECT price, bid, ask, offset" + from + ") GROUP BY seconds ORDER BY seconds")
                        .expectSize()
                        .withPlanContaining(projectionPlan(isParallel, 5), isParallel ? "CoveringIndex on: sym" : "FilterOnValues")
                        .returns("""
                                seconds\tn\tmarkout
                                1\t10000\t1.0
                                5\t10000\t1.0
                                10\t10000\t1.0
                                30\t10000\t1.0
                                60\t10000\t1.0
                                """);
                try (RecordCursorFactory factory = select("SELECT t.ts, t.sym, h.offset, bid" + from)) {
                    assertProjectionFactory(factory, isParallel);
                    printRows(factory, sqlExecutionContext, isParallel ? parallel : serial);
                }
            }
            TestUtils.assertEquals(serial, parallel);
        });
    }

    @Test
    public void testProjectionKeepsTimestampInEverySpelling() throws Exception {
        // The master's designated timestamp stays the designated timestamp of the projection in
        // every spelling that resolves to it: with or without the table alias, in any letter case,
        // and under a column alias. TaqBid names its timestamp bid_ts, so a bare ts is not ambiguous.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE TaqBid (bid_ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(bid_ts) PARTITION BY DAY");
            execute("INSERT INTO TaqBid SELECT ts, sym, bid FROM TaqQuote");
            execute("CREATE TABLE TaqPrint (Timestamp TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(Timestamp) PARTITION BY DAY");
            execute("INSERT INTO TaqPrint SELECT ts, sym, price FROM TaqTrade");
            // One row per trade and offset: the trade's timestamp and the bid at the horizon.
            final String rows = """
                    2026-01-01T00:00:00.000000Z\tnull
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:00.000000Z\tnull
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:02.000000Z\t10.0
                    2026-01-01T00:00:02.000000Z\t10.0
                    2026-01-01T00:00:02.000000Z\t14.0
                    2026-01-01T00:00:02.000000Z\t20.0
                    2026-01-01T00:00:02.000000Z\t20.0
                    2026-01-01T00:00:02.000000Z\t22.0
                    2026-01-01T00:00:02.000000Z\tnull
                    2026-01-01T00:00:02.000000Z\tnull
                    2026-01-01T00:00:02.000000Z\tnull
                    2026-01-01T00:00:02.000000Z\tnull
                    2026-01-01T00:00:02.000000Z\tnull
                    2026-01-01T00:00:02.000000Z\tnull
                    """;
            // Each row's bid next to the price of the latest trade of its symbol.
            final String asOfJoinRows = """
                    bid\tprice
                    null\t10.0
                    10.0\t10.0
                    10.0\t10.0
                    null\t10.0
                    10.0\t10.0
                    10.0\t10.0
                    10.0\t10.0
                    10.0\t10.0
                    14.0\t10.0
                    20.0\t20.0
                    20.0\t20.0
                    22.0\t20.0
                    null\t30.0
                    null\t30.0
                    null\t30.0
                    null\t30.0
                    null\t30.0
                    null\t30.0
                    """;
            // {select list item, its output column, master table and alias}
            final String[][] spellings = {
                    {"t.ts", "ts", "TaqTrade t"},
                    {"ts", "ts", "TaqTrade t"},
                    {"TS", "TS", "TaqTrade t"},
                    {"T.ts", "ts", "TaqTrade t"},
                    {"t.TS", "TS", "TaqTrade t"},
                    {"ts AS x", "x", "TaqTrade t"},
                    {"taqtrade.TS", "TS", "TaqTrade"},
                    {"t.timestamp", "timestamp", "TaqPrint t"},
            };
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String[] spelling : spellings) {
                    final String column = spelling[1];
                    final String masterAlias = spelling[2].substring(spelling[2].lastIndexOf(' ') + 1);
                    final String from = " FROM " + spelling[2] + " HORIZON JOIN TaqBid q ON (sym) LIST (-1s, 0s, 1s) AS h";
                    final String projection = "SELECT " + spelling[0] + ", q.bid" + from;
                    try (RecordCursorFactory factory = select(projection)) {
                        assertProjectionFactory(factory, isParallel);
                    }
                    assertQuery(projection).noRandomAccess().expectSize().timestamp(column)
                            .returns(column + "\tbid\n" + rows);
                    assertQuery(projection + " ORDER BY " + column).noRandomAccess().expectSize().timestamp(column)
                            .withPlanNotContaining("sort", "Sort").returns(column + "\tbid\n" + rows);
                    assertQuery("SELECT " + column + ", count() AS n, sum(bid) AS bids FROM (" + projection + ") SAMPLE BY 1s")
                            .noRandomAccess().timestamp(column).returns(column + """
                                    \tn\tbids
                                    2026-01-01T00:00:00.000000Z\t6\t40.0
                                    2026-01-01T00:00:02.000000Z\t12\t96.0
                                    """);
                    // The outer query reads no timestamp, so the projection keeps it as a hidden
                    // column for the ASOF JOIN.
                    assertQuery("SELECT m.bid, x.price FROM (SELECT " + spelling[0] + ", " + masterAlias + ".sym, q.bid" + from + ") m ASOF JOIN TaqTrade x ON (sym)")
                            .noRandomAccess().expectSize().returns(asOfJoinRows);
                }
            }
        });
    }

    @Test
    public void testProjectionKeyTypesMatchAsOfJoin() throws Exception {
        // Each join key shape must pick exactly the rows the ASOF JOIN picks for the same key.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trades (
                        id LONG, ts TIMESTAMP, sym SYMBOL, venue SYMBOL, i INT, s STRING, v VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY HOUR
                    """);
            execute("""
                    CREATE TABLE quotes (
                        ts TIMESTAMP, sym SYMBOL, venue SYMBOL, i INT, s STRING, v VARCHAR, bid DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR
                    """);
            execute("""
                    INSERT INTO trades
                    SELECT x, '2026-01-01'::TIMESTAMP + x * 28_333_333L,
                           CASE WHEN x % 7 = 0 THEN NULL ELSE rnd_symbol('A', 'B', 'C', 'Z') END,
                           rnd_symbol('X', 'Y'),
                           CASE WHEN x % 6 = 0 THEN NULL ELSE rnd_int(0, 4, 0) END,
                           CASE WHEN x % 5 = 0 THEN NULL ELSE rnd_str('A', 'B', 'C', 'Z') END,
                           CASE WHEN x % 4 = 0 THEN NULL ELSE rnd_varchar('A', 'B', 'C', 'Z') END
                    FROM long_sequence(300)
                    """);
            execute("""
                    INSERT INTO quotes
                    SELECT '2026-01-01'::TIMESTAMP + x * 16_666_666L,
                           CASE WHEN x % 9 = 0 THEN NULL ELSE rnd_symbol('A', 'B', 'C') END,
                           rnd_symbol('X', 'Y', 'W'),
                           CASE WHEN x % 8 = 0 THEN NULL ELSE rnd_int(0, 3, 0) END,
                           CASE WHEN x % 7 = 0 THEN NULL ELSE rnd_str('A', 'B', 'C') END,
                           CASE WHEN x % 6 = 0 THEN NULL ELSE rnd_varchar('A', 'B', 'C') END,
                           rnd_double()
                    FROM long_sequence(600)
                    """);
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String on : List.of(
                        "ON (sym)",
                        "ON (t.sym = q.sym AND t.venue = q.venue)",
                        "ON (i)",
                        "ON (s)",
                        "ON (v)",
                        "ON (t.sym = q.s)",
                        "ON (t.s = q.v)",
                        "ON (t.v = q.sym)",
                        ""
                )) {
                    assertMatchesAsOfJoin("trades", "quotes", on, "t.id, q.bid, q.ts");
                }
            }
        });
    }

    @Test
    public void testProjectionLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String head = "SELECT t.ts, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h ";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery(head + "LIMIT 4").noRandomAccess().expectSize().timestamp("ts").returns("""
                        ts\toffset\tbid
                        2026-01-01T00:00:00.000000Z\t-1000000\tnull
                        2026-01-01T00:00:00.000000Z\t0\t10.0
                        2026-01-01T00:00:00.000000Z\t1000000\t10.0
                        2026-01-01T00:00:00.000000Z\t-1000000\tnull
                        """);
                assertQuery(head + "LIMIT 5, 8").noRandomAccess().expectSize().timestamp("ts").returns("""
                        ts\toffset\tbid
                        2026-01-01T00:00:00.000000Z\t1000000\t10.0
                        2026-01-01T00:00:02.000000Z\t-1000000\t10.0
                        2026-01-01T00:00:02.000000Z\t0\t10.0
                        """);
                assertQuery(head + "LIMIT -4").noRandomAccess().expectSize().timestamp("ts").returns("""
                        ts\toffset\tbid
                        2026-01-01T00:00:02.000000Z\t1000000\tnull
                        2026-01-01T00:00:02.000000Z\t-1000000\tnull
                        2026-01-01T00:00:02.000000Z\t0\tnull
                        2026-01-01T00:00:02.000000Z\t1000000\tnull
                        """);
            }
        });
    }

    @Test
    public void testProjectionManyOffsetsKeepFilterFrames() throws Exception {
        // Past 100 slots per master row, offsets times right-hand tables, the serial factory runs
        // the projection and the master's async filter scans frames of its usual size: a selective
        // filter at 7,201 offsets dispatches the reduce tasks of the filter on its own. Up to 100
        // slots the parallel factory cuts the master into frames of the small frame budget divided
        // by the slots, so a scan makes at most 100 times the frames of a small-frame scan.
        setProductionPageFrameSizes();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            // One partition of 200,000 trades, 100ms apart; four of them cost 5.
            execute("""
                    INSERT INTO trades
                    SELECT '2026-01-01'::TIMESTAMP + x * 100_000L, rnd_symbol('A', 'B', 'C'), (x % 50_000)::DOUBLE
                    FROM long_sequence(200_000)
                    """);
            execute("""
                    INSERT INTO quotes
                    SELECT '2026-01-01'::TIMESTAMP + x * 20_000_000L, rnd_symbol('A', 'B', 'C'), x::DOUBLE
                    FROM long_sequence(1_000)
                    """);
            // A context created at this point takes the production sizes as its defaults.
            try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine, 1)) {
                context.setParallelHorizonJoinEnabled(true);
                final long filterTaskCount;
                try (RecordCursorFactory factory = engine.select("SELECT ts, price FROM trades WHERE price = 5", context)) {
                    filterTaskCount = drainAndCountReduceTasks(engine, factory, context);
                }
                final String select = "SELECT t.ts, t.price, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) RANGE FROM 0s TO ";
                final String manyOffsets = select + "7200s STEP 1s AS h WHERE t.price = 5";
                try (RecordCursorFactory factory = engine.select(manyOffsets, context)) {
                    assertProjectionFactory(factory, false);
                    Assert.assertEquals(manyOffsets, filterTaskCount, drainAndCountReduceTasks(engine, factory, context));
                }
                try (RecordCursorFactory factory = engine.select(manyOffsets + " LIMIT 10", context)) {
                    assertProjectionFactory(factory, false);
                    final long taskCount = drainAndCountReduceTasks(engine, factory, context);
                    Assert.assertTrue("filter tasks: " + filterTaskCount + ", tasks: " + taskCount, taskCount <= filterTaskCount);
                }
                // 100 offsets cut the trades into frames of 1,000 rows, 100 times the two frames of
                // the small frame budget.
                Assert.assertEquals(200, assertMatchesSerialAndCountReduceTasks(context, select + "99s STEP 1s AS h WHERE t.price = 5"));
            }
        });
    }

    @Test
    public void testProjectionManyOffsetsMatchFusedAggregate() throws Exception {
        // 100 offsets per trade, the most the parallel factory takes with one right-hand table: the
        // parallel task budget shrinks to few rows per task.
        assertMemoryLeak(() -> {
            createRandomTradesAndQuotes(2_000, 20_000, 20);
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                final StringSink fused = new StringSink();
                printSql("""
                        SELECT h.offset, count() n, count(q.bid) matched, sum(q.bid) bids
                        FROM trades t HORIZON JOIN quotes q ON (sym) RANGE FROM -50s TO 49s STEP 1s AS h
                        ORDER BY h.offset
                        """, fused);
                assertQuery("""
                        SELECT offset, count() n, count(bid) matched, sum(bid) bids
                        FROM (
                            SELECT h.offset, q.bid FROM trades t
                            HORIZON JOIN quotes q ON (sym) RANGE FROM -50s TO 49s STEP 1s AS h
                        )
                        ORDER BY offset
                        """).expectSize().withPlanContaining(projectionPlan(isParallel, 100)).returns(fused.toString());
            }
        });
    }

    @Test
    public void testProjectionManyOffsetsMatchSerial() throws Exception {
        // 50 offsets leave a task budget of one master row and master frames of one row; the two
        // right-hand tables of the last query make the 100 slots the parallel factory takes at
        // most.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createRandomTradesAndQuotes(1_500, 6_000, 10);
            assertManyOffsetsParallelMatchesSerial(engine, sqlExecutionContext);

            // A filter failure travels from a task back to the reader; the factory then reads the
            // rows again.
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            try (RecordCursorFactory factory = select("SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS + " WHERE test_fault()")) {
                assertProjectionFactory(factory, true);
                for (int failAfter = 0; failAfter < 4; failAfter++) {
                    TestFaultFunctionFactory.armToFailAfter(failAfter);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("expected injected failure after " + failAfter + " rows");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected failure");
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                }
                // The filter fails to initialize after the factory has opened the master frames.
                TestFaultFunctionFactory.armToFailAfterInits(0);
                try {
                    factory.getCursor(sqlExecutionContext).close();
                    Assert.fail("expected injected init failure");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected init failure");
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    long rowCount = 0;
                    while (cursor.hasNext()) {
                        rowCount++;
                    }
                    Assert.assertEquals(75_000, rowCount);
                }
            }

            // The page frame cursor cannot split a Parquet row group, so a task matches the first
            // row of a row group and the reading thread matches the rest.
            execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertManyOffsetsParallelMatchesSerial(engine, sqlExecutionContext);
        });
    }

    @Test
    public void testProjectionManyOffsetsWorkerPoolMatchesSerial() throws Exception {
        // Same frame shape as testProjectionManyOffsetsMatchSerial(), across real workers.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        createRandomTradesAndQuotes(engine, context, 1_500, 6_000, 10);
                        assertManyOffsetsParallelMatchesSerial(engine, context);
                        // A LIMIT closes the cursor while tasks are still in flight.
                        context.setParallelHorizonJoinEnabled(true);
                        try (RecordCursorFactory factory = engine.select(
                                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS + " LIMIT 70",
                                context
                        )) {
                            assertProjectionFactory(factory, true);
                            for (int i = 0; i < 10; i++) {
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    int rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals(70, rows);
                                }
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testProjectionMasterFilterAndNegativeOffsets() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT offset / 1_000_000 AS seconds, count() AS n, count(bid) AS matches FROM (
                            SELECT h.offset, q.bid FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) RANGE FROM -3s TO 1s STEP 2s AS h
                            WHERE concat(t.sym, '') = 'A'
                        ) GROUP BY seconds ORDER BY seconds
                        """).expectSize().returns("seconds\tn\tmatches\n-3\t3\t0\n-1\t3\t1\n1\t3\t3\n");
            }
        });
    }

    @Test
    public void testProjectionMixedTimestampTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE nanoTrades AS (SELECT ts::TIMESTAMP_NS ts, sym, price FROM TaqTrade) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE nanoQuotes AS (SELECT ts::TIMESTAMP_NS ts, sym, bid, ask FROM TaqQuote) TIMESTAMP(ts) PARTITION BY DAY");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String trades : List.of("TaqTrade", "nanoTrades")) {
                    for (String quotes : List.of("TaqQuote", "nanoQuotes")) {
                        long divisor = trades.equals("TaqTrade") ? 1_000_000L : 1_000_000_000L;
                        assertQuery("SELECT offset / " + divisor + " AS seconds, count() AS n, sum(bid) AS total FROM ("
                                + "SELECT h.offset, q.bid FROM " + trades + " t HORIZON JOIN " + quotes
                                + " q ON (sym) LIST (1s, 5s) AS h) GROUP BY seconds ORDER BY seconds")
                                .expectSize().returns("seconds\tn\ttotal\n1\t6\t56.0\n5\t6\t64.0\n");
                    }
                }
                // The horizon timestamp keeps the master's precision.
                assertQuery("""
                        SELECT t.ts, h.timestamp, q.bid FROM nanoTrades t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (1s) AS h LIMIT 3
                        """).noRandomAccess().expectSize().timestamp("ts").returns("""
                        ts\ttimestamp\tbid
                        2026-01-01T00:00:00.000000000Z\t2026-01-01T00:00:01.000000000Z\t10.0
                        2026-01-01T00:00:00.000000000Z\t2026-01-01T00:00:01.000000000Z\t10.0
                        2026-01-01T00:00:02.000000000Z\t2026-01-01T00:00:03.000000000Z\t14.0
                        """);
            }
        });
    }

    @Test
    public void testProjectionMultiSlaveRows() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE TaqPrint (ts TIMESTAMP, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO TaqPrint VALUES ('2026-01-01T00:00:01Z', 100), ('2026-01-01T00:00:03Z', 300)");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT t.ts, t.sym, h.offset, q.bid, p.px FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym)
                        HORIZON JOIN TaqPrint p LIST (0s, 2s) AS h
                        """).noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 2)).returns("""
                                ts\tsym\toffset\tbid\tpx
                                2026-01-01T00:00:00.000000Z\tA\t0\t10.0\tnull
                                2026-01-01T00:00:00.000000Z\tA\t2000000\t10.0\t100.0
                                2026-01-01T00:00:00.000000Z\tA\t0\t10.0\tnull
                                2026-01-01T00:00:00.000000Z\tA\t2000000\t10.0\t100.0
                                2026-01-01T00:00:02.000000Z\tA\t0\t10.0\t100.0
                                2026-01-01T00:00:02.000000Z\tA\t2000000\t14.0\t300.0
                                2026-01-01T00:00:02.000000Z\tB\t0\t20.0\t100.0
                                2026-01-01T00:00:02.000000Z\tB\t2000000\t22.0\t300.0
                                2026-01-01T00:00:02.000000Z\tC\t0\tnull\t100.0
                                2026-01-01T00:00:02.000000Z\tC\t2000000\tnull\t300.0
                                2026-01-01T00:00:02.000000Z\tC\t0\tnull\t100.0
                                2026-01-01T00:00:02.000000Z\tC\t2000000\tnull\t300.0
                                """);
            }
        });
    }

    @Test
    public void testProjectionNullSlaveColumnsMatchAsOfJoin() throws Exception {
        // An unmatched slave row reads as typed NULLs, the same values an unmatched ASOF JOIN row
        // reads, for every column type.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    CREATE TABLE typed (
                        ts TIMESTAMP, sym SYMBOL, b BOOLEAN, bt BYTE, sh SHORT, ch CHAR, i INT, l LONG,
                        f FLOAT, d DOUBLE, dt DATE, tsx TIMESTAMP, tag SYMBOL, str STRING, vc VARCHAR,
                        u UUID, l256 LONG256, ip IPV4, g1 GEOHASH(1c), g2 GEOHASH(2c), g4 GEOHASH(4c),
                        g8 GEOHASH(8c), dec DECIMAL(18, 3), arr DOUBLE[], bin BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO trades VALUES
                        (1, '2026-01-01T00:00:00Z', 'A'),
                        (2, '2026-01-01T00:00:01Z', 'A'),
                        (3, '2026-01-01T00:00:01Z', 'B')
                    """);
            execute("""
                    INSERT INTO typed VALUES (
                        '2026-01-01T00:00:01Z', 'A', true, 1, 2, 'c', 3, 4, 5.5, 6.5, '2026-01-02'::DATE,
                        '2026-01-03T00:00:00Z', 'tag1', 'str', 'vc', '11111111-1111-1111-1111-111111111111',
                        1::LONG256, '1.2.3.4', #u, #u3, #u33g, #u33gzzzz, 12.345m, ARRAY[1.0, 2.0],
                        rnd_bin(4, 4, 0)
                    )
                    """);
            final String columns = """
                    t.id, q.b, q.bt, q.sh, q.ch, q.i, q.l, q.f, q.d, q.dt, q.tsx, q.tag, q.str, q.vc, q.u,
                    q.l256, q.ip, q.g1, q.g2, q.g4, q.g8, q.dec, q.arr, q.bin, q.tag IS NULL AS tag_is_null
                    """;
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertMatchesAsOfJoin("trades", "typed", "ON (sym)", columns);
                assertMatchesAsOfJoin("trades", "typed", "", columns);
            }
        });
    }

    @Test
    public void testProjectionOrderByMasterTimestampNeedsNoSort() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h
                        ORDER BY t.ts
                        """).noRandomAccess().expectSize().timestamp("ts")
                        .withPlanNotContaining("sort").returns(ROWS_IN_MASTER_ORDER);
                assertQuery("""
                        SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h
                        ORDER BY t.ts DESC, t.sym DESC, h.offset DESC
                        """).expectSize().timestampDesc("ts").returns("""
                        ts\tsym\toffset\tbid
                        2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                        2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                        2026-01-01T00:00:02.000000Z\tC\t0\tnull
                        2026-01-01T00:00:02.000000Z\tC\t0\tnull
                        2026-01-01T00:00:02.000000Z\tC\t-1000000\tnull
                        2026-01-01T00:00:02.000000Z\tC\t-1000000\tnull
                        2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0
                        2026-01-01T00:00:02.000000Z\tB\t0\t20.0
                        2026-01-01T00:00:02.000000Z\tB\t-1000000\t20.0
                        2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0
                        2026-01-01T00:00:02.000000Z\tA\t0\t10.0
                        2026-01-01T00:00:02.000000Z\tA\t-1000000\t10.0
                        2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
                        2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
                        2026-01-01T00:00:00.000000Z\tA\t0\t10.0
                        2026-01-01T00:00:00.000000Z\tA\t0\t10.0
                        2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                        2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                        """);
            }
        });
    }

    @Test
    public void testProjectionOtherTimestampColumnsAreNotDesignated() throws Exception {
        // A timestamp column of a right-hand table, under a bare or a qualified name in any letter
        // case, and the horizon timestamp do not become the designated timestamp of the projection.
        // A bare name that both tables share stays ambiguous.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE TaqBid (bid_ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(bid_ts) PARTITION BY DAY");
            execute("INSERT INTO TaqBid SELECT ts, sym, bid FROM TaqQuote");
            // The timestamp of the matching quote, if any, at each trade and offset.
            final String quoteTimestamps = """
                    \tbid
                    \tnull
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:00.000000Z\t10.0
                    \tnull
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:00.000000Z\t10.0
                    2026-01-01T00:00:03.000000Z\t14.0
                    2026-01-01T00:00:00.000000Z\t20.0
                    2026-01-01T00:00:00.000000Z\t20.0
                    2026-01-01T00:00:03.000000Z\t22.0
                    \tnull
                    \tnull
                    \tnull
                    \tnull
                    \tnull
                    \tnull
                    """;
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String column : new String[]{"bid_ts", "q.bid_ts", "Q.BID_TS"}) {
                    final String sql = "SELECT " + column + ", q.bid FROM TaqTrade t HORIZON JOIN TaqBid q ON (sym) LIST (-1s, 0s, 1s) AS h";
                    try (RecordCursorFactory factory = select(sql)) {
                        assertProjectionFactory(factory, isParallel);
                    }
                    assertQuery(sql).noRandomAccess().expectSize()
                            .returns(column.substring(column.indexOf('.') + 1) + quoteTimestamps);
                }
                for (String column : new String[]{"q.ts", "Q.TS"}) {
                    final String sql = "SELECT " + column + ", q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h";
                    assertQuery(sql).noRandomAccess().expectSize()
                            .returns(column.substring(column.indexOf('.') + 1) + quoteTimestamps);
                    assertQuery("SELECT count() FROM (" + sql + ") SAMPLE BY 1s")
                            .fails(0, "base query does not provide designated TIMESTAMP column");
                }
                assertQuery("""
                        SELECT h.timestamp, q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h
                        """).noRandomAccess().expectSize().returns("""
                        timestamp\tbid
                        2025-12-31T23:59:59.000000Z\tnull
                        2026-01-01T00:00:00.000000Z\t10.0
                        2026-01-01T00:00:01.000000Z\t10.0
                        2025-12-31T23:59:59.000000Z\tnull
                        2026-01-01T00:00:00.000000Z\t10.0
                        2026-01-01T00:00:01.000000Z\t10.0
                        2026-01-01T00:00:01.000000Z\t10.0
                        2026-01-01T00:00:02.000000Z\t10.0
                        2026-01-01T00:00:03.000000Z\t14.0
                        2026-01-01T00:00:01.000000Z\t20.0
                        2026-01-01T00:00:02.000000Z\t20.0
                        2026-01-01T00:00:03.000000Z\t22.0
                        2026-01-01T00:00:01.000000Z\tnull
                        2026-01-01T00:00:02.000000Z\tnull
                        2026-01-01T00:00:03.000000Z\tnull
                        2026-01-01T00:00:01.000000Z\tnull
                        2026-01-01T00:00:02.000000Z\tnull
                        2026-01-01T00:00:03.000000Z\tnull
                        """);
                assertQuery("SELECT ts, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h")
                        .fails(7, "Ambiguous column [name=ts]");
            }
        });
    }

    @Test
    public void testProjectionOversizedFramesMatchedByReader() throws Exception {
        // A task matches at most cairo.sql.page.frame.max.rows / (offsets * slaves) rows of its
        // frame; the reading thread matches the rest. Both the native frames (sized for the small
        // frame budget) and the Parquet row groups here are larger than that. The rows must also
        // stay the same after a rewind, following a full pass and a pass that stops in a tail.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 16);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 500);
        assertMemoryLeak(() -> {
            createRandomTradesAndQuotes(3_000, 12_000, 10);
            assertParallelMatchesSerial(engine, sqlExecutionContext, 1);
            execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertParallelMatchesSerial(engine, sqlExecutionContext, 1);
        });
    }

    @Test
    public void testProjectionParquetAndVariableColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            // The conversion skips the active partition, so a row in the next day's partition of
            // each table makes the first day convertible. The trade of the second day matches the
            // last A quote of the first day, which lives in the Parquet partition.
            execute("INSERT INTO TaqTrade VALUES ('2026-01-02T00:00:00Z', 'A', 10)");
            execute("INSERT INTO TaqQuote VALUES ('2026-01-02T00:00:00Z', 'B', 26, 28)");
            execute("ALTER TABLE TaqQuote ADD COLUMN description STRING, label VARCHAR");
            execute("UPDATE TaqQuote SET description = sym::STRING, label = sym::VARCHAR");
            execute("ALTER TABLE TaqTrade CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE TaqQuote CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertQuery("""
                    SELECT 'TaqTrade' AS tbl, name, isParquet FROM table_partitions('TaqTrade')
                    UNION ALL
                    SELECT 'TaqQuote' AS tbl, name, isParquet FROM table_partitions('TaqQuote')
                    """).noRandomAccess().expectSize().returns("""
                    tbl\tname\tisParquet
                    TaqTrade\t2026-01-01\ttrue
                    TaqTrade\t2026-01-02\tfalse
                    TaqQuote\t2026-01-01\ttrue
                    TaqQuote\t2026-01-02\tfalse
                    """);
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT s, description, label, count() AS n FROM (
                            SELECT q.sym AS s, q.description, q.label FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        ) GROUP BY s, description, label ORDER BY s
                        """).expectSize().withPlanContaining(projectionPlan(isParallel, 2))
                        .returns("s\tdescription\tlabel\tn\n\t\t\t4\nA\tA\tA\t8\nB\tB\tB\t2\n");
                assertQuery("""
                        SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h
                        """).noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3)).returns(ROWS_IN_MASTER_ORDER + """
                                2026-01-02T00:00:00.000000Z\tA\t-1000000\t14.0
                                2026-01-02T00:00:00.000000Z\tA\t0\t14.0
                                2026-01-02T00:00:00.000000Z\tA\t1000000\t14.0
                                """);
            }
        });
    }

    @Test
    public void testProjectionRowsFollowMasterOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // One row per trade and offset, in trade order and then offset order; C has no
                // quote and B's second quote lands after the trade.
                assertQuery("""
                        SELECT t.ts, t.sym, t.price, h.offset, h.timestamp, q.bid, q.ask FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h
                        """).noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3)).returns("""
                                ts\tsym\tprice\toffset\ttimestamp\tbid\task
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t-1000000\t2025-12-31T23:59:59.000000Z\tnull\tnull
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t0\t2026-01-01T00:00:00.000000Z\t10.0\t12.0
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t1000000\t2026-01-01T00:00:01.000000Z\t10.0\t12.0
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t-1000000\t2025-12-31T23:59:59.000000Z\tnull\tnull
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t0\t2026-01-01T00:00:00.000000Z\t10.0\t12.0
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t1000000\t2026-01-01T00:00:01.000000Z\t10.0\t12.0
                                2026-01-01T00:00:02.000000Z\tA\t10.0\t-1000000\t2026-01-01T00:00:01.000000Z\t10.0\t12.0
                                2026-01-01T00:00:02.000000Z\tA\t10.0\t0\t2026-01-01T00:00:02.000000Z\t10.0\t12.0
                                2026-01-01T00:00:02.000000Z\tA\t10.0\t1000000\t2026-01-01T00:00:03.000000Z\t14.0\t16.0
                                2026-01-01T00:00:02.000000Z\tB\t20.0\t-1000000\t2026-01-01T00:00:01.000000Z\t20.0\t22.0
                                2026-01-01T00:00:02.000000Z\tB\t20.0\t0\t2026-01-01T00:00:02.000000Z\t20.0\t22.0
                                2026-01-01T00:00:02.000000Z\tB\t20.0\t1000000\t2026-01-01T00:00:03.000000Z\t22.0\t24.0
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t-1000000\t2026-01-01T00:00:01.000000Z\tnull\tnull
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t0\t2026-01-01T00:00:02.000000Z\tnull\tnull
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t1000000\t2026-01-01T00:00:03.000000Z\tnull\tnull
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t-1000000\t2026-01-01T00:00:01.000000Z\tnull\tnull
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t0\t2026-01-01T00:00:02.000000Z\tnull\tnull
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t1000000\t2026-01-01T00:00:03.000000Z\tnull\tnull
                                """);
            }
        });
    }

    @Test
    public void testProjectionSampleByOverProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT ts, count() AS n, sum(bid) AS bids FROM (
                            SELECT t.ts, q.bid FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h
                        ) SAMPLE BY 1s
                        """).noRandomAccess().timestamp("ts").returns("""
                        ts\tn\tbids
                        2026-01-01T00:00:00.000000Z\t6\t40.0
                        2026-01-01T00:00:02.000000Z\t12\t96.0
                        """);
            }
        });
    }

    @Test
    public void testProjectionSlotCapMatchesSerial() throws Exception {
        // On both sides of the 100-slot cap, the rows equal those the projection returns with
        // parallel HORIZON JOIN disabled, under a LIMIT and after a rewind. 100 slots leave master
        // frames of 10 rows here.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 100);
        assertMemoryLeak(() -> {
            createRandomTradesAndQuotes(400, 3_000, 10);
            // Pairs of queries at 100 and 101 slots, then at 100 and 102 slots.
            final String[] queries = {
                    "SELECT t.id, t.ts, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) RANGE FROM -50s TO 49s STEP 1s AS h WHERE t.qty > 0.3",
                    "SELECT t.id, t.ts, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) RANGE FROM -50s TO 50s STEP 1s AS h WHERE t.qty > 0.3",
                    "SELECT t.id, t.ts, h.offset, q.bid, r.bid FROM trades t HORIZON JOIN quotes q ON (sym) HORIZON JOIN quotes r RANGE FROM -25s TO 24s STEP 1s AS h",
                    "SELECT t.id, t.ts, h.offset, q.bid, r.bid FROM trades t HORIZON JOIN quotes q ON (sym) HORIZON JOIN quotes r RANGE FROM -25s TO 25s STEP 1s AS h",
            };
            final StringSink expected = new StringSink();
            for (int i = 0; i < queries.length; i++) {
                final boolean isParallel = i % 2 == 0;
                final boolean isFiltered = i < 2;
                for (String limit : new String[]{"", " LIMIT 1_234, 4_567"}) {
                    final String sql = queries[i] + limit;
                    sqlExecutionContext.setParallelHorizonJoinEnabled(false);
                    printSql(sql, expected);
                    sqlExecutionContext.setParallelHorizonJoinEnabled(true);
                    try (RecordCursorFactory factory = select(sql)) {
                        assertProjectionFactory(factory, isParallel);
                    }
                    assertQuery(sql).noRandomAccess().expectSize(!isFiltered).timestamp("ts").returns(expected.toString());
                }
            }
        });
    }

    @Test
    public void testProjectionSlotCapPicksFactory() throws Exception {
        // The parallel factory runs a projection of up to 100 slots per master row, offsets times
        // right-hand tables. Past that the serial factory does, and the filter stays on the
        // master's async filter. Aggregations keep their parallel factory.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE TaqPrint (ts TIMESTAMP, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            final String oneSlave = "SELECT t.ts, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) ";
            for (String horizon : List.of("RANGE FROM 0s TO 99s STEP 1s AS h", listOffsets(100))) {
                assertQuery(oneSlave + horizon + " WHERE t.price > 15").assertsPlan("""
                        VirtualRecord
                          functions: [t.ts,h.offset,q.bid]
                            Async JIT Horizon Join Projection workers: 1 offsets: 100
                              filter: 15<price
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: TaqTrade
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: TaqQuote
                        """);
            }
            for (String horizon : List.of("RANGE FROM 0s TO 100s STEP 1s AS h", listOffsets(101))) {
                assertQuery(oneSlave + horizon + " WHERE t.price > 15").assertsPlan("""
                        VirtualRecord
                          functions: [t.ts,h.offset,q.bid]
                            Horizon Join Projection offsets: 101
                                Async JIT Filter workers: 1
                                  filter: 15<price
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: TaqTrade
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: TaqQuote
                        """);
                assertQuery(oneSlave + horizon).assertsPlan("""
                        VirtualRecord
                          functions: [t.ts,h.offset,q.bid]
                            Horizon Join Projection offsets: 101
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: TaqTrade
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: TaqQuote
                        """);
            }

            final String twoSlaves = "SELECT t.ts, h.offset, q.bid, p.px FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) HORIZON JOIN TaqPrint p ";
            assertQuery(twoSlaves + "RANGE FROM 0s TO 49s STEP 1s AS h WHERE concat(t.sym, '') = 'A'").assertsPlan("""
                    VirtualRecord
                      functions: [t.ts,h.offset,q.bid,p.px]
                        Async Horizon Join Projection workers: 1 offsets: 50
                          filter: concat([sym,''])='A'
                            PageFrame
                                Row forward scan
                                Frame forward scan on: TaqTrade
                            PageFrame
                                Row forward scan
                                Frame forward scan on: TaqQuote
                            PageFrame
                                Row forward scan
                                Frame forward scan on: TaqPrint
                    """);
            assertQuery(twoSlaves + "RANGE FROM 0s TO 50s STEP 1s AS h WHERE concat(t.sym, '') = 'A'").assertsPlan("""
                    VirtualRecord
                      functions: [t.ts,h.offset,q.bid,p.px]
                        Horizon Join Projection offsets: 51
                            Async Filter workers: 1
                              filter: concat([sym,''])='A'
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: TaqTrade
                            PageFrame
                                Row forward scan
                                Frame forward scan on: TaqQuote
                            PageFrame
                                Row forward scan
                                Frame forward scan on: TaqPrint
                    """);

            assertQuery("""
                    SELECT h.offset, avg(q.bid) FROM TaqTrade t
                    HORIZON JOIN TaqQuote q ON (sym) RANGE FROM 0s TO 100s STEP 1s AS h
                    """).assertsPlan("""
                    Async Horizon Join workers: 1 offsets: 101
                      keys: [offset]
                      values: [avg(q.bid)]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: TaqTrade
                        PageFrame
                            Row forward scan
                            Frame forward scan on: TaqQuote
                    """);

            sqlExecutionContext.setParallelHorizonJoinEnabled(false);
            assertQuery(oneSlave + "RANGE FROM 0s TO 99s STEP 1s AS h WHERE t.price > 15").assertsPlan("""
                    VirtualRecord
                      functions: [t.ts,h.offset,q.bid]
                        Horizon Join Projection offsets: 100
                            Async JIT Filter workers: 1
                              filter: 15<price
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: TaqTrade
                            PageFrame
                                Row forward scan
                                Frame forward scan on: TaqQuote
                    """);
        });
    }

    @Test
    public void testProjectionTimestampOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts)");
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts)");
            execute("INSERT INTO trades VALUES ('9999-12-31T23:59:59.999999Z', 'A')");
            execute("INSERT INTO quotes VALUES ('9999-12-31T23:59:59.999999Z', 'A', 10)");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                try (RecordCursorFactory factory = select("""
                        SELECT t.ts, h.offset, q.bid FROM trades t
                        HORIZON JOIN quotes q ON (sym) LIST (0s, 9000000000000s) AS h
                        """)) {
                    assertProjectionFactory(factory, isParallel);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("expected horizon timestamp overflow");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "horizon timestamp overflow");
                    }
                }
            }
        });
    }

    @Test
    public void testProjectionWorkerPoolMatchesSerial() throws Exception {
        // Many small frames across real workers; the parallel rows must equal the serial rows,
        // order included.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 300);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final boolean isParquet = rnd.nextBoolean();
        if (rnd.nextBoolean()) {
            // Tasks match a few rows of each frame and the reading thread matches the rest.
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 16);
        }
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        createRandomTradesAndQuotes(engine, context, 5_000, 20_000, 30);
                        if (isParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
                            engine.execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
                        }
                        assertParallelMatchesSerial(engine, context, 4);
                        // A LIMIT closes the cursor while tasks are still in flight.
                        context.setParallelHorizonJoinEnabled(true);
                        try (RecordCursorFactory factory = engine.select(
                                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0s, 1s) AS h LIMIT 3",
                                context
                        )) {
                            assertProjectionFactory(factory, true);
                            for (int i = 0; i < 10; i++) {
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    int rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals(3, rows);
                                }
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testSingleSymbolAndUnkeyedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT count() FROM (
                            SELECT t.price FROM (SELECT * FROM TaqTrade WHERE sym = 'A') t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("count\n6\n");
                assertQuery("""
                        SELECT count() FROM (
                            SELECT q.bid FROM TaqTrade t
                            HORIZON JOIN TaqQuote q LIST (1s, 5s) AS h
                        )
                        """).noRandomAccess().expectSize().returns("count\n12\n");
            }
        });
    }

    private static void assertManyOffsetsParallelMatchesSerial(CairoEngine engine, SqlExecutionContext context) throws Exception {
        final String[] queries = {
                // Keyed, unfiltered.
                "SELECT t.id, t.ts, t.sym, h.offset, h.timestamp, q.bid, q.ts FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS,
                // Unkeyed.
                "SELECT t.id, h.offset, q.bid, q.sym FROM trades t HORIZON JOIN quotes q" + MANY_OFFSETS,
                // JIT filter that passes half of the rows.
                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS + " WHERE t.qty > 0.5",
                // Selective filter: most frames reduce to nothing.
                "SELECT t.id, t.sym, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS + " WHERE t.qty > 0.95",
                // Java filter with a clone per worker.
                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS
                        + " WHERE concat(t.sym, '') IN ('s1', 's7')",
                // Interval scan over all but the first trades.
                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS
                        + " WHERE t.ts >= '2026-01-01T00:00:08'",
                // Interval scan over the first trades.
                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS
                        + " WHERE t.ts < '2026-01-01T00:10:00'",
                // Interval that a sub-query resolves whenever the master's page frame cursor opens.
                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym)" + MANY_OFFSETS
                        + " WHERE t.ts > (SELECT min(ts) FROM trades) AND t.qty >= 0",
                // Two slaves, one keyed: 100 slots per master row.
                "SELECT t.id, h.offset, q.bid, r.bid FROM trades t HORIZON JOIN quotes q ON (sym) HORIZON JOIN quotes r" + MANY_OFFSETS,
        };
        final StringSink serial = new StringSink();
        final StringSink parallel = new StringSink();
        for (String sql : queries) {
            context.setParallelHorizonJoinEnabled(false);
            try (RecordCursorFactory factory = engine.select(sql, context)) {
                assertProjectionFactory(factory, false);
                printRows(factory, context, serial);
            }
            context.setParallelHorizonJoinEnabled(true);
            try (RecordCursorFactory factory = engine.select(sql, context)) {
                assertProjectionFactory(factory, true);
                printRows(factory, context, parallel);
                TestUtils.assertEquals(sql, serial, parallel);
                assertRowsAfterPartialPass(sql, factory, context, serial, parallel);
            }
        }
    }

    // Asserts that the parallel factory returns the rows of the serial one and returns the number
    // of reduce tasks that a single pass over the parallel cursor publishes.
    private static long assertMatchesSerialAndCountReduceTasks(SqlExecutionContext context, String sql) throws Exception {
        final StringSink serial = new StringSink();
        final StringSink parallel = new StringSink();
        context.setParallelHorizonJoinEnabled(false);
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            assertProjectionFactory(factory, false);
            printRows(factory, context, serial);
        }
        context.setParallelHorizonJoinEnabled(true);
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            assertProjectionFactory(factory, true);
            printRows(factory, context, parallel);
            TestUtils.assertEquals(sql, serial, parallel);
            return drainAndCountReduceTasks(engine, factory, context);
        }
    }

    private static void assertParallelMatchesSerial(CairoEngine engine, SqlExecutionContext context, int workers) throws Exception {
        final String[] queries = {
                // Keyed, several offsets on both sides of the trade.
                "SELECT t.id, t.ts, t.sym, h.offset, h.timestamp, q.bid, q.ts FROM trades t "
                        + "HORIZON JOIN quotes q ON (sym) LIST (-2s, 0s, 1s, 7s) AS h",
                // Unkeyed.
                "SELECT t.id, h.offset, q.bid, q.sym FROM trades t HORIZON JOIN quotes q RANGE FROM -1s TO 2s STEP 1s AS h",
                // JIT filter, stolen by the parallel factory.
                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0s, 3s) AS h WHERE t.qty > 0.5",
                // A selective filter decodes only its own columns of a Parquet frame first.
                "SELECT t.id, t.sym, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (-1s, 1s) AS h WHERE t.qty > 0.95",
                // Java filter with a clone per worker.
                "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0s, 3s) AS h "
                        + "WHERE concat(t.sym, '') IN ('s1', 's7', 's11')",
                // Two slaves, one keyed.
                "SELECT t.id, h.offset, q.bid, r.bid FROM trades t HORIZON JOIN quotes q ON (sym) "
                        + "HORIZON JOIN quotes r LIST (-1s, 2s) AS h",
        };
        final StringSink serial = new StringSink();
        final StringSink parallel = new StringSink();
        for (String sql : queries) {
            context.setParallelHorizonJoinEnabled(false);
            try (RecordCursorFactory factory = engine.select(sql, context)) {
                assertProjectionFactory(factory, false);
                printRows(factory, context, serial);
            }
            context.setParallelHorizonJoinEnabled(true);
            try (RecordCursorFactory factory = engine.select(sql, context)) {
                assertProjectionFactory(factory, true);
                TestUtils.assertContains(explain(engine, context, sql), "workers: " + workers);
                printRows(factory, context, parallel);
                TestUtils.assertEquals(sql, serial, parallel);
                assertRowsAfterPartialPass(sql, factory, context, serial, parallel);
            }
        }
    }

    private static void assertProductionPageFrameSizes(SqlExecutionContext context) {
        Assert.assertEquals(100_000, context.getPageFrameMinRows());
        Assert.assertEquals(1_000_000, context.getPageFrameMaxRows());
    }

    private static void assertProjectionFactory(RecordCursorFactory factory, boolean isParallel) {
        TestUtils.assertFactoryInTree(
                factory,
                isParallel ? AsyncHorizonJoinProjectionRecordCursorFactory.class : HorizonJoinProjectionRecordCursorFactory.class
        );
    }

    // Stops part of the way into a frame, rewinds and reads every row. When a frame holds more
    // rows than a task matches, most of its rows lie in the tail that the reading thread matches:
    // with 4 offsets, frames of 250 rows and tasks of 4 rows, the stop falls in the middle of the
    // 309th master row, in the tail of the second frame.
    private static void assertRowsAfterPartialPass(
            String sql,
            RecordCursorFactory factory,
            SqlExecutionContext context,
            StringSink expected,
            StringSink actual
    ) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            //noinspection StatementWithEmptyBody
            for (int i = 0; i < 1_234 && cursor.hasNext(); i++) {
            }
            cursor.toTop();
            actual.clear();
            CursorPrinter.println(cursor, factory.getMetadata(), actual);
        }
        TestUtils.assertEquals(sql, expected, actual);
    }

    private static void createRandomTradesAndQuotes(CairoEngine engine, SqlExecutionContext context, int tradeRows, int quoteRows, int symbols) throws Exception {
        // Trades and quotes span several hourly partitions and interleave in time.
        engine.execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR", context);
        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR", context);
        engine.execute(
                "INSERT INTO trades SELECT x, '2026-01-01'::TIMESTAMP + x * (10_800_000_000L / " + tradeRows + "), "
                        + "('s' || rnd_int(0, " + (symbols - 1) + ", 0))::SYMBOL, rnd_double() FROM long_sequence(" + tradeRows + ")",
                context
        );
        engine.execute(
                "INSERT INTO quotes SELECT '2026-01-01'::TIMESTAMP - 60_000_000L + x * (10_900_000_000L / " + quoteRows + "), "
                        + "('s' || rnd_int(0, " + symbols + ", 0))::SYMBOL, rnd_double() FROM long_sequence(" + quoteRows + ")",
                context
        );
    }

    // Reads every row once and returns the number of reduce tasks the pass publishes: one per
    // page frame of the factory and of the sub-queries its cursor opens.
    private static long drainAndCountReduceTasks(CairoEngine engine, RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        final long before = publishedReduceTaskCount(engine);
        try (RecordCursor cursor = factory.getCursor(context)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
        }
        return publishedReduceTaskCount(engine) - before;
    }

    private static String explain(CairoEngine engine, SqlExecutionContext context, String sql) throws Exception {
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, context, "EXPLAIN " + sql, sink);
        return sink.toString();
    }

    // LIST (0s, 1s, ...) AS h with the given number of offsets.
    private static String listOffsets(int offsetCount) {
        final StringSink sink = new StringSink();
        sink.put("LIST (");
        for (int i = 0; i < offsetCount; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            sink.put(i).put('s');
        }
        return sink.put(") AS h").toString();
    }

    private static void printRows(RecordCursorFactory factory, SqlExecutionContext context, StringSink sink) throws Exception {
        sink.clear();
        try (RecordCursor cursor = factory.getCursor(context)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
            // Count the same rows again without reading them.
            long rowCount = -1;
            for (int i = 0, n = sink.length(); i < n; i++) {
                if (sink.charAt(i) == '\n') {
                    rowCount++;
                }
            }
            final long size = cursor.size();
            Assert.assertTrue(size == -1 || size == rowCount);
            cursor.toTop();
            final RecordCursor.Counter counter = new RecordCursor.Counter();
            cursor.calculateSize(context.getCircuitBreaker(), counter);
            Assert.assertEquals(rowCount, counter.get());
            Assert.assertFalse(cursor.hasNext());
            // Rewind after the full passes and read the same rows again.
            cursor.toTop();
            final StringSink rewoundRows = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), rewoundRows);
            TestUtils.assertEquals(sink, rewoundRows);
        }
    }

    private static String projectionPlan(boolean isParallel, int offsetCount) {
        return isParallel
                ? "Horizon Join Projection workers: 1 offsets: " + offsetCount
                : "Horizon Join Projection offsets: " + offsetCount;
    }

    private static long publishedReduceTaskCount(CairoEngine engine) {
        final MessageBus messageBus = engine.getMessageBus();
        long taskCount = 0;
        for (int i = 0, n = messageBus.getPageFrameReduceShardCount(); i < n; i++) {
            taskCount += messageBus.getPageFrameReducePubSeq(i).current();
        }
        return taskCount;
    }

    // The test configuration shrinks the page frame sizes; these are the server defaults.
    private static void setProductionPageFrameSizes() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 100_000);
    }

    /**
     * Asserts that a single-offset HORIZON JOIN projection returns the rows of the ASOF JOIN with
     * the same key, in the same order. The master table needs an ascending id column.
     */
    private void assertMatchesAsOfJoin(String master, String slave, String on, String columns) throws Exception {
        final StringSink expected = new StringSink();
        printSql("SELECT " + columns + " FROM " + master + " t ASOF JOIN " + slave + " q " + on, expected);
        final String sql = "SELECT " + columns + " FROM " + master + " t HORIZON JOIN " + slave + " q " + on + " LIST (0s) AS h";
        assertQuery(sql).noRandomAccess().expectSize().returns(expected.toString());
        // A second offset must not change the first offset's rows.
        final StringSink twoOffsets = new StringSink();
        printSql(
                "SELECT " + columns + " FROM " + master + " t HORIZON JOIN " + slave + " q " + on + " LIST (0s, 1s) AS h",
                twoOffsets
        );
        final StringSink everyOther = new StringSink();
        final String[] lines = twoOffsets.toString().split("\n");
        everyOther.put(lines[0]).put('\n');
        for (int i = 1; i < lines.length; i += 2) {
            everyOther.put(lines[i]).put('\n');
        }
        TestUtils.assertEquals(sql, expected, everyOther);
    }

    private void assertRewindAndSize(String sql, int rowCount, boolean isSizeKnown, StringSink firstPass, StringSink secondPass) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertProjectionFactory(factory, sqlExecutionContext.isParallelHorizonJoinEnabled());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertEquals(sql, isSizeKnown ? rowCount : -1, cursor.size());
                // Read part of the rows, then count the rest.
                for (int i = 0; i < 4; i++) {
                    Assert.assertTrue(cursor.hasNext());
                }
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(sql, rowCount - 4, counter.get());
                Assert.assertFalse(cursor.hasNext());

                // Rewind and read everything twice.
                cursor.toTop();
                firstPass.clear();
                CursorPrinter.println(cursor, factory.getMetadata(), firstPass);
                cursor.toTop();
                secondPass.clear();
                CursorPrinter.println(cursor, factory.getMetadata(), secondPass);
                TestUtils.assertEquals(firstPass, secondPass);
                Assert.assertEquals(sql, rowCount + 1, firstPass.toString().split("\n").length);

                // Rewind mid-frame, then count everything.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                cursor.toTop();
                counter.clear();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(sql, rowCount, counter.get());
            }
            // A second cursor from the same factory reads the same rows.
            secondPass.clear();
            printRows(factory, sqlExecutionContext, secondPass);
            TestUtils.assertEquals(firstPass, secondPass);
        }
    }

    private void createRandomTradesAndQuotes(int tradeRows, int quoteRows, int symbols) throws Exception {
        createRandomTradesAndQuotes(engine, sqlExecutionContext, tradeRows, quoteRows, symbols);
    }

    private void createTradesAndQuotes() throws Exception {
        execute("CREATE TABLE TaqTrade (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE TaqQuote (ts TIMESTAMP, sym SYMBOL, bid DOUBLE, ask DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        // Exact duplicate trades, repeated prices, and a symbol with no quotes must all retain multiplicity.
        execute("""
                INSERT INTO TaqTrade VALUES
                    ('2026-01-01T00:00:00Z', 'A', 10),
                    ('2026-01-01T00:00:00Z', 'A', 10),
                    ('2026-01-01T00:00:02Z', 'A', 10),
                    ('2026-01-01T00:00:02Z', 'B', 20),
                    ('2026-01-01T00:00:02Z', 'C', 30),
                    ('2026-01-01T00:00:02Z', 'C', 30)
                """);
        execute("""
                INSERT INTO TaqQuote VALUES
                    ('2026-01-01T00:00:00Z', 'A', 10, 12),
                    ('2026-01-01T00:00:00Z', 'B', 20, 22),
                    ('2026-01-01T00:00:03Z', 'A', 14, 16),
                    ('2026-01-01T00:00:03Z', 'B', 22, 24)
                """);
    }
}
