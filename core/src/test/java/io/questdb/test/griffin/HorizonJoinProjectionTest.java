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
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncFilterResources;
import io.questdb.griffin.engine.table.AsyncHorizonJoinProjectionRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinResources;
import io.questdb.griffin.engine.table.HorizonJoinProjectionRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinSlaveState;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
    // Right-hand filters that test_fault() can fail; %1$s is the left-hand table and %2$s the
    // right-hand one.
    private static final String[] SLAVE_FILTER_FAULT_QUERIES = {
            // One right-hand table, keyed.
            "SELECT t.ts, t.sym, h.offset, q.bid FROM %1$s t HORIZON JOIN (%2$s WHERE test_fault()) q ON (sym) LIST (-1s, 0s, 1s) AS h",
            // One right-hand table, unkeyed.
            "SELECT t.ts, h.offset, q.sym, q.bid FROM %1$s t HORIZON JOIN (%2$s WHERE test_fault()) q LIST (0s, 1s) AS h",
            // Two right-hand tables. Only the second one has a filter, so a failed init finds the
            // first one initialized.
            "SELECT t.ts, t.sym, h.offset, q.bid, r.bid FROM %1$s t HORIZON JOIN %2$s q ON (sym) HORIZON JOIN (%2$s WHERE test_fault()) r ON (sym) LIST (0s, 1s) AS h",
            // Two right-hand tables, each with a filter.
            "SELECT t.ts, h.offset, q.bid, r.bid FROM %1$s t HORIZON JOIN (%2$s WHERE test_fault()) q ON (sym) HORIZON JOIN (%2$s WHERE test_fault()) r LIST (0s, 1s) AS h",
    };

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
                        """).expectSize().returns("""
                        n
                        2
                        4
                        6
                        """);
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
                        """).noRandomAccess().sizeMayVary().returns("""
                        n
                        42
                        42
                        """);
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
                            .expectSize().returns("""
                                    seconds\tn
                                    1\t3
                                    5\t3
                                    """);
                }
            }
        });
    }

    @Test
    public void testDistinctFusesIntoHorizonAggregate() throws Exception {
        // DISTINCT groups the horizon rows by the selected columns in the fused horizon aggregate,
        // as GROUP BY does, so the grouped rows keep no designated timestamp.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String distinct = "SELECT DISTINCT t.ts, t.sym FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (0s, 1s) AS h";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("SELECT * FROM (" + distinct + ") ORDER BY ts, sym")
                        .withPlanContaining("keys: [ts,sym]")
                        .withPlanNotContaining("Distinct", "Projection")
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tsym
                                2026-01-01T00:00:00.000000Z\tA
                                2026-01-01T00:00:02.000000Z\tA
                                2026-01-01T00:00:02.000000Z\tB
                                2026-01-01T00:00:02.000000Z\tC
                                """);
                assertQuery("SELECT ts, count() FROM (" + distinct + ") SAMPLE BY 1s")
                        .fails(0, "base query does not provide designated TIMESTAMP column");
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
    public void testHorizonWildcards() throws Exception {
        // h.* lists the offset and timestamp columns of the horizon pseudo-table where the
        // wildcard stands among the other select items. * lists them just before the last
        // right-hand table, whether a projection or an aggregate reads the join.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String join = " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (0s) AS h";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("SELECT h.*" + join)
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                offset\ttimestamp
                                0\t2026-01-01T00:00:00.000000Z
                                0\t2026-01-01T00:00:00.000000Z
                                0\t2026-01-01T00:00:02.000000Z
                                0\t2026-01-01T00:00:02.000000Z
                                0\t2026-01-01T00:00:02.000000Z
                                0\t2026-01-01T00:00:02.000000Z
                                """);
                assertQuery("SELECT q.*, h.*, t.*" + join)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts1")
                        .returns("""
                                ts\tsym\tbid\task\toffset\ttimestamp\tts1\tsym1\tprice
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0\t0\t2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\tA\t10.0
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0\t0\t2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\tA\t10.0
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:02.000000Z\tA\t10.0
                                2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:02.000000Z\tB\t20.0
                                \t\tnull\tnull\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:02.000000Z\tC\t30.0
                                \t\tnull\tnull\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:02.000000Z\tC\t30.0
                                """);
                assertQuery("SELECT h.*, avg(q.bid)" + join + " ORDER BY 1, 2")
                        .expectSize()
                        .returns("""
                                offset\ttimestamp\tavg
                                0\t2026-01-01T00:00:00.000000Z\t10.0
                                0\t2026-01-01T00:00:02.000000Z\t15.0
                                """);
                final String twoSlaves = " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) HORIZON JOIN TaqQuote r LIST (0s) AS h ORDER BY 1, 2";
                final String header = "ts\tsym\tprice\tts1\tsym1\tbid\task\toffset\ttimestamp\tts2\tsym2\tbid1\task1";
                final String[] rows = {
                        "2026-01-01T00:00:00.000000Z\tA\t10.0\t2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0\t0\t2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0",
                        "2026-01-01T00:00:02.000000Z\tA\t10.0\t2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0",
                        "2026-01-01T00:00:02.000000Z\tB\t20.0\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0",
                        "2026-01-01T00:00:02.000000Z\tC\t30.0\t\t\tnull\tnull\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0"
                };
                assertQuery("SELECT DISTINCT *" + twoSlaves)
                        .expectSize()
                        .timestamp("ts")
                        .returns(header + "\n" + String.join("\n", rows) + "\n");
                assertQuery("SELECT *, count() c" + twoSlaves)
                        .expectSize()
                        .timestamp("ts")
                        .returns(header + "\tc\n" + rows[0] + "\t2\n" + rows[1] + "\t1\n" + rows[2] + "\t1\n" + rows[3] + "\t2\n");
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
                        """).expectSize().returns("""
                        s\tn\ttotal
                        \t4\tnull
                        A\t6\t164.0
                        B\t2\t92.0
                        """);
                assertQuery("""
                        SELECT s, count() AS n FROM (
                            SELECT q.sym AS s FROM TaqTrade t
                            HORIZON JOIN TaqQuote q ON (sym) LIST (1s, 5s) AS h
                        ) GROUP BY s ORDER BY s
                        """).expectSize().returns("""
                        s\tn
                        \t4
                        A\t6
                        B\t2
                        """);
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
    public void testProjectionAndAggregateRejectInputsAlike() throws Exception {
        // A right-hand side that serves no time frames, and a serial join over a left-hand side
        // without random access, such as a covering index scan, fail the same way whether a
        // projection or an aggregate reads the join.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE)
                    TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO cov VALUES ('2026-01-01T00:00:01Z', 'A', 1), ('2026-01-01T00:00:02Z', 'B', 2)");
            execute("INSERT INTO quotes VALUES ('2026-01-01T00:00:00Z', 'A', 10), ('2026-01-01T00:00:00Z', 'B', 20)");
            engine.releaseAllWriters();
            final String covering = " FROM cov t HORIZON JOIN quotes q ON (sym) LIST (0s) AS h WHERE t.sym = 'A'";
            final String sampled = " FROM cov t HORIZON JOIN (SELECT ts, sym, max(bid) bid FROM quotes SAMPLE BY 1s) q ON (sym) LIST (0s) AS h";
            final String leftMessage = "left-hand side of HORIZON JOIN can only be a table with an optional filter";
            final String rightMessage = "right-hand side of HORIZON JOIN can only be a table with an optional filter";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String select : List.of("SELECT t.px, h.offset, q.bid", "SELECT h.offset, avg(q.bid) bid")) {
                    final String sampledSql = select + sampled;
                    assertQuery(sampledSql).fails(sampledSql.indexOf("HORIZON"), rightMessage);
                    final String coveringSql = select + covering;
                    if (isParallel) {
                        final boolean isAggregate = select.contains("avg");
                        assertQuery(coveringSql)
                                .withPlanContaining("CoveringIndex on: sym")
                                .supportsRandomAccess(isAggregate)
                                .expectSize()
                                .returns(isAggregate ? """
                                        offset\tbid
                                        0\t10.0
                                        """ : """
                                        px\toffset\tbid
                                        1.0\t0\t10.0
                                        """);
                    } else {
                        assertQuery(coveringSql).fails(coveringSql.indexOf("HORIZON"), leftMessage);
                    }
                }
                // string_agg() cannot run on workers, so the fused aggregate reads the master serially
                final String serialAggregate = "SELECT string_agg(q.sym::STRING, ',') s" + covering;
                assertQuery(serialAggregate).fails(serialAggregate.indexOf("HORIZON"), leftMessage);
            }
        });
    }

    @Test
    public void testProjectionAsInputOfOtherOperators() throws Exception {
        // Every operator must read the projection as it reads a table that holds the same rows. A
        // time series join reads the projection as its right-hand side, a hash JOIN builds its map
        // from it, a CROSS JOIN rewinds it for every left-hand row, and EXCEPT, LATEST ON and the
        // window functions read it once.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String projection = "(SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h)";
            execute("CREATE TABLE proj AS " + projection + " TIMESTAMP(ts) PARTITION BY DAY");
            final String[] templates = {
                    "SELECT x.ts, x.sym, x.ask, p.offset, p.bid FROM TaqQuote x ASOF JOIN %1$s p ON (sym)",
                    "SELECT x.ts, x.sym, x.ask, p.offset, p.bid FROM TaqQuote x LT JOIN %1$s p ON (sym)",
                    "SELECT x.ts, x.sym, x.ask, p.ts pts, p.offset, p.bid FROM TaqQuote x JOIN %1$s p ON (sym) ORDER BY x.ts, x.sym, pts, p.offset",
                    "SELECT x.ts, x.sym, p.sym psym, p.offset, p.bid FROM TaqQuote x CROSS JOIN %1$s p",
                    "SELECT * FROM %1$s EXCEPT SELECT * FROM %1$s WHERE offset = 0",
                    "SELECT * FROM %1$s LATEST ON ts PARTITION BY sym, offset",
                    "SELECT ts, sym, offset, bid, lag(bid) OVER (PARTITION BY sym, offset ORDER BY ts) prev, row_number() OVER () rn FROM %1$s",
            };
            final StringSink expected = new StringSink();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String template : templates) {
                    printSql(template.formatted("proj"), expected);
                    // Guard against a template that selects nothing: header plus at least two rows.
                    Assert.assertTrue(template, expected.toString().split("\n").length > 2);
                    assertQuery(template.formatted(projection))
                            .inferRandomAccess()
                            .inferTimestamp()
                            .sizeMayVary()
                            .withPlanContaining(projectionPlan(isParallel, 3))
                            .returns(expected);
                }
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
    public void testProjectionBareBindVariable() throws Exception {
        // The optimiser lifts a bare bind variable to the projection above the HORIZON JOIN. It
        // used to put it in a virtual model under the horizon model, which hid the join columns
        // from that model: compilation failed an assertion (an ArrayIndexOutOfBoundsException
        // with assertions disabled), and generateSelectChoose() leaked the projection factory.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            bindVariableService.clear();
            bindVariableService.setStr(0, "x");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // An aggregating HORIZON JOIN takes the same path.
                assertQuery("SELECT $1 tag, count() c FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (0s, 1s) AS h")
                        .noRandomAccess().expectSize().returns("tag\tc\nx\t12\n");
                // The projection selects no column of the join, only the bind variable.
                assertQuery("SELECT $1 tag FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (0s, 1s) AS h")
                        .noRandomAccess().expectSize().returns("tag\n" + "x\n".repeat(12));
                assertQuery("SELECT $1 tag, t.price, h.offset FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (0s, 1s) AS h")
                        .noRandomAccess().expectSize().returns("""
                                tag\tprice\toffset
                                x\t10.0\t0
                                x\t10.0\t1000000
                                x\t10.0\t0
                                x\t10.0\t1000000
                                x\t10.0\t0
                                x\t10.0\t1000000
                                x\t20.0\t0
                                x\t20.0\t1000000
                                x\t30.0\t0
                                x\t30.0\t1000000
                                x\t30.0\t0
                                x\t30.0\t1000000
                                """);
            }
        });
    }

    @Test
    public void testProjectionBindVariablePositions() throws Exception {
        // Bind variables in an output column, in LIMIT and in an interval on the designated
        // timestamp. A prepared statement runs the same factory again with new values, and each
        // execution must read them.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String head = " FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (0s, 1s) AS h";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                bindVariableService.clear();
                bindVariableService.setDouble(0, 2.0);
                try (RecordCursorFactory factory = select("SELECT t.price * $1 p, h.offset, q.bid" + head)) {
                    assertProjectionFactory(factory, isParallel);
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("""
                            p\toffset\tbid
                            20.0\t0\t10.0
                            20.0\t1000000\t10.0
                            20.0\t0\t10.0
                            20.0\t1000000\t10.0
                            20.0\t0\t10.0
                            20.0\t1000000\t14.0
                            40.0\t0\t20.0
                            40.0\t1000000\t22.0
                            60.0\t0\tnull
                            60.0\t1000000\tnull
                            60.0\t0\tnull
                            60.0\t1000000\tnull
                            """);
                    bindVariableService.setDouble(0, -1.0);
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("""
                            p\toffset\tbid
                            -10.0\t0\t10.0
                            -10.0\t1000000\t10.0
                            -10.0\t0\t10.0
                            -10.0\t1000000\t10.0
                            -10.0\t0\t10.0
                            -10.0\t1000000\t14.0
                            -20.0\t0\t20.0
                            -20.0\t1000000\t22.0
                            -30.0\t0\tnull
                            -30.0\t1000000\tnull
                            -30.0\t0\tnull
                            -30.0\t1000000\tnull
                            """);
                }

                bindVariableService.clear();
                bindVariableService.setLong(0, 3);
                try (RecordCursorFactory factory = select("SELECT t.sym, h.offset, q.bid" + head + " LIMIT $1")) {
                    assertProjectionFactory(factory, isParallel);
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("""
                            sym\toffset\tbid
                            A\t0\t10.0
                            A\t1000000\t10.0
                            A\t0\t10.0
                            """);
                    bindVariableService.setLong(0, 7);
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("""
                            sym\toffset\tbid
                            A\t0\t10.0
                            A\t1000000\t10.0
                            A\t0\t10.0
                            A\t1000000\t10.0
                            A\t0\t10.0
                            A\t1000000\t14.0
                            B\t0\t20.0
                            """);
                }

                bindVariableService.clear();
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2026-01-01T00:00:01.000000Z"));
                try (RecordCursorFactory factory = select("SELECT t.ts, t.sym, h.offset, q.bid" + head + " WHERE t.ts > $1")) {
                    assertProjectionFactory(factory, isParallel);
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).noRandomAccess().sizeMayVary().timestamp("ts").returns("""
                            ts\tsym\toffset\tbid
                            2026-01-01T00:00:02.000000Z\tA\t0\t10.0
                            2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0
                            2026-01-01T00:00:02.000000Z\tB\t0\t20.0
                            2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0
                            2026-01-01T00:00:02.000000Z\tC\t0\tnull
                            2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                            2026-01-01T00:00:02.000000Z\tC\t0\tnull
                            2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                            """);
                    bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2026-01-01T00:00:02.000000Z"));
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).noRandomAccess().sizeMayVary().timestamp("ts")
                            .returns("ts\tsym\toffset\tbid\n");
                    bindVariableService.setTimestamp(0, 0);
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).noRandomAccess().sizeMayVary().timestamp("ts")
                            .returns("""
                                    ts\tsym\toffset\tbid
                                    2026-01-01T00:00:00.000000Z\tA\t0\t10.0
                                    2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
                                    2026-01-01T00:00:00.000000Z\tA\t0\t10.0
                                    2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
                                    2026-01-01T00:00:02.000000Z\tA\t0\t10.0
                                    2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0
                                    2026-01-01T00:00:02.000000Z\tB\t0\t20.0
                                    2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0
                                    2026-01-01T00:00:02.000000Z\tC\t0\tnull
                                    2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                                    2026-01-01T00:00:02.000000Z\tC\t0\tnull
                                    2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                                    """);
                }
                bindVariableService.clear();
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
                            .expectSize().returns("""
                                    seconds\tn
                                    1\t6
                                    5\t6
                                    """);
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
    public void testProjectionCreateTableAsSelectInheritsTimestamp() throws Exception {
        // CREATE TABLE AS SELECT without a TIMESTAMP clause takes the designated timestamp of its
        // SELECT. The projection keeps the master's timestamp in every spelling, so the new table
        // gets it and holds one row per trade and offset. A plain SELECT passes the timestamp on
        // only under the exact spelling, see
        // testPlainSelectCreatesTableWithoutTimestampForOtherSpelling().
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String timestamp : List.of("t.ts", "T.TS")) {
                    final String column = timestamp.substring(timestamp.indexOf('.') + 1);
                    execute("CREATE TABLE markouts AS (SELECT " + timestamp
                            + ", q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h)");
                    assertQuery("markouts").expectSize().timestamp(column).returns(column + """
                            \tbid
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
                            """);
                    execute("DROP TABLE markouts");
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
    public void testProjectionDateaddOverHorizonTimestampIsSorted() throws Exception {
        // The projection emits its rows in left-hand row order and then offset order, so neither
        // h.timestamp nor a dateadd() over it follows the order of the rows. The sub-query outputs
        // h.timestamp under the name of the left-hand designated timestamp column, and the
        // optimiser used to take the dateadd() over it for a designated timestamp: ORDER BY x
        // skipped the sort and returned 00:01, 00:06, 00:02, 00:07.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (timestamp TIMESTAMP, sym SYMBOL) TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("CREATE TABLE quotes (timestamp TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES ('2026-01-01T00:00:00Z', 'A'), ('2026-01-01T00:00:01Z', 'A')");
            execute("INSERT INTO quotes VALUES ('2026-01-01T00:00:00Z', 'A', 1.0)");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("""
                        SELECT dateadd('s', 1, timestamp) x FROM (
                            SELECT h.timestamp FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0s, 5s) AS h
                        ) ORDER BY x
                        """).expectSize().timestamp("x").returns("""
                        x
                        2026-01-01T00:00:01.000000Z
                        2026-01-01T00:00:02.000000Z
                        2026-01-01T00:00:06.000000Z
                        2026-01-01T00:00:07.000000Z
                        """);
            }
        });
    }

    @Test
    public void testProjectionDeepLookupsMatchAsOfJoin() throws Exception {
        // Trades 1 and 163 look up a key that the quotes last held 5,000 rows back, across an
        // hourly partition boundary: the backward scan puts about 4,500 keys into the key cache,
        // which grows to fit them. Every other trade finds its key within 10 rows. Batches and
        // tasks hold 50 trades. The lookups after a deep one keep their few keys in the grown cache
        // rather than clear it, and the batches or tasks after it keep the grown capacity: their
        // first lookup clears the cache. The key shapes cover every map the cache can be: an
        // Unordered8Map for LONG, an Unordered4Map for INT and SYMBOL, an UnorderedVarcharMap for
        // VARCHAR and an OrderedMap for two columns. The rows must equal those of the ASOF JOIN
        // in the serial factory and on four workers, with and without a filter on the quotes.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 50);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        engine.execute("""
                                CREATE TABLE quotes AS (
                                    SELECT '2026-01-01'::TIMESTAMP + x * 1_000_000L ts, k, k::INT ki, k::SYMBOL ks,
                                           k::VARCHAR kv, 0 kz, (x % 997) / 997.0 bid
                                    FROM (SELECT x, CASE WHEN x % 10 = 0 THEN -1 ELSE x END k FROM long_sequence(12_000))
                                ) TIMESTAMP(ts) PARTITION BY HOUR
                                """, context);
                        engine.execute("""
                                CREATE TABLE trades AS (
                                    SELECT x id, '2026-01-01'::TIMESTAMP + (10_000 + x) * 1_000_000L + 500_000L ts,
                                           k, k::INT ki, k::SYMBOL ks, k::VARCHAR kv, 0 kz
                                    FROM (SELECT x, CASE WHEN x IN (1, 163) THEN 5_000 + x ELSE -1 END k FROM long_sequence(300))
                                ) TIMESTAMP(ts) PARTITION BY HOUR
                                """, context);
                        final StringSink expected = new StringSink();
                        for (String slave : List.of("quotes", "(quotes WHERE bid < 0.9)")) {
                            for (String on : List.of(
                                    "ON (k)",
                                    "ON (ki)",
                                    "ON (ks)",
                                    "ON (kv)",
                                    "ON (t.k = q.k AND t.kz = q.kz)"
                            )) {
                                TestUtils.printSql(engine, context, "SELECT t.id, q.bid, q.ts FROM trades t ASOF JOIN " + slave + " q " + on, expected);
                                for (boolean isParallel : MODES) {
                                    context.setParallelHorizonJoinEnabled(isParallel);
                                    assertQuery("SELECT t.id, q.bid, q.ts FROM trades t HORIZON JOIN " + slave + " q " + on + " LIST (0s) AS h")
                                            .withEngine(engine).withContext(context).noLeakCheck().noRandomAccess().expectSize()
                                            .withPlanContaining(isParallel ? "Horizon Join Projection workers: 4 offsets: 1" : "Horizon Join Projection offsets: 1")
                                            .returns(expected);
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
    public void testProjectionFactoryFreesArgumentsWhenConstructorFails() throws Exception {
        // The code generator hands the metadata, the factories and the filter resources over to
        // the parallel factory and frees none of them itself once the constructor runs, so a
        // constructor that fails has to. Zero offsets make it fail: it sizes the master frames by
        // the slots per master row, offsets times right-hand tables. No statement gets that far:
        // every RANGE and LIST the code generator accepts has at least one offset.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final RecordCursorFactory masterFactory = select("TaqTrade");
            final ObjList<HorizonJoinSlaveState> slaveStates = new ObjList<>();
            slaveStates.add(new HorizonJoinSlaveState(select("TaqQuote"), 1, 1, null, null, null, masterFactory.getMetadata().getColumnCount(), null, null));
            // The metadata and the bind variable memory of a stolen filter hold native memory.
            final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 1);
            final MemoryCARW bindVarMemory = Vm.getCARWInstance(1_024, 1, MemoryTag.NATIVE_JIT);
            bindVarMemory.putLong(42);
            final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(null, null, null,
                    new AsyncFilterResources(null, null, bindVarMemory, null, null, null, null), null, null, null);
            try {
                new AsyncHorizonJoinProjectionRecordCursorFactory(
                        configuration,
                        engine,
                        engine.getMessageBus(),
                        metadata,
                        masterFactory,
                        slaveStates,
                        new long[0],
                        0,
                        new int[0],
                        new int[0],
                        resources,
                        () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_SQL_COMPILER),
                        1
                ).close();
                Assert.fail("expected the constructor to fail on zero offsets");
            } catch (ArithmeticException ignore) {
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
                            .noRandomAccess().sizeMayVary().returns("""
                                    n\tis_ok
                                    42\ttrue
                                    42\ttrue
                                    """);
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
        // or 100 offsets would cut the scan of big into 20 frames of 10,000 rows. Past 100 slots
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
    public void testProjectionFilteredCountOverParquet() throws Exception {
        // A count over a filtered projection only counts rows: the parallel factory runs
        // count-only tasks, and with a selective filter they decode the filter's columns of a
        // Parquet frame first and nothing else. The counts must equal those over native
        // partitions, the filtered trades times the offsets.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 300);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        assertMemoryLeak(() -> {
            createRandomTradesAndQuotes(5_000, 20_000, 30);
            final String[] filters = {
                    // Selective JIT filter: late materialization.
                    "t.qty > 0.95",
                    // Half of the rows pass.
                    "t.qty > 0.5",
                    // Java filter.
                    "concat(t.sym, '') IN ('s1', 's7')",
                    // Nothing passes.
                    "t.qty > 2",
            };
            final ObjList<String> expected = new ObjList<>();
            final StringSink sink = new StringSink();
            for (String filter : filters) {
                printSql("SELECT count() * 3 count FROM trades t WHERE " + filter, sink);
                expected.add(sink.toString());
            }
            execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (int i = 0; i < filters.length; i++) {
                    assertQuery("SELECT count() FROM (SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (-1s, 0s, 1s) AS h WHERE "
                            + filters[i] + ")")
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining(projectionPlan(isParallel, 3))
                            .returns(expected.getQuick(i));
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
                // Java filter that is not thread-safe. Without a worker pool the owner thread
                // filters every frame with the original filter, so the worker clones stay idle.
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
    public void testProjectionInsertAsSelectChecksTimestampPosition() throws Exception {
        // INSERT INTO ... SELECT without a column list copies its columns by position. When the
        // SELECT has a designated timestamp, it has to sit at the position of the target's. The
        // projection keeps the master's timestamp in every spelling, here in its first column, so
        // a target that designates its second column rejects the statement, and a target that
        // designates its first column takes one row per trade and offset. A plain SELECT under
        // another spelling has no designated timestamp and inserts by position alone, see
        // testPlainSelectInsertsWithOtherTimestampSpelling().
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE by_trade (ts TIMESTAMP, horizon_ts TIMESTAMP, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE by_horizon (trade_ts TIMESTAMP, ts TIMESTAMP, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String timestamp : List.of("t.ts", "T.TS")) {
                    final String select = " SELECT " + timestamp
                            + ", h.timestamp, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h";
                    assertExceptionNoLeakCheck(
                            "INSERT INTO by_horizon" + select,
                            12,
                            "designated timestamp of existing table (1) does not match designated timestamp in select query (0)"
                    );
                    execute("INSERT INTO by_trade" + select);
                    assertQuery("by_trade").expectSize().timestamp("ts").returns("""
                            ts\thorizon_ts\tbid
                            2026-01-01T00:00:00.000000Z\t2025-12-31T23:59:59.000000Z\tnull
                            2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\t10.0
                            2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:01.000000Z\t10.0
                            2026-01-01T00:00:00.000000Z\t2025-12-31T23:59:59.000000Z\tnull
                            2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\t10.0
                            2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:01.000000Z\t10.0
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:01.000000Z\t10.0
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:02.000000Z\t10.0
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:03.000000Z\t14.0
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:01.000000Z\t20.0
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:02.000000Z\t20.0
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:03.000000Z\t22.0
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:01.000000Z\tnull
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:02.000000Z\tnull
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:03.000000Z\tnull
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:01.000000Z\tnull
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:02.000000Z\tnull
                            2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:03.000000Z\tnull
                            """);
                    execute("TRUNCATE TABLE by_trade");
                }
            }
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
    public void testProjectionKeyMissReopenAfterInsert() throws Exception {
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1);
        for (boolean isParallel : MODES) {
            sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
            assertQuery("""
                    SELECT t.k, q.bid FROM trades t
                    HORIZON JOIN quotes q ON (k, v) LIST (0s) AS h
                    """)
                    .ddl("""
                            CREATE TABLE trades AS (
                                SELECT x::INT k, 'missing'::VARCHAR v, 10::TIMESTAMP ts FROM long_sequence(3)
                            ) TIMESTAMP(ts) PARTITION BY DAY
                            """, """
                            CREATE TABLE quotes AS (
                                SELECT 0::INT k, 'missing'::VARCHAR v, 1::LONG bid, 8::TIMESTAMP ts FROM long_sequence(1)
                            ) TIMESTAMP(ts) PARTITION BY DAY
                            """)
                    .mutateWith("INSERT INTO quotes VALUES (2, 'missing', 42, 5)")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            k\tbid
                            1\tnull
                            2\tnull
                            3\tnull
                            """, """
                            k\tbid
                            1\tnull
                            2\t42
                            3\tnull
                            """);
            execute("DROP TABLE trades");
            execute("DROP TABLE quotes");
        }
    }

    @Test
    public void testProjectionKeyMissReopenAfterWalInsert() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "true");
        testProjectionKeyMissReopenAfterInsert();
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
    public void testProjectionKeyedLookupRestartRechecksKeptEntriesWithFilter() throws Exception {
        // Quote row r holds id r and symbol 's' || (r % 5); only the s0 to s3 rows 90 to 99 of
        // every 100 pass the filter. The s4 trade sits on quote row 140: its backward scan finds no
        // qualifying s4 row, reads all 141 rows down to row 0, past the absolute threshold of 100,
        // and leaves the s0 to s3 rows 95 to 98 in the key cache. The s0 to s3 trades sit on quote
        // row 1,340, 1,200 rows on. The lookup switches to forward scan mode there, but a forward
        // scan would read more than the 141 x 8 rows the cache is worth, so the lookup restarts it
        // at row 1,340, and five entries fill under 1/8 of the cache, so the restart keeps them.
        // The backward scan from row 1,340 must run the filter over the rows of a key whose kept
        // entry is older and return rows 1,295 to 1,298, as the ASOF JOIN does. A scan that takes
        // a kept entry as resolved skips those rows and returns the stale rows 95 to 98.
        setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_ABSOLUTE_THRESHOLD, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quotes (ts TIMESTAMP, id LONG, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO quotes
                    SELECT '2026-01-01'::TIMESTAMP + (x - 1) * 10_000L, x - 1, ('s' || ((x - 1) % 5))::SYMBOL,
                           CASE WHEN (x - 1) % 5 <> 4 AND (x - 1) % 100 >= 90 THEN 100.0 ELSE 1.0 END
                    FROM long_sequence(1_500)
                    """);
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO trades VALUES
                        ('2026-01-01T00:00:01.400', 's4'),
                        ('2026-01-01T00:00:13.400', 's0'),
                        ('2026-01-01T00:00:13.400', 's1'),
                        ('2026-01-01T00:00:13.400', 's2'),
                        ('2026-01-01T00:00:13.400', 's3')
                    """);
            final String expected = """
                    sym\tid
                    s4\tnull
                    s0\t1295
                    s1\t1296
                    s2\t1297
                    s3\t1298
                    """;
            assertQuery("SELECT t.sym, q.id FROM trades t ASOF JOIN (quotes WHERE bid > 50) q ON (sym)")
                    .noRandomAccess().expectSize().returns(expected);
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("SELECT t.sym, q.id FROM trades t HORIZON JOIN (quotes WHERE bid > 50) q ON (sym) LIST (0s) AS h")
                        .noRandomAccess().expectSize()
                        .withPlanContaining(projectionPlan(isParallel, 1)).returns(expected);
            }
        });
    }

    @Test
    public void testProjectionKeyedLookupRestartsForwardScan() throws Exception {
        // 400 trades 2 ms apart and 6,000 quotes 1 ms apart cycle through 10 symbols. A min gap
        // of 8 rows and a switch factor of 1 let the window check switch the keyed lookup to
        // forward scan mode a few trades into a batch. The offsets lie 1 s apart, far more quote
        // rows than the key cache is worth, so the lookup restarts the forward scan at each
        // offset. The quotes straddle an hourly partition 2,500 rows in: the trades reach it at
        // the 2 s offset, so restarts and forward scans there span two frames, and the first
        // partition is then converted to Parquet row groups of 100 rows. The scan mode is not
        // observable from SQL: the rows must equal those of a lookup whose thresholds keep it in
        // backward-only mode.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("""
                    INSERT INTO quotes
                    SELECT '2026-01-01T00:59:57.500'::TIMESTAMP + x * 1_000L, ('s' || (x % 10))::SYMBOL, x::DOUBLE
                    FROM long_sequence(6_000)
                    """);
            execute("""
                    INSERT INTO trades
                    SELECT x, '2026-01-01T00:59:57.600'::TIMESTAMP + x * 2_000L, ('s' || (x % 10))::SYMBOL
                    FROM long_sequence(400)
                    """);
            final String sql = "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0s, 1s, 2s) AS h";

            // Backward-only reference: neither the relative nor the window check can pass.
            setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_ABSOLUTE_THRESHOLD, Long.MAX_VALUE);
            setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_MIN_GAP, Long.MAX_VALUE);
            final StringSink expected = new StringSink();
            sqlExecutionContext.setParallelHorizonJoinEnabled(false);
            try (RecordCursorFactory factory = select(sql)) {
                assertProjectionFactory(factory, false);
                printRows(factory, sqlExecutionContext, expected);
            }

            setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_MIN_GAP, 8);
            setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_SWITCH_FACTOR, 1);
            for (boolean isParquet : new boolean[]{false, true}) {
                if (isParquet) {
                    // The conversion skips the active partition, the second hour.
                    execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                }
                assertQuery("SELECT isParquet FROM table_partitions('quotes')")
                        .noRandomAccess().expectSize()
                        .returns(isParquet ? """
                                isParquet
                                true
                                false
                                """ : """
                                isParquet
                                false
                                false
                                """);
                for (boolean isParallel : MODES) {
                    sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                    assertQuery(sql).noRandomAccess().expectSize()
                            .withPlanContaining(projectionPlan(isParallel, 3)).returns(expected);
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
    public void testProjectionLoopingKeySinkMatchesAsOfJoin() throws Exception {
        // debug.cairo.copier.type=3 makes the join key sinks fall back to the looping
        // implementation, which a key too large for a generated sink also takes when
        // cairo.sql.copier.chunked=false. A keyed projection must still match the ASOF JOIN.
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, 3);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL, venue SYMBOL, i INT, v VARCHAR) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, venue SYMBOL, i INT, v VARCHAR, bid DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("""
                    INSERT INTO trades
                    SELECT x, '2026-01-01'::TIMESTAMP + x * 28_333_333L, rnd_symbol('A', 'B', 'C', 'Z'),
                           rnd_symbol('X', 'Y'), rnd_int(0, 4, 0), rnd_varchar('A', 'B', 'C', 'Z')
                    FROM long_sequence(300)
                    """);
            execute("""
                    INSERT INTO quotes
                    SELECT '2026-01-01'::TIMESTAMP + x * 16_666_666L, rnd_symbol('A', 'B', 'C'),
                           rnd_symbol('X', 'Y', 'W'), rnd_int(0, 3, 0), rnd_varchar('A', 'B', 'C'), rnd_double()
                    FROM long_sequence(600)
                    """);
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String on : List.of("ON (sym)", "ON (t.sym = q.sym AND t.venue = q.venue)", "ON (i)", "ON (v)")) {
                    assertMatchesAsOfJoin("trades", "quotes", on, "t.id, q.bid, q.ts");
                }
            }
        });
    }

    @Test
    public void testProjectionManyOffsetsKeepFilterFrames() throws Exception {
        // Past 100 slots per master row, offsets times right-hand tables, the serial factory runs
        // the projection and the master's async filter scans frames of its usual size: a selective
        // filter at 7,201 offsets dispatches the reduce tasks of the filter on its own. Up to 100
        // slots the parallel factory cuts the master into frames of the small frame budget divided
        // by the slots, but no smaller than a tenth of the budget, so a scan makes at most 10
        // times the frames of a small-frame scan.
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
                // 100 offsets cut the trades into frames of 10,000 rows, the floor of a tenth of the
                // small frame budget: 10 times the two frames of the budget.
                Assert.assertEquals(20, assertMatchesSerialAndCountReduceTasks(context, select + "99s STEP 1s AS h WHERE t.price = 5"));
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
                        """).expectSize().returns("""
                        seconds\tn\tmatches
                        -3\t3\t0
                        -1\t3\t1
                        1\t3\t3
                        """);
            }
        });
    }

    @Test
    public void testProjectionMaterializedViewIncrementalRefresh() throws Exception {
        // An incremental refresh recomputes the SAMPLE BY buckets that new left-hand rows touch.
        // After each refresh the view must hold what its query returns over all rows, and a full
        // refresh must not change it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Quotes every 7 seconds for two days; D has no quotes.
            execute("""
                    INSERT INTO quotes
                    SELECT '2026-01-01'::TIMESTAMP + x * 7_000_000L, rnd_symbol('A', 'B', 'C'), rnd_double()
                    FROM long_sequence(24_000)
                    """);
            execute("""
                    INSERT INTO trades
                    SELECT '2026-01-01'::TIMESTAMP + x * 13_000_000L, rnd_symbol('A', 'B', 'C', 'D'), rnd_double()
                    FROM long_sequence(1_000)
                    """);
            final String query = """
                    SELECT ts, count() c, count(bid) cb, min(bid) lo, max(bid) hi FROM (
                        SELECT t.ts, t.sym, h.offset, q.bid FROM trades t
                        HORIZON JOIN quotes q ON (sym) LIST (0s, 1s, 5s) AS h
                    ) TIMESTAMP(ts) SAMPLE BY 10m
                    """;
            execute("CREATE MATERIALIZED VIEW mv WITH BASE trades AS (" + query + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertViewMatchesQuery(query);

            // Later trades open new buckets, and trades that arrive out of order, duplicates
            // included, land in buckets that the view already holds.
            execute("""
                    INSERT INTO trades
                    SELECT '2026-01-01T04:00:00'::TIMESTAMP + x * 11_000_000L, rnd_symbol('A', 'B', 'C', 'D'), rnd_double()
                    FROM long_sequence(1_000)
                    """);
            execute("""
                    INSERT INTO trades VALUES
                        ('2026-01-01T00:20:00.000000Z', 'A', 1.0),
                        ('2026-01-01T00:20:00.000000Z', 'A', 1.0),
                        ('2026-01-01T01:05:00.000000Z', 'D', 1.0),
                        ('2026-01-01T02:59:59.999999Z', 'C', 1.0)
                    """);
            drainWalAndMatViewQueues();
            assertViewMatchesQuery(query);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            assertViewMatchesQuery(query);
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
                                .expectSize().returns("""
                                        seconds\tn\ttotal
                                        1\t6\t56.0
                                        5\t6\t64.0
                                        """);
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
    public void testProjectionOrderByUnselectedColumn() throws Exception {
        // A projection keeps one row per left-hand row and offset, so it could sort by a column it
        // doesn't output, as a plain SELECT does. The optimiser rejects such a column for every
        // HORIZON JOIN instead, as it must for one with aggregates, where the column would become
        // a grouping key. Accepting it later won't change the result of any query that runs today.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String sql = """
                    SELECT t.sym, h.offset, q.bid FROM TaqTrade t
                    HORIZON JOIN TaqQuote q ON (sym) LIST (0s, 1s) AS h
                    ORDER BY t.price DESC, h.offset, q.bid
                    """;
            final int position = sql.indexOf("t.price");
            final String message = "ORDER BY expressions must appear in select list. Invalid column: t.price";
            final String outerPrefix = "SELECT count() FROM (";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery(sql).noLeakCheck().fails(position, message);
                assertQuery(outerPrefix + sql + ")").noLeakCheck().fails(outerPrefix.length() + position, message);
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
    public void testProjectionOuterFilterStaysAboveJoin() throws Exception {
        // An outer WHERE on a right-hand column filters the joined rows, as it does over an ASOF
        // JOIN. The same predicate means something else inside the HORIZON JOIN: in the ON clause
        // or in a right-hand sub-query it filters the quotes before matching, and in the HORIZON
        // JOIN's own WHERE it fails. The optimiser must keep the outer filter above the join.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String projection = "SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                final String projectionNode = isParallel
                        ? "        Async Horizon Join Projection workers: 1 offsets: 3\n"
                        : "        Horizon Join Projection offsets: 3\n";
                final String scans = """
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: TaqTrade
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: TaqQuote
                        """;
                assertQuery("SELECT * FROM (" + projection + ") WHERE bid > 11")
                        .noRandomAccess()
                        .timestamp("ts")
                        .withPlan("""
                                SelectedRecord
                                    Filter filter: 11<q.bid
                                """ + projectionNode + scans)
                        .returns("""
                                ts\tsym\toffset\tbid
                                2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0
                                2026-01-01T00:00:02.000000Z\tB\t-1000000\t20.0
                                2026-01-01T00:00:02.000000Z\tB\t0\t20.0
                                2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0
                                """);
                assertQuery("SELECT * FROM (" + projection + ") WHERE offset > 0")
                        .noRandomAccess()
                        .timestamp("ts")
                        .withPlan("""
                                SelectedRecord
                                    Filter filter: 0<h.offset
                                """ + projectionNode + scans)
                        .returns("""
                                ts\tsym\toffset\tbid
                                2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
                                2026-01-01T00:00:00.000000Z\tA\t1000000\t10.0
                                2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0
                                2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0
                                2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                                2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                                """);
                // In the ON clause, the predicate filters the quotes and keeps every trade and offset.
                assertQuery("""
                        SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (t.sym = q.sym AND q.bid > 11) LIST (-1s, 0s, 1s) AS h
                        """)
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\toffset\tbid
                                2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                                2026-01-01T00:00:00.000000Z\tA\t0\tnull
                                2026-01-01T00:00:00.000000Z\tA\t1000000\tnull
                                2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                                2026-01-01T00:00:00.000000Z\tA\t0\tnull
                                2026-01-01T00:00:00.000000Z\tA\t1000000\tnull
                                2026-01-01T00:00:02.000000Z\tA\t-1000000\tnull
                                2026-01-01T00:00:02.000000Z\tA\t0\tnull
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
                                """);
                assertQuery(projection + " WHERE q.bid > 11")
                        .noLeakCheck()
                        .fails(115, "WHERE clause of HORIZON JOIN can only reference left-hand side columns");
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
                        .returns("""
                                s\tdescription\tlabel\tn
                                \t\t\t4
                                A\tA\tA\t8
                                B\tB\tB\t2
                                """);
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
    public void testProjectionRangeSpanOverflow() throws Exception {
        // FROM and TO lie more than Long.MAX_VALUE timestamp units apart, so TO - FROM does not
        // fit a long. Depending on the STEP, the wrapped difference used to yield no offsets at
        // all, which both projection factories divide by, a negative number of them or a wrong
        // positive one. The span now counts as an unsigned number, so each of these ranges yields
        // all of its offsets. The values of a RANGE take no underscore separators.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("CREATE TABLE nanoTrades AS (SELECT ts::TIMESTAMP_NS ts, sym, price FROM TaqTrade) TIMESTAMP(ts) PARTITION BY DAY");
            final String micros = "SELECT t.sym, h.offset, q.bid FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) RANGE FROM ";
            final String nanos = "SELECT t.sym, h.offset, q.bid FROM nanoTrades t HORIZON JOIN TaqQuote q ON (sym) RANGE FROM ";
            final String summary = "SELECT count() AS n, count(bid) AS matches, count_distinct(offset) AS offsets, min(offset) AS lo, max(offset) AS hi FROM (";
            // 61 offsets, 31 of them non-negative. Only a non-negative offset finds a quote.
            final String wideRange = "-9000000000000000000U TO 9000000000000000000U STEP 300000000000000000U AS h";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // Unfiltered, with a JIT-compiled filter and with a Java filter, which the
                // parallel factory steals from the master. A and B have quotes; C has none.
                assertQuery(summary + micros + wideRange + ")").noRandomAccess().expectSize()
                        .withPlanContaining(projectionPlan(isParallel, 61))
                        .returns("n\tmatches\toffsets\tlo\thi\n366\t124\t61\t-9000000000000000000\t9000000000000000000\n");
                assertQuery(summary + micros + wideRange + " WHERE t.price > 15)").noRandomAccess().expectSize()
                        .withPlanContaining(projectionPlan(isParallel, 61))
                        .returns("n\tmatches\toffsets\tlo\thi\n183\t31\t61\t-9000000000000000000\t9000000000000000000\n");
                assertQuery(summary + micros + wideRange + " WHERE concat(t.sym, '') = 'A')").noRandomAccess().expectSize()
                        .withPlanContaining(projectionPlan(isParallel, 61))
                        .returns("n\tmatches\toffsets\tlo\thi\n183\t93\t61\t-9000000000000000000\t9000000000000000000\n");

                // The wrapped difference used to yield -3 offsets; the range has 181. They exceed
                // the slot cap, so the serial projection runs in both modes.
                assertQuery(summary + micros + "-9000000000000000000U TO 9000000000000000000U STEP 100000000000000000U AS h)")
                        .noRandomAccess().expectSize().withPlanContaining(projectionPlan(false, 181))
                        .returns("n\tmatches\toffsets\tlo\thi\n1086\t364\t181\t-9000000000000000000\t9000000000000000000\n");
                // The wrapped difference used to yield one offset; the range has 37.
                assertQuery(summary + micros + "-9000000000000000000U TO 9000000000000000000U STEP 500000000000000000U AS h)")
                        .noRandomAccess().expectSize().withPlanContaining(projectionPlan(isParallel, 37))
                        .returns("n\tmatches\toffsets\tlo\thi\n222\t76\t37\t-9000000000000000000\t9000000000000000000\n");
                // The narrowest span that overflows, one unit past Long.MAX_VALUE.
                assertQuery(summary + micros + "-4611686018427387904U TO 4611686018427387904U STEP 9223372036854775807U AS h)")
                        .noRandomAccess().expectSize().withPlanContaining(projectionPlan(isParallel, 2))
                        .returns("n\tmatches\toffsets\tlo\thi\n12\t4\t2\t-4611686018427387904\t4611686018427387903\n");
                // The span fits; the STEP of one unit overflows the number of offsets.
                assertQuery(micros + "0U TO 9223372036854775807U STEP 1U AS h")
                        .fails(micros.length(), "RANGE generates too many offsets [count>9223372036854775807");
                // 120,000 days exceed Long.MAX_VALUE in nanoseconds.
                assertQuery(summary + nanos + "-60000d TO 60000d STEP 50000d AS h)")
                        .noRandomAccess().expectSize().withPlanContaining(projectionPlan(isParallel, 3))
                        .returns("n\tmatches\toffsets\tlo\thi\n18\t4\t3\t-5184000000000000000\t3456000000000000000\n");

                // The same days fit a microsecond master.
                assertQuery("SELECT offset / 86_400_000_000 AS days, count() AS n FROM ("
                        + micros + "-60000d TO 60000d STEP 50000d AS h) GROUP BY days ORDER BY days")
                        .expectSize().returns("""
                                days\tn
                                -60000\t6
                                -10000\t6
                                40000\t6
                                """);
                // So does the widest span, exactly Long.MAX_VALUE.
                assertQuery("SELECT offset AS micros, count() AS n, count(bid) AS matches FROM ("
                        + micros + "-4611686018427387904U TO 4611686018427387903U STEP 9223372036854775807U AS h) GROUP BY micros ORDER BY micros")
                        .expectSize().returns("""
                                micros\tn\tmatches
                                -4611686018427387904\t6\t0
                                4611686018427387903\t6\t4
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
    public void testProjectionSelectStarColumnNames() throws Exception {
        // SELECT * lists the columns of the left-hand table, the horizon pseudo-table and the
        // right-hand tables. The parser places the pseudo-table just before the last right-hand
        // table, and a repeated name takes the next free numeric suffix.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                assertQuery("SELECT * FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) LIST (0s) AS h")
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\tprice\toffset\ttimestamp\tts1\tsym1\tbid\task
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t0\t2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t0\t2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0
                                2026-01-01T00:00:02.000000Z\tA\t10.0\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0
                                2026-01-01T00:00:02.000000Z\tB\t20.0\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t0\t2026-01-01T00:00:02.000000Z\t\t\tnull\tnull
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t0\t2026-01-01T00:00:02.000000Z\t\t\tnull\tnull
                                """);
                // The unkeyed second right-hand table matches the last quote at or before the
                // horizon timestamp, whatever its symbol.
                assertQuery("SELECT * FROM TaqTrade t HORIZON JOIN TaqQuote q ON (sym) HORIZON JOIN TaqQuote r LIST (0s) AS h")
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tsym\tprice\tts1\tsym1\tbid\task\toffset\ttimestamp\tts2\tsym2\tbid1\task1
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0\t0\t2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0
                                2026-01-01T00:00:00.000000Z\tA\t10.0\t2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0\t0\t2026-01-01T00:00:00.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0
                                2026-01-01T00:00:02.000000Z\tA\t10.0\t2026-01-01T00:00:00.000000Z\tA\t10.0\t12.0\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0
                                2026-01-01T00:00:02.000000Z\tB\t20.0\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t\t\tnull\tnull\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0
                                2026-01-01T00:00:02.000000Z\tC\t30.0\t\t\tnull\tnull\t0\t2026-01-01T00:00:02.000000Z\t2026-01-01T00:00:00.000000Z\tB\t20.0\t22.0
                                """);
            }
        });
    }

    @Test
    public void testProjectionSlaveFilterFailureRecovers() throws Exception {
        // A right-hand filter fails as the cursor initializes it or as the matcher reads a quote,
        // with one and with two right-hand tables. Without a pool, both factories run everything
        // on the reading thread. Each failure must leave nothing behind, and the same factory must
        // then read every row.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                for (String sql : SLAVE_FILTER_FAULT_QUERIES) {
                    assertSlaveFilterFailuresRecover(engine, sqlExecutionContext, isParallel, sql.formatted("TaqTrade", "TaqQuote"), 0, 1, 2, 5);
                }
            }
        });
    }

    @Test
    public void testProjectionSlaveFilterFailureRecoversWorkerPool() throws Exception {
        // The same failures with worker threads: the per-worker clones of the right-hand filter
        // initialize on the reading thread, and a clone that fails on a worker reports through
        // the task.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 300);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        createRandomTradesAndQuotes(engine, context, 3_000, 12_000, 10);
                        for (String sql : SLAVE_FILTER_FAULT_QUERIES) {
                            assertSlaveFilterFailuresRecover(engine, context, true, sql.formatted("trades", "quotes"), 0, 1, 100, 1_000, 10_000);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testProjectionSlaveFilters() throws Exception {
        // A filter on the right-hand table skips the slave rows it rejects: a horizon timestamp
        // matches the latest row at or before it that passes the filter, as an ASOF JOIN against
        // the filtered table does. The A quote at 00:00 has bid 10, so bid > 10 rejects it.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            final String keyedRows = """
                    ts\tsym\toffset\tbid
                    2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                    2026-01-01T00:00:00.000000Z\tA\t0\tnull
                    2026-01-01T00:00:00.000000Z\tA\t1000000\tnull
                    2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                    2026-01-01T00:00:00.000000Z\tA\t0\tnull
                    2026-01-01T00:00:00.000000Z\tA\t1000000\tnull
                    2026-01-01T00:00:02.000000Z\tA\t-1000000\tnull
                    2026-01-01T00:00:02.000000Z\tA\t0\tnull
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
            final String allNullRows = """
                    ts\tsym\toffset\tbid
                    2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                    2026-01-01T00:00:00.000000Z\tA\t0\tnull
                    2026-01-01T00:00:00.000000Z\tA\t1000000\tnull
                    2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                    2026-01-01T00:00:00.000000Z\tA\t0\tnull
                    2026-01-01T00:00:00.000000Z\tA\t1000000\tnull
                    2026-01-01T00:00:02.000000Z\tA\t-1000000\tnull
                    2026-01-01T00:00:02.000000Z\tA\t0\tnull
                    2026-01-01T00:00:02.000000Z\tA\t1000000\tnull
                    2026-01-01T00:00:02.000000Z\tB\t-1000000\tnull
                    2026-01-01T00:00:02.000000Z\tB\t0\tnull
                    2026-01-01T00:00:02.000000Z\tB\t1000000\tnull
                    2026-01-01T00:00:02.000000Z\tC\t-1000000\tnull
                    2026-01-01T00:00:02.000000Z\tC\t0\tnull
                    2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                    2026-01-01T00:00:02.000000Z\tC\t-1000000\tnull
                    2026-01-01T00:00:02.000000Z\tC\t0\tnull
                    2026-01-01T00:00:02.000000Z\tC\t1000000\tnull
                    """;
            final String select = "SELECT t.ts, t.sym, h.offset, q.bid FROM TaqTrade t ";
            final String offsets = " LIST (-1s, 0s, 1s) AS h";
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // The filter in a subquery and in the ON clause.
                assertQuery(select + "HORIZON JOIN (TaqQuote WHERE bid > 10) q ON (sym)" + offsets)
                        .noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3), "slave filter").returns(keyedRows);
                assertQuery(select + "HORIZON JOIN TaqQuote q ON (t.sym = q.sym AND q.bid > 10)" + offsets)
                        .noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3), "slave filter").returns(keyedRows);
                // Without a key, every horizon timestamp takes the latest row that passes. Both
                // 00:03 quotes pass, and the B quote comes last.
                assertQuery(select + "HORIZON JOIN (TaqQuote WHERE bid > 10) q" + offsets)
                        .noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3), "slave filter").returns("""
                                ts\tsym\toffset\tbid
                                2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                                2026-01-01T00:00:00.000000Z\tA\t0\t20.0
                                2026-01-01T00:00:00.000000Z\tA\t1000000\t20.0
                                2026-01-01T00:00:00.000000Z\tA\t-1000000\tnull
                                2026-01-01T00:00:00.000000Z\tA\t0\t20.0
                                2026-01-01T00:00:00.000000Z\tA\t1000000\t20.0
                                2026-01-01T00:00:02.000000Z\tA\t-1000000\t20.0
                                2026-01-01T00:00:02.000000Z\tA\t0\t20.0
                                2026-01-01T00:00:02.000000Z\tA\t1000000\t22.0
                                2026-01-01T00:00:02.000000Z\tB\t-1000000\t20.0
                                2026-01-01T00:00:02.000000Z\tB\t0\t20.0
                                2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0
                                2026-01-01T00:00:02.000000Z\tC\t-1000000\t20.0
                                2026-01-01T00:00:02.000000Z\tC\t0\t20.0
                                2026-01-01T00:00:02.000000Z\tC\t1000000\t22.0
                                2026-01-01T00:00:02.000000Z\tC\t-1000000\t20.0
                                2026-01-01T00:00:02.000000Z\tC\t0\t20.0
                                2026-01-01T00:00:02.000000Z\tC\t1000000\t22.0
                                """);
                // A Java filter that is not thread-safe. Without a worker pool the owner thread
                // matches every frame with the original filter, so the worker clones stay idle;
                // testProjectionWorkerPoolSlaveFilterClonesMatchSerial() runs them. Only the A
                // quotes pass.
                assertQuery(select + "HORIZON JOIN (TaqQuote WHERE concat(sym, '_') = 'A_') q" + offsets)
                        .noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3), "slave filter").returns("""
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
                                2026-01-01T00:00:02.000000Z\tB\t-1000000\t10.0
                                2026-01-01T00:00:02.000000Z\tB\t0\t10.0
                                2026-01-01T00:00:02.000000Z\tB\t1000000\t14.0
                                2026-01-01T00:00:02.000000Z\tC\t-1000000\t10.0
                                2026-01-01T00:00:02.000000Z\tC\t0\t10.0
                                2026-01-01T00:00:02.000000Z\tC\t1000000\t14.0
                                2026-01-01T00:00:02.000000Z\tC\t-1000000\t10.0
                                2026-01-01T00:00:02.000000Z\tC\t0\t10.0
                                2026-01-01T00:00:02.000000Z\tC\t1000000\t14.0
                                """);
                // A filter that rejects every row leaves every slave column NULL, keyed or not.
                assertQuery(select + "HORIZON JOIN (TaqQuote WHERE false) q ON (sym)" + offsets)
                        .noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 3), "slave filter: false").returns(allNullRows);
                assertQuery(select + "HORIZON JOIN (TaqQuote WHERE bid > 100) q" + offsets)
                        .noRandomAccess().expectSize().timestamp("ts").returns(allNullRows);
                // Every slave of a multi-slave join applies its own filter.
                assertQuery("""
                        SELECT t.ts, t.sym, h.offset, q.bid, r.ask FROM TaqTrade t
                        HORIZON JOIN (TaqQuote WHERE bid > 10) q ON (sym)
                        HORIZON JOIN (TaqQuote WHERE ask < 16) r LIST (0s, 1s) AS h
                        """).noRandomAccess().expectSize().timestamp("ts")
                        .withPlanContaining(projectionPlan(isParallel, 2), "slave filter").returns("""
                                ts\tsym\toffset\tbid\task
                                2026-01-01T00:00:00.000000Z\tA\t0\tnull\t12.0
                                2026-01-01T00:00:00.000000Z\tA\t1000000\tnull\t12.0
                                2026-01-01T00:00:00.000000Z\tA\t0\tnull\t12.0
                                2026-01-01T00:00:00.000000Z\tA\t1000000\tnull\t12.0
                                2026-01-01T00:00:02.000000Z\tA\t0\tnull\t12.0
                                2026-01-01T00:00:02.000000Z\tA\t1000000\t14.0\t12.0
                                2026-01-01T00:00:02.000000Z\tB\t0\t20.0\t12.0
                                2026-01-01T00:00:02.000000Z\tB\t1000000\t22.0\t12.0
                                2026-01-01T00:00:02.000000Z\tC\t0\tnull\t12.0
                                2026-01-01T00:00:02.000000Z\tC\t1000000\tnull\t12.0
                                2026-01-01T00:00:02.000000Z\tC\t0\tnull\t12.0
                                2026-01-01T00:00:02.000000Z\tC\t1000000\tnull\t12.0
                                """);
                // The rows match those of an ASOF JOIN against the filtered table.
                assertMatchesAsOfJoin("TaqTrade", "(TaqQuote WHERE bid > 10)", "ON (sym)", "t.sym, t.price, q.bid, q.ask");
                assertMatchesAsOfJoin("TaqTrade", "(TaqQuote WHERE bid > 10)", "", "t.sym, t.price, q.sym, q.bid");
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
                        SelectedRecord
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
                        SelectedRecord
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
                        SelectedRecord
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
                    SelectedRecord
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
                    SelectedRecord
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
                    SelectedRecord
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
    public void testProjectionTimeoutFiberHostPool() throws Exception {
        // The thread that finds the timeout ahead of a task cancels the frame sequence. Under a
        // fiber host pool the sequence then fails the reading thread itself; under a legacy pool
        // only the cursor does. Each pool mode therefore has its test, where the other worker
        // pool tests of this class pick one at random.
        testProjectionTimeout(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testProjectionTimeoutLegacyPool() throws Exception {
        testProjectionTimeout(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testProjectionTimeoutSerial() throws Exception {
        testProjectionTimeout(null);
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
    public void testProjectionViewUnderSampleBy() throws Exception {
        // A view over the projection keeps the master's designated timestamp, so a SAMPLE BY over
        // the view buckets one row per trade and offset by the trade's timestamp.
        assertMemoryLeak(() -> {
            createTradesAndQuotes();
            execute("""
                    CREATE VIEW TaqMarkout AS (
                        SELECT t.ts, q.bid FROM TaqTrade t
                        HORIZON JOIN TaqQuote q ON (sym) LIST (-1s, 0s, 1s) AS h
                    )
                    """);
            for (boolean isParallel : MODES) {
                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                // The leak check of the assertion clears the engine, the view definition included.
                assertQuery("SELECT ts, count() AS n, sum(bid) AS bids FROM TaqMarkout SAMPLE BY 1s")
                        .noLeakCheck().noRandomAccess().timestamp("ts").returns("""
                                ts\tn\tbids
                                2026-01-01T00:00:00.000000Z\t6\t40.0
                                2026-01-01T00:00:02.000000Z\t12\t96.0
                                """);
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
    public void testProjectionWorkerPoolSlaveFilterClonesMatchSerial() throws Exception {
        // The code generator compiles a clone of a right-hand filter that is not thread-safe for
        // every worker slot, and the matcher of each slot must use its own clone. concat() is
        // such a filter: it builds its string in a sink of its own, so threads that share one
        // filter corrupt each other's strings or fail, and a worker matcher without a filter
        // accepts every quote. Frames of at most 100 trades, each matched whole by one task,
        // leave hundreds of tasks per pass to the four workers. The parallel rows must equal the
        // serial rows, order included.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 300);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        createRandomTradesAndQuotes(engine, context, 20_000, 100_000, 30);
                        final ObjList<String> queries = new ObjList<>(
                                // Keyed: the s3 trades find no quote.
                                """
                                        SELECT t.id, h.offset, q.bid FROM trades t
                                        HORIZON JOIN (quotes WHERE concat(sym, '_') != 's3_') q ON (sym) LIST (-1s, 0s, 1s) AS h
                                        """,
                                // Unkeyed: only 2 of the 31 quote symbols pass.
                                """
                                        SELECT t.id, h.offset, q.bid, q.sym FROM trades t
                                        HORIZON JOIN (quotes WHERE concat(sym, '_') IN ('s1_', 's2_')) q LIST (-1s, 0s, 1s) AS h
                                        """,
                                // Two slaves, each with clones of its own filter.
                                """
                                        SELECT t.id, h.offset, q.bid, r.bid FROM trades t
                                        HORIZON JOIN (quotes WHERE concat(sym, '_') != 's3_') q ON (sym)
                                        HORIZON JOIN (quotes WHERE concat(sym, '_') IN ('s1_', 's2_')) r LIST (-1s, 1s) AS h
                                        """
                        );
                        final StringSink expected = new StringSink();
                        for (int i = 0, n = queries.size(); i < n; i++) {
                            final String sql = queries.getQuick(i);
                            context.setParallelHorizonJoinEnabled(false);
                            TestUtils.printSql(engine, context, sql, expected);
                            context.setParallelHorizonJoinEnabled(true);
                            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().noRandomAccess().expectSize()
                                    .withPlanContaining("Horizon Join Projection workers: 4", "slave filter: concat")
                                    .returns(expected);
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

    private static void assertReadsAllRows(CairoEngine engine, SqlExecutionContext context, RecordCursorFactory factory, String expected) throws Exception {
        context.getCircuitBreaker().resetTimer();
        new QueryAssertion(engine, factory).withContext(context).noRandomAccess().expectSize().returns(expected);
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

    // Fails the right-hand filters of the query on each init call that a cursor open makes, one
    // after another, and then on the given evaluations. After each failure the same factory must
    // read the rows of the query without the filters.
    private static void assertSlaveFilterFailuresRecover(
            CairoEngine engine,
            SqlExecutionContext context,
            boolean isParallel,
            String sql,
            int... failAfterEvaluations
    ) throws Exception {
        final StringSink expectedSink = new StringSink();
        context.setParallelHorizonJoinEnabled(false);
        TestUtils.printSql(engine, context, sql.replace(" WHERE test_fault()", ""), expectedSink);
        final String expected = expectedSink.toString();
        context.setParallelHorizonJoinEnabled(isParallel);
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            assertProjectionFactory(factory, isParallel);
            TestUtils.assertContains(explain(engine, context, sql), "slave filter: test_fault()");
            int initFailureCount = 0;
            while (true) {
                TestFaultFunctionFactory.armToFailAfterInits(initFailureCount);
                boolean isFailed = false;
                try (RecordCursor cursor = factory.getCursor(context)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected init failure");
                    isFailed = true;
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                // A failure that the cursor swallows would pass off missing rows as the result.
                Assert.assertEquals(sql, TestFaultFunctionFactory.faultsTriggered() > 0, isFailed);
                if (!isFailed) {
                    break;
                }
                initFailureCount++;
                assertReadsAllRows(engine, context, factory, expected);
            }
            Assert.assertTrue("expected an init failure: " + sql, initFailureCount > 0);

            int evaluationFailureCount = 0;
            for (int failAfter : failAfterEvaluations) {
                TestFaultFunctionFactory.armToFailAfter(failAfter);
                boolean isFailed = false;
                try (RecordCursor cursor = factory.getCursor(context)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected failure");
                    isFailed = true;
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                Assert.assertEquals(sql, TestFaultFunctionFactory.faultsTriggered() > 0, isFailed);
                if (isFailed) {
                    evaluationFailureCount++;
                }
                assertReadsAllRows(engine, context, factory, expected);
            }
            Assert.assertTrue("expected an evaluation failure: " + sql, evaluationFailureCount > 0);
        }
    }

    // Times one execution after another out and, after each, reads every row from the same
    // factory again. First the clock of the breakers trips on each reading the open makes, in
    // turn: every breaker check on the way to an open cursor fails the open once, with whatever
    // the open holds by then. Then, for each of tripRows, the reader takes that many rows before
    // the clock trips, and the next breaker check that reads it, on any thread, times the query
    // out.
    private static void assertTimeoutRecovers(
            CairoEngine engine,
            SqlExecutionContext context,
            TrippingBreakerConfiguration breakerConfiguration,
            boolean isParallel,
            String sql,
            String expected,
            int... tripRows
    ) throws Exception {
        context.setParallelHorizonJoinEnabled(isParallel);
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            assertProjectionFactory(factory, isParallel);
            int openFailureCount = 0;
            while (true) {
                // A server starts the timer ahead of every execution.
                context.getCircuitBreaker().resetTimer();
                breakerConfiguration.tripOnReading(openFailureCount + 1);
                try (RecordCursor ignored = factory.getCursor(context)) {
                    // The open made fewer readings than that, and each of them has failed it.
                    break;
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
                    openFailureCount++;
                } finally {
                    breakerConfiguration.disarm();
                }
                assertReadsAllRows(engine, context, factory, expected);
            }
            Assert.assertTrue("expected a timeout at open: " + sql, openFailureCount > 0);

            for (int tripRow : tripRows) {
                context.getCircuitBreaker().resetTimer();
                long rowCount = 0;
                try (RecordCursor cursor = factory.getCursor(context)) {
                    while (rowCount < tripRow) {
                        Assert.assertTrue(cursor.hasNext());
                        rowCount++;
                    }
                    breakerConfiguration.tripOnReading(1);
                    while (cursor.hasNext()) {
                        rowCount++;
                    }
                    // Ending without an error passes off the rows read so far as the whole result.
                    Assert.fail("expected a timeout past row " + tripRow + ", read " + rowCount + " rows: " + sql);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
                } finally {
                    breakerConfiguration.disarm();
                }
                assertReadsAllRows(engine, context, factory, expected);
            }
        }
    }

    private static void createRandomTradesAndQuotes(CairoEngine engine, SqlExecutionContext context, int tradeRows, int quoteRows, int symbols) throws Exception {
        // Trades and quotes span several hourly partitions and interleave in time.
        engine.execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR", context);
        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR", context);
        engine.execute("""
                INSERT INTO trades
                SELECT x, '2026-01-01'::TIMESTAMP + x * (10_800_000_000L / %d), ('s' || rnd_int(0, %d, 0))::SYMBOL, rnd_double()
                FROM long_sequence(%d)
                """.formatted(tradeRows, symbols - 1, tradeRows), context);
        engine.execute("""
                INSERT INTO quotes
                SELECT '2026-01-01'::TIMESTAMP - 60_000_000L + x * (10_900_000_000L / %d), ('s' || rnd_int(0, %d, 0))::SYMBOL, rnd_double()
                FROM long_sequence(%d)
                """.formatted(quoteRows, symbols, quoteRows), context);
    }

    // One trade a second, ids from 1, with the symbols s0, s1 and s2 taking turns. From the
    // fourth second to 3 seconds past the last trade, every second carries a quote on the symbol
    // whose turn it is, and the bid is that second. The first three trades therefore find no
    // quote at an offset under 3 seconds.
    private static void createSteppedTradesAndQuotes(CairoEngine engine, SqlExecutionContext context, int tradeCount) throws Exception {
        engine.execute("CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("""
                INSERT INTO trades
                SELECT x, '2026-01-01'::TIMESTAMP + x * 1_000_000L, ('s' || (x %% 3))::SYMBOL
                FROM long_sequence(%d)
                """.formatted(tradeCount), context);
        engine.execute("""
                INSERT INTO quotes
                SELECT '2026-01-01'::TIMESTAMP + (x + 3) * 1_000_000L, ('s' || (x %% 3))::SYMBOL, (x + 3)::DOUBLE
                FROM long_sequence(%d)
                """.formatted(tradeCount), context);
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

    // The rows of SELECT t.id, h.offset, q.bid over createSteppedTradesAndQuotes(), joined on the
    // symbol at the offsets of listOffsets(offsetCount).
    private static String steppedRows(int tradeCount, int offsetCount) {
        final StringSink sink = new StringSink();
        sink.put("id\toffset\tbid\n");
        for (int id = 1; id <= tradeCount; id++) {
            for (int offset = 0; offset < offsetCount; offset++) {
                // The symbol of the trade is quoted every 3 seconds; quotes end at tradeCount + 3.
                final int quoteSecond = id + 3 * ((Math.min(id + offset, tradeCount + 3) - id) / 3);
                sink.put(id).put('\t').put(offset * 1_000_000L).put('\t');
                if (quoteSecond > 3) {
                    sink.put(quoteSecond).put(".0\n");
                } else {
                    sink.put("null\n");
                }
            }
        }
        return sink.toString();
    }

    // Times a row-preserving HORIZON JOIN out at several points of its execution. The cursor has
    // to fail at each of them, leak nothing, and read every row again on the next execution of
    // the same factory. Without a pool the serial factory runs the projection, with one the
    // parallel factory does.
    private static void testProjectionTimeout(@Nullable WorkerPoolMode poolMode) throws Exception {
        // At 3 offsets a frame of the parallel factory and a batch of the serial one hold 10
        // trades, 30 rows. At 30 offsets a frame holds one trade.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 30);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 10);
        if (poolMode != null) {
            // A task matches the first 4 trades of a frame of 10 and the reading thread matches
            // the rest. The serial factory keeps the default: its master then scans the trades
            // as one page frame and checks the breaker no more once it has returned a row.
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 12);
        }
        assertMemoryLeak(() -> {
            // A breaker reads the clock on one counted check in 5 under a pool, where the reading
            // thread makes 18 such checks as it matches the rest of a frame. The serial factory
            // gets one in 100, more than its scan has batches: the check its cursor makes per
            // batch could not find a timeout alone, the one it makes per matched row has to.
            final TrippingBreakerConfiguration breakerConfiguration = new TrippingBreakerConfiguration(poolMode != null ? 5 : 100);
            // The reduce jobs build their breakers from the configuration of the engine, so the
            // workers read the same clock as the reading thread.
            circuitBreakerConfiguration = breakerConfiguration;
            try {
                final WorkerPool pool = poolMode != null ? new TestWorkerPool(4, poolMode) : null;
                TestUtils.execute(
                        pool,
                        (engine, compiler, context) -> {
                            final int tradeCount = 600;
                            createSteppedTradesAndQuotes(engine, context, tradeCount);
                            final String select = "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) ";
                            final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, breakerConfiguration);
                            try {
                                ((SqlExecutionContextImpl) context).with(circuitBreaker);
                                final String fewOffsets = select + listOffsets(3);
                                final String fewOffsetRows = steppedRows(tradeCount, 3);
                                if (pool == null) {
                                    // 0 rows: the open cursor times out reading its first batch.
                                    // 315 rows: the reader is inside a batch and the master checks
                                    // the breaker no more, so the cursor itself has to find the
                                    // timeout as it matches the batches that follow.
                                    assertTimeoutRecovers(engine, context, breakerConfiguration, false, fewOffsets, fewOffsetRows, 0, 315);
                                } else {
                                    // 0 rows: the thread that picks up the first task finds the
                                    // timeout ahead of it and cancels the frame sequence. 315
                                    // rows: the reading thread times out matching the rest of the
                                    // eleventh frame. 1,771 rows: the reader is in the last frame,
                                    // every task has finished, and nothing but the reading thread,
                                    // matching the rest of that frame, checks the breaker any more.
                                    assertTimeoutRecovers(engine, context, breakerConfiguration, true, fewOffsets, fewOffsetRows, 0, 315, 1_771);
                                    // 45 rows: the reader is in the second of 600 frames, and the
                                    // queue never holds more tasks than its capacity, so frames
                                    // remain to dispatch. The thread that picks up one of their
                                    // tasks finds the timeout ahead of it and cancels the frame
                                    // sequence. The cursor then has to fail instead of ending on
                                    // the rows of the frames reduced earlier.
                                    Assert.assertTrue(configuration.getPageFrameReduceQueueCapacity() + 1 < tradeCount);
                                    final String manyOffsets = select + listOffsets(30);
                                    final String manyOffsetRows = steppedRows(tradeCount, 30);
                                    assertTimeoutRecovers(engine, context, breakerConfiguration, true, manyOffsets, manyOffsetRows, 0, 45);
                                }
                            } finally {
                                Misc.free(circuitBreaker);
                            }
                        },
                        configuration,
                        LOG
                );
            } finally {
                // Scope the override to this block; tearDown() would reset it as well.
                circuitBreakerConfiguration = null;
            }
        });
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

    private void assertViewMatchesQuery(String query) throws Exception {
        final StringSink expected = new StringSink();
        printSql(query, expected);
        Assert.assertTrue(expected.toString().split("\n").length > 2);
        // A nested leak check would clear the engine and, with it, the view's refresh state.
        assertQuery("mv").noLeakCheck().expectSize().timestamp("ts").returns(expected);
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

    // A query timeout of one millisecond on a clock that reads zero until a test trips it and the
    // end of time from then on, shared by every breaker built from this configuration.
    private static class TrippingBreakerConfiguration extends DefaultSqlExecutionCircuitBreakerConfiguration {
        private final AtomicLong readingCount = new AtomicLong();
        private final int throttle;
        private volatile long tripReading = Long.MAX_VALUE;

        private TrippingBreakerConfiguration(int throttle) {
            this.throttle = throttle;
        }

        @Override
        public int getCircuitBreakerThrottle() {
            return throttle;
        }

        @Override
        public @NotNull MillisecondClock getClock() {
            return () -> readingCount.incrementAndGet() < tripReading ? 0 : Long.MAX_VALUE;
        }

        @Override
        public long getQueryTimeout() {
            return 1;
        }

        private void disarm() {
            tripReading = Long.MAX_VALUE;
        }

        // The clock trips on the given reading, counted from now on, and stays tripped.
        private void tripOnReading(int reading) {
            tripReading = readingCount.get() + reading;
        }
    }
}
