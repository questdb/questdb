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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for HORIZON JOIN SQL syntax.
 */
public class HorizonJoinTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(HorizonJoinTest.class);

    @Test
    public void testHorizonJoinCannotBeCombinedWithOtherJoins() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE other (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts)");

            // HORIZON JOIN after another join
            assertExceptionNoLeakCheck(
                    "SELECT h.offset, avg(p.price) " +
                            "FROM trades AS t " +
                            "JOIN other AS o ON (t.sym = o.sym) " +
                            "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                            "RANGE FROM -10s TO 10s STEP 1s AS h",
                    82,
                    "horizon join cannot be combined with other joins"
            );

            // Another join after HORIZON JOIN
            assertExceptionNoLeakCheck(
                    "SELECT h.offset, avg(p.price) " +
                            "FROM trades AS t " +
                            "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                            "RANGE FROM -10s TO 10s STEP 1s AS h " +
                            "JOIN other AS o ON (t.sym = o.sym)",
                    127,
                    "horizon join cannot be combined with other joins"
            );
        });
    }

    @Test
    public void testHorizonJoinEmptyList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT h.offset, avg(p.price) " +
                            "FROM trades AS t " +
                            "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                            "LIST () AS h",
                    97,
                    "at least one offset expression expected"
            );
        });
    }

    @Test
    public void testHorizonJoinMissingRangeOrList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT h.offset, avg(p.price) " +
                            "FROM trades AS t " +
                            "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                            "AS h", // Missing RANGE or LIST
                    91,
                    "'range' or 'list' expected"
            );
        });
    }

    /**
     * Test parallel execution of HORIZON JOIN GROUP BY with larger dataset.
     */
    @Test
    public void testHorizonJoinParallelExecution() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10);

        final int workerCount = 4;
        WorkerPool pool = new WorkerPool(() -> workerCount);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    String symbolGen = "rnd_symbol_zipf(1000, 2.0)";

                    // Create prices table with enough data points
                    engine.execute(
                            """
                                    CREATE TABLE prices (
                                        price_ts TIMESTAMP,
                                        sym SYMBOL,
                                        price DOUBLE)
                                    TIMESTAMP(price_ts) PARTITION BY HOUR
                                    """,
                            sqlExecutionContext
                    );

                    engine.execute(
                            String.format(
                                    """
                                            INSERT INTO prices SELECT
                                                generate_series,
                                                %s,
                                                9.0 + 2.0 * rnd_double()
                                            FROM generate_series('2025-12-01', '2025-12-01T02', '200u');
                                            """,
                                    symbolGen
                            ),
                            sqlExecutionContext
                    );

                    // Create orders table with integer amount to avoid floating-point precision issues
                    engine.execute(
                            """
                                    CREATE TABLE orders (
                                        order_ts TIMESTAMP,
                                        sym SYMBOL,
                                        amount LONG
                                    ) TIMESTAMP(order_ts);
                                    """,
                            sqlExecutionContext
                    );

                    engine.execute(
                            String.format(
                                    """
                                            INSERT INTO orders SELECT
                                              generate_series,
                                              %s,
                                              90 + rnd_long(0, 20, 0)
                                            FROM generate_series('2025-12-01', '2025-12-01T00:05', '1s');
                                            """,
                                    symbolGen
                            ),
                            sqlExecutionContext
                    );

                    // HORIZON JOIN query with RANGE -600s to 600s step 1s (1201 offsets)
                    final String sql = """
                            SELECT
                                h.offset / 1000000 AS sec_offs,
                                sum(amount),
                                count(*)
                            FROM orders AS t
                            HORIZON JOIN prices AS p ON (t.sym = p.sym)
                            RANGE FROM -600s TO 600s STEP 1s AS h
                            ORDER BY h.offset
                            """;

                    StringSink planSink = new StringSink();
                    try (
                            RecordCursorFactory planFactory = engine.select("EXPLAIN " + sql, sqlExecutionContext);
                            RecordCursor cursor = planFactory.getCursor(sqlExecutionContext)
                    ) {
                        CursorPrinter.println(cursor, planFactory.getMetadata(), planSink);
                    }
                    TestUtils.assertContains(planSink, "Async Markout GroupBy");

                    // Execute the query to verify it runs successfully
                    StringSink result = new StringSink();
                    engine.print(sql, result, sqlExecutionContext);
                    // Verify we got results (1201 offset values)
                    TestUtils.assertContains(result, "sec_offs");
                },
                configuration,
                LOG
        );
    }

    /**
     * Test that the Async Markout GroupBy factory appears in the query plan.
     */
    @Test
    public void testHorizonJoinQueryPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;");
            execute("CREATE TABLE prices (sym SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertPlanNoLeakCheck(
                    """
                            SELECT h.offset / 1000000 AS sec_off, avg(p.bid), avg(p.ask)
                            FROM trades AS t
                            HORIZON JOIN prices AS p ON (t.sym = p.sym)
                            RANGE FROM 0s TO 1s STEP 1s AS h
                            """,
                    """
                            VirtualRecord
                              functions: [offset/1000000,avg,avg1]
                                Async Markout GroupBy workers: 1 offsets: 2
                                  keys: [offset]
                                  values: [avg(p.bid),avg(p.ask)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: prices
                            """
            );
        });
    }

    @Test
    public void testHorizonJoinSmoke() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE prices (
                                price_ts TIMESTAMP,
                                sym SYMBOL,
                                price DOUBLE)
                            TIMESTAMP(price_ts) PARTITION BY HOUR
                            """
            );
            execute(
                    """
                            INSERT INTO prices VALUES
                                (0_000_000, 'AX', 2),
                                (1_100_000, 'AX', 4),
                                (3_100_000, 'AX', 8)
                            """
            );

            execute(
                    """
                            CREATE TABLE orders (
                                order_ts TIMESTAMP,
                                sym SYMBOL,
                                qty DOUBLE
                            ) TIMESTAMP(order_ts)
                            """
            );
            execute(
                    """
                            INSERT INTO orders VALUES
                                (0_000_000, 'AX', 100),
                                (1_000_000, 'AX', 200),
                                (2_000_000, 'AX', 300)
                            """
            );

            // Query with HORIZON JOIN
            String sql = """
                    SELECT h.offset / 1000000 AS sec_offs, avg(p.price)
                    FROM orders AS t
                    HORIZON JOIN prices AS p ON (t.sym = p.sym)
                    RANGE FROM 0s TO 2s STEP 1s AS h
                    ORDER BY sec_offs;
                    """;

            // Verify the query plan contains the Async Markout GroupBy factory
            StringSink planSink = new StringSink();
            try (
                    RecordCursorFactory planFactory = select("EXPLAIN " + sql);
                    RecordCursor cursor = planFactory.getCursor(sqlExecutionContext)
            ) {
                planSink.clear();
                CursorPrinter.println(cursor, planFactory.getMetadata(), planSink);
            }
            TestUtils.assertContains(planSink, "Async Markout GroupBy");

            // Verify results
            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg
                            0\t2.6666666666666665
                            1\t3.3333333333333335
                            2\t5.333333333333333
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithListBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            // Insert test data
            execute(
                    """
                            INSERT INTO prices VALUES
                                (0, 'AX', 10),
                                (1_000_000, 'AX', 20),
                                (2_000_000, 'AX', 30)
                            """
            );
            execute(
                    """
                            INSERT INTO trades VALUES
                                (1_000_000, 'AX', 100)
                            """
            );

            // LIST with explicit offsets in microseconds: 0, 1000000 (1s)
            // For trade at 1s:
            //   offset=0: look at 1s+0=1s -> ASOF to price at 1s -> 20
            //   offset=1000000: look at 1s+1s=2s -> ASOF to price at 2s -> 30
            String sql = "SELECT h.offset / 1000000 AS sec_offs, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "LIST (0, 1000000) AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg
                            0\t20.0
                            1\t30.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithRangeAndGroupByNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            // Explicit GROUP BY is not supported with HORIZON JOIN
            assertExceptionNoLeakCheck(
                    "SELECT h.offset, t.sym, avg(p.price) " +
                            "FROM trades AS t " +
                            "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                            "RANGE FROM -600s TO 600s STEP 1s AS h " +
                            "GROUP BY h.offset, t.sym",
                    145,
                    "GROUP BY cannot be used with HORIZON JOIN"
            );
        });
    }

    @Test
    public void testHorizonJoinWithRangeBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            // Insert test data
            // Prices at 0s, 1s, 2s, 3s for sym 'AX'
            execute(
                    """
                            INSERT INTO prices VALUES
                                (0_000_000, 'AX', 10),
                                (1_000_000, 'AX', 20),
                                (2_000_000, 'AX', 30),
                                (3_000_000, 'AX', 40)
                            """
            );

            // Trade at 1s for sym 'AX'
            execute(
                    """
                            INSERT INTO trades VALUES
                                (1_000_000, 'AX', 100)
                            """
            );

            // RANGE FROM 0s TO 2s STEP 1s gives offsets: 0, 1000000, 2000000 microseconds
            // For trade at 1s:
            //   offset=0: look at 1s+0=1s -> ASOF to price at 1s -> 20
            //   offset=1s: look at 1s+1s=2s -> ASOF to price at 2s -> 30
            //   offset=2s: look at 1s+2s=3s -> ASOF to price at 3s -> 40
            String sql = "SELECT h.offset / 1000000 AS sec_offs, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM 0s TO 2s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            // Verify the query plan
            assertPlanNoLeakCheck(
                    sql,
                    """
                            Radix sort light
                              keys: [sec_offs]
                                VirtualRecord
                                  functions: [offset/1000000,avg]
                                    Async Markout GroupBy workers: 1 offsets: 3
                                      keys: [offset]
                                      values: [avg(p.price)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: trades
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: prices
                            """
            );

            // Verify results
            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg
                            0\t20.0
                            1\t30.0
                            2\t40.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithRangeMinuteUnits() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            // Insert test data with minute-level timestamps
            // Prices at 0m, 1m, 2m, 3m for sym 'AX'
            execute(
                    """
                            INSERT INTO prices VALUES
                                (0, 'AX', 10),
                                (60_000_000, 'AX', 20),
                                (120_000_000, 'AX', 30),
                                (180_000_000, 'AX', 40)
                            """
            );

            // Trade at 1m for sym 'AX'
            execute(
                    """
                            INSERT INTO trades VALUES
                                (60_000_000, 'AX', 100)
                            """
            );

            // RANGE FROM 0m TO 2m STEP 1m gives offsets: 0, 60000000, 120000000 microseconds
            // For trade at 1m (60s):
            //   offset=0m: look at 1m+0=1m -> ASOF to price at 1m -> 20
            //   offset=1m: look at 1m+1m=2m -> ASOF to price at 2m -> 30
            //   offset=2m: look at 1m+2m=3m -> ASOF to price at 3m -> 40
            String sql = "SELECT h.offset / 60000000 AS min_offs, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM 0m TO 2m STEP 1m AS h " +
                    "ORDER BY min_offs";

            // Verify results
            assertQueryNoLeakCheck(
                    """
                            min_offs\tavg
                            0\t20.0
                            1\t30.0
                            2\t40.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;");
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('sym1', '2000-01-01T00:00:00.000000Z'),
                                ('sym2', '2000-01-01T00:00:05.000000Z'),
                                ('sym3', '2000-01-01T00:00:10.000000Z')
                            """
            );
            execute("CREATE TABLE prices (sym SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('sym1', 1, 2, '2000-01-01T00:00:00.000000Z'),
                                ('sym2', 3, 4, '2000-01-01T00:00:05.000000Z'),
                                ('sym3', 5, 6, '2000-01-01T00:00:10.000000Z')
                            """
            );

            assertQueryNoLeakCheck(
                    """
                            sec_off\tavg\tavg1
                            0\t3.0\t4.0
                            1\t3.0\t4.0
                            """,
                    """
                            SELECT h.offset / 1000000 AS sec_off, avg(p.bid), avg(p.ask)
                            FROM trades AS t
                            HORIZON JOIN prices AS p ON (t.sym = p.sym)
                            RANGE FROM 0s TO 1s STEP 1s AS h
                            ORDER BY sec_off
                            """,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithSymbolTableSources() throws Exception {
        // Test that symbol tables from both left (master) and right (slave) tables
        // are correctly resolved when used as keys and in aggregate functions
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (order_sym SYMBOL, category SYMBOL, ts TIMESTAMP, qty LONG) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (price_sym SYMBOL, exchange SYMBOL, ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts)");

            // Insert data with distinct symbol values
            execute(
                    """
                            INSERT INTO orders VALUES
                                ('AAPL', 'TECH', '2000-01-01T00:00:01.000000Z', 100),
                                ('AAPL', 'TECH', '2000-01-01T00:00:02.000000Z', 200),
                                ('GOOG', 'TECH', '2000-01-01T00:00:03.000000Z', 150),
                                ('MSFT', 'SOFT', '2000-01-01T00:00:04.000000Z', 300)
                            """
            );

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('AAPL', 'NYSE', '2000-01-01T00:00:00.500000Z', 100.0),
                                ('AAPL', 'NYSE', '2000-01-01T00:00:01.500000Z', 110.0),
                                ('AAPL', 'NASDAQ', '2000-01-01T00:00:02.500000Z', 120.0),
                                ('GOOG', 'NASDAQ', '2000-01-01T00:00:02.500000Z', 200.0),
                                ('MSFT', 'NYSE', '2000-01-01T00:00:03.500000Z', 300.0)
                            """
            );

            // Test 1: Left-hand symbol (order_sym) used as a grouping key
            // Right-hand symbol (exchange) used via first() aggregate
            assertQueryNoLeakCheck(
                    """
                            order_sym\tfirst\tavg
                            AAPL\tNYSE\t105.0
                            GOOG\tNASDAQ\t200.0
                            MSFT\tNYSE\t300.0
                            """,
                    """
                            SELECT t.order_sym, first(p.exchange), avg(p.price)
                            FROM orders AS t
                            HORIZON JOIN prices AS p ON (t.order_sym = p.price_sym)
                            RANGE FROM 0s TO 0s STEP 1s AS h
                            ORDER BY t.order_sym
                            """,
                    null,
                    true,
                    true
            );

            // Test 2: Both left and right symbols in SELECT
            assertQueryNoLeakCheck(
                    """
                            order_sym\tcategory\tfirst\tavg
                            AAPL\tTECH\tNYSE\t105.0
                            GOOG\tTECH\tNASDAQ\t200.0
                            MSFT\tSOFT\tNYSE\t300.0
                            """,
                    """
                            SELECT t.order_sym, t.category, first(p.exchange), avg(p.price)
                            FROM orders AS t
                            HORIZON JOIN prices AS p ON (t.order_sym = p.price_sym)
                            RANGE FROM 0s TO 0s STEP 1s AS h
                            ORDER BY t.order_sym
                            """,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithSymbolTableSourcesFirstLast() throws Exception {
        // Test first() and last() aggregates on slave symbols
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (order_sym SYMBOL, ts TIMESTAMP, qty LONG) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (price_sym SYMBOL, exchange SYMBOL, ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts)");

            // Insert orders - two orders for AAPL at different times
            execute(
                    """
                            INSERT INTO orders VALUES
                                ('AAPL', '2000-01-01T00:00:01.000000Z', 100),
                                ('AAPL', '2000-01-01T00:00:02.000000Z', 200)
                            """
            );

            // Insert prices - AAPL has NYSE then NASDAQ
            // Order at 1s will ASOF to price at 0.5s (NYSE)
            // Order at 2s will ASOF to price at 1.5s (NASDAQ)
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('AAPL', 'NYSE', '2000-01-01T00:00:00.500000Z', 100.0),
                                ('AAPL', 'NASDAQ', '2000-01-01T00:00:01.500000Z', 110.0)
                            """
            );

            // first() should return NYSE (from order at 1s)
            // last() should return NASDAQ (from order at 2s)
            assertQueryNoLeakCheck(
                    """
                            order_sym\tfirst\tlast
                            AAPL\tNYSE\tNASDAQ
                            """,
                    """
                            SELECT t.order_sym, first(p.exchange), last(p.exchange)
                            FROM orders AS t
                            HORIZON JOIN prices AS p ON (t.order_sym = p.price_sym)
                            RANGE FROM 0s TO 0s STEP 1s AS h
                            ORDER BY t.order_sym
                            """,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithSlaveSymbolAsKey() throws Exception {
        // Test using a slave symbol as a grouping key
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (order_sym SYMBOL, ts TIMESTAMP, qty LONG) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (price_sym SYMBOL, exchange SYMBOL, ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts)");

            // Insert orders for multiple symbols
            execute(
                    """
                            INSERT INTO orders VALUES
                                ('AAPL', '2000-01-01T00:00:01.000000Z', 100),
                                ('AAPL', '2000-01-01T00:00:02.000000Z', 200),
                                ('GOOG', '2000-01-01T00:00:03.000000Z', 150),
                                ('MSFT', '2000-01-01T00:00:04.000000Z', 300)
                            """
            );

            // Insert prices - different symbols on different exchanges
            // AAPL: NYSE at 0.5s, NASDAQ at 1.5s
            // GOOG: NASDAQ at 2.5s
            // MSFT: NYSE at 3.5s
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('AAPL', 'NYSE', '2000-01-01T00:00:00.500000Z', 100.0),
                                ('AAPL', 'NASDAQ', '2000-01-01T00:00:01.500000Z', 110.0),
                                ('GOOG', 'NASDAQ', '2000-01-01T00:00:02.500000Z', 200.0),
                                ('MSFT', 'NYSE', '2000-01-01T00:00:03.500000Z', 300.0)
                            """
            );

            // Group by slave symbol (p.exchange)
            // NYSE: AAPL order at 1s (price 100) + MSFT order at 4s (price 300) -> avg = 200, sum(qty) = 400
            // NASDAQ: AAPL order at 2s (price 110) + GOOG order at 3s (price 200) -> avg = 155, sum(qty) = 350
            assertQueryNoLeakCheck(
                    """
                            exchange\tsum\tavg
                            NASDAQ\t350\t155.0
                            NYSE\t400\t200.0
                            """,
                    """
                            SELECT p.exchange, sum(t.qty), avg(p.price)
                            FROM orders AS t
                            HORIZON JOIN prices AS p ON (t.order_sym = p.price_sym)
                            RANGE FROM 0s TO 0s STEP 1s AS h
                            ORDER BY p.exchange
                            """,
                    null,
                    true,
                    true
            );
        });
    }

    @Ignore("HORIZON JOIN without ON clause not yet supported")
    @Test
    public void testHorizonJoinWithoutOnClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, qty DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts)");

            // Insert test data
            execute(
                    """
                            INSERT INTO prices VALUES
                                (0, 10),
                                (1_000_000, 20),
                                (2_000_000, 30),
                                (3_000_000, 40)
                            """
            );
            execute(
                    """
                            INSERT INTO trades VALUES
                                (1_000_000, 100)
                            """
            );

            // HORIZON JOIN without ON clause - ASOF by timestamp only
            // RANGE FROM 0s TO 2s STEP 1s gives offsets: 0, 1000000, 2000000 microseconds
            // For trade at 1s:
            //   offset=0: look at 1s+0=1s -> ASOF to price at 1s -> 20
            //   offset=1s: look at 1s+1s=2s -> ASOF to price at 2s -> 30
            //   offset=2s: look at 1s+2s=3s -> ASOF to price at 3s -> 40
            String sql = "SELECT h.offset / 1000000 AS sec_offs, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p " +
                    "RANGE FROM 0s TO 2s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg
                            0\t20.0
                            1\t30.0
                            2\t40.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }
}
