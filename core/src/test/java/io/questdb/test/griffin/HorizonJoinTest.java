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
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class HorizonJoinTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(HorizonJoinTest.class);
    private final TestTimestampType leftTableTimestampType;
    private final TestTimestampType rightTableTimestampType;

    public HorizonJoinTest() {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        this.leftTableTimestampType = TestUtils.getTimestampType(rnd);
        this.rightTableTimestampType = TestUtils.getTimestampType(rnd);
    }

    @Test
    public void testHorizonJoinCannotBeCombinedWithOtherJoins() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE other (ts #TIMESTAMP, sym SYMBOL) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());

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
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

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
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

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

    @Test
    public void testHorizonJoinNotKeyedBasic() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 10),
                                ('1970-01-01T00:00:00.000000Z', 'BX', 100),
                                ('1970-01-01T00:00:01.000000Z', 'AX', 20),
                                ('1970-01-01T00:00:01.000000Z', 'BX', 200),
                                ('1970-01-01T00:00:02.000000Z', 'AX', 30)
                            """
            );

            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AX', 10),
                                ('1970-01-01T00:00:01.000000Z', 'BX', 20)
                            """
            );

            // Non-keyed query: no GROUP BY keys, only aggregates
            // For AX trade at 1s with offset 0: ASOF to AX price at 1s -> 20
            // For BX trade at 1s with offset 0: ASOF to BX price at 1s -> 200
            // avg price: (20+200)/2 = 110, sum qty: 10+20 = 30
            String sql = """
                    SELECT avg(p.price), sum(t.qty)
                    FROM trades AS t
                    HORIZON JOIN prices AS p ON (t.sym = p.sym)
                    RANGE FROM 0s TO 0s STEP 1s AS h
                    """;

            assertQueryNoLeakCheck(
                    """
                            avg\tsum
                            110.0\t30.0
                            """,
                    sql,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinNotKeyedFirstLastSymbol() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (order_sym SYMBOL, ts #TIMESTAMP, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (price_sym SYMBOL, exchange SYMBOL, ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('AAPL', '2000-01-01T00:00:01.000000Z', 100),
                                ('GOOG', '2000-01-01T00:00:02.000000Z', 200)
                            """
            );

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('AAPL', 'NYSE', '2000-01-01T00:00:00.500000Z', 100.0),
                                ('GOOG', 'NASDAQ', '2000-01-01T00:00:01.500000Z', 200.0)
                            """
            );

            // Order at 1s matches NYSE, order at 2s matches NASDAQ
            String sql = """
                    SELECT first(p.exchange), last(p.exchange), sum(t.qty)
                    FROM orders AS t
                    HORIZON JOIN prices AS p ON (t.order_sym = p.price_sym)
                    RANGE FROM 0s TO 0s STEP 1s AS h
                    """;

            assertQueryNoLeakCheck(
                    """
                            first\tlast\tsum
                            NYSE\tNASDAQ\t300
                            """,
                    sql,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinNotKeyedMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 10),
                                ('1970-01-01T00:00:01.000000Z', 'AX', 20),
                                ('1970-01-01T00:00:02.000000Z', 'AX', 30)
                            """
            );

            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AX', 100),
                                ('1970-01-01T00:00:02.000000Z', 'AX', 200)
                            """
            );

            // For trade at 1s with offset 0: price = 20
            // For trade at 2s with offset 0: price = 30
            String sql = """
                    SELECT min(p.price), max(p.price), avg(p.price), sum(p.price), count(*)
                    FROM trades AS t
                    HORIZON JOIN prices AS p ON (t.sym = p.sym)
                    RANGE FROM 0s TO 0s STEP 1s AS h
                    """;

            assertQueryNoLeakCheck(
                    """
                            min\tmax\tavg\tsum\tcount
                            20.0\t30.0\t25.0\t50.0\t2
                            """,
                    sql,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinNotKeyedMultipleOffsets() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 10),
                                ('1970-01-01T00:00:01.000000Z', 'AX', 20),
                                ('1970-01-01T00:00:02.000000Z', 'AX', 30),
                                ('1970-01-01T00:00:03.000000Z', 'AX', 40)
                            """
            );

            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AX', 100)
                            """
            );

            // For trade at 1s: offset=0 -> 20, offset=1s -> 30, offset=2s -> 40
            // sum: 90, avg: 30, count: 3
            String sql = """
                    SELECT sum(p.price), avg(p.price), count(*)
                    FROM trades AS t
                    HORIZON JOIN prices AS p ON (t.sym = p.sym)
                    RANGE FROM 0s TO 2s STEP 1s AS h
                    """;

            assertQueryNoLeakCheck(
                    """
                            sum\tavg\tcount
                            90.0\t30.0\t3
                            """,
                    sql,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinNotKeyedQueryPlan() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            assertPlanNoLeakCheck(
                    """
                            SELECT avg(p.price), sum(t.qty)
                            FROM trades AS t
                            HORIZON JOIN prices AS p ON (t.sym = p.sym)
                            RANGE FROM 0s TO 1s STEP 1s AS h
                            """,
                    """
                            Async Horizon Join workers: 1 offsets: 2
                              values: [avg(p.price),sum(t.qty)]
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
    public void testHorizonJoinNotKeyedWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 10),
                                ('1970-01-01T00:00:01.000000Z', 'AX', 20),
                                ('1970-01-01T00:00:02.000000Z', 'BX', 100),
                                ('1970-01-01T00:00:03.000000Z', 'BX', 200)
                            """
            );

            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AX', 10),
                                ('1970-01-01T00:00:02.000000Z', 'AX', 20),
                                ('1970-01-01T00:00:03.000000Z', 'BX', 30)
                            """
            );

            // Filter to only AX trades
            String sql = """
                    SELECT avg(p.price), sum(t.qty)
                    FROM trades AS t
                    HORIZON JOIN prices AS p ON (t.sym = p.sym)
                    RANGE FROM 0s TO 0s STEP 1s AS h
                    WHERE t.sym = 'AX'
                    """;

            assertQueryNoLeakCheck(
                    """
                            avg\tsum
                            20.0\t30.0
                            """,
                    sql,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinNotKeyedWithoutOnClause() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 10),
                                ('1970-01-01T00:00:01.000000Z', 20),
                                ('1970-01-01T00:00:02.000000Z', 30),
                                ('1970-01-01T00:00:03.000000Z', 40)
                            """
            );

            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 100),
                                ('1970-01-01T00:00:02.000000Z', 200)
                            """
            );

            // For trade at 1s: offset 0 -> 20, offset 1s -> 30
            // For trade at 2s: offset 0 -> 30, offset 1s -> 40
            // sum prices: 120, avg: 30, sum qty: 600
            String sql = """
                    SELECT sum(p.price), avg(p.price), sum(t.qty)
                    FROM trades AS t
                    HORIZON JOIN prices AS p
                    RANGE FROM 0s TO 1s STEP 1s AS h
                    """;

            assertQueryNoLeakCheck(
                    """
                            sum\tavg\tsum1
                            120.0\t30.0\t600.0
                            """,
                    sql,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinParallelExecution() throws Exception {
        // Test parallel execution of HORIZON JOIN GROUP BY with larger dataset
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
                    TestUtils.assertContains(planSink, "Async Horizon Join");

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

    @Test
    public void testHorizonJoinQueryPlan() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (sym SYMBOL, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (sym SYMBOL, bid DOUBLE, ask DOUBLE, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

            assertPlanNoLeakCheck(
                    "SELECT h.offset / " + getSecondsDivisor() + " AS sec_off, avg(p.bid), avg(p.ask) " +
                            "FROM trades AS t " +
                            "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                            "RANGE FROM 0s TO 1s STEP 1s AS h",
                    "VirtualRecord\n" +
                            "  functions: [offset/" + getSecondsDivisor() + ",avg,avg1]\n" +
                            "    Async Horizon Join workers: 1 offsets: 2\n" +
                            "      keys: [offset]\n" +
                            "      values: [avg(p.bid),avg(p.ask)]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: trades\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: prices\n"
            );
        });
    }

    @Test
    public void testHorizonJoinSmoke() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE prices (
                                price_ts #TIMESTAMP,
                                sym SYMBOL,
                                price DOUBLE)
                            TIMESTAMP(price_ts) PARTITION BY HOUR
                            """,
                    rightTableTimestampType.getTypeName()
            );
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 2),
                                ('1970-01-01T00:00:01.100000Z', 'AX', 4),
                                ('1970-01-01T00:00:03.100000Z', 'AX', 8)
                            """
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE orders (
                                order_ts #TIMESTAMP,
                                sym SYMBOL,
                                qty DOUBLE
                            ) TIMESTAMP(order_ts)
                            """,
                    leftTableTimestampType.getTypeName()
            );
            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 100),
                                ('1970-01-01T00:00:01.000000Z', 'AX', 200),
                                ('1970-01-01T00:00:02.000000Z', 'AX', 300)
                            """
            );

            // Query with HORIZON JOIN
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM 0s TO 2s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            // Verify the query plan contains the Async Markout GroupBy factory
            StringSink planSink = new StringSink();
            try (
                    RecordCursorFactory planFactory = select("EXPLAIN " + sql);
                    RecordCursor cursor = planFactory.getCursor(sqlExecutionContext)
            ) {
                planSink.clear();
                CursorPrinter.println(cursor, planFactory.getMetadata(), planSink);
            }
            TestUtils.assertContains(planSink, "Async Horizon Join");

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
            executeWithRewriteTimestamp(
                    "CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)",
                    rightTableTimestampType.getTypeName()
            );

            // Insert test data
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 10),
                                ('1970-01-01T00:00:01.000000Z', 'AX', 20),
                                ('1970-01-01T00:00:02.000000Z', 'AX', 30)
                            """
            );
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AX', 100)
                            """
            );

            // LIST with explicit offsets in microseconds: 0, 1000000 (1s)
            // For trade at 1s:
            //   offset=0: look at 1s+0=1s -> ASOF to price at 1s -> 20
            //   offset=1000000: look at 1s+1s=2s -> ASOF to price at 2s -> 30
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price) " +
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
    public void testHorizonJoinWithLongKey() throws Exception {
        // Test HORIZON JOIN with LONG column as ASOF JOIN key
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, account_id LONG, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, account_id LONG, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 1, 100.0),
                                ('1970-01-01T00:00:00.500000Z', 2, 200.0),
                                ('1970-01-01T00:00:01.500000Z', 1, 110.0),
                                ('1970-01-01T00:00:01.500000Z', 2, 210.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 1, 100),
                                ('1970-01-01T00:00:01.000000Z', 2, 200)
                            """
            );

            // At offset 0: account 1->100, account 2->200, avg=150
            // At offset 1s: account 1->110, account 2->210, avg=160
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.account_id = p.account_id) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t150.0\t300
                            1\t160.0\t300
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithMasterJavaFilter() throws Exception {
        // Test filter with concat() on master table (Java implementation, symbol table matters)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (order_sym SYMBOL, ts #TIMESTAMP, qty LONG) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (price_sym SYMBOL, ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('AAPL', '2000-01-01T00:00:01.000000Z', 100),
                                ('AAPL', '2000-01-01T00:00:02.000000Z', 200),
                                ('GOOG', '2000-01-01T00:00:03.000000Z', 150),
                                ('MSFT', '2000-01-01T00:00:04.000000Z', 300)
                            """
            );

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('AAPL', '2000-01-01T00:00:00.500000Z', 100.0),
                                ('AAPL', '2000-01-01T00:00:01.500000Z', 110.0),
                                ('GOOG', '2000-01-01T00:00:02.500000Z', 200.0),
                                ('MSFT', '2000-01-01T00:00:03.500000Z', 300.0)
                            """
            );

            // Filter using concat() - uses Java filter implementation
            assertQueryNoLeakCheck(
                    """
                            order_sym\tsum\tavg
                            AAPL\t300\t105.0
                            """,
                    """
                            SELECT t.order_sym, sum(t.qty), avg(p.price)
                            FROM orders AS t
                            HORIZON JOIN prices AS p ON (t.order_sym = p.price_sym)
                            RANGE FROM 0s TO 0s STEP 1s AS h
                            WHERE concat(t.order_sym, '_0') = 'AAPL_0'
                            ORDER BY t.order_sym
                            """,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithMasterJitFilter() throws Exception {
        // Test simple symbol filter on master table (JIT compiled)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (order_sym SYMBOL, ts #TIMESTAMP, qty LONG) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (price_sym SYMBOL, ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('AAPL', '2000-01-01T00:00:01.000000Z', 100),
                                ('AAPL', '2000-01-01T00:00:02.000000Z', 200),
                                ('GOOG', '2000-01-01T00:00:03.000000Z', 150),
                                ('MSFT', '2000-01-01T00:00:04.000000Z', 300)
                            """
            );

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('AAPL', '2000-01-01T00:00:00.500000Z', 100.0),
                                ('AAPL', '2000-01-01T00:00:01.500000Z', 110.0),
                                ('GOOG', '2000-01-01T00:00:02.500000Z', 200.0),
                                ('MSFT', '2000-01-01T00:00:03.500000Z', 300.0)
                            """
            );

            // Filter to only AAPL orders
            assertQueryNoLeakCheck(
                    """
                            order_sym\tsum\tavg
                            AAPL\t300\t105.0
                            """,
                    """
                            SELECT t.order_sym, sum(t.qty), avg(p.price)
                            FROM orders AS t
                            HORIZON JOIN prices AS p ON (t.order_sym = p.price_sym)
                            RANGE FROM 0s TO 0s STEP 1s AS h
                            WHERE t.order_sym = 'AAPL'
                            ORDER BY t.order_sym
                            """,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithMultiColumnMixedTypeKey() throws Exception {
        // Test HORIZON JOIN with mixed-type multi-column key (SYMBOL + LONG)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym SYMBOL, region LONG, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, region LONG, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 1, 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 2, 105.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 1, 110.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 2, 115.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 1, 100),
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 2, 200)
                            """
            );

            // At offset 0: AAPL/1->100, AAPL/2->105, avg=102.5
            // At offset 1s: AAPL/1->110, AAPL/2->115, avg=112.5
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym AND t.region = p.region) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t102.5\t300
                            1\t112.5\t300
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithMultiColumnSymbolKey() throws Exception {
        // Test HORIZON JOIN with two-column ASOF JOIN key (sym + exchange)
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym SYMBOL, exchange SYMBOL, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, exchange SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 'NYSE', 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 'NASDAQ', 101.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 'NYSE', 110.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 'NASDAQ', 111.0),
                                ('1970-01-01T00:00:02.500000Z', 'GOOG', 'NASDAQ', 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 'NYSE', 100),
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 'NASDAQ', 200),
                                ('1970-01-01T00:00:02.000000Z', 'AAPL', 'NYSE', 150),
                                ('1970-01-01T00:00:03.000000Z', 'GOOG', 'NASDAQ', 300)
                            """
            );

            // At offset 0:
            //   AAPL/NYSE at 1s -> ASOF to 0.5s -> 100
            //   AAPL/NASDAQ at 1s -> ASOF to 0.5s -> 101
            //   AAPL/NYSE at 2s -> ASOF to 1.5s -> 110
            //   GOOG/NASDAQ at 3s -> ASOF to 2.5s -> 200
            // avg: (100+101+110+200)/4 = 127.75, sum qty: 750
            // At offset 1s:
            //   AAPL/NYSE at 1s -> ASOF to 2s -> latest AAPL/NYSE is 1.5s -> 110
            //   AAPL/NASDAQ at 1s -> ASOF to 2s -> latest AAPL/NASDAQ is 1.5s -> 111
            //   AAPL/NYSE at 2s -> ASOF to 3s -> latest AAPL/NYSE is 1.5s -> 110
            //   GOOG/NASDAQ at 3s -> ASOF to 4s -> latest GOOG/NASDAQ is 2.5s -> 200
            // avg: (110+111+110+200)/4 = 132.75, sum qty: 750
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym AND t.exchange = p.exchange) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t127.75\t750
                            1\t132.75\t750
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
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

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
            executeWithRewriteTimestamp(
                    "CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)",
                    leftTableTimestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)",
                    rightTableTimestampType.getTypeName()
            );

            // Insert test data
            // Prices at 0s, 1s, 2s, 3s for sym 'AX'
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 10),
                                ('1970-01-01T00:00:01.000000Z', 'AX', 20),
                                ('1970-01-01T00:00:02.000000Z', 'AX', 30),
                                ('1970-01-01T00:00:03.000000Z', 'AX', 40)
                            """
            );

            // Trade at 1s for sym 'AX'
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AX', 100)
                            """
            );

            // RANGE FROM 0s TO 2s STEP 1s gives offsets: 0, 1000000, 2000000 microseconds
            // For trade at 1s:
            //   offset=0: look at 1s+0=1s -> ASOF to price at 1s -> 20
            //   offset=1s: look at 1s+1s=2s -> ASOF to price at 2s -> 30
            //   offset=2s: look at 1s+2s=3s -> ASOF to price at 3s -> 40
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM 0s TO 2s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            // Verify the query plan
            assertPlanNoLeakCheck(
                    sql,
                    "Radix sort light\n" +
                            "  keys: [sec_offs]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [offset/" + getSecondsDivisor() + ",avg]\n" +
                            "        Async Horizon Join workers: 1 offsets: 3\n" +
                            "          keys: [offset]\n" +
                            "          values: [avg(p.price)]\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: trades\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: prices\n"
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
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, sym SYMBOL, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            // Insert test data with minute-level timestamps
            // Prices at 0m, 1m, 2m, 3m for sym 'AX'
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 'AX', 10),
                                ('1970-01-01T00:01:00.000000Z', 'AX', 20),
                                ('1970-01-01T00:02:00.000000Z', 'AX', 30),
                                ('1970-01-01T00:03:00.000000Z', 'AX', 40)
                            """
            );

            // Trade at 1m for sym 'AX'
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:01:00.000000Z', 'AX', 100)
                            """
            );

            // RANGE FROM 0m TO 2m STEP 1m gives offsets: 0, 60000000, 120000000 microseconds
            // For trade at 1m (60s):
            //   offset=0m: look at 1m+0=1m -> ASOF to price at 1m -> 20
            //   offset=1m: look at 1m+1m=2m -> ASOF to price at 2m -> 30
            //   offset=2m: look at 1m+2m=3m -> ASOF to price at 3m -> 40
            String sql = "SELECT h.offset / " + getMinutesDivisor() + " AS min_offs, avg(p.price) " +
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
    public void testHorizonJoinWithSlaveSymbolAsKey() throws Exception {
        // Test using a slave symbol as a grouping key
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (order_sym SYMBOL, ts #TIMESTAMP, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (price_sym SYMBOL, exchange SYMBOL, ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

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

    @Test
    public void testHorizonJoinWithSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (sym SYMBOL, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY;", leftTableTimestampType.getTypeName());
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('sym1', '2000-01-01T00:00:00.000000Z'),
                                ('sym2', '2000-01-01T00:00:05.000000Z'),
                                ('sym3', '2000-01-01T00:00:10.000000Z')
                            """
            );
            executeWithRewriteTimestamp("CREATE TABLE prices (sym SYMBOL, bid DOUBLE, ask DOUBLE, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());
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
                    "SELECT h.offset / " + getSecondsDivisor() + " AS sec_off, avg(p.bid), avg(p.ask) " +
                            "FROM trades AS t " +
                            "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                            "RANGE FROM 0s TO 1s STEP 1s AS h " +
                            "ORDER BY sec_off",
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
            executeWithRewriteTimestamp("CREATE TABLE orders (order_sym SYMBOL, category SYMBOL, ts #TIMESTAMP, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (price_sym SYMBOL, exchange SYMBOL, ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

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
            executeWithRewriteTimestamp("CREATE TABLE orders (order_sym SYMBOL, ts #TIMESTAMP, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (price_sym SYMBOL, exchange SYMBOL, ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

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
    public void testHorizonJoinWithVarcharKey() throws Exception {
        // Test HORIZON JOIN with VARCHAR column as ASOF JOIN key
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym VARCHAR, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym VARCHAR, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'GOOG', 200.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 110.0),
                                ('1970-01-01T00:00:01.500000Z', 'GOOG', 210.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 100),
                                ('1970-01-01T00:00:01.000000Z', 'GOOG', 200)
                            """
            );

            // At offset 0: AAPL->100, GOOG->200, avg=150
            // At offset 1s: AAPL->110, GOOG->210, avg=160
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t150.0\t300
                            1\t160.0\t300
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithoutOnClause() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            // Insert test data
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 10),
                                ('1970-01-01T00:00:01.000000Z', 20),
                                ('1970-01-01T00:00:02.000000Z', 30),
                                ('1970-01-01T00:00:03.000000Z', 40)
                            """
            );
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 100)
                            """
            );

            // HORIZON JOIN without ON clause - ASOF by timestamp only
            // RANGE FROM 0s TO 2s STEP 1s gives offsets: 0, 1000000, 2000000 microseconds
            // For trade at 1s:
            //   offset=0: look at 1s+0=1s -> ASOF to price at 1s -> 20
            //   offset=1s: look at 1s+1s=2s -> ASOF to price at 2s -> 30
            //   offset=2s: look at 1s+2s=3s -> ASOF to price at 3s -> 40
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price) " +
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

    @Test
    public void testHorizonJoinWithoutOnClauseMultipleMasterRows() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            // Prices at 0s, 1s, 2s, 3s, 4s
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 10),
                                ('1970-01-01T00:00:01.000000Z', 20),
                                ('1970-01-01T00:00:02.000000Z', 30),
                                ('1970-01-01T00:00:03.000000Z', 40),
                                ('1970-01-01T00:00:04.000000Z', 50)
                            """
            );
            // Trades at 1s and 2s
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 100),
                                ('1970-01-01T00:00:02.000000Z', 200)
                            """
            );

            // HORIZON JOIN without ON clause with multiple master rows
            // For trade at 1s with offset 0: ASOF to 1s -> 20
            // For trade at 1s with offset 1s: ASOF to 2s -> 30
            // For trade at 2s with offset 0: ASOF to 2s -> 30
            // For trade at 2s with offset 1s: ASOF to 3s -> 40
            // avg at offset 0: (20+30)/2 = 25
            // avg at offset 1s: (30+40)/2 = 35
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t25.0\t300.0
                            1\t35.0\t300.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithoutOnClauseNegativeOffsets() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            // Prices at 0s, 1s, 2s, 3s
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 10),
                                ('1970-01-01T00:00:01.000000Z', 20),
                                ('1970-01-01T00:00:02.000000Z', 30),
                                ('1970-01-01T00:00:03.000000Z', 40)
                            """
            );
            // Trade at 2s
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:02.000000Z', 100)
                            """
            );

            // HORIZON JOIN without ON clause with negative offsets
            // For trade at 2s:
            //   offset=-1s: look at 2s-1s=1s -> ASOF to price at 1s -> 20
            //   offset=0: look at 2s -> ASOF to price at 2s -> 30
            //   offset=1s: look at 2s+1s=3s -> ASOF to price at 3s -> 40
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p " +
                    "RANGE FROM -1s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg
                            -1\t20.0
                            0\t30.0
                            1\t40.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithoutOnClauseWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (ts #TIMESTAMP, qty DOUBLE) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            // Prices at 0s, 1s, 2s, 3s, 4s
            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.000000Z', 10),
                                ('1970-01-01T00:00:01.000000Z', 20),
                                ('1970-01-01T00:00:02.000000Z', 30),
                                ('1970-01-01T00:00:03.000000Z', 40),
                                ('1970-01-01T00:00:04.000000Z', 50)
                            """
            );
            // Trades at 1s (qty=100), 2s (qty=200), 3s (qty=150)
            execute(
                    """
                            INSERT INTO trades VALUES
                                ('1970-01-01T00:00:01.000000Z', 100),
                                ('1970-01-01T00:00:02.000000Z', 200),
                                ('1970-01-01T00:00:03.000000Z', 150)
                            """
            );

            // Filter to only trades with qty > 100 (2s and 3s)
            // For trade at 2s with offset 0: ASOF to 2s -> 30
            // For trade at 3s with offset 0: ASOF to 3s -> 40
            // avg at offset 0: (30+40)/2 = 35
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p " +
                    "RANGE FROM 0s TO 0s STEP 1s AS h " +
                    "WHERE t.qty > 100 " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t35.0\t350.0
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithSymbolMasterStringSlaveKey() throws Exception {
        // Test HORIZON JOIN where master key column is SYMBOL and slave key column is STRING.
        // This should fall back to string-based comparison (no integer optimization).
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym SYMBOL, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym STRING, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'GOOG', 150.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 110.0),
                                ('1970-01-01T00:00:01.500000Z', 'GOOG', 160.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 100),
                                ('1970-01-01T00:00:01.000000Z', 'GOOG', 200)
                            """
            );

            // At offset 0: AAPL->100, GOOG->150, avg=125
            // At offset 1s: AAPL->110, GOOG->160, avg=135
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t125.0\t300
                            1\t135.0\t300
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithStringMasterSymbolSlaveKey() throws Exception {
        // Test HORIZON JOIN where master key column is STRING and slave key column is SYMBOL.
        // This should fall back to string-based comparison (no integer optimization).
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym STRING, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'GOOG', 150.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 110.0),
                                ('1970-01-01T00:00:01.500000Z', 'GOOG', 160.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 100),
                                ('1970-01-01T00:00:01.000000Z', 'GOOG', 200)
                            """
            );

            // At offset 0: AAPL->100, GOOG->150, avg=125
            // At offset 1s: AAPL->110, GOOG->160, avg=135
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t125.0\t300
                            1\t135.0\t300
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithSymbolMasterVarcharSlaveKey() throws Exception {
        // Test HORIZON JOIN where master key column is SYMBOL and slave key column is VARCHAR.
        // This should fall back to varchar-based comparison (no integer optimization).
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym SYMBOL, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym VARCHAR, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'GOOG', 150.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 110.0),
                                ('1970-01-01T00:00:01.500000Z', 'GOOG', 160.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 100),
                                ('1970-01-01T00:00:01.000000Z', 'GOOG', 200)
                            """
            );

            // At offset 0: AAPL->100, GOOG->150, avg=125
            // At offset 1s: AAPL->110, GOOG->160, avg=135
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t125.0\t300
                            1\t135.0\t300
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithMultiColumnMixedSymbolAndStringKey() throws Exception {
        // Test multi-column key where one column pair is SYMBOL+SYMBOL (uses integer optimization)
        // and another column pair is SYMBOL+STRING (falls back to string comparison).
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym SYMBOL, region SYMBOL, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            // Note: region is STRING on slave side, sym is SYMBOL on both sides
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, region STRING, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 'US', 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 'EU', 105.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 'US', 110.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 'EU', 115.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 'US', 100),
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 'EU', 200)
                            """
            );

            // At offset 0: AAPL/US->100, AAPL/EU->105, avg=102.5
            // At offset 1s: AAPL/US->110, AAPL/EU->115, avg=112.5
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym AND t.region = p.region) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t102.5\t300
                            1\t112.5\t300
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinNotKeyedWithSymbolMasterStringSlaveKey() throws Exception {
        // Non-keyed (no GROUP BY keys) variant with SYMBOL master + STRING slave key.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym SYMBOL, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym STRING, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'GOOG', 150.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 110.0),
                                ('1970-01-01T00:00:01.500000Z', 'GOOG', 160.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 100),
                                ('1970-01-01T00:00:01.000000Z', 'GOOG', 200)
                            """
            );

            // Non-keyed: no GROUP BY keys, only aggregates
            // At offset 0: AAPL->100, GOOG->150, avg=125, sum=300
            String sql = """
                    SELECT avg(p.price), sum(t.qty)
                    FROM orders AS t
                    HORIZON JOIN prices AS p ON (t.sym = p.sym)
                    RANGE FROM 0s TO 0s STEP 1s AS h
                    """;

            assertQueryNoLeakCheck(
                    """
                            avg\tsum
                            125.0\t300
                            """,
                    sql,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testHorizonJoinWithMultiColumnSymbolAndNullKey() throws Exception {
        // Test multi-column SYMBOL+SYMBOL key where some rows have null symbol values.
        // Verifies that null symbol IDs are handled correctly in the translation cache.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE orders (ts #TIMESTAMP, sym SYMBOL, exchange SYMBOL, qty LONG) TIMESTAMP(ts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE prices (ts #TIMESTAMP, sym SYMBOL, exchange SYMBOL, price DOUBLE) TIMESTAMP(ts)", rightTableTimestampType.getTypeName());

            execute(
                    """
                            INSERT INTO prices VALUES
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', 'NYSE', 100.0),
                                ('1970-01-01T00:00:00.500000Z', 'AAPL', null, 105.0),
                                ('1970-01-01T00:00:01.500000Z', 'AAPL', 'NYSE', 110.0),
                                ('1970-01-01T00:00:01.500000Z', null, 'NASDAQ', 200.0)
                            """
            );

            execute(
                    """
                            INSERT INTO orders VALUES
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', 'NYSE', 100),
                                ('1970-01-01T00:00:01.000000Z', 'AAPL', null, 200),
                                ('1970-01-01T00:00:02.000000Z', null, 'NASDAQ', 300)
                            """
            );

            // At offset 0:
            //   AAPL/NYSE at 1s -> ASOF to 0.5s -> 100
            //   AAPL/null at 1s -> ASOF to 0.5s -> 105
            //   null/NASDAQ at 2s -> ASOF to 1.5s -> 200
            // avg: (100+105+200)/3 = 135, sum qty: 600
            // At offset 1s:
            //   AAPL/NYSE at 1s -> ASOF to 2s -> latest AAPL/NYSE 1.5s -> 110
            //   AAPL/null at 1s -> ASOF to 2s -> latest AAPL/null 0.5s -> 105
            //   null/NASDAQ at 2s -> ASOF to 3s -> latest null/NASDAQ 1.5s -> 200
            // avg: (110+105+200)/3 = 138.33..., sum qty: 600
            String sql = "SELECT h.offset / " + getSecondsDivisor() + " AS sec_offs, avg(p.price), sum(t.qty) " +
                    "FROM orders AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym AND t.exchange = p.exchange) " +
                    "RANGE FROM 0s TO 1s STEP 1s AS h " +
                    "ORDER BY sec_offs";

            assertQueryNoLeakCheck(
                    """
                            sec_offs\tavg\tsum
                            0\t135.0\t600
                            1\t138.33333333333334\t600
                            """,
                    sql,
                    null,
                    true,
                    true
            );
        });
    }

    private long getMinutesDivisor() {
        return leftTableTimestampType == TestTimestampType.MICRO ? 60_000_000L : 60_000_000_000L;
    }

    private long getSecondsDivisor() {
        return leftTableTimestampType == TestTimestampType.MICRO ? 1_000_000L : 1_000_000_000L;
    }
}
