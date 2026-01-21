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
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

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
            try {
                assertSql(
                        "SELECT h.offset, avg(p.price) " +
                                "FROM trades AS t " +
                                "JOIN other AS o ON (t.sym = o.sym) " +
                                "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                                "RANGE FROM -10s TO 10s STEP 1s AS h"
                );
                fail("Expected SqlException");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "horizon join cannot be combined with other joins");
            }

            // Another join after HORIZON JOIN
            try {
                assertSql(
                        "SELECT h.offset, avg(p.price) " +
                                "FROM trades AS t " +
                                "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                                "RANGE FROM -10s TO 10s STEP 1s AS h " +
                                "JOIN other AS o ON (t.sym = o.sym)"
                );
                fail("Expected SqlException");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "horizon join cannot be combined with other joins");
            }
        });
    }

    @Test
    public void testHorizonJoinEmptyList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            try {
                assertSql(
                        "SELECT h.offset, avg(p.price) " +
                                "FROM trades AS t " +
                                "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                                "LIST () AS h"
                );
                fail("Expected SqlException");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "at least one offset expression expected");
            }
        });
    }

    @Test
    public void testHorizonJoinMissingRangeOrList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            try {
                assertSql(
                        "SELECT h.offset, avg(p.price) " +
                                "FROM trades AS t " +
                                "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                                "AS h"  // Missing RANGE or LIST
                );
                fail("Expected SqlException");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "'range' or 'list' expected");
            }
        });
    }

    /**
     * Test parallel execution of HORIZON JOIN GROUP BY with larger dataset.
     */
    @Ignore("HORIZON JOIN code generation not yet complete")
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
                            GROUP BY h.offset
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
    @Ignore("HORIZON JOIN code generation not yet complete")
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
                            GROUP BY h.offset
                            """,
                    """
                            Async Markout GroupBy workers: 1
                              keys: [offset]
                              values: [avg(bid),avg(ask)]
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

    @Ignore // FIXME
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
                    RANGE FROM -2s TO 0s STEP 1s AS h;
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

    @Ignore("HORIZON JOIN code generation not yet complete")
    @Test
    public void testHorizonJoinWithListBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            String sql = "SELECT h.offset, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "LIST (-600000000, 0, 600000000) AS h";

            assertSql(sql);
        });
    }

    @Ignore("HORIZON JOIN code generation not yet complete")
    @Test
    public void testHorizonJoinWithRangeAndGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            String sql = "SELECT h.offset, t.sym, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM -600s TO 600s STEP 1s AS h " +
                    "GROUP BY h.offset, t.sym";

            assertSql(sql);
        });
    }

    @Ignore("HORIZON JOIN code generation not yet complete")
    @Test
    public void testHorizonJoinWithRangeBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            String sql = "SELECT h.offset, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM -10s TO 10s STEP 1s AS h";

            assertSql(sql);
        });
    }

    @Ignore("HORIZON JOIN code generation not yet complete")
    @Test
    public void testHorizonJoinWithRangeMinuteUnits() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");

            String sql = "SELECT h.offset, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p ON (t.sym = p.sym) " +
                    "RANGE FROM -10m TO 10m STEP 1m AS h";

            assertSql(sql);
        });
    }

    @Ignore("HORIZON JOIN code generation not yet complete")
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
                            sec_off\tsym\tavg\tavg1
                            -1\t\tnull\tnull
                            0\tsym1\t1.0\t2.0
                            1\tsym1\t1.0\t2.0
                            0\tsym2\t3.0\t4.0
                            1\tsym2\t3.0\t4.0
                            0\tsym3\t5.0\t6.0
                            1\tsym3\t5.0\t6.0
                            """,
                    """
                            SELECT h.offset / 1000000 AS sec_off, p.sym, avg(p.bid), avg(p.ask)
                            FROM trades AS t
                            HORIZON JOIN prices AS p ON (t.sym = p.sym)
                            RANGE FROM -1s TO 1s STEP 1s AS h
                            GROUP BY h.offset, p.sym
                            """,
                    null,
                    true,
                    true
            );
        });
    }

    @Ignore("HORIZON JOIN code generation not yet complete")
    @Test
    public void testHorizonJoinWithoutOnClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts)");
            execute("CREATE TABLE prices (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts)");

            String sql = "SELECT h.offset, avg(p.price) " +
                    "FROM trades AS t " +
                    "HORIZON JOIN prices AS p " +
                    "RANGE FROM -10s TO 10s STEP 1s AS h";

            assertSql(sql);
        });
    }

    private static void assertContains(CharSequence actual, String expected) {
        if (!actual.toString().contains(expected)) {
            fail("Expected message to contain '" + expected + "' but was: " + actual);
        }
    }

    private void assertSql(String sql) throws SqlException {
        // Just compile the query to verify parsing works
        execute("EXPLAIN " + sql);
    }
}
