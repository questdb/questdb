/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.view;

import io.questdb.griffin.SqlCompilerImpl;
import org.junit.Test;

public class ViewQueryTest extends AbstractViewTest {

    @Test
    public void testAggregateOverUnionAllOfLatestOnView() throws Exception {
        // Regression test: count()/sum() over a VIEW defined as a UNION ALL of LATEST ON
        // sub-queries used to crash with an AssertionError in SqlCodeGenerator
        // (presenting as a 500 in the web console). Top-down column pruning over the
        // union chain pruned one branch down to just the LATEST ON partition key while
        // leaving the others with all projected columns, so the union sides diverged in
        // column count.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE 'equity_price_1m' (" +
                            "timestamp TIMESTAMP, " +
                            "tvId SYMBOL INDEX CAPACITY 256, " +
                            "symbol SYMBOL INDEX CAPACITY 256, " +
                            "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE" +
                            ") timestamp(timestamp) PARTITION BY DAY WAL " +
                            "DEDUP UPSERT KEYS(timestamp,tvId)"
            );
            execute("CREATE TABLE 'crypto_price_1m' (LIKE equity_price_1m)");
            execute("CREATE TABLE 'forex_price_1m' (LIKE equity_price_1m)");
            execute("CREATE TABLE 'commodity_price_1m' (LIKE equity_price_1m)");
            // equity: A -> latest close 11, B -> 20  (2 rows after LATEST ON)
            execute("INSERT INTO equity_price_1m(timestamp, tvId, close) VALUES " +
                    "('2024-01-01T00:00:01.000000Z', 'A', 10.0), " +
                    "('2024-01-01T00:00:02.000000Z', 'A', 11.0), " +
                    "('2024-01-01T00:00:01.000000Z', 'B', 20.0)");
            // crypto: C -> 30  (1 row)
            execute("INSERT INTO crypto_price_1m(timestamp, tvId, close) VALUES ('2024-01-01T00:00:01.000000Z', 'C', 30.0)");
            // forex: D -> latest close 41  (1 row)
            execute("INSERT INTO forex_price_1m(timestamp, tvId, close) VALUES " +
                    "('2024-01-01T00:00:01.000000Z', 'D', 40.0), " +
                    "('2024-01-01T00:00:02.000000Z', 'D', 41.0)");
            // commodity: E -> 50  (1 row)
            execute("INSERT INTO commodity_price_1m(timestamp, tvId, close) VALUES ('2024-01-01T00:00:01.000000Z', 'E', 50.0)");
            drainWalQueue();

            final String query1 = "SELECT tvId, close price, timestamp mts FROM equity_price_1m LATEST ON timestamp PARTITION BY tvId " +
                    "UNION ALL " +
                    "SELECT tvId, close price, timestamp mts FROM crypto_price_1m LATEST ON timestamp PARTITION BY tvId " +
                    "UNION ALL " +
                    "SELECT tvId, close price, timestamp mts FROM forex_price_1m LATEST ON timestamp PARTITION BY tvId " +
                    "UNION ALL " +
                    "SELECT tvId, close price, timestamp mts FROM commodity_price_1m LATEST ON timestamp PARTITION BY tvId";
            execute("CREATE VIEW latest_prices AS (" + query1 + ")");
            drainWalAndViewQueues();

            // 2 + 1 + 1 + 1 = 5 rows
            assertQuery("select count() from latest_prices")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            5
                            """);

            // 11 + 20 + 30 + 41 + 50 = 152
            assertQuery("select sum(price) from latest_prices")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sum
                            152.0
                            """);
        });
    }

    @Test
    public void testCallerCteDoesNotReachViewBody() throws Exception {
        // A view body resolves a table name against its own WITH clauses and real tables. A CTE
        // of the statement that reads the view is not visible anywhere inside the body, so a
        // principal with SELECT on a view cannot replace part of the view's filter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE)");
            execute("CREATE TABLE allowed (symbol SYMBOL)");
            execute("INSERT INTO trades VALUES ('AAPL', 100.5), ('MSFT', 300.75)");
            execute("INSERT INTO allowed VALUES ('AAPL')");
            createView("v_in", "SELECT symbol, price FROM trades WHERE symbol IN (SELECT symbol FROM allowed)");
            createView("v_from", "SELECT a.symbol, t.price FROM (SELECT symbol FROM allowed) a JOIN trades t ON a.symbol = t.symbol");
            createView("v_join", "SELECT t.symbol, t.price FROM trades t JOIN (SELECT symbol FROM allowed) a ON t.symbol = a.symbol");
            createView("v_declared", "DECLARE @a := (SELECT symbol FROM allowed) SELECT * FROM @a UNION ALL SELECT * FROM @a");
            createView("v_outer", "SELECT * FROM v_in");
            createView(
                    "v_overridable",
                    "DECLARE OVERRIDABLE @a := (SELECT symbol FROM allowed) SELECT * FROM @a UNION ALL SELECT * FROM @a"
            );

            final String callerCte = "WITH allowed AS (SELECT 'MSFT'::SYMBOL symbol) ";
            final String aapl = """
                    symbol\tprice
                    AAPL\t100.5
                    """;
            // The sub-query of the filter used to read the caller's CTE and return MSFT.
            assertQuery(callerCte + "SELECT * FROM v_in")
                    .noLeakCheck()
                    .returns(aapl);
            assertQuery("SELECT * FROM (" + callerCte + "SELECT * FROM v_in)")
                    .noLeakCheck()
                    .returns(aapl);
            assertQuery(callerCte + "SELECT * FROM v_outer")
                    .noLeakCheck()
                    .returns(aapl);
            // The second read of the view parsed the caller's CTE again, at its position in the
            // caller's text but from the view's text, and failed with "')' expected".
            assertQuery(callerCte + "SELECT * FROM v_in UNION ALL SELECT * FROM v_in")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            symbol\tprice
                            AAPL\t100.5
                            AAPL\t100.5
                            """);
            assertQuery(callerCte + "SELECT * FROM v_in")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: symbol in cursor\s
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: allowed [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                            """);
            // FROM and JOIN sub-queries used to read the caller's CTE as well.
            assertQuery(callerCte + "SELECT * FROM v_from")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(aapl);
            assertQuery(callerCte + "SELECT * FROM v_join")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(aapl);
            // A declared sub-query and its copy read the view's table, where the copy used to fail.
            final String aaplTwice = """
                    symbol
                    AAPL
                    AAPL
                    """;
            assertQuery(callerCte + "SELECT * FROM v_declared")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(aaplTwice);
            assertQuery(callerCte + "SELECT * FROM v_overridable")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(aaplTwice);
            // A value the caller passes for an OVERRIDABLE variable is the caller's text, so it reads
            // the caller's CTE, in every read the view makes of it.
            assertQuery(callerCte + "SELECT * FROM (DECLARE @a := (SELECT symbol FROM allowed) SELECT * FROM v_overridable)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            symbol
                            MSFT
                            MSFT
                            """);
        });
    }

    @Test
    public void testCallerCteDoesNotReachViewCteDefinitions() throws Exception {
        // The CTE definitions of a view body read the body's own CTEs, never a caller's CTE of the
        // same name, with or without EXPLAIN.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (l LONG)");
            execute("INSERT INTO k VALUES (1), (2), (3)");
            createView(
                    "v_cte",
                    "WITH c AS (SELECT l FROM k WHERE l > 1), d AS (SELECT * FROM c UNION ALL SELECT * FROM c) " +
                            "SELECT * FROM d UNION ALL SELECT * FROM d UNION ALL SELECT * FROM c"
            );
            final String expected = """
                    l
                    2
                    3
                    2
                    3
                    2
                    3
                    2
                    3
                    2
                    3
                    """;
            assertQuery("SELECT * FROM v_cte")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            // The first read of c in d used to read the caller's c, which added the row 1.
            assertQuery("WITH c AS (SELECT l FROM k) SELECT * FROM v_cte")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(expected);
            // EXPLAIN used to fail with "table and column names that are SQL keywords have to be
            // enclosed in double quotes": the second read of c in d parsed the caller's CTE from
            // its position in the caller's text, which EXPLAIN shifts, but with the view's text.
            final String plan = """
                    Union All
                        Union All
                            Union All
                                Async Filter workers: 1
                                  filter: 1<l
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: k
                                Async Filter workers: 1
                                  filter: 1<l
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: k
                            Union All
                                Async Filter workers: 1
                                  filter: 1<l
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: k
                                Async Filter workers: 1
                                  filter: 1<l
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: k
                        Async Filter workers: 1
                          filter: 1<l
                            PageFrame
                                Row forward scan
                                Frame forward scan on: k
                    """;
            assertQuery("SELECT * FROM v_cte")
                    .noLeakCheck()
                    .assertsPlan(plan);
            assertQuery("WITH c AS (SELECT l FROM k) SELECT * FROM v_cte")
                    .noLeakCheck()
                    .assertsPlan(plan);
        });
    }

    @Test
    public void testCreateConstantView() throws Exception {
        assertMemoryLeak(() -> {
            final String query1 = "select 42 as col";
            createView(VIEW1, query1);

            assertQuery(VIEW1)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            col
                            42
                            """);
        });
    }

    @Test
    public void testDeclareAsofJoinBetweenViews() throws Exception {
        // Test: DECLARE + ASOF JOIN between two VIEWs with different parameters
        assertMemoryLeak(() -> {
            // Quotes table - bid/ask prices
            execute("CREATE TABLE quotes (ts TIMESTAMP, symbol SYMBOL, bid DOUBLE, ask DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO quotes VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 'AAPL', 149.0, 150.0), " +
                    "('2024-01-01T00:00:05.000000Z', 'AAPL', 149.5, 150.5), " +
                    "('2024-01-01T00:00:10.000000Z', 'AAPL', 150.0, 151.0), " +
                    "('2024-01-01T00:00:15.000000Z', 'AAPL', 150.5, 151.5)");
            drainWalQueue();

            // Trades table
            execute("CREATE TABLE trade_log (ts TIMESTAMP, symbol SYMBOL, price DOUBLE, side SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO trade_log VALUES " +
                    "('2024-01-01T00:00:02.000000Z', 'AAPL', 149.8, 'BUY'), " +
                    "('2024-01-01T00:00:08.000000Z', 'AAPL', 150.2, 'SELL'), " +
                    "('2024-01-01T00:00:12.000000Z', 'AAPL', 150.8, 'BUY')");
            drainWalQueue();

            // VIEW1: Quotes with OVERRIDABLE spread filter
            final String query1 = "DECLARE OVERRIDABLE @max_spread := 2.0 " +
                    "SELECT ts, symbol, bid, ask FROM quotes WHERE (ask - bid) <= @max_spread";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // VIEW2: Trades with OVERRIDABLE side filter
            final String query2 = "DECLARE OVERRIDABLE @side := 'BUY' " +
                    "SELECT ts, symbol, price, side FROM trade_log WHERE side = @side";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            // ASOF JOIN: Get the most recent quote at the time of each trade
            // Using both views with default parameters
            assertQuery("SELECT t.ts as trade_ts, t.symbol, t.price, t.side, q.ts as quote_ts, q.bid, q.ask " +
                    "FROM " + VIEW2 + " t ASOF JOIN " + VIEW1 + " q ON (symbol)")
                    .noLeakCheck()
                    .timestamp("trade_ts")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            trade_ts\tsymbol\tprice\tside\tquote_ts\tbid\task
                            2024-01-01T00:00:02.000000Z\tAAPL\t149.8\tBUY\t2024-01-01T00:00:00.000000Z\t149.0\t150.0
                            2024-01-01T00:00:12.000000Z\tAAPL\t150.8\tBUY\t2024-01-01T00:00:10.000000Z\t150.0\t151.0
                            """);

            // Override to get SELL trades instead
            assertQuery("DECLARE @side := 'SELL' " +
                    "SELECT t.ts as trade_ts, t.symbol, t.price, t.side, q.ts as quote_ts, q.bid, q.ask " +
                    "FROM " + VIEW2 + " t ASOF JOIN " + VIEW1 + " q ON (symbol)")
                    .noLeakCheck()
                    .timestamp("trade_ts")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            trade_ts\tsymbol\tprice\tside\tquote_ts\tbid\task
                            2024-01-01T00:00:08.000000Z\tAAPL\t150.2\tSELL\t2024-01-01T00:00:05.000000Z\t149.5\t150.5
                            """);
        });
    }

    @Test
    public void testDeclareCTEWithViewInteraction() throws Exception {
        // Test: CTE + DECLARE + VIEW interaction
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW with OVERRIDABLE parameter
            final String query1 = "DECLARE OVERRIDABLE @threshold := 5 SELECT ts, v FROM " + TABLE1 + " WHERE v > @threshold";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // Query using CTE that references the VIEW, with DECLARE
            String query = """
                    DECLARE @threshold := 6, @multiplier := 10
                    WITH filtered AS (SELECT ts, v FROM view1)
                    SELECT ts, v * @multiplier as scaled_v FROM filtered
                    """;

            // @threshold=6 means v > 6, so rows 7, 8
            // @multiplier=10 scales the values
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tscaled_v
                            1970-01-01T00:01:10.000000Z\t70
                            1970-01-01T00:01:20.000000Z\t80
                            """);
        });
    }

    @Test
    public void testDeclareDeepSubqueryNesting() throws Exception {
        // Test 3+ levels of nested subqueries with DECLARE shadowing
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // Level 1: @x = 2
            // Level 2: @x = 5 (shadows), @y = 3
            // Level 3: @x = 8 (shadows), @y inherited, @z = 1
            // Expected: innermost uses @x=8, @y=3, @z=1 -> 8 + 3 + 1 = 12
            String query = """
                    DECLARE @x := 2
                    SELECT * FROM (
                        DECLARE @x := 5, @y := 3
                        SELECT * FROM (
                            DECLARE @x := 8, @z := 1
                            SELECT @x + @y + @z as result FROM long_sequence(1)
                        )
                    )
                    """;

            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            result
                            12
                            """);

            // Test that outer scope sees its own @x
            String query2 = """
                    DECLARE @x := 2, @y := 100
                    SELECT @x + @y as outer_result, inner_result FROM (
                        DECLARE @x := 5
                        SELECT @x + @y as inner_result FROM long_sequence(1)
                    )
                    """;

            assertQuery(query2)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            outer_result\tinner_result
                            102\t105
                            """);
        });
    }

    @Test
    public void testDeclareInUnionInsideView() throws Exception {
        // Test: DECLARE scoping across UNION branches inside a VIEW
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            // VIEW with DECLARE that applies to both UNION branches
            final String query1 = "DECLARE OVERRIDABLE @threshold := 5 " +
                    "SELECT ts, k as key, v FROM " + TABLE1 + " WHERE v > @threshold " +
                    "UNION ALL " +
                    "SELECT ts, k2 as key, v FROM " + TABLE2 + " WHERE v < @threshold";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // Default: v > 5 from TABLE1 (rows 6,7,8) UNION v < 5 from TABLE2 (rows 0,1,2,3,4)
            assertQuery(VIEW1)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tkey\tv
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            1970-01-01T00:00:00.000000Z\tk2_0\t0
                            1970-01-01T00:00:10.000000Z\tk2_1\t1
                            1970-01-01T00:00:20.000000Z\tk2_2\t2
                            1970-01-01T00:00:30.000000Z\tk2_3\t3
                            1970-01-01T00:00:40.000000Z\tk2_4\t4
                            """);

            // Override threshold to 3: v > 3 from TABLE1 (rows 4,5,6,7,8) UNION v < 3 from TABLE2 (rows 0,1,2)
            assertQuery("DECLARE @threshold := 3 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tkey\tv
                            1970-01-01T00:00:40.000000Z\tk4\t4
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            1970-01-01T00:00:00.000000Z\tk2_0\t0
                            1970-01-01T00:00:10.000000Z\tk2_1\t1
                            1970-01-01T00:00:20.000000Z\tk2_2\t2
                            """);
        });
    }

    @Test
    public void testDeclareInViewDefinition() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "DECLARE OVERRIDABLE @x := k, OVERRIDABLE @z := 'hohoho' select ts, @x, @z as red, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            String query = VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tk\tred\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\thohoho\t6
                            1970-01-01T00:01:10.000000Z\tk7\thohoho\t7
                            1970-01-01T00:01:20.000000Z\tk8\thohoho\t8
                            """,
                    query,
                    """
                            QUERY PLAN
                            VirtualRecord
                              functions: [ts,k,'hohoho',v_max]
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: 5<v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                            """,
                    VIEW1
            );

            query = "DECLARE @x := 1, @y := 2 select ts, @x as one, @y * v_max from " + VIEW1 + " where v_max > 6 order by ts";
            assertQueryAndPlan(
                    """
                            ts\tone\tcolumn
                            1970-01-01T00:01:10.000000Z\t1\t14
                            1970-01-01T00:01:20.000000Z\t1\t16
                            """,
                    query,
                    "ts",
                    true,
                    false,
                    """
                            QUERY PLAN
                            Encode sort light
                              keys: [ts]
                                VirtualRecord
                                  functions: [ts,1,2*v_max]
                                    VirtualRecord
                                      functions: [ts,v_max]
                                        Filter filter: 6<v_max
                                            Async Group By workers: 1
                                              keys: [ts]
                                              values: [max(v)]
                                              filter: 5<v
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testDeclareJoinBetweenViewsWithDeclare() throws Exception {
        // Test: JOIN between two VIEWs, each with their own DECLARE variables
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            // VIEW1: filters TABLE1 with @min_v
            final String query1 = "DECLARE OVERRIDABLE @min_v := 3 SELECT ts, k, v FROM " + TABLE1 + " WHERE v >= @min_v";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // VIEW2: filters TABLE2 with @max_v
            final String query2 = "DECLARE OVERRIDABLE @max_v := 6 SELECT ts as ts2, k2, v as v2 FROM " + TABLE2 + " WHERE v <= @max_v";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            // Query VIEW1 with default @min_v=3: v >= 3 -> rows 3,4,5,6,7,8 (6 rows)
            assertQuery("SELECT count() as cnt FROM " + VIEW1)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cnt
                            6
                            """);

            // Query VIEW2 with default @max_v=6: v <= 6 -> rows 0,1,2,3,4,5,6 (7 rows)
            assertQuery("SELECT count() as cnt FROM " + VIEW2)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cnt
                            7
                            """);

            // Override VIEW1's @min_v: v >= 5 -> rows 5,6,7,8 (4 rows)
            assertQuery("DECLARE @min_v := 5 SELECT count() as cnt FROM " + VIEW1)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cnt
                            4
                            """);

            // Override VIEW2's @max_v: v <= 4 -> rows 0,1,2,3,4 (5 rows)
            assertQuery("DECLARE @max_v := 4 SELECT count() as cnt FROM " + VIEW2)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cnt
                            5
                            """);

            // Cross join both views with overrides in same query
            // VIEW1: v >= 5 (4 rows), VIEW2: v <= 4 (5 rows) -> 4 * 5 = 20 rows
            assertQuery("DECLARE @min_v := 5, @max_v := 4 SELECT count() as cnt FROM " + VIEW1 + " CROSS JOIN " + VIEW2)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cnt
                            20
                            """);
        });
    }

    @Test
    public void testDeclareLatestByInView() throws Exception {
        // Test: DECLARE + LATEST BY - parameterized latest record per symbol
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, symbol SYMBOL, price DOUBLE, qty INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 'AAPL', 150.0, 100), " +
                    "('2024-01-01T00:01:00.000000Z', 'GOOG', 140.0, 50), " +
                    "('2024-01-01T00:02:00.000000Z', 'AAPL', 151.0, 200), " +
                    "('2024-01-01T00:03:00.000000Z', 'MSFT', 380.0, 75), " +
                    "('2024-01-01T00:04:00.000000Z', 'GOOG', 141.0, 60), " +
                    "('2024-01-01T00:05:00.000000Z', 'AAPL', 152.0, 150)");
            drainWalQueue();

            // VIEW with OVERRIDABLE minimum quantity filter
            final String query1 = "DECLARE OVERRIDABLE @min_qty := 50 " +
                    "SELECT ts, symbol, price, qty FROM trades WHERE qty >= @min_qty LATEST ON ts PARTITION BY symbol";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // Default: qty >= 50, latest per symbol
            // AAPL: 152.0 (qty=150), GOOG: 141.0 (qty=60), MSFT: 380.0 (qty=75)
            assertQuery(VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns("""
                            ts\tsymbol\tprice\tqty
                            2024-01-01T00:03:00.000000Z\tMSFT\t380.0\t75
                            2024-01-01T00:04:00.000000Z\tGOOG\t141.0\t60
                            2024-01-01T00:05:00.000000Z\tAAPL\t152.0\t150
                            """);

            // Override: qty >= 100, excludes GOOG and MSFT entirely
            // AAPL: latest with qty >= 100 is 152.0 (qty=150) - LATEST BY returns only ONE row per symbol
            assertQuery("DECLARE @min_qty := 100 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsymbol\tprice\tqty
                            2024-01-01T00:05:00.000000Z\tAAPL\t152.0\t150
                            """);
        });
    }

    @Test
    public void testDeclareNestedViewsChain() throws Exception {
        // Test: VIEW1(DECLARE) -> VIEW2(DECLARE) -> query(DECLARE)
        // Each level should have its own variable scope
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW1: filters where v > @threshold (default 3)
            final String query1 = "DECLARE OVERRIDABLE @threshold := 3 SELECT ts, v FROM " + TABLE1 + " WHERE v > @threshold";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // VIEW2: references VIEW1, adds its own filter with @max (default 7)
            final String query2 = "DECLARE OVERRIDABLE @max := 7 SELECT ts, v FROM " + VIEW1 + " WHERE v < @max";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            // Query VIEW2 with default values: v > 3 AND v < 7 -> rows 4, 5, 6
            assertQuery(VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:40.000000Z\t4
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            """);

            // Override @max at query level: v > 3 AND v < 6 -> rows 4, 5
            assertQuery("DECLARE @max := 6 SELECT * FROM " + VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:40.000000Z\t4
                            1970-01-01T00:00:50.000000Z\t5
                            """);

            // Override @threshold at query level: v > 5 AND v < 7 -> row 6
            assertQuery("DECLARE @threshold := 5 SELECT * FROM " + VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            """);

            // Override both at query level: v > 4 AND v < 8 -> rows 5, 6, 7
            assertQuery("DECLARE @threshold := 4, @max := 8 SELECT * FROM " + VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            """);
        });
    }

    @Test
    public void testDeclareOverridablePropagationThroughViewChain() throws Exception {
        // Test: VIEW1 has OVERRIDABLE @x, VIEW2 references VIEW1
        // Can we override @x when querying VIEW2?
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW1: OVERRIDABLE @x
            final String query1 = "DECLARE OVERRIDABLE @x := 5 SELECT ts, v FROM " + TABLE1 + " WHERE v = @x";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // VIEW2: just wraps VIEW1, no DECLARE of its own
            final String query2 = "SELECT ts, v FROM " + VIEW1;
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            // Default: @x = 5
            assertQuery(VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:50.000000Z\t5
                            """);

            // Override @x through VIEW2 - should propagate to VIEW1
            assertQuery("DECLARE @x := 6 SELECT * FROM " + VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            """);
        });
    }

    @Test
    public void testDeclareParameterizedView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "DECLARE OVERRIDABLE @x := 6 select ts, v from " + TABLE1 + " where v = @x";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();
            assertViewDefinition(VIEW1, query1, TABLE1);
            assertViewDefinitionFile(VIEW1, query1);
            assertViewState(VIEW1);

            String query = VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            """,
                    query,
                    "ts",
                    true,
                    false,
                    """
                            QUERY PLAN
                            Async Filter workers: 1
                              filter: v=6
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            query = "DECLARE @x := 5 " + VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tv
                            1970-01-01T00:00:50.000000Z\t5
                            """,
                    query,
                    "ts",
                    true,
                    false,
                    """
                            QUERY PLAN
                            Async Filter workers: 1
                              filter: v=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testDeclareSampleByInView() throws Exception {
        // Test: DECLARE + SAMPLE BY - parameterized time-series sampling
        assertMemoryLeak(() -> {
            // Create table with more granular timestamps for SAMPLE BY testing
            execute("CREATE TABLE samples (ts TIMESTAMP, sensor SYMBOL, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO samples VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 'A', 10.0), " +
                    "('2024-01-01T00:00:30.000000Z', 'A', 20.0), " +
                    "('2024-01-01T00:01:00.000000Z', 'A', 30.0), " +
                    "('2024-01-01T00:01:30.000000Z', 'A', 40.0), " +
                    "('2024-01-01T00:02:00.000000Z', 'A', 50.0), " +
                    "('2024-01-01T00:02:30.000000Z', 'A', 60.0)");
            drainWalQueue();

            // VIEW with OVERRIDABLE filter threshold - SAMPLE BY groups by 1 minute
            final String query1 = "DECLARE OVERRIDABLE @min_value := 15.0 " +
                    "SELECT ts, sensor, avg(value) as avg_val FROM samples WHERE value > @min_value SAMPLE BY 1m";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // Default: value > 15, so excludes first row (10.0)
            // Minute 0: avg(20) = 20, Minute 1: avg(30,40) = 35, Minute 2: avg(50,60) = 55
            assertQuery(VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns("""
                            ts\tsensor\tavg_val
                            2024-01-01T00:00:00.000000Z\tA\t20.0
                            2024-01-01T00:01:00.000000Z\tA\t35.0
                            2024-01-01T00:02:00.000000Z\tA\t55.0
                            """);

            // Override: value > 35, excludes first 3 rows
            // Minute 1: avg(40) = 40, Minute 2: avg(50,60) = 55
            assertQuery("DECLARE @min_value := 35.0 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsensor\tavg_val
                            2024-01-01T00:01:00.000000Z\tA\t40.0
                            2024-01-01T00:02:00.000000Z\tA\t55.0
                            """);
        });
    }

    @Test
    public void testDeclareSubqueryInFromClauseWithViewReference() throws Exception {
        // Test: Subquery in FROM clause that references a VIEW with DECLARE
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW with OVERRIDABLE @x
            final String query1 = "DECLARE OVERRIDABLE @x := 5 SELECT ts, v FROM " + TABLE1 + " WHERE v >= @x";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // Query with subquery that has its own DECLARE, referencing the VIEW
            String query = """
                    DECLARE @x := 6, @y := 2
                    SELECT * FROM (
                        DECLARE @z := 100
                        SELECT ts, v, @z as marker FROM view1
                    ) WHERE v > @y
                    """;

            // @x=6 overrides VIEW1's @x, so v >= 6 -> rows 6, 7, 8
            // Inner @z=100 is local to subquery
            // Outer @y=2 filters v > 2 (no effect since already v >= 6)
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv\tmarker
                            1970-01-01T00:01:00.000000Z\t6\t100
                            1970-01-01T00:01:10.000000Z\t7\t100
                            1970-01-01T00:01:20.000000Z\t8\t100
                            """);
        });
    }

    @Test
    public void testDeclareTypeCoercion() throws Exception {
        // Test: Type coercion - string declared, used in numeric/other contexts
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // Test 1: Numeric string coerced to number in comparison
            final String query1 = "DECLARE OVERRIDABLE @limit := '5' " +
                    "SELECT ts, v FROM " + TABLE1 + " WHERE v > cast(@limit as int)";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            assertQuery(VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """);

            // Override with different string value
            assertQuery("DECLARE @limit := '6' SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """);

            // Test 2: Symbol/string parameter for filtering
            final String query2 = "DECLARE OVERRIDABLE @key_filter := 'k5' " +
                    "SELECT ts, k, v FROM " + TABLE1 + " WHERE k = @key_filter";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            assertQuery(VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tv
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            """);

            // Override to different key
            assertQuery("DECLARE @key_filter := 'k7' SELECT * FROM " + VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tv
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            """);
        });
    }

    @Test
    public void testDeclareVariableReusedInSameExpression() throws Exception {
        // Test: Same DECLARE variable used multiple times in one expression
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW using @x multiple times in various expressions
            final String query1 = "DECLARE OVERRIDABLE @x := 2 " +
                    "SELECT ts, v, " +
                    "@x as x_val, " +
                    "@x + @x as x_plus_x, " +
                    "@x * @x as x_squared, " +
                    "@x * @x * @x as x_cubed, " +
                    "v + @x as v_plus_x, " +
                    "v * @x + @x as complex_expr " +
                    "FROM " + TABLE1 + " WHERE v <= @x + @x";  // v <= 4
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // Default @x = 2: filter v <= 4, expressions use 2
            assertQuery(VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv\tx_val\tx_plus_x\tx_squared\tx_cubed\tv_plus_x\tcomplex_expr
                            1970-01-01T00:00:00.000000Z\t0\t2\t4\t4\t8\t2\t2
                            1970-01-01T00:00:10.000000Z\t1\t2\t4\t4\t8\t3\t4
                            1970-01-01T00:00:20.000000Z\t2\t2\t4\t4\t8\t4\t6
                            1970-01-01T00:00:30.000000Z\t3\t2\t4\t4\t8\t5\t8
                            1970-01-01T00:00:40.000000Z\t4\t2\t4\t4\t8\t6\t10
                            """);

            // Override @x = 3: filter v <= 6, expressions use 3
            assertQuery("DECLARE @x := 3 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv\tx_val\tx_plus_x\tx_squared\tx_cubed\tv_plus_x\tcomplex_expr
                            1970-01-01T00:00:00.000000Z\t0\t3\t6\t9\t27\t3\t3
                            1970-01-01T00:00:10.000000Z\t1\t3\t6\t9\t27\t4\t6
                            1970-01-01T00:00:20.000000Z\t2\t3\t6\t9\t27\t5\t9
                            1970-01-01T00:00:30.000000Z\t3\t3\t6\t9\t27\t6\t12
                            1970-01-01T00:00:40.000000Z\t4\t3\t6\t9\t27\t7\t15
                            1970-01-01T00:00:50.000000Z\t5\t3\t6\t9\t27\t8\t18
                            1970-01-01T00:01:00.000000Z\t6\t3\t6\t9\t27\t9\t21
                            """);
        });
    }

    @Test
    public void testDeclareViewCannotOverrideByDefault() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "DECLARE @x := 6 select ts, v from " + TABLE1 + " where v = @x";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // sanity check
            assertQuery(VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            """);

            assertQuery("DECLARE @x := 5 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .fails(11, "variable is not overridable: @x");
        });
    }

    @Test
    public void testDeclareViewChainWithMixedOverridability() throws Exception {
        // Test: Complex chain with mixed OVERRIDABLE/non-overridable across views
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW1: @low is OVERRIDABLE, @high is NOT
            final String query1 = "DECLARE OVERRIDABLE @low := 2, @high := 7 SELECT ts, v FROM " + TABLE1 + " WHERE v >= @low AND v <= @high";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // VIEW2: wraps VIEW1, adds OVERRIDABLE @extra_filter
            final String query2 = "DECLARE OVERRIDABLE @extra_filter := 3 SELECT ts, v FROM " + VIEW1 + " WHERE v != @extra_filter";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            // Default: v >= 2 AND v <= 7 AND v != 3 -> 2, 4, 5, 6, 7
            assertQuery(VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:20.000000Z\t2
                            1970-01-01T00:00:40.000000Z\t4
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            """);

            // Can override @low (OVERRIDABLE in VIEW1) and @extra_filter (OVERRIDABLE in VIEW2)
            // @low=4, @extra_filter=5 -> v >= 4 AND v <= 7 AND v != 5 -> 4, 6, 7
            assertQuery("DECLARE @low := 4, @extra_filter := 5 SELECT * FROM " + VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:40.000000Z\t4
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            """);

            // Cannot override @high (not OVERRIDABLE in VIEW1)
            assertQuery("DECLARE @high := 8 SELECT * FROM " + VIEW2)
                    .noLeakCheck()
                    .fails(14, "variable is not overridable: @high");
        });
    }

    @Test
    public void testDeclareViewMixedOverridable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // view with mixed OVERRIDABLE and non-overridable variables
            // @lo is non-overridable (no modifier), @hi is OVERRIDABLE
            final String query1 = "DECLARE @lo := 5, OVERRIDABLE @hi := 8 select ts, v from " + TABLE1 + " where v >= @lo and v <= @hi";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // sanity check: no overrides at all
            assertQuery("VIEW1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts	v
                            1970-01-01T00:00:50.000000Z	5
                            1970-01-01T00:01:00.000000Z	6
                            1970-01-01T00:01:10.000000Z	7
                            1970-01-01T00:01:20.000000Z	8
                            """);

            // can override @hi (marked as OVERRIDABLE)
            assertQuery("DECLARE @hi := 7 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts	v
                            1970-01-01T00:00:50.000000Z	5
                            1970-01-01T00:01:00.000000Z	6
                            1970-01-01T00:01:10.000000Z	7
                            """);

            // override @lo (not overridable) should fail
            assertQuery("DECLARE @lo := 3 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .fails(12, "variable is not overridable: @lo");
        });
    }

    @Test
    public void testDeclareViewMultipleCannotOverrideByDefault() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // Neither variable is marked OVERRIDABLE, so neither can be overridden
            final String query1 = "DECLARE @x := 5, @y := 8 select ts, v from " + TABLE1 + " where v >= @x and v <= @y";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // default values
            assertQuery(VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """);

            assertQuery("DECLARE @x := 3 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .fails(11, "variable is not overridable: @x");
            assertQuery("DECLARE @y := 10 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .fails(11, "variable is not overridable: @y");
        });
    }

    @Test
    public void testDeclareViewReferencingViewCannotOverrideNonOverridable() throws Exception {
        // Test: VIEW1 has non-OVERRIDABLE @x, VIEW2 references VIEW1 and tries to use @x
        // This should fail because VIEW2 cannot override VIEW1's @x
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW1: uses @x for filtering (non-overridable)
            final String query1 = "DECLARE @x := 5 SELECT ts, v FROM " + TABLE1 + " WHERE v = @x";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // Attempting to query VIEW1 with external @x should fail
            assertQuery("DECLARE @x := 6 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .fails(11, "variable is not overridable: @x");
        });
    }

    @Test
    public void testDeclareViewReferencingViewWithDifferentVariableNames() throws Exception {
        // Test: VIEW1 has @x, VIEW2 references VIEW1 and has @marker (different name)
        // The variables are independent - no conflict
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW1: uses @x for filtering (non-overridable, default 5)
            final String query1 = "DECLARE @x := 5 SELECT ts, v FROM " + TABLE1 + " WHERE v = @x";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // VIEW2: references VIEW1, has its own @marker variable (different name)
            final String query2 = "DECLARE @marker := 999 SELECT ts, v, @marker as marker FROM " + VIEW1;
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            // VIEW1's @x=5 filters to v=5, VIEW2's @marker=999 is just a marker column
            assertQuery(VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv\tmarker
                            1970-01-01T00:00:50.000000Z\t5\t999
                            """);
        });
    }

    @Test
    public void testDeclareViewReferencingViewWithSameVariableName() throws Exception {
        // Test: VIEW1 has OVERRIDABLE @x, VIEW2 references VIEW1 and also declares @x
        // VIEW2's @x overrides VIEW1's @x since they share the same name
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // VIEW1: uses OVERRIDABLE @x for filtering (default 5)
            final String query1 = "DECLARE OVERRIDABLE @x := 5 SELECT ts, v FROM " + TABLE1 + " WHERE v = @x";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // VIEW2: references VIEW1, declares @x which overrides VIEW1's @x
            // Since both use @x, VIEW2's @x value (6) is used in VIEW1's filter
            final String query2 = "DECLARE @x := 6 SELECT ts, v, @x as marker FROM " + VIEW1;
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            // VIEW2's @x=6 overrides VIEW1's @x, so v=6 is selected
            assertQuery(VIEW2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv\tmarker
                            1970-01-01T00:01:00.000000Z\t6\t6
                            """);
        });
    }

    @Test
    public void testDeclareWithNullValues() throws Exception {
        // Test: DECLARE with NULL values and NULL comparisons
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nullable_data (ts TIMESTAMP, category SYMBOL, value INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO nullable_data VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 'A', 10), " +
                    "('2024-01-01T00:01:00.000000Z', 'B', NULL), " +
                    "('2024-01-01T00:02:00.000000Z', 'A', 20), " +
                    "('2024-01-01T00:03:00.000000Z', NULL, 30), " +
                    "('2024-01-01T00:04:00.000000Z', 'B', 40)");
            drainWalQueue();

            // VIEW with OVERRIDABLE default value for NULL replacement
            final String query1 = "DECLARE OVERRIDABLE @default_val := 0 " +
                    "SELECT ts, category, coalesce(value, @default_val) as value FROM nullable_data";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ")");
            drainWalAndViewQueues();

            // Default: NULL values replaced with 0
            assertQuery(VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .sizeMayVary()
                    .returns("""
                            ts\tcategory\tvalue
                            2024-01-01T00:00:00.000000Z\tA\t10
                            2024-01-01T00:01:00.000000Z\tB\t0
                            2024-01-01T00:02:00.000000Z\tA\t20
                            2024-01-01T00:03:00.000000Z\t\t30
                            2024-01-01T00:04:00.000000Z\tB\t40
                            """);

            // Override: NULL values replaced with -1
            assertQuery("DECLARE @default_val := -1 SELECT * FROM " + VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tcategory\tvalue
                            2024-01-01T00:00:00.000000Z\tA\t10
                            2024-01-01T00:01:00.000000Z\tB\t-1
                            2024-01-01T00:02:00.000000Z\tA\t20
                            2024-01-01T00:03:00.000000Z\t\t30
                            2024-01-01T00:04:00.000000Z\tB\t40
                            """);
        });
    }

    @Test
    public void testJoinWithViewAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (" +
                    "ts TIMESTAMP, " +
                    "ticker SYMBOL, " +
                    "price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 'AAPL', 150.0), " +
                    "('2024-01-01T00:01:00.000000Z', 'GOOG', 140.0), " +
                    "('2024-01-01T00:02:00.000000Z', 'MSFT', 151.0)");
            drainWalQueue();

            createView(VIEW1, "SELECT ts, ticker FROM x WHERE price > 145", "x");

            // view1 contains: AAPL (150.0) and MSFT (151.0), but not GOOG (140.0)
            // LEFT JOIN should match: AAPL->AAPL, GOOG->NULL, MSFT->MSFT
            assertQuery("SELECT view1.ts, x.ticker, x.price FROM x LEFT JOIN view1 ON ticker")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts\tticker\tprice
                            2024-01-01T00:00:00.000000Z\tAAPL\t150.0
                            \tGOOG\t140.0
                            2024-01-01T00:02:00.000000Z\tMSFT\t151.0
                            """);
        });
    }

    @Test
    public void testNonAsciiTableAndViewNames() throws Exception {
        assertMemoryLeak(() -> {
            final String TABLE1_1 = "Részvény_áíóúüűöő";
            final String TABLE1_2 = "RÉSZVÉNY_ÁÍÓÚÜŰÖŐ";
            final String TABLE2_1 = "Aкции_ягоды";
            final String TABLE2_2 = "AКЦИИ_ЯГОДЫ";
            final String VIEW1 = "股票";
            final String VIEW2 = "स्टॉक_के_शेयर";

            createTable(TABLE1_1);
            createTable(TABLE2_1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1_2 + " where v > 4";
            createView(VIEW1, query1, TABLE1_2);

            final String query2 = "select ts, k2, max(v) as v_max from '" + TABLE2_2 + "' where v > 6";
            createView(VIEW2, query2, TABLE2_2);

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "with t2 as (" + VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5') select t1.ts, v_max from " + TABLE1_2 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: Részvény_áíóúüűöő
                                    Hash
                                        UnionSymbolCast
                                          functions: [ts,k2::symbol,v_max]
                                            Union
                                                Filter filter: 7<v_max
                                                    Async Group By workers: 1
                                                      keys: [ts,k2]
                                                      values: [max(v)]
                                                      filter: 6<v
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: Aкции_ягоды
                                                Async Group By workers: 1
                                                  keys: [ts,k]
                                                  values: [max(v)]
                                                  filter: (4<v and k='k5')
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: Részvény_áíóúüűöő
                            """,
                    VIEW1, VIEW2
            );
        });
    }

    @Test
    public void testQueryViewInQuotes() throws Exception {
        assertMemoryLeak(() -> {
            final String query1 = "select 42 as col";
            createView(VIEW1, query1);

            assertQuery("SELECT * FROM '" + VIEW1 + "'")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            col
                            42
                            """);

            assertQuery("SELECT * FROM \"" + VIEW1 + "\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            col
                            42
                            """);
        });
    }

    @Test
    public void testQueryViewInQuotesJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + TABLE1 + " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + TABLE1 + " VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            drainWalQueue();
            createView(VIEW1, "SELECT * FROM " + TABLE1);

            assertQuery("SELECT * FROM " + TABLE1 + " JOIN '" + VIEW1 + "' ON (v)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tv\tts1\tv1
                            2024-01-01T00:00:00.000000Z\t1\t2024-01-01T00:00:00.000000Z\t1
                            2024-01-02T00:00:00.000000Z\t2\t2024-01-02T00:00:00.000000Z\t2
                            """);

            assertQuery("SELECT * FROM " + TABLE1 + " JOIN \"" + VIEW1 + "\" ON (v)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tv\tts1\tv1
                            2024-01-01T00:00:00.000000Z\t1\t2024-01-01T00:00:00.000000Z\t1
                            2024-01-02T00:00:00.000000Z\t2\t2024-01-02T00:00:00.000000Z\t2
                            """);
        });
    }

    @Test
    public void testSampleByOrdeByForceDesignatedTimestampMix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE eq_equities_market_data (" +
                    "timestamp TIMESTAMP, " +
                    "symbol SYMBOL, " +
                    "venue SYMBOL, " +
                    "asks DOUBLE[][], bids DOUBLE[][]" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("INSERT INTO eq_equities_market_data VALUES " +
                    "(0, 'HSBC', 'LSE', ARRAY[ [11.4, 12], [10.3, 15] ], ARRAY[ [21.1, 31], [20.1, 21] ]), " +
                    "(1, 'HSBC', 'HKG', ARRAY[ [11.5, 13], [10.4, 14] ], ARRAY[ [21.2, 32], [20.2, 22] ]), " +
                    "(2, 'BAC', 'NYSE', ARRAY[ [11.6, 17], [10.5, 15] ], ARRAY[ [21.3, 33], [20.3, 23] ]), " +
                    "(3, 'HSBC', 'LSE', ARRAY[ [11.2, 30], [10.2, 16] ], ARRAY[ [21.4, 34], [20.4, 24] ]), " +
                    "(4, 'BAC', 'NYSE', ARRAY[ [11.4, 20], [10.4,  7] ], ARRAY[ [21.5, 35], [20.5, 25] ]), " +
                    "(5, 'MQG', 'ASX', ARRAY[ [16.0,  3], [15.0,  2] ], ARRAY[ [15.6, 36], [14.6, 26] ])"
            );
            drainWalQueue();

            createView(VIEW1, """
                    select timestamp, symbol, count(bids[1][1]) as total
                    from eq_equities_market_data
                    where symbol = 'HSBC'
                    sample by 10s
                    order by timestamp desc
                    """);

            createView(VIEW2, """
                    (view1 order by timestamp) timestamp(timestamp)
                    """);

            assertQuery("""
                    select timestamp, count() from view2
                    sample by 10m
                    """)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .noRandomAccess()
                    .returns("""
                            timestamp\tcount
                            1970-01-01T00:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testSelectFromViewWithDeclare() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            String query = VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    query,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 5<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            query = "DECLARE @x := 1, @y := 2 select ts, @x as one, @y * v_max from " + VIEW1 + " where v_max > 6";
            assertQueryAndPlan(
                    """
                            ts\tone\tcolumn
                            1970-01-01T00:01:10.000000Z\t1\t14
                            1970-01-01T00:01:20.000000Z\t1\t16
                            """,
                    query,
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            VirtualRecord
                              functions: [ts,1,2*v_max]
                                Filter filter: 6<v_max
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: 5<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testSelectViewFields() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            String query = VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    query,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 5<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            query = "select ts, v_max from " + VIEW1;
            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    query,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: 5<v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testSelectViewMixedCase() throws Exception {
        assertMemoryLeak(() -> {
            final String TABLE1_1 = "taBLe1";
            final String TABLE1_2 = "TABLe1";
            createTable(TABLE1_1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1_2 + " where v > 5";
            final String VIEW1_1 = "viEw1";
            final String VIEW1_2 = "ViEW1";
            final String VIEW1_3 = "vIeW1";
            createView(VIEW1_1, query1, TABLE1_2);

            String query = VIEW1_2;
            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    query,
                    "QUERY PLAN\n" +
                            "Async Group By workers: 1\n" +
                            "  keys: [ts,k]\n" +
                            "  values: [max(v)]\n" +
                            "  filter: 5<v\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: " + TABLE1_1 + "\n",
                    VIEW1
            );

            query = "select ts, v_max from " + VIEW1_3;
            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    query,
                    "QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,k]\n" +
                            "      values: [max(v)]\n" +
                            "      filter: 5<v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: " + TABLE1_1 + "\n",
                    VIEW1
            );
        });
    }

    @Test
    public void testSharedViewLayersReadManyTimesWithinModelBudget() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE raw_trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty DOUBLE, ccy SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE raw_quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE, ask DOUBLE, ccy SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE fx (ts TIMESTAMP, ccy SYMBOL, rate DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO raw_trades VALUES ('2026-01-01T00:00:00', 'A', 10, 1, 'EUR'), ('2026-01-01T00:01:00', 'B', 20, 2, 'USD')");
            execute("INSERT INTO raw_quotes VALUES ('2026-01-01T00:00:00', 'A', 9.9, 10.1, 'EUR'), ('2026-01-01T00:01:00', 'B', 19.9, 20.1, 'USD')");
            execute("INSERT INTO fx VALUES ('2026-01-01T00:00:00', 'EUR', 1.1), ('2026-01-01T00:00:00', 'USD', 1.0)");
            // A trading analytics layer of ten views: fx_v, trades_usd and quotes_usd are read by
            // several views each, so a read of dashboard_v expands them several times.
            execute("CREATE VIEW fx_v AS (SELECT ts, ccy, rate FROM fx)");
            execute("CREATE VIEW trades_v AS (SELECT ts, sym, px, qty, ccy FROM raw_trades WHERE qty > 0)");
            execute("CREATE VIEW quotes_v AS (SELECT ts, sym, bid, ask, ccy FROM raw_quotes WHERE ask > bid)");
            execute("CREATE VIEW trades_usd AS (SELECT t.ts, t.sym, t.px * f.rate px, t.qty FROM trades_v t ASOF JOIN fx_v f ON (ccy))");
            execute("CREATE VIEW quotes_usd AS (SELECT q.ts, q.sym, q.bid * f.rate bid, q.ask * f.rate ask FROM quotes_v q ASOF JOIN fx_v f ON (ccy))");
            execute("CREATE VIEW vwap_v AS (SELECT ts, sym, sum(px * qty) / sum(qty) vwap FROM trades_usd SAMPLE BY 1h)");
            execute("CREATE VIEW spread_v AS (SELECT ts, sym, avg(ask - bid) spread FROM quotes_usd SAMPLE BY 1h)");
            execute("CREATE VIEW markout_v AS (SELECT t.ts, t.sym, t.px - (q.bid + q.ask) / 2 markout FROM trades_usd t JOIN quotes_usd q ON t.sym = q.sym)");
            execute("CREATE VIEW slippage_v AS (SELECT t.ts, t.sym, t.px - v.vwap slip FROM trades_usd t JOIN vwap_v v ON t.sym = v.sym)");
            execute("CREATE VIEW dashboard_v AS (SELECT v.ts, v.sym, v.vwap, s.spread, m.markout, l.slip FROM vwap_v v JOIN spread_v s ON v.sym = s.sym JOIN markout_v m ON v.sym = m.sym JOIN slippage_v l ON v.sym = l.sym)");
            drainWalAndViewQueues();
            // Every read of dashboard_v takes 61 query models. Five reads take 305, far below the
            // budget of the statement, 1,000 models plus 2 for each character of its text and of
            // the bodies of the views it reads.
            final StringBuilder periods = new StringBuilder();
            for (int i = 0; i < 5; i++) {
                if (i > 0) {
                    periods.append(" UNION ALL ");
                }
                periods.append("SELECT ").append(i).append(" period, count() c FROM dashboard_v WHERE ts > dateadd('d', -").append(i).append(", '2026-01-02')");
            }
            assertQuery(periods)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            period\tc
                            0\t0
                            1\t0
                            2\t2
                            3\t2
                            4\t2
                            """);
            assertQuery("SELECT count() FROM dashboard_v d0 JOIN dashboard_v d1 ON d0.sym = d1.sym JOIN dashboard_v d2 ON d0.sym = d2.sym JOIN dashboard_v d3 ON d0.sym = d3.sym JOIN dashboard_v d4 ON d0.sym = d4.sym")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            2
                            """);
        });
    }

    @Test
    public void testSharedViewLayersWithinModelBudget() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE fact (ts TIMESTAMP, k1 INT, k2 INT, k3 INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE dim (k INT, name STRING)");
            execute("INSERT INTO fact VALUES ('2026-01-01', 1, 1, 1, 1.0)");
            execute("INSERT INTO dim VALUES (1, 'one')");
            // A star schema: a chain of four views under three dimension views, ten marts that each
            // join a fact view with the three dimensions, and a KPI view that joins the marts. A
            // read of kpi10 expands the dimension chain 30 times.
            execute("CREATE VIEW ref_src AS (SELECT k, name FROM dim)");
            execute("CREATE VIEW ref_raw AS (SELECT k, name FROM ref_src)");
            execute("CREATE VIEW ref_v AS (SELECT k, name FROM ref_raw WHERE name IS NOT NULL)");
            execute("CREATE VIEW dim_base AS (SELECT k, upper(name) name FROM ref_v)");
            execute("CREATE VIEW fact_v AS (SELECT ts, k1, k2, k3, v FROM fact)");
            for (int d = 1; d < 4; d++) {
                execute("CREATE VIEW dim" + d + " AS (SELECT k, name n" + d + " FROM dim_base)");
            }
            for (int m = 1; m < 11; m++) {
                execute("CREATE VIEW mart" + m + " AS (SELECT f.ts, f.v * " + m + " v, a.n1, b.n2, c.n3 FROM fact_v f JOIN dim1 a ON f.k1 = a.k JOIN dim2 b ON f.k2 = b.k JOIN dim3 c ON f.k3 = c.k)");
            }
            drainWalAndViewQueues();
            // Creating kpi10 and reading it once take 381 query models, and the self-join of the
            // five-mart kpi5 takes about as many, far below the budget of each statement, 1,000
            // models plus 2 for each character of its text and of the bodies of the views it reads.
            execute("CREATE VIEW kpi10 AS (" + kpiBody(10) + ")");
            execute("CREATE VIEW kpi5 AS (" + kpiBody(5) + ")");
            drainWalAndViewQueues();
            assertViewState("kpi10");
            assertViewState("kpi5");
            assertQuery("SELECT v1, v2, v10 FROM kpi10")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v1\tv2\tv10
                            1.0\t2.0\t10.0
                            """);
            assertQuery(kpiBody(10))
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tv1\tv2\tv3\tv4\tv5\tv6\tv7\tv8\tv9\tv10
                            2026-01-01T00:00:00.000000Z\t1.0\t2.0\t3.0\t4.0\t5.0\t6.0\t7.0\t8.0\t9.0\t10.0
                            """);
            assertQuery("SELECT count() FROM kpi5 a JOIN kpi5 b ON a.ts = b.ts")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            1
                            """);
        });
    }

    @Test
    public void testSpecifyTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "(select v1.ts, v1.v_max from " + VIEW1 + " v1 where v_max > 6) timestamp(ts)",
                    "ts",
                    true,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                SelectedRecord
                                    Filter filter: 6<v_max
                                        Async Group By workers: 1
                                          keys: [ts,k]
                                          values: [max(v)]
                                          filter: 5<v
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testViewAllowNonDetermisticFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // rnd_byte() is technically a non-deterministic function
            String view = "select * from " + TABLE1 + " where rnd_byte() >= 0";

            createView(VIEW1, view, TABLE1);

            assertQuery(VIEW1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts	k	k2	v
                            1970-01-01T00:00:00.000000Z	k0	k2_0	0
                            1970-01-01T00:00:10.000000Z	k1	k2_1	1
                            1970-01-01T00:00:20.000000Z	k2	k2_2	2
                            1970-01-01T00:00:30.000000Z	k3	k2_3	3
                            1970-01-01T00:00:40.000000Z	k4	k2_4	4
                            1970-01-01T00:00:50.000000Z	k5	k2_5	5
                            1970-01-01T00:01:00.000000Z	k6	k2_6	6
                            1970-01-01T00:01:10.000000Z	k7	k2_7	7
                            1970-01-01T00:01:20.000000Z	k8	k2_8	8
                            """);
        });
    }

    @Test
    public void testViewBodiesCountTowardsModelBudget() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k (s SYMBOL)");
            execute("INSERT INTO k VALUES ('1'), ('2'), ('3'), ('4'), ('5')");
            // The body of wide reads its CTE 300 times and takes 1,202 query models, more than the
            // 1,000 every statement may take, and its 8,178 characters allow 16,356 more.
            final StringBuilder body = new StringBuilder("WITH c AS (SELECT x::STRING s FROM long_sequence(3)) SELECT count() c FROM k WHERE s IN (SELECT s FROM c)");
            for (int i = 1; i < 300; i++) {
                body.append(" AND s IN (SELECT s FROM c)");
            }
            execute("CREATE VIEW wide AS (" + body + ')');
            drainWalAndViewQueues();
            assertViewState("wide");
            // A statement that reads a view parses the view's body, and the body's text counts
            // towards the budget of the statement as its own text does, so the 18-character read
            // keeps the allowance of the body.
            assertQuery("SELECT c FROM wide")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c
                            3
                            """);
        });
    }

    @Test
    public void testViewBodyNestedWithShadowsBodyCte() throws Exception {
        // A nested WITH in a view body may reuse the name of a CTE of the body. CREATE VIEW and
        // every read of the view parse the body with the same bindings, whatever CTEs the
        // statement that reads the view defines.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE)");
            execute("INSERT INTO trades VALUES ('AAPL', 100.5), ('MSFT', 300.75)");
            // The sub-query of the filter reads the inner c. Creating these views failed with
            // "duplicate name" once a sub-query in an expression saw the WITH clauses around it.
            createView(
                    "v_shadow1",
                    "WITH c AS (SELECT 'AAPL'::SYMBOL symbol) SELECT * FROM trades WHERE symbol IN (DECLARE @v := 1 WITH c AS (SELECT 'MSFT'::SYMBOL symbol) SELECT symbol FROM c)"
            );
            createView(
                    "v_shadow2",
                    "SELECT * FROM (WITH c AS (SELECT 'AAPL'::SYMBOL symbol) SELECT * FROM trades WHERE symbol IN (SELECT symbol FROM (WITH c AS (SELECT 'MSFT'::SYMBOL symbol) SELECT symbol FROM c)))"
            );
            // d, defined before the inner c, reads the body's c on both of its reads.
            createView(
                    "v_shadow3",
                    "WITH c AS (SELECT 'AAPL'::SYMBOL symbol) SELECT * FROM trades WHERE symbol IN (SELECT symbol FROM (WITH d AS (SELECT symbol FROM c), c AS (SELECT 'MSFT'::SYMBOL symbol) SELECT symbol FROM d UNION ALL SELECT symbol FROM d))"
            );

            final String msft = """
                    symbol\tprice
                    MSFT\t300.75
                    """;
            final String msftTwice = """
                    symbol\tprice
                    MSFT\t300.75
                    MSFT\t300.75
                    """;
            assertQuery("SELECT * FROM v_shadow1")
                    .noLeakCheck()
                    .returns(msft);
            assertQuery("WITH c AS (SELECT 'AAPL'::SYMBOL symbol) SELECT * FROM v_shadow1")
                    .noLeakCheck()
                    .returns(msft);
            assertQuery("SELECT * FROM v_shadow1 UNION ALL SELECT * FROM v_shadow1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(msftTwice);
            assertQuery("SELECT * FROM v_shadow2")
                    .noLeakCheck()
                    .returns(msft);
            assertQuery("WITH c AS (SELECT 'AAPL'::SYMBOL symbol) SELECT * FROM v_shadow2")
                    .noLeakCheck()
                    .returns(msft);
            assertQuery("SELECT * FROM v_shadow2 UNION ALL SELECT * FROM v_shadow2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns(msftTwice);

            // A CTE c of the reading statement would add MSFT if it reached d.
            final String callerCte = "WITH c AS (SELECT 'MSFT'::SYMBOL symbol) ";
            final String aapl = """
                    symbol\tprice
                    AAPL\t100.5
                    """;
            assertQuery("SELECT * FROM v_shadow3")
                    .noLeakCheck()
                    .returns(aapl);
            assertQuery(callerCte + "SELECT * FROM v_shadow3")
                    .noLeakCheck()
                    .returns(aapl);
            assertQuery(callerCte + "SELECT * FROM v_shadow3 UNION ALL SELECT * FROM v_shadow3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            symbol\tprice
                            AAPL\t100.5
                            AAPL\t100.5
                            """);
        });
    }

    @Test
    public void testViewCteReadBySubQueries() throws Exception {
        // A sub-query in a view body sees the WITH clauses around it in the body, as CREATE VIEW
        // does when it validates the body. Reading these views used to fail with "table does not
        // exist", and a caller's CTE of the same name used to stand in for the view's own.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE)");
            execute("CREATE TABLE pub (symbol SYMBOL, price DOUBLE)");
            execute("CREATE TABLE allowed (symbol SYMBOL)");
            execute("INSERT INTO trades VALUES ('AAPL', 100.5), ('MSFT', 300.75)");
            execute("INSERT INTO pub VALUES ('AAPL', 1.0), ('MSFT', 2.0)");
            execute("INSERT INTO allowed VALUES ('AAPL')");
            createView(
                    "v_in",
                    "WITH c AS (SELECT symbol FROM allowed) SELECT * FROM trades WHERE symbol IN (SELECT symbol FROM c)"
            );
            createView(
                    "v_pivot",
                    "WITH c AS (SELECT symbol FROM trades) SELECT * FROM pub PIVOT (sum(price) FOR symbol IN (SELECT DISTINCT symbol FROM c ORDER BY symbol))"
            );

            final String aapl = """
                    symbol\tprice
                    AAPL\t100.5
                    """;
            assertQuery("SELECT * FROM v_in")
                    .noLeakCheck()
                    .returns(aapl);
            assertQuery("WITH c AS (SELECT 'MSFT'::SYMBOL symbol) SELECT * FROM v_in")
                    .noLeakCheck()
                    .returns(aapl);
            final String pivot = """
                    AAPL\tMSFT
                    1.0\t2.0
                    """;
            assertQuery("SELECT * FROM v_pivot")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(pivot);
            assertQuery("WITH c AS (SELECT 'MSFT'::SYMBOL symbol) SELECT * FROM v_pivot")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(pivot);

            // CREATE VIEW used to reject this body: a sub-query saw only the top-level WITH.
            createView(
                    "v_nested",
                    "SELECT * FROM (WITH c AS (SELECT symbol FROM allowed) SELECT * FROM trades WHERE symbol IN (SELECT symbol FROM c))"
            );
            assertQuery("SELECT * FROM v_nested")
                    .noLeakCheck()
                    .returns(aapl);
        });
    }

    @Test
    public void testViewDenseChainHasNodeBudget() throws Exception {
        assertMemoryLeak(() -> {
            // An expansion of a view takes as many expression nodes as its body holds, but only one
            // or two query models. The body of v0 sums 430 terms, and each view after it reads the
            // one before twice. A statement may take 10,000 nodes, plus 20 for each character of its
            // text and of the body of each view it reads.
            final StringBuilder v0 = new StringBuilder("CREATE VIEW v0 AS (SELECT 1");
            for (int i = 1; i < 430; i++) {
                v0.append("+1");
            }
            execute(v0.append(" a FROM long_sequence(1))").toString());
            for (int i = 1; i < 6; i++) {
                execute("CREATE VIEW v" + i + " AS (SELECT * FROM v" + (i - 1) + " UNION ALL SELECT * FROM v" + (i - 1) + ')');
            }
            drainWalAndViewQueues();
            // A read of v5 expands v0 32 times, 27,777 nodes of its 32,700.
            assertQuery("SELECT count(), sum(a) FROM v5")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            32\t13760
                            """);
            // v6 would expand v0 64 times. While only models counted, the chain could be created up
            // to v9, and the compiler kept 464 MB after compiling a 22-character read of v9. The
            // parser refuses the expansion that finds the nodes spent, at the second read of v5.
            final String v6 = "CREATE VIEW v6 AS (SELECT * FROM v5 UNION ALL SELECT * FROM v5)";
            assertExceptionNoLeakCheck(
                    v6,
                    v6.lastIndexOf("v5"),
                    "statement is too complex to parse [nodes=33847, max=33360]"
            );
            final String read = "SELECT count() FROM (SELECT * FROM v5 UNION ALL SELECT * FROM v5)";
            assertExceptionNoLeakCheck(
                    read,
                    read.lastIndexOf("v5"),
                    "statement is too complex to parse [nodes=33848, max=33400]"
            );
        });
    }

    @Test
    public void testViewDoublingChainHasModelBudget() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE VIEW v0 AS (SELECT x::STRING s, x FROM long_sequence(3))");
            for (int i = 1; i < 9; i++) {
                execute("CREATE VIEW v" + i + " AS (SELECT * FROM v" + (i - 1) + " UNION ALL SELECT * FROM v" + (i - 1) + ')');
            }
            drainWalAndViewQueues();
            // Every read of a view expands its body anew, and the expansion expands every view the
            // body reads. So when each view reads the one before it twice, the query models double
            // with every level: a 22-character read of v9 used to run out of a 1.5 GB heap. A
            // statement may take 1,000 models, plus 2 for each character of its text and of the
            // body of each view it reads. A read of v8 takes 1,534 of its 1,834.
            assertQuery("SELECT count(), sum(x) FROM v8")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            768\t1536
                            """);
            // v9 would take over 3,000. The parser refuses the expansion that finds the budget
            // spent. That expansion sits in the body of a view, so the error points at the read of
            // the outermost view in the statement that it expands, the second read of v8.
            final String v9 = "CREATE VIEW v9 AS (SELECT * FROM v8 UNION ALL SELECT * FROM v8)";
            assertExceptionNoLeakCheck(
                    v9,
                    v9.lastIndexOf("v8"),
                    "statement is too complex to parse [models=1902, max=1900]"
            );
            final String read = "SELECT count() FROM (SELECT * FROM v8 UNION ALL SELECT * FROM v8)";
            assertExceptionNoLeakCheck(
                    read,
                    read.lastIndexOf("v8"),
                    "statement is too complex to parse [models=1906, max=1904]"
            );
        });
    }

    @Test
    public void testViewUpdateRejectsLiveWalProgressAtViewReference() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(
                    VIEW1,
                    "SELECT * FROM " + TABLE1 + " WHERE wait_wal_table('" + TABLE1 + "')",
                    TABLE1
            );
            final String walSql = "UPDATE " + TABLE1 + " t SET v = t.v + 1 FROM " + VIEW1 + " x WHERE t.ts = x.ts";
            assertExceptionNoLeakCheck(
                    walSql,
                    0,
                    "UPDATE statements with join are not supported yet for WAL tables"
            );
            execute("CREATE TABLE plain (ts TIMESTAMP, v LONG)");
            final String sql = "UPDATE plain t SET v = t.v + 1 FROM " + VIEW1 + " x WHERE t.ts = x.ts";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf(VIEW1),
                    "UPDATE cannot require live WAL progress"
            );
        });
    }

    @Test
    public void testViewFilterPushedDownToTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 5<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            """,
                    VIEW1 + " where k = 'k6'",
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: (5<v and k='k6')
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1 + " where k in ('k6', 'k8')",
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: (5<v and k in [k6,k8])
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    "(" + VIEW1 + " where k in ('k6', 'k8')) where k = 'k8'",
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: (5<v and k in [k6,k8] and k='k8')
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testViewJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "select v1.ts, v_max from " + VIEW1 + " v1 join " + TABLE2 + " t2 on t2.v = v1.v_max",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: t2.v=v1.v_max
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: 4<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table2
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "select t1.ts, v_max from " + TABLE1 + " t1 join (" + VIEW1 + " where v_max > 6) t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        SelectedRecord
                                            Filter filter: 6<v_max
                                                Async Group By workers: 1
                                                  keys: [ts,k]
                                                  values: [max(v)]
                                                  filter: 4<v
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "with t2 as (" + VIEW1 + " where v_max > 6) select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max", "ts",
                    false,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        SelectedRecord
                                            Filter filter: 6<v_max
                                                Async Group By workers: 1
                                                  keys: [ts,k]
                                                  values: [max(v)]
                                                  filter: 4<v
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max\tts1\tk1\tv_max1
                            1970-01-01T00:00:50.000000Z\tk5\t5\t1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6\t1970-01-01T00:01:00.000000Z\tk6\t6
                            """,
                    VIEW1 + " v11 join " + VIEW1 + " v12 on v_max where v12.v_max < 7",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join Light
                                  condition: v12.v_max=v11.v_max
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: 4<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                                    Hash
                                        Filter filter: v_max<7
                                            Async Group By workers: 1
                                              keys: [ts,k]
                                              values: [max(v)]
                                              filter: 4<v
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "with t2 as (" + VIEW1 + " v11 join " + VIEW1 + " v12 on v_max where v12.v_max > 6) select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        SelectedRecord
                                            Hash Join Light
                                              condition: v12.v_max=v11.v_max
                                                Async Group By workers: 1
                                                  keys: [ts,k]
                                                  values: [max(v)]
                                                  filter: 4<v
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: table1
                                                Hash
                                                    Filter filter: 6<v_max
                                                        Async Group By workers: 1
                                                          keys: [ts,k]
                                                          values: [max(v)]
                                                          filter: 4<v
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    @Test
    public void testViewPastModelBudgetBecomesInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE VIEW w AS (SELECT x FROM long_sequence(3))");
            execute("CREATE VIEW v0 AS (SELECT x FROM long_sequence(3))");
            for (int i = 1; i < 9; i++) {
                execute("CREATE VIEW v" + i + " AS (SELECT * FROM v" + (i - 1) + " UNION ALL SELECT * FROM v" + (i - 1) + ')');
            }
            drainWalAndViewQueues();
            // The new body of v0 reads w twice, which adds to the query models every read of v0
            // in the chain takes. The statement that replaces v0 takes few, but v8 now needs more
            // than its budget allows, so recompiling it after the change marks it invalid, and
            // reading it fails, at the read of v8.
            execute("CREATE OR REPLACE VIEW v0 AS (SELECT * FROM w UNION ALL SELECT * FROM w)");
            drainWalAndViewQueues();
            // The view compiler job compiles the body of v8 as a statement of its own.
            assertViewState("v7");
            assertViewState("v8", "statement is too complex to parse [models=1832, max=1830]");
            assertExceptionNoLeakCheck(
                    "SELECT count() FROM v8",
                    "SELECT count() FROM ".length(),
                    "statement is too complex to parse [models=1878, max=1874]"
            );
            assertQuery("SELECT count(), sum(x) FROM v7")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            768\t1536
                            """);
            // Putting the old body back brings v8 under the budget again.
            execute("CREATE OR REPLACE VIEW v0 AS (SELECT x FROM long_sequence(3))");
            drainWalAndViewQueues();
            assertViewState("v8");
            assertQuery("SELECT count(), sum(x) FROM v8")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            768\t1536
                            """);
        });
    }

    @Test
    public void testViewReadAfterConstantsKeepsGeneratedColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            // The test configuration names an unaliased constant with a dot in it column1, column2
            // and so on, from a counter the parser keeps for the whole statement. A view body used
            // to go on counting where the reading statement had got to, so a view read after the
            // statement's own constants exposed other names than its metadata lists: after
            // (SELECT 6.5, 7.5), the body of v named 1.5 and 2.5 column2 and column3, and vs
            // returned 1.5 for column2. Every expansion now numbers the body from 1.
            execute("CREATE VIEW v AS (SELECT 1.5, 2.5)");
            execute("CREATE VIEW vs AS (SELECT column2 FROM v)");
            execute("CREATE VIEW vq AS (DECLARE OVERRIDABLE @q := (SELECT 1.5, 2.5) SELECT column2 FROM @q)");
            execute("CREATE VIEW vv AS (SELECT 3.5, 4.5, * FROM v)");
            execute("CREATE VIEW vc AS (WITH c AS (SELECT 1.5, 2.5) SELECT column2 FROM c)");
            execute("CREATE VIEW vd AS (DECLARE @q := (SELECT 1.5, 2.5) SELECT column2 FROM @q)");
            execute("CREATE VIEW vjoin AS (SELECT * FROM (SELECT 6.5, 7.5) x CROSS JOIN vs)");
            drainWalAndViewQueues();
            assertQuery("SELECT * FROM (SELECT 6.5, 7.5) x CROSS JOIN vs")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1\tcolumn2\tcolumn21
                            6.5\t7.5\t2.5
                            """);
            // Two reads after the constants used to return 1.5 twice.
            assertQuery("SELECT * FROM (SELECT 6.5, 7.5) x CROSS JOIN vs y CROSS JOIN vs z")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1\tcolumn2\tcolumn21\tcolumn22
                            6.5\t7.5\t2.5\t2.5
                            """);
            // The first read of vq parses the caller's value for @q, which moves the counter, and
            // the second read of vq used to name its own value's columns from there.
            assertQuery("SELECT * FROM (DECLARE @q := (SELECT 6.5, 7.5) SELECT * FROM vq) UNION ALL SELECT * FROM vq")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            7.5
                            2.5
                            """);
            // A view inside a view: vv numbers its own constants and the body of v from 1, as the
            // metadata of vv lists them, wherever the statement reads vv.
            assertQuery("SELECT \"column\" FROM table_columns('vv')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column
                            column1
                            column2
                            column11
                            column21
                            """);
            assertQuery("SELECT column21 FROM (SELECT 6.5, 7.5) x CROSS JOIN vv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column21
                            2.5
                            """);
            // A CTE and a declared sub-query inside a view body.
            assertQuery("SELECT * FROM (SELECT 6.5, 7.5) x CROSS JOIN vc")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1\tcolumn2\tcolumn21
                            6.5\t7.5\t2.5
                            """);
            assertQuery("SELECT * FROM (SELECT 6.5, 7.5) x CROSS JOIN vd")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1\tcolumn2\tcolumn21
                            6.5\t7.5\t2.5
                            """);
            // A view whose body reads vs after constants: CREATE VIEW parsed the body the same way,
            // so the view stored 1.5 under the name of the column that holds 2.5 in vs.
            assertQuery("SELECT \"column\" FROM table_columns('vjoin')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column
                            column1
                            column2
                            column21
                            """);
            assertQuery("vjoin")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1\tcolumn2\tcolumn21
                            6.5\t7.5\t2.5
                            """);
            assertQuery("SELECT column21 FROM (SELECT 8.5, 9.5) z CROSS JOIN vjoin")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column21
                            2.5
                            """);
            // A CTE that reads vs after constants, read twice.
            assertQuery("WITH w AS (SELECT * FROM (SELECT 6.5, 7.5) x CROSS JOIN vs) SELECT column21 FROM w UNION ALL SELECT column21 FROM w")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column21
                            2.5
                            2.5
                            """);
            // A read moves the statement's counter on as far as the body moves it from 1, so the
            // constants after it keep the names they get when the statement reads v first.
            assertQuery("SELECT * FROM (SELECT 7.5, 8.5) x CROSS JOIN v a CROSS JOIN (SELECT 5.5, 6.5) b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1\tcolumn2\tcolumn11\tcolumn21\tcolumn3\tcolumn4
                            7.5\t8.5\t1.5\t2.5\t5.5\t6.5
                            """);
        });
    }

    @Test
    public void testViewReadMoreThanOnceKeepsGeneratedColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            // The test configuration names an unaliased constant with a dot in it column1, column2
            // and so on, from a counter the parser keeps for the whole statement. Every read of a
            // view parses its body, and a later read used to go on counting where the statement
            // had got to: the second read of v saw column2 and column3, and returned 1.5 for
            // column2.
            execute("CREATE VIEW v AS (SELECT 1.5, 2.5)");
            execute("CREATE VIEW v_union AS (SELECT column2 FROM v UNION ALL SELECT column2 FROM v)");
            execute("CREATE VIEW v_join AS (SELECT * FROM v a CROSS JOIN v b)");
            drainWalAndViewQueues();
            assertQuery("SELECT column2 FROM v UNION ALL SELECT column2 FROM v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            """);
            assertQuery("v_union")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column2
                            2.5
                            2.5
                            """);
            // The second read had no column1 at all.
            assertQuery("SELECT column1 FROM v UNION ALL SELECT column1 FROM v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1
                            1.5
                            1.5
                            """);
            // The reads expose the same names, so a join of them suffixes the second read's. The
            // second read used to expose column2 and column3, which the join named column21 and
            // column3, and the view kept those names.
            assertQuery("SELECT * FROM v a CROSS JOIN v b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column11	column21
                            1.5	2.5	1.5	2.5
                            """);
            assertQuery("SELECT \"column\" FROM table_columns('v_join')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column
                            column1
                            column2
                            column11
                            column21
                            """);
            // A CTE that reads v, read twice. The definition of w expands v first, from the count
            // of 1, and names 3.5 and 4.5 after it. The second reference parses w again from the
            // same count, and expands v again: it reads v under the names of the first read, and
            // moves the count on as far, so it names 3.5 and 4.5 the same as well.
            assertQuery("WITH w AS (SELECT * FROM v CROSS JOIN (SELECT 3.5, 4.5)) SELECT column3 FROM w UNION ALL SELECT column3 FROM w")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column3
                            4.5
                            4.5
                            """);
            // So the statement's own constants after two reads get the names they got before.
            assertQuery("SELECT * FROM (SELECT * FROM v UNION ALL SELECT * FROM v) a CROSS JOIN (SELECT 5.5, 6.5) b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            column1	column2	column3	column4
                            1.5	2.5	5.5	6.5
                            1.5	2.5	5.5	6.5
                            """);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertQuery("SELECT column2 FROM v UNION ALL SELECT column2 FROM v")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                column2
                                2.5
                                2.5
                                """);
                // Nothing the parser keeps about the views of one statement reaches the next one.
                // The read of v here comes after the constants of the select list, and names 1.5
                // and 2.5 column1 and column2 from the count of 1, as the metadata of v lists them.
                // A single read used to go on from the count of 2 and name them column2 and
                // column3, which the select list renamed column21 and column3.
                assertQuery("SELECT 3.5, 4.5, * FROM v")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                column1	column2	column11	column21
                                3.5	4.5	1.5	2.5
                                """);
            }
        });
    }

    @Test
    public void testViewUnion() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            createView(VIEW2, query2, TABLE2);

            assertQueryAndPlan(
                    """
                            ts\tk2\tv_max
                            1970-01-01T00:01:20.000000Z\tk2_8\t8
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            """,
                    VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5'",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            UnionSymbolCast
                              functions: [ts,k2::symbol,v_max]
                                Union
                                    Filter filter: 7<v_max
                                        Async Group By workers: 1
                                          keys: [ts,k2]
                                          values: [max(v)]
                                          filter: 6<v
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: table2
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: (4<v and k='k5')
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                            """,
                    VIEW1, VIEW2
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            1970-01-01T00:00:50.000000Z\t5
                            """,
                    "(select ts, v_max from " + VIEW2 + " where v_max > 6) union (select ts, v_max from " + VIEW1 + " where k = 'k5')",
                    null,
                    false,
                    false,
                    """
                            QUERY PLAN
                            Union
                                SelectedRecord
                                    Filter filter: 6<v_max
                                        Async Group By workers: 1
                                          keys: [ts,k2]
                                          values: [max(v)]
                                          filter: 6<v
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: table2
                                SelectedRecord
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: (4<v and k='k5')
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                            """,
                    VIEW1, VIEW2
            );

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "with t2 as (" + VIEW2 + " where v_max > 7 union " + VIEW1 + " where k = 'k5') select t1.ts, v_max from " + TABLE1 + " t1 join t2 on t1.v = t2.v_max",
                    "ts",
                    false,
                    true,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Hash Join
                                  condition: t2.v_max=t1.v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                                    Hash
                                        UnionSymbolCast
                                          functions: [ts,k2::symbol,v_max]
                                            Union
                                                Filter filter: 7<v_max
                                                    Async Group By workers: 1
                                                      keys: [ts,k2]
                                                      values: [max(v)]
                                                      filter: 6<v
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: table2
                                                Async Group By workers: 1
                                                  keys: [ts,k]
                                                  values: [max(v)]
                                                  filter: (4<v and k='k5')
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: table1
                            """,
                    VIEW1, VIEW2
            );
        });
    }

    @Test
    public void testViewWithAlias() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 5";
            createView(VIEW1, query1, TABLE1);

            assertQueryAndPlan(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "select v1.ts, v1.v_max from " + VIEW1 + " v1 where v_max > 6",
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            SelectedRecord
                                Filter filter: 6<v_max
                                    Async Group By workers: 1
                                      keys: [ts,k]
                                      values: [max(v)]
                                      filter: 5<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table1
                            """,
                    VIEW1
            );
        });
    }

    // SELECT m1.ts, m1.v v1, ..., m<marts>.v v<marts> FROM mart1 m1
    // JOIN mart2 m2 ON m1.ts = m2.ts ... JOIN mart<marts> ...
    private static String kpiBody(int marts) {
        final StringBuilder sql = new StringBuilder("SELECT m1.ts, m1.v v1");
        for (int i = 2; i <= marts; i++) {
            sql.append(", m").append(i).append(".v v").append(i);
        }
        sql.append(" FROM mart1 m1");
        for (int i = 2; i <= marts; i++) {
            sql.append(" JOIN mart").append(i).append(" m").append(i).append(" ON m1.ts = m").append(i).append(".ts");
        }
        return sql.toString();
    }
}
