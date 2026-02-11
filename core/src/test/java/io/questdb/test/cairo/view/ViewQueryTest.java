/*******************************************************************************
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

import org.junit.Test;

public class ViewQueryTest extends AbstractViewTest {

    @Test
    public void testCreateConstantView() throws Exception {
        assertMemoryLeak(() -> {
            final String query1 = "select 42 as col";
            createView(VIEW1, query1);

            assertQueryNoLeakCheck(
                    """
                            col
                            42
                            """,
                    VIEW1
            );
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
            assertQueryNoLeakCheck("""
                            trade_ts\tsymbol\tprice\tside\tquote_ts\tbid\task
                            2024-01-01T00:00:02.000000Z\tAAPL\t149.8\tBUY\t2024-01-01T00:00:00.000000Z\t149.0\t150.0
                            2024-01-01T00:00:12.000000Z\tAAPL\t150.8\tBUY\t2024-01-01T00:00:10.000000Z\t150.0\t151.0
                            """,
                    "SELECT t.ts as trade_ts, t.symbol, t.price, t.side, q.ts as quote_ts, q.bid, q.ask " +
                            "FROM " + VIEW2 + " t ASOF JOIN " + VIEW1 + " q ON (symbol)",
                    "trade_ts", false, false, true);

            // Override to get SELL trades instead
            assertQueryNoLeakCheck("""
                            trade_ts\tsymbol\tprice\tside\tquote_ts\tbid\task
                            2024-01-01T00:00:08.000000Z\tAAPL\t150.2\tSELL\t2024-01-01T00:00:05.000000Z\t149.5\t150.5
                            """,
                    "DECLARE @side := 'SELL' " +
                            "SELECT t.ts as trade_ts, t.symbol, t.price, t.side, q.ts as quote_ts, q.bid, q.ask " +
                            "FROM " + VIEW2 + " t ASOF JOIN " + VIEW1 + " q ON (symbol)",
                    "trade_ts", false, false, true);
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
            assertQueryNoLeakCheck("""
                            ts\tscaled_v
                            1970-01-01T00:01:10.000000Z\t70
                            1970-01-01T00:01:20.000000Z\t80
                            """,
                    query, "ts", true, sqlExecutionContext);
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

            assertQueryNoLeakCheck("""
                            result
                            12
                            """,
                    query);

            // Test that outer scope sees its own @x
            String query2 = """
                    DECLARE @x := 2, @y := 100
                    SELECT @x + @y as outer_result, inner_result FROM (
                        DECLARE @x := 5
                        SELECT @x + @y as inner_result FROM long_sequence(1)
                    )
                    """;

            assertQueryNoLeakCheck("""
                            outer_result\tinner_result
                            102\t105
                            """,
                    query2);
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
            assertQueryNoLeakCheck("""
                            ts\tkey\tv
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            1970-01-01T00:00:00.000000Z\tk2_0\t0
                            1970-01-01T00:00:10.000000Z\tk2_1\t1
                            1970-01-01T00:00:20.000000Z\tk2_2\t2
                            1970-01-01T00:00:30.000000Z\tk2_3\t3
                            1970-01-01T00:00:40.000000Z\tk2_4\t4
                            """,
                    VIEW1, null, false, sqlExecutionContext);

            // Override threshold to 3: v > 3 from TABLE1 (rows 4,5,6,7,8) UNION v < 3 from TABLE2 (rows 0,1,2)
            assertQueryNoLeakCheck("""
                            ts\tkey\tv
                            1970-01-01T00:00:40.000000Z\tk4\t4
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            1970-01-01T00:00:00.000000Z\tk2_0\t0
                            1970-01-01T00:00:10.000000Z\tk2_1\t1
                            1970-01-01T00:00:20.000000Z\tk2_2\t2
                            """,
                    "DECLARE @threshold := 3 SELECT * FROM " + VIEW1, null, false, sqlExecutionContext);
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
            assertQueryNoLeakCheck("""
                            cnt
                            6
                            """,
                    "SELECT count() as cnt FROM " + VIEW1, null, false, true);

            // Query VIEW2 with default @max_v=6: v <= 6 -> rows 0,1,2,3,4,5,6 (7 rows)
            assertQueryNoLeakCheck("""
                            cnt
                            7
                            """,
                    "SELECT count() as cnt FROM " + VIEW2, null, false, true);

            // Override VIEW1's @min_v: v >= 5 -> rows 5,6,7,8 (4 rows)
            assertQueryNoLeakCheck("""
                            cnt
                            4
                            """,
                    "DECLARE @min_v := 5 SELECT count() as cnt FROM " + VIEW1, null, false, true);

            // Override VIEW2's @max_v: v <= 4 -> rows 0,1,2,3,4 (5 rows)
            assertQueryNoLeakCheck("""
                            cnt
                            5
                            """,
                    "DECLARE @max_v := 4 SELECT count() as cnt FROM " + VIEW2, null, false, true);

            // Cross join both views with overrides in same query
            // VIEW1: v >= 5 (4 rows), VIEW2: v <= 4 (5 rows) -> 4 * 5 = 20 rows
            assertQueryNoLeakCheck("""
                            cnt
                            20
                            """,
                    "DECLARE @min_v := 5, @max_v := 4 SELECT count() as cnt FROM " + VIEW1 + " CROSS JOIN " + VIEW2, null, false, true);
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
            assertQueryNoLeakCheck("""
                            ts\tsymbol\tprice\tqty
                            2024-01-01T00:03:00.000000Z\tMSFT\t380.0\t75
                            2024-01-01T00:04:00.000000Z\tGOOG\t141.0\t60
                            2024-01-01T00:05:00.000000Z\tAAPL\t152.0\t150
                            """,
                    VIEW1, "ts", true, true, true);

            // Override: qty >= 100, excludes GOOG and MSFT entirely
            // AAPL: latest with qty >= 100 is 152.0 (qty=150) - LATEST BY returns only ONE row per symbol
            assertQueryNoLeakCheck("""
                            ts\tsymbol\tprice\tqty
                            2024-01-01T00:05:00.000000Z\tAAPL\t152.0\t150
                            """,
                    "DECLARE @min_qty := 100 SELECT * FROM " + VIEW1, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:00:40.000000Z\t4
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            """,
                    VIEW2, "ts", true, sqlExecutionContext);

            // Override @max at query level: v > 3 AND v < 6 -> rows 4, 5
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:00:40.000000Z\t4
                            1970-01-01T00:00:50.000000Z\t5
                            """,
                    "DECLARE @max := 6 SELECT * FROM " + VIEW2, "ts", true, sqlExecutionContext);

            // Override @threshold at query level: v > 5 AND v < 7 -> row 6
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            """,
                    "DECLARE @threshold := 5 SELECT * FROM " + VIEW2, "ts", true, sqlExecutionContext);

            // Override both at query level: v > 4 AND v < 8 -> rows 5, 6, 7
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            """,
                    "DECLARE @threshold := 4, @max := 8 SELECT * FROM " + VIEW2, "ts", true, sqlExecutionContext);
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
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:00:50.000000Z\t5
                            """,
                    VIEW2, "ts", true, sqlExecutionContext);

            // Override @x through VIEW2 - should propagate to VIEW1
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            """,
                    "DECLARE @x := 6 SELECT * FROM " + VIEW2, "ts", true, sqlExecutionContext);
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
            assertQueryNoLeakCheck("""
                            ts\tsensor\tavg_val
                            2024-01-01T00:00:00.000000Z\tA\t20.0
                            2024-01-01T00:01:00.000000Z\tA\t35.0
                            2024-01-01T00:02:00.000000Z\tA\t55.0
                            """,
                    VIEW1, "ts", true, true, true);

            // Override: value > 35, excludes first 3 rows
            // Minute 1: avg(40) = 40, Minute 2: avg(50,60) = 55
            assertQueryNoLeakCheck("""
                            ts\tsensor\tavg_val
                            2024-01-01T00:01:00.000000Z\tA\t40.0
                            2024-01-01T00:02:00.000000Z\tA\t55.0
                            """,
                    "DECLARE @min_value := 35.0 SELECT * FROM " + VIEW1, "ts", true, true);
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
            assertQueryNoLeakCheck("""
                            ts\tv\tmarker
                            1970-01-01T00:01:00.000000Z\t6\t100
                            1970-01-01T00:01:10.000000Z\t7\t100
                            1970-01-01T00:01:20.000000Z\t8\t100
                            """,
                    query, "ts", true, sqlExecutionContext);
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

            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    VIEW1, "ts", true, sqlExecutionContext);

            // Override with different string value
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    "DECLARE @limit := '6' SELECT * FROM " + VIEW1, "ts", true, sqlExecutionContext);

            // Test 2: Symbol/string parameter for filtering
            final String query2 = "DECLARE OVERRIDABLE @key_filter := 'k5' " +
                    "SELECT ts, k, v FROM " + TABLE1 + " WHERE k = @key_filter";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ")");
            drainWalAndViewQueues();

            assertQueryNoLeakCheck("""
                            ts\tk\tv
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            """,
                    VIEW2, "ts", true, sqlExecutionContext);

            // Override to different key
            assertQueryNoLeakCheck("""
                            ts\tk\tv
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            """,
                    "DECLARE @key_filter := 'k7' SELECT * FROM " + VIEW2, "ts", true, sqlExecutionContext);
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
            assertQueryNoLeakCheck("""
                            ts\tv\tx_val\tx_plus_x\tx_squared\tx_cubed\tv_plus_x\tcomplex_expr
                            1970-01-01T00:00:00.000000Z\t0\t2\t4\t4\t8\t2\t2
                            1970-01-01T00:00:10.000000Z\t1\t2\t4\t4\t8\t3\t4
                            1970-01-01T00:00:20.000000Z\t2\t2\t4\t4\t8\t4\t6
                            1970-01-01T00:00:30.000000Z\t3\t2\t4\t4\t8\t5\t8
                            1970-01-01T00:00:40.000000Z\t4\t2\t4\t4\t8\t6\t10
                            """,
                    VIEW1, "ts", true, sqlExecutionContext);

            // Override @x = 3: filter v <= 6, expressions use 3
            assertQueryNoLeakCheck("""
                            ts\tv\tx_val\tx_plus_x\tx_squared\tx_cubed\tv_plus_x\tcomplex_expr
                            1970-01-01T00:00:00.000000Z\t0\t3\t6\t9\t27\t3\t3
                            1970-01-01T00:00:10.000000Z\t1\t3\t6\t9\t27\t4\t6
                            1970-01-01T00:00:20.000000Z\t2\t3\t6\t9\t27\t5\t9
                            1970-01-01T00:00:30.000000Z\t3\t3\t6\t9\t27\t6\t12
                            1970-01-01T00:00:40.000000Z\t4\t3\t6\t9\t27\t7\t15
                            1970-01-01T00:00:50.000000Z\t5\t3\t6\t9\t27\t8\t18
                            1970-01-01T00:01:00.000000Z\t6\t3\t6\t9\t27\t9\t21
                            """,
                    "DECLARE @x := 3 SELECT * FROM " + VIEW1, "ts", true, sqlExecutionContext);
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
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:01:00.000000Z\t6
                            """,
                    VIEW1, "ts", true, sqlExecutionContext);

            assertExceptionNoLeakCheck("DECLARE @x := 5 SELECT * FROM " + VIEW1, 11, "variable is not overridable: @x");
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
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:00:20.000000Z\t2
                            1970-01-01T00:00:40.000000Z\t4
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            """,
                    VIEW2, "ts", true, sqlExecutionContext);

            // Can override @low (OVERRIDABLE in VIEW1) and @extra_filter (OVERRIDABLE in VIEW2)
            // @low=4, @extra_filter=5 -> v >= 4 AND v <= 7 AND v != 5 -> 4, 6, 7
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:00:40.000000Z\t4
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            """,
                    "DECLARE @low := 4, @extra_filter := 5 SELECT * FROM " + VIEW2, "ts", true, sqlExecutionContext);

            // Cannot override @high (not OVERRIDABLE in VIEW1)
            assertExceptionNoLeakCheck(
                    "DECLARE @high := 8 SELECT * FROM " + VIEW2,
                    14,
                    "variable is not overridable: @high"
            );
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
            assertQueryNoLeakCheck("""
                            ts	v
                            1970-01-01T00:00:50.000000Z	5
                            1970-01-01T00:01:00.000000Z	6
                            1970-01-01T00:01:10.000000Z	7
                            1970-01-01T00:01:20.000000Z	8
                            """,
                    "VIEW1", "ts", true, sqlExecutionContext);

            // can override @hi (marked as OVERRIDABLE)
            assertQueryNoLeakCheck("""
                            ts	v
                            1970-01-01T00:00:50.000000Z	5
                            1970-01-01T00:01:00.000000Z	6
                            1970-01-01T00:01:10.000000Z	7
                            """,
                    "DECLARE @hi := 7 SELECT * FROM " + VIEW1, "ts", true, sqlExecutionContext);

            // override @lo (not overridable) should fail
            assertExceptionNoLeakCheck("DECLARE @lo := 3 SELECT * FROM " + VIEW1, 12, "variable is not overridable: @lo");
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
            assertQueryNoLeakCheck("""
                            ts\tv
                            1970-01-01T00:00:50.000000Z\t5
                            1970-01-01T00:01:00.000000Z\t6
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
                    VIEW1, "ts", true, sqlExecutionContext);

            assertExceptionNoLeakCheck("DECLARE @x := 3 SELECT * FROM " + VIEW1, 11, "variable is not overridable: @x");
            assertExceptionNoLeakCheck("DECLARE @y := 10 SELECT * FROM " + VIEW1, 11, "variable is not overridable: @y");
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
            assertExceptionNoLeakCheck(
                    "DECLARE @x := 6 SELECT * FROM " + VIEW1,
                    11,
                    "variable is not overridable: @x"
            );
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
            assertQueryNoLeakCheck("""
                            ts\tv\tmarker
                            1970-01-01T00:00:50.000000Z\t5\t999
                            """,
                    VIEW2, "ts", true, sqlExecutionContext);
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
            assertQueryNoLeakCheck("""
                            ts\tv\tmarker
                            1970-01-01T00:01:00.000000Z\t6\t6
                            """,
                    VIEW2, "ts", true, sqlExecutionContext);
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
            assertQueryNoLeakCheck("""
                            ts\tcategory\tvalue
                            2024-01-01T00:00:00.000000Z\tA\t10
                            2024-01-01T00:01:00.000000Z\tB\t0
                            2024-01-01T00:02:00.000000Z\tA\t20
                            2024-01-01T00:03:00.000000Z\t\t30
                            2024-01-01T00:04:00.000000Z\tB\t40
                            """,
                    VIEW1, "ts", true, true, true);

            // Override: NULL values replaced with -1
            assertQueryNoLeakCheck("""
                            ts\tcategory\tvalue
                            2024-01-01T00:00:00.000000Z\tA\t10
                            2024-01-01T00:01:00.000000Z\tB\t-1
                            2024-01-01T00:02:00.000000Z\tA\t20
                            2024-01-01T00:03:00.000000Z\t\t30
                            2024-01-01T00:04:00.000000Z\tB\t40
                            """,
                    "DECLARE @default_val := -1 SELECT * FROM " + VIEW1, "ts", true, true);
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
            assertQueryNoLeakCheck(
                    """
                            ts\tticker\tprice
                            2024-01-01T00:00:00.000000Z\tAAPL\t150.0
                            \tGOOG\t140.0
                            2024-01-01T00:02:00.000000Z\tMSFT\t151.0
                            """,
                    "SELECT view1.ts, x.ticker, x.price FROM x LEFT JOIN view1 ON ticker",
                    null, false, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            col
                            42
                            """,
                    "SELECT * FROM '" + VIEW1 + "'"
            );

            assertQueryNoLeakCheck(
                    """
                            col
                            42
                            """,
                    "SELECT * FROM \"" + VIEW1 + "\""
            );
        });
    }

    @Test
    public void testQueryViewInQuotesJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + TABLE1 + " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + TABLE1 + " VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
            drainWalQueue();
            createView(VIEW1, "SELECT * FROM " + TABLE1);

            assertQueryNoLeakCheck(
                    """
                            ts\tv\tts1\tv1
                            2024-01-01T00:00:00.000000Z\t1\t2024-01-01T00:00:00.000000Z\t1
                            2024-01-02T00:00:00.000000Z\t2\t2024-01-02T00:00:00.000000Z\t2
                            """,
                    "SELECT * FROM " + TABLE1 + " JOIN '" + VIEW1 + "' ON (v)",
                    "ts", false, false
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tv\tts1\tv1
                            2024-01-01T00:00:00.000000Z\t1\t2024-01-01T00:00:00.000000Z\t1
                            2024-01-02T00:00:00.000000Z\t2\t2024-01-02T00:00:00.000000Z\t2
                            """,
                    "SELECT * FROM " + TABLE1 + " JOIN \"" + VIEW1 + "\" ON (v)",
                    "ts", false, false
            );
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

            assertQueryAndCache(
                    """
                            timestamp\tcount
                            1970-01-01T00:00:00.000000Z\t1
                            """,
                    """
                            select timestamp, count() from view2
                            sample by 10m
                            """,
                    "timestamp",
                    false,
                    false
            );
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

            assertQueryNoLeakCheck("""
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
                            """,
                    VIEW1, "ts", true, sqlExecutionContext);
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
}
