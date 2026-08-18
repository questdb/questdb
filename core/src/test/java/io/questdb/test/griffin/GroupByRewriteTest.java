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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class GroupByRewriteTest extends AbstractCairoTest {

    @Test
    public void testFilterAliasIsNotAClause() throws Exception {
        // 'filter' is not reserved, so it stays usable as a column alias when '(' does not follow it
        assertAggQuery("""
                        filter
                        55
                        """,
                "select sum(x) filter from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterArgMaxDoesNotLetNonMatchingKeyWin() throws Exception {
        // arg_max null-checks only its ordering key, so the key must be wrapped too. Without that,
        // the row holding the global maximum key (k=10) would win even though it fails the condition.
        assertAggQuery("""
                        r
                        5.0
                        """,
                "select arg_max(v, k) filter (where k <= 5) r from t",
                "create table t as ( select x::double v, x::double k from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterAvgMinMaxMatchFilteredSubQuery() throws Exception {
        assertAggQuery("""
                        a\tmi\tma
                        8.0\t6\t10
                        """,
                "select avg(x) a, min(x) mi, max(x) ma from (select x from y where x > 5)",
                "create table y as ( select x from long_sequence(10) )"
        );
        assertAggQuery("""
                        a\tmi\tma
                        8.0\t6\t10
                        """,
                "select avg(x) filter (where x > 5) a, min(x) filter (where x > 5) mi, max(x) filter (where x > 5) ma from y",
                null
        );
    }

    @Test
    public void testFilterBindVariableAsAggregateParameter() throws Exception {
        // a bind variable in a parameter position must pass through the CASE wrapper untouched,
        // exactly like a literal constant would
        assertMemoryLeak(() -> {
            execute("create table t2 as ( select x from long_sequence(10) )");
            bindVariableService.clear();
            bindVariableService.setDouble(0, 0.5);
            assertQuery("select approx_percentile(x::double, $1) filter (where x > 5) r from t2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            r
                            8.25
                            """);
            assertQuery("select approx_percentile(x::double, $1, 5) filter (where x > 5) r from t2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            r
                            8.000030517578125
                            """);
        });
    }

    @Test
    public void testFilterBindVariableCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y as ( select x from long_sequence(10) )");
            bindVariableService.clear();
            bindVariableService.setLong(0, 5);
            assertQuery("select sum(x) filter (where x > $1) r from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            r
                            40
                            """);
        });
    }

    @Test
    public void testFilterCoalesceRestoresZero() throws Exception {
        assertAggQuery("""
                        r
                        0
                        """,
                "select coalesce(sum(x) filter (where x > 100), 0) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterComplexCondition() throws Exception {
        assertAggQuery("""
                        r
                        4
                        """,
                "select count(*) filter (where x > 2 and x < 8 and x != 5) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
        assertAggQuery("""
                        r
                        5
                        """,
                "select count(*) filter (where not (x > 5)) r from y",
                null
        );
        assertAggQuery("""
                        r
                        3
                        """,
                "select count(*) filter (where x between 3 and 5) r from y",
                null
        );
        assertAggQuery("""
                        r
                        3
                        """,
                "select count(*) filter (where x in (1, 2, 3)) r from y",
                null
        );
    }

    @Test
    public void testFilterConstantParameterPassesThrough() throws Exception {
        // string_agg's delimiter is a configuration parameter, not a per-row value, so it must not
        // be wrapped in the CASE - otherwise non-matching rows would null the delimiter itself
        assertAggQuery("""
                        r
                        a4,a5
                        """,
                "select string_agg(s, ',') filter (where x > 3) r from t",
                "create table t as ( select 'a' || x s, x from long_sequence(5) )"
        );
        // approx_percentile takes a non-constant-typed percentile argument that is still a parameter
        assertAggQuery("""
                        r
                        8.25
                        """,
                "select approx_percentile(x::double, 0.5) filter (where x > 5) r from t2",
                "create table t2 as ( select x from long_sequence(10) )"
        );
        assertAggQuery("""
                        r
                        8.25
                        """,
                "select approx_percentile(x::double, 0.5) r from (select x from t2 where x > 5)",
                null
        );
        // three arguments exercises the args-list path, where only argument 0 is wrapped and the
        // trailing percentile and precision constants pass through. An explicit precision changes
        // the approximation, so the value differs from the two-argument form above - what matters
        // is that the filtered and pre-filtered forms agree exactly
        assertAggQuery("""
                        r
                        8.000030517578125
                        """,
                "select approx_percentile(x::double, 0.5, 5) filter (where x > 5) r from t2",
                null
        );
        assertAggQuery("""
                        r
                        8.000030517578125
                        """,
                "select approx_percentile(x::double, 0.5, 5) r from (select x from t2 where x > 5)",
                null
        );
    }

    @Test
    public void testFilterCorrMatchesFilteredSubQuery() throws Exception {
        assertAggQuery("""
                        r
                        1.0
                        """,
                "select corr(a, b) filter (where id > 5) r from t",
                "create table t as ( select x::double a, (x * 2)::double b, x id from long_sequence(10) )"
        );
        assertAggQuery("""
                        r
                        1.0
                        """,
                "select corr(a, b) r from (select a, b from t where id > 5)",
                null
        );
    }

    @Test
    public void testFilterCountDistinctMatchesFilteredSubQuery() throws Exception {
        assertAggQuery("""
                        r
                        3
                        """,
                "select count_distinct(g) filter (where x > 5) r from t",
                "create table t as ( select x % 3 g, x from long_sequence(10) )"
        );
        assertAggQuery("""
                        r
                        3
                        """,
                "select count_distinct(g) r from (select g from t where x > 5)",
                null
        );
    }

    @Test
    public void testFilterCountOfColumnSkipsNulls() throws Exception {
        // count(column) counts non-null values among matching rows, unlike count(*)
        assertAggQuery("""
                        c\tcs
                        4\t5
                        """,
                "select count(v) filter (where id > 5) c, count(*) filter (where id > 5) cs from t",
                "create table t as ( select case when x % 4 = 0 then null else x end v, x id from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterCountStarMatchesFilteredSubQuery() throws Exception {
        assertAggQuery("""
                        r
                        5
                        """,
                "select count(*) filter (where x > 5) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
        assertAggQuery("""
                        r
                        5
                        """,
                "select count(*) r from (select x from y where x > 5)",
                null
        );
    }

    @Test
    public void testFilterDecimalAndLong256Arguments() throws Exception {
        // CASE routes DECIMAL through getDecimalCommonType and LONG256 through its own case
        // function, so neither is implied by the INT/LONG/FLOAT/DOUBLE coverage
        assertAggQuery("""
                        dec\tl256
                        40.00\t0x28
                        """,
                "select sum(x::decimal(20,2)) filter (where x > 5) dec, sum(x::long256) filter (where x > 5) l256 from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterDeclareVariableCondition() throws Exception {
        assertAggQuery("""
                        r
                        40
                        """,
                "declare @lim := 5 select sum(x) filter (where x > @lim) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterDistinctConditionsAreNotDeduplicated() throws Exception {
        // two aggregates that differ only in their condition must stay distinct
        assertAggQuery("""
                        a\tb
                        40\t15
                        """,
                "select sum(x) filter (where x > 5) a, sum(x) filter (where x <= 5) b from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterEmptyTable() throws Exception {
        assertAggQuery("""
                        r
                        null
                        """,
                "select sum(x) filter (where x > 5) r from e",
                "create table e (x long, ts timestamp) timestamp(ts) partition by day"
        );
    }

    @Test
    public void testFilterInCte() throws Exception {
        assertAggQuery("""
                        r
                        40
                        """,
                "with c as (select sum(x) filter (where x > 5) r from y) select * from c",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterInJoinedQuery() throws Exception {
        // exercises the join-model arm of the lowering pass's recursion
        assertMemoryLeak(() -> {
            execute("create table a as ( select x id, ('g' || (x % 2))::symbol g from long_sequence(10) )");
            execute("create table b as ( select x id, x v from long_sequence(10) )");
            assertQuery("select a.g, sum(b.v) filter (where b.v > 5) s from a join b on (id) order by a.g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\ts
                            g0\t24
                            g1\t16
                            """);
        });
    }

    @Test
    public void testFilterInOrderBy() throws Exception {
        // exercises the ORDER BY arm of the lowering pass: the aggregate appears in the order-by
        // list as an expression rather than as an alias reference
        assertMemoryLeak(() -> {
            execute("create table p as ( select x from long_sequence(10) )");
            assertQuery("select g, sum(x) filter (where x > 5) s from (select x, (x % 2)::symbol g from p)"
                    + " order by sum(x) filter (where x > 5)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\ts
                            1\t16
                            0\t24
                            """);
        });
    }

    @Test
    public void testFilterInSubQuery() throws Exception {
        assertAggQuery("""
                        r
                        40
                        """,
                "select * from (select sum(x) filter (where x > 5) r from y)",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterInUnionBranches() throws Exception {
        // exercises the union-model arm of the lowering pass's recursion
        assertMemoryLeak(() -> {
            execute("create table y as ( select x from long_sequence(10) )");
            assertQuery("select sum(x) filter (where x > 5) r from y"
                    + " union all select sum(x) filter (where x <= 5) r from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            r
                            40
                            15
                            """);
        });
    }

    @Test
    public void testFilterKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as ( select x, (case when x % 2 = 0 then 'even' else 'odd' end)::symbol g from long_sequence(10) )");
            assertQuery("select g, count(*) filter (where x > 5) c from t order by g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tc
                            even\t3
                            odd\t2
                            """);
        });
    }

    @Test
    public void testFilterNumericTypes() throws Exception {
        assertAggQuery("""
                        si\tsl\tss\tsf\tsd
                        40\t40\t40\t40.0\t40.0
                        """,
                "select sum(i) filter (where l > 5) si, sum(l) filter (where l > 5) sl, sum(s) filter (where l > 5) ss," +
                        " sum(f) filter (where l > 5) sf, sum(d) filter (where l > 5) sd from t",
                "create table t as ( select x::int i, x::long l, x::short s, x::float f, x::double d from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into t select x, timestamp_sequence(0, 1000000) from long_sequence(10)");
            assertQuery("select sum(x) filter (where x > 5) r from t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            r
                            40
                            """);
        });
    }

    @Test
    public void testFilterOnEmptyMatchReturnsNull() throws Exception {
        assertAggQuery("""
                        r
                        null
                        """,
                "select sum(x) filter (where x > 100) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterParserErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y as ( select x from long_sequence(10) )");
            assertException("select sum(x) filter (x > 5) from y", 22, "'where' expected");
            assertException("select sum(x) filter (where x > 5 from y", 34, "')' expected");
            assertException("select sum(x) filter (", 21, "'where' expected");
            assertException("select sum(x) filter (where", 22, "filter condition expected");
            assertException("select sum(x) filter (where)", 27, "filter condition expected");
        });
    }

    @Test
    public void testFilterParallelGroupByMatchesSerial() throws Exception {
        // the lowered aggregate still goes through the parallel path, and must agree with the
        // single-threaded result for the same data
        assertMemoryLeak(() -> {
            execute("create table t as ( select x, (x % 2)::symbol g from long_sequence(100_000) )");
            final String query = "select g, sum(x) filter (where x <= 50_000) s, count(*) filter (where x > 50_000) c from t order by g";
            // even x in 2..50000 sum to 25000 * 25001; odd x in 1..49999 sum to 25000 squared;
            // each parity contributes 25000 rows above 50000
            final String expected = """
                    g\ts\tc
                    0\t625025000\t25000
                    1\t625000000\t25000
                    """;
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
            assertQuery(query).noLeakCheck().expectSize().returns(expected);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "false");
            assertQuery(query).noLeakCheck().expectSize().returns(expected);
        });
    }

    @Test
    public void testFilterPlanRetainsAsyncGroupBy() throws Exception {
        // the lowering must not knock a keyed aggregate off the parallel path
        assertMemoryLeak(() -> {
            execute("create table s2 as ( select x, (x % 2)::symbol g, timestamp_sequence(0, 1000) ts"
                    + " from long_sequence(1000) ) timestamp(ts) partition by day");
            assertQuery("select g, count(*) filter (where x > 500) c from s2")
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By");
        });
    }

    @Test
    public void testFilterPlanShowsLoweredCase() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y as ( select x from long_sequence(10) )");
            assertQuery("select count(*) filter (where x > 5) c from y")
                    .noLeakCheck()
                    .assertsPlanContaining("case([5<x,1,null])");
        });
    }

    @Test
    public void testFilterRejectedForAggregateInCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y as ( select x from long_sequence(10) )");
            assertException(
                    "select sum(x) filter (where sum(x) > 5) from y",
                    28,
                    "aggregate functions are not allowed in FILTER"
            );
        });
    }

    @Test
    public void testFilterRejectedForAggregateNestedInConditionFunction() throws Exception {
        // the condition is validated recursively, including through a function's args list
        assertMemoryLeak(() -> {
            execute("create table y as ( select x from long_sequence(10) )");
            assertException(
                    "select sum(x) filter (where coalesce(sum(x), 0, 0) > 1) r from y",
                    37,
                    "aggregate functions are not allowed in FILTER"
            );
        });
    }

    @Test
    public void testFilterRejectedForNonAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y as ( select x from long_sequence(10) )");
            assertException(
                    "select abs(x) filter (where x > 5) from y",
                    7,
                    "FILTER is supported only for aggregate functions"
            );
        });
    }

    @Test
    public void testFilterRejectedForNullPreservingAggregates() throws Exception {
        // these aggregates treat a NULL input as a value rather than skipping the row, so the
        // CASE lowering would change their result instead of dropping non-matching rows
        assertMemoryLeak(() -> {
            execute("create table y as ( select x, x % 2 = 0 b, x::double d from long_sequence(10) )");
            assertException("select first(x) filter (where x > 5) from y", 7, "FILTER is not supported for 'first'");
            assertException("select last(x) filter (where x > 5) from y", 7, "FILTER is not supported for 'last'");
            assertException("select array_agg(d) filter (where x > 5) from y", 7, "FILTER is not supported for 'array_agg'");
            assertException("select bool_and(b) filter (where x > 5) from y", 7, "FILTER is not supported for 'bool_and'");
            assertException("select bool_or(b) filter (where x > 5) from y", 7, "FILTER is not supported for 'bool_or'");
            assertException("select mode(b) filter (where x > 5) from y", 7, "FILTER is not supported for 'mode'");
        });
    }

    @Test
    public void testFilterRejectedForWindowFunctionInCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y as ( select x from long_sequence(10) )");
            assertException(
                    "select sum(x) filter (where row_number() over () > 1) from y",
                    28,
                    "window functions are not allowed in FILTER"
            );
        });
    }

    @Test
    public void testFilterSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as ( select x, timestamp_sequence(0, 1000000) ts from long_sequence(10) ) timestamp(ts) partition by day");
            assertQuery("select ts, count(*) filter (where x > 5) c from t sample by 5s")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tc
                            1970-01-01T00:00:00.000000Z\t0
                            1970-01-01T00:00:05.000000Z\t5
                            """);
        });
    }

    @Test
    public void testFilterSampleByFillModes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table g as ( select x, x::double d, timestamp_sequence(0, 2000000) ts"
                    + " from long_sequence(10) ) timestamp(ts) partition by day");
            final String counts = """
                    ts\tc
                    1970-01-01T00:00:00.000000Z\t0
                    1970-01-01T00:00:05.000000Z\t0
                    1970-01-01T00:00:10.000000Z\t0
                    1970-01-01T00:00:15.000000Z\t2
                    """;
            assertQuery("select ts, count(*) filter (where x > 8) c from g sample by 5s fill(none)")
                    .noLeakCheck().timestamp("ts").expectSize().returns(counts);
            assertQuery("select ts, count(*) filter (where x > 8) c from g sample by 5s fill(null)")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary().returns(counts);
            assertQuery("select ts, sum(d) filter (where x > 8) s from g sample by 5s fill(prev)")
                    .noLeakCheck().timestamp("ts").noRandomAccess().sizeMayVary()
                    .returns("""
                            ts\ts
                            1970-01-01T00:00:00.000000Z\tnull
                            1970-01-01T00:00:05.000000Z\tnull
                            1970-01-01T00:00:10.000000Z\tnull
                            1970-01-01T00:00:15.000000Z\t19.0
                            """);
        });
    }

    @Test
    public void testFilterSumMatchesFilteredSubQuery() throws Exception {
        assertAggQuery("""
                        r
                        40
                        """,
                "select sum(x) filter (where x > 5) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
        assertAggQuery("""
                        r
                        40
                        """,
                "select sum(x) r from (select x from y where x > 5)",
                null
        );
    }

    @Test
    public void testFilterSumOfConstantCountsMatchingRows() throws Exception {
        // argument 0 is wrapped even when it is a constant, so this counts matching rows rather
        // than degrading into sum(1) over every row
        assertAggQuery("""
                        r
                        5
                        """,
                "select sum(1) filter (where x > 5) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testFilterWeightedAvgMatchesFilteredSubQuery() throws Exception {
        assertAggQuery("""
                        r
                        8.25
                        """,
                "select weighted_avg(v, w) filter (where id > 5) r from t",
                "create table t as ( select x::double v, x::double w, x id from long_sequence(10) )"
        );
        assertAggQuery("""
                        r
                        8.25
                        """,
                "select weighted_avg(v, w) r from (select v, w from t where id > 5)",
                null
        );
    }

    @Test
    public void testRewriteAggregateDoesNotCreateDuplicateKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym symbol, price double, amount double, ts timestamp) timestamp(ts) partition by day;");
            execute("CREATE TABLE trades2 (sym symbol, price double, amount double, ts timestamp) timestamp(ts) partition by day;");

            // key first
            assertQuery("SELECT ts, price, price / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price/sum]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key first, aliased
            assertQuery("SELECT ts, PricE as price0, price / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price0,price0/sum]
                                Async Group By workers: 1
                                  keys: [ts,price0]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key first, multiple column occurrences
            assertQuery("SELECT ts, price, (price + price) / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price+price/sum]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key first, multiple keys, multiple column occurrences
            assertQuery("SELECT ts, price, price as price0, (price + price) / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price,price+price/sum]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key first, aliased, multiple column occurrences
            assertQuery("SELECT ts, price as price0, (price + price) / sum(amount) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price0,price0+price0/sum]
                                Async Group By workers: 1
                                  keys: [ts,price0]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            // key second
            assertQuery("SELECT ts, price / sum(amount), price FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price/sum,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, aliased
            assertQuery("SELECT ts, price / sum(amount), PricE as price0 FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price/sum,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, aliased, multiple columns
            assertQuery("SELECT ts, sym price, price / sum(amount), price price1 FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price1/sum,price1]
                                Async Group By workers: 1
                                  keys: [ts,price,price1]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, multiple column occurrences
            assertQuery("SELECT ts, (price + price) / sum(amount), price FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price+price/sum,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, multiple keys, multiple column occurrences
            assertQuery("SELECT ts, (price + price) / sum(amount), price, price as price0 FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price+price/sum,price,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
            // key second, aliased, multiple column occurrences
            assertQuery("SELECT ts, (price + price) / sum(amount), price as price0 FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price+price/sum,price]
                                Async Group By workers: 1
                                  keys: [ts,price]
                                  values: [sum(amount)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);

            // joined tables with same column names - the rewrite should not deduplicate the keys
            assertQuery("SELECT t1.ts, t1.price, t2.price / sum(t1.amount) FROM trades t1 JOIN trades2 t2 ON (sym);")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,price,price1/sum]
                                GroupBy vectorized: false
                                  keys: [ts,price,price1]
                                  values: [sum(amount)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: t2.sym=t1.sym
                                          symbolKeyJoin: true
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: trades
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: trades2
                            """);
        });
    }

    @Test
    public void testRewriteAggregateExtractsConstantKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (price double, amount double, ts timestamp) timestamp(ts) partition by day;");
            assertQuery("SELECT 42, 'foobar', amount, sum(price) FROM trades;")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [42,'foobar',amount,sum]
                                Async Group By workers: 1
                                  keys: [amount]
                                  values: [sum(price)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: trades
                            """);
        });
    }

    @Test
    public void testRewriteAggregateOnJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( ax int, aid int );");
            execute("INSERT INTO taba values (1,1), (2,2)");
            execute("CREATE TABLE tabb ( bx int, bid int );");
            execute("INSERT INTO tabb values (3,1), (4,2)");

            assertQuery("SELECT sum(ax), sum(bx), sum(ax+10), sum(bx+10) " +
                    "FROM taba " +
                    "join tabb on aid = bid")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            sum\tsum1\tsum2\tsum3
                            3\t7\t23\t27
                            """);
        });
    }

    @Test
    public void testRewriteAggregateOnJoin3() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, aid int );");
            execute("CREATE TABLE tabb ( x int, bid int );");
        });

        assertQuery("SELECT sum(tabc.x*1),sum(x), sum(ax+10), sum(bx+10) " +
                "FROM taba " +
                "join tabb on aid = bid")
                .fails(11, "Invalid table name or alias");
    }

    @Test
    public void testRewriteAggregateOnJoin4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, aid int );");
            execute("CREATE TABLE tabb ( x int, bid int );");
            assertQuery("SELECT sum(taba.k*1),sum(x), sum(ax+10), sum(bx+10) " +
                    "FROM taba " +
                    "join tabb on aid = bid")
                    .fails(11, "Invalid column: taba.k");
        });
    }

    @Test
    public void testRewriteAggregateOnJoinFailsOnAmbiguousColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("  CREATE TABLE taba ( x int, aid int );");
            execute("  CREATE TABLE tabb ( x int, bid int );");
            assertQuery("SELECT sum(x*1),sum(x), sum(ax+10), sum(bx+10) " +
                    "FROM taba " +
                    "join tabb on aid = bid")
                    .fails(11, "Ambiguous column [name=x]");
        });
    }

    @Test
    public void testRewriteAggregateOnOrderBySumBadQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE telemetry (created timestamp)");
            assertQuery("SELECT telemetry.created FROM telemetry ORDER BY SUM(1, 1 IN (telemetry.created), 1);")
                    .noLeakCheck()
                    .fails(49, "there is no matching function `SUM` with the argument types: (INT, BOOLEAN, INT)");
        });
    }

    @Test
    public void testSumOfAddition1() throws Exception {
        assertAggQuery("""
                        r
                        65
                        """,
                "select sum(x+1) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfAddition2() throws Exception {
        assertAggQuery("""
                        r
                        65
                        """,
                "select sum(1+x) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfAdditionOfDouble1() throws Exception {
        assertAggQuery(
                """
                        r
                        66.0
                        """,
                "select sum(d+1) r from y",
                "create table y as ( select x + 0.1d as d from long_sequence(10) )"
        );
    }

    @Test // all values except first overflow to Infinity, sum overflows to null
    public void testSumOfAdditionOfDouble2() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(d+1) r from y",
                "create table y as ( select 1.7E308 * x as d  from long_sequence(10) )"
        );
    }

    @Test // all values except first are null and thus ignored
    public void testSumOfAdditionOfDouble3() throws Exception {
        assertAggQuery(
                """
                        r
                        2.0
                        """,
                "select sum(d+1) r from y",
                "create table y as ( select (1.7E308 * x)/(1.7E308*x) as d  from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfAdditionOfShort() throws Exception {
        assertAggQuery(
                """
                        r
                        65
                        """,
                "select sum(x+1) r from y",
                "create table y as ( select x::short x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfAdditionOverflow1() throws Exception {
        assertAggQuery(
                """
                        r
                        -9223372036854775805
                        """,
                "select sum(x+9223372036854775807) r from y",
                "create table y as ( select x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfAdditionOverflow2() throws Exception {
        assertAggQuery(
                """
                        r
                        -9223372036854775805
                        """,
                "select sum(x) + 9223372036854775807*3 r from y",
                "create table y as ( select x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfAdditionWithNull() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(x+null) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );

        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(null+x) r from y",
                null
        );
    }

    // multiplication
    @Test
    public void testSumOfMultiplication1() throws Exception {
        assertAggQuery(
                """
                        r
                        55
                        """,
                "select sum(x*1) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfMultiplication2() throws Exception {
        assertAggQuery(
                """
                        r
                        55
                        """,
                "select sum(1*x) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfMultiplicationOfDouble1() throws Exception {
        assertAggQuery(
                """
                        r
                        112.00000000000001
                        """,
                "select sum(d*2) r from y",
                "create table y as ( select x + 0.1d as d from long_sequence(10) )"
        );
    }

    @Test // all values except first overflow to Infinity, sum overflows to null
    public void testSumOfMultiplicationOfDouble2() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(d*2) r from y",
                "create table y as ( select (1.7E308/2)*x as d  from long_sequence(10) )"
        );
    }

    @Test // all values except first are null and thus ignored
    public void testSumOfMultiplicationOfDouble3() throws Exception {
        assertAggQuery(
                """
                        r
                        2.0
                        """,
                "select sum(d*2) r from y",
                "create table y as ( select (1.7E308 * x)/(1.7E308*x) as d  from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfMultiplicationOverflow1() throws Exception {
        assertAggQuery(
                """
                        r
                        -6
                        """,
                "select sum(x*9223372036854775807) r from y",
                "create table y as ( select x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfMultiplicationOverflow2() throws Exception {
        assertAggQuery(
                """
                        r
                        -6
                        """,
                "select sum(x) * 9223372036854775807 r from y",
                "create table y as ( select x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfMultiplicationWithNull() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(x*null) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );

        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(null*x) r from y",
                null
        );
    }

    // subtraction
    @Test
    public void testSumOfSubtraction1() throws Exception {
        assertAggQuery(
                """
                        r
                        45
                        """,
                "select sum(x-1) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfSubtraction2() throws Exception {
        assertAggQuery(
                """
                        r
                        -45
                        """,
                "select sum(1-x) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfSubtractionOfDouble1() throws Exception {
        assertAggQuery(
                """
                        r
                        46.0
                        """,
                "select sum(d-1) r from y",
                "create table y as ( select x + 0.1d as d from long_sequence(10) )"
        );
    }

    @Test // all values except first overflow to Infinity, sum overflows to null
    public void testSumOfSubtractionOfDouble2() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(d-1) r from y",
                "create table y as ( select -1.7E308 * x as d  from long_sequence(10) )"
        );
    }

    @Test // all values except first are null and thus ignored
    public void testSumOfSubtractionOfDouble3() throws Exception {
        assertAggQuery(
                """
                        r
                        0.0
                        """,
                "select sum(d-1) r from y",
                "create table y as ( select (1.7E308 * x)/(1.7E308 * x) as d from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfSubtractionOfShort() throws Exception {
        assertAggQuery(
                """
                        r
                        45
                        """,
                "select sum(x-1) r from y",
                "create table y as ( select x::short x from long_sequence(10) )"
        );
    }

    @Test
    public void testSumOfSubtractionOverflow1() throws Exception {
        assertAggQuery(
                """
                        r
                        9223372036854775805
                        """,
                "select sum(x-9223372036854775807) r from y",
                "create table y as ( select -x x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfSubtractionOverflow2() throws Exception {
        assertAggQuery(
                """
                        r
                        9223372036854775805
                        """,
                "select sum(x) - 9223372036854775807*3 r from y",
                "create table y as ( select -x x from long_sequence(3) )"
        );
    }

    @Test
    public void testSumOfSubtractionWithNull() throws Exception {
        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(x-null) r from y",
                "create table y as ( select x from long_sequence(10) )"
        );

        assertAggQuery(
                """
                        r
                        null
                        """,
                "select sum(null-x) r from y",
                null
        );
    }

    private void assertAggQuery(
            String expected,
            String query,
            String ddl
    ) throws Exception {
        assertQuery(query)
                .ddl(ddl)
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }
}
