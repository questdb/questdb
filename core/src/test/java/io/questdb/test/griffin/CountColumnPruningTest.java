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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TableFunctionTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A count reads no column of its input: it counts the input's rows, whatever the input computes. So a computed column
 * that nothing above the count's input reads leaves the plan, as it does under any other consumer, while a column that
 * a node beneath the count reads (a filter, a sort with a LIMIT, a grouping or partition key, a join key, the tuple a
 * set operation compares) stays. QuestDB evaluates a projection's columns only when a consumer reads them, so pruning
 * an unread column changes no result, whether the column is non-deterministic or would fail when evaluated; it saves
 * constructing and initialising the column's function, and keeps the plan free of calls the query never makes.
 */
public class CountColumnPruningTest extends AbstractCairoTest {
    private static final String SCAN_PLAN = """
            Count
                PageFrame
                    Row forward scan
                    Frame forward scan on: t
            """;
    // counted(x) returns x and counts the instances its factory constructs, their init() calls and their evaluations
    private static int constructionCount;
    private static int evaluationCount;
    private static int initCount;

    @Test
    public void testCountKeepsColumnsItsInputReads() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // a filter over a computed column: y + 1 is NULL on the rows where y is NULL (x = 4, 8), which the filter drops
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t) WHERE z > 2",
                    """
                            Count
                                Filter filter: 2<z
                                    VirtualRecord
                                      functions: [y+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """,
                    8
            );
            // the sort key of a sort with a LIMIT
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t ORDER BY z LIMIT 3)",
                    """
                            Count
                                Encode sort light lo: 3
                                  keys: [z]
                                    VirtualRecord
                                      functions: [y+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """,
                    3
            );
            // a computed grouping key defines the groups the count counts, x % 3 has three values
            assertCount(
                    "SELECT count() FROM (SELECT x % 3 k, sum(y + 1) s FROM t GROUP BY k)",
                    """
                            Count
                                Async Group By workers: 1
                                  keys: [k]
                                  keyFunctions: [x%3]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """,
                    3
            );
            // DISTINCT compares every column: the ten (x % 3, y + 1) pairs differ, the two NULL ones by x % 3
            assertCount(
                    "SELECT count() FROM (SELECT DISTINCT x % 3 k, y + 1 z FROM t)",
                    """
                            Count
                                Async Group By workers: 1
                                  keys: [k,z]
                                  keyFunctions: [x%3,y+1]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """,
                    10
            );
            // a computed partition key and the designated timestamp of LATEST ON
            assertCount(
                    "SELECT count() FROM (SELECT x % 3 k, y + 1 z, ts FROM t) LATEST ON ts PARTITION BY k",
                    """
                            Count
                                LatestBy light order_by_timestamp: true
                                    VirtualRecord
                                      functions: [x%3,ts]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """,
                    3
            );
            // a computed join key: x + 1 matches u.x for x = 1..9
            assertCount(
                    "SELECT count() FROM (SELECT x + 1 k, y + 1 z FROM t) a JOIN u ON a.k = u.x",
                    """
                            Count
                                Hash Join Light
                                  condition: u.x=a.k
                                    VirtualRecord
                                      functions: [x+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: u
                            """,
                    9
            );
        });
    }

    @Test
    public void testCountOverLateralReadsOnlyTheJoinKeys() throws Exception {
        // Decorrelation joins the master to the body on the master's column a, which the body reads; the master's
        // other columns, x and the computed b, and the projection that compensates an empty body are unread.
        assertMemoryLeak(() -> {
            createTables();
            final String master = "(SELECT x, x + 1 a, y + 1 b, ts FROM t) m";
            // a keyless aggregate body returns one row per master row
            final String scalarBody = "SELECT count() FROM " + master + " CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.x > m.a) l";
            assertCount(
                    scalarBody,
                    """
                            Count
                                Hash Left Outer Join Light
                                  condition: l.__qdb_outer_ref__0_a=m.a
                                    VirtualRecord
                                      functions: [x+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Hash
                                        GroupBy vectorized: false
                                          keys: [__qdb_outer_ref__0_a]
                                            Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_a<x
                                                Cross Join
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: u
                                                    GroupBy vectorized: false
                                                      keys: [__qdb_outer_ref__0_a]
                                                        VirtualRecord
                                                          functions: [x+1]
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: t
                            """,
                    10
            );
            assertSqlCursors("SELECT count() FROM t", scalarBody);

            // the body keeps at most two of the rows u.x = m.a matches, one each for a = 2..10
            final String limitBody = "SELECT count() FROM " + master + " CROSS JOIN LATERAL (SELECT z FROM u WHERE u.x = m.a LIMIT 2) l";
            assertCount(
                    limitBody,
                    """
                            Count
                                Hash Join
                                  condition: l.__qdb_outer_ref__0_a=m.a
                                    VirtualRecord
                                      functions: [x+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Hash
                                        Filter filter: 2L>=__lateral_rn
                                            Window
                                              functions: [row_number() over (partition by [__qdb_outer_ref__0_a])]
                                                SelectedRecord
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: u
                            """,
                    9
            );
            assertSqlCursors("SELECT count() FROM t JOIN u ON u.x = t.x + 1", limitBody);

            // a GROUP BY master, which the domain reads again: its key defines the master rows, the body reads sx, and
            // nothing reads sum(y) or w; x % 3 has three groups
            assertCount(
                    "SELECT count() FROM (SELECT x % 3 k, sum(x) sx, sum(y) * 2 w FROM t GROUP BY k) m"
                            + " CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.x > m.sx) l",
                    """
                            Count
                                Hash Left Outer Join Light
                                  condition: l.__qdb_outer_ref__0_sx=m.sx
                                    SelectedRecord
                                        Async Group By workers: 1
                                          keys: [k]
                                          keyFunctions: [x%3]
                                          values: [sum(x)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                                    Hash
                                        GroupBy vectorized: false
                                          keys: [__qdb_outer_ref__0_sx]
                                            Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_sx<x
                                                Cross Join
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: u
                                                    GroupBy vectorized: false
                                                      keys: [__qdb_outer_ref__0_sx]
                                                        Async Group By workers: 1
                                                          keys: [k]
                                                          keyFunctions: [x%3]
                                                          values: [sum(x)]
                                                          filter: null
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: t
                            """,
                    3
            );

            // u.x > m.a matches 10 - a rows for a < 10, of which the body keeps two; a = 10 and 11 keep the master row
            // with NULLs: 7 * 2 + 1 + 1 + 1
            assertCount(
                    "SELECT count() FROM " + master + " LEFT JOIN LATERAL (SELECT z FROM u WHERE u.x > m.a LIMIT 2) l ON true",
                    """
                            Count
                                Hash Left Outer Join
                                  condition: l.__qdb_outer_ref__0_a=m.a
                                  filter: true
                                    VirtualRecord
                                      functions: [x+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Hash
                                        Filter filter: 2L>=__lateral_rn
                                            Window
                                              functions: [row_number() over (partition by [__qdb_outer_ref__0_a])]
                                                SelectedRecord
                                                    Filter filter: __qdb_outer_ref__0.__qdb_outer_ref__0_a<x
                                                        Cross Join
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: u
                                                            GroupBy vectorized: false
                                                              keys: [__qdb_outer_ref__0_a]
                                                                VirtualRecord
                                                                  functions: [x+1]
                                                                    PageFrame
                                                                        Row forward scan
                                                                        Frame forward scan on: t
                            """,
                    17
            );
        });
    }

    @Test
    public void testCountOverSetOperations() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // UNION ALL concatenates the rows of its branches, so the count reads no column of either
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t UNION ALL SELECT x, z + 1 FROM u)",
                    """
                            Count
                                Union All
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: u
                            """,
                    20
            );
            // UNION, INTERSECT and EXCEPT compare whole rows, so each branch computes all its columns. Pairs (x, y + 1)
            // of t equal pairs (x, 2 * x + 1) of u wherever y is not NULL, and differ from pairs (x, 3 * x + 1) of u
            // everywhere; comparing x alone would count 10, 10 and 0.
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t UNION SELECT x, z + 1 FROM u)",
                    """
                            Count
                                Union
                                    VirtualRecord
                                      functions: [x,y+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    VirtualRecord
                                      functions: [x,z+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: u
                            """,
                    20
            );
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t INTERSECT SELECT x, 2 * x + 1 FROM u)",
                    """
                            Count
                                Intersect
                                    VirtualRecord
                                      functions: [x,y+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Hash
                                        VirtualRecord
                                          functions: [x,2*x+1]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: u
                            """,
                    8
            );
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t EXCEPT SELECT x, 2 * x + 1 FROM u)",
                    """
                            Count
                                Except
                                    VirtualRecord
                                      functions: [x,y+1]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Hash
                                        VirtualRecord
                                          functions: [x,2*x+1]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: u
                            """,
                    2
            );
        });
    }

    @Test
    public void testCountReadsNoColumnOfAComputingProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] queries = {
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t)",
                    "SELECT count(*) FROM (SELECT x, y + 1 z FROM t)",
                    // a non-deterministic column: unread, it is never evaluated, kept or not
                    "SELECT count() FROM (SELECT x, y + 1 z, rnd_int() r FROM t)",
                    // a column that reads another by alias
                    "SELECT count() FROM (SELECT x, y + 1 z, z * 2 w FROM t)",
                    // nested computing projections
                    "SELECT count() FROM (SELECT z + 1 w FROM (SELECT x, y + 1 z FROM t))",
                    // a declared timestamp, which the count does not need either
                    "SELECT count() FROM (SELECT x, y + 1 z, ts FROM t) TIMESTAMP(ts)",
                    "SELECT count() FROM v",
                    "WITH c AS (SELECT x, y + 1 z FROM t) SELECT count() FROM c",
                    // a sort that nothing above the count needs
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t ORDER BY z)",
                    // a window function that nothing reads
                    "SELECT count() FROM (SELECT x, y + 1 z, sum(x) OVER (ORDER BY x DESC) w FROM t)"
            };
            for (String query : queries) {
                assertCount(query, SCAN_PLAN, 10);
            }
            assertQuery("SELECT count() c FROM (SELECT x, y + 1 z FROM t)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan(SCAN_PLAN)
                    .returns("""
                            c
                            10
                            """);
            assertQuery("SELECT count() FROM (SELECT x, y + 1 z FROM t) LIMIT 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Limit value: 1 skip-rows: 0 take-rows: 1
                                Count
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """)
                    .returns("""
                            count
                            10
                            """);
            // the empty table
            assertQuery("SELECT count() FROM (SELECT x, y + 1 z FROM e)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Count
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: e
                            """)
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testCountReadsOnlyTheColumnsBeneathItRead() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t) WHERE x > 2",
                    """
                            Count
                                Async JIT Filter workers: 1
                                  filter: 2<x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """,
                    8
            );
            bindVariableService.clear();
            bindVariableService.setLong(0, 5);
            bindVariableService.setLong(1, 2);
            assertCount(
                    "SELECT count() FROM (SELECT x, y + $1 z FROM t) WHERE x > $2",
                    """
                            Count
                                Async JIT Filter workers: 1
                                  filter: $1::long<x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """,
                    8
            );
            bindVariableService.clear();
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t LIMIT 3)",
                    """
                            Count
                                Limit value: 3 skip-rows: 0 take-rows: 3
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """,
                    3
            );
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z FROM t ORDER BY x DESC LIMIT 3)",
                    """
                            Count
                                Async Top K lo: 3 workers: 1
                                  filter: null
                                  keys: [x desc]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """,
                    3
            );
            // the sort by the designated timestamp becomes a backward scan
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z, ts FROM t ORDER BY ts DESC LIMIT 4)",
                    """
                            Count
                                Limit value: 4 skip-rows: 0 take-rows: 4
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: t
                            """,
                    4
            );
            // the interval scan takes the designated timestamp conjunct, ts = 4..9 seconds
            assertCount(
                    "SELECT count() FROM (SELECT x, y + 1 z, ts FROM t) WHERE ts > '1970-01-01T00:00:03'",
                    """
                            Count
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: t
                                      intervals: [("1970-01-01T00:00:03.000001Z","MAX")]
                            """,
                    6
            );
        });
    }

    @Test
    public void testUnreadColumnIsNeitherConstructedNorEvaluated() throws Exception {
        // Binding constructs counted(y) once to type the call. Code generation constructs, and the cursor initialises,
        // only the calls the generated plan keeps, and a kept call is evaluated once per row a consumer reads.
        assertMemoryLeak(() -> {
            createTables();
            registerTestFunctions();
            try {
                final String[][] unread = {
                        {"SELECT count() FROM (SELECT x, counted(y) z FROM t)", "10"},
                        {"SELECT count() FROM (SELECT x, counted(y) z FROM t) WHERE x > 2", "8"},
                        {"SELECT count() FROM (SELECT x, counted(y) z FROM t LIMIT 3)", "3"},
                        {"SELECT count() FROM (SELECT x, counted(y) z FROM t ORDER BY x LIMIT 3)", "3"},
                        {"SELECT count() FROM (SELECT x, counted(y) z FROM t UNION ALL SELECT x, z FROM u)", "20"},
                        {"SELECT count() FROM (SELECT x, x + 1 a, counted(y) b FROM t) m CROSS JOIN LATERAL (SELECT count() c FROM u WHERE u.x > m.a) l", "10"},
                        {"SELECT count() FROM (SELECT x, counted(y) z FROM e)", "0"}
                };
                for (String[] c : unread) {
                    assertEvaluations(c[0], c[1], 1, 0, 0);
                }
                // a filter reads the column on every row, NULL ones included
                assertEvaluations("SELECT count() FROM (SELECT x, counted(y) z FROM t) WHERE z > 1", "8", 2, 1, 10);
                assertEvaluations("SELECT count() FROM (SELECT x, counted(y) z FROM t ORDER BY z LIMIT 3)", "3", 2, 1, 10);
                assertEvaluations("SELECT count() FROM (SELECT x, counted(y) z FROM e) WHERE z > 1", "0", 2, 1, 0);

                // a column that would fail when evaluated: unread, the count succeeds, as it does on master, which
                // evaluates no column of a count's input either; read, the error surfaces
                assertQuery("SELECT count() FROM (SELECT x, fails(y) z FROM t)")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlan(SCAN_PLAN)
                        .returns("""
                                count
                                10
                                """);
                assertQuery("SELECT count() FROM (SELECT x, fails(y) z FROM t) WHERE z > 1").noLeakCheck().failsWith("fails() evaluated");
            } finally {
                TableFunctionTestUtils.unregister(engine, "counted");
                TableFunctionTestUtils.unregister(engine, "fails");
            }
        });
    }

    private void assertCount(String query, String expectedPlan, long expectedCount) throws Exception {
        assertQuery(query)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlan(expectedPlan)
                .returns("count\n" + expectedCount + '\n');
    }

    private static void assertEvaluations(String query, String expectedCount, int constructions, int inits, int evaluations) throws Exception {
        constructionCount = 0;
        initCount = 0;
        evaluationCount = 0;
        final StringSink sink = new StringSink();
        printSql(query, sink);
        TestUtils.assertEquals(query, "count\n" + expectedCount + '\n', sink);
        Assert.assertEquals(query, constructions, constructionCount);
        Assert.assertEquals(query, inits, initCount);
        Assert.assertEquals(query, evaluations, evaluationCount);
    }

    private static void createTables() throws Exception {
        // y is NULL where x is a multiple of 4
        execute("""
                CREATE TABLE t AS (
                    SELECT x, CASE WHEN x % 4 = 0 THEN NULL ELSE x * 2 END y, timestamp_sequence(0, 1_000_000) ts
                    FROM long_sequence(10)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("""
                CREATE TABLE u AS (
                    SELECT x, x * 3 z, timestamp_sequence(0, 1_000_000) ts FROM long_sequence(10)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE e (x LONG, y LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE VIEW v AS (SELECT x, y + 1 c, ts FROM t)");
    }

    private static void registerTestFunctions() throws SqlException {
        final ObjList<FunctionFactoryDescriptor> counted = new ObjList<>();
        counted.add(new FunctionFactoryDescriptor(new CountedFunctionFactory()));
        Assert.assertNull(engine.getFunctionFactoryCache().getFactories().get("counted"));
        engine.getFunctionFactoryCache().getFactories().put("counted", counted);
        final ObjList<FunctionFactoryDescriptor> fails = new ObjList<>();
        fails.add(new FunctionFactoryDescriptor(new FailsFunctionFactory()));
        Assert.assertNull(engine.getFunctionFactoryCache().getFactories().get("fails"));
        engine.getFunctionFactoryCache().getFactories().put("fails", fails);
    }

    private static class CountedFunction extends LongFunction implements UnaryFunction {
        private final Function arg;

        private CountedFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong(Record rec) {
            evaluationCount++;
            return arg.getLong(rec);
        }

        @Override
        public String getName() {
            return "counted";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            initCount++;
            UnaryFunction.super.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class CountedFunctionFactory implements FunctionFactory {
        @Override
        public String getSignature() {
            return "counted(L)";
        }

        @Override
        public Function newInstance(
                int position,
                ObjList<Function> args,
                IntList argPositions,
                CairoConfiguration configuration,
                SqlExecutionContext executionContext
        ) {
            constructionCount++;
            return new CountedFunction(args.getQuick(0));
        }
    }

    private static class FailsFunction extends LongFunction implements UnaryFunction {
        private final Function arg;

        private FailsFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong(Record rec) {
            throw CairoException.nonCritical().put("fails() evaluated");
        }

        @Override
        public String getName() {
            return "fails";
        }
    }

    private static class FailsFunctionFactory implements FunctionFactory {
        @Override
        public String getSignature() {
            return "fails(L)";
        }

        @Override
        public Function newInstance(
                int position,
                ObjList<Function> args,
                IntList argPositions,
                CairoConfiguration configuration,
                SqlExecutionContext executionContext
        ) {
            return new FailsFunction(args.getQuick(0));
        }
    }
}
