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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JoinFilterPushdownTest extends AbstractCairoTest {
    @Test
    public void testInnerKeyConstantsReachSlaveAfterPredicatePushdown() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k "
                    + "WHERE l.k=1 ORDER BY lid,rid", """
                    lid	rid
                    1	11
                    """, 1, 1, false, false, false, 2);
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k "
                    + "WHERE 1=l.k ORDER BY lid,rid", """
                    lid	rid
                    1	11
                    """, 1, 1, false, false, false, 2);
            assertOptimised("SELECT lid,rid FROM (SELECT l.id lid,r.id rid,l.k lk FROM lp_join_push_l l "
                            + "JOIN lp_join_push_r r ON l.k=r.k) q WHERE lk=1 ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            """,
                    1, 1, false, false, false, 2);
        });
    }

    @Test
    public void testInnerOnPartitionsConjunctsWithoutDuplicatingResidual() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r "
                            + "ON l.k=r.k AND l.id>0 AND r.id>0 AND l.v<r.v ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            """,
                    1, 1, false, true, false);
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r "
                            + "ON l.k=r.k AND l.id>0 AND r.id>0 ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            2	12
                            3	13
                            """,
                    1, 1, false, false, false);
        });
    }

    @Test
    public void testInnerWherePartitionsConjunctsAndPrunesBothSources() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k "
                            + "WHERE l.id IN (1,2,3) AND r.id>0 AND l.v>0 AND r.k<4 AND l.v<r.v ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            """,
                    1, 1, false, true, false);
        });
    }

    @Test
    public void testLeftOnAndNullableWhereTermsKeepNullExtensionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l LEFT JOIN lp_join_push_r r "
                            + "ON l.k=r.k AND l.id>1 AND r.v>10 ORDER BY lid,rid", """
                            lid	rid
                            1	null
                            2	null
                            3	null
                            4	null
                            """,
                    0, 0, true, false, false);
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l LEFT JOIN lp_join_push_r r "
                            + "ON l.k=r.k WHERE l.id>1 AND r.v=null ORDER BY lid,rid", """
                            lid	rid
                            3	13
                            4	null
                            """,
                    1, 0, false, true, false);
            assertOptimised("SELECT lid,rid FROM (SELECT l.id lid,r.id rid,r.v rv FROM lp_join_push_l l "
                            + "LEFT JOIN lp_join_push_r r ON l.k=r.k) q WHERE lid>1 AND rv=null ORDER BY lid,rid", """
                            lid	rid
                            3	13
                            4	null
                            """,
                    1, 0, false, false, false);
        });
    }

    @Test
    public void testNullRejectingSymbolInFiltersNullExtendedInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ns_a (s SYMBOL, x INT)");
            execute("CREATE TABLE ns_c (z INT)");
            execute("CREATE TABLE ns_d (k INT)");
            execute("INSERT INTO ns_a VALUES ('a', 1), ('b', 2), (NULL, 3)");
            execute("INSERT INTO ns_c VALUES (1)");
            execute("INSERT INTO ns_d VALUES (1), (2), (3), (4)");
            final String query = "SELECT a.s, a.x, d.k FROM ns_a a RIGHT JOIN ns_d d ON a.x = d.k WHERE %s ORDER BY 3";
            assertQuery(query.formatted("a.s IN ('a')"))
                    .withPlanContaining("filter: s in [a]")
                    .returns("""
                            s\tx\tk
                            a\t1\t1
                            """);
            assertQuery(query.formatted("a.s IN ('a', NULL)"))
                    .withPlanNotContaining("filter: s in [a,null]")
                    .returns("""
                            s\tx\tk
                            a\t1\t1
                            \t3\t3
                            \tnull\t4
                            """);
            assertQuery("SELECT a.s, a.x, d.k FROM ns_c c, ns_a a RIGHT JOIN ns_d d ON a.x = d.k WHERE a.x > 1 ORDER BY 3")
                    .withPlanContaining("filter: 1<x")
                    .withPlanNotContaining("1<x and 1<x")
                    .returns("""
                            s\tx\tk
                            b\t2\t2
                            \t3\t3
                            """);
        });
    }

    @Test
    public void testNullRejectingWhereFiltersNullExtendedInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nr_a (x INT, y INT)");
            execute("CREATE TABLE nr_c (z INT)");
            execute("CREATE TABLE nr_d (k INT)");
            execute("INSERT INTO nr_a VALUES (1, 1), (2, 2), (3, 5), (4, NULL), (5, 0), (NULL, 9)");
            execute("INSERT INTO nr_c VALUES (1), (2)");
            execute("INSERT INTO nr_d VALUES (2), (3), (4), (5), (7), (NULL)");
            final String query = "SELECT a.x, c.z, d.k FROM nr_a a CROSS JOIN nr_c c %s JOIN nr_d d ON a.x = d.k WHERE %s ORDER BY 1, 2, 3";
            assertQuery(query.formatted("RIGHT", "a.x > 3"))
                    .withPlanContaining("filter: 3<x")
                    .returns("""
                            x\tz\tk
                            4\t1\t4
                            4\t2\t4
                            5\t1\t5
                            5\t2\t5
                            """);
            final String sum = """
                    x\tz\tk
                    2\t1\t2
                    2\t2\t2
                    3\t1\t3
                    3\t2\t3
                    5\t1\t5
                    5\t2\t5
                    """;
            assertQuery(query.formatted("RIGHT", "a.x + a.y > 3"))
                    .withPlanContaining("filter: 3<x+y")
                    .returns(sum);
            assertQuery(query.formatted("FULL", "a.x + a.y > 3"))
                    .withPlanContaining("filter: 3<x+y")
                    .returns(sum);
            assertQuery(query.formatted("RIGHT", "a.x = 4 OR a.y = 2"))
                    .withPlanContaining("filter: (x=4 or y=2)")
                    .returns("""
                            x\tz\tk
                            2\t1\t2
                            2\t2\t2
                            4\t1\t4
                            4\t2\t4
                            """);
            assertQuery(query.formatted("RIGHT", "a.x = 4 OR a.y = NULL"))
                    .withPlanNotContaining("filter: (x=4 or y=null)")
                    .returns("""
                            x\tz\tk
                            null\tnull\t7
                            4\t1\t4
                            4\t2\t4
                            """);
            assertQuery("SELECT d.k, a.x, c.z FROM nr_d d LEFT JOIN nr_a a ON a.x = d.k CROSS JOIN nr_c c WHERE a.x + a.y > 3 ORDER BY 1, 2, 3")
                    .withPlanNotContaining("filter: 3<x+y")
                    .returns("""
                            k\tx\tz
                            2\t2\t1
                            2\t2\t2
                            3\t3\t1
                            3\t3\t2
                            5\t5\t1
                            5\t5\t2
                            """);
        });
    }

    @Test
    public void testOrRemainsIndivisibleAndConstantsKeepTheirBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k "
                            + "WHERE l.id>0 AND (l.v<r.v OR r.v=null) ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            3	13
                            """,
                    1, 0, false, true, false);
            bindVariableService.setBoolean(0, true);
            bindVariableService.setBoolean(1, true);
            // Parameter links report non-determinism and stay global. They do
            // not prevent independent source-local conjuncts from moving.
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k "
                            + "WHERE $1 AND l.id>0 AND $2 AND r.id>0 ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            2	12
                            3	13
                            """,
                    1, 1, false, true, false, 3);
            assertOptimised("SELECT lid,rid FROM (SELECT l.id lid,r.id rid FROM lp_join_push_l l "
                            + "JOIN lp_join_push_r r ON l.k=r.k) q WHERE $1 AND lid>0 AND $2 AND rid>0 ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            2	12
                            3	13
                            """,
                    1, 1, false, false, false, 3);
            assertOptimised("SELECT lid,rid FROM (SELECT l.id lid,r.id rid FROM lp_join_push_l l "
                            + "JOIN lp_join_push_r r ON l.k=r.k ORDER BY l.id) q "
                            + "WHERE $1 AND lid>0 AND $2 AND rid>0 ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            2	12
                            3	13
                            """,
                    1, 1, false, false, false, 3);
        });
    }

    @Test
    public void testOuterConjunctsReachEverySourceOfOrderedInnerRegion() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT lid,rid,xid FROM (SELECT l.id lid,r.id rid,x.id xid "
                    + "FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k "
                    + "JOIN lp_join_push_l x ON r.k=x.k) q "
                    + "WHERE lid>0 AND rid>0 AND xid>0 ORDER BY lid,rid,xid";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                final JoinPlan join = findJoin(compiler.getPlanForTesting());
                Assert.assertNotNull(join);
                Assert.assertEquals(3, join.getOrderedInputs().size());
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals(1, countFilters(join.inputAt(i)));
                    assertPrunedInputs(join.inputAt(i));
                }
                assertPhysicalFilterCount(factory, 3);
                assertRowsOnly(factory, """
                        lid	rid	xid
                        1	11	1
                        2	12	2
                        3	13	3
                        """);
            }
        });
    }

    @Test
    public void testSeparatedTimestampTermsReachOneNativeIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k "
                            + "WHERE l.ts>='2020-01-02T00:00:00.000000Z' AND r.v>0 "
                            + "AND l.ts<'2020-01-04T00:00:00.000000Z' ORDER BY lid,rid", """
                            lid	rid
                            2	12
                            """,
                    1, 1, false, false, true);
            assertOptimised("SELECT lid,rid FROM (SELECT l.id lid,r.id rid,l.ts ts "
                            + "FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k) q "
                            + "WHERE q.ts>='2020-01-02T00:00:00.000000Z' AND q.rid>0 "
                            + "AND q.ts<'2020-01-04T00:00:00.000000Z' ORDER BY lid,rid", """
                            lid	rid
                            2	12
                            3	13
                            """,
                    1, 1, false, false, true);
        });
    }

    @Test
    public void testSourceLimitAndComputedProjectionRemainBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("SELECT l.id lid,r.id rid FROM (SELECT id,k,v FROM lp_join_push_l LIMIT 2) l "
                            + "JOIN lp_join_push_r r ON l.k=r.k WHERE l.id>1 AND r.id>0 ORDER BY lid,rid", """
                            lid	rid
                            2	12
                            """,
                    1, 1, false, false, false);
            assertOptimised("SELECT l.id lid,r.id rid FROM (SELECT id,k,v+1 value FROM lp_join_push_l) l "
                            + "JOIN lp_join_push_r r ON l.k=r.k WHERE l.value>1 AND r.id>0 ORDER BY lid,rid", """
                            lid	rid
                            1	11
                            2	12
                            3	13
                            """,
                    1, 1, false, false, false);
        });
    }

    @Test
    public void testSplitNativeClosuresSurviveCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            final String expected = """
                    lid	rid
                    1	11
                    """;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    final String sql = "SELECT l.id lid,r.id rid FROM lp_join_push_l l JOIN lp_join_push_r r ON l.k=r.k "
                            + "WHERE l.id IN (1,2,3) AND r.id IN (11,12,13) AND l.v<r.v ORDER BY lid,rid";
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(factory, expected);
                    }
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_join_push_l", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    compiler.clear();
                }
                assertRowsOnly(retained, expected);
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testTransitiveConstantReachesJoinedTableThroughComputedSetOperationColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tc_a (k INT, v INT)");
            execute("CREATE TABLE tc_b (k INT, v INT)");
            execute("CREATE TABLE tc_c (k INT, x INT)");
            execute("CREATE TABLE tc_d (x INT)");
            execute("INSERT INTO tc_a VALUES (5, 1), (6, 2)");
            execute("INSERT INTO tc_b VALUES (5, 3), (7, 4)");
            execute("INSERT INTO tc_c VALUES (5, 1), (5, 2), (6, 1), (NULL, 1)");
            execute("INSERT INTO tc_d VALUES (1), (2), (3)");
            assertQuery("""
                    SELECT u.v, c.x, d.x dx
                    FROM (SELECT k, v FROM tc_a UNION ALL SELECT k + 0 AS k, v FROM tc_b) u
                    JOIN tc_c c ON c.k = u.k
                    RIGHT JOIN tc_d d ON d.x = c.x
                    WHERE u.k = 5
                    ORDER BY v, x
                    """)
                    .withPlan("""
                            Encode sort
                              keys: [v, x]
                                SelectedRecord
                                    Filter filter: u.k=5
                                        Hash Right Outer Join Light
                                          condition: d.x=c.x
                                            Hash Join Light
                                              condition: c.k=u.k
                                                Filter filter: k=5
                                                    Union All
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tc_a
                                                        VirtualRecord
                                                          functions: [k+0,v]
                                                            PageFrame
                                                                Row forward scan
                                                                Frame forward scan on: tc_b
                                                Hash
                                                    Async JIT Filter workers: 1
                                                      filter: k=5
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: tc_c
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tc_d
                            """)
                    .returns("""
                            v\tx\tdx
                            1\t1\t1
                            1\t2\t2
                            3\t1\t1
                            3\t2\t2
                            """);
        });
    }

    private static void assertExpressionColumns(BoundExpression expression, OutputSchema input) {
        if (expression instanceof ColumnExpression column) {
            Assert.assertTrue(input.getColumnIndexById(column.getColumnId()) >= 0);
        } else if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                assertExpressionColumns(function.argumentAt(i), input);
            }
        }
    }

    private static void assertPrunedInputs(LogicalPlan plan) {
        Assert.assertEquals(-1, plan.getOutput().getColumnIndexQuiet("unused"));
        if (plan instanceof FilterPlan) {
            final FilterPlan filter = (FilterPlan) plan;
            assertExpressionColumns(filter.getPredicate(), filter.getChild().getOutput());
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            assertPrunedInputs(plan.inputAt(i));
        }
    }

    private static int countFilters(LogicalPlan plan) {
        int count = plan instanceof FilterPlan ? 1 : 0;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            count += countFilters(plan.inputAt(i));
        }
        return count;
    }

    private static JoinPlan findJoin(LogicalPlan plan) {
        if (plan instanceof JoinPlan) {
            return (JoinPlan) plan;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final JoinPlan join = findJoin(plan.inputAt(i));
            if (join != null) {
                return join;
            }
        }
        return null;
    }

    private void assertIntervalPlan(RecordCursorFactory factory) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        TestUtils.assertContains(plan.getSink(), "Interval forward scan");
    }

    private void assertOptimised(String sql, String expected, int masterFilters, int slaveFilters, boolean hasOnResidual,
                                 boolean hasPostResidual, boolean hasIntervals, int physicalFilterCount) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
             RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            final JoinPlan join = findJoin(compiler.getPlanForTesting());
            Assert.assertNotNull(join);
            Assert.assertEquals(masterFilters, countFilters(join.inputAt(0)));
            Assert.assertEquals(slaveFilters, countFilters(join.inputAt(1)));
            final JoinInput step = join.getOrderedInputs().getQuick(1);
            Assert.assertEquals(hasOnResidual, step.getOnResidual() != null);
            Assert.assertEquals(hasPostResidual, step.getPostJoinFilter() != null);
            assertPrunedInputs(join.inputAt(0));
            assertPrunedInputs(join.inputAt(1));
            if (hasIntervals) {
                assertIntervalPlan(factory);
            }
            if (physicalFilterCount >= 0) {
                assertPhysicalFilterCount(factory, physicalFilterCount);
            }
            assertRowsOnly(factory, expected);
        }
    }

    private void assertOptimised(String sql, String expected, int masterFilters, int slaveFilters, boolean hasOnResidual,
                                 boolean hasPostResidual, boolean hasIntervals) throws Exception {
        assertOptimised(sql, expected, masterFilters, slaveFilters, hasOnResidual, hasPostResidual, hasIntervals, -1);
    }

    private void assertPhysicalFilterCount(RecordCursorFactory factory, int expectedCount) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        int count = 0;
        for (int i = 1, n = plan.getLineCount(); i <= n; i++) {
            if (plan.getLine(i).toString().contains("Filter")) {
                count++;
            }
        }
        Assert.assertEquals(plan.getSink().toString(), expectedCount, count);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_join_push_l(id INT,k INT,v INT,unused LONG,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_join_push_r(id INT,k INT,v INT,unused LONG,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO lp_join_push_l VALUES
                (1,1,10,1,'2020-01-01'),(2,2,20,2,'2020-01-02'),
                (3,3,30,3,'2020-01-03'),(4,4,40,4,'2020-01-04')
                """);
        execute("""
                INSERT INTO lp_join_push_r VALUES
                (11,1,11,1,'2020-01-01'),(12,2,5,2,'2020-01-02'),(13,3,null,3,'2020-01-03')
                """);
    }
}
