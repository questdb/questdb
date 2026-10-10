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

    private static void assertExpressionColumns(BoundExpression expression, OutputSchema input) {
        if (expression instanceof ColumnExpression column) {
            Assert.assertTrue(input.getColumnIndexById(column.getColumnId()) >= 0);
        } else if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                assertExpressionColumns(function.argumentAt(i), input);
            }
        }
    }

    private void assertOptimised(String sql, String expected, int masterFilters, int slaveFilters, boolean hasOnResidual,
                                 boolean hasPostResidual, boolean hasIntervals) throws Exception {
        assertOptimised(sql, expected, masterFilters, slaveFilters, hasOnResidual, hasPostResidual, hasIntervals, -1);
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

    private void assertIntervalPlan(RecordCursorFactory factory) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        TestUtils.assertContains(plan.getSink(), "Interval forward scan");
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

    private static void assertPrunedInputs(LogicalPlan plan) {
        Assert.assertEquals(-1, plan.getOutput().getColumnIndexQuiet("unused"));
        if (plan instanceof FilterPlan) {
            final FilterPlan filter = (FilterPlan) plan;
            assertExpressionColumns(filter.getPredicate(), filter.getInput().getOutput());
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
}
