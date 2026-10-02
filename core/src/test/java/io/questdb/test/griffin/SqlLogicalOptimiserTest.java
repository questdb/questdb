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
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalOptimiserTest extends AbstractCairoTest {
    @Test
    public void testAdjacentFiltersCombineAfterColumnRemapping() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("SELECT value FROM (SELECT id AS value FROM lp_pushdown WHERE id IN(1,2,3)) WHERE value>1 ORDER BY value",
                    "value\n2\n3\n", LogicalPlan.Type.SCAN);
            assertOptimised("SELECT id FROM (SELECT id FROM (SELECT id FROM lp_pushdown WHERE id IN(1,2,3)) WHERE id<3) WHERE id>1",
                    "id\n2\n", LogicalPlan.Type.SCAN);
            assertOptimised("SELECT id FROM (SELECT id FROM lp_pushdown WHERE false) WHERE id IN(1,2,3)",
                    "id\n", LogicalPlan.Type.SCAN);
        });
    }

    @Test
    public void testAdjacentColumnProjectsCollapseWithReorderedRepeatedOutputs() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertCollapsed("""
                    SELECT second AS value,first AS duplicate
                    FROM (SELECT a AS first,a AS second FROM (SELECT id AS a FROM lp_pushdown))
                    """, "value\tduplicate\n3\t3\n1\t1\n2\t2\n4\t4\n", 1);
        });
    }

    @Test
    public void testAdjacentProjectionCollapseKeepsTimestampMetadata() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_projection_ts (id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_projection_ts VALUES (1,'2020-01-01T00:00:00.000000Z'),(2,'2020-01-01T00:00:01.000000Z')");
            final String expected = "b\n2020-01-01T00:00:00.000000Z\n2020-01-01T00:00:01.000000Z\n";
            assertCollapsed("SELECT b FROM (SELECT ts AS a,ts AS b FROM lp_projection_ts)", expected, 1);
            assertCollapsed("SELECT b FROM (SELECT ts AS b FROM lp_projection_ts ORDER BY lp_projection_ts.ts)", expected, 1);
        });
    }

    @Test
    public void testComputedProjectionPushesColumnFilter() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("""
                    SELECT value FROM (SELECT id+1 AS value,id AS raw FROM lp_pushdown) q
                    WHERE q.raw>1 ORDER BY value
                    """, "value\n3\n4\n5\n", LogicalPlan.Type.SCAN);
            assertOptimised("""
                    SELECT value FROM (SELECT id+1 AS value,id AS raw FROM lp_pushdown) q
                    WHERE q.value>2 ORDER BY value
                    """, "value\n3\n4\n5\n", LogicalPlan.Type.PROJECT);
        });
    }

    @Test
    public void testTimestampCastProjectionComparesAtConsumerPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_projection_ts (id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_projection_ts VALUES (1,'2020-01-01'),(2,'2020-01-02')");
            final String literal = "'2020-01-01T00:00:00.000000001Z'";
            assertOptimised("SELECT id FROM (SELECT id,ts::timestamp AS renamed FROM lp_projection_ts) WHERE renamed=" + literal,
                    "id\n", LogicalPlan.Type.SCAN);
            assertOptimised("SELECT id FROM (SELECT id,ts::timestamp AS ts FROM lp_projection_ts) WHERE ts=" + literal,
                    "id\n", LogicalPlan.Type.SCAN);
            assertOptimised("SELECT id FROM (SELECT id,renamed AS second FROM"
                            + " (SELECT id,ts::timestamp AS renamed FROM lp_projection_ts)) WHERE second=" + literal,
                    "id\n", LogicalPlan.Type.SCAN);
            assertOptimised("SELECT id FROM (SELECT id,ts::timestamp AS renamed FROM lp_projection_ts) WHERE renamed=" + literal + " AND id>0",
                    "id\n", LogicalPlan.Type.SCAN);
            assertOptimised("SELECT renamed FROM (SELECT id,ts::timestamp AS renamed FROM lp_projection_ts) WHERE id>0",
                    "renamed\n2020-01-01T00:00:00.000000Z\n2020-01-02T00:00:00.000000Z\n", LogicalPlan.Type.SCAN);
        });
    }

    @Test
    public void testFilterCrossesAliasedColumnProjection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("""
                    SELECT value FROM (SELECT id AS value,unused AS extra FROM lp_pushdown) q
                    WHERE q.value>1 ORDER BY value
                    """, "value\n2\n3\n4\n", LogicalPlan.Type.SCAN);
        });
    }

    @Test
    public void testFilterCrossesConsecutiveProjectsAndSort() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("""
                    SELECT b FROM
                    (SELECT a AS b FROM (SELECT id AS a FROM lp_pushdown ORDER BY id DESC) q) q
                    WHERE q.b>1
                    """, "b\n4\n3\n2\n", LogicalPlan.Type.SCAN);
        });
    }

    @Test
    public void testInnerLimitKeepsItsOriginalRowSelection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("""
                    SELECT id FROM (SELECT id FROM lp_pushdown ORDER BY id LIMIT 2) q
                    WHERE q.id>1
                    """, "id\n2\n", LogicalPlan.Type.LIMIT);
        });
    }

    @Test
    public void testProjectionCollapseAcrossLimitKeepsRowSelection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertCollapsed("SELECT value FROM (SELECT id AS value FROM lp_pushdown LIMIT 2)", "value\n3\n1\n", 1);
            assertCollapsed("SELECT value AS final_value FROM (SELECT value FROM (SELECT id AS value FROM lp_pushdown LIMIT 3) LIMIT 1,2)",
                    "final_value\n1\n", 1);
        });
    }

    @Test
    public void testPushedFunctionSurvivesCompilerResetAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("""
                            SELECT value FROM (SELECT id AS value FROM lp_pushdown WHERE id IN(1,2,3,4)) q
                            WHERE q.value+1>2 ORDER BY value
                            """, sqlExecutionContext).getRecordCursorFactory();
                    assertFilterInput(compiler.getLogicalPlanForTesting(), LogicalPlan.Type.SCAN);
                    try (RecordCursorFactory other = compiler.compile(
                            "SELECT id FROM lp_pushdown WHERE id<3", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertResult(other, "id\n1\n2\n");
                    }
                    compiler.clear();
                    assertResult(retained, "value\n2\n3\n4\n");
                }
                assertResult(retained, "value\n2\n3\n4\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testQualifiedPredicatesKeepQuotedDotsThroughProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_prefix (unused INT,id INT,active BOOLEAN,note STRING)");
            execute("INSERT INTO lp_prefix VALUES (10,1,false,'x'),(20,2,true,'y'),(30,3,true,'X'),(40,4,true,NULL)");
            assertOptimised("""
                    SELECT "t.alias".id FROM lp_prefix AS "t.alias"
                    WHERE "t.alias".active ORDER BY id
                    """, "id\n2\n3\n4\n", LogicalPlan.Type.SCAN);
            assertOptimised("""
                    SELECT "q.alias"."result id" AS id FROM
                    (SELECT id AS "result id",active AS enabled,note AS message FROM lp_prefix) AS "q.alias"
                    WHERE "q.alias".enabled AND "q.alias"."result id"+1>2
                    AND "q.alias"."result id" IN (1,2,3) AND upper("q.alias".message)='X'
                    ORDER BY id
                    """, "id\n3\n", LogicalPlan.Type.SCAN);
        });
    }

    @Test
    public void testRepeatedAliasesMapToOneSourceColumn() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertOptimised("""
                    SELECT a FROM (SELECT id AS a,id AS b FROM lp_pushdown) q
                    WHERE q.a=q.b AND q.b>1 ORDER BY a
                    """, "a\n2\n3\n4\n", LogicalPlan.Type.SCAN);
        });
    }

    private static void assertExpressionInputs(BoundExpression expression, OutputSchema input) {
        if (expression instanceof ColumnExpression column) {
            final int index = input.getColumnIndexById(column.getColumnId());
            Assert.assertTrue("predicate refers to an unavailable logical column", index >= 0);
            Assert.assertEquals(column.getDataType(), input.getColumnType(index));
        } else if (expression instanceof FunctionExpression function) {
            for (int i = 0, n = function.getArgumentCount(); i < n; i++) {
                assertExpressionInputs(function.argumentAt(i), input);
            }
        }
    }

    private static void assertFilterInput(LogicalPlan root, LogicalPlan.Type expectedInput) {
        LogicalPlan node = root;
        while (node.getType() != LogicalPlan.Type.FILTER) {
            Assert.assertEquals("expected a filter in the unary plan", 1, node.inputCount());
            node = node.inputAt(0);
        }
        final FilterPlan filter = (FilterPlan) node;
        Assert.assertEquals(expectedInput, filter.getInput().getType());
        assertExpressionInputs(filter.getPredicate(), filter.getInput().getOutput());
        while (node.inputCount() > 0) {
            node = node.inputAt(0);
        }
        Assert.assertEquals(-1, node.getOutput().getColumnIndexQuiet("unused"));
    }

    private static int countProjects(LogicalPlan plan) {
        int count = plan.getType() == LogicalPlan.Type.PROJECT ? 1 : 0;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            count += countProjects(plan.inputAt(i));
        }
        return count;
    }

    private void assertCollapsed(String sql, String expected, int expectedProjects) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
             RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertEquals(expectedProjects, countProjects(compiler.getLogicalPlanForTesting()));
            assertResult(factory, expected);
        }
    }

    private void assertOptimised(String sql, String expected, LogicalPlan.Type expectedFilterInput) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
             RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            assertFilterInput(compiler.getLogicalPlanForTesting(), expectedFilterInput);
            assertResult(factory, expected);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_pushdown (unused INT,id INT)");
        execute("INSERT INTO lp_pushdown VALUES (30,3),(10,1),(20,2),(40,4)");
    }
}
