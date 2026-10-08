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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanVerifier;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PlanVerifierTest extends AbstractCairoTest {
    private static final String PASS = "TestPass.rewrite";
    private final PlanVerifier verifier = PlanVerifier.newStandalone();

    @Test
    public void testAggregateShape() {
        final FunctionSourcePlan source = source();
        final AggregatePlan aggregate = AggregatePlan.FACTORY.newInstance().of(source, 7);
        aggregate.getGroupingExpressions().add(column(1, ColumnType.INT));
        assertFails(aggregate, PlanVerifier.AGGREGATE_SHAPE, "AggregatePlan");
    }

    @Test
    public void testBroadSqlSamplePasses() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE quotes (sym SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE tags (sym SYMBOL, tag STRING, ids DOUBLE[])");
            execute("""
                    INSERT INTO trades VALUES ('a', 1.0, 10, '2024-01-01T00:00:00.000000Z'), ('b', 2.0, 20, '2024-01-01T00:00:01.000000Z'),
                    ('a', 3.0, 30, '2024-01-01T00:00:02.000000Z')
                    """);
            execute("INSERT INTO quotes VALUES ('a', 0.9, 1.1, '2024-01-01T00:00:00.500000Z'), ('b', 1.9, 2.1, '2024-01-01T00:00:01.500000Z')");
            execute("INSERT INTO tags VALUES ('a', 'x', ARRAY[1.0, 2.0]), ('b', 'y', ARRAY[3.0])");
            final String[] queries = {
                    "SELECT * FROM trades",
                    "SELECT sym, price * qty AS notional FROM trades WHERE price > 1 ORDER BY notional DESC LIMIT 2",
                    "SELECT sym, sum(price), count() FROM trades GROUP BY sym ORDER BY sym",
                    "SELECT sym, avg(price), last(ts) FROM trades SAMPLE BY 1s FILL(PREV)",
                    "SELECT sym, max(ts) FROM trades",
                    "SELECT DISTINCT sym FROM trades ORDER BY sym",
                    "SELECT sym, row_number() OVER (PARTITION BY sym ORDER BY ts) rn, sum(qty) OVER (ORDER BY ts) running FROM trades",
                    "SELECT t.sym, t.price, q.bid FROM trades t ASOF JOIN quotes q ON sym",
                    "SELECT t.sym, q.ask FROM trades t JOIN quotes q ON t.sym = q.sym WHERE q.ask > 1 AND t.qty > 5",
                    "SELECT t.sym, q.ask FROM trades t LEFT JOIN quotes q ON t.sym = q.sym AND q.bid > 1",
                    "SELECT t.sym, g.tag FROM trades t CROSS JOIN tags g WHERE t.sym = g.sym",
                    "SELECT sym FROM trades UNION SELECT sym FROM quotes ORDER BY 1",
                    "SELECT sym, price FROM trades UNION ALL SELECT sym, bid FROM quotes",
                    "SELECT sym FROM trades WHERE sym IN (SELECT sym FROM quotes WHERE bid > 1)",
                    "SELECT * FROM trades LATEST ON ts PARTITION BY sym",
                    "SELECT sym, price FROM trades ORDER BY ts DESC LIMIT -2",
                    "SELECT t.sym, l.total FROM trades t JOIN LATERAL (SELECT sum(bid) total FROM quotes q WHERE q.sym = t.sym) l ON true",
                    "SELECT t.sym, l.n FROM trades t LEFT JOIN LATERAL (SELECT count() n FROM quotes q WHERE q.sym = t.sym AND q.ask > t.price) l ON true",
                    "SELECT g.sym, u.v FROM tags g, UNNEST(g.ids) AS u(v)",
                    "SELECT * FROM (SELECT sym, sum(price) s FROM trades) WHERE s > 1",
                    "SELECT sym, s + 1 FROM (SELECT sym, sum(qty * 2) s FROM trades GROUP BY sym) ORDER BY sym",
                    "SELECT * FROM trades PIVOT (sum(price) FOR sym IN ('a', 'b'))",
                    "SELECT t.sym, count(q.bid) FROM trades t WINDOW JOIN quotes q RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW",
                    "SELECT sym, first(price), last(price) FROM trades SAMPLE BY 1s ALIGN TO CALENDAR WITH OFFSET '00:30'",
                    "SELECT ts, price FROM trades WHERE ts IN '2024-01-01' AND sym = 'a' ORDER BY ts",
                    "SELECT * FROM (SELECT sym, max(price) - min(price) spread, max(price) top FROM trades WHERE qty > 5 GROUP BY sym) WHERE top > 1",
            };
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (String query : queries) {
                    try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(query, factory);
                        Assert.assertTrue(query, verifier.verify(compiler.getPlanForTesting(), PASS));
                    }
                }
            }
        });
    }

    @Test
    public void testColumnType() {
        final FunctionSourcePlan source = source();
        assertFails(filter(source, column(1, ColumnType.BOOLEAN)), PlanVerifier.COLUMN_TYPE, "FilterPlan");
    }

    @Test
    public void testDependentStepSurvivesDecorrelation() {
        final JoinPlan join = join(source(), source(5, 6));
        join.getInputs().getQuick(1).setDependent(true);
        Assert.assertTrue(verifier.verifyBound(join));
        assertFails(join, PlanVerifier.DEPENDENT_STEP, "JoinPlan");
    }

    @Test
    public void testDuplicateColumnId() {
        final FunctionSourcePlan source = source();
        source.getOutput().add(1, "again", ColumnType.LONG, true);
        assertFails(source, PlanVerifier.DUPLICATE_COLUMN_ID, "FunctionSourcePlan");
    }

    @Test
    public void testExpressionShared() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE shared_node (a INT, b INT)");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile("SELECT a FROM shared_node WHERE a = b", sqlExecutionContext).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                LogicalPlan node = plan;
                while (!(node instanceof FilterPlan)) {
                    node = node.inputAt(0);
                }
                final FunctionExpression predicate = (FunctionExpression) ((FilterPlan) node).getPredicate();
                Assert.assertTrue(verifier.verify(plan, PASS));
                predicate.getArguments().setQuick(1, predicate.argumentAt(0));
                assertFails(plan, PlanVerifier.EXPRESSION_SHARED, "FilterPlan");
            }
        });
    }

    @Test
    public void testExpressionSharedCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE shared_cursor (a INT, ts TIMESTAMP) TIMESTAMP(ts)");
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    RecordCursorFactory ignored = compiler.compile(
                            "SELECT a FROM shared_cursor WHERE a > 0 AND ts > (SELECT min(ts) FROM shared_cursor)",
                            sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final LogicalPlan plan = compiler.getPlanForTesting();
                LogicalPlan node = plan;
                while (!(node instanceof FilterPlan)) {
                    node = node.inputAt(0);
                }
                final FunctionExpression predicate = (FunctionExpression) ((FilterPlan) node).getPredicate();
                final FunctionExpression comparison = (FunctionExpression) predicate.argumentAt(1);
                Assert.assertTrue(comparison.argumentAt(1) instanceof CursorExpression);
                comparison.getArguments().setQuick(0, comparison.argumentAt(1));
                Assert.assertTrue(verifier.verify(plan, PASS));
            }
        });
    }

    @Test
    public void testJoinKeys() {
        final JoinPlan join = join(source(), source(5, 6));
        join.getInputs().getQuick(1).getMasterKeyColumnIds().add(1);
        assertFails(join, PlanVerifier.JOIN_KEYS, "JoinPlan");
    }

    @Test
    public void testJoinOrder() {
        final JoinPlan join = join(source(), source(5, 6));
        join.getOrderedInputs().add(join.getInputs().getQuick(0));
        assertFails(join, PlanVerifier.JOIN_ORDER, "JoinPlan");
    }

    @Test
    public void testJoinOutput() {
        final JoinPlan join = join(source(), source(5, 6));
        join.getOutput().remove(0);
        assertFails(join, PlanVerifier.JOIN_OUTPUT, "JoinPlan");
    }

    @Test
    public void testLimitBound() {
        final FunctionSourcePlan source = source();
        final LimitPlan limit = LimitPlan.FACTORY.newInstance().of(source, column(1, ColumnType.INT), null, 3);
        limit.getOutput().copyFrom(source.getOutput());
        assertFails(limit, PlanVerifier.OPERAND_COLUMN, "LimitPlan");
    }

    @Test
    public void testNodeShared() {
        final FunctionSourcePlan source = source();
        final SetOperationPlan union = SetOperationPlan.FACTORY.newInstance().of(source, source, SetOperationKind.UNION_ALL, 0, 0, false);
        union.getOutput().copyFrom(source.getOutput());
        assertFails(union, PlanVerifier.NODE_SHARED, "FunctionSourcePlan");
    }

    @Test
    public void testOuterColumnResolvesInPrecedingInput() {
        final FunctionSourcePlan master = source();
        final FunctionSourcePlan slave = source(5, 6);
        final JoinPlan join = join(master, filter(slave, OuterColumnExpression.FACTORY.newInstance().of(2, ColumnType.BOOLEAN, 0)));
        join.getInputs().getQuick(1).setDependent(true);
        Assert.assertTrue(verifier.verifyBound(join));
        join.getInputs().getQuick(1).setDependent(false);
        assertFails(join, PlanVerifier.OUTER_COLUMN_SCOPE, "FilterPlan");
    }

    @Test
    public void testOuterColumnUnresolved() {
        final JoinPlan join = join(source(), filter(source(5, 6), OuterColumnExpression.FACTORY.newInstance().of(9, ColumnType.BOOLEAN, 0)));
        join.getInputs().getQuick(1).setDependent(true);
        try {
            verifier.verifyBound(join);
            Assert.fail();
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), PlanVerifier.OUTER_COLUMN_UNRESOLVED);
            TestUtils.assertContains(e.getMessage(), "after SqlBinder.bind");
        }
    }

    @Test
    public void testFillTimestamp() {
        final FunctionSourcePlan source = source();
        source.getOutput().add(3, "ts", ColumnType.TIMESTAMP, true);
        final FillPlan fill = FillPlan.FACTORY.newInstance().of(source, 5);
        fill.setTimestampColumnId(3);
        fill.getOutput().copyFrom(source.getOutput());
        assertFails(fill, PlanVerifier.FILL_TIMESTAMP, "FillPlan");
        fill.deriveOutput();
        Assert.assertEquals(2, fill.getOutput().getTimestampIndex());
        Assert.assertTrue(verifier.verify(fill, PASS));
    }

    @Test
    public void testOutputForwarding() {
        final FunctionSourcePlan source = source();
        final FilterPlan filter = filter(source, column(2, ColumnType.BOOLEAN));
        filter.getOutput().remove(0);
        assertFails(filter, PlanVerifier.OUTPUT_FORWARDING, "FilterPlan");
    }

    @Test
    public void testOutputForwardingAttributes() {
        final FunctionSourcePlan source = source();
        final FilterPlan filter = filter(source, column(2, ColumnType.BOOLEAN));
        filter.getOutput().setColumnName(0, "renamed", null);
        assertFails(filter, PlanVerifier.OUTPUT_FORWARDING + " [column id 1]", "FilterPlan");
        filter.deriveOutput();
        filter.getOutput().setSymbolTableStatic(1, true);
        assertFails(filter, PlanVerifier.OUTPUT_FORWARDING + " [column id 2]", "FilterPlan");
        filter.deriveOutput();
        filter.getOutput().protectName(1);
        assertFails(filter, PlanVerifier.OUTPUT_FORWARDING + " [column id 2]", "FilterPlan");
        filter.deriveOutput();
        Assert.assertTrue(verifier.verify(filter, PASS));
    }

    @Test
    public void testPredicateType() {
        final FunctionSourcePlan source = source();
        assertFails(filter(source, column(1, ColumnType.INT)), PlanVerifier.PREDICATE_TYPE, "FilterPlan");
    }

    @Test
    public void testProjectShape() {
        final FunctionSourcePlan source = source();
        final ProjectPlan project = ProjectPlan.FACTORY.newInstance().of(source, 4);
        project.getExpressions().add(column(1, ColumnType.INT));
        assertFails(project, PlanVerifier.PROJECT_SHAPE, "ProjectPlan");
    }

    @Test
    public void testProjectType() {
        final FunctionSourcePlan source = source();
        final ProjectPlan project = ProjectPlan.FACTORY.newInstance().of(source, 4);
        project.getExpressions().add(column(1, ColumnType.INT));
        project.getOutput().add(3, "a", ColumnType.LONG, true);
        assertFails(project, PlanVerifier.PROJECT_TYPE, "ProjectPlan");
    }

    @Test
    public void testReplaceInputDerivesOutput() {
        final FilterPlan filter = filter(source(), column(2, ColumnType.BOOLEAN));
        final FunctionSourcePlan replacement = source(5, 2);
        replacement.getOutput().add(3, "ts", ColumnType.TIMESTAMP, true);
        replacement.getOutput().setTimestampIndex(2);
        filter.replaceInput(0, replacement);
        Assert.assertEquals(3, filter.getOutput().getColumnCount());
        Assert.assertEquals(5, filter.getOutput().getColumnId(0));
        Assert.assertEquals(2, filter.getOutput().getTimestampIndex());
        Assert.assertTrue(verifier.verify(filter, PASS));
        final SortPlan sort = SortPlan.FACTORY.newInstance().of(source(), 4);
        sort.getColumnIds().add(3);
        sort.getDirections().add(SortDirection.DESCENDING);
        sort.replaceInput(0, filter);
        Assert.assertEquals(2, sort.getOutput().getTimestampIndex());
        Assert.assertTrue(verifier.verify(sort, PASS));
        sort.getColumnIds().setQuick(0, 5);
        sort.replaceInput(0, filter);
        Assert.assertEquals(-1, sort.getOutput().getTimestampIndex());
        Assert.assertTrue(verifier.verify(sort, PASS));
    }

    @Test
    public void testSetOperationShape() {
        final FunctionSourcePlan left = source();
        final FunctionSourcePlan right = source(5, 6);
        right.getOutput().remove(1);
        final SetOperationPlan union = SetOperationPlan.FACTORY.newInstance().of(left, right, SetOperationKind.UNION_ALL, 0, 0, false);
        union.getOutput().copyFrom(left.getOutput());
        assertFails(union, PlanVerifier.SET_OPERATION_SHAPE, "SetOperationPlan");
    }

    @Test
    public void testSortKeys() {
        final FunctionSourcePlan source = source();
        final SortPlan sort = SortPlan.FACTORY.newInstance().of(source, 2);
        sort.getOutput().copyFrom(source.getOutput());
        assertFails(sort, PlanVerifier.SORT_KEYS, "SortPlan");
    }

    @Test
    public void testSortTimestamp() {
        final FunctionSourcePlan source = source();
        source.getOutput().add(3, "ts", ColumnType.TIMESTAMP, true);
        final SortPlan sort = SortPlan.FACTORY.newInstance().of(source, 2);
        sort.getColumnIds().add(3);
        sort.getDirections().add(SortDirection.ASCENDING);
        sort.getOutput().copyFrom(source.getOutput());
        assertFails(sort, PlanVerifier.SORT_TIMESTAMP, "SortPlan");
        sort.deriveOutput();
        Assert.assertTrue(verifier.verify(sort, PASS));
    }

    @Test
    public void testTimestampType() {
        final FunctionSourcePlan source = source();
        source.getOutput().setTimestampIndex(0);
        assertFails(source, PlanVerifier.TIMESTAMP_TYPE, "FunctionSourcePlan");
    }

    @Test
    public void testUnresolvedColumn() {
        final FunctionSourcePlan source = source();
        assertFails(filter(source, column(9, ColumnType.BOOLEAN)), PlanVerifier.UNRESOLVED_COLUMN + " [column id 9]", "FilterPlan");
    }

    @Test
    public void testValidPlanPasses() {
        final FunctionSourcePlan source = source();
        Assert.assertTrue(verifier.verify(filter(source, column(2, ColumnType.BOOLEAN)), PASS));
    }

    @Test
    public void testWindowShape() {
        final FunctionSourcePlan source = source();
        final WindowPlan window = WindowPlan.FACTORY.newInstance().of(source, 0);
        window.getOutput().copyFrom(source.getOutput());
        window.getFunctionColumnIds().add(8);
        assertFails(window, PlanVerifier.WINDOW_SHAPE, "WindowPlan");
    }

    private static ColumnExpression column(int columnId, int type) {
        return ColumnExpression.FACTORY.newInstance().of(columnId, type, 0);
    }

    private static FilterPlan filter(LogicalPlan input, BoundExpression predicate) {
        final FilterPlan filter = FilterPlan.FACTORY.newInstance().of(input, predicate, 1);
        filter.getOutput().copyFrom(input.getOutput());
        return filter;
    }

    private static JoinPlan join(LogicalPlan master, LogicalPlan slave) {
        final JoinPlan join = JoinPlan.FACTORY.newInstance().of(0);
        join.getInputs().add(JoinInput.FACTORY.newInstance().of(master, JoinKind.CROSS, "m", 0));
        join.getInputs().add(JoinInput.FACTORY.newInstance().of(slave, JoinKind.INNER, "s", 0));
        final OutputSchema output = join.getOutput();
        for (int i = 0; i < 2; i++) {
            final OutputSchema source = join.getInputs().getQuick(i).getSourceOutput();
            for (int k = 0, n = source.getColumnCount(); k < n; k++) {
                output.add(source.getColumnId(k), source.getColumnName(k), source.getColumnType(k), true);
            }
        }
        return join;
    }

    /**
     * A leaf with an INT column {@code a} and a BOOLEAN column {@code b} under the given ids (1 and 2 by default).
     */
    private static FunctionSourcePlan source(int... columnIds) {
        final FunctionSourcePlan source = FunctionSourcePlan.FACTORY.newInstance().of(0);
        source.getOutput().add(columnIds.length > 0 ? columnIds[0] : 1, "a", ColumnType.INT, true);
        source.getOutput().add(columnIds.length > 1 ? columnIds[1] : 2, "b", ColumnType.BOOLEAN, true);
        return source;
    }

    private void assertFails(LogicalPlan plan, String invariant, String nodeName) {
        try {
            verifier.verify(plan, PASS);
            Assert.fail("expected " + invariant);
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), invariant);
            TestUtils.assertContains(e.getMessage(), " at " + nodeName + " position ");
            TestUtils.assertContains(e.getMessage(), "after " + PASS);
        }
    }
}
