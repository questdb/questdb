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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalUnnestOptimiserTest extends AbstractCairoTest {
    @Test
    public void testDependentOccurrencesAreNotPlanInputEdges() {
        final ScanPlan master = scan("master", 1);
        final ScanPlan slave = scan("slave", 2);
        final UnnestSpec first = new UnnestSpec().of(false, false);
        final UnnestSpec second = new UnnestSpec().of(false, false);
        final JoinInput masterInput = new JoinInput().of(master, QueryModel.JOIN_CROSS, "m", 0);
        final JoinInput slaveInput = new JoinInput().of(slave, QueryModel.JOIN_INNER, "s", 30);
        final JoinInput firstInput = new JoinInput().ofUnnest(first, "u", 10);
        final JoinInput secondInput = new JoinInput().ofUnnest(second, "v", 40);
        final JoinPlan join = join(new ObjList<>(masterInput, firstInput, slaveInput, secondInput));
        Assert.assertEquals(2, join.inputCount());
        Assert.assertSame(master, join.inputAt(0));
        Assert.assertSame(slave, join.inputAt(1));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> join.inputAt(-1));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> join.inputAt(2));
        final ScanPlan replacement = scan("replacement", 3);
        join.replaceInput(1, replacement);
        Assert.assertSame(replacement, slaveInput.getInput());
        Assert.assertSame(replacement, join.getOrderedInputs().getQuick(2).getInput());
        Assert.assertNull(firstInput.getInput());
        Assert.assertNull(secondInput.getInput());
        Assert.assertSame(first.getOutput(), firstInput.getSourceOutput());
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> join.replaceInput(2, slave));
    }

    @Test
    public void testEqualityUsesSourceOrdinalsAcrossDependentOccurrence() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT m.id,u.value,s.v FROM lp_un_m m,UNNEST(m.a) u JOIN lp_un_s s ON s.v=u.value ORDER BY m.id";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                final JoinPlan join = findJoin(compiler.getLogicalPlanForTesting());
                final JoinInput dependent = input(join, "u");
                final JoinInput ordinary = input(join, "s");
                Assert.assertTrue(join.getOrderedInputs().indexOf(dependent) < join.getOrderedInputs().indexOf(ordinary));
                Assert.assertEquals(1, ordinary.getMasterKeyColumnIds().size());
                Assert.assertEquals(dependent.getUnnest().getOutput().getColumnId(0), ordinary.getMasterKeyColumnIds().getQuick(0));
                Assert.assertEquals(ordinary.getSourceOutput().getColumnId(0), ordinary.getSlaveKeyColumnIds().getQuick(0));
            }
            assertQuery(sql).noLeakCheck().returns("""
                    id\tvalue\tv
                    1\t2.0\t2.0
                    2\t3.0\t3.0
                    """);
        });
    }

    @Test
    public void testEveryReferencedSourcePrecedesDependentOccurrence() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT m.id,u.value1,u.value2 FROM lp_un_m m CROSS JOIN lp_un_s s CROSS JOIN lp_un_c c,UNNEST(m.a,c.c) u";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                final JoinPlan join = findJoin(compiler.getLogicalPlanForTesting());
                final JoinInput dependent = input(join, "u");
                Assert.assertEquals(4, join.getOrderedInputs().size());
                final int unnestPosition = join.getOrderedInputs().indexOf(dependent);
                Assert.assertTrue(join.getOrderedInputs().indexOf(input(join, "m")) < unnestPosition);
                Assert.assertTrue(join.getOrderedInputs().indexOf(input(join, "c")) < unnestPosition);
                Assert.assertEquals(3, join.inputCount());
            }
        });
    }

    @Test
    public void testMasterArgumentsSurvivePruningAndKeepPredicateStaysAtOccurrence() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT u.value FROM lp_un_m m,UNNEST(m.a,m.b) u(value,k) WHERE u.k>0";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                final JoinPlan join = findJoin(compiler.getLogicalPlanForTesting());
                final JoinInput master = input(join, "m");
                final JoinInput dependent = input(join, "u");
                final ScanPlan scan = (ScanPlan) master.getInput();
                Assert.assertEquals(2, scan.getOutput().getColumnCount());
                Assert.assertEquals(-1, scan.getOutput().getColumnIndexQuiet("id"));
                Assert.assertNotNull(dependent.getPostJoinFilter());
                Assert.assertNull(dependent.getInput());
                Assert.assertSame(scan, join.inputAt(0));
                Assert.assertEquals(1, join.inputCount());
            }
            assertQuery(sql).noLeakCheck().noRandomAccess().returns("""
                    value
                    1.0
                    3.0
                    """);
        });
    }

    @Test
    public void testOrdinaryJoinKeepsInputEdgesAndSourceFilterPushdown() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory ignored = compiler.compile(
                         "SELECT m.id,s.v FROM lp_un_m m CROSS JOIN lp_un_s s WHERE m.id>1", sqlExecutionContext
                 ).getRecordCursorFactory()) {
                final JoinPlan join = findJoin(compiler.getLogicalPlanForTesting());
                final JoinInput left = input(join, "m");
                final JoinInput right = input(join, "s");
                Assert.assertEquals(2, join.inputCount());
                Assert.assertSame(LogicalPlan.Type.FILTER, left.getInput().getType());
                Assert.assertSame(LogicalPlan.Type.SCAN, left.getInput().inputAt(0).getType());
                Assert.assertSame(right.getInput(), join.inputAt(1));
                Assert.assertSame(LogicalPlan.Type.SCAN, right.getInput().getType());
                Assert.assertNull(right.getPostJoinFilter());
            }
        });
    }

    @Test
    public void testParentOutputPredicateDoesNotPushIntoDependentNullInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT u.value FROM lp_un_m m,UNNEST(m.a) u,lp_un_s s WHERE u.value>2.0 AND s.f";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                final JoinPlan join = findJoin(compiler.getLogicalPlanForTesting());
                final JoinInput dependent = input(join, "u");
                final JoinInput ordinary = input(join, "s");
                Assert.assertNotNull(dependent.getPostJoinFilter());
                Assert.assertNull(dependent.getInput());
                Assert.assertSame(LogicalPlan.Type.FILTER, ordinary.getInput().getType());
                Assert.assertSame(LogicalPlan.Type.SCAN, ordinary.getInput().inputAt(0).getType());
                Assert.assertNull(ordinary.getPostJoinFilter());
            }
            assertQuery(sql).noLeakCheck().noRandomAccess().returns("""
                    value
                    3.0
                    3.0
                    """);
        });
    }

    @Test
    public void testStandaloneOutputDoesNotExposeSyntheticMaster() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "SELECT * FROM UNNEST(ARRAY[1.0,2.0]) WITH ORDINALITY u(value,ord)";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                final JoinPlan join = findJoin(compiler.getLogicalPlanForTesting());
                final JoinInput dependent = input(join, "u");
                final OutputSchema unnestOutput = dependent.getUnnest().getOutput();
                Assert.assertEquals(2, join.getOutput().getColumnCount());
                Assert.assertEquals(unnestOutput.getColumnId(0), join.getOutput().getColumnId(0));
                Assert.assertEquals(unnestOutput.getColumnId(1), join.getOutput().getColumnId(1));
                Assert.assertEquals(-1, join.getOutput().getColumnIndexById(join.inputAt(0).getOutput().getColumnId(0)));
                Assert.assertEquals(1, join.inputCount());
                Assert.assertNull(dependent.getInput());
            }
            assertQuery(sql).noLeakCheck().noRandomAccess().returns("""
                    value\tord
                    1.0\t1
                    2.0\t2
                    """);
        });
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE lp_un_m(id INT,a DOUBLE[],b DOUBLE[])");
        execute("INSERT INTO lp_un_m VALUES(1,ARRAY[1.0,2.0],ARRAY[1.0,-1.0]),(2,ARRAY[3.0],ARRAY[1.0])");
        execute("CREATE TABLE lp_un_s(v DOUBLE,f BOOLEAN)");
        execute("INSERT INTO lp_un_s VALUES(2.0,true),(3.0,true),(4.0,false)");
        execute("CREATE TABLE lp_un_c(c DOUBLE[])");
        execute("INSERT INTO lp_un_c VALUES(ARRAY[5.0])");
    }

    private static JoinPlan findJoin(LogicalPlan plan) {
        if (plan instanceof JoinPlan join) {
            return join;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final JoinPlan join = findJoin(plan.inputAt(i));
            if (join != null) {
                return join;
            }
        }
        return null;
    }

    private static JoinInput input(JoinPlan join, String alias) {
        final ObjList<JoinInput> inputs = join.getInputs();
        for (int i = 0, n = inputs.size(); i < n; i++) {
            if (alias.contentEquals(inputs.getQuick(i).getBindingAlias())) {
                return inputs.getQuick(i);
            }
        }
        throw new AssertionError("missing join input: " + alias);
    }

    private static JoinPlan join(ObjList<JoinInput> inputs) {
        final JoinPlan join = new JoinPlan().of(0);
        join.getInputs().addAll(inputs);
        join.getOrderedInputs().addAll(inputs);
        for (int i = 0; i < inputs.size(); i++) {
            final JoinInput input = inputs.getQuick(i);
            final OutputSchema prefix = input.getOutput();
            prefix.copyFrom(join.getOutput());
            final OutputSchema source = input.getSourceOutput();
            for (int k = 0; k < source.getColumnCount(); k++) {
                prefix.add(source.getColumnId(k), source.getColumnName(k), source.getColumnType(k), true);
            }
            join.getOutput().copyFrom(prefix);
        }
        return join;
    }

    private static ScanPlan scan(String name, int id) {
        return new ScanPlan().of(new TableToken(name, name, null, id, false, false, false), 0, 0);
    }
}
