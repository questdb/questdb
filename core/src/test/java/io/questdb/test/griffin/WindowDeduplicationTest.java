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
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class WindowDeduplicationTest extends AbstractCairoTest {
    @Test
    public void testEqualCallsOfNestedAndTopLevelWindowsComputeOnce() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id, sum(v) OVER (ORDER BY id) a, sum(sum(v) OVER (ORDER BY id)) OVER (ORDER BY id) b FROM lp_window_prune")
                    .expectSize()
                    .withPlan("""
                            CachedWindowLight
                              orderedFunctions: [[id] => [sum(sum) over (rows between unbounded preceding and current row)]]
                                CachedWindowLight
                                  orderedFunctions: [[id] => [sum(v) over (rows between unbounded preceding and current row)]]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            id\ta\tb
                            3\t60.0\t100.0
                            1\t10.0\t10.0
                            2\t30.0\t40.0
                            4\t100.0\t200.0
                            """);
        });
    }

    @Test
    public void testEqualCallsOrderedByEqualAliasesComputeOnce() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id + 1 a, id + 1 b, sum(v) OVER (ORDER BY a) s1, sum(v) OVER (ORDER BY b) s2 FROM lp_window_prune")
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                CachedWindowLight
                                  orderedFunctions: [[a] => [sum(v) over (rows between unbounded preceding and current row)]]
                                    VirtualRecord
                                      functions: [id+1,id+1,v]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            a\tb\ts1\ts2
                            4\t4\t60.0\t60.0
                            2\t2\t10.0\t10.0
                            3\t3\t30.0\t30.0
                            5\t5\t100.0\t100.0
                            """);
        });
    }

    @Test
    public void testEqualWindowCallsComputeOnce() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id, sum(v) OVER (PARTITION BY grp) a, sum(v) OVER (PARTITION BY grp) b, sum(lp_window_prune.v) OVER (PARTITION BY grp) c FROM lp_window_prune")
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                CachedWindowLight
                                  unorderedFunctions: [sum(v) over (partition by [grp])]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            id\ta\tb\tc
                            3\t40.0\t40.0\t40.0
                            1\t40.0\t40.0\t40.0
                            2\t60.0\t60.0\t60.0
                            4\t60.0\t60.0\t60.0
                            """);
        });
    }

    @Test
    public void testEqualWindowCallsMergeUnderFilter() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT * FROM (SELECT id, row_number() OVER (ORDER BY v) a, row_number() OVER (ORDER BY v) b FROM lp_window_prune) WHERE b > 2")
                    .withPlan("""
                            SelectedRecord
                                Filter filter: 2<a
                                    CachedWindowLight
                                      orderedFunctions: [[v] => [row_number()]]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            id\ta\tb
                            3\t3\t3
                            4\t4\t4
                            """);
        });
    }

    @Test
    public void testFilterCannotChangeWindowPartition() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryBothPaths("SELECT id,rn FROM (SELECT id,row_number() OVER (ORDER BY id) rn FROM lp_window_prune) WHERE id>=3 ORDER BY id",
                    "id\trn\n3\t3\n4\t4\n");
        });
    }

    @Test
    public void testHiddenPartitionAndOrderColumnsSurvivePruning() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryBothPaths("SELECT rn FROM (SELECT id,row_number() OVER (PARTITION BY grp ORDER BY v DESC) rn FROM lp_window_prune) ORDER BY rn",
                    "rn\n1\n1\n2\n2\n");
        });
    }

    @Test
    public void testInputOrderIsPreservedThroughWindowBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryBothPaths("SELECT id,prev FROM (SELECT id,lag(id) OVER () prev FROM (SELECT id FROM lp_window_prune ORDER BY id DESC)) WHERE id<4",
                    "id\tprev\n3\t4\n2\t3\n1\t2\n");
        });
    }

    @Test
    public void testInsertSelectUsesWindowSource() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_window_insert (id INT,rn LONG)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO lp_window_insert SELECT id,row_number() OVER (ORDER BY id) FROM lp_window_prune");
                Assert.assertNotNull(compiler.getPlanForTesting());
                assertWindowLayouts(compiler.getPlanForTesting());
            }
            assertQuery("SELECT * FROM lp_window_insert ORDER BY id").expectSize().returns("id\trn\n1\t1\n2\t2\n3\t3\n4\t4\n");
        });
    }

    @Test
    public void testNestedWindowsKeepSeparateInputLayouts() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryBothPaths("SELECT id,rn,lag(rn) OVER (ORDER BY id) prev FROM (SELECT id,row_number() OVER (PARTITION BY grp ORDER BY v) rn FROM lp_window_prune) ORDER BY id",
                    "id\trn\tprev\n1\t1\tnull\n2\t1\t1\n3\t2\t1\n4\t2\t2\n");
        });
    }

    @Test
    public void testVolatileWindowCallsDoNotMerge() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT a = b eq, count() FROM (SELECT first_value(rnd_int(1, 1_000_000, 0)) OVER (ORDER BY id ROWS CURRENT ROW) a, " +
                    "first_value(rnd_int(1, 1_000_000, 0)) OVER (ORDER BY id ROWS CURRENT ROW) b FROM lp_window_prune)")
                    .expectSize()
                    .withPlan("""
                            GroupBy vectorized: false
                              keys: [eq]
                              values: [count(*)]
                                CachedWindowLight
                                  orderedFunctions: [[id] => [first_value(rnd_int(1,1000000,0)) over (),first_value(rnd_int(1,1000000,0)) over ()]]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_window_prune
                            """)
                    .returns("""
                            eq\tcount
                            false\t4
                            """);
        });
    }

    private static void assertWindowLayouts(LogicalPlan plan) {
        if (plan instanceof WindowPlan) {
            final WindowPlan window = (WindowPlan) plan;
            final OutputSchema input = window.getInput().getOutput();
            final OutputSchema output = window.getOutput();
            Assert.assertEquals(input.getColumnCount() + window.getFunctions().size(), output.getColumnCount());
            for (int i = 0, n = input.getColumnCount(); i < n; i++) {
                Assert.assertEquals(input.getColumnId(i), output.getColumnId(i));
            }
            for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
                Assert.assertEquals(window.getFunctionColumnIds().getQuick(i), output.getColumnId(input.getColumnCount() + i));
            }
            Assert.assertEquals(-1, input.getColumnIndexQuiet("unused"));
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            assertWindowLayouts(plan.inputAt(i));
        }
    }

    private void assertQueryBothPaths(String sql, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
            }
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertWindowLayouts(compiler.getPlanForTesting());
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
            }
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_window_prune (unused INT,id INT,grp SYMBOL,v DOUBLE)");
        execute("INSERT INTO lp_window_prune VALUES (30,3,'a',30),(10,1,'a',10),(20,2,'b',20),(40,4,'b',40)");
    }
}
