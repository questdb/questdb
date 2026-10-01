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

public class SqlLogicalWindowOptimiserTest extends AbstractCairoTest {
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
                Assert.assertNotNull(compiler.getLogicalPlanForTesting());
                assertWindowLayouts(compiler.getLogicalPlanForTesting());
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

    private static void assertWindowLayouts(LogicalPlan plan) {
        if (plan.getType() == LogicalPlan.Type.WINDOW) {
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
                assertWindowLayouts(compiler.getLogicalPlanForTesting());
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
            }
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_window_prune (unused INT,id INT,grp SYMBOL,v DOUBLE)");
        execute("INSERT INTO lp_window_prune VALUES (30,3,'a',30),(10,1,'a',10),(20,2,'b',20),(40,4,'b',40)");
    }
}
