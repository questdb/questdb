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
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalFillOptimiserTest extends AbstractCairoTest {
    @Test
    public void testOuterFilterObservesFilledRows() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertLogical("SELECT ts,n FROM (SELECT ts,count() n FROM lp_fill_prune"
                            + " SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR) WHERE n IS NULL ORDER BY ts",
                    "ts\tn\n2024-01-01T01:00:00.000000Z\tnull\n");
            assertLogical("SELECT ts,n FROM (SELECT ts,count() n FROM lp_fill_prune"
                            + " SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR)"
                            + " WHERE ts>='2024-01-01T00:30:00.000000Z' ORDER BY ts",
                    "ts\tn\n2024-01-01T01:00:00.000000Z\t2\n2024-01-01T02:00:00.000000Z\t1\n"
                            + "2024-01-01T03:00:00.000000Z\t1\n");
        });
    }

    @Test
    public void testHiddenKeyAndPreviousSourceSurvivePruning() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String hiddenSource = "SELECT ts,b FROM (SELECT ts,g,sum(v) a,count() b FROM lp_fill_prune"
                    + " SAMPLE BY 1h FILL(PREV,PREV(a)) ALIGN TO CALENDAR) WHERE g=1 AND b>1 ORDER BY ts";
            assertLogical(hiddenSource, "ts\tb\n2024-01-01T01:00:00.000000Z\t10\n");
            final String visibleSource = "SELECT ts,a,b FROM (SELECT ts,g,sum(v) a,count() b FROM lp_fill_prune"
                    + " SAMPLE BY 1h FILL(PREV,PREV(a)) ALIGN TO CALENDAR) WHERE g=1 AND b>1 ORDER BY ts";
            assertLogical(visibleSource, "ts\ta\tb\n2024-01-01T01:00:00.000000Z\t10\t10\n");
            assertLogical("SELECT b FROM (SELECT ts,sum(v) a,sum(v) b FROM lp_fill_prune"
                            + " SAMPLE BY 1h FILL(11,22) ALIGN TO CALENDAR) ORDER BY ts",
                    "b\n30\n22\n30\n40\n");
        });
    }

    @Test
    public void testLimitAndSharedConsumersPreserveFillBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertLogical("SELECT ts,n FROM (SELECT ts,count() n FROM lp_fill_prune"
                            + " SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR) ORDER BY ts DESC LIMIT 2",
                    "ts\tn\n2024-01-01T03:00:00.000000Z\t1\n2024-01-01T02:00:00.000000Z\t1\n");
            assertLogical("WITH q AS (SELECT ts,sum(v) a,count() n FROM lp_fill_prune"
                            + " SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR)"
                            + " SELECT value FROM (SELECT a value FROM q WHERE n=2"
                            + " UNION ALL SELECT n value FROM q WHERE a>=40) ORDER BY value",
                    "value\n1\n30\n30\n");
        });
    }

    @Test
    public void testInsertSelectKeepsFilledTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String table = "lp_filled_insert";
            execute("CREATE TABLE " + table + " (ts TIMESTAMP,n LONG) TIMESTAMP(ts)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO " + table + " SELECT ts,count() FROM lp_fill_prune"
                        + " SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR");
                Assert.assertTrue(assertFillDependencies(compiler.getLogicalPlanForTesting()) > 0);
            }
            assertQuery("SELECT * FROM " + table + " ORDER BY ts").timestamp("ts").expectSize().returns(
                    "ts\tn\n2024-01-01T00:00:00.000000Z\t2\n2024-01-01T01:00:00.000000Z\t2\n"
                            + "2024-01-01T02:00:00.000000Z\t1\n2024-01-01T03:00:00.000000Z\t1\n");
        });
    }

    private static int assertFillDependencies(LogicalPlan plan) {
        Assert.assertNotNull(plan);
        int fills = 0;
        if (plan.getType() == LogicalPlan.Type.FILL) {
            final FillPlan fill = (FillPlan) plan;
            Assert.assertEquals(LogicalPlan.Type.AGGREGATE, fill.getInput().getType());
            Assert.assertEquals(fill.getTimestampColumnId(), fill.getOutput().getTimestampColumnId());
            Assert.assertEquals(fill.getInput().getOutput().getColumnCount(), fill.getOutput().getColumnCount());
            for (int i = 0, n = fill.getTargetColumnIds().size(); i < n; i++) {
                Assert.assertTrue(fill.getInput().getOutput().getColumnIndexById(fill.getTargetColumnIds().getQuick(i)) >= 0);
                if (fill.getModes().getQuick(i) == FillPlan.FILL_PREV_COLUMN) {
                    Assert.assertTrue(fill.getInput().getOutput().getColumnIndexById(fill.getSourceColumnIds().getQuick(i)) >= 0);
                }
            }
            fills++;
        } else if (plan.getType() == LogicalPlan.Type.SCAN) {
            Assert.assertEquals(-1, plan.getOutput().getColumnIndexQuiet("unused"));
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            fills += assertFillDependencies(plan.inputAt(i));
        }
        return fills;
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_fill_prune(unused INT,g INT,v INT,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_fill_prune VALUES(1,1,10,'2024-01-01T00:10:00Z'),(2,2,20,'2024-01-01T00:40:00Z'),"
                + "(3,1,30,'2024-01-01T02:10:00Z'),(4,2,40,'2024-01-01T03:10:00Z')");
    }

    private void assertLogical(String sql, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertTrue(assertFillDependencies(compiler.getLogicalPlanForTesting()) > 0);
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
            }
        }
    }
}
