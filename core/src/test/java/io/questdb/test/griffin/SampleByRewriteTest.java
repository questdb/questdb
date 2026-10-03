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
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SampleByRewriteTest extends AbstractCairoTest {
    @Test
    public void testHiddenBucketSurvivesCountProjection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertPrunedSampleBy("SELECT count() n FROM lp_sample_prune SAMPLE BY 1h FILL(NONE) ALIGN TO CALENDAR",
                    "n\n2\n1\n2\n");
        });
    }

    @Test
    public void testFirstObservationFilterPreservesAnchorAndPreviousFill() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertPrunedSampleBy("SELECT count() n FROM lp_sample_prune SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION",
                    "n\n2\n1\n1\n2\n");
            assertPrunedSampleBy("SELECT ts,n FROM (SELECT ts,count() n FROM lp_sample_prune"
                            + " SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION)"
                            + " WHERE ts>='2024-01-01T00:30:00.000000Z' ORDER BY ts",
                    "ts\tn\n2024-01-01T01:10:00.000000Z\t1\n2024-01-01T02:10:00.000000Z\t1\n2024-01-01T03:10:00.000000Z\t2\n");
        });
    }

    @Test
    public void testFillValuesKeepDistinctAggregateOccurrences() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertPrunedSampleBy("SELECT ts,sum(v) a,sum(v) b FROM lp_sample_prune SAMPLE BY 1h FILL(11,22) ALIGN TO FIRST OBSERVATION",
                    "ts\ta\tb\n2024-01-01T00:10:00.000000Z\t30\t30\n2024-01-01T01:10:00.000000Z\t30\t30\n"
                            + "2024-01-01T02:10:00.000000Z\t11\t22\n2024-01-01T03:10:00.000000Z\t90\t90\n");
            assertPrunedSampleBy("SELECT n FROM (SELECT ts,v%2 parity,count() n FROM lp_sample_prune"
                    + " SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION)", "n\n2\n1\n1\n2\n");
        });
    }

    @Test
    public void testFirstObservationInsertSelectKeepsTimestampAndFill() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String table = "lp_sample_first_insert";
            execute("CREATE TABLE " + table + " (ts TIMESTAMP,n LONG) TIMESTAMP(ts)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO " + table + " SELECT ts,count() FROM lp_sample_prune"
                        + " SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION");
                Assert.assertTrue(assertPrunedBuckets(compiler.getPlanForTesting()) > 0);
            }
            assertQuery("SELECT * FROM " + table + " ORDER BY ts").timestamp("ts").expectSize().returns(
                    "ts\tn\n2024-01-01T00:10:00.000000Z\t2\n2024-01-01T01:10:00.000000Z\t1\n"
                            + "2024-01-01T02:10:00.000000Z\t1\n2024-01-01T03:10:00.000000Z\t2\n");
        });
    }

    @Test
    public void testOuterFiltersCannotChangeBucketMembership() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertPrunedSampleBy("SELECT n FROM (SELECT ts,count() n FROM lp_sample_prune SAMPLE BY 1h FILL(NONE) ALIGN TO CALENDAR)"
                    + " WHERE ts>='2024-01-01T00:30:00.000000Z' ORDER BY ts", "n\n1\n2\n");
            assertPrunedSampleBy("SELECT ts,n FROM (SELECT ts,count() n FROM lp_sample_prune SAMPLE BY 1h FILL(NONE) ALIGN TO CALENDAR)"
                            + " WHERE n=2 ORDER BY ts",
                    "ts\tn\n2024-01-01T00:00:00.000000Z\t2\n2024-01-01T03:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testInsertSelectKeepsSampledTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String table = "lp_sample_insert";
            execute("CREATE TABLE " + table + " (ts TIMESTAMP,n LONG) TIMESTAMP(ts)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO " + table + " SELECT ts,count() FROM lp_sample_prune SAMPLE BY 1h FILL(NONE) ALIGN TO CALENDAR");
                Assert.assertTrue(assertPrunedBuckets(compiler.getPlanForTesting()) > 0);
            }
            assertQuery("SELECT * FROM " + table + " ORDER BY ts").timestamp("ts").expectSize().returns(
                    "ts\tn\n2024-01-01T00:00:00.000000Z\t2\n2024-01-01T01:00:00.000000Z\t1\n2024-01-01T03:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testRepeatedCteConsumersKeepSeparateFiltersAndColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (String sampling : new String[]{"FILL(NONE) ALIGN TO CALENDAR", "FILL(PREV) ALIGN TO FIRST OBSERVATION"}) {
                assertPrunedSampleBy("WITH q AS (SELECT ts,count() n,sum(v) total FROM lp_sample_prune SAMPLE BY 1h " + sampling + ")"
                        + " SELECT value FROM (SELECT total value FROM q WHERE n=2"
                        + " UNION ALL SELECT n value FROM q WHERE total>=90) ORDER BY value", "value\n2\n30\n90\n");
            }
        });
    }

    private static int assertPrunedBuckets(LogicalPlan plan) {
        Assert.assertNotNull(plan);
        int aggregates = 0;
        if (plan instanceof GroupingPlan aggregate) {
            if (aggregate instanceof AggregatePlan) {
                Assert.assertTrue(aggregate.getGroupingExpressions().size() > 0);
            }
            Assert.assertEquals(aggregate.getGroupingExpressions().size() + aggregate.getAggregates().size(),
                    aggregate.getOutput().getColumnCount());
            if (aggregate instanceof SampleByPlan sample) {
                Assert.assertTrue(aggregate.getInput().getOutput().getColumnIndexById(sample.getTimestampColumnId()) >= 0);
            }
            aggregates++;
        } else if (plan instanceof ScanPlan) {
            Assert.assertEquals(-1, plan.getOutput().getColumnIndexQuiet("unused"));
            Assert.assertTrue(plan.getOutput().getColumnIndexQuiet("ts") >= 0);
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            aggregates += assertPrunedBuckets(plan.inputAt(i));
        }
        return aggregates;
    }

    private void assertPrunedSampleBy(String sql, String expected) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertTrue(assertPrunedBuckets(compiler.getPlanForTesting()) > 0);
            assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_sample_prune(unused INT,v INT,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_sample_prune VALUES(1,10,'2024-01-01T00:10:00Z'),(2,20,'2024-01-01T00:40:00Z'),"
                + "(3,30,'2024-01-01T01:10:00Z'),(4,40,'2024-01-01T03:10:00Z'),(5,50,'2024-01-01T03:30:00Z')");
    }
}
