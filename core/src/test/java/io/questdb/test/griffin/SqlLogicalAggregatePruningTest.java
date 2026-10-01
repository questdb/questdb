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
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalAggregatePruningTest extends AbstractCairoTest {
    @Test
    public void testUnusedAggregatesReleaseInputColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertPruned("SELECT total FROM (SELECT sum(v) total,avg(unused) ignored FROM lp_agg_prune)",
                    "total\n60\n", 1, 0, null);
            assertPruned("SELECT total FROM (SELECT g,sum(v) total,avg(unused) ignored FROM lp_agg_prune) ORDER BY total",
                    "total\n30\n30\n", 1, 1, null);
        });
    }

    @Test
    public void testPruningEveryAggregatePreservesGlobalAndGroupedCardinality() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertPruned("SELECT 7 value FROM (SELECT sum(v) total FROM lp_agg_prune)",
                    "value\n7\n", 0, 0, null);
            assertPruned("SELECT 7 value FROM (SELECT sum(v) total FROM lp_agg_prune WHERE v<0)",
                    "value\n7\n", 0, 0, null);
            assertPruned("SELECT 7 value FROM (SELECT g,sum(v) total FROM lp_agg_prune)",
                    "value\n7\n7\n", 0, 1, null);
            assertPruned("SELECT 7 value FROM (SELECT g,sum(v) total FROM lp_agg_prune WHERE v<0)",
                    "value\n", 0, 1, null);
        });
    }

    @Test
    public void testOrderingDependsOnRetainedAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertPruned("SELECT total FROM (SELECT sum(v) total,last(unused) ignored"
                            + " FROM (SELECT * FROM lp_agg_prune ORDER BY unused DESC))",
                    "total\n60\n", 1, 0, null);
            assertPruned("SELECT first_value FROM (SELECT first(v) first_value,avg(unused) ignored"
                            + " FROM (SELECT * FROM lp_agg_prune ORDER BY v DESC))",
                    "first_value\n30\n", 1, 0, null);
        });
    }

    @Test
    public void testCalendarFillRetainsOriginalTargetValueAfterPruning() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertPruned("SELECT b FROM (SELECT ts,sum(v) a,sum(v) b FROM lp_agg_prune"
                            + " SAMPLE BY 1h FILL(11,22) ALIGN TO CALENDAR) ORDER BY ts",
                    "b\n30\n22\n30\n", 1, 1, """
                    SelectedRecord
                        Sample By Fill
                          stride: '1h'
                          fill: value
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('1h',ts)]
                                  values: [sum(v)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_agg_prune
                    """);
            assertPruned("SELECT a,b FROM (SELECT ts,sum(v) a,sum(v) b,count() ignored FROM lp_agg_prune"
                            + " SAMPLE BY 1h FILL(PREV,PREV(a),0) ALIGN TO CALENDAR) ORDER BY ts",
                    "a\tb\n30\t30\n30\t30\n30\t30\n", 2, 1, null);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT b FROM (SELECT ts,sum(v) a,max(CAST(v AS LONG)) b,count() ignored"
                        + " FROM lp_agg_prune SAMPLE BY 1h FILL(PREV,PREV(a),0) ALIGN TO CALENDAR) ORDER BY ts",
                        sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(1, assertRetainedAggregates(compiler.getLogicalPlanForTesting(), 2, 1));
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp()
                            .sizeMayVary().returns("b\n20\n30\n30\n");
                }
            }
            assertPruned("SELECT 7 value FROM (SELECT ts,sum(v) total FROM lp_agg_prune"
                            + " SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR)",
                    "value\n7\n7\n7\n", 0, 1, null);
        });
    }

    private static int assertRetainedAggregates(LogicalPlan plan, int count, int keys) {
        int found = 0;
        if (plan.getType() == LogicalPlan.Type.AGGREGATE) {
            final AggregatePlan aggregate = (AggregatePlan) plan;
            Assert.assertEquals(count, aggregate.getAggregates().size());
            Assert.assertEquals(keys, aggregate.getGroupingExpressions().size());
            Assert.assertEquals(keys + count, aggregate.getOutput().getColumnCount());
            found++;
        } else if (plan.getType() == LogicalPlan.Type.SCAN) {
            Assert.assertEquals(-1, plan.getOutput().getColumnIndexQuiet("unused"));
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            found += assertRetainedAggregates(plan.inputAt(i), count, keys);
        }
        return found;
    }

    private void assertPruned(String sql, String expected, int count, int keys, String plan) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertEquals(sql, 1, assertRetainedAggregates(compiler.getLogicalPlanForTesting(), count, keys));
            if (plan != null) {
                final TextPlanSink sink = new TextPlanSink();
                sink.of(factory, sqlExecutionContext);
                final StringSink actual = new StringSink();
                for (int i = 1; i <= sink.getLineCount(); i++) {
                    actual.put(sink.getLine(i)).put('\n');
                }
                TestUtils.assertEquals(plan, actual);
            }
            assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_agg_prune(unused INT,g INT,v INT,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_agg_prune VALUES(100,1,10,'2024-01-01T00:10:00Z'),"
                + "(200,1,20,'2024-01-01T00:40:00Z'),(300,2,30,'2024-01-01T02:10:00Z')");
    }
}
