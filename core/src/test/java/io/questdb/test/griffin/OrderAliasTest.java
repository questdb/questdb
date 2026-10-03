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
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class OrderAliasTest extends AbstractCairoTest {
    @Test
    public void testAliasExpressionsKeepSourcePrecedenceAndHiddenDependencies() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows("SELECT d+1.0 v FROM oa_u ORDER BY v+1.0", "v\n2.0\n2.0\n3.0\n4.0\n");
            assertRows("SELECT d+1.0 v FROM oa_t ORDER BY v+1.0", "v\n4.0\n3.0\n2.0\n2.0\n");
            assertRows("SELECT d+1.0 v FROM oa_t ORDER BY v", "v\n2.0\n2.0\n3.0\n4.0\n");
            assertRows("SELECT d+1.0 v FROM oa_t ORDER BY oa_t.v+1.0", "v\n4.0\n3.0\n2.0\n2.0\n");
            assertRows("SELECT d+1.0 v FROM oa_u ORDER BY v-id", "v\n2.0\n3.0\n2.0\n4.0\n");
            assertRows("SELECT d+1.0 v FROM oa_u ORDER BY id+1.0,v+1.0", "v\n2.0\n4.0\n3.0\n2.0\n");
            assertRows("SELECT d+1.0 v,id FROM oa_u ORDER BY v+1.0,oa_u.id", "v\tid\n2.0\t1\n2.0\t4\n3.0\t3\n4.0\t2\n");
        });
    }

    @Test
    public void testDistinctOrderReadsOnlySelectedColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT DISTINCT d+1.0 v FROM oa_t ORDER BY v+1.0").noLeakCheck().fails(43, "ORDER BY expressions must appear in select list. Invalid column: v");
            assertQuery("SELECT DISTINCT d v FROM oa_t ORDER BY v+1.0").noLeakCheck().fails(39, "ORDER BY expressions must appear in select list. Invalid column: v");
            assertQuery("SELECT DISTINCT d AS x FROM oa_t ORDER BY v+1.0").noLeakCheck().fails(42, "ORDER BY expressions must appear in select list. Invalid column: v");
            assertQuery("SELECT DISTINCT d+1.0 v, max(id) OVER () m FROM oa_t ORDER BY v+1.0").noLeakCheck().fails(62, "ORDER BY expressions must appear in select list. Invalid column: v");
            assertQuery("SELECT DISTINCT d+1.0 v, max(id) OVER () m FROM oa_u ORDER BY id+1.0").noLeakCheck().fails(62, "ORDER BY expressions must appear in select list. Invalid column: id");
            assertRows("SELECT DISTINCT d+1.0 v, max(id) OVER () m FROM oa_u ORDER BY v+1.0", "v\tm\n2.0\t4\n3.0\t4\n4.0\t4\n");
            assertRows("SELECT DISTINCT d+1.0 v FROM oa_u ORDER BY v+1.0", "v\n2.0\n3.0\n4.0\n");
            assertRows("SELECT DISTINCT d+1.0 v FROM oa_u ORDER BY abs(v) DESC", "v\n4.0\n3.0\n2.0\n");
            assertQuery("SELECT DISTINCT d+1.0 v FROM oa_u ORDER BY v-id").noLeakCheck().fails(45, "ORDER BY expressions must appear in select list. Invalid column: id");
        });
    }

    @Test
    public void testSelectedAndRepeatedOrderingExpressionsReuseValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows("SELECT d+1.0 v FROM oa_u ORDER BY d+1.0 DESC,d+1.0 ASC", "v\n4.0\n3.0\n2.0\n2.0\n");
            assertRows("SELECT d+1.0 v FROM oa_u ORDER BY v+1.0 ASC,v+1.0 DESC", "v\n2.0\n2.0\n3.0\n4.0\n");
            final boolean wasMemoizationEnabled = SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION;
            SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = true;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertPlan("SELECT d+1.0 v FROM oa_u ORDER BY v+1.0", """
                        SelectedRecord
                            Encode sort light
                              keys: [column]
                                VirtualRecord
                                  functions: [memoize(v),v+1.0]
                                    VirtualRecord
                                      functions: [memoize(d+1.0)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: oa_u
                        """);
                assertPlan("SELECT d+1.0 v FROM oa_u ORDER BY d+1.0", """
                        Encode sort light
                          keys: [v]
                            VirtualRecord
                              functions: [memoize(d+1.0)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: oa_u
                        """);
                try (RecordCursorFactory factory = compiler.compile("SELECT d+1.0 v FROM oa_u ORDER BY v+v", sqlExecutionContext).getRecordCursorFactory()) {
                    final TextPlanSink plan = new TextPlanSink();
                    plan.of(factory, sqlExecutionContext);
                    final String text = plan.getSink().toString();
                    TestUtils.assertContains(text, "memoize(d+1.0)");
                    Assert.assertEquals(text.indexOf("d+1.0"), text.lastIndexOf("d+1.0"));
                    assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                            .returns("v\n2.0\n2.0\n3.0\n4.0\n");
                }
            } finally {
                SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = wasMemoizationEnabled;
            }
        });
    }

    @Test
    public void testSetOrderUsesOnlyCombinedOutputScope() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows("SELECT d+1.0 v FROM oa_u UNION ALL SELECT d+2.0 w FROM oa_u ORDER BY v+1.0",
                    "v\n2.0\n2.0\n3.0\n3.0\n3.0\n4.0\n4.0\n5.0\n");
            assertQuery("SELECT d+1.0 v FROM oa_u UNION ALL SELECT d+2.0 w FROM oa_u ORDER BY w+1.0").noLeakCheck().fails(69, "Invalid column: w");
            assertQuery("SELECT d+1.0 v FROM oa_u UNION ALL SELECT d+2.0 w FROM oa_u ORDER BY d+1.0").noLeakCheck().fails(69, "Invalid column: d");
        });
    }

    @Test
    public void testRejectedScopesAndDiagnosticPositions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT d+1.0 v FROM oa_u ORDER BY oa_u.v+1.0").noLeakCheck().fails(34, "Invalid column: oa_u.v");
            assertQuery("SELECT d+1.0 v FROM oa_u ORDER BY missing+1.0").noLeakCheck().fails(34, "Invalid column: missing");
            assertQuery("SELECT d+1.0 v FROM oa_u ORDER BY v+1.0,id").noLeakCheck().fails(34, "Invalid column: v");
            assertQuery("SELECT d+1.0 v FROM oa_u ORDER BY id,v+1.0").noLeakCheck().fails(37, "Invalid column: v");
            assertQuery("SELECT d+1.0 v FROM oa_u ORDER BY v+1.0,missing").noLeakCheck().fails(40, "Invalid column: missing");
            assertQuery("SELECT sum(d) v FROM oa_u ORDER BY v+1.0").noLeakCheck().fails(35, "Invalid column: v");
            assertQuery("SELECT id,sum(d) v FROM oa_u GROUP BY id ORDER BY v+1.0").noLeakCheck().fails(50, "Invalid column: v");
            assertQuery("SELECT l.d+1.0 d FROM oa_u l JOIN oa_u r ON l.id=r.id ORDER BY d+1.0").noLeakCheck().fails(63, "Ambiguous column [name=d]");
        });
    }

    @Test
    public void testOrderByColumnReadingEarlierAlias() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows("SELECT v, v2 FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2)", "v\tv2\n2.0\t2.0\n2.0\t2.0\n3.0\t3.0\n4.0\t4.0\n");
            assertRows("SELECT v FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2 DESC)", "v\n4.0\n3.0\n2.0\n2.0\n");
            assertRows("SELECT v FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2) WHERE v > 2.0 LIMIT 1", "v\n3.0\n");
            assertRows("SELECT lag(v) OVER () prev FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2)", "prev\nnull\n2.0\n2.0\n3.0\n");
            assertRows("SELECT row_number() OVER () rn, sum(v) OVER () total FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2)",
                    "rn\ttotal\n1\t11.0\n2\t11.0\n3\t11.0\n4\t11.0\n");
            assertRows("SELECT count() FROM (SELECT r, lag(r) OVER () prev FROM (SELECT rnd_int(1, 1_000_000, 0) r, r r2 FROM long_sequence(20) ORDER BY r2)) WHERE prev > r",
                    "count\n0\n");
        });
    }

    @Test
    public void testOrderingProjectionPreservesTimestampForTemporalConsumers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE oa_ts(d DOUBLE,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO oa_ts VALUES(3,'2020-01-01T00:00:01'),(1,'2020-01-01T00:00:02'),(2,'2020-01-01T00:00:03')");
            final String sql = "SELECT ts,d+1.0 v FROM oa_ts ORDER BY ts,v+1.0 LIMIT 2";
            assertRows(sql, "ts\tv\n2020-01-01T00:00:01.000000Z\t4.0\n2020-01-01T00:00:02.000000Z\t2.0\n");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    LogicalPlan plan = compiler.getPlanForTesting();
                    while (!(plan instanceof SortPlan)) {
                        plan = plan.inputAt(0);
                    }
                    Assert.assertEquals(0, plan.inputAt(0).getOutput().getTimestampIndex());
                    Assert.assertEquals(0, factory.getMetadata().getTimestampIndex());
                }
            }
            assertRows("SELECT a.ts,a.v,b.d FROM (SELECT ts,d+1.0 v FROM oa_ts ORDER BY ts,v+1.0 LIMIT 2) a TIMESTAMP(ts) ASOF JOIN oa_ts b",
                    "ts\tv\td\n2020-01-01T00:00:01.000000Z\t4.0\t3.0\n2020-01-01T00:00:02.000000Z\t2.0\t1.0\n");
        });
    }

    @Test
    public void testRetainedAliasOrderingFactoryRebindsAfterCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setDouble(0, 1.0);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT d+$1 v FROM oa_u ORDER BY v-id", sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory other = compiler.compile("SELECT d FROM oa_u", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(other);
                    }
                    compiler.clear();
                }
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("v\n2.0\n3.0\n2.0\n4.0\n");
                bindVariableService.setDouble(0, 10.0);
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("v\n11.0\n12.0\n11.0\n13.0\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void assertPlan(String sql, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                final TextPlanSink plan = new TextPlanSink();
                plan.of(factory, sqlExecutionContext);
                final StringBuilder actual = new StringBuilder();
                for (int i = 1; i <= plan.getLineCount(); i++) {
                    actual.append(plan.getLine(i)).append('\n');
                }
                TestUtils.assertEquals(expected, actual);
            }
        }
    }

    private void assertRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE oa_t(d DOUBLE,v DOUBLE,id INT)");
        execute("INSERT INTO oa_t VALUES(1,30,1),(3,10,2),(2,20,3),(1,40,4)");
        execute("CREATE TABLE oa_u(d DOUBLE,id INT)");
        execute("INSERT INTO oa_u VALUES(1,1),(3,2),(2,3),(1,4)");
    }
}
