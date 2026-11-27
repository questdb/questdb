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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.codegen.SqlCodeGenerator;
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
    // An alias keeps its name when an earlier expression reads a source column of that name, as in PostgreSQL. A column a
    // joined query leaves unaliased takes the next free name when another column of that name is an argument of an earlier
    // expression, which PostgreSQL does not do.
    @Test
    public void testAliasExpressionsKeepSourcePrecedenceAndHiddenDependencies() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT d+1.0 v FROM oa_u ORDER BY v+1.0", "v\n2.0\n2.0\n3.0\n4.0\n");
            assertRowsOnly("SELECT d+1.0 v FROM oa_t ORDER BY v+1.0", "v\n4.0\n3.0\n2.0\n2.0\n");
            assertRowsOnly("SELECT d+1.0 v FROM oa_t ORDER BY v", "v\n2.0\n2.0\n3.0\n4.0\n");
            assertRowsOnly("SELECT d+1.0 v FROM oa_t ORDER BY oa_t.v+1.0", "v\n4.0\n3.0\n2.0\n2.0\n");
            assertRowsOnly("SELECT d+1.0 v FROM oa_u ORDER BY v-id", "v\n2.0\n3.0\n2.0\n4.0\n");
            assertRowsOnly("SELECT d+1.0 v FROM oa_u ORDER BY id+1.0,v+1.0", "v\n2.0\n4.0\n3.0\n2.0\n");
            assertRowsOnly("SELECT d+1.0 v,id FROM oa_u ORDER BY v+1.0,oa_u.id", "v\tid\n2.0\t1\n2.0\t4\n3.0\t3\n4.0\t2\n");
        });
    }

    @Test
    public void testAliasOfAnotherColumnDuplicateFails() throws Exception {
        assertMemoryLeak(() -> {
            createCollidingTables();
            assertException("SELECT v1 + 1 x, v1 AS x FROM w", 20, "Duplicate column [name=x]");
            assertException("SELECT t.v1 + 1 x, t.v2 AS v1, t.v1 AS v1 FROM w t CROSS JOIN u", 36, "Duplicate column [name=v1]");
        });
    }

    @Test
    public void testAliasOfAnotherColumnKeepsItsName() throws Exception {
        assertMemoryLeak(() -> {
            createCollidingTables();
            final String[][] cases = {
                    {"SELECT v1 + 1 x, v2 AS v1 FROM w", "x,v1"},
                    {"SELECT abs(v1), v2 AS v1 FROM w", "abs,v1"},
                    {"SELECT v1 + 1 AS v2, v2 AS x FROM w", "v2,x"},
                    {"SELECT v2 * 2 v, avg(v) OVER (), v1 FROM w", "v,avg,v1"},
                    {"SELECT v2 * 2 v, avg(v) OVER (), v2 AS v1 FROM w", "v,avg,v1"},
                    {"SELECT v1 + 1 x, v2 AS v1, avg(v2) OVER () FROM w", "x,v1,avg"},
                    {"SELECT ts, sum(v1) s, v2 AS v1 FROM w SAMPLE BY 1h", "ts,s,v1"},
                    {"SELECT ts, sum(v1) s, v2 AS v1 FROM w GROUP BY ts, v2", "ts,s,v1"},
                    {"SELECT t.v1 + 1 x, t.v2 AS v1 FROM w t CROSS JOIN u", "x,v1"},
                    {"SELECT t.v1 + 1 x, u.v1 AS v1 FROM w t CROSS JOIN u", "x,v1"},
                    {"SELECT t.v1 + 1 x, t.v2 AS v1, u.v1 FROM w t CROSS JOIN u", "x,v1,v11"},
                    {"SELECT t.v1 + 1 x, t.v2 AS y, u.y FROM w t CROSS JOIN u", "x,y,y1"},
            };
            for (String[] c : cases) {
                assertColumnNames(c[0], c[1]);
            }
        });
    }

    @Test
    public void testAliasOfAnotherColumnReadsTheColumnItNames() throws Exception {
        assertMemoryLeak(() -> {
            createCollidingTables();
            assertQuery("SELECT v1 + 1 x, v2 AS v1 FROM w").noLeakCheck().expectSize().returns("""
                    x\tv1
                    101.0\t1.0
                    201.0\t3.0
                    """);
            assertQuery("SELECT t.v1 + 1 x, t.v2 AS v1 FROM w t CROSS JOIN u").noLeakCheck().noRandomAccess().expectSize().returns("""
                    x\tv1
                    101.0\t1.0
                    201.0\t3.0
                    """);
            assertQuery("SELECT t.v1 + 1 x, u.v1 AS v1 FROM w t CROSS JOIN u").noLeakCheck().noRandomAccess().expectSize().returns("""
                    x\tv1
                    101.0\t7.0
                    201.0\t7.0
                    """);
            assertSqlCursors("SELECT t.v1 + 1 x, t.v2 FROM w t CROSS JOIN u", "SELECT x, v1 v2 FROM (SELECT t.v1 + 1 x, t.v2 AS v1 FROM w t CROSS JOIN u)");
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
            assertRowsOnly("SELECT DISTINCT d+1.0 v, max(id) OVER () m FROM oa_u ORDER BY v+1.0", "v\tm\n2.0\t4\n3.0\t4\n4.0\t4\n");
            assertRowsOnly("SELECT DISTINCT d+1.0 v FROM oa_u ORDER BY v+1.0", "v\n2.0\n3.0\n4.0\n");
            assertRowsOnly("SELECT DISTINCT d+1.0 v FROM oa_u ORDER BY abs(v) DESC", "v\n4.0\n3.0\n2.0\n");
            assertQuery("SELECT DISTINCT d+1.0 v FROM oa_u ORDER BY v-id").noLeakCheck().fails(45, "ORDER BY expressions must appear in select list. Invalid column: id");
        });
    }

    @Test
    public void testOrderByColumnReadingEarlierAlias() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT v, v2 FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2)", "v\tv2\n2.0\t2.0\n2.0\t2.0\n3.0\t3.0\n4.0\t4.0\n");
            assertRowsOnly("SELECT v FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2 DESC)", "v\n4.0\n3.0\n2.0\n2.0\n");
            assertRowsOnly("SELECT v FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2) WHERE v > 2.0 LIMIT 1", "v\n3.0\n");
            assertRowsOnly("SELECT lag(v) OVER () prev FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2)", "prev\nnull\n2.0\n2.0\n3.0\n");
            assertRowsOnly("SELECT row_number() OVER () rn, sum(v) OVER () total FROM (SELECT d + 1.0 v, v v2 FROM oa_u ORDER BY v2)",
                    "rn\ttotal\n1\t11.0\n2\t11.0\n3\t11.0\n4\t11.0\n");
            assertRowsOnly("SELECT count() FROM (SELECT r, lag(r) OVER () prev FROM (SELECT rnd_int(1, 1_000_000, 0) r, r r2 FROM long_sequence(20) ORDER BY r2)) WHERE prev > r",
                    "count\n0\n");
        });
    }

    @Test
    public void testOrderingProjectionPreservesTimestampForTemporalConsumers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE oa_ts(d DOUBLE,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO oa_ts VALUES(3,'2020-01-01T00:00:01'),(1,'2020-01-01T00:00:02'),(2,'2020-01-01T00:00:03')");
            final String sql = "SELECT ts,d+1.0 v FROM oa_ts ORDER BY ts,v+1.0 LIMIT 2";
            assertRowsOnly(sql, "ts\tv\n2020-01-01T00:00:01.000000Z\t4.0\n2020-01-01T00:00:02.000000Z\t2.0\n");
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
            assertRowsOnly("SELECT a.ts,a.v,b.d FROM (SELECT ts,d+1.0 v FROM oa_ts ORDER BY ts,v+1.0 LIMIT 2) a TIMESTAMP(ts) ASOF JOIN oa_ts b",
                    "ts\tv\td\n2020-01-01T00:00:01.000000Z\t4.0\t3.0\n2020-01-01T00:00:02.000000Z\t2.0\t1.0\n");
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

    @Test
    public void testSelectedAndRepeatedOrderingExpressionsReuseValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT d+1.0 v FROM oa_u ORDER BY d+1.0 DESC,d+1.0 ASC", "v\n4.0\n3.0\n2.0\n2.0\n");
            assertRowsOnly("SELECT d+1.0 v FROM oa_u ORDER BY v+1.0 ASC,v+1.0 DESC", "v\n2.0\n2.0\n3.0\n4.0\n");
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
            assertRowsOnly("SELECT d+1.0 v FROM oa_u UNION ALL SELECT d+2.0 w FROM oa_u ORDER BY v+1.0",
                    "v\n2.0\n2.0\n3.0\n3.0\n3.0\n4.0\n4.0\n5.0\n");
            assertQuery("SELECT d+1.0 v FROM oa_u UNION ALL SELECT d+2.0 w FROM oa_u ORDER BY w+1.0").noLeakCheck().fails(69, "Invalid column: w");
            assertQuery("SELECT d+1.0 v FROM oa_u UNION ALL SELECT d+2.0 w FROM oa_u ORDER BY d+1.0").noLeakCheck().fails(69, "Invalid column: d");
        });
    }

    @Test
    public void testUnaliasedColumnOfJoinTakesTheNextName() throws Exception {
        assertMemoryLeak(() -> {
            createCollidingTables();
            assertColumnNames("SELECT t.v1 + 1 x, u.v1 FROM w t CROSS JOIN u", "x,v11");
            assertColumnNames("SELECT t.v1 + 1 x, t.v1 FROM w t CROSS JOIN u", "x,v1");
            assertColumnNames("SELECT t.v1, u.v1 FROM w t CROSS JOIN u", "v1,v11");
        });
    }

    @Test
    public void testViewPublishesAliasOfAnotherColumn() throws Exception {
        assertMemoryLeak(() -> {
            createCollidingTables();
            execute("CREATE VIEW iv AS (SELECT v1 + 1 x, v2 AS v1 FROM w)");
            execute("CREATE VIEW ov AS (SELECT v1 FROM iv)");
            drainWalAndViewQueues();
            assertColumnNames("iv", "x,v1");
            assertQuery("SELECT v1 FROM ov").noLeakCheck().expectSize().returns("""
                    v1
                    1.0
                    3.0
                    """);
            assertExceptionNoLeakCheck("SELECT v11 FROM iv", 7, "Invalid column: v11");
        });
    }

    private static void assertColumnNames(String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final RecordMetadata metadata = factory.getMetadata();
            final StringBuilder names = new StringBuilder();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                names.append(i > 0 ? "," : "").append(metadata.getColumnName(i));
            }
            Assert.assertEquals(sql, expected, names.toString());
        }
    }

    private static void createCollidingTables() throws Exception {
        execute("CREATE TABLE w (v1 DOUBLE, v2 DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO w VALUES (100, 1, '2024-01-01T00:00:00Z'), (200, 3, '2024-01-01T01:00:00Z')");
        execute("CREATE TABLE u (v1 DOUBLE, y DOUBLE)");
        execute("INSERT INTO u VALUES (7, 8)");
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

    private void createRows() throws Exception {
        execute("CREATE TABLE oa_t(d DOUBLE,v DOUBLE,id INT)");
        execute("INSERT INTO oa_t VALUES(1,30,1),(3,10,2),(2,20,3),(1,40,4)");
        execute("CREATE TABLE oa_u(d DOUBLE,id INT)");
        execute("INSERT INTO oa_u VALUES(1,1),(3,2),(2,3),(1,4)");
    }
}
