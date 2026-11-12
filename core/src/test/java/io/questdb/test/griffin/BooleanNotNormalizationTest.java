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

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.ComparisonFunction;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BooleanNotNormalizationTest extends AbstractCairoTest {
    @Test
    public void testComparisonComplementsAndDeMorganPreserveNulls() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNot("SELECT id FROM lp_not WHERE NOT(v<=0 OR v>1) ORDER BY id", "id\n3\n4\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(v>0) ORDER BY id", "id\n1\n2\n4\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(NOT(v=1)) ORDER BY id", "id\n3\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(v<>1) ORDER BY id", "id\n3\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(v=1 OR b) ORDER BY id", "id\n1\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(NOT(b)) ORDER BY id", "id\n2\n4\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(1=2) ORDER BY id", "id\n1\n2\n3\n4\n", null);
            assertNot("SELECT id,NOT(v>0) n FROM lp_not ORDER BY id", "id\tn\n1\ttrue\n2\ttrue\n3\tfalse\n4\ttrue\n", null);
        });
    }

    @Test
    public void testNegatedComparisonsKeepNullRowsInEveryClause() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM lp_not WHERE NOT (NOT (v < 1))").noLeakCheck().returns("id\n1\n2\n");
            assertQuery("SELECT id FROM lp_not WHERE NOT (v < 1 AND id > 1)").noLeakCheck().returns("id\n1\n3\n4\n");
            assertQuery("SELECT id FROM lp_not WHERE NOT (v < 1 OR id < 2)").noLeakCheck().returns("id\n3\n4\n");
            assertQuery("SELECT id FROM lp_not WHERE CASE WHEN NOT (v > 0) THEN true ELSE false END")
                    .noLeakCheck()
                    .returns("id\n1\n2\n4\n");
            assertQuery("SELECT id FROM (SELECT id, v FROM lp_not) WHERE NOT (v > 0)").noLeakCheck().returns("id\n1\n2\n4\n");
            assertQuery("SELECT a.id FROM lp_not a JOIN lp_not b ON a.id = b.id AND NOT (a.v > 0) ORDER BY a.id")
                    .noLeakCheck()
                    .returns("id\n1\n2\n4\n");
            assertQuery("SELECT a.id, b.id bid FROM lp_not a LEFT JOIN lp_not b ON a.id = b.id AND NOT (b.v > 0) ORDER BY a.id")
                    .noLeakCheck()
                    .returns("""
                            id\tbid
                            1\t1
                            2\t2
                            3\tnull
                            4\t4
                            """);
            assertQuery("SELECT * FROM (SELECT s, max(v) m FROM lp_not GROUP BY s) WHERE NOT (m > 0) ORDER BY s")
                    .noLeakCheck()
                    .returns("s\tm\n\tnull\nB\t0\n");
            assertQuery("SELECT id FROM (SELECT id, lag(v) OVER (ORDER BY id) pv FROM lp_not) WHERE NOT (pv < 1)")
                    .noLeakCheck()
                    .returns("id\n1\n4\n");
            assertQuery("SELECT id, ts FROM lp_not WHERE NOT (v > 0) LATEST ON ts PARTITION BY s")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-02T00:00:00.000000Z
                            4\t2020-01-04T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNegatedComparisonFoldsIntoTheComparison() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int previousJitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = select("SELECT id FROM lp_not WHERE NOT (v > 0)")) {
                final Function filter = factory.getBaseFactory().getFilter();
                Assert.assertTrue(String.valueOf(filter), filter instanceof ComparisonFunction);
                assertRowsOnly(factory, "id\n1\n2\n4\n");
            } finally {
                sqlExecutionContext.setJitMode(previousJitMode);
            }
            assertQuery("SELECT id, NOT (v > 0) n, NOT (NOT (v > 0)) nn FROM lp_not")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("not (0<v)", "0<v")
                    .returns("""
                            id\tn\tnn
                            1\ttrue\tfalse
                            2\ttrue\tfalse
                            3\tfalse\ttrue
                            4\ttrue\tfalse
                            """);
            assertQuery("SELECT id, v > 0 p, NOT p np FROM lp_not")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tp\tnp
                            1\tfalse\ttrue
                            2\tfalse\ttrue
                            3\ttrue\tfalse
                            4\tfalse\ttrue
                            """);
            assertQuery("SELECT id, p FROM (SELECT id, v > 0 p FROM lp_not) WHERE NOT p")
                    .noLeakCheck()
                    .returns("""
                            id\tp
                            1\tfalse
                            2\tfalse
                            4\tfalse
                            """);
        });
    }

    @Test
    public void testNegatedOrderingComparisonsKeepNullRowsOfEveryType() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE lp_not_types (
                        id INT, i INT, l LONG, f FLOAT, d DOUBLE, dt DATE, t TIMESTAMP, tn TIMESTAMP_NS, ip IPV4,
                        u UUID, s STRING, v VARCHAR, sy SYMBOL, dc DECIMAL(10,2), c CHAR, l256 LONG256
                    )""");
            execute("""
                    INSERT INTO lp_not_types VALUES
                        (1, 1, 1, 1.0, 1.0, '2024-01-01', '2024-01-01', '2024-01-01', '1.1.1.1',
                         '11111111-1111-1111-1111-111111111111', 'a', 'a', 'a', 1::DECIMAL(10,2), 'a', '0x01'),
                        (2, 2, 2, 2.0, 2.0, '2024-01-02', '2024-01-02', '2024-01-02', '1.1.1.2',
                         '22222222-2222-2222-2222-222222222222', 'b', 'b', 'b', 2::DECIMAL(10,2), 'b', '0x02'),
                        (3, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
                    """);
            final String[][] columns = {
                    {"i", "1", "2"},
                    {"l", "1", "2"},
                    {"f", "1.0", "2.0"},
                    {"d", "1.0", "2.0"},
                    {"dt", "'2024-01-01'::DATE", "'2024-01-02'::DATE"},
                    {"t", "'2024-01-01'", "'2024-01-02'"},
                    {"tn", "'2024-01-01'", "'2024-01-02'"},
                    {"ip", "'1.1.1.1'::IPV4", "'1.1.1.2'::IPV4"},
                    {"u", "'11111111-1111-1111-1111-111111111111'::UUID", "'22222222-2222-2222-2222-222222222222'::UUID"},
                    {"s", "'a'", "'b'"},
                    {"v", "'a'", "'b'"},
                    {"sy", "'a'", "'b'"},
                    {"dc", "1::DECIMAL(10,2)", "2::DECIMAL(10,2)"},
                    {"c", "'a'", "'b'"},
                    {"l256", "'0x01'::LONG256", "'0x02'::LONG256"}
            };
            for (String[] column : columns) {
                final String name = column[0];
                final String low = column[1];
                final String high = column[2];
                assertNegatedComparison(name + " < " + high, "id\n2\n3\n");
                assertNegatedComparison(high + " > " + name, "id\n2\n3\n");
                assertNegatedComparison(name + " <= " + low, "id\n2\n3\n");
                assertNegatedComparison(low + " >= " + name, "id\n2\n3\n");
                assertNegatedComparison(name + " > " + low, "id\n1\n3\n");
                assertNegatedComparison(low + " < " + name, "id\n1\n3\n");
                assertNegatedComparison(name + " >= " + high, "id\n1\n3\n");
                assertNegatedComparison(high + " <= " + name, "id\n1\n3\n");
                assertNegatedComparison(name + " < NULL", "id\n1\n2\n3\n");
                assertNegatedComparison("NULL > " + name, "id\n1\n2\n3\n");
            }
        });
    }

    @Test
    public void testNormalizedKeysReachJoinAnalysisButOuterOnKeepsItsScope() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNot("SELECT a.id FROM lp_not a CROSS JOIN lp_not b WHERE NOT(a.id<>b.id) ORDER BY a.id",
                    "id\n1\n2\n3\n4\n", "Hash Join Light");
            assertNot("SELECT a.id FROM lp_not a CROSS JOIN lp_not b CROSS JOIN lp_not c "
                            + "WHERE NOT(a.id<>b.id OR b.id<>c.id) ORDER BY a.id",
                    "id\n1\n2\n3\n4\n", "Hash Join Light");
            assertNot("SELECT a.id,b.id bid FROM lp_not a LEFT JOIN lp_not b ON NOT(a.id<>b.id) ORDER BY a.id",
                    "id\tbid\n1\t1\n2\t2\n3\t3\n4\t4\n", "Nested Loop Left Join");
        });
    }

    @Test
    public void testNormalizedPredicatesKeepIndexAndIntervalPlans() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNot("SELECT id FROM lp_not WHERE NOT(s!='A')", "id\n1\n3\n", "Index forward scan");
            assertNot("SELECT id FROM lp_not WHERE NOT(s!='A' OR v<=0)", "id\n3\n", "Index forward scan");
            assertNot("SELECT id FROM lp_not WHERE NOT(ts<'2020-01-02' OR ts>='2020-01-04')",
                    "id\n2\n3\n", "Interval forward scan");
            assertNot("SELECT id FROM lp_not WHERE NOT(ts<'2020-01-03')", "id\n3\n4\n", "Interval forward scan");
            assertNot("SELECT id FROM lp_not WHERE NOT(s!='A') AND NOT(ts<'2020-01-02')", "id\n3\n", "Index forward scan");
            assertNot("SELECT id FROM lp_not WHERE NOT(ts<'2020-01-02') LATEST ON ts PARTITION BY s",
                    "id\n2\n3\n4\n", "Interval backward scan");
        });
    }

    @Test
    public void testNormalizedPredicateFactorySurvivesReuseAndParameterRebind() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 0);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT id FROM lp_not WHERE NOT(v<=$1 OR b)", sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_not WHERE NOT(s!='A')", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    compiler.clear();
                }
                assertRowsOnly(retained, "id\n3\n");
                bindVariableService.setInt(0, -2);
                assertRowsOnly(retained, "id\n1\n3\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testRepeatedDeclaredOccurrencesKeepTheirPredicates() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNot("WITH q AS (SELECT id FROM lp_not WHERE NOT(s!='A')) "
                            + "SELECT * FROM (SELECT id FROM q UNION ALL SELECT id FROM q) ORDER BY id",
                    "id\n1\n1\n3\n3\n", "Index forward scan");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(
                        "DECLARE @q := (SELECT id FROM lp_not WHERE NOT(s!='A')) "
                                + "SELECT * FROM (SELECT id FROM @q UNION ALL SELECT id FROM @q) ORDER BY id",
                        sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n1\n1\n3\n3\n");
                }
            }
        });
    }

    @Test
    public void testValidationKeepsNormalizedOperatorAndPosition() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM lp_not WHERE NOT(missing>0)").noLeakCheck().fails(32, "Invalid column: missing");
            assertQuery("SELECT id FROM lp_not WHERE NOT(v>0 OR id)").noLeakCheck().fails(39, "argument type mismatch for function `not` at #1 expected: BOOLEAN, actual: INT");
            assertQuery("SELECT id FROM lp_not WHERE NOT(v)").noLeakCheck().fails(32, "argument type mismatch for function `not` at #1 expected: BOOLEAN, actual: INT");
            assertNot("SELECT id FROM lp_not WHERE NOT(v<>1)", "id\n3\n", null);
        });
    }

    private void assertNegatedComparison(String comparison, String expected) throws Exception {
        assertQuery("SELECT id FROM lp_not_types WHERE NOT (" + comparison + ")").noLeakCheck().returns(expected);
        assertQuery("SELECT id FROM (SELECT id, NOT (" + comparison + ") n FROM lp_not_types) WHERE n").noLeakCheck().returns(expected);
    }

    private void assertNot(String sql, String expected, String planPart) throws Exception {
        final int previousJitMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(sql)) {
            if (planPart != null) {
                final TextPlanSink sink = new TextPlanSink();
                sink.of(factory, sqlExecutionContext);
                TestUtils.assertContains(sink.getSink(), planPart);
            }
            assertRowsOnly(factory, expected);
        } finally {
            sqlExecutionContext.setJitMode(previousJitMode);
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_not (id INT,ts TIMESTAMP,s SYMBOL INDEX,v INT,b BOOLEAN) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO lp_not VALUES (1,'2020-01-01','A',-1,false),(2,'2020-01-02','B',0,true),"
                + "(3,'2020-01-03','A',1,false),(4,'2020-01-04',null,null,true)");
    }
}
