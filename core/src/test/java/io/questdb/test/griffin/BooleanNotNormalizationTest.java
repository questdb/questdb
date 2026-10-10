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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
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
            assertNot("SELECT id FROM lp_not WHERE NOT(v<=0 OR v>1) ORDER BY id", "id\n3\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(NOT(v=1)) ORDER BY id", "id\n3\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(v<>1) ORDER BY id", "id\n3\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(v=1 OR b) ORDER BY id", "id\n1\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(NOT(b)) ORDER BY id", "id\n2\n4\n", null);
            assertNot("SELECT id FROM lp_not WHERE NOT(1=2) ORDER BY id", "id\n1\n2\n3\n4\n", null);
            assertNot("SELECT id,NOT(v>0) n FROM lp_not ORDER BY id", "id\tn\n1\ttrue\n2\ttrue\n3\tfalse\n4\ttrue\n", null);
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
