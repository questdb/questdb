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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalJoinTest extends AbstractCairoTest {
    @Test
    public void testCrossInnerExplicitShorthandAndCompositeKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean fullFat : new boolean[]{false, true}) {
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l CROSS JOIN lp_join_r r ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        1	11
                        1	12
                        1	13
                        2	10
                        2	11
                        2	12
                        2	13
                        3	10
                        3	11
                        3	12
                        3	13
                        4	10
                        4	11
                        4	12
                        4	13
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l CROSS JOIN lp_join_r r WHERE l.k=r.k ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        1	11
                        2	12
                        3	13
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        1	11
                        2	12
                        3	13
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON(k) ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        1	11
                        2	12
                        3	13
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND r.b=l.b ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        2	12
                        3	13
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON(k,b) ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        2	12
                        3	13
                        """);
            }
        });
    }

    @Test
    public void testLeftOnResidualAndWhereApplyAtDifferentBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean fullFat : new boolean[]{false, true}) {
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k AND l.v<r.v ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        2	null
                        3	13
                        4	null
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k WHERE l.v<r.v ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        3	13
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        2	null
                        3	13
                        4	null
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k WHERE r.id=null ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        4	null
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k AND false ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	null
                        2	null
                        3	null
                        4	null
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.v<r.v ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        1	13
                        2	13
                        3	13
                        4	null
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON false ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	null
                        2	null
                        3	null
                        4	null
                        """);
            }
        });
    }

    @Test
    public void testSymbolStringAndMixedTimestampPrecisionKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean fullFat : new boolean[]{false, true}) {
                assertRows("SELECT l.id lid,r.id rid,l.s ls,r.s rs FROM lp_join_l l JOIN lp_join_r r ON l.s=r.s ORDER BY lid,rid", fullFat,
                        """
                        lid	rid	ls	rs
                        1	10	a	a
                        1	11	a	a
                        2	12	b	b
                        3	13	\t
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.s=r.s AND l.v<r.v ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        2	null
                        3	13
                        4	null
                        """);
                assertRows("SELECT l.id lid,r.id rid,l.ts lts,r.ts rts FROM lp_join_l l JOIN lp_join_r r ON l.ts=r.ts ORDER BY lid,rid", fullFat,
                        """
                        lid	rid	lts	rts
                        1	10	1970-01-01T00:00:00.001000Z	1970-01-01T00:00:00.001000000Z
                        2	12	1970-01-01T00:00:00.002000Z	1970-01-01T00:00:00.002000000Z
                        """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_r r JOIN lp_join_l l ON r.ts=l.ts ORDER BY lid,rid", fullFat,
                        """
                        lid	rid
                        1	10
                        2	12
                        """);
            }
        });
    }

    @Test
    public void testProjectedDerivedSourcesPruningAndQuotedScopes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows("SELECT l.id lid,r.id rid FROM (SELECT id,k,v+1 value FROM lp_join_l WHERE id>0) l JOIN (SELECT id,k,v value FROM lp_join_r) r ON l.k=r.k WHERE l.value<r.value ORDER BY lid,rid", false, "lid\trid\n");
            assertRows("SELECT rid FROM (SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k) q WHERE lid>1 ORDER BY rid", false,
                    """
                    rid
                    12
                    13
                    """);
            assertRows("SELECT l.* FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k ORDER BY id,v", false,
                    """
                    id	k	b	v	s	ts
                    1	1	1	10	a	1970-01-01T00:00:00.001000Z
                    1	1	1	10	a	1970-01-01T00:00:00.001000Z
                    2	2	1	20	b	1970-01-01T00:00:00.002000Z
                    3	null	2	30	\t
                    """);
            assertRows("SELECT l.*,r.* FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k ORDER BY 1,7", false,
                    """
                    id	k	b	v	s	ts	id1	k1	b1	v1	s1	ts1
                    1	1	1	10	a	1970-01-01T00:00:00.001000Z	10	1	1	11	a	1970-01-01T00:00:00.001000000Z
                    1	1	1	10	a	1970-01-01T00:00:00.001000Z	11	1	2	5	a	1970-01-01T00:00:00.001000001Z
                    2	2	1	20	b	1970-01-01T00:00:00.002000Z	12	2	1	5	b	1970-01-01T00:00:00.002000000Z
                    3	null	2	30			13	null	2	31	\t
                    """);
            assertRows("SELECT l.*,sum(r.v) total FROM (SELECT id,k,v FROM lp_join_l) l JOIN lp_join_r r ON l.k=r.k ORDER BY l.id", false,
                    """
                    id	k	v	total
                    1	1	10	16
                    2	2	20	5
                    3	null	30	31
                    """);
            assertRows("SELECT DISTINCT l.* FROM (SELECT id,k,v FROM lp_join_l) l JOIN lp_join_r r ON l.k=r.k ORDER BY id", false,
                    """
                    id	k	v
                    1	1	10
                    2	2	20
                    3	null	30
                    """);
            assertRows("SELECT leftalias.id lid,r.id rid FROM lp_join_l \"LeftAlias\" JOIN lp_join_r r ON leftalias.k=r.k ORDER BY lid,rid", false,
                    """
                    lid	rid
                    1	10
                    1	11
                    2	12
                    3	13
                    """);
        });
    }

    @Test
    public void testJoinFactoriesAndExplainSurviveCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            RecordCursorFactory explain = null;
            final String expected = """
                    lid	rid
                    1	10
                    2	null
                    3	13
                    4	null
                    """;
            String expectedPlan;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final String sql = "SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid";
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, expected);
                }
                try {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    Assert.assertNotNull(compiler.getLogicalPlanForTesting());
                    explain = compiler.compile("EXPLAIN " + sql, sqlExecutionContext).getRecordCursorFactory();
                    expectedPlan = print(explain);
                    assertFailure(compiler, "SELECT lp_missing_fn(l.id) FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k", "unknown function name");
                    try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_join_l", sqlExecutionContext).getRecordCursorFactory()) {
                        TestUtils.assertEquals("count\n4\n", print(factory));
                    }
                } catch (Throwable th) {
                    if (retained != null) {
                        retained.close();
                    }
                    if (explain != null) {
                        explain.close();
                    }
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained; RecordCursorFactory plan = explain) {
                assertResult(factory, expected);
                TestUtils.assertEquals(expectedPlan, print(plan));
            }
        });
    }

    @Test
    public void testInnerThenLeftJoinUsesAllThreeSources() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k "
                        + "LEFT JOIN lp_join_r x ON l.k=x.k ORDER BY l.id", isFullFat,
                        """
                        id
                        1
                        1
                        1
                        1
                        2
                        3
                        """);
            }
        });
    }

    @Test
    public void testJoinValidationAndMissingTimestampRecover() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k").noLeakCheck().fails(7, "Ambiguous column [name=id]");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON k=r.k").noLeakCheck().fails(49, "Ambiguous column [name=k]");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=missing.k").noLeakCheck().fails(53, "Invalid table name or alias");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.missing=r.k").noLeakCheck().fails(49, "Invalid column: l.missing");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r l ON l.k=l.k").noLeakCheck().fails(44, "Duplicate table or alias: l");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.ts").noLeakCheck().fails(53, "join column type mismatch");
            assertQuery("SELECT l.id FROM lp_join_l l SPLICE JOIN lp_join_r r ON l.k=r.k JOIN lp_join_r x ON l.k=x.k").noLeakCheck().fails(29, "left side of time series join has no timestamp");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertFailure(compiler, "SELECT l.id FROM lp_join_l l SPLICE JOIN lp_join_r r ON l.k=r.k JOIN lp_join_r x ON l.k=x.k", "left side of time series join has no timestamp");
                try (RecordCursorFactory factory = compiler.compile("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k ORDER BY lid,rid", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertNotNull(compiler.getLogicalPlanForTesting());
                    assertResult(factory, "lid\trid\n1\t10\n1\t11\n2\t12\n3\t13\n");
                }
            }
        });
    }

    @Test
    public void testMixedSourceConjunctsPreserveNativeTimestampLiteralPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_join_ts_l(id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_join_ts_r(id INT)");
            execute("INSERT INTO lp_join_ts_l VALUES (1,'2020-01-01T00:00:00.000000Z'),(2,'2020-01-01T12:00:00.000000Z')");
            execute("INSERT INTO lp_join_ts_r VALUES (1),(2)");
            final String predicate = "l.ts<'2020-01-01T00:00:00.000000001Z' AND r.id>0";
            final String[] emptyQueries = {
                    "SELECT l.id lid,r.id rid FROM lp_join_ts_l l JOIN lp_join_ts_r r ON l.id=r.id WHERE " + predicate,
                    "SELECT l.id lid,r.id rid FROM lp_join_ts_l l LEFT JOIN lp_join_ts_r r ON l.id=r.id WHERE " + predicate,
                    "SELECT l.id lid,r.id rid FROM lp_join_ts_l l JOIN lp_join_ts_r r ON l.id=r.id AND " + predicate
            };
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (String sql : emptyQueries) {
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, "lid\trid\n");
                    }
                }
                // LEFT ON remains a pair predicate: the literal uses adaptive
                // nanosecond precision and unmatched master rows are retained.
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT l.id lid,r.id rid FROM lp_join_ts_l l LEFT JOIN lp_join_ts_r r ON l.id=r.id AND "
                                + predicate + " ORDER BY lid", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, "lid\trid\n1\t1\n2\tnull\n");
                }
            }
        });
    }

    @Test
    public void testJoinPruningRetainsSurvivingTimestampAliasDesignation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_join_ts_l(id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_join_ts_r(id INT)");
            execute("INSERT INTO lp_join_ts_l VALUES (1,'2020-01-01'),(2,'2020-01-02')");
            execute("INSERT INTO lp_join_ts_r VALUES (1),(2)");
            final String sql = "SELECT q.b FROM (SELECT ts a,ts b,id FROM lp_join_ts_l) q JOIN lp_join_ts_r r ON q.id=r.id";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (boolean fullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(fullFat);
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertEquals(0, factory.getMetadata().getTimestampIndex());
                        assertResult(factory, "b\n2020-01-01T00:00:00.000000Z\n2020-01-02T00:00:00.000000Z\n");
                    }
                }
            }
        });
    }

    @Test
    public void testIndependentConjunctBindingPreservesBooleanNullAndParameterTypes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String from = " FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k WHERE ";
            assertRows("SELECT l.id" + from + "null AND true ORDER BY l.id", false, "id\n");
            assertRows("SELECT l.id" + from + "false AND null ORDER BY l.id", false, "id\n");
            // LEFT ON keeps these terms in one matching predicate.
            assertRows("SELECT l.id FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k "
                    + "AND l.id IN (1,2,3) AND null ORDER BY l.id", false,
                    """
                    id
                    1
                    2
                    3
                    4
                    """);
            assertRows("SELECT l.id FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k "
                    + "AND null AND l.id IN (1,2,3) ORDER BY l.id", false,
                    """
                    id
                    1
                    2
                    3
                    4
                    """);
            assertRows("SELECT l.id" + from + "l.id<r.id AND true ORDER BY l.id", false,
                    """
                    id
                    1
                    1
                    2
                    3
                    """);
            assertQuery("SELECT l.id" + from + "l.id<r.id AND 1").noLeakCheck().fails(77, "boolean expression expected");
            assertQuery("SELECT l.id" + from + "null AND 1").noLeakCheck().fails(63, "expression type mismatch, expected: BOOLEAN, actual: NULL");
            assertQuery("SELECT l.id" + from + "1 AND null").noLeakCheck().fails(63, "expression type mismatch, expected: BOOLEAN, actual: INT");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final String expected = """
                        id
                        1
                        1
                        2
                        3
                        """;
                try (RecordCursorFactory factory = compiler.compile("SELECT l.id" + from
                        + "l.id<r.id ORDER BY l.id", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, expected);
                }
                bindVariableService.clear();
                try (RecordCursorFactory factory = compiler.compile("SELECT l.id" + from
                        + "$1 AND l.id<r.id ORDER BY l.id", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.BOOLEAN, bindVariableService.getFunction(0).getType());
                    bindVariableService.setBoolean(0, true);
                    assertResult(factory, expected);
                    bindVariableService.setBoolean(0, false);
                    assertResult(factory, "id\n");
                }
            }
        });
    }

    @Test
    public void testCompileTimeWhereConjunctsKeepTheirOwnBooleanValidation() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> joins = new ObjList<>(
                    "JOIN lp_join_r r ON l.k=r.k", "CROSS JOIN lp_join_r r",
                    "LEFT JOIN lp_join_r r ON l.k=r.k", "RIGHT JOIN lp_join_r r ON l.k=r.k",
                    "FULL JOIN lp_join_r r ON l.k=r.k");
            {
                final int i = 0;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(74, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(63, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(72, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            {
                final int i = 1;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(69, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(58, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(67, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            {
                final int i = 2;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(79, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(68, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(77, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            {
                final int i = 3;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(80, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(69, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(78, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            {
                final int i = 4;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(79, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(68, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(77, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            // Failed constant-group validation also releases the already bound
            // native IN preparation. The following compilation must still work.
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k "
                    + "WHERE l.id IN (1,2,3) AND null").noLeakCheck().fails(83, "boolean expression expected");
            assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k "
                    + "WHERE l.id IN (1,2,3) ORDER BY l.id", false,
                    """
                    id
                    1
                    1
                    2
                    3
                    """);
        });
    }

    @Test
    public void testInnerConstantTermsCombineAcrossOriginsButOuterOnStaysSeparate() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND null WHERE true").noLeakCheck().fails(61, "boolean expression expected");
            assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND null WHERE false", false, "id\n");
            assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND null "
                    + "JOIN lp_join_l x ON r.k=x.k AND true ORDER BY l.id", false,
                    "id\n");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND true "
                    + "JOIN lp_join_l x ON r.k=x.k AND null").noLeakCheck().fails(98, "boolean expression expected");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND null "
                    + "JOIN lp_join_l x ON r.k=x.k WHERE l.id>0").noLeakCheck().fails(61, "boolean expression expected");
            assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k "
                    + "JOIN lp_join_l x ON r.k=x.k WHERE null AND true ORDER BY l.id", false,
                    "id\n");
            assertRows("SELECT l.id FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k "
                    + "AND null AND l.id IN (1,2,3) ORDER BY l.id", false, """
                    id
                    1
                    2
                    3
                    4
                    """);
            assertRows("SELECT l.id FROM lp_join_l l RIGHT JOIN lp_join_r r ON l.k=r.k "
                    + "AND null AND l.id IN (1,2,3) ORDER BY l.id", false, """
                    id
                    null
                    null
                    null
                    null
                    """);
            assertRows("SELECT l.id FROM lp_join_l l FULL JOIN lp_join_r r ON l.k=r.k "
                    + "AND null AND l.id IN (1,2,3) ORDER BY l.id", false, """
                    id
                    null
                    null
                    null
                    null
                    1
                    2
                    3
                    4
                    """);
            assertQuery("SELECT l.id FROM lp_join_l l CROSS JOIN lp_join_r r ON l.k=r.k AND null").noLeakCheck().fails(52, "Cross joins cannot have join clauses");
        });
    }

    @Test
    public void testRuntimeTermsDoNotTypeAnIsolatedCompileTimeNull() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setBoolean(0, true);
            final String prefix = "SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k WHERE ";
            assertQuery(prefix + "null AND $1").noLeakCheck().fails(63, "boolean expression expected");
            assertQuery(prefix + "null AND abs(1)>0").noLeakCheck().fails(63, "boolean expression expected");
            assertRows(prefix + "null AND true AND $1 ORDER BY l.id", false, "id\n");
        });
    }

    private void assertRows(String sql, boolean isFullFat, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertResult(factory, expected);
            }
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void assertFailure(SqlCompilerImpl compiler, String sql, String message) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_join_l(id INT,k INT,b INT,v INT,s SYMBOL,ts TIMESTAMP)");
        execute("CREATE TABLE lp_join_r(id INT,k INT,b INT,v INT,s STRING,ts TIMESTAMP_NS)");
        execute("""
                INSERT INTO lp_join_l VALUES
                (1,1,1,10,'a','1970-01-01T00:00:00.001000Z'),
                (2,2,1,20,'b','1970-01-01T00:00:00.002000Z'),
                (3,null,2,30,null,null),
                (4,4,2,40,'d','1970-01-01T00:00:00.004000Z')
                """);
        execute("""
                INSERT INTO lp_join_r VALUES
                (10,1,1,11,'a','1970-01-01T00:00:00.001000000Z'),
                (11,1,2,5,'a','1970-01-01T00:00:00.001000001Z'),
                (12,2,1,5,'b','1970-01-01T00:00:00.002000000Z'),
                (13,null,2,31,null,null)
                """);
    }

    private String print(RecordCursorFactory factory) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
            return sink.toString();
        }
    }
}
