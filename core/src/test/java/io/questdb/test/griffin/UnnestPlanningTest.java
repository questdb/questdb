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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UnnestPlanningTest extends AbstractCairoTest {
    @Test
    public void testArrayZipNullsAndOrdinality() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT t.id,u.value FROM lp_unnest t,UNNEST(t.a) u ORDER BY t.id,u.value",
                    """
                            id	value
                            1	1.0
                            1	2.0
                            2	3.0
                            4	4.0
                            """
            );
            assertQueryRows(
                    "SELECT t.id,u.x,u.y,u.ord FROM lp_unnest t,UNNEST(t.a,t.b) WITH ORDINALITY u(x,y,ord) ORDER BY t.id,u.ord",
                    """
                            id	x	y	ord
                            1	1.0	10.0	1
                            1	2.0	null	2
                            2	3.0	20.0	1
                            2	null	30.0	2
                            3	null	40.0	1
                            4	4.0	null	1
                            """
            );
            assertQueryRows(
                    "SELECT t.id,u.x,u.value2 FROM lp_unnest t,UNNEST(t.a,t.b) u(x) ORDER BY t.id,u.x,u.value2",
                    """
                            id	x	value2
                            1	1.0	10.0
                            1	2.0	null
                            2	3.0	20.0
                            2	null	30.0
                            3	null	40.0
                            4	4.0	null
                            """
            );
            assertQueryRows("SELECT count() FROM lp_unnest t,UNNEST(t.a) u", """
                    count
                    4
                    """);
            assertQueryRows(
                    "SELECT t.id FROM lp_unnest t,UNNEST(t.a) u WHERE u.value>1 ORDER BY t.id",
                    """
                            id
                            1
                            2
                            4
                            """
            );
        });
    }

    @Test
    public void testJsonObjectsScalarsAndMixedZip() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT t.id,u.n,u.s,u.ordinality FROM lp_unnest t,UNNEST(t.j COLUMNS(n INT,s VARCHAR)) WITH ORDINALITY u ORDER BY t.id,u.ordinality",
                    """
                            id	n	s	ordinality
                            1	1	a	1
                            1	2		2
                            2	null		1
                            2	3	b	2
                            """
            );
            assertQueryRows(
                    "SELECT t.id,u.x,u.n,u.s,u.ord FROM lp_unnest t,UNNEST(t.a,t.j COLUMNS(n INT,s VARCHAR)) WITH ORDINALITY u(x,n,s,ord) ORDER BY t.id,u.ord",
                    """
                            id	x	n	s	ord
                            1	1.0	1	a	1
                            1	2.0	2		2
                            2	3.0	null		1
                            2	null	3	b	2
                            4	4.0	null		1
                            """
            );
            assertQueryRows(
                    "SELECT * FROM UNNEST('[1,null,3]'::VARCHAR COLUMNS(value DOUBLE)) WITH ORDINALITY",
                    """
                            value	ordinality
                            1.0	1
                            null	2
                            3.0	3
                            """
            );
            assertQueryRows(
                    "SELECT * FROM UNNEST('[{\"n\":2,\"s\":\"x\"},null]'::VARCHAR COLUMNS(n INT,s VARCHAR)) u(k,label)",
                    """
                            k	label
                            2	x
                            null\t
                            """
            );
            assertQueryRows("SELECT * FROM UNNEST('[]'::VARCHAR COLUMNS(n INT)) u", """
                    n
                    """);
        });
    }

    @Test
    public void testStandaloneAndNestedArrayExpansion() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryRows(
                    "SELECT * FROM UNNEST(ARRAY[1.0,2.0]) WITH ORDINALITY u(v,ord)",
                    """
                            v	ord
                            1.0	1
                            2.0	2
                            """
            );
            assertQueryRows("SELECT * FROM UNNEST(ARRAY[1.0,2.0],ARRAY[3.0]) u", """
                    value1	value2
                    1.0	3.0
                    2.0	null
                    """);
            assertQueryRows("SELECT count() FROM UNNEST(ARRAY[1.0,2.0])", """
                    count
                    2
                    """);
            execute("CREATE TABLE lp_unnest_nested(a DOUBLE[][])");
            execute("INSERT INTO lp_unnest_nested VALUES(ARRAY[ARRAY[1.0,2.0],ARRAY[3.0,4.0]])");
            assertQueryRows("SELECT u.value FROM lp_unnest_nested t,UNNEST(t.a) u", """
                    value
                    [1.0,2.0]
                    [3.0,4.0]
                    """);
            assertQueryRows(
                    "SELECT v.value FROM lp_unnest_nested t,UNNEST(t.a) u,UNNEST(u.value) v ORDER BY v.value",
                    """
                            value
                            1.0
                            2.0
                            3.0
                            4.0
                            """
            );
        });
    }

    @Test
    public void testPredicatesJoinKeysAndTimestampPreservation() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_unnest_lookup(v DOUBLE,name STRING,ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_unnest_lookup VALUES(1.0,'one','2024-01-01',1),(2.0,'two','2024-01-02',2),(3.0,'three','2024-01-03',3)");
            assertQueryRows(
                    "SELECT t.id,u.value FROM lp_unnest t,UNNEST(t.a) u WHERE t.id=u.value ORDER BY t.id,u.value",
                    """
                            id	value
                            1	1.0
                            4	4.0
                            """
            );
            assertQueryRows(
                    "SELECT t.id,u.value FROM lp_unnest t,UNNEST(t.a) u WHERE t.id>1 AND u.value>1 ORDER BY t.id,u.value",
                    """
                            id	value
                            2	3.0
                            4	4.0
                            """
            );
            assertUnnest("SELECT t.id,u.value,l.name FROM lp_unnest t,UNNEST(t.a) u JOIN lp_unnest_lookup l ON l.v=u.value ORDER BY t.id,u.value", """
                    id\tvalue\tname
                    1\t1.0\tone
                    1\t2.0\ttwo
                    2\t3.0\tthree
                    """);
            assertUnnest("SELECT t.id,u.value,l.name FROM lp_unnest t,UNNEST(ARRAY[1.0]) u JOIN lp_unnest_lookup l ON l.v=u.value ORDER BY t.id", """
                    id\tvalue\tname
                    1\t1.0\tone
                    2\t1.0\tone
                    3\t1.0\tone
                    4\t1.0\tone
                    """);
            assertQueryRows(
                    "SELECT t.ts,u.value,l.name FROM lp_unnest t,UNNEST(t.a) u ASOF JOIN lp_unnest_lookup l ORDER BY t.ts,u.value",
                    """
                            ts	value	name
                            2024-01-01T00:00:00.000000Z	1.0	one
                            2024-01-01T00:00:00.000000Z	2.0	one
                            2024-01-02T00:00:00.000000Z	3.0	two
                            2024-01-04T00:00:00.000000Z	4.0	three
                            """
            );
            assertUnnest("SELECT t.id,u.value,l.v FROM lp_unnest t JOIN lp_unnest_lookup l ON t.id=l.id,UNNEST(t.a) u ORDER BY t.id,u.value", """
                    id\tvalue\tv
                    1\t1.0\t1.0
                    1\t2.0\t1.0
                    2\t3.0\t2.0
                    """);
        });
    }

    @Test
    public void testAggregationWindowLimitAndCteOccurrences() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT u.value,count() n FROM lp_unnest t,UNNEST(t.a) u GROUP BY u.value ORDER BY u.value",
                    """
                            value	n
                            1.0	1
                            2.0	1
                            3.0	1
                            4.0	1
                            """
            );
            assertQueryRows(
                    "SELECT t.id,sum(u.value) v FROM lp_unnest t,UNNEST(t.a) u GROUP BY t.id ORDER BY t.id",
                    """
                            id	v
                            1	3.0
                            2	3.0
                            4	4.0
                            """
            );
            assertQueryRows(
                    "SELECT t.id,u.value,row_number() OVER(PARTITION BY t.id ORDER BY u.value) rn FROM lp_unnest t,UNNEST(t.a) u ORDER BY t.id,u.value",
                    """
                            id	value	rn
                            1	1.0	1
                            1	2.0	2
                            2	3.0	1
                            4	4.0	1
                            """
            );
            assertQueryRows(
                    "SELECT u.value FROM lp_unnest t,UNNEST(t.a) u ORDER BY u.value LIMIT 2",
                    """
                            value
                            1.0
                            2.0
                            """
            );
            assertQueryRows(
                    "WITH q AS(SELECT id,a FROM lp_unnest) SELECT q.id,u.value FROM q,UNNEST(q.a) u ORDER BY q.id,u.value",
                    """
                            id	value
                            1	1.0
                            1	2.0
                            2	3.0
                            4	4.0
                            """
            );
            assertQueryRows(
                    "WITH q AS(SELECT u.value FROM lp_unnest t,UNNEST(t.a) u) SELECT value FROM q UNION ALL SELECT value FROM q ORDER BY value",
                    """
                            value
                            1.0
                            1.0
                            2.0
                            2.0
                            3.0
                            3.0
                            4.0
                            4.0
                            """
            );
        });
    }

    @Test
    public void testWildcardAndProtectedAliases() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT t.id,u.* FROM lp_unnest t,UNNEST(t.a,t.b) WITH ORDINALITY u(x,y,ord) ORDER BY t.id,u.ord",
                    """
                            id	x	y	ord
                            1	1.0	10.0	1
                            1	2.0	null	2
                            2	3.0	20.0	1
                            2	null	30.0	2
                            3	null	40.0	1
                            4	4.0	null	1
                            """
            );
            assertQueryRows(
                    "SELECT * FROM lp_unnest t,UNNEST(t.a) u(id) ORDER BY t.ts,u.id",
                    """
                            id	a	b	j	ts	id1
                            1	[1.0,2.0]	[10.0]	[{"n":1,"s":"a"},{"n":2,"s":null}]	2024-01-01T00:00:00.000000Z	1.0
                            1	[1.0,2.0]	[10.0]	[{"n":1,"s":"a"},{"n":2,"s":null}]	2024-01-01T00:00:00.000000Z	2.0
                            2	[3.0]	[20.0,30.0]	[null,{"n":3,"s":"b"}]	2024-01-02T00:00:00.000000Z	3.0
                            4	[4.0]	null		2024-01-04T00:00:00.000000Z	4.0
                            """
            );
            assertQueryRows(
                    "SELECT u.\"a.b\" FROM lp_unnest t,UNNEST(t.a) u(\"a.b\") ORDER BY u.\"a.b\"",
                    """
                            a.b
                            1.0
                            2.0
                            3.0
                            4.0
                            """
            );
            assertQueryRows(
                    "SELECT u.\"select\" FROM lp_unnest t,UNNEST(t.a) u(\"select\") ORDER BY u.\"select\"",
                    """
                            select
                            1.0
                            2.0
                            3.0
                            4.0
                            """
            );
        });
    }

    @Test
    public void testMetadataAndFunctionsOutliveCompiler() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT t.id,u.number,u.label,u.ord FROM lp_unnest t,"
                    + "UNNEST(t.j COLUMNS(n INT,s VARCHAR)) WITH ORDINALITY u(number,label,ord) ORDER BY t.id,u.ord";
            RecordCursorFactory retained = null;
            try {
                final String expected = """
                        id	number	label	ord
                        1	1	a	1
                        1	2		2
                        2	null		1
                        2	3	b	2
                        """;
                final String expectedPlan;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    expectedPlan = plan(retained);
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_unnest", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                }
                assertResult(retained, expected);
                TestUtils.assertEquals(expectedPlan, plan(retained));
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testDiagnosticsAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT * FROM lp_unnest t,UNNEST(t.id) u").noLeakCheck().fails(33, "array type expected in UNNEST, got INT");
            assertQuery("SELECT * FROM lp_unnest t,UNNEST(t.a COLUMNS(n INT)) u").noLeakCheck().fails(33, "VARCHAR expected for JSON UNNEST, got DOUBLE[]");
            assertQuery("SELECT * FROM lp_unnest t,UNNEST(t.missing) u").noLeakCheck().fails(33, "Invalid column: missing");
            assertQuery("SELECT * FROM lp_unnest t,UNNEST(t.\"missing.dot\") u").noLeakCheck().fails(33, "Invalid column: \"missing.dot\"");
            assertQuery("SELECT * FROM lp_unnest t,UNNEST(ARRAY[t.missing]) u").noLeakCheck().fails(39, "Invalid column: missing");
            assertQuery("SELECT * FROM lp_unnest t,UNNEST(other.a) u,lp_unnest other").noLeakCheck().fails(33, "Invalid column: other.a");
            assertQuery("SELECT * FROM lp_unnest t,UNNEST(unknown.a) u").noLeakCheck().fails(33, "Invalid column: unknown.a");
            assertQuery("SELECT * FROM lp_unnest t,UNNEST(unknown.\"a.b\") u").noLeakCheck().fails(33, "Invalid column: unknown.\"a.b\"");
            assertQuery("SELECT * FROM UNNEST(1) u").noLeakCheck().fails(21, "array type expected in UNNEST, got INT");
            assertQuery("SELECT missing FROM lp_unnest t,UNNEST(t.a) u").noLeakCheck().fails(7, "Invalid column: missing");
            assertQuery("SELECT value FROM lp_unnest t,UNNEST(t.a) u,UNNEST(t.b) v").noLeakCheck().fails(7, "Ambiguous column [name=value]");
        });
    }

    private void assertUnnest(String sql, String expectedRows) throws Exception {
        final int previousJit = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertTrue(sql, hasUnnest(compiler.getPlanForTesting()));
            TestUtils.assertContains(plan(factory), "Unnest");
            assertResult(factory, expectedRows);
        } finally {
            sqlExecutionContext.setJitMode(previousJit);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_unnest(id INT,a DOUBLE[],b DOUBLE[],j VARCHAR,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_unnest VALUES(1,ARRAY[1.0,2.0],ARRAY[10.0],"
                + "'[{\"n\":1,\"s\":\"a\"},{\"n\":2,\"s\":null}]','2024-01-01'),"
                + "(2,ARRAY[3.0],ARRAY[20.0,30.0],'[null,{\"n\":3,\"s\":\"b\"}]','2024-01-02'),"
                + "(3,NULL,ARRAY[40.0],'[]','2024-01-03'),(4,ARRAY[4.0],NULL,NULL,'2024-01-04')");
    }

    private boolean hasUnnest(LogicalPlan plan) {
        if (plan instanceof JoinPlan join) {
            for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                if (join.getInputs().getQuick(i).getUnnest() != null) {
                    return true;
                }
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (hasUnnest(plan.inputAt(i))) {
                return true;
            }
        }
        return false;
    }

    private String plan(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        return sink.getSink().toString();
    }

    private String print(RecordCursorFactory factory) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        }
        return sink.toString();
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
