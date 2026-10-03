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
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WindowPlanningTest extends AbstractCairoTest {
    @Test
    public void testStreamingAndCachedRankingUseBoundKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,row_number() OVER() rn FROM lp_window ORDER BY id",
                    """
                            id	rn
                            1	1
                            2	2
                            3	3
                            4	4
                            5	5
                            """
            );
            assertQueryRows(
                    "SELECT id,row_number() OVER(PARTITION BY k ORDER BY ts) rn,"
                            + "rank() OVER(ORDER BY v) r,dense_rank() OVER(ORDER BY v) dr FROM lp_window ORDER BY id",
                    """
                            id	rn	r	dr
                            1	1	1	1
                            2	1	2	2
                            3	2	5	4
                            4	3	4	3
                            5	2	2	2
                            """
            );
            assertQueryRows(
                    "SELECT id,rank() OVER(PARTITION BY k ORDER BY v,id DESC) r FROM lp_window ORDER BY id",
                    """
                            id	r
                            1	1
                            2	2
                            3	3
                            4	2
                            5	1
                            """
            );
            assertQueryRows(
                    "SELECT id,row_number() OVER() rn FROM lp_window ORDER BY row_number() OVER() DESC",
                    """
                            id	rn
                            5	5
                            4	4
                            3	3
                            2	2
                            1	1
                            """
            );
            assertQueryRows(
                    "SELECT id,sum(v) OVER() s FROM lp_window ORDER BY sum(v) OVER(),id",
                    """
                            id	s
                            1	9.0
                            2	9.0
                            3	9.0
                            4	9.0
                            5	9.0
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_window ORDER BY abs(row_number() OVER()) DESC",
                    """
                            id
                            5
                            4
                            3
                            2
                            1
                            """
            );
        });
    }

    @Test
    public void testRowsRangeNullTreatmentAndBothTimestampPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,sum(v) OVER(PARTITION BY k ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s,"
                            + "avg(v) OVER(PARTITION BY k) a,count(v) OVER() c FROM lp_window ORDER BY id",
                    """
                            id	s	a	c
                            1	1.0	2.5	4
                            2	2.0	2.0	4
                            3	1.0	2.5	4
                            4	4.0	2.5	4
                            5	4.0	2.0	4
                            """
            );
            assertQueryRows(
                    "SELECT id,min(v) OVER(ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) mn,"
                            + "max(v) OVER(ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) mx FROM lp_window ORDER BY id",
                    """
                            id	mn	mx
                            1	1.0	1.0
                            2	1.0	2.0
                            3	1.0	2.0
                            4	1.0	4.0
                            5	1.0	4.0
                            """
            );
            assertQueryRows(
                    "SELECT id,sum(v) OVER(ORDER BY ts RANGE BETWEEN 2 SECOND PRECEDING AND CURRENT ROW) s FROM lp_window ORDER BY id",
                    """
                            id	s
                            1	1.0
                            2	3.0
                            3	3.0
                            4	6.0
                            5	6.0
                            """
            );
            execute("CREATE TABLE lp_window_ns(id INT,v DOUBLE,ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("INSERT INTO lp_window_ns SELECT id,v,ts::TIMESTAMP_NS FROM lp_window");
            assertQueryRows(
                    "SELECT id,sum(v) OVER(ORDER BY ts RANGE BETWEEN 2 SECOND PRECEDING AND CURRENT ROW) s FROM lp_window_ns ORDER BY id",
                    """
                            id	s
                            1	1.0
                            2	3.0
                            3	3.0
                            4	6.0
                            5	6.0
                            """
            );
            assertQueryRows(
                    "SELECT id,first_value(v) IGNORE NULLS OVER(ORDER BY ts) f,"
                            + "last_value(v) IGNORE NULLS OVER(ORDER BY ts) l FROM lp_window ORDER BY id",
                    """
                            id	f	l
                            1	1.0	1.0
                            2	1.0	2.0
                            3	1.0	2.0
                            4	1.0	4.0
                            5	1.0	2.0
                            """
            );
        });
    }

    @Test
    public void testLeadLagNthAndComputedKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,lag(v,2,0.0) OVER(PARTITION BY k ORDER BY ts) l,"
                            + "lead(v) OVER(PARTITION BY k ORDER BY ts) n FROM lp_window ORDER BY id",
                    """
                            id	l	n
                            1	0.0	null
                            2	0.0	2.0
                            3	0.0	4.0
                            4	1.0	null
                            5	0.0	null
                            """
            );
            assertQueryRows(
                    "SELECT id,nth_value(v,2) OVER(ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n FROM lp_window ORDER BY id",
                    """
                            id	n
                            1	null
                            2	2.0
                            3	2.0
                            4	2.0
                            5	2.0
                            """
            );
            assertQueryRows(
                    "SELECT id,v+id x,rank() OVER(PARTITION BY id%2 ORDER BY x,id DESC) r FROM lp_window ORDER BY id",
                    """
                            id	x	r
                            1	2.0	1
                            2	4.0	1
                            3	null	3
                            4	8.0	2
                            5	7.0	2
                            """
            );
            assertQueryRows(
                    "SELECT *,v+id x,row_number() OVER(ORDER BY x) rn FROM lp_window ORDER BY id",
                    """
                            id	k	v	ts	x	rn
                            1	a	1.0	2024-01-01T00:00:01.000000Z	2.0	1
                            2	b	2.0	2024-01-01T00:00:02.000000Z	4.0	2
                            3	a	null	2024-01-01T00:00:03.000000Z	null	5
                            4	a	4.0	2024-01-01T00:00:04.000000Z	8.0	4
                            5	b	2.0	2024-01-01T00:00:05.000000Z	7.0	3
                            """
            );
        });
    }

    @Test
    public void testNamedWindowsInheritanceAndIndependentOccurrences() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,sum(v) OVER w2 s,avg(v) OVER w2 a FROM lp_window "
                            + "WINDOW w AS(PARTITION BY k),w2 AS(w ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY id",
                    """
                            id	s	a
                            1	1.0	1.0
                            2	2.0	2.0
                            3	1.0	1.0
                            4	4.0	4.0
                            5	4.0	2.0
                            """
            );
            assertQueryRows(
                    "WITH q AS(SELECT id,row_number() OVER w rn FROM lp_window WINDOW w AS(ORDER BY ts)) "
                            + "SELECT id,rn FROM q UNION ALL SELECT id,rn FROM q ORDER BY id,rn",
                    """
                            id	rn
                            1	1
                            1	1
                            2	2
                            2	2
                            3	3
                            3	3
                            4	4
                            4	4
                            5	5
                            5	5
                            """
            );
        });
    }

    @Test
    public void testJoinDuplicateColumnNamesUnderWindow() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT a.id, b.id, a.k, b.k, sum(b.v) OVER (PARTITION BY a.k ORDER BY b.id) s "
                            + "FROM lp_window a JOIN lp_window b ON a.id = b.id ORDER BY a.id",
                    """
                            id	id1	k	k1	s
                            1	1	a	a	1.0
                            2	2	b	b	2.0
                            3	3	a	a	1.0
                            4	4	a	a	5.0
                            5	5	b	b	4.0
                            """
            );
            assertQueryRows(
                    "SELECT a.id + b.id x, a.k, b.k, row_number() OVER (PARTITION BY b.k ORDER BY a.id DESC) rn "
                            + "FROM lp_window a JOIN lp_window b ON a.id = b.id ORDER BY x",
                    """
                            x	k	k1	rn
                            2	a	a	3
                            4	b	b	2
                            6	a	a	2
                            8	a	a	1
                            10	b	b	1
                            """
            );
        });
    }

    @Test
    public void testNestedWindowsScalarWrappersAndSelectAliases() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,abs(sum(v) OVER()-avg(v) OVER()) d,row_number() OVER()+1 n FROM lp_window ORDER BY id",
                    """
                            id	d	n
                            1	6.75	2
                            2	6.75	3
                            3	6.75	4
                            4	6.75	5
                            5	6.75	6
                            """
            );
            assertQueryRows(
                    "SELECT id,sum(sum(v) OVER()) OVER() s FROM lp_window ORDER BY id",
                    """
                            id	s
                            1	45.0
                            2	45.0
                            3	45.0
                            4	45.0
                            5	45.0
                            """
            );
            assertQueryRows("SELECT v x,sum(x) OVER() s FROM lp_window ORDER BY id", """
                    x	s
                    1.0	9.0
                    2.0	9.0
                    null	9.0
                    4.0	9.0
                    2.0	9.0
                    """);
            assertQueryRows(
                    "SELECT v+id x,lag(x) OVER(ORDER BY ts) l FROM lp_window ORDER BY id",
                    """
                            x	l
                            2.0	null
                            4.0	2.0
                            null	4.0
                            8.0	null
                            7.0	8.0
                            """
            );
            assertQueryRows(
                    "SELECT k,v s,row_number() OVER() rn FROM (SELECT k,sum(v) v FROM lp_window GROUP BY k)",
                    """
                            k	s	rn
                            a	5.0	1
                            b	4.0	2
                            """
            );
        });
    }

    @Test
    public void testAggregateOverWindowAndDistinctWindowResults() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows("SELECT sum(sum(v) OVER()+sum(v) OVER()) s FROM lp_window", """
                    s
                    90.0
                    """);
            assertQueryRows(
                    "SELECT k,max(avg(v) OVER()) m FROM lp_window GROUP BY k ORDER BY k",
                    """
                            k	m
                            a	2.25
                            b	2.25
                            """
            );
            assertQueryRows(
                    "SELECT DISTINCT k,sum(v) OVER(PARTITION BY k) s FROM lp_window ORDER BY k",
                    """
                            k	s
                            a	5.0
                            b	4.0
                            """
            );
            assertQueryRows(
                    "SELECT k,v,row_number() OVER(ORDER BY v DESC) rn FROM (SELECT k,sum(v) v FROM lp_window GROUP BY k) ORDER BY k",
                    """
                            k	v	rn
                            a	5.0	1
                            b	4.0	2
                            """
            );
        });
    }

    @Test
    public void testValidationPositionsAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT row_number() OVER missing FROM lp_window").noLeakCheck().fails(25, "window 'missing' is not defined");
            assertQuery("SELECT row_number() OVER w FROM lp_window WINDOW w AS(x),x AS(w)").noLeakCheck().fails(54, "window 'x' is not defined");
            assertQuery("SELECT id FROM lp_window WHERE row_number() OVER()>0").noLeakCheck().fails(31, "window function is not allowed in WHERE clause");
            assertQuery("SELECT id FROM lp_window ORDER BY row_number() OVER()").noLeakCheck().fails(34, "window function called in non-window context, make sure to add OVER clause");
            assertQuery("SELECT id FROM lp_window ORDER BY row_number()").noLeakCheck().fails(34, "window function called in non-window context, make sure to add OVER clause");
            assertQuery("SELECT id FROM lp_window ORDER BY sum(v) OVER()").noLeakCheck().fails(34, "Window function is not allowed in context of aggregation. Use sub-query.");
            assertQuery("SELECT row_number() OVER(PARTITION BY row_number() OVER()) FROM lp_window").noLeakCheck().fails(38, "window function is not allowed in PARTITION BY clause");
            assertQuery("SELECT sum(v) OVER(ORDER BY missing) FROM lp_window").noLeakCheck().fails(28, "Invalid column: missing");
            assertQuery("SELECT sum(v),row_number() OVER() FROM lp_window").noLeakCheck().fails(0, "Window function is not allowed in context of aggregation. Use sub-query.");
            assertQuery("SELECT row_number() OVER(),sum(v) FROM lp_window").noLeakCheck().fails(0, "Window function is not allowed in context of aggregation. Use sub-query.");
            assertQuery("SELECT sum(v),row_number() OVER()+1 FROM lp_window").noLeakCheck().fails(14, "Window function is not allowed in context of aggregation. Use sub-query.");
            assertQuery("SELECT sum(v) OVER(ORDER BY ts ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM lp_window").noLeakCheck().fails(44, "non-negative integer expression expected");
            assertQuery("SELECT sum(v) OVER(ORDER BY ts RANGE BETWEEN 9223372036854775807 SECOND PRECEDING AND CURRENT ROW) FROM lp_window").noLeakCheck().fails(45, "RANGE frame start is out of range for the designated timestamp [width=9223372036854775807 second, max=9223372036854 second]");
            assertQuery("SELECT id FROM lp_window WINDOW unused AS(ROWS BETWEEN -1 PRECEDING AND CURRENT ROW)")
                    .noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().withPlanNotContaining("Window").returns("id\n1\n2\n3\n4\n5\n");
            assertQuery("SELECT id FROM lp_window WINDOW unused AS(missing)").noLeakCheck().fails(42, "window 'missing' is not defined");
            assertQuery("SELECT id FROM lp_window WINDOW a AS(b),b AS(a)").noLeakCheck().fails(37, "window 'b' is not defined");
        });
    }

    @Test
    public void testNativeArgumentsAndPartitionFunctionsOutliveCompiler() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,sum(CASE WHEN id IN(1,2,4) THEN v ELSE 0.0 END) "
                    + "OVER(PARTITION BY lower(k) ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s "
                    + "FROM lp_window ORDER BY id";
            RecordCursorFactory retained = null;
            try {
                final String expected = """
                        id	s
                        1	1.0
                        2	2.0
                        3	1.0
                        4	4.0
                        5	2.0
                        """;
                final String expectedPlan;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    expectedPlan = plan(retained);
                    assertResult(retained, expected);
                    try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_window", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    compiler.clear();
                }
                assertResult(retained, expected);
                TestUtils.assertEquals(expectedPlan, plan(retained));
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testRankingComparatorNamesOutliveNamedWindowSyntax() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,percent_rank() OVER w p,cume_dist() OVER w c,ntile(3) OVER w n "
                    + "FROM lp_window WINDOW w AS(PARTITION BY k ORDER BY v,id) ORDER BY id";
            RecordCursorFactory retained = null;
            try {
                final String expected = """
                        id	p	c	n
                        1	0.0	0.3333333333333333	1
                        2	0.0	0.5	1
                        3	1.0	1.0	3
                        4	0.5	0.6666666666666666	2
                        5	1.0	1.0	2
                        """;
                final String expectedPlan;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    expectedPlan = plan(retained);
                    assertResult(retained, expected);
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile(
                            "SELECT id,row_number() OVER(ORDER BY id DESC) other FROM lp_window", sqlExecutionContext
                    ).getRecordCursorFactory()) {
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

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_window(id INT,k STRING,v DOUBLE,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_window VALUES(1,'a',1.0,'2024-01-01T00:00:01'),"
                + "(2,'b',2.0,'2024-01-01T00:00:02'),(3,'a',null,'2024-01-01T00:00:03'),"
                + "(4,'a',4.0,'2024-01-01T00:00:04'),(5,'b',2.0,'2024-01-01T00:00:05')");
    }

    private boolean hasWindowPlan(LogicalPlan plan) {
        if (plan instanceof WindowPlan) {
            final WindowPlan window = (WindowPlan) plan;
            Assert.assertEquals(window.getFunctions().size(), window.getSpecs().size());
            Assert.assertEquals(window.getFunctions().size(), window.getFunctionColumnIds().size());
            return true;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (hasWindowPlan(plan.inputAt(i))) {
                return true;
            }
        }
        return false;
    }

    private String plan(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        final StringSink text = new StringSink();
        for (int i = 1, n = sink.getLineCount(); i <= n; i++) {
            text.put(sink.getLine(i)).put('\n');
        }
        return text.toString();
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
