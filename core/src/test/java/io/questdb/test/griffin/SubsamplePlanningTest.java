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

import io.questdb.PropertyKey;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SubsamplePlanningTest extends AbstractCairoTest {
    @Test
    public void testUniformCadenceAndBothCachedFactories() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (int mode = 0; mode < 2; mode++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, mode == 1 ? "true" : "false");
                assertQueryRows("SELECT * FROM lp_subsample SUBSAMPLE uniform(3)", """
                        id	v	x	ts
                        1	10.0	60	2024-01-01T00:00:01.000000Z
                        4	40.0	20	2024-01-01T00:00:04.000000Z
                        6	20.0	50	2024-01-01T00:00:06.000000Z
                        """);
                assertQueryRows("SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(30)", """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:02.000000Z	50.0
                        2024-01-01T00:00:03.000000Z	null
                        2024-01-01T00:00:04.000000Z	40.0
                        2024-01-01T00:00:05.000000Z	30.0
                        2024-01-01T00:00:06.000000Z	20.0
                        """);
                assertQueryRows("SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(1)", """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:02.000000Z	50.0
                        2024-01-01T00:00:03.000000Z	null
                        2024-01-01T00:00:04.000000Z	40.0
                        2024-01-01T00:00:05.000000Z	30.0
                        2024-01-01T00:00:06.000000Z	20.0
                        """);
                assertQueryRows("SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(2)", """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:03.000000Z	null
                        2024-01-01T00:00:05.000000Z	30.0
                        2024-01-01T00:00:06.000000Z	20.0
                        """);
                assertQueryRows("SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(2,7)", """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:03.000000Z	null
                        2024-01-01T00:00:05.000000Z	30.0
                        2024-01-01T00:00:06.000000Z	20.0
                        """);
                assertQueryRows("SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(1,NULL)", """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:02.000000Z	50.0
                        2024-01-01T00:00:03.000000Z	null
                        2024-01-01T00:00:04.000000Z	40.0
                        2024-01-01T00:00:05.000000Z	30.0
                        2024-01-01T00:00:06.000000Z	20.0
                        """);
                assertQueryRows(
                        "SELECT ts,v FROM lp_subsample WHERE v IS NULL SUBSAMPLE uniform(2)",
                        """
                                ts	v
                                2024-01-01T00:00:03.000000Z	null
                                """
                );
                assertQueryRows(
                        "SELECT ts,v FROM lp_subsample WHERE id>10 SUBSAMPLE cadence(2)",
                        """
                                ts	v
                                """
                );
            }
            execute("CREATE TABLE lp_subsample_ns AS (SELECT id,v,x,ts::TIMESTAMP_NS ts FROM lp_subsample) TIMESTAMP(ts)");
            assertQueryRows("SELECT ts,v FROM lp_subsample_ns SUBSAMPLE uniform(3)", """
                    ts	v
                    2024-01-01T00:00:01.000000000Z	10.0
                    2024-01-01T00:00:04.000000000Z	40.0
                    2024-01-01T00:00:06.000000000Z	20.0
                    """);
        });
    }

    @Test
    public void testAliasesWildcardAndHiddenOrderColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT ts clock,v AS x FROM lp_subsample SUBSAMPLE uniform(3) ORDER BY x",
                    """
                            clock	x
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:04.000000Z	40.0
                            """
            );
            assertQueryRows(
                    "SELECT ts clock,v AS x FROM lp_subsample SUBSAMPLE uniform(3) ORDER BY lp_subsample.x",
                    """
                            clock	x
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:01.000000Z	10.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(3) ORDER BY x%2,v DESC",
                    """
                            ts	v
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:01.000000Z	10.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,v AS __keep_subsample FROM lp_subsample SUBSAMPLE uniform(3) ORDER BY x",
                    """
                            ts	__keep_subsample
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:01.000000Z	10.0
                            """
            );
            assertQueryRows(
                    "SELECT *,id AS __keep_subsample,id AS __order_subsample FROM lp_subsample SUBSAMPLE cadence(2) ORDER BY id DESC",
                    """
                            id	v	x	ts	__keep_subsample	__order_subsample
                            6	20.0	50	2024-01-01T00:00:06.000000Z	6	6
                            5	30.0	30	2024-01-01T00:00:05.000000Z	5	5
                            3	null	40	2024-01-01T00:00:03.000000Z	3	3
                            1	10.0	60	2024-01-01T00:00:01.000000Z	1	1
                            """
            );
            assertQueryRows(
                    "SELECT ts,v+id value FROM lp_subsample SUBSAMPLE uniform(3) ORDER BY value+1",
                    """
                            ts	value
                            2024-01-01T00:00:01.000000Z	11.0
                            2024-01-01T00:00:06.000000Z	26.0
                            2024-01-01T00:00:04.000000Z	44.0
                            """
            );
        });
    }

    @Test
    public void testInputOrderAndFinalOrderLimitBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT ts,v FROM (SELECT * FROM lp_subsample ORDER BY ts DESC) SUBSAMPLE uniform(3)",
                    """
                            ts	v
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:01.000000Z	10.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,v FROM (SELECT * FROM lp_subsample ORDER BY x) SUBSAMPLE uniform(3)",
                    """
                            ts	v
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:01.000000Z	10.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(3) ORDER BY x LIMIT 2",
                    """
                            ts	v
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:06.000000Z	20.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(2) ORDER BY ts DESC LIMIT -2",
                    """
                            ts	v
                            2024-01-01T00:00:03.000000Z	null
                            2024-01-01T00:00:01.000000Z	10.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,v FROM (SELECT * FROM lp_subsample LIMIT 4) SUBSAMPLE uniform(3)",
                    """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:03.000000Z	null
                            2024-01-01T00:00:04.000000Z	40.0
                            """
            );
            assertQueryRows(
                    "SELECT * FROM (SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(3)) WHERE v>15 ORDER BY v",
                    """
                            ts	v
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:04.000000Z	40.0
                            """
            );
        });
    }

    @Test
    public void testCompletedAggregateDistinctAndWindowProjections() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT ts,sum(v) v FROM lp_subsample GROUP BY ts SUBSAMPLE uniform(3)",
                    """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:06.000000Z	20.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,sum(v) v FROM lp_subsample GROUP BY ts SUBSAMPLE cadence(2) ORDER BY v",
                    """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:05.000000Z	30.0
                            2024-01-01T00:00:03.000000Z	null
                            """
            );
            assertQueryRows(
                    "SELECT DISTINCT ts,v FROM lp_subsample SUBSAMPLE uniform(3) ORDER BY v",
                    """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:04.000000Z	40.0
                            """
            );
            assertQueryRows(
                    "SELECT ts,row_number() OVER(ORDER BY ts) rn FROM lp_subsample SUBSAMPLE uniform(3)",
                    """
                            ts	rn
                            2024-01-01T00:00:01.000000Z	1
                            2024-01-01T00:00:04.000000Z	4
                            2024-01-01T00:00:06.000000Z	6
                            """
            );
            assertQueryRows(
                    "SELECT ts,v FROM (SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(4)) SUBSAMPLE cadence(2)",
                    """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:06.000000Z	20.0
                            """
            );
        });
    }

    @Test
    public void testJoinsCteAndSetOccurrences() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT a.ts,a.v,b.x FROM lp_subsample a ASOF JOIN lp_subsample b SUBSAMPLE uniform(3) ORDER BY b.x",
                    """
                            ts	v	x
                            2024-01-01T00:00:04.000000Z	40.0	20
                            2024-01-01T00:00:06.000000Z	20.0	50
                            2024-01-01T00:00:01.000000Z	10.0	60
                            """
            );
            assertQueryRows(
                    "SELECT b.ts,a.* FROM lp_subsample a ASOF JOIN lp_subsample b SUBSAMPLE cadence(2)",
                    """
                            ts	id	v	x	ts1
                            2024-01-01T00:00:01.000000Z	1	10.0	60	2024-01-01T00:00:01.000000Z
                            2024-01-01T00:00:03.000000Z	3	null	40	2024-01-01T00:00:03.000000Z
                            2024-01-01T00:00:05.000000Z	5	30.0	30	2024-01-01T00:00:05.000000Z
                            2024-01-01T00:00:06.000000Z	6	20.0	50	2024-01-01T00:00:06.000000Z
                            """
            );
            assertQueryRows(
                    "WITH q AS(SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(3)) SELECT * FROM q UNION ALL SELECT * FROM q ORDER BY ts,v",
                    """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:06.000000Z	20.0
                            """
            );
            assertQueryRows(
                    "(SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(3)) UNION ALL (SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(2)) ORDER BY ts,v",
                    """
                            ts	v
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:01.000000Z	10.0
                            2024-01-01T00:00:03.000000Z	null
                            2024-01-01T00:00:04.000000Z	40.0
                            2024-01-01T00:00:05.000000Z	30.0
                            2024-01-01T00:00:06.000000Z	20.0
                            2024-01-01T00:00:06.000000Z	20.0
                            """
            );
        });
    }

    @Test
    public void testBindVariablesAndFactoryLifetime() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT ts,v FROM lp_subsample SUBSAMPLE cadence($1,$2)";
            bindVariableService.setLong(0, 2);
            bindVariableService.setLong(1, 7);
            RecordCursorFactory retained = null;
            try {
                final String expected = """
                        ts	v
                        2024-01-01T00:00:01.000000Z	10.0
                        2024-01-01T00:00:03.000000Z	null
                        2024-01-01T00:00:05.000000Z	30.0
                        2024-01-01T00:00:06.000000Z	20.0
                        """;
                final String expectedPlan;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    assertResult(retained, expected);
                    expectedPlan = plan(retained);
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_subsample", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                }
                assertResult(retained, expected);
                TestUtils.assertEquals(expectedPlan, plan(retained));
                bindVariableService.setLong(0, 1);
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                     RecordCursorFactory all = compiler.compile("SELECT ts,v FROM lp_subsample", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(retained, print(all));
                }
                bindVariableService.setLong(0, 0);
                try (RecordCursor ignored = retained.getCursor(sqlExecutionContext)) {
                    Assert.fail("stride must be rejected on reopen");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "stride must be at least 1");
                }
                bindVariableService.setLong(0, 2);
                assertResult(retained, expected);
            } finally {
                Misc.free(retained);
            }
            bindVariableService.setLong(0, 3);
            assertQueryRows("SELECT ts,v FROM lp_subsample SUBSAMPLE uniform($1)", """
                    ts	v
                    2024-01-01T00:00:01.000000Z	10.0
                    2024-01-01T00:00:04.000000Z	40.0
                    2024-01-01T00:00:06.000000Z	20.0
                    """);
        });
    }

    @Test
    public void testValidationAndRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_subsample_plain(id INT,ts TIMESTAMP)");
            assertQuery("SELECT v FROM lp_subsample SUBSAMPLE uniform(3)").noLeakCheck().fails(27, "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
            assertQuery("SELECT ts::TIMESTAMP ts,v FROM lp_subsample SUBSAMPLE uniform(3)").noLeakCheck().fails(44, "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
            assertQuery("SELECT v FROM lp_subsample SUBSAMPLE uniform(3) ORDER BY ts").noLeakCheck().fails(27, "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
            assertQuery("SELECT * FROM lp_subsample_plain SUBSAMPLE uniform(3)").noLeakCheck().fails(33, "SUBSAMPLE requires a designated timestamp column; the query source has no designated timestamp");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(1)").noLeakCheck().fails(48, "target points must be at least 2");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(NULL)").noLeakCheck().fails(48, "target point count must be set");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(2.5)").noLeakCheck().fails(48, "integer expected for target point count");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(id)").noLeakCheck().fails(48, "target point count must be a constant or bind variable");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE uniform(2,3)").noLeakCheck().fails(40, "uniform() requires exactly 1 argument: target points");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(0)").noLeakCheck().fails(48, "stride must be at least 1");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(2,id)").noLeakCheck().fails(50, "seed must be a constant, bind variable, or NULL");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE cadence(2,'bad')").noLeakCheck().fails(50, "integer or NULL expected for seed");
            assertQuery("SELECT ts,v FROM lp_subsample SUBSAMPLE unknown(2)").noLeakCheck().fails(40, "unknown subsample method: unknown. Supported methods: lttb, m4, minmax, uniform, cadence, sdt");
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_subsample(id INT,v DOUBLE,x INT,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_subsample VALUES(1,10.0,60,'2024-01-01T00:00:01'),"
                + "(2,50.0,10,'2024-01-01T00:00:02'),(3,NULL,40,'2024-01-01T00:00:03'),"
                + "(4,40.0,20,'2024-01-01T00:00:04'),(5,30.0,30,'2024-01-01T00:00:05'),"
                + "(6,20.0,50,'2024-01-01T00:00:06')");
    }

    private boolean hasSubsample(LogicalPlan plan) {
        if (plan instanceof WindowPlan) {
            final WindowPlan window = (WindowPlan) plan;
            for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
                if (window.getSpecs().getQuick(i).isSubsampleKeepFlag()) {
                    return true;
                }
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (hasSubsample(plan.inputAt(i))) {
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
