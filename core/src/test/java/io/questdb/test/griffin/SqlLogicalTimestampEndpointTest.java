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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalTimestampEndpointTest extends AbstractCairoTest {
    @Test
    public void testNativeTimestampEndpointsBothPrecisions() throws Exception {
        assertMemoryLeak(() -> {
            for (String type : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                final String table = "lp_endpoint_" + type;
                createRows(table, type);
                for (String function : new String[]{"first", "min", "last", "max"}) {
                    final boolean isBackward = function.equals("last") || function.equals("max");
                    final String value = (isBackward ? "2024-01-04" : "2024-01-01")
                            + (type.equals("TIMESTAMP") ? "T00:00:00.000000Z\n" : "T00:00:00.000000000Z\n");
                    assertEndpoint("SELECT " + function + "(ts) endpoint FROM " + table, "endpoint\n" + value,
                            "Frame " + (isBackward ? "backward" : "forward") + " scan on: " + table);
                    assertEndpoint("SELECT " + function + "(t.ts) endpoint FROM " + table + " t", "endpoint\n" + value, "Limit value: 1");
                    assertEndpoint("SELECT " + function + "(ts) ts FROM " + table, "ts\n" + value, "Limit value: 1");
                }
            }
        });
    }

    @Test
    public void testEmptyAndFilteredInputsReturnOneNullRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_endpoint_empty(x INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            createRows("lp_endpoint", "TIMESTAMP");
            for (String function : new String[]{"first", "min", "last", "max"}) {
                final boolean isBackward = function.equals("last") || function.equals("max");
                final String call = function + "(ts) endpoint";
                final String[] emptyQueries = {
                        "SELECT " + call + " FROM lp_endpoint_empty",
                        "SELECT " + call + " FROM lp_endpoint_empty WHERE x>0",
                        "SELECT " + call + " FROM lp_endpoint WHERE x<0",
                        "SELECT " + call + " FROM lp_endpoint WHERE ts IN '2023'",
                        "SELECT " + call + " FROM lp_endpoint WHERE x<0 ORDER BY endpoint DESC",
                        "SELECT * FROM (SELECT " + call + " FROM lp_endpoint_empty)"
                };
                for (String sql : emptyQueries) {
                    assertQuery(sql).noLeakCheck().timestamp("endpoint").noRandomAccess().expectSize()
                            .withPlanContaining("GroupBy vectorized: false")
                            .returns("endpoint\n\n");
                }
                assertQuery("SELECT " + function + "(ts) FROM lp_endpoint_empty").noLeakCheck().timestamp(function).noRandomAccess().expectSize()
                        .returns(function + "\n\n");
                final String scan = isBackward ? "Frame backward scan on: lp_endpoint" : "Frame forward scan on: lp_endpoint";
                assertQuery("SELECT " + call + " FROM lp_endpoint WHERE x>=2 AND x<4").noLeakCheck().timestamp("endpoint").noRandomAccess().expectSize()
                        .withPlanContaining("GroupBy vectorized: false", scan)
                        .returns(isBackward ? "endpoint\n2024-01-03T00:00:00.000000Z\n" : "endpoint\n2024-01-02T00:00:00.000000Z\n");
                assertQuery("SELECT " + call + " FROM lp_endpoint WHERE ts>='2024-01-02' ORDER BY endpoint").noLeakCheck().timestamp("endpoint").noRandomAccess().expectSize()
                        .withPlanContaining("GroupBy vectorized: false", "Limit value: 1")
                        .returns(isBackward ? "endpoint\n2024-01-04T00:00:00.000000Z\n" : "endpoint\n2024-01-02T00:00:00.000000Z\n");
            }
        });
    }

    @Test
    public void testFilterLimitAdviceKeepsOrderAndSerialFallback() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_endpoint", "TIMESTAMP");
            final boolean wasParallel = sqlExecutionContext.isParallelFilterEnabled();
            try {
                for (int mode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setParallelFilterEnabled(true);
                    assertEndpoint(
                            "SELECT first(ts) endpoint FROM lp_endpoint WHERE x>=2",
                            """
                            endpoint
                            2024-01-02T00:00:00.000000Z
                            """,
                            null,
                            mode
                    );
                    assertEndpoint(
                            "SELECT last(ts) endpoint FROM lp_endpoint WHERE x<4",
                            """
                            endpoint
                            2024-01-03T00:00:00.000000Z
                            """,
                            null,
                            mode
                    );
                    assertEndpoint(
                            "SELECT ts FROM lp_endpoint WHERE x>=2 ORDER BY x DESC LIMIT 1",
                            """
                            ts
                            2024-01-04T00:00:00.000000Z
                            """,
                            null,
                            mode
                    );
                    assertEndpoint(
                            "SELECT ts FROM lp_endpoint WHERE x>=2 ORDER BY ts DESC LIMIT -2",
                            """
                            ts
                            2024-01-03T00:00:00.000000Z
                            2024-01-02T00:00:00.000000Z
                            """,
                            null,
                            mode
                    );
                    assertEndpoint(
                            "SELECT ts FROM lp_endpoint WHERE x>=2 LIMIT 1,3",
                            """
                            ts
                            2024-01-03T00:00:00.000000Z
                            2024-01-04T00:00:00.000000Z
                            """,
                            null,
                            mode
                    );
                    sqlExecutionContext.setParallelFilterEnabled(false);
                    assertEndpoint(
                            "SELECT first(ts) endpoint FROM lp_endpoint WHERE x>=2",
                            """
                            endpoint
                            2024-01-02T00:00:00.000000Z
                            """,
                            "Limit value: 1",
                            mode
                    );
                    assertEndpoint(
                            "SELECT last(ts) endpoint FROM lp_endpoint WHERE x<4",
                            """
                            endpoint
                            2024-01-03T00:00:00.000000Z
                            """,
                            "Limit value: 1",
                            mode
                    );
                }
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testAliasesRepeatedCallsAndScalarOutputs() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_endpoint", "TIMESTAMP");
            assertEndpoint(
                    "SELECT first(ts) a,first(ts) b FROM lp_endpoint",
                    """
                    a	b
                    2024-01-01T00:00:00.000000Z	2024-01-01T00:00:00.000000Z
                    """,
                    null
            );
            assertEndpoint(
                    "SELECT last(ts) a,last(ts) b FROM lp_endpoint",
                    """
                    a	b
                    2024-01-04T00:00:00.000000Z	2024-01-04T00:00:00.000000Z
                    """,
                    null
            );
            assertEndpoint(
                    "SELECT dateadd('h',1,first(ts)) shifted FROM lp_endpoint",
                    """
                    shifted
                    2024-01-01T01:00:00.000000Z
                    """,
                    null
            );
            assertEndpoint(
                    "SELECT first(ts) endpoint,42 value FROM lp_endpoint",
                    """
                    endpoint	value
                    2024-01-01T00:00:00.000000Z	42
                    """,
                    null
            );
            assertEndpoint(
                    "SELECT first(ts) endpoint FROM lp_endpoint ORDER BY endpoint DESC LIMIT 1",
                    """
                    endpoint
                    2024-01-01T00:00:00.000000Z
                    """,
                    null
            );
            assertEndpoint("SELECT last(ts) endpoint FROM lp_endpoint ORDER BY 1 LIMIT 0", "endpoint\n", null);
        });
    }

    @Test
    public void testCastAliasesAndEndpointsAfterCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_endpoint", "TIMESTAMP");
            final String[][] cases = {
                    {"SELECT ts FROM (SELECT ts FROM (SELECT ts::timestamp ts FROM lp_endpoint))", "ts\n2024-01-01T00:00:00.000000Z\n2024-01-02T00:00:00.000000Z\n2024-01-03T00:00:00.000000Z\n2024-01-04T00:00:00.000000Z\n"},
                    {"SELECT first(ts) endpoint FROM lp_endpoint", "endpoint\n2024-01-01T00:00:00.000000Z\n"},
                    {"SELECT ts endpoint FROM lp_endpoint", "endpoint\n2024-01-01T00:00:00.000000Z\n2024-01-02T00:00:00.000000Z\n2024-01-03T00:00:00.000000Z\n2024-01-04T00:00:00.000000Z\n"},
                    {"SELECT last(ts) endpoint FROM lp_endpoint", "endpoint\n2024-01-04T00:00:00.000000Z\n"},
                    {"SELECT ts::timestamp ts FROM (SELECT ts FROM (SELECT ts FROM lp_endpoint))", "ts\n2024-01-01T00:00:00.000000Z\n2024-01-02T00:00:00.000000Z\n2024-01-03T00:00:00.000000Z\n2024-01-04T00:00:00.000000Z\n"},
                    {"SELECT first(ts) endpoint FROM lp_endpoint", "endpoint\n2024-01-01T00:00:00.000000Z\n"}
            };
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (String[] c : cases) {
                    try (RecordCursorFactory factory = compiler.compile(c[0], sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, c[1]);
                    }
                }
            }
        });
    }

    @Test
    public void testDesignationIsAvailableToParentBinding() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_endpoint", "TIMESTAMP");
            assertEndpoint(
                    "SELECT * FROM (SELECT last(ts) endpoint FROM lp_endpoint)",
                    """
                    endpoint
                    2024-01-04T00:00:00.000000Z
                    """,
                    "Frame backward scan"
            );
            assertEndpoint(
                    "SELECT * FROM (SELECT first(ts) endpoint FROM lp_endpoint) q WHERE endpoint>='2024-01-01'",
                    """
                    endpoint
                    2024-01-01T00:00:00.000000Z
                    """,
                    null
            );
            assertEndpoint(
                    "SELECT a.endpoint,b.x FROM (SELECT first(ts) endpoint FROM lp_endpoint) a ASOF JOIN lp_endpoint b",
                    """
                    endpoint	x
                    2024-01-01T00:00:00.000000Z	1
                    """,
                    null
            );
            assertEndpoint(
                    "SELECT first(ts) endpoint FROM lp_endpoint UNION ALL SELECT last(ts) endpoint FROM lp_endpoint",
                    """
                    endpoint
                    2024-01-01T00:00:00.000000Z
                    2024-01-04T00:00:00.000000Z
                    """,
                    null
            );
            assertEndpoint(
                    "WITH q AS (SELECT first(ts) endpoint FROM lp_endpoint) SELECT * FROM q UNION ALL SELECT * FROM q",
                    """
                    endpoint
                    2024-01-01T00:00:00.000000Z
                    2024-01-01T00:00:00.000000Z
                    """,
                    null
            );
        });
    }

    @Test
    public void testOtherAggregatesKeepTheirGroupingBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_endpoint", "TIMESTAMP");
            assertGrouped(
                    "SELECT first(other) endpoint FROM lp_endpoint",
                    """
                    endpoint
                    2024-01-04T00:00:00.000000Z
                    """
            );
            assertGrouped(
                    "SELECT first(ts::TIMESTAMP) endpoint FROM lp_endpoint",
                    """
                    endpoint
                    2024-01-01T00:00:00.000000Z
                    """
            );
            assertGrouped(
                    "SELECT first(dateadd('h',1,ts)) endpoint FROM lp_endpoint",
                    """
                    endpoint
                    2024-01-01T01:00:00.000000Z
                    """
            );
            assertGrouped(
                    "SELECT first(ts),last(ts) FROM lp_endpoint",
                    """
                    first	last
                    2024-01-01T00:00:00.000000Z	2024-01-04T00:00:00.000000Z
                    """
            );
            assertGrouped(
                    "SELECT x,first(ts) endpoint FROM lp_endpoint GROUP BY x ORDER BY x",
                    """
                    x	endpoint
                    1	2024-01-01T00:00:00.000000Z
                    2	2024-01-02T00:00:00.000000Z
                    3	2024-01-03T00:00:00.000000Z
                    4	2024-01-04T00:00:00.000000Z
                    """
            );
            assertGrouped(
                    "SELECT first(ts) endpoint FROM (SELECT ts FROM lp_endpoint LIMIT 2)",
                    """
                    endpoint
                    2024-01-01T00:00:00.000000Z
                    """
            );
            assertGrouped(
                    "SELECT first(a.ts) endpoint FROM lp_endpoint a CROSS JOIN lp_endpoint b",
                    """
                    endpoint
                    2024-01-01T00:00:00.000000Z
                    """
            );
        });
    }

    @Test
    public void testUserLimitsRetainZeroNegativeRangeAndRuntimeSemantics() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_endpoint", "TIMESTAMP");
            for (String function : new String[]{"first", "last", "min", "max"}) {
                final boolean isBackward = function.equals("last") || function.equals("max");
                final String endpoint = isBackward ? "2024-01-04T00:00:00.000000Z\n" : "2024-01-01T00:00:00.000000Z\n";
                for (String limit : new String[]{"0", "2", "-1", "0,1", "1,2"}) {
                    final String rows = limit.equals("0") || limit.equals("1,2") ? "endpoint\n" : "endpoint\n" + endpoint;
                    assertEndpoint("SELECT " + function + "(ts) endpoint FROM lp_endpoint ORDER BY endpoint "
                            + (isBackward ? "ASC" : "DESC") + " LIMIT " + limit, rows, null);
                }
            }
            bindVariableService.setLong(0, 1);
            try (RecordCursorFactory factory = select("SELECT first(ts) endpoint FROM lp_endpoint ORDER BY endpoint DESC LIMIT $1")) {
                for (long limit : new long[]{1, 0, -1}) {
                    bindVariableService.setLong(0, limit);
                    assertResult(factory, limit == 0 ? "endpoint\n" : "endpoint\n2024-01-01T00:00:00.000000Z\n");
                }
            }
        });
    }

    @Test
    public void testRetainedEndpointReopensAfterCompilerReuseAndTableGrowth() throws Exception {
        assertMemoryLeak(() -> {
            createRows("lp_endpoint", "TIMESTAMP");
            RecordCursorFactory retained = null;
            try {
                final String originalPlan;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT last(ts) endpoint FROM lp_endpoint", sqlExecutionContext).getRecordCursorFactory();
                    originalPlan = planOf(retained);
                    try (RecordCursorFactory ignored = compiler.compile("SELECT first(other) value FROM lp_endpoint", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                Assert.assertEquals(originalPlan, planOf(retained));
                assertResult(retained, "endpoint\n2024-01-04T00:00:00.000000Z\n");
                execute("INSERT INTO lp_endpoint VALUES(5,'2024-01-05','2024-01-05')");
                assertResult(retained, "endpoint\n2024-01-05T00:00:00.000000Z\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void assertEndpoint(String sql, String rows, String requiredPlan) throws Exception {
        assertEndpoint(sql, rows, requiredPlan, SqlJitMode.JIT_MODE_DISABLED);
    }

    private void assertEndpoint(String sql, String rows, String requiredPlan, int jitMode) throws Exception {
        final int oldJitMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(jitMode);
        try (RecordCursorFactory factory = select(sql)) {
            if (requiredPlan != null) {
                TestUtils.assertContains(planOf(factory), requiredPlan);
            }
            assertResult(factory, rows);
        } finally {
            sqlExecutionContext.setJitMode(oldJitMode);
        }
    }

    private void assertGrouped(String sql, String rows) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final String plan = planOf(factory);
            TestUtils.assertContains(plan, "Group");
            TestUtils.assertNotContains(plan, "Limit value: 1");
            assertResult(factory, rows);
        }
    }

    private void assertResult(RecordCursorFactory factory, String rows) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(rows);
    }

    private void createRows(String table, String type) throws SqlException {
        execute("CREATE TABLE " + table + "(x INT,ts " + type + ",other " + type + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO " + table + " VALUES (1,'2024-01-01','2024-01-04'),(2,'2024-01-02',null),"
                + "(3,'2024-01-03','2024-01-02'),(4,'2024-01-04','2024-01-01')");
    }

    private String planOf(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        return sink.getSink().toString();
    }
}
