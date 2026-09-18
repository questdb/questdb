/*******************************************************************************
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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowMapGroups;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the functions' record requirements and the LIGHT executor's positioning work in both
 * passes. Uniform and cadence need no input record even when sorted; value-reading selectors
 * still need one in pass1. Partitioned avg also needs each row's partition key in pass2.
 */
public class WindowPass2BaseRecordTest extends AbstractCairoTest {

    private static final String DDL = "CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v DOUBLE) TIMESTAMP(ts)";
    private static final String INSERT = """
            INSERT INTO t VALUES
            (1::TIMESTAMP, 'a', 1.0),
            (2::TIMESTAMP, 'b', 2.0),
            (3::TIMESTAMP, 'a', 3.0),
            (4::TIMESTAMP, 'b', 4.0),
            (5::TIMESTAMP, 'a', 5.0),
            (6::TIMESTAMP, 'b', 6.0)
            """;

    @Test
    public void testCadenceMixedWithRecordReadingPass2FunctionStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            // cadence(2) keeps ordinals {0, 2, 4} plus the pinned last row 5.
            assertPositioning(
                    "SELECT ts, sym, v, cadence(2) OVER (ORDER BY ts) keep, avg(v) OVER (PARTITION BY sym) a FROM t",
                    "avg", true, true, 6,
                    """
                            ts\tsym\tv\tkeep\ta
                            1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\t3.0
                            1970-01-01T00:00:00.000002Z\tb\t2.0\tfalse\t4.0
                            1970-01-01T00:00:00.000003Z\ta\t3.0\ttrue\t3.0
                            1970-01-01T00:00:00.000004Z\tb\t4.0\tfalse\t4.0
                            1970-01-01T00:00:00.000005Z\ta\t5.0\ttrue\t3.0
                            1970-01-01T00:00:00.000006Z\tb\t6.0\ttrue\t4.0
                            """
            );
        });
    }

    @Test
    public void testCadenceOrderedPassesDoNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            String query = "SELECT ts, v, cadence(2) OVER (ORDER BY v DESC) keep FROM t";
            assertQuery(query).noLeakCheck().assertsPlanContaining("orderedFunctions: [[v desc]");
            assertPositioning(
                    query, "cadence", false, false, 0,
                    """
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\ttrue
                            1970-01-01T00:00:00.000003Z\t3.0\tfalse
                            1970-01-01T00:00:00.000004Z\t4.0\ttrue
                            1970-01-01T00:00:00.000005Z\t5.0\tfalse
                            1970-01-01T00:00:00.000006Z\t6.0\ttrue
                            """
            );
        });
    }

    @Test
    public void testCadencePass2DoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            assertPositioning(
                    "SELECT ts, v, cadence(2) OVER (ORDER BY ts) keep FROM t",
                    "cadence", false, false, 0,
                    """
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\ttrue
                            1970-01-01T00:00:00.000004Z\t4.0\tfalse
                            1970-01-01T00:00:00.000005Z\t5.0\ttrue
                            1970-01-01T00:00:00.000006Z\t6.0\ttrue
                            """
            );
        });
    }

    @Test
    public void testOrderedRecordRequirementsAreIndependent() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            // The compiler walks sort groups in hash order, not SELECT-list order. Swap
            // which function owns each sort key, and verify that both group orders occur.
            boolean hasSelectorFirst = false;
            boolean hasReaderFirst = false;
            for (int order = 0; order < 2; order++) {
                boolean isSelectorAscending = order == 0;
                String query = """
                        SELECT ts, uniform(3) OVER (ORDER BY v %s) keep,
                               avg(v) OVER (PARTITION BY sym ORDER BY v %s
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a
                        FROM t
                        """.formatted(isSelectorAscending ? "ASC" : "DESC", isSelectorAscending ? "DESC" : "ASC");
                try (RecordCursorFactory factory = select(query)) {
                    TextPlanSink sink = new TextPlanSink();
                    sink.of(factory, sqlExecutionContext);
                    String plan = sink.getSink().toString();
                    int selectorIndex = plan.indexOf("=> [uniform(");
                    int readerIndex = plan.indexOf("=> [avg(");
                    Assert.assertTrue(plan, selectorIndex >= 0 && readerIndex >= 0);
                    hasSelectorFirst |= selectorIndex < readerIndex;
                    hasReaderFirst |= readerIndex < selectorIndex;
                }
                // Only avg's group positions rows: six in pass1 and six in pass2.
                assertPositioning(
                        query, "avg", true, true, 12,
                        isSelectorAscending ? """
                                ts\tkeep\ta
                                1970-01-01T00:00:00.000001Z\ttrue\t3.0
                                1970-01-01T00:00:00.000002Z\tfalse\t4.0
                                1970-01-01T00:00:00.000003Z\tfalse\t3.0
                                1970-01-01T00:00:00.000004Z\ttrue\t4.0
                                1970-01-01T00:00:00.000005Z\tfalse\t3.0
                                1970-01-01T00:00:00.000006Z\ttrue\t4.0
                                """ : """
                                ts\tkeep\ta
                                1970-01-01T00:00:00.000001Z\ttrue\t3.0
                                1970-01-01T00:00:00.000002Z\tfalse\t4.0
                                1970-01-01T00:00:00.000003Z\ttrue\t3.0
                                1970-01-01T00:00:00.000004Z\tfalse\t4.0
                                1970-01-01T00:00:00.000005Z\tfalse\t3.0
                                1970-01-01T00:00:00.000006Z\ttrue\t4.0
                                """
                );
            }
            Assert.assertTrue("must cover a selector group before a record-reading group", hasSelectorFirst);
            Assert.assertTrue("must cover a record-reading group before a selector group", hasReaderFirst);
        });
    }

    @Test
    public void testOrderedSelectorsIncludeNullSortKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute("""
                    INSERT INTO t VALUES
                    (1::TIMESTAMP, 'a', 1.0),
                    (2::TIMESTAMP, 'b', NULL),
                    (3::TIMESTAMP, 'a', 3.0),
                    (4::TIMESTAMP, 'b', 4.0),
                    (5::TIMESTAMP, 'a', NULL),
                    (6::TIMESTAMP, 'b', 6.0)
                    """);
            // DOUBLE NULLs sort first in descending order. Pin that traversal independently
            // of the window functions: uniform keeps ordinals 0, 3, 5; cadence keeps 0, 2, 4, 5.
            assertQuery("SELECT ts::LONG AS t, v FROM t ORDER BY v DESC, ts DESC").expectSize().returns("""
                    t\tv
                    5\tnull
                    2\tnull
                    6\t6.0
                    4\t4.0
                    3\t3.0
                    1\t1.0
                    """);
            assertPositioning(
                    """
                            SELECT ts, v, uniform(3) OVER (ORDER BY v DESC, ts DESC) u,
                                   cadence(2) OVER (ORDER BY v DESC, ts DESC) c
                            FROM t
                            """,
                    "uniform", false, false, 0,
                    """
                            ts\tv\tu\tc
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue\ttrue
                            1970-01-01T00:00:00.000002Z\tnull\tfalse\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\tfalse\ttrue
                            1970-01-01T00:00:00.000004Z\t4.0\ttrue\tfalse
                            1970-01-01T00:00:00.000005Z\tnull\ttrue\ttrue
                            1970-01-01T00:00:00.000006Z\t6.0\tfalse\ttrue
                            """
            );
        });
    }

    @Test
    public void testOrderedSubsampleDoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            for (int method = 0; method < 2; method++) {
                boolean isUniform = method == 0;
                String functionName = isUniform ? "uniform" : "cadence";
                String query = "SELECT ts, v FROM (SELECT ts, v FROM t ORDER BY ts DESC LIMIT 6) SUBSAMPLE "
                        + (isUniform ? "uniform(3)" : "cadence(2)");
                assertQuery(query).noLeakCheck().assertsPlanContaining("CachedWindowLightSelect");
                assertQuery(query).noLeakCheck().assertsPlanContaining("orderedFunctions: [[ts]");
                assertPositioning(
                        query, functionName, false, false, 0, isUniform ? 3 : 4, true,
                        isUniform ? """
                                ts\tv
                                1970-01-01T00:00:00.000006Z\t6.0
                                1970-01-01T00:00:00.000004Z\t4.0
                                1970-01-01T00:00:00.000001Z\t1.0
                                """ : """
                                ts\tv
                                1970-01-01T00:00:00.000006Z\t6.0
                                1970-01-01T00:00:00.000005Z\t5.0
                                1970-01-01T00:00:00.000003Z\t3.0
                                1970-01-01T00:00:00.000001Z\t1.0
                                """
                );
            }
        });
    }

    @Test
    public void testOrderedUniformAndSeededCadenceDoNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            // Seed 1 gives cadence an offset of 1, so both selectors keep ordinals 0, 3, 5.
            assertPositioning(
                    """
                            SELECT ts, v, uniform(3) OVER (ORDER BY v DESC) u,
                                   cadence(2, 1) OVER (ORDER BY v DESC) c
                            FROM t
                            """,
                    "cadence", false, false, 0,
                    """
                            ts\tv\tu\tc
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\ttrue\ttrue
                            1970-01-01T00:00:00.000004Z\t4.0\tfalse\tfalse
                            1970-01-01T00:00:00.000005Z\t5.0\tfalse\tfalse
                            1970-01-01T00:00:00.000006Z\t6.0\ttrue\ttrue
                            """
            );
        });
    }

    @Test
    public void testSdtMixedWithRecordReadingPass2FunctionStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            // v is a clean ramp: sdt keeps only the endpoints.
            assertPositioning(
                    "SELECT ts, sym, v, sdt(ts, v, 0.5) OVER (ORDER BY ts) keep, avg(v) OVER (PARTITION BY sym) a FROM t",
                    "avg", true, true, 6,
                    """
                            ts\tsym\tv\tkeep\ta
                            1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\t3.0
                            1970-01-01T00:00:00.000002Z\tb\t2.0\tfalse\t4.0
                            1970-01-01T00:00:00.000003Z\ta\t3.0\tfalse\t3.0
                            1970-01-01T00:00:00.000004Z\tb\t4.0\tfalse\t4.0
                            1970-01-01T00:00:00.000005Z\ta\t5.0\tfalse\t3.0
                            1970-01-01T00:00:00.000006Z\tb\t6.0\ttrue\t4.0
                            """
            );
        });
    }

    @Test
    public void testSdtPartitionedPass2DoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            assertPositioning(
                    "SELECT ts, sym, v, sdt(ts, v, 0.5) OVER (PARTITION BY sym ORDER BY ts) keep FROM t",
                    "sdt", true, false, 0,
                    """
                            ts\tsym\tv\tkeep
                            1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\tb\t2.0\ttrue
                            1970-01-01T00:00:00.000003Z\ta\t3.0\tfalse
                            1970-01-01T00:00:00.000004Z\tb\t4.0\tfalse
                            1970-01-01T00:00:00.000005Z\ta\t5.0\ttrue
                            1970-01-01T00:00:00.000006Z\tb\t6.0\ttrue
                            """
            );
        });
    }

    @Test
    public void testSdtUnpartitionedPass2DoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            assertPositioning(
                    "SELECT ts, v, sdt(ts, v, 0.5) OVER (ORDER BY ts) keep FROM t",
                    "sdt", true, false, 0,
                    """
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\tfalse
                            1970-01-01T00:00:00.000004Z\t4.0\tfalse
                            1970-01-01T00:00:00.000005Z\t5.0\tfalse
                            1970-01-01T00:00:00.000006Z\t6.0\ttrue
                            """
            );
        });
    }

    @Test
    public void testUniformMixedWithRecordReadingPass2FunctionStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            // uniform(3) keeps ordinals {0, 3, 5}: (i*5 + 1) / 2 for i in 0..2.
            assertPositioning(
                    "SELECT ts, sym, v, uniform(3) OVER (ORDER BY ts) keep, avg(v) OVER (PARTITION BY sym) a FROM t",
                    "avg", true, true, 6,
                    """
                            ts\tsym\tv\tkeep\ta
                            1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\t3.0
                            1970-01-01T00:00:00.000002Z\tb\t2.0\tfalse\t4.0
                            1970-01-01T00:00:00.000003Z\ta\t3.0\tfalse\t3.0
                            1970-01-01T00:00:00.000004Z\tb\t4.0\ttrue\t4.0
                            1970-01-01T00:00:00.000005Z\ta\t5.0\tfalse\t3.0
                            1970-01-01T00:00:00.000006Z\tb\t6.0\ttrue\t4.0
                            """
            );
        });
    }

    @Test
    public void testUniformOrderedMixedWithRecordReadingPass1FunctionStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            // Both functions opt out of pass2 positioning, but sdt still reads pass1's rows.
            assertPositioning(
                    """
                            SELECT ts, v, uniform(3) OVER (ORDER BY v) keep,
                                   sdt(ts, v, 0.5) OVER (ORDER BY v) s
                            FROM t
                            """,
                    "sdt", true, false, 6,
                    """
                            ts\tv\tkeep\ts
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\tfalse\tfalse
                            1970-01-01T00:00:00.000004Z\t4.0\ttrue\tfalse
                            1970-01-01T00:00:00.000005Z\t5.0\tfalse\tfalse
                            1970-01-01T00:00:00.000006Z\t6.0\ttrue\ttrue
                            """
            );
        });
    }

    @Test
    public void testUniformOrderedMixedWithRecordReadingPass2FunctionStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            String query = """
                    SELECT ts, sym, v, uniform(3) OVER (ORDER BY v DESC) keep,
                           avg(v) OVER (PARTITION BY sym ORDER BY v DESC
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a
                    FROM t
                    """;
            assertQuery(query).noLeakCheck().assertsPlanContaining("orderedFunctions: [[v desc]");
            // Both functions share a sorted traversal. Pass1 positions six rows; avg forces
            // another six positions in pass2, despite uniform opting out.
            assertPositioning(
                    query, "avg", true, true, 12,
                    """
                            ts\tsym\tv\tkeep\ta
                            1970-01-01T00:00:00.000001Z\ta\t1.0\ttrue\t3.0
                            1970-01-01T00:00:00.000002Z\tb\t2.0\tfalse\t4.0
                            1970-01-01T00:00:00.000003Z\ta\t3.0\ttrue\t3.0
                            1970-01-01T00:00:00.000004Z\tb\t4.0\tfalse\t4.0
                            1970-01-01T00:00:00.000005Z\ta\t5.0\tfalse\t3.0
                            1970-01-01T00:00:00.000006Z\tb\t6.0\ttrue\t4.0
                            """
            );
        });
    }

    @Test
    public void testUniformOrderedPassesDoNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            String query = "SELECT ts, v, uniform(3) OVER (ORDER BY v DESC) keep FROM t";
            assertQuery(query).noLeakCheck().assertsPlanContaining("orderedFunctions: [[v desc]");
            // Unlike ORDER BY ts, this sort cannot reuse the incoming timestamp order,
            // but neither pass reads the base record.
            assertPositioning(
                    query, "uniform", false, false, 0,
                    """
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\ttrue
                            1970-01-01T00:00:00.000004Z\t4.0\tfalse
                            1970-01-01T00:00:00.000005Z\t5.0\tfalse
                            1970-01-01T00:00:00.000006Z\t6.0\ttrue
                            """
            );
        });
    }

    @Test
    public void testUniformOrderedWithMapStateNeedsBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            String query = """
                    SELECT ts, v, uniform(3) OVER (ORDER BY v DESC) keep,
                           avg(v) OVER w a, sum(v) OVER w s
                    FROM t
                    WINDOW w AS (PARTITION BY sym ORDER BY v DESC
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    """;
            for (int fusion = 0; fusion < 2; fusion++) {
                boolean isFusionEnabled = fusion == 0;
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, isFusionEnabled ? "true" : "false");
                try (RecordCursorFactory factory = select(query)) {
                    CachedWindowLightRecordCursorFactory lightFactory = findLightFactory(factory);
                    Assert.assertNotNull(lightFactory);
                    CachedWindowMapGroups groups = lightFactory.getWindowMapGroups();
                    Assert.assertNotNull(groups);
                    Assert.assertEquals(isFusionEnabled ? 1 : 0, groups.getStates().size());
                    if (isFusionEnabled) {
                        Assert.assertEquals(1, groups.getOrderedStates(0).size());
                    }
                }
                assertPositioning(
                        query, "avg", true, true, 12,
                        """
                                ts\tv\tkeep\ta\ts
                                1970-01-01T00:00:00.000001Z\t1.0\ttrue\t3.0\t9.0
                                1970-01-01T00:00:00.000002Z\t2.0\tfalse\t4.0\t12.0
                                1970-01-01T00:00:00.000003Z\t3.0\ttrue\t3.0\t9.0
                                1970-01-01T00:00:00.000004Z\t4.0\tfalse\t4.0\t12.0
                                1970-01-01T00:00:00.000005Z\t5.0\tfalse\t3.0\t9.0
                                1970-01-01T00:00:00.000006Z\t6.0\ttrue\t4.0\t12.0
                                """
                );
            }
        });
    }

    @Test
    public void testUniformPass2DoesNotNeedBaseRecord() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(INSERT);
            assertPositioning(
                    "SELECT ts, v, uniform(3) OVER (ORDER BY ts) keep FROM t",
                    "uniform", false, false, 0,
                    """
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\tfalse
                            1970-01-01T00:00:00.000004Z\t4.0\ttrue
                            1970-01-01T00:00:00.000005Z\t5.0\tfalse
                            1970-01-01T00:00:00.000006Z\t6.0\ttrue
                            """
            );
        });
    }

    private void assertPositioning(
            String query,
            String functionName,
            boolean isExpectedPass1RecordRequired,
            boolean isExpectedPass2RecordRequired,
            long expectedPositioningCalls,
            String expected
    ) throws Exception {
        assertPositioning(query, functionName, isExpectedPass1RecordRequired, isExpectedPass2RecordRequired,
                expectedPositioningCalls, 6, false, expected);
    }

    private void assertPositioning(
            String query,
            String functionName,
            boolean isExpectedPass1RecordRequired,
            boolean isExpectedPass2RecordRequired,
            long expectedPositioningCalls,
            long expectedRowCount,
            boolean isDescending,
            String expected
    ) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            CachedWindowLightRecordCursorFactory lightFactory = findLightFactory(factory);
            Assert.assertNotNull("expected a cached LIGHT window factory for: " + query, lightFactory);
            ObjList<WindowFunction> functions = lightFactory.getAllWindowFunctions();
            WindowFunction match = null;
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (functionName.equals(functions.getQuick(i).getName())) {
                    Assert.assertNull("more than one '" + functionName + "' window function", match);
                    match = functions.getQuick(i);
                }
            }
            Assert.assertNotNull("no window function named '" + functionName + "' in: " + query, match);
            Assert.assertEquals(WindowFunction.TWO_PASS, match.getPassCount());
            Assert.assertEquals(
                    functionName + "().isPass1RecordRequired()",
                    isExpectedPass1RecordRequired,
                    match.isPass1RecordRequired()
            );
            Assert.assertEquals(
                    functionName + "().pass2NeedsBaseRecord()",
                    isExpectedPass2RecordRequired,
                    match.pass2NeedsBaseRecord()
            );

            RecordCursor.Counter positioningCalls = new RecordCursor.Counter();
            lightFactory.wrapBaseFactory(base -> new CountingRecordCursorFactory(base, positioningCalls));
            // Check both first open and reopen. calculateSize computes the window without
            // emitting output rows, whose legitimate recordAt calls would obscure pass work.
            for (int open = 0; open < 2; open++) {
                positioningCalls.clear();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    RecordCursor.Counter rowCount = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), rowCount);
                    Assert.assertEquals(expectedRowCount, rowCount.get());
                    Assert.assertEquals("base positioning during computation: " + query, expectedPositioningCalls, positioningCalls.get());
                    Assert.assertFalse(cursor.hasNext());

                    cursor.toTop();
                    rowCount.clear();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), rowCount);
                    Assert.assertEquals(expectedRowCount, rowCount.get());
                    Assert.assertEquals("toTop must not recompute the window", expectedPositioningCalls, positioningCalls.get());
                }
            }
            // Exercise results, re-reads and random access through the same decorated plan.
            QueryAssertion assertion = assertFactory(factory).withContext(sqlExecutionContext)
                    .expectSize(!match.isSubsampleKeepFlag());
            if (isDescending) {
                assertion.timestampDesc("ts");
            } else {
                assertion.timestamp("ts");
            }
            assertion.returns(expected);
        }
    }

    private static CachedWindowLightRecordCursorFactory findLightFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof CachedWindowLightRecordCursorFactory lightFactory) {
                return lightFactory;
            }
        }
        return null;
    }

    private static class CountingRecordCursor implements RecordCursor {
        private final Counter positioningCalls;
        private RecordCursor base;

        private CountingRecordCursor(Counter positioningCalls) {
            this.positioningCalls = positioningCalls;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            base.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            RecordCursor base = this.base;
            this.base = null;
            Misc.free(base);
        }

        @Override
        public void expectLimitedIteration() {
            base.expectLimitedIteration();
        }

        @Override
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return base.hasNext();
        }

        @Override
        public boolean isUsingIndex() {
            return base.isUsingIndex();
        }

        @Override
        public void longTopK(DirectLongLongSortedList list, int columnIndex) {
            base.longTopK(list, columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return base.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            positioningCalls.inc();
            base.recordAt(record, atRowId);
        }

        @Override
        public void setParentUsedColumns(IntHashSet columnIndexes) {
            base.setParentUsedColumns(columnIndexes);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            base.setParquetDecodeHint(hint);
        }

        @Override
        public void setRecordAtRows(RowIdSource source) {
            base.setRecordAtRows(source);
        }

        @Override
        public long size() {
            return base.size();
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            base.skipRows(rowCount, maxRowsAfterSkip);
        }

        @Override
        public void toTop() {
            base.toTop();
        }

        private void of(RecordCursor base) {
            this.base = base;
        }
    }

    private static class CountingRecordCursorFactory extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        private final CountingRecordCursor cursor;

        private CountingRecordCursorFactory(RecordCursorFactory base, RecordCursor.Counter positioningCalls) {
            super(base.getMetadata());
            this.base = base;
            this.cursor = new CountingRecordCursor(positioningCalls);
        }

        @Override
        public boolean followedOrderByAdvice() {
            return base.followedOrderByAdvice();
        }

        @Override
        public String getBaseColumnName(int idx) {
            return base.getBaseColumnName(idx);
        }

        @Override
        public RecordCursorFactory getBaseFactory() {
            return base;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            cursor.of(base.getCursor(executionContext));
            return cursor;
        }

        @Override
        public int getScanDirection() {
            return base.getScanDirection();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }

        @Override
        public boolean usesCompiledFilter() {
            return base.usesCompiledFilter();
        }

        @Override
        public boolean usesIndex() {
            return base.usesIndex();
        }

        @Override
        protected void _close() {
            // The window cursor owns the counting cursor; this factory owns only the base factory.
            Misc.free(base);
        }
    }
}
