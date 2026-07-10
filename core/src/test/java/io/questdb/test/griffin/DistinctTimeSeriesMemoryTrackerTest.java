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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.groupby.DistinctTimeSeriesRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Exercises the per-query memory limit through {@link DistinctTimeSeriesRecordCursorFactory}'s
 * {@code dataMap}. The factory is only reachable with the distinct-to-GROUP BY rewrite disabled,
 * which has no production property and is overridden to false on the {@link CairoTestConfiguration}
 * for this test in {@link #setUpStatic()}; plain SELECT DISTINCT otherwise
 * rewrites to (Async) GROUP BY. With the rewrite off, SELECT DISTINCT over a random-access,
 * designated-timestamp base routes here. The dataMap clears on every designated-timestamp change,
 * so it only grows under duplicated timestamps; a constant-timestamp table makes it grow unbounded.
 */
public class DistinctTimeSeriesMemoryTrackerTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Force DistinctTimeSeriesRecordCursorFactory: otherwise rewriteDistinct turns
        // SELECT DISTINCT into (Async) GROUP BY and this factory never runs. The flag has
        // no production property, so override it directly on the CairoConfiguration.
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean isSqlDistinctGroupByRewriteEnabled() {
                        return false;
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUpLimit() {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
    }

    @Test
    public void testFailsOnHighCardinality() throws Exception {
        // One shared timestamp: the dataMap never clears and grows with the distinct row
        // count until it trips the per-query limit.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab AS (" +
                            "  SELECT 0::timestamp ts, x v" +
                            "  FROM long_sequence(100_000)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            drainWalQueue();
            assertBreach("SELECT DISTINCT * FROM tab");
        });
    }

    @Test
    public void testRepeatedCursorRunsReleaseAllocations() throws Exception {
        // ~10 distinct rows per timestamp group: the dataMap grows then clears on each
        // timestamp change. Repeating the scan must release every byte each close frees and
        // each of() reopens. assertMemoryLeak around the loop catches a malloc/free asymmetry.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE tab AS (" +
                            "  SELECT ((x / 10) * 1_000_000L)::timestamp ts, x v" +
                            "  FROM long_sequence(1000)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT DISTINCT * FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                assertInTree(factory);
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        // v is globally unique, so all 1000 rows are distinct; without this a
                        // cursor returning zero/wrong rows would still pass the leak check.
                        Assert.assertEquals(1000, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testRoutesAndReturnsDistinctRows() throws Exception {
        // Monotonic timestamps: every row is distinct and the dataMap clears each step, so the scan
        // stays under the limit. The plan guard pins the run to DistinctTimeSeries; returns() self-leak-checks.
        assertQuery("SELECT DISTINCT * FROM tab")
                .ddl("CREATE TABLE tab AS (SELECT (x * 1_000_000L)::timestamp ts, (x % 3)::long v FROM long_sequence(6)) TIMESTAMP(ts) PARTITION BY DAY")
                .timestamp("ts")
                .withPlanContaining("DistinctTimeSeries")
                .returns("ts\tv\n" +
                        "1970-01-01T00:00:01.000000Z\t1\n" +
                        "1970-01-01T00:00:02.000000Z\t2\n" +
                        "1970-01-01T00:00:03.000000Z\t0\n" +
                        "1970-01-01T00:00:04.000000Z\t1\n" +
                        "1970-01-01T00:00:05.000000Z\t2\n" +
                        "1970-01-01T00:00:06.000000Z\t0\n");
    }

    @Test
    public void testExplicitTimestampRedesignationSurvivesDistinct() throws Exception {
        // An explicit timestamp() on the subquery must survive DISTINCT, and the redesignated column
        // must feed DistinctTimeSeriesRecordCursor's dedup fast path (which reads its row-adjacency
        // decision directly off the factory's own timestampIndex). NOTE on what this specific test
        // covers: this class disables the DISTINCT->GROUP-BY rewrite, so the query routes through
        // generateSelectDistinct, where ts2 reaches DistinctTimeSeries because the optimizer pushes
        // timestamp(ts2) down into the inner table scan (generateTableQuery) -- NOT via
        // applyExplicitTimestamp. It therefore validates the end-to-end DISTINCT-over-redesignation
        // behavior + dedup adjacency; the dedicated regression guard for THIS PR's generateSelectGroupBy
        // wiring is DistinctTest#testDistinctExplicitTimestampSurvivesGroupByRewrite (default config).
        assertQuery("SELECT DISTINCT * FROM (SELECT * FROM tab) timestamp(ts2)")
                .ddl(
                        "CREATE TABLE tab (ts TIMESTAMP, ts2 TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY",
                        "INSERT INTO tab VALUES " +
                                "('2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:01.000000Z', 1), " +
                                "('2024-01-01T00:01:00.000000Z', '2024-01-01T00:01:01.000000Z', 2)"
                )
                .timestamp("ts2")
                .withPlanContaining("DistinctTimeSeries")
                .returns("""
                        ts\tts2\tv
                        2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:01.000000Z\t1
                        2024-01-01T00:01:00.000000Z\t2024-01-01T00:01:01.000000Z\t2
                        """);
    }

    @Test
    public void testExplicitTimestampRedesignationDrivesDistinctAdjacency() throws Exception {
        // Unlike testExplicitTimestampRedesignationSurvivesDistinct (globally-unique rows that never
        // reach checkIfNotDupe), this exercises the fast-path dedup ADJACENCY under the redesignation.
        // ts2 forms a three-row group (10,10,10) then a singleton (20); the natural ts is unique per
        // row except the exact-duplicate pair. DistinctTimeSeriesRecordCursor emits a row unconditionally
        // when the DESIGNATED timestamp changes and only consults the dataMap within a run of equal
        // timestamps -- so it can only dedupe the duplicate against its partner, and keep the distinct
        // third row of the same ts2 group, if the fast path keys off ts2 (the redesignation reached it).
        // The output's designated timestamp is ts2, which a downstream temporal consumer would use.
        assertQuery("SELECT DISTINCT * FROM (SELECT * FROM tab) timestamp(ts2)")
                .ddl(
                        "CREATE TABLE tab (ts TIMESTAMP, ts2 TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY",
                        "INSERT INTO tab VALUES " +
                                "('2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:10.000000Z', 1), " +
                                "('2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:10.000000Z', 1), " +
                                "('2024-01-01T00:00:01.000000Z', '2024-01-01T00:00:10.000000Z', 2), " +
                                "('2024-01-01T00:00:02.000000Z', '2024-01-01T00:00:20.000000Z', 3)"
                )
                .timestamp("ts2")
                .withPlanContaining("DistinctTimeSeries")
                .returns("""
                        ts\tts2\tv
                        2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:10.000000Z\t1
                        2024-01-01T00:00:01.000000Z\t2024-01-01T00:00:10.000000Z\t2
                        2024-01-01T00:00:02.000000Z\t2024-01-01T00:00:20.000000Z\t3
                        """);
    }

    private static void assertBreach(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                assertInTree(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // drain until breach
                    }
                    Assert.fail("expected per-query memory breach");
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                }
            }
        }
    }

    private static void assertInTree(RecordCursorFactory factory) {
        RecordCursorFactory cur = factory;
        while (cur != null) {
            if (cur instanceof DistinctTimeSeriesRecordCursorFactory) {
                return;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        Assert.fail("expected DistinctTimeSeriesRecordCursorFactory in base chain of " + factory.getClass().getSimpleName());
    }
}
