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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises the per-query memory limit through {@link DistinctTimeSeriesRecordCursorFactory}'s
 * {@code dataMap}. The factory is only reachable with the distinct-to-GROUP BY rewrite disabled
 * ({@code cairo.sql.distinct.group.by.rewrite.enabled=false}); plain SELECT DISTINCT otherwise
 * rewrites to (Async) GROUP BY. With the rewrite off, SELECT DISTINCT over a random-access,
 * designated-timestamp base routes here. The dataMap clears on every designated-timestamp change,
 * so it only grows under duplicated timestamps; a constant-timestamp table makes it grow unbounded.
 */
public class DistinctTimeSeriesMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUpLimit() {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
        // Force DistinctTimeSeriesRecordCursorFactory: otherwise rewriteDistinct turns
        // SELECT DISTINCT into (Async) GROUP BY and this factory never runs.
        setProperty(PropertyKey.CAIRO_SQL_DISTINCT_GROUP_BY_REWRITE_ENABLED, "false");
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
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain
                        }
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
