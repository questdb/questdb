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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware map family wired by PR 2.2 ({@link io.questdb.cairo.map.OrderedMap},
 * {@link io.questdb.cairo.map.Unordered4Map}, {@link io.questdb.cairo.map.Unordered8Map},
 * {@link io.questdb.cairo.map.UnorderedVarcharMap}).
 * <p>
 * GROUP BY, DISTINCT and hash-join factories construct their maps with deferred
 * allocation (lazy mode) and bind the active workload's MemoryTracker before
 * the first cursor open. The malloc/free pairs that follow are charged
 * symmetrically to the per-query counter; a runaway query crosses the limit
 * and fails at the offending allocation site.
 * <p>
 * The per-query limit is set in {@link #beforeClass()} because
 * {@code CairoEngine#getMemoryTrackerProvider} caches the
 * {@code PerQueryMemoryTrackerProvider} on first access with the limit then in
 * effect. Per-test overrides via {@code node1.setProperty} land too late on a
 * shared engine. Tests that should breach the limit use large workloads;
 * tests that should succeed use small ones.
 */
public class MapMemoryTrackerTest extends AbstractCairoTest {

    @BeforeClass
    public static void beforeClass() {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
    }

    @Test
    public void testGroupByFailsOnLargeKeySet() throws Exception {
        // Force the synchronous GROUP BY path: PR 2.2 wires the sync GroupByRecordCursorFactory
        // only. Parallel GROUP BY runs through GroupByMapFragment, which PR 2.2 does not
        // touch; once a follow-up PR wires it, this override can be lifted.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x AS k, x * 2 AS v FROM long_sequence(50_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT k, sum(v) FROM tab GROUP BY k", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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
        });
    }

    @Test
    public void testGroupBySucceedsOnSmallKeySet() throws Exception {
        // Sync GROUP BY on a small key set fits the per-query limit; the
        // tracker accounting holds malloc/free in balance and the query
        // returns the expected aggregates.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x % 5 AS k, x AS v FROM long_sequence(100))");
            drainWalQueue();
            assertSql(
                    "k\tsum\n" +
                            "0\t1050\n" +
                            "1\t970\n" +
                            "2\t990\n" +
                            "3\t1010\n" +
                            "4\t1030\n",
                    "SELECT k, sum(v) FROM tab GROUP BY k ORDER BY k"
            );
        });
    }

    @Test
    public void testHashJoinFailsOnLargeBuildSide() throws Exception {
        // Hash join's build map allocates against the per-query tracker. Both
        // tables are large so the smaller-side swap still leaves a sizable
        // build; the build's hash-directory growth trips the limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master AS (SELECT cast(x AS varchar) k FROM long_sequence(100_000))");
            execute("CREATE TABLE slave AS (SELECT cast(x AS varchar) k, x AS v FROM long_sequence(100_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT master.k, slave.v FROM master JOIN slave ON k", sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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
        });
    }

    @Test
    public void testHashJoinSucceedsOnSmallBuildSide() throws Exception {
        // Hash join with a small build fits the per-query limit and returns
        // the expected matches.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            execute("CREATE TABLE slave AS (SELECT cast(x AS varchar) k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            assertSql(
                    "count\n20\n",
                    "SELECT count(*) FROM (SELECT master.k, slave.v FROM master JOIN slave ON k)"
            );
        });
    }

    @Test
    public void testRepeatedCursorRunsReleaseAllocations() throws Exception {
        // Repeat the same GROUP BY many times to verify that close/reopen cycles
        // through the map family release every byte they allocated.
        // {@code assertMemoryLeak} around the loop is the load-bearing check;
        // a malloc/free asymmetry would manifest as a residual native allocation
        // count at the end of the test.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute("CREATE TABLE tab AS (SELECT x % 10 AS k, x AS v FROM long_sequence(100))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT k, sum(v) FROM tab GROUP BY k", sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(10, rows);
                    }
                }
            }
        });
    }
}
