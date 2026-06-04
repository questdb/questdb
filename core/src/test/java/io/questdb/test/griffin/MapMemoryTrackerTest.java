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
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware map family ({@link io.questdb.cairo.map.OrderedMap},
 * {@link io.questdb.cairo.map.Unordered4Map}, {@link io.questdb.cairo.map.Unordered8Map},
 * {@link io.questdb.cairo.map.UnorderedVarcharMap}).
 * <p>
 * GROUP BY, DISTINCT and hash-join factories construct their maps with deferred
 * allocation (lazy mode) and bind the active workload's MemoryTracker before
 * the first cursor open. The malloc/free pairs that follow are charged
 * symmetrically to the per-query counter; a runaway query crosses the limit
 * and fails at the offending allocation site.
 * <p>
 * The per-query limit is applied per test in {@link #setUp()} via
 * {@code node1.setProperty} so it survives the per-test override reset; the
 * provider reads it live on each tracker acquisition. Tests that should breach
 * the limit use large workloads; tests that should succeed use small ones.
 */
public class MapMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // 256 KiB clears the light hash join's 128 KiB LongChain initial page --
        // now tracker-charged -- for the small-build success case, while the
        // large-workload breach tests (tens of thousands of keys / rows) still
        // exceed it.
        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
    }

    @Test
    public void testGroupByFailsOnLargeKeySet() throws Exception {
        // Pin the synchronous GROUP BY path so the breach lands at a single deterministic
        // site in the OrderedMap. Parallel GROUP BY is tracker-aware too (AsyncGroupByAtom
        // binds the tracker on its fragments and allocators), but a breach inside its
        // shard-merge job surfaces as a cancellation rather than the per-query OOM, so the
        // sync path keeps this test's assertion stable.
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
            assertQuery("SELECT k, sum(v) FROM tab GROUP BY k ORDER BY k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\tsum\n" +
                            "0\t1050\n" +
                            "1\t970\n" +
                            "2\t990\n" +
                            "3\t1010\n" +
                            "4\t1030\n");
        });
    }

    @Test
    public void testHashJoinFactoryBuiltButNeverExecutedDoesNotLeak() throws Exception {
        // A light hash join over a varchar key builds its lazy UnorderedVarcharMap
        // when the cursor is constructed at codegen time, but the cursor's close()
        // frees that map only when of() has run (the isOpen guard). A query compiled
        // but never executed - which happens repeatedly during server boot - must
        // therefore allocate nothing at map construction; otherwise the unopened
        // map's key arena and GROUP BY allocator chunk index leak (under tag
        // NATIVE_GROUP_BY_FUNCTION) when its owner skips close().
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master AS (SELECT cast(x AS varchar) k FROM long_sequence(20))");
            execute("CREATE TABLE slave AS (SELECT cast(x AS varchar) k, x AS v FROM long_sequence(20))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("SELECT master.k, slave.v FROM master JOIN slave ON k", sqlExecutionContext);
                // Build the factory (and its inner hash-join cursor + lazy map), then
                // close without ever calling getCursor(): of() never binds a tracker
                // or reopens, so the cursor's close() leaves the unopened map untouched.
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    Assert.assertNotNull(factory);
                }
            }
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
            assertQuery("SELECT count(*) FROM (SELECT master.k, slave.v FROM master JOIN slave ON k)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n20\n");
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
