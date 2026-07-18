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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.WindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.WindowJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * single-threaded WINDOW JOIN aggregation operators in
 * {@code io.questdb.griffin.engine.join}.
 * <p>
 * These are the synchronous siblings of the operators covered by
 * {@link ParallelWindowJoinMemoryTrackerTest}. The codegen routes to them
 * whenever the parallel WINDOW JOIN path is unavailable: a deployment with no
 * shared query workers, the {@code cairo.sql.parallel.window.join.enabled} knob
 * turned off (as these tests do), or a query shape the parallel path cannot take.
 * They reach the same unbounded native structures as the parallel variants - the
 * {@code GroupByAllocator}s backing group-by function state (e.g. {@code array_agg})
 * and, for the symbol-keyed fast path, the temporary slave row id / timestamp /
 * column lists - which the cursors bind to the per-query tracker in {@code of()}
 * before the build loop and free in {@code close()}.
 * <p>
 * A symbol equality join ({@code ON t.sym = p.sym}) routes to the fast
 * {@link WindowJoinFastRecordCursorFactory}; a join without it routes to the general
 * {@link WindowJoinRecordCursorFactory}. Within the fast factory a batch-computable
 * aggregate (e.g. {@code sum}) routes to the vectorized cursor and a non-batch one
 * (e.g. {@code array_agg}) to the scalar cursor; both carry their own allocators, so
 * both are exercised here. The {@code assertFactoryInTree} routing guard pins each test to
 * the synchronous factory, so a future change that drops the binding or re-routes to
 * the parallel path fails loudly rather than silently passing.
 * <p>
 * Each query runs on a dedicated {@link WorkerPool} via {@link TestUtils#execute},
 * which builds a fresh {@code CairoEngine} from the test configuration; the
 * per-query limit is therefore read fresh by every test and can be set in
 * {@link #setUp()}.
 */
public class SyncWindowJoinMemoryTrackerTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // 8 MiB: small enough that a wide-window array_agg fills past it during the
        // build loop, large enough for the success/leak cases to fit.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
        // Force the single-threaded WINDOW JOIN path.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        super.setUp();
    }

    @Test
    public void testKeyedWindowJoinArrayAggFailsOnLargeSet() throws Exception {
        // Keyed array_agg over a WINDOW JOIN routes through the scalar WindowJoinFastRecordCursor.
        // Each master row accumulates its matched slave prices into a list allocated through the
        // allocator the cursor binds to the per-query tracker in of(); the bump allocator is reset
        // only on toTop()/close(), so the build-loop growth trips the limit. Without the binding the
        // lists escape and the query completes, firing Assert.fail below.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: in
        // the build loop, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 40_000, 8);
                        createPrices(engine, sqlExecutionContext, 400_000, 8);
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 15 seconds PRECEDING AND 15 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinFastRecordCursorFactory.class);
                            assertQueryBreaches(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedWindowJoinOpenFailureReleasesAllocations() throws Exception {
        // A tiny limit breaches during the first drain (the chunk index is off the per-query tracker,
        // so nothing per-query-tracked allocates at open); the loop verifies each breach releases
        // allocations and leaves the factory reusable.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinFastRecordCursorFactory.class);
                            assertOpenFailureReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedWindowJoinReleasesAllocations() throws Exception {
        // A small keyed array_agg fits the per-query limit; the allocator and the temporary slave
        // lists are bound to the tracker on each open and must release every byte on close. Repeated
        // getCursor/close cycles, wrapped by assertMemoryLeak, would expose a malloc/free asymmetry.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 5_000, 8);
                        createPrices(engine, sqlExecutionContext, 50_000, 8);
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinFastRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedWindowJoinArrayAggFailsOnLargeSet() throws Exception {
        // Non-symbol array_agg over a WINDOW JOIN routes through the general WindowJoinRecordCursor.
        // Every slave row in the window matches, so each master row's list grows through the same
        // allocator bound in of(). The build-loop growth trips the limit; without the binding the
        // query completes and Assert.fail fires.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: in
        // the build loop, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 40_000, 8);
                        createPrices(engine, sqlExecutionContext, 400_000, 8);
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p " +
                                "RANGE BETWEEN 15 seconds PRECEDING AND 15 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinRecordCursorFactory.class);
                            assertQueryBreaches(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedWindowJoinCompileWithoutOpenDoesNotLeak() throws Exception {
        // A window-join factory compiled but never opened (plan caching, EXPLAIN, an aborted
        // execution) must still free the native resources its cursor holds. The cursor starts
        // isOpen=false so the first of() runs the tracker-bound reopen(); its lazy allocators hold
        // no backing until then, so a never-opened close() leaves nothing behind. assertMemoryLeak
        // catches a regression of that property for both the general and the fast factory.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.ts, array_agg(p.price) FROM trades t WINDOW JOIN prices p " +
                                        "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, WindowJoinRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.ts, array_agg(p.price) FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                        "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, WindowJoinFastRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedWindowJoinOpenFailureReleasesAllocations() throws Exception {
        // Non-symbol variant of testKeyedWindowJoinOpenFailureReleasesAllocations.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p " +
                                "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinRecordCursorFactory.class);
                            assertOpenFailureReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedWindowJoinReleasesAllocations() throws Exception {
        // Non-symbol variant of testKeyedWindowJoinReleasesAllocations.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 5_000, 8);
                        createPrices(engine, sqlExecutionContext, 50_000, 8);
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p " +
                                "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testVectorizedWindowJoinOpenFailureReleasesAllocations() throws Exception {
        // A batch-computable aggregate (sum) over a symbol-keyed WINDOW JOIN routes to the vectorized
        // WindowJoinFastVectRecordCursor, which carries its own allocator + slaveAllocator. A tiny
        // limit breaches during the first drain (the chunk index is off the per-query tracker, so
        // nothing per-query-tracked allocates at open); the slave data charged to it trips the limit.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        final String query = "SELECT t.ts, sum(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinFastRecordCursorFactory.class);
                            assertOpenFailureReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testVectorizedWindowJoinReleasesAllocations() throws Exception {
        // A small batch-computable aggregate (sum) over a symbol-keyed WINDOW JOIN exercises the
        // vectorized cursor's allocator + slaveAllocator (which back the column sink and timestamp
        // list); both are bound on each open and must release every byte on close. Repeated
        // getCursor/close cycles, wrapped by assertMemoryLeak, would expose a malloc/free asymmetry.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 5_000, 8);
                        createPrices(engine, sqlExecutionContext, 50_000, 8);
                        final String query = "SELECT t.ts, sum(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinFastRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertOpenFailureReleasesAllocations(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        for (int i = 0; i < 5; i++) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // lazy-map path: the chunk index is no longer per-query-tracked, so the
                    // breach lands during the reduce on the first drain, not at cursor open.
                }
                Assert.fail("expected a per-query memory breach at iteration " + i);
            } catch (CairoException e) {
                Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
            }
        }
    }

    private static void assertQueryBreaches(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
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

    private static void assertReleasesAllocations(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        long expectedRows = -1;
        for (int i = 0; i < 10; i++) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                long rows = 0;
                while (cursor.hasNext()) {
                    rows++;
                }
                if (expectedRows == -1) {
                    expectedRows = rows;
                }
                Assert.assertEquals("iteration " + i, expectedRows, rows);
                Assert.assertTrue("expected rows at iteration " + i, rows > 0);
            }
        }
    }

    private static void createPrices(CairoEngine engine, SqlExecutionContext ctx, int rows, int symbols) throws Exception {
        engine.execute(
                "CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        // Prices 0.1s apart so each window covers many slave rows.
        engine.execute(
                "INSERT INTO prices SELECT (x * 100_000)::timestamp, (x % " + symbols + ")::symbol, x::double FROM long_sequence(" + rows + ")",
                ctx
        );
    }

    private static void createTrades(CairoEngine engine, SqlExecutionContext ctx, int rows, int symbols) throws Exception {
        engine.execute(
                "CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        // Trades 1s apart; prices are 10x denser and span the same range.
        engine.execute(
                "INSERT INTO trades SELECT (x * 1_000_000)::timestamp, (x % " + symbols + ")::symbol, x::double FROM long_sequence(" + rows + ")",
                ctx
        );
    }
}
