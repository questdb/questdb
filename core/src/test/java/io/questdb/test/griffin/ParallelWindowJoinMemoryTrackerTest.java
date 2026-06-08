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
import io.questdb.griffin.engine.join.AsyncWindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the parallel
 * WINDOW JOIN aggregation operators in {@code io.questdb.griffin.engine.join}.
 * <p>
 * WINDOW JOIN aggregates slave rows in a per-master-row window and reaches the
 * same unbounded native structures as the parallel GROUP BY: the owner/per-worker
 * {@code FastGroupByAllocator}s that back group-by function state (e.g.
 * {@code array_agg}) and the temporary row id / timestamp lists. Those structures
 * live in {@link io.questdb.griffin.engine.join.AsyncWindowJoinAtom}, which binds
 * the four allocators to the active workload's tracker in {@code reopen()} and
 * unbinds them in {@code close()}; the keyed (symbol) variant routes through
 * {@link AsyncWindowJoinFastRecordCursorFactory} whose atom subclass inherits the
 * same binding.
 * <p>
 * {@code array_agg} is the vehicle here: its single growing list (per master row)
 * is allocated through the {@code FastGroupByAllocator}s and accumulates across
 * frames (only the temporary lists are cleared per frame), so combined per-worker
 * reduce growth trips the limit and surfaces with {@code isOutOfMemory()} set.
 * Without the binding the lists escape the limit and the query completes, firing
 * the {@code Assert.fail} below.
 * <p>
 * Each query runs on a dedicated {@link WorkerPool} via {@link TestUtils#execute},
 * which builds a fresh {@code CairoEngine} from the test configuration; the
 * per-query limit is therefore read fresh by every test and can be set in
 * {@link #setUp()}.
 */
public class ParallelWindowJoinMemoryTrackerTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // 8 MiB: small enough that a wide-window array_agg fills past it during the
        // per-worker reduce, large enough for the success/leak cases to fit.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
        // Many small page frames so the master scan fans out across the worker pool.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        super.setUp();
    }

    @Test
    public void testKeyedWindowJoinArrayAggFailsOnLargeSet() throws Exception {
        // Keyed array_agg over a WINDOW JOIN routes through AsyncWindowJoinFastRecordCursorFactory.
        // Each master row accumulates its matched slave prices into a list allocated through the
        // owner/per-worker FastGroupByAllocators, which the atom binds to the per-query tracker in
        // reopen(). Combined per-worker reduce growth trips the limit. Without the binding the lists
        // escape and the query completes, firing Assert.fail below.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 200_000, 8);
                        createPrices(engine, sqlExecutionContext, 2_000_000, 8);
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 15 seconds PRECEDING AND 15 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncWindowJoinFastRecordCursorFactory.class);
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
        // A tiny limit breaches the cursor's of() at atom.reopen() before any row; reusing the factory
        // catches a failed open that leaves isOpen set (the next open would skip reopen() and not breach).
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
                            assertInTree(factory, AsyncWindowJoinFastRecordCursorFactory.class);
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
        // A small keyed array_agg fits the per-query limit; the owner and per-worker allocators are
        // bound to the tracker on each open and must release every byte on close. Repeated
        // getCursor/close cycles, wrapped by assertMemoryLeak, would expose a malloc/free asymmetry
        // or a tracker imbalance from the close()-time unbinding.
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
                            assertInTree(factory, AsyncWindowJoinFastRecordCursorFactory.class);
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
        // Non-keyed array_agg over a WINDOW JOIN routes through AsyncWindowJoinRecordCursorFactory.
        // Every slave row in the window matches, so each master row's list grows through the same
        // FastGroupByAllocators bound in the atom's reopen(). Combined per-worker reduce growth trips
        // the limit; without the binding the query completes and Assert.fail fires.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 200_000, 8);
                        createPrices(engine, sqlExecutionContext, 2_000_000, 8);
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p " +
                                "RANGE BETWEEN 15 seconds PRECEDING AND 15 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncWindowJoinRecordCursorFactory.class);
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
        // execution) must still free the native resources its cursor allocates at construction.
        // The cursor starts isOpen=false so the first of() runs reopen() (the lazy allocators need
        // it); close() must free constructor-scoped resources regardless of isOpen, or this
        // assertMemoryLeak catches it.
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
                            assertInTree(f, AsyncWindowJoinRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.ts, array_agg(p.price) FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                        "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(f, AsyncWindowJoinFastRecordCursorFactory.class);
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
        // Non-keyed variant of testKeyedWindowJoinOpenFailureReleasesAllocations.
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
                            assertInTree(factory, AsyncWindowJoinRecordCursorFactory.class);
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
        // Non-keyed variant of testKeyedWindowJoinReleasesAllocations.
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
                            assertInTree(factory, AsyncWindowJoinRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertInTree(RecordCursorFactory factory, Class<?> expected) {
        RecordCursorFactory f = factory;
        while (f != null) {
            if (expected.isInstance(f)) {
                return;
            }
            f = f.getBaseFactory();
        }
        Assert.fail("expected " + expected.getSimpleName() + " in the factory tree, but top was " + factory.getClass().getName());
    }

    private static void assertOpenFailureReleasesAllocations(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        for (int i = 0; i < 5; i++) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                Assert.fail("expected a per-query memory breach during cursor open at iteration " + i);
            } catch (CairoException e) {
                Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
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
