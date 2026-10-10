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
import io.questdb.griffin.engine.table.HorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinProjectionRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * single-threaded HORIZON JOIN aggregation operators in
 * {@code io.questdb.griffin.engine.table}.
 * <p>
 * These are the synchronous siblings of the operators covered by
 * {@link ParallelHorizonJoinMemoryTrackerTest}. The codegen routes to them
 * whenever the parallel HORIZON JOIN path is unavailable: a deployment with no
 * shared query workers, the {@code cairo.sql.parallel.horizon.join.enabled} knob
 * turned off (as these tests do), or a query shape the parallel path cannot take.
 * They reach the same unbounded native structures as the parallel variants - the
 * {@code GroupByAllocator} backing group-by function state (e.g. {@code array_agg})
 * and, for keyed queries, the GROUP BY {@code dataMap} - which the cursors bind to
 * the per-query tracker in {@code of()} before the build loop and free in
 * {@code close()}.
 * <p>
 * {@code array_agg} is the vehicle: its single growing list (per group) is
 * allocated through the {@code GroupByAllocator}, so the build-loop growth trips
 * the limit and surfaces with {@code isOutOfMemory()} set. Without the binding the
 * lists grow unbounded and escape the limit, so the query would complete and the
 * {@code Assert.fail} below would fire. The {@code assertFactoryInTree} routing guard pins
 * the test to the synchronous factory, so a future change that drops the binding or
 * re-routes to the parallel path fails loudly here rather than silently passing.
 * <p>
 * Each query runs on a dedicated {@link WorkerPool} via {@link TestUtils#execute},
 * which builds a fresh {@code CairoEngine} from the test configuration; the
 * per-query limit is therefore read fresh by every test and can be set in
 * {@link #setUp()}.
 */
public class SyncHorizonJoinMemoryTrackerTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // 8 MiB: small enough that a wide-window array_agg fills past it during the
        // build loop, large enough for the success/leak cases to fit.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
        // Force the single-threaded HORIZON JOIN path.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        super.setUp();
    }

    @Test
    public void testHorizonJoinCompileWithoutOpenDoesNotLeak() throws Exception {
        // A horizon-join factory compiled but never opened (plan caching, EXPLAIN, an aborted
        // execution) must still free the native resources its cursor holds. The cursor starts
        // isOpen=false so the first of() runs the tracker-bound reopen(); its lazy allocator/maps
        // and keepClosed horizon iterator therefore hold no backing until then, so a never-opened
        // close() leaves nothing behind. assertMemoryLeak catches a regression of that property.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.sym, array_agg(p.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) RANGE FROM -2s TO 2s STEP 1s AS h",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, HorizonJoinRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT array_agg(p.price) FROM trades t HORIZON JOIN prices p RANGE FROM -2s TO 2s STEP 1s AS h",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, HorizonJoinNotKeyedRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedHorizonJoinArrayAggFailsOnLargeSet() throws Exception {
        // Keyed array_agg over a HORIZON JOIN routes through HorizonJoinRecordCursorFactory. Each
        // group accumulates the matched slave prices across a wide horizon window into a growing
        // list allocated through the GroupByAllocator, which the cursor binds to the per-query
        // tracker in of(). The build-loop growth trips the limit and surfaces with isOutOfMemory()
        // set. Without the binding the lists escape the limit and the query completes, firing
        // Assert.fail below.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: in
        // the build loop, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 40_000, 8);
                        createPrices(engine, sqlExecutionContext, 400_000, 8);
                        final String query = "SELECT t.sym, array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) " +
                                "RANGE FROM -15s TO 15s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, HorizonJoinRecordCursorFactory.class);
                            assertQueryBreaches(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedHorizonJoinOpenFailureReleasesAllocations() throws Exception {
        // A tiny limit breaches the cursor's of() at the first reopen() before any row; reusing the
        // factory catches a failed open that would otherwise leave isOpen set (the next open would
        // skip reopen() and not breach). The getCursor() catch frees the cursor under the tracker.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        final String query = "SELECT t.sym, array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, HorizonJoinRecordCursorFactory.class);
                            assertOpenFailureReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedHorizonJoinReleasesAllocations() throws Exception {
        // A small keyed array_agg fits the per-query limit; the allocator, the dataMap, and the
        // ASOF map are bound to the tracker on each open and must release every byte on close.
        // Repeated getCursor/close cycles on the same factory, wrapped by assertMemoryLeak, would
        // expose a malloc/free asymmetry or a tracker imbalance.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 5_000, 8);
                        createPrices(engine, sqlExecutionContext, 50_000, 8);
                        final String query = "SELECT t.sym, array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, HorizonJoinRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext, 8);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedMultiHorizonJoinArrayAggFailsOnLargeSet() throws Exception {
        // Keyed array_agg over a multi-slave HORIZON JOIN routes through
        // MultiHorizonJoinRecordCursorFactory, whose cursor owns the allocator, the dataMap, and the
        // per-slave ASOF maps. array_agg(p0.px0) grows the allocator past the limit and surfaces with
        // isOutOfMemory() set; without the binding it escapes and the query completes.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: in
        // the build loop, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createMultiHorizonTables(engine, sqlExecutionContext, 40_000);
                        final String query = "SELECT t.sym, array_agg(p0.px0), count(p1.px1) " +
                                "FROM trades t " +
                                "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) " +
                                "HORIZON JOIN prices1 p1 " +
                                "RANGE FROM -15s TO 15s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, MultiHorizonJoinRecordCursorFactory.class);
                            assertQueryBreaches(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedMultiHorizonJoinOpenFailureReleasesAllocations() throws Exception {
        // Multi-slave keyed variant of testKeyedHorizonJoinOpenFailureReleasesAllocations.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createMultiHorizonTables(engine, sqlExecutionContext, 100);
                        final String query = "SELECT t.sym, array_agg(p0.px0), count(p1.px1) " +
                                "FROM trades t " +
                                "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) " +
                                "HORIZON JOIN prices1 p1 " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, MultiHorizonJoinRecordCursorFactory.class);
                            assertOpenFailureReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedMultiHorizonJoinReleasesAllocations() throws Exception {
        // A small keyed multi-slave array_agg fits the per-query limit; the allocator, dataMap, and
        // per-slave ASOF maps are bound on each open and must release every byte on close. Repeated
        // getCursor/close cycles, wrapped by assertMemoryLeak, would expose a malloc/free asymmetry.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createMultiHorizonTables(engine, sqlExecutionContext, 5_000);
                        final String query = "SELECT t.sym, array_agg(p0.px0), count(p1.px1) " +
                                "FROM trades t " +
                                "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) " +
                                "HORIZON JOIN prices1 p1 " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, MultiHorizonJoinRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext, 8);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testMultiHorizonJoinCompileWithoutOpenDoesNotLeak() throws Exception {
        // Multi-slave variant of testHorizonJoinCompileWithoutOpenDoesNotLeak.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createMultiHorizonTables(engine, sqlExecutionContext, 100);
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.sym, array_agg(p0.px0), count(p1.px1) FROM trades t " +
                                        "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) HORIZON JOIN prices1 p1 " +
                                        "RANGE FROM -2s TO 2s STEP 1s AS h",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, MultiHorizonJoinRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT array_agg(p0.px0), count(p1.px1) FROM trades t " +
                                        "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) HORIZON JOIN prices1 p1 " +
                                        "RANGE FROM -2s TO 2s STEP 1s AS h",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, MultiHorizonJoinNotKeyedRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedHorizonJoinArrayAggFailsOnLargeSet() throws Exception {
        // Non-keyed array_agg over a HORIZON JOIN routes through HorizonJoinNotKeyedRecordCursorFactory.
        // It carries no GROUP BY map, but its single growing list is allocated through the
        // GroupByAllocator the cursor binds to the per-query tracker in of(). The build-loop growth
        // trips the limit and surfaces with isOutOfMemory() set.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: in
        // the build loop, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 40_000, 8);
                        createPrices(engine, sqlExecutionContext, 400_000, 8);
                        final String query = "SELECT array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p " +
                                "RANGE FROM -15s TO 15s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, HorizonJoinNotKeyedRecordCursorFactory.class);
                            assertQueryBreaches(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedHorizonJoinReleasesAllocations() throws Exception {
        // A small non-keyed array_agg fits the per-query limit; the allocator and the ASOF map are
        // bound on each open and must release every byte on close. Repeated getCursor/close cycles,
        // wrapped by assertMemoryLeak, would expose a malloc/free asymmetry.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 5_000, 8);
                        createPrices(engine, sqlExecutionContext, 50_000, 8);
                        final String query = "SELECT array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, HorizonJoinNotKeyedRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext, 1);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedMultiHorizonJoinArrayAggFailsOnLargeSet() throws Exception {
        // Non-keyed multi-slave variant of testKeyedMultiHorizonJoinArrayAggFailsOnLargeSet; dropping
        // the GROUP BY key routes to MultiHorizonJoinNotKeyedRecordCursorFactory.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: in
        // the build loop, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createMultiHorizonTables(engine, sqlExecutionContext, 40_000);
                        final String query = "SELECT array_agg(p0.px0), count(p1.px1) " +
                                "FROM trades t " +
                                "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) " +
                                "HORIZON JOIN prices1 p1 " +
                                "RANGE FROM -15s TO 15s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, MultiHorizonJoinNotKeyedRecordCursorFactory.class);
                            assertQueryBreaches(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedMultiHorizonJoinReleasesAllocations() throws Exception {
        // A small non-keyed multi-slave array_agg fits the per-query limit; the allocator and per-slave
        // ASOF maps are bound on each open and must release every byte on close.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createMultiHorizonTables(engine, sqlExecutionContext, 5_000);
                        final String query = "SELECT array_agg(p0.px0), count(p1.px1) " +
                                "FROM trades t " +
                                "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) " +
                                "HORIZON JOIN prices1 p1 " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, MultiHorizonJoinNotKeyedRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext, 1);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testProjectionBatchListsBreachLimitAtOpen() throws Exception {
        // The serial cursor opens the row id list and the timestamp list of a batch at 200 master
        // rows each: the small page frame budget of 1,000 over 5 offsets and one slave. That is
        // 1,600 bytes per list, so a 2,400-byte limit admits one list and breaches on the other,
        // inside getCursor(). The join has no key, so the open charges the tracker for nothing
        // else. Without the binding of either list the remaining one fits the limit and
        // getCursor() returns, tripping the Assert.fail.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2_400L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(2, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTrades(engine, ctx, 1_000, 8);
                createPrices(engine, ctx, 10_000, 8);
                try (RecordCursorFactory factory = compiler.compile("""
                        SELECT t.sym, p.price FROM trades t
                        HORIZON JOIN prices p RANGE FROM -2s TO 2s STEP 1s AS h
                        """, ctx).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, HorizonJoinProjectionRecordCursorFactory.class);
                    assertOpenBreachesOn(factory, ctx, "used=1600, size=1600, memoryTag=" + MemoryTag.NATIVE_DEFAULT + ']');
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testProjectionBatchSlotsBreachLimit() throws Exception {
        // A batch of 200 master rows holds one slave row id per row and offset: 1,000 slots, or
        // 8,000 bytes, which the cursor allocates when it reads the first batch. A 4 KiB limit
        // admits the two 1,600-byte lists that the open allocates and breaches on the slots. The
        // join has no key, so the cursor charges the tracker for nothing else. Without the binding
        // the slots escape the limit and the query completes, tripping the Assert.fail.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(2, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTrades(engine, ctx, 1_000, 8);
                createPrices(engine, ctx, 10_000, 8);
                try (RecordCursorFactory factory = compiler.compile("""
                        SELECT t.sym, p.price FROM trades t
                        HORIZON JOIN prices p RANGE FROM -2s TO 2s STEP 1s AS h
                        """, ctx).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, HorizonJoinProjectionRecordCursorFactory.class);
                    assertDrainBreachesOn(factory, ctx, "size=8000, memoryTag=" + MemoryTag.NATIVE_DEFAULT + ']');
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testProjectionHighCardinalityKeyMapBreachesLimit() throws Exception {
        // The keyed ASOF map caches the row id of every key that a backward scan passes, so it
        // grows with the key cardinality of the slave. Every trade follows the last price and
        // carries the key of one of the first 1,000 prices, so its lookup scans backward past
        // nearly all 200,000 distinct keys. Holding them takes the map 8 MiB. Under the 1 MiB
        // limit it breaches at about 23,000 keys, when it rehashes to 65,536 entries of 16 bytes
        // plus one for the zero key, a block of 1,048,592 bytes. The memory tag names the map as
        // the allocation that breached: the batch lists, the only other structures this cursor
        // charges, carry NATIVE_DEFAULT and take 11,200 bytes. Without the binding the map
        // escapes the limit and the query completes, tripping the Assert.fail.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(2, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createHighCardinalityTables(engine, ctx);
                try (RecordCursorFactory factory = compiler.compile("""
                        SELECT t.id, p.price FROM trades t
                        HORIZON JOIN prices p ON (t.id = p.id) RANGE FROM -2s TO 2s STEP 1s AS h
                        """, ctx).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, HorizonJoinProjectionRecordCursorFactory.class);
                    assertDrainBreachesOn(factory, ctx, "memoryTag=" + MemoryTag.NATIVE_UNORDERED_MAP + ']');
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testProjectionOpenFailureReleasesAllocations() throws Exception {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(2, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTrades(engine, ctx, 100, 8);
                createPrices(engine, ctx, 1_000, 8);
                try (RecordCursorFactory factory = compiler.compile("""
                        SELECT t.sym, p.price FROM trades t
                        HORIZON JOIN prices p ON (sym) RANGE FROM -2s TO 2s STEP 1s AS h
                        """, ctx).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, HorizonJoinProjectionRecordCursorFactory.class);
                    assertOpenFailureReleasesAllocations(factory, ctx);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testProjectionReleasesAllocations() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(2, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createMultiHorizonTables(engine, ctx, 100);
                final String query = """
                        SELECT t.sym, p0.px0, p1.px1 FROM trades t
                        HORIZON JOIN prices0 p0 ON (t.sym = p0.sym)
                        HORIZON JOIN prices1 p1
                        RANGE FROM -2s TO 2s STEP 1s AS h
                        """;
                try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                    TestUtils.assertFactoryInTree(factory, HorizonJoinProjectionRecordCursorFactory.class);
                    // Compile and close without opening, as an EXPLAIN or unused cached plan does.
                }
                try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
                    assertReleasesAllocations(factory, ctx, 500);
                }
            }, configuration, LOG);
        });
    }

    // expectedAllocation is a fragment from the end of the tracker's message, which reports the
    // bytes the query had charged, then the size and the memory tag of the allocation that breached.
    private static void assertBreachOn(CairoException e, String expectedAllocation) {
        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
        TestUtils.assertContains(e.getFlyweightMessage(), expectedAllocation);
    }

    // Expects the breach while the cursor reads rows, after getCursor() has returned. Repeating
    // the cycle also verifies that the failed execution released what it had charged.
    private static void assertDrainBreachesOn(RecordCursorFactory factory, SqlExecutionContext ctx, String expectedAllocation) throws SqlException {
        for (int i = 0; i < 3; i++) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                long rows = 0;
                try {
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.fail("expected a per-query memory breach at iteration " + i + ", drained " + rows + " rows");
                } catch (CairoException e) {
                    assertBreachOn(e, expectedAllocation);
                }
            }
        }
    }

    // Expects the breach inside getCursor(). Repeating the open also verifies that the failed one
    // released what it had charged.
    private static void assertOpenBreachesOn(RecordCursorFactory factory, SqlExecutionContext ctx, String expectedAllocation) throws SqlException {
        for (int i = 0; i < 3; i++) {
            try (RecordCursor ignore = factory.getCursor(ctx)) {
                Assert.fail("expected a per-query memory breach during cursor open at iteration " + i);
            } catch (CairoException e) {
                assertBreachOn(e, expectedAllocation);
            }
        }
    }

    private static void assertOpenFailureReleasesAllocations(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        for (int i = 0; i < 5; i++) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                Assert.fail("expected a per-query memory breach during cursor open at iteration " + i);
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

    private static void assertReleasesAllocations(RecordCursorFactory factory, SqlExecutionContext ctx, long expectedRows) throws SqlException {
        for (int i = 0; i < 10; i++) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                long rows = 0;
                while (cursor.hasNext()) {
                    rows++;
                }
                Assert.assertEquals("iteration " + i, expectedRows, rows);
            }
        }
    }

    private static void createHighCardinalityTables(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        engine.execute(
                "CREATE TABLE trades (ts TIMESTAMP, id LONG) TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        // Trades 1s apart, the first one 101s after the last price, so that every horizon
        // timestamp has the last price as its ASOF position. Trade x carries the key of price x.
        engine.execute(
                "INSERT INTO trades SELECT ((300 + x) * 1_000_000)::timestamp, x FROM long_sequence(1_000)",
                ctx
        );
        engine.execute(
                "CREATE TABLE prices (ts TIMESTAMP, id LONG, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        // Prices 1ms apart over 200s, one distinct key per row.
        engine.execute(
                "INSERT INTO prices SELECT (x * 1_000)::timestamp, x, x::double FROM long_sequence(200_000)",
                ctx
        );
    }

    private static void createMultiHorizonTables(CairoEngine engine, SqlExecutionContext ctx, int tradeRows) throws Exception {
        engine.execute(
                "CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                "INSERT INTO trades SELECT (x * 1_000_000)::timestamp, (x % 8)::symbol FROM long_sequence(" + tradeRows + ")",
                ctx
        );
        // Slave 0 is keyed (ON clause); slave 1 is non-keyed. Both are denser than trades so each
        // horizon point finds an ASOF match.
        engine.execute(
                "CREATE TABLE prices0 (ts TIMESTAMP, sym SYMBOL, px0 DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                "INSERT INTO prices0 SELECT (x * 100_000)::timestamp, (x % 8)::symbol, x::double FROM long_sequence(" + (10 * tradeRows) + ")",
                ctx
        );
        engine.execute(
                "CREATE TABLE prices1 (ts TIMESTAMP, px1 DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                "INSERT INTO prices1 SELECT (x * 200_000)::timestamp, x::double FROM long_sequence(" + (5 * tradeRows) + ")",
                ctx
        );
    }

    private static void createPrices(CairoEngine engine, SqlExecutionContext ctx, int rows, int symbols) throws Exception {
        engine.execute(
                "CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        // Prices 0.1s apart so each horizon point finds an ASOF match.
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
