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
import io.questdb.griffin.engine.table.AsyncHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the parallel
 * HORIZON JOIN aggregation operators in {@code io.questdb.griffin.engine.table}.
 * <p>
 * HORIZON JOIN aggregates slave rows in a per-master-row window (markout
 * analysis) and reaches the same unbounded native structures as the parallel
 * GROUP BY: the owner/per-worker {@code FastGroupByAllocator}s that back group-by
 * function state (e.g. {@code array_agg}), and, for keyed queries, the
 * {@code GroupByShardingContext} fragment maps. Those structures live in
 * {@link io.questdb.griffin.engine.table.BaseAsyncHorizonJoinAtom} and the keyed
 * {@link AsyncHorizonJoinRecordCursorFactory} atom; the base atom binds the
 * allocators (and ASOF lookup maps) to the active workload's tracker in
 * {@code reopen()} and unbinds them in {@code close()}, while the keyed atom binds
 * its sharding context.
 * <p>
 * {@code array_agg} is the vehicle here for the same reason as in the parallel
 * GROUP BY tracker tests: its single growing list (per group) is allocated through
 * the {@code FastGroupByAllocator}s, so combined per-worker reduce growth trips the
 * limit and surfaces with {@code isOutOfMemory()} set. Without the binding the
 * lists grow unbounded and escape the limit, so the query would complete and the
 * {@code Assert.fail} below would fire. The keyed query additionally binds the
 * sharding context (the same {@code GroupByShardingContext.setMemoryTracker} path
 * proven by {@code ParallelGroupByMemoryTrackerTest}).
 * <p>
 * Each query runs on a dedicated {@link WorkerPool} via {@link TestUtils#execute},
 * which builds a fresh {@code CairoEngine} from the test configuration; the
 * per-query limit is therefore read fresh by every test and can be set in
 * {@link #setUp()}.
 */
public class ParallelHorizonJoinMemoryTrackerTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // 8 MiB: small enough that a wide-window array_agg fills past it during the
        // per-worker reduce, large enough for the success/leak cases to fit.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // Low threshold so a keyed aggregation shards during the reduce, exercising the
        // per-fragment shard maps alongside the allocators.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
        // Many small page frames so the master scan fans out across the worker pool.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        super.setUp();
    }

    @Test
    public void testHorizonJoinCompileWithoutOpenDoesNotLeak() throws Exception {
        // A horizon-join factory compiled but never opened (plan caching, EXPLAIN, an aborted
        // execution) must still free the native resources its cursor allocates at construction -
        // the slave ConcurrentTimeFrameState DirectLists. The cursor starts isOpen=false so the
        // first of() runs reopen() (the lazy allocators need it); close() must therefore free those
        // constructor-scoped resources regardless of isOpen, or this assertMemoryLeak catches it.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.sym, array_agg(p.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) RANGE FROM -2s TO 2s STEP 1s AS h",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, AsyncHorizonJoinRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT array_agg(p.price) FROM trades t HORIZON JOIN prices p RANGE FROM -2s TO 2s STEP 1s AS h",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, AsyncHorizonJoinNotKeyedRecordCursorFactory.class);
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
        // Keyed array_agg over a HORIZON JOIN routes through AsyncHorizonJoinRecordCursorFactory.
        // Each of the few groups accumulates the matched slave prices across a wide horizon
        // window into a growing list allocated through the owner/per-worker FastGroupByAllocators,
        // which the base atom binds to the per-query tracker in reopen(). The combined per-worker
        // reduce growth trips the limit and surfaces with isOutOfMemory() set. Without the binding
        // the lists escape the limit and the query would complete, firing Assert.fail below.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: on
        // the combined per-worker reduce growth, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 40_000, 8);
                        createPrices(engine, sqlExecutionContext, 400_000, 8);
                        final String query = "SELECT t.sym, array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) " +
                                "RANGE FROM -15s TO 15s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncHorizonJoinRecordCursorFactory.class);
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
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedHorizonJoinOpenFailureReleasesAllocations() throws Exception {
        // A tiny limit breaches during the reduce on the first drain (the chunk index is off the
        // per-query tracker, so nothing per-query-tracked allocates at open); the loop verifies reuse.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        final String query = "SELECT t.sym, array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncHorizonJoinRecordCursorFactory.class);
                            for (int i = 0; i < 5; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedHorizonJoinReleasesAllocations() throws Exception {
        // A small keyed array_agg fits the per-query limit; the owner and per-worker allocators,
        // the sharding context, and the ASOF maps are bound to the tracker on each open and must
        // release every byte on close. Repeated getCursor/close cycles on the same factory, wrapped
        // by assertMemoryLeak, would expose a malloc/free asymmetry or a tracker imbalance.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 5_000, 8);
                        createPrices(engine, sqlExecutionContext, 50_000, 8);
                        final String query = "SELECT t.sym, array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncHorizonJoinRecordCursorFactory.class);
                            for (int i = 0; i < 10; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    long rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals("iteration " + i, 8, rows);
                                }
                            }
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
        // AsyncMultiHorizonJoinRecordCursorFactory, whose BaseAsyncMultiHorizonJoinAtom owns the
        // per-worker allocators and the per-worker x per-slave ASOF maps. array_agg(p0.px0) grows
        // the allocators past the limit and surfaces with isOutOfMemory() set; without the binding
        // it escapes and the query completes, firing Assert.fail below.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: on
        // the combined per-worker reduce growth, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
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
                            TestUtils.assertFactoryInTree(factory, AsyncMultiHorizonJoinRecordCursorFactory.class);
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
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedMultiHorizonJoinOpenFailureReleasesAllocations() throws Exception {
        // Multi-slave keyed variant of testKeyedHorizonJoinOpenFailureReleasesAllocations. The
        // reducer acquires a per-worker slot before it navigates to the frame, so each breached
        // execution must also hand its slot back: PerWorkerLocks has no reset and the atom belongs
        // to the factory, so a slot lost here is lost for as long as the factory stays in the SQL
        // cache, and once all of them have leaked every worker spins for a slot nobody will release.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
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
                            TestUtils.assertFactoryInTree(factory, AsyncMultiHorizonJoinRecordCursorFactory.class);
                            for (int i = 0; i < 5; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedMultiHorizonJoinReleasesAllocations() throws Exception {
        // A small keyed multi-slave array_agg fits the per-query limit; the per-worker allocators
        // and the per-worker x per-slave ASOF maps are bound on each open and must release every
        // byte on close. Repeated getCursor/close cycles, wrapped by assertMemoryLeak, would expose
        // a malloc/free asymmetry in the multi-slave atom's flat per-worker x per-slave indexing.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
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
                            TestUtils.assertFactoryInTree(factory, AsyncMultiHorizonJoinRecordCursorFactory.class);
                            for (int i = 0; i < 10; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    long rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals("iteration " + i, 8, rows);
                                }
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testMultiHorizonJoinCompileWithoutOpenDoesNotLeak() throws Exception {
        // Multi-slave variant of testHorizonJoinCompileWithoutOpenDoesNotLeak: the cursor allocates
        // one ConcurrentTimeFrameState per slave at construction, so a never-opened multi factory
        // must free them all on close() regardless of isOpen.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createMultiHorizonTables(engine, sqlExecutionContext, 100);
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.sym, array_agg(p0.px0), count(p1.px1) FROM trades t " +
                                        "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) HORIZON JOIN prices1 p1 " +
                                        "RANGE FROM -2s TO 2s STEP 1s AS h",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, AsyncMultiHorizonJoinRecordCursorFactory.class);
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
        // Non-keyed array_agg over a HORIZON JOIN routes through
        // AsyncHorizonJoinNotKeyedRecordCursorFactory. It carries no key map, but its single
        // growing list is allocated through the owner/per-worker FastGroupByAllocators of the base
        // atom, which binds them to the per-query tracker in reopen(). The combined per-worker
        // reduce growth trips the limit and surfaces with isOutOfMemory() set. Without the binding
        // the list escapes the limit and the query would complete, firing Assert.fail below.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: on
        // the combined per-worker reduce growth, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 40_000, 8);
                        createPrices(engine, sqlExecutionContext, 400_000, 8);
                        final String query = "SELECT array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p " +
                                "RANGE FROM -15s TO 15s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncHorizonJoinNotKeyedRecordCursorFactory.class);
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
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedHorizonJoinOpenFailureReleasesAllocations() throws Exception {
        // Non-keyed variant of testKeyedHorizonJoinOpenFailureReleasesAllocations.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        final String query = "SELECT array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncHorizonJoinNotKeyedRecordCursorFactory.class);
                            for (int i = 0; i < 5; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedHorizonJoinReleasesAllocations() throws Exception {
        // A small non-keyed array_agg fits the per-query limit; the owner and per-worker allocators
        // and the ASOF maps are bound to the tracker on each open and must release every byte on
        // close. Repeated getCursor/close cycles on the same factory, wrapped by assertMemoryLeak,
        // would expose a malloc/free asymmetry or a tracker imbalance from the close()-time unbinding.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 5_000, 8);
                        createPrices(engine, sqlExecutionContext, 50_000, 8);
                        final String query = "SELECT array_agg(p.price) " +
                                "FROM trades t HORIZON JOIN prices p " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncHorizonJoinNotKeyedRecordCursorFactory.class);
                            for (int i = 0; i < 10; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    long rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals("iteration " + i, 1, rows);
                                }
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedMultiHorizonJoinArrayAggFailsOnLargeSet() throws Exception {
        // Non-keyed array_agg over a multi-slave HORIZON JOIN routes through
        // AsyncMultiHorizonJoinNotKeyedRecordCursorFactory. Its per-worker FastGroupByAllocators back the
        // single growing array_agg(p0.px0) list; the combined per-worker reduce growth trips the limit and
        // surfaces with isOutOfMemory() set. Without the binding it escapes and the query completes, firing
        // Assert.fail below. The drain-to-breach companion the non-keyed multi-slave variant previously lacked.
        // Its own limit, tighter than the class default. Trimming the input to keep CI time down left
        // this case storing only ~1.2x the 8 MiB default, and a breach margin that thin is one
        // allocator or array_agg compaction away from not breaching at all - at which point the
        // Assert.fail below turns the case red rather than silently green, but red all the same. At
        // 2 MiB the same trimmed input breaches by ~5x, and still breaches where it is meant to: on
        // the combined per-worker reduce growth, far above the first chunk malloc.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
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
                            TestUtils.assertFactoryInTree(factory, AsyncMultiHorizonJoinNotKeyedRecordCursorFactory.class);
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
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedMultiHorizonJoinOpenFailureReleasesAllocations() throws Exception {
        // Multi-slave non-keyed variant of testKeyedHorizonJoinOpenFailureReleasesAllocations;
        // dropping the GROUP BY key routes to the non-keyed multi-horizon factory. Its reducer
        // acquires a per-worker slot before navigating to the frame, so the breached executions
        // must hand every slot back - see testKeyedMultiHorizonJoinOpenFailureReleasesAllocations.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createMultiHorizonTables(engine, sqlExecutionContext, 100);
                        final String query = "SELECT array_agg(p0.px0), count(p1.px1) " +
                                "FROM trades t " +
                                "HORIZON JOIN prices0 p0 ON (t.sym = p0.sym) " +
                                "HORIZON JOIN prices1 p1 " +
                                "RANGE FROM -2s TO 2s STEP 1s AS h";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncMultiHorizonJoinNotKeyedRecordCursorFactory.class);
                            for (int i = 0; i < 5; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNotKeyedMultiHorizonJoinReleasesAllocations() throws Exception {
        // A small non-keyed multi-slave array_agg fits the per-query limit; the per-worker allocators and
        // the per-worker x per-slave ASOF maps are bound on each open and must release every byte on close.
        // Repeated getCursor/close cycles, wrapped by assertMemoryLeak, would expose a malloc/free asymmetry
        // in the non-keyed multi-slave atom (the variant the multi-horizon leak loop previously skipped).
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
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
                            TestUtils.assertFactoryInTree(factory, AsyncMultiHorizonJoinNotKeyedRecordCursorFactory.class);
                            for (int i = 0; i < 10; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    long rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals("iteration " + i, 1, rows);
                                }
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void createMultiHorizonTables(io.questdb.cairo.CairoEngine engine, io.questdb.griffin.SqlExecutionContext ctx, int tradeRows) throws Exception {
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

    private static void createPrices(io.questdb.cairo.CairoEngine engine, io.questdb.griffin.SqlExecutionContext ctx, int rows, int symbols) throws Exception {
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

    private static void createTrades(io.questdb.cairo.CairoEngine engine, io.questdb.griffin.SqlExecutionContext ctx, int rows, int symbols) throws Exception {
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
