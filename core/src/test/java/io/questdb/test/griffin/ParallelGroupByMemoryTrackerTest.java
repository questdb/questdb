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
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware parallel GROUP BY operators in
 * {@code io.questdb.griffin.engine.table}.
 * <p>
 * The keyed operator {@link AsyncGroupByRecordCursorFactory} is covered with its
 * {@code GroupByShardingContext} / {@code GroupByMapFragment} maps and the
 * owner/per-worker {@code FastGroupByAllocator}s.
 * <p>
 * The non-keyed operator {@link AsyncGroupByNotKeyedRecordCursorFactory} is
 * covered through {@code array_agg}: it carries no key map, but its single-group
 * list grows without bound and is allocated through the owner/per-worker
 * {@code FastGroupByAllocator}s, which the atom binds to the active workload's
 * tracker in {@code reopen()} (and unbinds in {@code close()}). Bounded non-keyed
 * aggregates such as {@code count_distinct} hold only a {@code SimpleMapValue} and
 * never route here, so {@code array_agg} is the vehicle for the unbounded
 * non-keyed path.
 * <p>
 * Each query runs on a dedicated {@link WorkerPool} via {@link TestUtils#execute},
 * which builds a fresh {@code CairoEngine} from the test configuration; the
 * per-query limit is therefore read fresh by every test and can be set in
 * {@link #setUp()}.
 * <p>
 * Breaches are asserted only for the reduce phase (per-worker map growth, shard
 * fill, and per-worker allocator growth) and the owner-side merge, where the
 * offending {@code CairoException} surfaces verbatim through
 * {@code UnorderedPageFrameSequence.dispatchAndAwait} with {@code isOutOfMemory()}
 * set. A breach inside the parallel shard-merge job is caught there and converted
 * to a cancellation, so it is intentionally not used as a breach probe. The
 * sharded path's allocation accounting is exercised instead by a success + leak
 * loop, and more broadly by {@code ParallelGroupByFuzzTest}, whose
 * {@code recordPerQueryMemAlloc} assertion is live on the default (unlimited)
 * tracker.
 */
public class ParallelGroupByMemoryTrackerTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // 8 MiB: large enough for the sharded success case (256 shards per fragment
        // across the owner and four workers, each a 4 KiB initial map) to fit, small
        // enough that a high-cardinality reduce fills past it after a few doublings.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // Low threshold so a high-cardinality GROUP BY shards during the reduce,
        // exercising the per-fragment shard maps; a low-cardinality GROUP BY stays
        // below it and never shards.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        // Small page size keeps the per-fragment and shard maps compact so the limit
        // tuning above is predictable.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
        // Many small page frames so the scan fans out across the worker pool.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        super.setUp();
    }

    @Test
    public void testParallelKeyedGroupByFailsOnLargeKeySet() throws Exception {
        // High-cardinality VARCHAR-keyed GROUP BY routes through the parallel
        // AsyncGroupByRecordCursorFactory (VARCHAR keys are not vectorized). The
        // per-worker fragment maps grow with the key set, shard once they cross the
        // threshold, and the combined growth trips the per-query limit during the
        // reduce - the breach surfaces with isOutOfMemory() set.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (ts TIMESTAMP, k VARCHAR, v LONG) timestamp(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT (x * 1_000_000)::timestamp, x::varchar, x FROM long_sequence(200_000)",
                                sqlExecutionContext
                        );
                        try (RecordCursorFactory factory = compiler.compile("SELECT k, count(*) FROM tab GROUP BY k", sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
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
    public void testParallelKeyedGroupByOpenFailureReleasesAllocations() throws Exception {
        // A tiny limit breaches during the reduce on the first drain (the chunk index is off the
        // per-query tracker, so nothing per-query-tracked allocates at open); the loop verifies reuse.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (ts TIMESTAMP, k VARCHAR, v LONG) timestamp(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT (x * 1_000_000)::timestamp, (x % 5)::varchar, x FROM long_sequence(1_000)",
                                sqlExecutionContext
                        );
                        try (RecordCursorFactory factory = compiler.compile("SELECT k, count(*) FROM tab GROUP BY k", sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
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
    public void testParallelKeyedGroupBySucceedsOnSmallKeySet() throws Exception {
        // Low-cardinality keyed GROUP BY stays below the sharding threshold; the
        // handful of per-worker maps fit the per-query limit, accounting stays
        // balanced, and the query returns one row per distinct key.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (ts TIMESTAMP, k VARCHAR, v LONG) timestamp(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT (x * 1_000_000)::timestamp, (x % 5)::varchar, x FROM long_sequence(100_000)",
                                sqlExecutionContext
                        );
                        try (RecordCursorFactory factory = compiler.compile("SELECT k, count(*) FROM tab GROUP BY k", sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                long rows = 0;
                                while (cursor.hasNext()) {
                                    rows++;
                                }
                                Assert.assertEquals(5, rows);
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelNotKeyedArrayAggFailsOnLargeSet() throws Exception {
        // Non-keyed parallel array_agg accumulates every input value into a single
        // growing list, allocated through the owner/per-worker FastGroupByAllocators
        // of AsyncGroupByNotKeyedRecordCursorFactory. The atom binds those allocators
        // to the per-query tracker in reopen(), so the combined per-worker reduce and
        // owner merge growth trips the limit and surfaces with isOutOfMemory() set.
        // Without the binding the list grows unbounded and escapes the limit (the
        // GROUP BY allocator caps only the chunk size, not the total), so the query
        // would complete and Assert.fail below would fire.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (ts TIMESTAMP, v DOUBLE) timestamp(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT (x * 1000)::timestamp, x::double FROM long_sequence(2_000_000)",
                                sqlExecutionContext
                        );
                        try (RecordCursorFactory factory = compiler.compile("SELECT array_agg(v) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByNotKeyedRecordCursorFactory.class);
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
    public void testParallelNotKeyedArrayAggOpenFailureReleasesAllocations() throws Exception {
        // Non-keyed variant of testParallelKeyedGroupByOpenFailureReleasesAllocations.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (ts TIMESTAMP, v DOUBLE) timestamp(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT (x * 1000)::timestamp, x::double FROM long_sequence(1_000)",
                                sqlExecutionContext
                        );
                        try (RecordCursorFactory factory = compiler.compile("SELECT array_agg(v) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByNotKeyedRecordCursorFactory.class);
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
    public void testParallelNotKeyedArrayAggReleasesAllocations() throws Exception {
        // A small non-keyed array_agg fits the per-query limit; the owner and
        // per-worker allocators are bound to the tracker on each open and must
        // release every byte on close. Repeated getCursor/close cycles on the same
        // factory, wrapped by assertMemoryLeak, would expose a malloc/free asymmetry
        // between the bound allocators and the retained allocator index, or a tracker
        // imbalance from the close()-time unbinding.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (ts TIMESTAMP, v DOUBLE) timestamp(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT (x * 1000)::timestamp, x::double FROM long_sequence(50_000)",
                                sqlExecutionContext
                        );
                        try (RecordCursorFactory factory = compiler.compile("SELECT array_agg(v) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByNotKeyedRecordCursorFactory.class);
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
    public void testParallelNotKeyedSharedCursorIsUnsupportedWithoutFunctions() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute("CREATE TABLE tab AS (SELECT x v FROM long_sequence(10))", sqlExecutionContext);
                        try (RecordCursorFactory factory = compiler.compile("SELECT sum(v) FROM tab", sqlExecutionContext).getRecordCursorFactory()) {
                            RecordCursorFactory asyncFactory = factory;
                            while (asyncFactory != null && !(asyncFactory instanceof AsyncGroupByNotKeyedRecordCursorFactory)) {
                                asyncFactory = asyncFactory.getBaseFactory();
                            }
                            Assert.assertNotNull(asyncFactory);
                            final RecordCursorFactory factoryWithoutSharedFunctions = asyncFactory;
                            Assert.assertThrows(
                                    UnsupportedOperationException.class,
                                    () -> factoryWithoutSharedFunctions.getSharedCursor(sqlExecutionContext, 1)
                            );
                            Assert.assertFalse(factoryWithoutSharedFunctions.supportsSharedCursors());
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelShardedGroupByReleasesAllocations() throws Exception {
        // A moderate-cardinality keyed GROUP BY crosses the sharding threshold, so the
        // per-fragment shard maps and the destination shards are allocated under the
        // bound tracker, yet stays within the limit. Repeated getCursor/close cycles on
        // the same factory must release every byte they allocate; the assertMemoryLeak
        // around the loop is the load-bearing check - a malloc/free asymmetry between
        // the lazy shard maps and the retained allocator index would show up as a
        // residual native allocation count.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE tab (ts TIMESTAMP, k VARCHAR, v LONG) timestamp(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO tab SELECT (x * 1_000_000)::timestamp, (x % 500)::varchar, x FROM long_sequence(100_000)",
                                sqlExecutionContext
                        );
                        try (RecordCursorFactory factory = compiler.compile("SELECT k, count(*), sum(v) FROM tab GROUP BY k", sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, AsyncGroupByRecordCursorFactory.class);
                            for (int i = 0; i < 10; i++) {
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    long rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals("iteration " + i, 500, rows);
                                }
                            }
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
}
