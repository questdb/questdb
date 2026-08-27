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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.WindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.WindowJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
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
 * column lists, the slave symbol lookup and slaveData hash maps and the prevailing
 * cache - which the cursors bind to the per-query tracker in {@code of()} before the
 * build loop and free in {@code close()}. The maps scale with the join's symbol
 * cardinality, which is what the {@code HighCardinality} cases below exercise.
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
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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
    public void testKeyedWindowJoinHighCardinalitySymbolMapsBreachLimitAtOpen() throws Exception {
        // The symbol-keyed fast cursor carries two native maps next to its allocators:
        // slaveSymbolLookupMap (one entry per master symbol, built in of()) and slaveData (one entry
        // per key in the indexed window). Both scale with symbol cardinality and both used to
        // allocate off the global counter only, so a keyed join over a wide symbol set could run
        // past the configured per-query limit without breaching it.
        //
        // 50k distinct symbols grow the lookup map to a 1 MiB block through doubling rehashes, which
        // is why a 256 KiB limit breaches. The breach lands in getCursor(), not in the drain: of()
        // builds the lookup map before a single row is read, and the group-by allocators next to it
        // hold nothing yet. sum() keeps the aggregate cheap, and the helper pins the breaching
        // allocation's memory tag to NATIVE_UNORDERED_MAP - the allocators charge
        // NATIVE_GROUP_BY_FUNCTION - so no other structure can stand in for the maps. Without the
        // binding getCursor() returns normally and the Assert.fail below fires.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createHighCardinalityTrades(engine, sqlExecutionContext, 50_000);
                        createHighCardinalityPrices(engine, sqlExecutionContext, 50_000);
                        final String query = "SELECT t.ts, sum(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 1 seconds PRECEDING AND 1 seconds FOLLOWING EXCLUDE PREVAILING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, WindowJoinFastRecordCursorFactory.class);
                            assertOpenBreachesOnUnorderedMap(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testKeyedWindowJoinHighCardinalitySymbolMapsReleaseAllocations() throws Exception {
        // The same 50k-symbol join under a limit it fits in. The two maps are bound on each open and
        // must release every byte on close; repeated getCursor/close cycles under assertMemoryLeak
        // expose a malloc/free asymmetry, and the tracker's own assert (Unsafe.recordPerQueryMemAlloc)
        // fires under -ea if a block is ever freed under a tracker other than the one that charged it.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024 * 1024L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createHighCardinalityTrades(engine, sqlExecutionContext, 50_000);
                        createHighCardinalityPrices(engine, sqlExecutionContext, 50_000);
                        final String query = "SELECT t.ts, sum(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 1 seconds PRECEDING AND 1 seconds FOLLOWING EXCLUDE PREVAILING";
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
    public void testKeyedWindowJoinOpenFailureReleasesAllocations() throws Exception {
        // A tiny limit breaches on the keyed cursor's first tracked allocation, which is the symbol
        // lookup map of() builds at open; the loop verifies each breach releases allocations and
        // leaves the factory reusable.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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
    public void testKeyedWindowJoinUnmatchedKeysDoNotGrowAllocatorScalar() throws Exception {
        // A master key absent from the slave leaves slaveData holding 0, and GroupByLongList.of(0)
        // ALLOCATES a fresh empty list rather than binding an existing one. The serial cursors never
        // wrote that pointer back, so every master row carrying an unmatched key allocated again.
        // FastGroupByAllocator.free() is a no-op below its chunk size, so the only reclamation is
        // slaveAllocator.clear() at an index rebuild - and once the slave is exhausted the cursor
        // sets lastSlaveTimestamp = INDEX_COMPLETE (Long.MAX_VALUE), after which the rebuild gate
        // masterTimestampHi > lastSlaveTimestamp can never fire again. Growth is then unbounded for
        // the rest of the scan. The async sibling guards this with `if (rowIdsPtr != 0)`.
        //
        // Symbols are disjoint here ('t...' vs 'p...'), so NO master row matches: the aggregates
        // allocate nothing and the per-query limit can only be reached by the leak. The slave spans
        // 10_000s against the master's 100_000s, so the index completes about a tenth of the way in
        // and the remaining ~90_000 master rows accumulate two lists each, roughly 24 MB against the
        // 8 MiB limit. array_agg is not batch-computable, which is what routes to the scalar cursor.
        assertUnmatchedKeysDoNotGrowAllocator("array_agg(p.price)", false);
    }

    @Test
    public void testKeyedWindowJoinUnmatchedKeysDoNotGrowAllocatorVectorized() throws Exception {
        // The vectorized twin of testKeyedWindowJoinUnmatchedKeysDoNotGrowAllocatorScalar, covering
        // the second unguarded bind. sum() is batch-computable, which is what routes here. It leaks
        // one list per master row rather than two, so roughly 12 MB against the 8 MiB limit. Kept
        // separate so a regression names the cursor that broke instead of stopping at the first.
        assertUnmatchedKeysDoNotGrowAllocator("sum(p.price)", true);
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
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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
    public void testSerialWindowJoinFreesGroupByFunctionsOnClose() throws Exception {
        // Both serial factories took groupByFunctions without adopting it: the generator's catch
        // owns the list only on failure, neither factory kept a field for it, and the cursors only
        // Misc.clearObjList it -- which calls Mutable.clear(), not close(). On the success path the
        // list therefore had no owner at all, while the async siblings free theirs in
        // AsyncWindowJoinAtom.close().
        //
        // The aggregate has to allocate in its CONSTRUCTOR for a compile-and-close cycle to expose
        // that: array_agg sizes its native scratch only when a row is rendered, which is why the
        // sibling test above stays green. string_distinct_agg mallocs two DirectUtf16Sinks in its
        // constructor, so the orphaned function is native memory and assertMemoryLeak sees it.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 100, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.ts, string_distinct_agg(p.sym::string, ',') FROM trades t WINDOW JOIN prices p " +
                                        "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(f, WindowJoinRecordCursorFactory.class);
                            // Drain and render so the aggregate actually accumulates and fills its
                            // sink, rather than only pinning the compile-and-close shape. Note this
                            // does NOT pin the _close() free ordering: the cursor closes before the
                            // factory and its clear is isOpen-guarded, so reordering the two frees
                            // still passes. That ordering is defensive only.
                            assertDrainsRenderingAggregate(f, sqlExecutionContext, 100);
                        }
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.ts, string_distinct_agg(p.sym::string, ',') FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                        "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(f, WindowJoinFastRecordCursorFactory.class);
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
        // WindowJoinFastVectRecordCursor, which carries its own allocator + slaveAllocator on top of
        // the two symbol maps. A tiny limit breaches on the first of those to allocate - the lookup
        // map of() builds at open - well before the slave data the drain would charge.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
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

    // Drains the cursor AND reads the aggregate column. string_distinct_agg fills its output
    // sink only when rendered, so a hasNext()-only drain leaves the sink at its initial capacity
    // and never exercises the grow -> clear() -> resetCapacity -> close() sequence.
    private static void assertDrainsRenderingAggregate(RecordCursorFactory factory, SqlExecutionContext ctx, long expectedRows) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            long rows = 0;
            while (cursor.hasNext()) {
                record.getStrA(1);
                rows++;
            }
            Assert.assertEquals(expectedRows, rows);
        }
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

    // Asserts the breach lands in getCursor() rather than in the drain, that the allocation that
    // breached is one of the maps, and that every repeat of the failed open breaches the same way -
    // i.e. the failed open released what it had charged and left the factory reusable.
    //
    // The memoryTag assertion is what names the maps: they carry NATIVE_UNORDERED_MAP, while the
    // allocators next to them charge NATIVE_GROUP_BY_FUNCTION, so an aggregate that outgrew the
    // limit instead would report a different tag.
    private static void assertOpenBreachesOnUnorderedMap(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        for (int i = 0; i < 3; i++) {
            try (RecordCursor ignore = factory.getCursor(ctx)) {
                Assert.fail("expected a per-query memory breach at open, iteration " + i);
            } catch (CairoException e) {
                Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                TestUtils.assertContains(e.getFlyweightMessage(), "memoryTag=" + MemoryTag.NATIVE_UNORDERED_MAP);
            }
        }
    }

    private static void assertOpenFailureReleasesAllocations(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        for (int i = 0; i < 5; i++) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // The keyed cursor breaches at open, on the symbol lookup map. The non-keyed one
                    // has no map to charge there and breaches during the first drain instead - the
                    // allocators' chunk index is off the per-query tracker, so nothing of theirs
                    // allocates at open. getCursor() sits inside the try to cover both.
                }
                Assert.fail("expected a per-query memory breach at iteration " + i);
            } catch (CairoException e) {
                Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
            }
        }
    }

    private void assertUnmatchedKeysDoNotGrowAllocator(String aggregate, boolean isVectorized) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createDisjointTrades(engine, sqlExecutionContext, 100_000);
                        createDisjointPrices(engine, sqlExecutionContext, 100_000);
                        final String query = "SELECT t.ts, " + aggregate + " " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING EXCLUDE PREVAILING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            assertInTree(factory, WindowJoinFastRecordCursorFactory.class);
                            // Pin WHICH inner cursor runs, so a routing change cannot silently move
                            // this off the site it covers.
                            final TextPlanSink planSink = new TextPlanSink();
                            planSink.of(factory, sqlExecutionContext);
                            TestUtils.assertContains(planSink.getSink(), "vectorized: " + isVectorized);
                            TestUtils.assertContains(planSink.getSink(), "(exclude prevailing)");
                            assertDrainsFully(factory, sqlExecutionContext, 100_000);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertDrainsFully(RecordCursorFactory factory, SqlExecutionContext ctx, long expectedRows) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            long rows = 0;
            while (cursor.hasNext()) {
                rows++;
            }
            Assert.assertEquals(expectedRows, rows);
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

    // Repeats the cursor lifecycle and watches the per-query tracker itself on every cycle, not just
    // the row count. assertMemoryLeak sees only the GLOBAL counter, so it says nothing about which
    // tracker a block was charged to; a cycle that charged the tracker and freed the block off the
    // global counter is globally balanced and passes it.
    //
    // Such an asymmetry does already fail today, but only indirectly and only in the middle of the
    // loop: close() hands the pooled tracker back, and the NEXT acquire trips
    // PerQueryMemoryTracker.init()'s `assert getUsed() == 0` - one iteration late, blaming the query
    // that recycled the block rather than the one that leaked it, and only under -ea. The LAST
    // iteration escapes it entirely, because nothing re-acquires that tracker before the provider
    // destroys it at engine close, which checks no balance. The assertion after close() below states
    // the contract directly instead: every byte the cycle charged is debited by the time the cursor
    // is closed. It fails on the cycle that broke it, names it, covers the last iteration, and holds
    // with assertions disabled.
    //
    // The used > 0 assertion inside the cycle is only there to keep the balance from passing
    // vacuously - it says the query is charged SOMEWHERE, not which structure charged it. What pins
    // the keyed maps specifically is the breach case above, through its memoryTag assertion.
    private static void assertReleasesAllocations(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        long expectedRows = -1;
        for (int i = 0; i < 10; i++) {
            // QueryProgress registers the query on getCursor() and unregisters it on close(), so the
            // context carries a tracker only for the cursor's lifetime.
            Assert.assertNull("a tracker outlived the cursor before iteration " + i, ctx.getMemoryTracker());
            final MemoryTracker tracker;
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                tracker = ctx.getMemoryTracker();
                Assert.assertNotNull("no per-query tracker at iteration " + i, tracker);
                long rows = 0;
                while (cursor.hasNext()) {
                    rows++;
                }
                if (expectedRows == -1) {
                    expectedRows = rows;
                }
                Assert.assertEquals("iteration " + i, expectedRows, rows);
                Assert.assertTrue("expected rows at iteration " + i, rows > 0);
                Assert.assertTrue(
                        "the query charged the per-query tracker nothing at iteration " + i,
                        tracker.getUsed() > 0
                );
            }
            Assert.assertEquals(
                    "the closed cursor left the per-query tracker charged at iteration " + i,
                    0,
                    tracker.getUsed()
            );
        }
    }

    private static void createDisjointPrices(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute(
                "CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        // 0.1s apart, so the slave spans a tenth of the master's range and the index completes early.
        // The 'p' prefix keeps every symbol out of the master's set.
        engine.execute(
                "INSERT INTO prices SELECT (x * 100_000)::timestamp, ('p' || (x % 2))::symbol, x::double FROM long_sequence(" + rows + ")",
                ctx
        );
    }

    private static void createDisjointTrades(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute(
                "CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                "INSERT INTO trades SELECT (x * 1_000_000)::timestamp, ('t' || (x % 8))::symbol, x::double FROM long_sequence(" + rows + ")",
                ctx
        );
    }

    private static void createHighCardinalityPrices(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute(
                "CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL CAPACITY 65536, price DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        // One distinct symbol per row, drawn from the same set as the master, so every master symbol
        // resolves and the lookup map ends up with an entry per master symbol.
        engine.execute(
                "INSERT INTO prices SELECT (x * 1_000_000)::timestamp, ('s' || x)::symbol, x::double FROM long_sequence(" + rows + ")",
                ctx
        );
    }

    private static void createHighCardinalityTrades(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute(
                "CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL CAPACITY 65536, qty DOUBLE) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                "INSERT INTO trades SELECT (x * 1_000_000)::timestamp, ('s' || x)::symbol, x::double FROM long_sequence(" + rows + ")",
                ctx
        );
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
