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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.AsyncWindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
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
        // A slot-leak test can only observe a leak on a slot a worker actually took. With the default
        // threshold of 16, WorkStealingStrategyFactory hands a 4-worker pool the
        // AlwaysWorkStealingStrategy, whose owner never spins before reducing a frame itself, so on
        // few frames it can win most of them and the workers are left with too few to reliably
        // acquire. Dropping the threshold to 1 selects the adaptive strategy - the owner steals only
        // after it has spun - which keeps the acquire off the timing of the box.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        // Selecting the adaptive strategy is necessary but not sufficient: its owner spins for only
        // 50us before stealing, which a worker often fails to wake up inside, so the acquire the
        // leak assertions rest on would still ride on the timing of the box. This gate makes the
        // owner wait for it instead.
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        // The filtering reducers populate the frame while already holding a slot, and only over a
        // parquet master does that populate charge the per-query tracker, so
        // testWindowJoinReleasesWorkerSlotsOnBreach runs those eight over a converted partition too.
        // A parquet scan cuts its page frames on row group boundaries, so the row group size, not
        // CAIRO_SQL_PAGE_FRAME_MAX_ROWS, is what decides how many frames that master fans out into:
        // at the default the whole partition would be a single frame for the owner to reduce alone,
        // and no worker would ever take the slot the test is there to watch. 5_000 gives the 40k-row
        // master eight frames against four workers. The compression codec is left at its default,
        // which already compresses.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 5_000);
        super.setUp();
    }

    @Test
    public void testKeyedWindowJoinArrayAggFailsOnLargeSet() throws Exception {
        // Keyed array_agg over a WINDOW JOIN routes through AsyncWindowJoinFastRecordCursorFactory.
        // Each master row accumulates its matched slave prices into a list allocated through the
        // owner/per-worker FastGroupByAllocators, which the atom binds to the per-query tracker in
        // reopen(). Combined per-worker reduce growth trips the limit. Without the binding the lists
        // escape and the query completes, firing Assert.fail below.
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
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 15 seconds PRECEDING AND 15 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncWindowJoinFastRecordCursorFactory.class);
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
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncWindowJoinFastRecordCursorFactory.class);
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
                            TestUtils.assertFactoryInTree(factory, AsyncWindowJoinFastRecordCursorFactory.class);
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
                        final String query = "SELECT t.ts, array_agg(p.price) " +
                                "FROM trades t WINDOW JOIN prices p " +
                                "RANGE BETWEEN 15 seconds PRECEDING AND 15 seconds FOLLOWING";
                        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(factory, AsyncWindowJoinRecordCursorFactory.class);
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
                            TestUtils.assertFactoryInTree(f, AsyncWindowJoinRecordCursorFactory.class);
                            // intentionally never call getCursor()
                        }
                        try (RecordCursorFactory f = compiler.compile(
                                "SELECT t.ts, array_agg(p.price) FROM trades t WINDOW JOIN prices p ON t.sym = p.sym " +
                                        "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING",
                                sqlExecutionContext).getRecordCursorFactory()) {
                            TestUtils.assertFactoryInTree(f, AsyncWindowJoinFastRecordCursorFactory.class);
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
                            TestUtils.assertFactoryInTree(factory, AsyncWindowJoinRecordCursorFactory.class);
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
                            TestUtils.assertFactoryInTree(factory, AsyncWindowJoinRecordCursorFactory.class);
                            assertReleasesAllocations(factory, sqlExecutionContext);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testWindowJoinReleasesWorkerSlotsOnBreach() throws Exception {
        // A window join reducer acquires a per-worker slot and only then sizes the temporary row id
        // and timestamp lists, whose backing chunk is the first thing the reduce charges to the
        // per-query tracker. With a limit this tight that allocation is the one that throws, so the
        // acquire must sit inside the try that releases the slot. PerWorkerLocks has no reset and
        // the atom belongs to the factory, so a slot leaked on a failed reduce is gone for as long
        // as the factory stays in the SQL cache; once all four have leaked, every worker spins in
        // acquireSlot for a slot nobody will release.
        //
        // There is not one reducer but sixteen, each with its own acquire/release pair, picked by five
        // compile-time flags: dynamic bounds, INCLUDE/EXCLUDE PREVAILING, a join filter, a vectorizable
        // aggregate, and a stolen master filter. So every row below pins its reducer by name - covering
        // one says nothing about the other fifteen, and a re-routed query would still breach, still
        // release, and cover the wrong method.
        //
        // Which master a row needs follows from where in the reducer the breach has to land. The eight
        // aggregate* reducers size the temporary lists while holding the slot, and the of() chunk
        // malloc is the first thing they charge to the tracker, so a native master faults them. The
        // eight filterAndAggregate* ones already sized those lists inside the try before this fix;
        // what the fix moved in is populateFrameMemory(), which on a native frame decodes nothing and
        // so charges nothing - a native master cannot fault it at all, and the row would stay green
        // with the fix reverted. Those eight therefore run twice, once per master: over a parquet
        // master the populate decodes a real column and the breach lands on the moved call, and over
        // the native master it lands further down, on the temporary lists, which have to stay inside
        // the try just as they do in the aggregate* family. Reverting the try/finally in any one
        // reducer turns exactly the row that names it red.
        //
        // 40k master rows over small (1k-row) frames give ~40 page frames, so there is work for the
        // pool to pick up. assertNoSlotLeakOnBreach coordinates each execution so a worker
        // acquires a slot before the owner can take over, then verifies that the cached factory
        // remains reusable.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
        // The owner defers to a worker on the reducer's slot-acquire latch only when it reaches the
        // latch-gated steal branch, and it reaches that branch only once the reduce queue is full.
        // A queue deeper than the master's page-frame count lets the owner publish every frame and
        // then drain them itself through the ungated gang-steal loop, so it can reduce the breaching
        // frame on the owner path - which takes no per-worker slot - before any worker acquires one,
        // and assertNoSlotLeakOnBreach then fails with "no worker acquired a slot". The native master
        // fans out into ~40 frames and fills the 32-deep default queue, but the parquet master cuts on
        // row-group boundaries into only ~8, well under it. Cap the queue below both frame counts so
        // the gate engages for either master.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTrades(engine, sqlExecutionContext, 40_000, 8);
                        createParquetTrades(engine, sqlExecutionContext, 40_000, 8);
                        createPrices(engine, sqlExecutionContext, 1_000, 8);
                        // A fixed window, and a dynamic one: a bound that reads a master column cannot
                        // be folded to a constant, which is what makes the window dynamic.
                        final String fixed = "RANGE BETWEEN 2 seconds PRECEDING AND 2 seconds FOLLOWING";
                        final String dynamic = "RANGE BETWEEN t.qty::long seconds PRECEDING AND 2 seconds FOLLOWING";
                        // array_agg has no batch computation, so it never vectorizes; sum(p.price) over
                        // a slave-only column does. A join filter suppresses vectorization outright, and
                        // so does a dynamic window.
                        final String scalar = "SELECT t.ts, array_agg(p.price) FROM trades t WINDOW JOIN prices p ";
                        final String vect = "SELECT t.ts, sum(p.price) FROM trades t WINDOW JOIN prices p ";

                        // Non-dynamic, EXCLUDE PREVAILING.
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + fixed + " EXCLUDE PREVAILING", "AGGREGATE");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + fixed + " EXCLUDE PREVAILING", "AGGREGATE_VECT");
                        // Non-dynamic, INCLUDE PREVAILING (the default when the clause is absent).
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + fixed + " INCLUDE PREVAILING", "AGGREGATE_PREVAILING");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + fixed + " INCLUDE PREVAILING", "AGGREGATE_VECT_PREVAILING");
                        // A non-symbol ON clause stays with the general factory and becomes the join
                        // filter. A symbol equality would be extracted into the Fast factory instead.
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + "ON p.price > 0 " + fixed + " INCLUDE PREVAILING",
                                "AGGREGATE_PREVAILING_JOIN_FILTERED");
                        // Dynamic bounds: never vectorized, and the join filter is never extracted.
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + dynamic + " EXCLUDE PREVAILING", "AGGREGATE_DYNAMIC");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + dynamic + " INCLUDE PREVAILING", "AGGREGATE_DYNAMIC_PREVAILING");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + "ON t.sym = p.sym " + dynamic + " INCLUDE PREVAILING",
                                "AGGREGATE_DYNAMIC_PREVAILING_JOIN_FILTERED");

                        // The same eight with a WHERE over the master. The parallel filter factory
                        // hands it to the atom, which routes the reduce to the filterAndAggregate
                        // family; without the steal these would cover the eight above a second time.
                        //
                        // Each of the eight runs twice, once per master, because the two masters breach
                        // at different points in the same reducer and neither alone covers it. Over the
                        // native master the breach lands on the temporary lists, as it does for the
                        // aggregate* family above; those calls sit inside the try and must stay there.
                        // Over the parquet master it lands earlier, on populateFrameMemory(), which is
                        // the call this fix moved in - and the only one a native frame cannot fault,
                        // since it decodes nothing there and so charges the tracker nothing.
                        final String where = " WHERE t.qty > 0";
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + fixed + " EXCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + fixed + " EXCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE_VECT");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + fixed + " INCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE_PREVAILING");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + fixed + " INCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE_VECT_PREVAILING");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + "ON p.price > 0 " + fixed + " INCLUDE PREVAILING" + where,
                                "FILTER_AND_AGGREGATE_PREVAILING_JOIN_FILTERED");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + dynamic + " EXCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE_DYNAMIC");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + dynamic + " INCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE_DYNAMIC_PREVAILING");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + "ON t.sym = p.sym " + dynamic + " INCLUDE PREVAILING" + where,
                                "FILTER_AND_AGGREGATE_DYNAMIC_PREVAILING_JOIN_FILTERED");

                        // And the same eight over the parquet master. The ts predicate is interval-
                        // extracted off the designated timestamp, so it pins the scan to the converted
                        // partition without becoming a master filter of its own; t.s != 'zzz' is the
                        // master filter that routes to this family. Filtering on the wide VARCHAR is
                        // belt and braces: the limit is far below any decoded column, so the populate
                        // would fault on the narrow ones too, but a filter over the widest column keeps
                        // the row faulting even if that limit is ever loosened.
                        final String pqScalar = "SELECT t.ts, array_agg(p.price) FROM parquet_trades t WINDOW JOIN prices p ";
                        final String pqVect = "SELECT t.ts, sum(p.price) FROM parquet_trades t WINDOW JOIN prices p ";
                        final String pqWhere = " WHERE t.ts < '1970-01-02' AND t.s != 'zzz'";
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                pqScalar + fixed + " EXCLUDE PREVAILING" + pqWhere, "FILTER_AND_AGGREGATE");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                pqVect + fixed + " EXCLUDE PREVAILING" + pqWhere, "FILTER_AND_AGGREGATE_VECT");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                pqScalar + fixed + " INCLUDE PREVAILING" + pqWhere, "FILTER_AND_AGGREGATE_PREVAILING");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                pqVect + fixed + " INCLUDE PREVAILING" + pqWhere, "FILTER_AND_AGGREGATE_VECT_PREVAILING");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                pqVect + "ON p.price > 0 " + fixed + " INCLUDE PREVAILING" + pqWhere,
                                "FILTER_AND_AGGREGATE_PREVAILING_JOIN_FILTERED");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                pqVect + dynamic + " EXCLUDE PREVAILING" + pqWhere, "FILTER_AND_AGGREGATE_DYNAMIC");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                pqVect + dynamic + " INCLUDE PREVAILING" + pqWhere, "FILTER_AND_AGGREGATE_DYNAMIC_PREVAILING");
                        assertReducerReleasesSlots(compiler, sqlExecutionContext,
                                pqVect + "ON t.sym = p.sym " + dynamic + " INCLUDE PREVAILING" + pqWhere,
                                "FILTER_AND_AGGREGATE_DYNAMIC_PREVAILING_JOIN_FILTERED");

                        // A symbol equality in the ON clause is extracted into the join key, which
                        // routes to the keyed AsyncWindowJoinFastRecordCursorFactory and its own
                        // atom. It has ten reducers of its own, on the same five flags minus the
                        // dynamic window (a dynamic bound keeps the query on the general factory),
                        // and each owns its own acquire/release pair - so they are pinned by name
                        // for the reason the sixteen above are.
                        final String on = "ON t.sym = p.sym ";
                        // A second ON predicate cannot be folded into the key, so it stays behind as
                        // the join filter, which also suppresses vectorization.
                        final String onFiltered = "ON t.sym = p.sym AND p.price > 0 ";
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + on + fixed + " EXCLUDE PREVAILING", "AGGREGATE");
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + on + fixed + " EXCLUDE PREVAILING", "AGGREGATE_VECT");
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + on + fixed + " INCLUDE PREVAILING", "AGGREGATE_PREVAILING");
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + on + fixed + " INCLUDE PREVAILING", "AGGREGATE_VECT_PREVAILING");
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + onFiltered + fixed + " INCLUDE PREVAILING",
                                "AGGREGATE_PREVAILING_JOIN_FILTERED");

                        // The same five with a WHERE over the master, which the parallel filter
                        // factory hands to the atom, routing the reduce to the filtering family.
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + on + fixed + " EXCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE");
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + on + fixed + " EXCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE_VECT");
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + on + fixed + " INCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE_PREVAILING");
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                vect + on + fixed + " INCLUDE PREVAILING" + where, "FILTER_AND_AGGREGATE_VECT_PREVAILING");
                        assertFastReducerReleasesSlots(compiler, sqlExecutionContext,
                                scalar + onFiltered + fixed + " INCLUDE PREVAILING" + where,
                                "FILTER_AND_AGGREGATE_PREVAILING_JOIN_FILTERED");
                    },
                    configuration,
                    LOG
            );
        });
    }

    /**
     * The {@link #assertReducerReleasesSlots} of the keyed (Fast) factory: asserts that {@code query}
     * routes to the named one of its ten reducers, and that the reducer releases every per-worker
     * slot it takes when the reduce breaches the per-query memory limit.
     */
    private static void assertFastReducerReleasesSlots(
            SqlCompiler compiler,
            SqlExecutionContext ctx,
            String query,
            String expectedReducer
    ) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
            TestUtils.assertFactoryInTree(factory, AsyncWindowJoinFastRecordCursorFactory.class, query);
            AsyncWindowJoinFastRecordCursorFactory joinFactory = null;
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                if (f instanceof AsyncWindowJoinFastRecordCursorFactory fastFactory) {
                    joinFactory = fastFactory;
                    break;
                }
            }
            Assert.assertNotNull(query, joinFactory);
            Assert.assertEquals(
                    "query routed to a different reducer: " + query,
                    expectedReducer,
                    joinFactory.getReducerName()
            );
        }
        TestUtils.assertNoSlotLeakOnBreach(compiler, ctx, query);
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

    /**
     * Asserts that {@code query} routes to the named reducer, and that the reducer releases every
     * per-worker slot it takes when the reduce breaches the per-query memory limit. Pinning the name
     * is the point: a query that drifted to a neighbouring reducer would still breach and still pass
     * a bare slot-count assertion, leaving the intended one uncovered.
     */
    private static void assertReducerReleasesSlots(
            SqlCompiler compiler,
            SqlExecutionContext ctx,
            String query,
            String expectedReducer
    ) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
            TestUtils.assertFactoryInTree(factory, AsyncWindowJoinRecordCursorFactory.class, query);
            AsyncWindowJoinRecordCursorFactory joinFactory = null;
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                if (f instanceof AsyncWindowJoinRecordCursorFactory windowJoinFactory) {
                    joinFactory = windowJoinFactory;
                    break;
                }
            }
            Assert.assertNotNull(query, joinFactory);
            Assert.assertEquals(
                    "query routed to a different reducer: " + query,
                    expectedReducer,
                    joinFactory.getReducerName()
            );
        }
        TestUtils.assertNoSlotLeakOnBreach(compiler, ctx, query);
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

    /**
     * A master whose first partition is parquet, carrying a wide VARCHAR the scan has to decode.
     * The filtering reducers populate the frame while already holding a per-worker slot, and that
     * decode is the only tracked allocation they make there - on a native master the same call
     * charges nothing, so it cannot breach the limit and cannot fault the slot release.
     */
    private static void createParquetTrades(CairoEngine engine, SqlExecutionContext ctx, int rows, int symbols) throws Exception {
        engine.execute(
                "CREATE TABLE parquet_trades (ts TIMESTAMP, sym SYMBOL, qty DOUBLE, s VARCHAR) timestamp(ts) PARTITION BY DAY",
                ctx
        );
        // Same shape as trades, plus a ~256-byte value per row, which is what the filtering queries
        // filter on: the widest column is the one whose decode a reducer is least likely to skip.
        engine.execute(
                "INSERT INTO parquet_trades SELECT (x * 1_000_000)::timestamp, (x % " + symbols + ")::symbol, x::double,"
                        + " rpad(x::varchar, 256, 'a') FROM long_sequence(" + rows + ")",
                ctx
        );
        // A row in a later partition seals the first one, which is what CONVERT needs.
        engine.execute("INSERT INTO parquet_trades VALUES ('1970-01-02T00:00:00.000000Z', '0', -1, 'z')", ctx);
        engine.execute("ALTER TABLE parquet_trades CONVERT PARTITION TO PARQUET LIST '1970-01-01'", ctx);
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
