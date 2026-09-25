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
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.join.AbstractJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinDenseRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.LtJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncMultiHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHorizonJoinNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;

/**
 * Verifies that every owner of a {@link SymbolTranslatingRecord} binds the per-query memory
 * tracker to it, so that the translation caches count toward the per-query memory limit.
 * <p>
 * Each query translates 40,000 distinct symbols, so a translation cache grows to 1 MiB
 * (131,072 slots of 8 bytes) and briefly holds 1.5 MiB while it rehashes from 512 KiB. The other
 * side of the join holds none of these symbols in the hash join tables, and only the symbol '1'
 * in the time-series and HORIZON tables; the cache stores a miss just as it stores a hit, so it
 * grows the same either way. The join's own tracked structures do not grow with the translated
 * symbols:
 * <ul>
 *   <li>the time-series and HORIZON joins read a one-row slave, so their maps hold one key;</li>
 *   <li>the hash joins translate every build-side symbol to VALUE_NOT_FOUND, so their join key
 *   map holds one key, and the build side fits the first 512 KiB page of the slave chain.</li>
 * </ul>
 * For each query, the helper first drains the cursor with translation caching disabled and
 * expects no breach, which shows that nothing but the cache growth can cross the 1 MiB limit.
 * It then drains the cursor with caching enabled and expects a breach at a
 * {@link MemoryTag#NATIVE_JOIN_MAP} allocation. For these queries, only the translation caches
 * allocate under that tag once the query compiles, both when a cursor opens and when it runs.
 * Without the tracker binding the caches escape the limit, the query completes and trips
 * {@code Assert.fail}.
 * <p>
 * An async HORIZON atom binds the tracker to its owner record and, separately, to each
 * per-worker record, and a query uses the record of whichever thread reduces the page frame.
 * The owner pass runs without a worker pool, so the query owner reduces every frame. The worker
 * pass gates the owner with {@link SlotGatedWorkStealingStrategy} until a worker takes a slot,
 * and the master table fits a single page frame, so the worker that takes the slot reduces the
 * whole table. With a second frame, the owner could steal part of the table once the gate opens,
 * and the owner record would then share the translations with the per-worker record.
 * <p>
 * The open-failure tests check that a breach while a join opens its translation caches frees
 * each child cursor exactly once. A failed open leaves the child cursors to the catch block
 * in the join factory's getCursor(), which closes them, and then closes the join cursor, so a
 * join cursor that had already adopted them closes them a second time. The helper steps the
 * per-query limit through the allocations of an open, one allocation per step, and a wrapper
 * around each child factory checks after every failed open that each child cursor received
 * exactly one close() call.
 */
public class SymbolTranslationCacheMemoryTrackerTest extends AbstractCairoTest {
    // An open makes a few dozen allocations at most; the bound only stops a runaway loop.
    private static final int MAX_OPEN_STEPS = 100;
    private static final long QUERY_MEMORY_LIMIT = 1024 * 1024L;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, QUERY_MEMORY_LIMIT);
        // 40,000 build-side rows take 480,000 bytes in a slave chain (12 bytes per row), so the
        // chains fit their first page and never grow. The full-fat default page is 16 MiB.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 512 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 512 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 4 * 1024L);
    }

    @Test
    public void testAsyncHorizonJoinsChargeOwnerTranslationCaches() throws Exception {
        // No worker pool consumes the reduce queue of this engine, so the query owner reduces
        // every page frame with its own translating records, and the per-worker records stay idle.
        assertMemoryLeak(() -> {
            createTimeSeriesTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertHorizonJoins(
                        compiler,
                        sqlExecutionContext,
                        false,
                        AsyncHorizonJoinRecordCursorFactory.class,
                        AsyncHorizonJoinNotKeyedRecordCursorFactory.class,
                        AsyncMultiHorizonJoinRecordCursorFactory.class,
                        AsyncMultiHorizonJoinNotKeyedRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testAsyncHorizonJoinsChargeWorkerTranslationCaches() throws Exception {
        // The frame sequence reads the provider when the query compiles, and the overrides reset
        // it after the test. Queries without a latch run under the plain adaptive strategy.
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        // The test configuration splits the master table into 4 page frames of 10,000 rows, one per
        // worker. This minimum frame size keeps the table in the single frame that the class javadoc
        // asks for. TestUtils.execute() creates the context, which reads the minimum frame size,
        // after this call.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100_000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTimeSeriesTables(engine, sqlExecutionContext);
                        assertSingleMasterFrame(compiler, sqlExecutionContext);
                        assertHorizonJoins(
                                compiler,
                                sqlExecutionContext,
                                true,
                                AsyncHorizonJoinRecordCursorFactory.class,
                                AsyncHorizonJoinNotKeyedRecordCursorFactory.class,
                                AsyncMultiHorizonJoinRecordCursorFactory.class,
                                AsyncMultiHorizonJoinNotKeyedRecordCursorFactory.class
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testFullFatHashJoinOpenFailuresFreeChildCursorsOnce() throws Exception {
        assertMemoryLeak(() -> {
            createOpenFailureTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // the compiler pool resets the flag when the compiler returns to the pool
                compiler.setFullFatJoins(true);
                assertHashJoinOpenFailures(
                        compiler,
                        HashJoinRecordCursorFactory.class,
                        HashOuterJoinRecordCursorFactory.class,
                        HashOuterJoinFilteredRecordCursorFactory.class,
                        false
                );
            }
        });
    }

    @Test
    public void testFullFatHashJoinsChargeTranslationCaches() throws Exception {
        assertMemoryLeak(() -> {
            createHashJoinTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertHashJoins(
                        compiler,
                        HashJoinRecordCursorFactory.class,
                        HashOuterJoinRecordCursorFactory.class,
                        HashOuterJoinFilteredRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testHashJoinOpenFailuresFreeChildCursorsOnce() throws Exception {
        assertMemoryLeak(() -> {
            createOpenFailureTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertHashJoinOpenFailures(
                        compiler,
                        HashJoinLightRecordCursorFactory.class,
                        HashOuterJoinLightRecordCursorFactory.class,
                        HashOuterJoinFilteredLightRecordCursorFactory.class,
                        true
                );
            }
        });
    }

    @Test
    public void testHashJoinsChargeTranslationCaches() throws Exception {
        assertMemoryLeak(() -> {
            createHashJoinTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertHashJoins(
                        compiler,
                        HashJoinLightRecordCursorFactory.class,
                        HashOuterJoinLightRecordCursorFactory.class,
                        HashOuterJoinFilteredLightRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testSyncHorizonJoinsChargeTranslationCaches() throws Exception {
        // setUp() has already copied the configuration flag into the context, and the code
        // generator reads the context, so the test switches the context itself. The next
        // setUp() restores the flag from the configuration.
        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
        assertMemoryLeak(() -> {
            createTimeSeriesTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertHorizonJoins(
                        compiler,
                        sqlExecutionContext,
                        false,
                        HorizonJoinRecordCursorFactory.class,
                        HorizonJoinNotKeyedRecordCursorFactory.class,
                        MultiHorizonJoinRecordCursorFactory.class,
                        MultiHorizonJoinNotKeyedRecordCursorFactory.class
                );
            }
        });
    }

    @Test
    public void testTimeSeriesJoinOpenFailuresFreeChildCursorsOnce() throws Exception {
        // The fast and dense ASOF joins read the slave through a time frame cursor, which the counting
        // wrapper does not provide, so the helper wraps only their master factory.
        assertMemoryLeak(() -> {
            createOpenFailureTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertOpenFailuresFreeChildCursorsOnce(
                        compiler,
                        "SELECT om.k1, os.price FROM om ASOF JOIN os ON (k1, k2)",
                        AsOfJoinFastRecordCursorFactory.class,
                        false
                );
                assertOpenFailuresFreeChildCursorsOnce(
                        compiler,
                        "SELECT om.k1, os.price FROM om ASOF JOIN (os WHERE price >= 0) os ON (k1, k2)",
                        FilteredAsOfJoinFastRecordCursorFactory.class,
                        false
                );
                assertOpenFailuresFreeChildCursorsOnce(
                        compiler,
                        "SELECT /*+ asof_dense(om os) */ om.k1, os.price FROM om ASOF JOIN os ON (k1, k2)",
                        AsOfJoinDenseRecordCursorFactory.class,
                        false
                );
                assertOpenFailuresFreeChildCursorsOnce(
                        compiler,
                        "SELECT /*+ asof_linear(om os) */ om.k1, os.price FROM om ASOF JOIN os ON (k1, k2)",
                        AsOfJoinLightRecordCursorFactory.class,
                        true
                );
                assertOpenFailuresFreeChildCursorsOnce(
                        compiler,
                        "SELECT om.k1, os.price FROM om LT JOIN os ON (k1, k2)",
                        LtJoinLightRecordCursorFactory.class,
                        true
                );
            }
        });
    }

    @Test
    public void testTimeSeriesJoinsChargeTranslationCaches() throws Exception {
        assertMemoryLeak(() -> {
            createTimeSeriesTables(engine, sqlExecutionContext);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertCachesChargeTracker(
                        compiler,
                        sqlExecutionContext,
                        "SELECT m.k1 FROM m ASOF JOIN s ON (k1, k2)",
                        AsOfJoinFastRecordCursorFactory.class
                );
                assertCachesChargeTracker(
                        compiler,
                        sqlExecutionContext,
                        "SELECT m.k1 FROM m ASOF JOIN (s WHERE price >= 0) s ON (k1, k2)",
                        FilteredAsOfJoinFastRecordCursorFactory.class
                );
                assertCachesChargeTracker(
                        compiler,
                        sqlExecutionContext,
                        "SELECT /*+ asof_dense(m s) */ m.k1 FROM m ASOF JOIN s ON (k1, k2)",
                        AsOfJoinDenseRecordCursorFactory.class
                );
                assertCachesChargeTracker(
                        compiler,
                        sqlExecutionContext,
                        "SELECT /*+ asof_linear(m s) */ m.k1 FROM m ASOF JOIN s ON (k1, k2)",
                        AsOfJoinLightRecordCursorFactory.class
                );
                assertCachesChargeTracker(
                        compiler,
                        sqlExecutionContext,
                        "SELECT m.k1 FROM m LT JOIN s ON (k1, k2)",
                        LtJoinLightRecordCursorFactory.class
                );
            }
        });
    }

    private static void assertCachesChargeTracker(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String query,
            Class<? extends RecordCursorFactory> expectedFactory
    ) throws Exception {
        assertCachesChargeTracker(compiler, sqlExecutionContext, query, expectedFactory, false);
    }

    private static void assertCachesChargeTracker(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String query,
            Class<? extends RecordCursorFactory> expectedFactory,
            boolean isWorkerReducing
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            // guards against a silent reroute to a factory that the test does not target
            TestUtils.assertFactoryInTree(factory, expectedFactory, query);
            // findPerWorkerLocks() fails when the atom holds no per-worker locks to gate on
            final PerWorkerLocks workerLocks = isWorkerReducing ? TestUtils.findPerWorkerLocks(factory, query) : null;

            // With caching disabled the caches keep their initial 256 bytes, so the query must fit the limit.
            setProperty(PropertyKey.CAIRO_SQL_JOIN_SYMBOL_TRANSLATION_CACHE_CAPACITY, 0);
            try {
                openAndDrain(factory, sqlExecutionContext, workerLocks, query, false);
            } catch (CairoException e) {
                throw new AssertionError("breach with translation caching disabled: " + query + ", " + e.getFlyweightMessage(), e);
            } finally {
                setProperty(PropertyKey.CAIRO_SQL_JOIN_SYMBOL_TRANSLATION_CACHE_CAPACITY, null);
            }

            // Reusing the factory checks that each open binds the tracker before the caches reopen.
            for (int i = 0; i < 2; i++) {
                try {
                    openAndDrain(factory, sqlExecutionContext, workerLocks, query, true);
                    Assert.fail("expected a per-query memory breach at iteration " + i + ": " + query);
                } catch (CairoException e) {
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "memoryTag=" + MemoryTag.NATIVE_JOIN_MAP + ']');
                }
            }
        }
    }

    private static void assertFullJoinOpenFailures(
            SqlCompiler compiler,
            String filter,
            Class<? extends RecordCursorFactory> expectedFactory,
            boolean isLight
    ) throws Exception {
        // om holds fewer rows than os. A Light full join builds its hash table from the smaller
        // side, so it swaps the sides when om is the master, and it reads the build side through
        // random access, which the wrapper counts. A full-fat join copies the build side into a
        // record chain and never swaps the sides.
        final CloseCountingFactory swappedMaster = assertOpenFailuresFreeChildCursorsOnce(
                compiler,
                "SELECT om.k1, os.k1 FROM om FULL JOIN os ON om.k1 = os.k1 AND om.k2 = os.k2" + filter,
                expectedFactory,
                true
        );
        Assert.assertEquals(
                "om, the master, must be the build side only for a Light join",
                isLight,
                swappedMaster.getRecordAtCount() > 0
        );
        final CloseCountingFactory keptMaster = assertOpenFailuresFreeChildCursorsOnce(
                compiler,
                "SELECT om.k1, os.k1 FROM os FULL JOIN om ON om.k1 = os.k1 AND om.k2 = os.k2" + filter,
                expectedFactory,
                true
        );
        Assert.assertEquals("os, the master, must not be the build side", 0, keptMaster.getRecordAtCount());
    }

    private static void assertHashJoinOpenFailures(
            SqlCompiler compiler,
            Class<? extends RecordCursorFactory> innerJoinFactory,
            Class<? extends RecordCursorFactory> outerJoinFactory,
            Class<? extends RecordCursorFactory> filteredOuterJoinFactory,
            boolean isLight
    ) throws Exception {
        assertOpenFailuresFreeChildCursorsOnce(
                compiler,
                "SELECT om.k1 FROM om JOIN os ON (k1, k2)",
                innerJoinFactory,
                true
        );
        assertOpenFailuresFreeChildCursorsOnce(
                compiler,
                "SELECT om.k1 FROM om LEFT JOIN os ON (k1, k2)",
                outerJoinFactory,
                true
        );
        assertOpenFailuresFreeChildCursorsOnce(
                compiler,
                "SELECT os.k1 FROM om RIGHT JOIN os ON (k1, k2)",
                outerJoinFactory,
                true
        );
        assertFullJoinOpenFailures(compiler, "", outerJoinFactory, isLight);
        // A non-equi condition routes to the filtered factories.
        final String filter = " AND om.ts <> os.ts";
        assertOpenFailuresFreeChildCursorsOnce(
                compiler,
                "SELECT om.k1 FROM om LEFT JOIN os ON om.k1 = os.k1 AND om.k2 = os.k2" + filter,
                filteredOuterJoinFactory,
                true
        );
        assertOpenFailuresFreeChildCursorsOnce(
                compiler,
                "SELECT os.k1 FROM om RIGHT JOIN os ON om.k1 = os.k1 AND om.k2 = os.k2" + filter,
                filteredOuterJoinFactory,
                true
        );
        assertFullJoinOpenFailures(compiler, filter, filteredOuterJoinFactory, isLight);
    }

    private static void assertHashJoins(
            SqlCompiler compiler,
            Class<? extends RecordCursorFactory> innerJoinFactory,
            Class<? extends RecordCursorFactory> outerJoinFactory,
            Class<? extends RecordCursorFactory> filteredOuterJoinFactory
    ) throws Exception {
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                "SELECT hm.k FROM hm JOIN hs ON (k)",
                innerJoinFactory
        );
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                "SELECT hm.k FROM hm LEFT JOIN hs ON (k)",
                outerJoinFactory
        );
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                "SELECT hs.k FROM hm RIGHT JOIN hs ON (k)",
                outerJoinFactory
        );
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                "SELECT hm.k, hs.k FROM hm FULL JOIN hs ON (k)",
                outerJoinFactory
        );
        // A non-equi condition routes to the filtered factories. No key matches, so the
        // condition never runs.
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                "SELECT hm.k FROM hm LEFT JOIN hs ON hm.k = hs.k AND hm.k <> hs.k",
                filteredOuterJoinFactory
        );
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                "SELECT hs.k FROM hm RIGHT JOIN hs ON hm.k = hs.k AND hm.k <> hs.k",
                filteredOuterJoinFactory
        );
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                "SELECT hm.k, hs.k FROM hm FULL JOIN hs ON hm.k = hs.k AND hm.k <> hs.k",
                filteredOuterJoinFactory
        );
    }

    private static void assertHorizonJoins(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            boolean isWorkerReducing,
            Class<? extends RecordCursorFactory> keyedFactory,
            Class<? extends RecordCursorFactory> notKeyedFactory,
            Class<? extends RecordCursorFactory> multiKeyedFactory,
            Class<? extends RecordCursorFactory> multiNotKeyedFactory
    ) throws Exception {
        // The horizon offset is the only GROUP BY key, so the keyed data maps hold a single key.
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                """
                        SELECT h.offset, count() FROM m
                        HORIZON JOIN s ON (m.k1 = s.k1 AND m.k2 = s.k2)
                        RANGE FROM 0s TO 0s STEP 1s AS h
                        """,
                keyedFactory,
                isWorkerReducing
        );
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                """
                        SELECT count(s.price) FROM m
                        HORIZON JOIN s ON (m.k1 = s.k1 AND m.k2 = s.k2)
                        RANGE FROM 0s TO 0s STEP 1s AS h
                        """,
                notKeyedFactory,
                isWorkerReducing
        );
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                """
                        SELECT h.offset, count(s.price), count(s2.price) FROM m
                        HORIZON JOIN s ON (m.k1 = s.k1 AND m.k2 = s.k2)
                        HORIZON JOIN s s2 ON (m.k1 = s2.k1 AND m.k2 = s2.k2)
                        RANGE FROM 0s TO 0s STEP 1s AS h
                        """,
                multiKeyedFactory,
                isWorkerReducing
        );
        assertCachesChargeTracker(
                compiler,
                sqlExecutionContext,
                """
                        SELECT count(s.price), count(s2.price) FROM m
                        HORIZON JOIN s ON (m.k1 = s.k1 AND m.k2 = s.k2)
                        HORIZON JOIN s s2 ON (m.k1 = s2.k1 AND m.k2 = s2.k2)
                        RANGE FROM 0s TO 0s STEP 1s AS h
                        """,
                multiNotKeyedFactory,
                isWorkerReducing
        );
    }

    /**
     * Steps the per-query limit through the allocations of a cursor open, until an open fits
     * the limit. Each failed open admits one more allocation than the previous one, so the opens
     * breach at every allocation in turn, and the first translation cache allocation must be one
     * of them. After each failed open, each child cursor must have received exactly one close()
     * call. Returns the wrapper around the master factory.
     */
    private static CloseCountingFactory assertOpenFailuresFreeChildCursorsOnce(
            SqlCompiler compiler,
            String query,
            Class<? extends RecordCursorFactory> expectedFactory,
            boolean isSlaveWrapped
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
            final RecordCursorFactory join = findFactory(factory, expectedFactory, query);
            final CloseCountingFactory master = wrapChildFactory(join, "masterFactory");
            final CloseCountingFactory slave = isSlaveWrapped ? wrapChildFactory(join, "slaveFactory") : null;
            boolean isCacheBreachSeen = false;
            long limit = 1;
            RecordCursor cursor = null;
            for (int step = 0; cursor == null; step++) {
                Assert.assertTrue("no open fits the limit after " + step + " steps: " + query, step < MAX_OPEN_STEPS);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                try {
                    cursor = factory.getCursor(sqlExecutionContext);
                } catch (CairoException e) {
                    final CharSequence message = e.getFlyweightMessage();
                    Assert.assertTrue("expected isOutOfMemory(), got: " + message, e.isOutOfMemory());
                    // a NATIVE_JOIN_MAP breach is a translation cache breach, see the class javadoc
                    isCacheBreachSeen |= Chars.contains(message, "memoryTag=" + MemoryTag.NATIVE_JOIN_MAP + ']');
                    master.assertCursorsClosedOnce(query, message);
                    if (slave != null) {
                        slave.assertCursorsClosedOnce(query, message);
                    }
                    // The next open admits the allocation that breached, and breaches at the one after it.
                    limit = parseBreachAttribute(message, "used=") + parseBreachAttribute(message, "size=");
                }
            }
            cursor.close();
            Assert.assertTrue("no open breached at a translation cache allocation: " + query, isCacheBreachSeen);

            // The failed opens leave the factory reusable.
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, QUERY_MEMORY_LIMIT);
            try (RecordCursor reusedCursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.drainCursor(reusedCursor);
            }
            master.assertCursorsClosedOnce(query, "after a successful run");
            master.assertCursorsOpened(query);
            if (slave != null) {
                slave.assertCursorsClosedOnce(query, "after a successful run");
                slave.assertCursorsOpened(query);
            }
            return master;
        }
    }

    private static void assertSingleMasterFrame(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception {
        try (
                RecordCursorFactory factory = compiler.compile("m", sqlExecutionContext).getRecordCursorFactory();
                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)
        ) {
            Assert.assertNotNull("no page frame in the master table", cursor.next());
            Assert.assertNull("the master table must fit a single page frame", cursor.next());
        }
    }

    private static void createHashJoinTables(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws Exception {
        // Both sides hold as many rows as distinct symbols, so the Light hash joins keep the
        // build side, and no symbol appears on both sides.
        engine.execute(
                "CREATE TABLE hm AS (SELECT ('m' || x)::SYMBOL k FROM long_sequence(40_000))",
                sqlExecutionContext
        );
        engine.execute(
                "CREATE TABLE hs AS (SELECT ('s' || x)::SYMBOL k FROM long_sequence(40_000))",
                sqlExecutionContext
        );
    }

    private static void createOpenFailureTables(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws Exception {
        // om holds fewer rows than os, and both tables hold the keys k1 to k3. ASOF joins take the
        // SymbolTranslatingRecord path only for multi-column keys, so every query joins on (k1, k2).
        engine.execute(
                """
                        CREATE TABLE om AS (
                            SELECT ('k' || x)::SYMBOL k1, ('k' || x)::SYMBOL k2, (x * 1_000_000)::TIMESTAMP ts
                            FROM long_sequence(3)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """,
                sqlExecutionContext
        );
        engine.execute(
                """
                        CREATE TABLE os AS (
                            SELECT ('k' || x)::SYMBOL k1, ('k' || x)::SYMBOL k2, x::DOUBLE price, (x * 1_000_000)::TIMESTAMP ts
                            FROM long_sequence(5)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """,
                sqlExecutionContext
        );
    }

    private static void createTimeSeriesTables(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws Exception {
        // ASOF joins take the SymbolTranslatingRecord path only for multi-column keys. The master
        // table fits a single DAY partition.
        engine.execute(
                """
                        CREATE TABLE m AS (
                            SELECT x::SYMBOL k1, x::SYMBOL k2, (x * 1_000_000)::TIMESTAMP ts
                            FROM long_sequence(40_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """,
                sqlExecutionContext
        );
        engine.execute(
                """
                        CREATE TABLE s AS (
                            SELECT x::SYMBOL k1, x::SYMBOL k2, x::DOUBLE price, (x * 1_000_000)::TIMESTAMP ts
                            FROM long_sequence(1)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """,
                sqlExecutionContext
        );
    }

    private static RecordCursorFactory findFactory(
            RecordCursorFactory factory,
            Class<? extends RecordCursorFactory> expectedFactory,
            String query
    ) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (expectedFactory.isInstance(f)) {
                return f;
            }
        }
        throw new AssertionError("expected " + expectedFactory.getSimpleName() + " in the factory tree, but top was "
                + factory.getClass().getSimpleName() + ": " + query);
    }

    /**
     * Opens a cursor and drains it. Given per-worker locks, it holds the query owner off until a
     * worker takes a slot, and then checks that a worker did. It runs the check after a successful
     * drain and after an expected breach, and lets an unexpected breach reach the caller unchanged.
     */
    private static void openAndDrain(
            RecordCursorFactory factory,
            SqlExecutionContext sqlExecutionContext,
            @Nullable PerWorkerLocks workerLocks,
            String query,
            boolean isBreachExpected
    ) throws SqlException {
        if (workerLocks == null) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.drainCursor(cursor);
            }
            return;
        }
        // a fresh latch per open, so that the acquisition it records belongs to this open
        final CountDownLatch acquired = new CountDownLatch(1);
        workerLocks.setTestAcquireLatch(acquired);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            TestUtils.drainCursor(cursor);
        } catch (CairoException e) {
            // A latch that still stands means that the owner reduced the frame itself, so an
            // expected breach would say nothing about the per-worker records.
            if (isBreachExpected && acquired.getCount() != 0) {
                throw new AssertionError("no worker acquired a slot: " + query, e);
            }
            throw e;
        } finally {
            workerLocks.setTestAcquireLatch(null);
        }
        Assert.assertEquals("no worker acquired a slot: " + query, 0, acquired.getCount());
    }

    // Reads a numeric attribute, such as "used=", from a per-query memory limit breach message.
    private static long parseBreachAttribute(CharSequence message, String name) {
        final int nameLo = Chars.indexOf(message, 0, message.length(), name);
        Assert.assertTrue("no " + name + " attribute in: " + message, nameLo > -1);
        final int valueLo = nameLo + name.length();
        final int valueHi = Chars.indexOf(message, valueLo, message.length(), ',');
        Assert.assertTrue("unterminated " + name + " attribute in: " + message, valueHi > valueLo);
        return Numbers.parseLong(message, valueLo, valueHi);
    }

    // AbstractJoinRecordCursorFactory has no setter for its child factories, so the test replaces
    // the field. A renamed field fails the lookup, not the assertions.
    private static CloseCountingFactory wrapChildFactory(RecordCursorFactory join, String fieldName) throws Exception {
        final Field field = AbstractJoinRecordCursorFactory.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        final CloseCountingFactory wrapper = new CloseCountingFactory((RecordCursorFactory) field.get(join));
        field.set(join, wrapper);
        return wrapper;
    }

    /**
     * Forwards every RecordCursor call, the default methods included, to the cursor of the wrapped
     * child factory, and counts the opens, the close() calls and the random access reads. It
     * forwards each close() call, so the child cursor sees exactly the calls that the join makes.
     */
    private static class CloseCountingCursor implements RecordCursor {
        private RecordCursor base;
        private int closeCount;
        private int openCount;
        private int recordAtCount;

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            base.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            closeCount++;
            base.close();
        }

        @Override
        public void expectLimitedIteration() {
            base.expectLimitedIteration();
        }

        @Override
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return base.hasNext();
        }

        @Override
        public boolean isUsingIndex() {
            return base.isUsingIndex();
        }

        @Override
        public void longTopK(DirectLongLongSortedList list, int columnIndex) {
            base.longTopK(list, columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return base.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordAtCount++;
            base.recordAt(record, atRowId);
        }

        @Override
        public void resumeTimer() {
            base.resumeTimer();
        }

        @Override
        public void setParentUsedColumns(@Nullable IntHashSet columnIndexes) {
            base.setParentUsedColumns(columnIndexes);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            base.setParquetDecodeHint(hint);
        }

        @Override
        public void setRecordAtRows(@Nullable RowIdSource source) {
            base.setRecordAtRows(source);
        }

        @Override
        public long size() {
            return base.size();
        }

        @Override
        public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
            base.skipRows(rowCount, maxRowsAfterSkip);
        }

        @Override
        public void suspendTimer() {
            base.suspendTimer();
        }

        @Override
        public void toTop() {
            base.toTop();
        }

        void of(RecordCursor base) {
            this.base = base;
            openCount++;
        }
    }

    private static class CloseCountingFactory extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        private final CloseCountingCursor cursor = new CloseCountingCursor();

        CloseCountingFactory(RecordCursorFactory base) {
            super(base.getMetadata());
            this.base = base;
        }

        @Override
        public boolean followedOrderByAdvice() {
            return base.followedOrderByAdvice();
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            cursor.of(base.getCursor(executionContext));
            return cursor;
        }

        @Override
        public int getScanDirection() {
            return base.getScanDirection();
        }

        @Override
        public TableToken getTableToken() {
            return base.getTableToken();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }

        @Override
        protected void _close() {
            Misc.free(base);
        }

        void assertCursorsClosedOnce(String query, CharSequence context) {
            Assert.assertEquals(
                    "child cursor close() calls must match its opens: " + query + ", " + context,
                    cursor.openCount,
                    cursor.closeCount
            );
        }

        // Without an open, the close check above passes at 0 == 0, and a join that stopped
        // reading the replaced field would go unchecked.
        void assertCursorsOpened(String query) {
            Assert.assertTrue("the join opened no cursor of the wrapped child factory: " + query, cursor.openCount > 0);
        }

        int getRecordAtCount() {
            return cursor.recordAtCount;
        }
    }
}
