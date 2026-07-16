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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinResources;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AsyncFilterContextTest extends AbstractCairoTest {

    @Test
    public void testCloseAttemptsEveryOwnedFilterAndPreservesFailureOrder() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException compiledFailure = new RuntimeException("compiled");
            final RuntimeException bindFailure = new RuntimeException("bind");
            final RuntimeException ownerFailure = new RuntimeException("owner");
            final RuntimeException worker0Failure = new RuntimeException("worker 0");
            final RuntimeException worker2Failure = new RuntimeException("worker 2");
            final ThrowingCompiledFilter compiledFilter = new ThrowingCompiledFilter(compiledFailure);
            final TrackingFunction bindFunction = new TrackingFunction(bindFailure);
            final TrackingFunction ownerFilter = new TrackingFunction(ownerFailure);
            final TrackingFunction worker0 = new TrackingFunction(worker0Failure);
            final TrackingFunction worker1 = new TrackingFunction(null);
            final TrackingFunction worker2 = new TrackingFunction(worker2Failure);
            final ObjList<Function> bindFunctions = list(bindFunction);
            final ObjList<Function> workerFilters = list(worker0, worker1, worker2);
            final AsyncFilterContext context = new AsyncFilterContext(
                    configuration,
                    compiledFilter,
                    null,
                    bindFunctions,
                    ownerFilter,
                    null,
                    workerFilters,
                    3,
                    0,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE
            );

            try {
                context.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(compiledFailure, e);
                Assert.assertArrayEquals(
                        new Throwable[]{bindFailure, ownerFailure, worker0Failure, worker2Failure},
                        e.getSuppressed()
                );
            }
            Assert.assertEquals(1, compiledFilter.closeCount);
            Assert.assertEquals(1, bindFunction.closeCount);
            Assert.assertEquals(1, ownerFilter.closeCount);
            Assert.assertEquals(1, worker0.closeCount);
            Assert.assertEquals(1, worker1.closeCount);
            Assert.assertEquals(1, worker2.closeCount);
            Assert.assertNull(bindFunctions.getQuick(0));
            Assert.assertNull(workerFilters.getQuick(0));
            Assert.assertNull(workerFilters.getQuick(1));
            Assert.assertNull(workerFilters.getQuick(2));
        });
    }

    @Test
    public void testCloseDoesNotSelfSuppress() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException sharedFailure = new RuntimeException("shared");
            final ThrowingCompiledFilter compiledFilter = new ThrowingCompiledFilter(sharedFailure);
            final TrackingFunction ownerFilter = new TrackingFunction(sharedFailure);
            final AsyncFilterContext context = new AsyncFilterContext(
                    configuration,
                    compiledFilter,
                    null,
                    null,
                    ownerFilter,
                    null,
                    null,
                    0,
                    0,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE
            );

            try {
                context.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(sharedFailure, e);
                Assert.assertEquals(0, e.getSuppressed().length);
            }
            Assert.assertEquals(1, compiledFilter.closeCount);
            Assert.assertEquals(1, ownerFilter.closeCount);
        });
    }

    @Test
    public void testConstructorFailureRollsBackAdoptedOwners() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException constructionFailure = new RuntimeException("injected construction failure");
            final RuntimeException compiledFailure = new RuntimeException("compiled cleanup");
            final RuntimeException bindFailure = new RuntimeException("bind cleanup");
            final RuntimeException ownerFailure = new RuntimeException("owner cleanup");
            final RuntimeException workerFailure = new RuntimeException("worker cleanup");
            final ThrowingCompiledFilter compiledFilter = new ThrowingCompiledFilter(compiledFailure);
            final TrackingFunction bindFunction = new TrackingFunction(bindFailure);
            final TrackingFunction ownerFilter = new TrackingFunction(ownerFailure);
            final TrackingFunction worker0 = new TrackingFunction(workerFailure);
            final TrackingFunction worker1 = new TrackingFunction(null);
            final ObjList<Function> bindFunctions = list(bindFunction);
            final ObjList<Function> workerFilters = list(worker0, worker1);
            final CairoConfigurationWrapper throwingConfiguration = new CairoConfigurationWrapper(configuration) {
                private int rowCapacityCallCount;

                @Override
                public int getPageFrameReduceRowIdListCapacity() {
                    if (++rowCapacityCallCount == 2) {
                        throw constructionFailure;
                    }
                    return super.getPageFrameReduceRowIdListCapacity();
                }
            };

            try {
                new AsyncFilterContext(
                        throwingConfiguration,
                        compiledFilter,
                        null,
                        bindFunctions,
                        ownerFilter,
                        null,
                        workerFilters,
                        2,
                        0,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE
                );
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(constructionFailure, e);
                Assert.assertArrayEquals(new Throwable[]{compiledFailure}, e.getSuppressed());
                Assert.assertArrayEquals(
                        new Throwable[]{bindFailure, ownerFailure, workerFailure},
                        compiledFailure.getSuppressed()
                );
            }
            Assert.assertEquals(1, compiledFilter.closeCount);
            Assert.assertEquals(1, bindFunction.closeCount);
            Assert.assertEquals(1, ownerFilter.closeCount);
            Assert.assertEquals(1, worker0.closeCount);
            Assert.assertEquals(1, worker1.closeCount);
            Assert.assertNull(bindFunctions.getQuick(0));
            Assert.assertNull(workerFilters.getQuick(0));
            Assert.assertNull(workerFilters.getQuick(1));
        });
    }

    @Test
    public void testHorizonResourcesContinueAcrossOuterWorkerLists() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException worker00Failure = new RuntimeException("worker 0 function 0");
            final RuntimeException worker01Failure = new RuntimeException("worker 0 function 1");
            final RuntimeException worker10Failure = new RuntimeException("worker 1 function 0");
            final TrackingFunction worker00 = new TrackingFunction(worker00Failure);
            final TrackingFunction worker01 = new TrackingFunction(worker01Failure);
            final TrackingFunction worker10 = new TrackingFunction(worker10Failure);
            final TrackingFunction worker11 = new TrackingFunction(null);
            final PerWorkerFunctionList<Function> worker0 = new PerWorkerFunctionList<>(2);
            worker0.add(worker00, true);
            worker0.add(worker01, true);
            final PerWorkerFunctionList<Function> worker1 = new PerWorkerFunctionList<>(2);
            worker1.add(worker10, true);
            worker1.add(worker11, true);
            final ObjList<ObjList<Function>> workers = new ObjList<>();
            workers.add(worker0);
            workers.add(worker1);
            final AsyncHorizonJoinResources resources = new AsyncHorizonJoinResources(
                    null,
                    workers,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
            );

            try {
                resources.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(worker00Failure, e);
                Assert.assertArrayEquals(new Throwable[]{worker01Failure, worker10Failure}, e.getSuppressed());
            }
            resources.close();
            Assert.assertEquals(1, worker00.closeCount);
            Assert.assertEquals(1, worker01.closeCount);
            Assert.assertEquals(1, worker10.closeCount);
            Assert.assertEquals(1, worker11.closeCount);
            Assert.assertNull(workers.getQuick(0));
            Assert.assertNull(workers.getQuick(1));
        });
    }

    @Override
    @Before
    public void setUp() {
        // testClearShrinksRowIdListsThroughCursorTeardown runs a parallel JIT group by;
        // force the parallel path and a full 1M-row page frame so a single filter list
        // reaches ~8 MB. Harmless for the direct-construction tests.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        super.setUp();
    }

    @Test
    public void testClearAfterCloseDoesNotResurrectLists() throws Exception {
        // On the horizon-join error path the factory frees frameSequence (-> filterCtx
        // .close()) before the half-open cursor's close() re-runs reset() -> clear(), so
        // clear() must be safe after close(). A closed list has capacity 0, so an unguarded
        // resetCapacity() would re-malloc it into a leak that nothing frees.
        assertMemoryLeak(() -> {
            final long memBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
            AsyncFilterContext ctx = new AsyncFilterContext(
                    configuration,
                    new CompiledFilter(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    4,
                    0,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE
            );
            // Grow a list so close() has something sizeable to free.
            ctx.getFilteredRows(-1).setCapacity(1_000_000);
            ctx.close();

            final long memAfterClose = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
            Assert.assertEquals("close() should release all NATIVE_OFFLOAD", memBefore, memAfterClose);

            // clear() after close() must not re-allocate the freed lists.
            ctx.clear();

            final long memAfterClear = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
            Assert.assertEquals(
                    "clear() after close() must not resurrect freed lists, leaked "
                            + (memAfterClear - memAfterClose) + " bytes",
                    memAfterClose,
                    memAfterClear
            );
        });
    }

    @Test
    public void testClearShrinksGrownColumnAddressLists() throws Exception {
        // Under a JIT filter the context also allocates per-worker column-address lists
        // (data and aux). clear() must shrink those back to their initial capacity too,
        // alongside the row-id lists.
        //
        // In production the column-address lists hold one entry per scanned column
        // (populateJitAddresses add()s once per column), so they only ever reach
        // columnCount entries - tens of longs, not a frame's worth. Only the row-id
        // list reaches ~8 MB. This test inflates all of them to the same large size
        // purely to make the shrink observable in the NATIVE_OFFLOAD delta; it is not a
        // claim that the column-address lists are a major consumer.
        assertMemoryLeak(() -> {
            final int slotCount = 4;
            final long rowIdInitialCapacity = configuration.getPageFrameReduceRowIdListCapacity();
            final long columnInitialCapacity = configuration.getPageFrameReduceColumnListCapacity();
            final long grownCapacity = 100_000;

            // A non-null compiled filter makes the constructor allocate the owner plus
            // per-worker data/aux address lists. The function is never compiled, so its
            // close() is a no-op.
            AsyncFilterContext ctx = new AsyncFilterContext(
                    configuration,
                    new CompiledFilter(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    slotCount,
                    0,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE
            );
            try {
                final long memAtInitial = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);

                // Grow the owner plus every per-worker row-id, data, and aux list.
                ctx.getFilteredRows(-1).setCapacity(grownCapacity);
                ctx.getDataAddresses(-1).setCapacity(grownCapacity);
                ctx.getAuxAddresses(-1).setCapacity(grownCapacity);
                for (int i = 0; i < slotCount; i++) {
                    ctx.getFilteredRows(i).setCapacity(grownCapacity);
                    ctx.getDataAddresses(i).setCapacity(grownCapacity);
                    ctx.getAuxAddresses(i).setCapacity(grownCapacity);
                }

                final long memGrown = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                final long expectedGrowthBytes = ((1 + slotCount) * (grownCapacity - rowIdInitialCapacity)
                        + 2 * (1 + slotCount) * (grownCapacity - columnInitialCapacity)) * Long.BYTES;
                Assert.assertTrue(
                        "row-id and column lists should have grown by ~" + expectedGrowthBytes + " bytes, grew by " + (memGrown - memAtInitial),
                        memGrown - memAtInitial >= expectedGrowthBytes
                );

                // The fix: clear() shrinks every list back to its initial capacity.
                ctx.clear();

                Assert.assertEquals(rowIdInitialCapacity, ctx.getFilteredRows(-1).getCapacity());
                Assert.assertEquals(columnInitialCapacity, ctx.getDataAddresses(-1).getCapacity());
                Assert.assertEquals(columnInitialCapacity, ctx.getAuxAddresses(-1).getCapacity());
                for (int i = 0; i < slotCount; i++) {
                    Assert.assertEquals(rowIdInitialCapacity, ctx.getFilteredRows(i).getCapacity());
                    Assert.assertEquals(columnInitialCapacity, ctx.getDataAddresses(i).getCapacity());
                    Assert.assertEquals(columnInitialCapacity, ctx.getAuxAddresses(i).getCapacity());
                }

                final long memCleared = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                Assert.assertTrue(
                        "clear() should release the grown memory, still holding " + (memCleared - memAtInitial) + " extra bytes",
                        memCleared <= memAtInitial
                );
            } finally {
                Misc.free(ctx);
            }
        });
    }

    @Test
    public void testClearShrinksGrownRowIdLists() throws Exception {
        // A parallel GROUP BY/TOP K/join keeps its AsyncFilterContext alive while the
        // factory is cached or idle. clear() must hand back the row-id lists, which a
        // JIT filter grows to a full page frame (up to cairo.sql.page.frame.max.rows
        // longs = 8 MB each), otherwise an idle factory pins peak-sized NATIVE_OFFLOAD
        // buffers until eviction.
        assertMemoryLeak(() -> {
            final int slotCount = 4;
            final long initialCapacity = configuration.getPageFrameReduceRowIdListCapacity();
            final long grownCapacity = 1_000_000; // a full default page frame: 8 MB per list

            AsyncFilterContext ctx = new AsyncFilterContext(
                    configuration,
                    null, // no compiled filter -> only the row-id lists are allocated
                    null,
                    null,
                    null,
                    null,
                    null,
                    slotCount,
                    0,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE
            );
            try {
                final long memAtInitial = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);

                // Grow the owner plus every per-worker row-id list to a full frame.
                ctx.getFilteredRows(-1).setCapacity(grownCapacity);
                for (int i = 0; i < slotCount; i++) {
                    ctx.getFilteredRows(i).setCapacity(grownCapacity);
                }

                final long memGrown = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                final long expectedGrowthBytes = (1 + slotCount) * (grownCapacity - initialCapacity) * Long.BYTES;
                Assert.assertTrue(
                        "row-id lists should have grown by ~" + expectedGrowthBytes + " bytes, grew by " + (memGrown - memAtInitial),
                        memGrown - memAtInitial >= expectedGrowthBytes
                );

                // The fix: clear() shrinks the lists back to their initial capacity.
                ctx.clear();

                Assert.assertEquals(initialCapacity, ctx.getFilteredRows(-1).getCapacity());
                for (int i = 0; i < slotCount; i++) {
                    Assert.assertEquals(initialCapacity, ctx.getFilteredRows(i).getCapacity());
                }

                final long memCleared = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                Assert.assertTrue(
                        "clear() should release the grown row-id memory, still holding " + (memCleared - memAtInitial) + " extra bytes",
                        memCleared <= memAtInitial
                );
            } finally {
                Misc.free(ctx);
            }
        });
    }

    @Test
    public void testClearShrinksRowIdListsThroughCursorTeardown() throws Exception {
        // End-to-end guard for the production teardown path. A JIT-filtered parallel
        // GROUP BY grows the per-slot row-id lists to a full frame during the scan;
        // closing the cursor while the factory stays cached must run
        // PageFrameSequence.reset() -> atom.clear() -> AsyncFilterContext.clear() and hand
        // the peak buffers back. NATIVE_OFFLOAD is excluded from the per-factory leak
        // check (AbstractCairoTest), so this assertion is the only cover for the wiring.
        final long frameRows = 1_000_000;
        final long thresholdBytes = frameRows * Long.BYTES / 4; // a quarter of a grown frame list
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                        engine.execute(
                                "CREATE TABLE tab AS (" +
                                        "SELECT (x % 1000)::int key, x v FROM long_sequence(" + (2 * frameRows) + ")" +
                                        ")",
                                sqlExecutionContext
                        );
                        // WHERE v > 0 matches every row, so filteredRows grows to the full
                        // frame on both the JIT (setCapacity) and the scalar (add) reduce paths.
                        try (RecordCursorFactory factory = compiler.compile(
                                "SELECT key, count() FROM tab WHERE v > 0",
                                sqlExecutionContext
                        ).getRecordCursorFactory()) {
                            // The compiler wraps the base factory in QueryProgress; unwrap it.
                            final RecordCursorFactory base = factory.getBaseFactory();
                            Assert.assertTrue(
                                    "expected the parallel async group by path, got " + base.getClass().getName(),
                                    base instanceof AsyncGroupByRecordCursorFactory
                            );

                            // Baseline after compile: the filter and initial-capacity lists are
                            // already allocated, but no frame has been reduced yet.
                            final long memBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);

                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                final Record record = cursor.getRecord();
                                long rowCount = 0;
                                while (cursor.hasNext()) {
                                    record.getInt(0);
                                    rowCount++;
                                }
                                Assert.assertEquals(1000, rowCount);
                                Assert.assertTrue(
                                        "row-id lists should have grown well past baseline during the scan",
                                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD) - memBaseline >= thresholdBytes
                                );
                            }

                            // The fix: cursor teardown shrank the buffers back. Without it a full
                            // frame (>= 8 MB) would still be pinned on the cached factory here.
                            final long residual = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD) - memBaseline;
                            Assert.assertTrue(
                                    "cursor teardown should release the grown row-id memory, still holding " + residual + " bytes",
                                    residual < thresholdBytes
                            );
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @SafeVarargs
    private static <T> ObjList<T> list(T... values) {
        final ObjList<T> list = new ObjList<>(values.length);
        for (int i = 0; i < values.length; i++) {
            list.add(values[i]);
        }
        return list;
    }

    private static class ThrowingCompiledFilter extends CompiledFilter {
        private final RuntimeException failure;
        private int closeCount;

        private ThrowingCompiledFilter(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void close() {
            closeCount++;
            throw failure;
        }
    }

    private static class TrackingFunction extends BooleanFunction {
        private final RuntimeException failure;
        private int closeCount;

        private TrackingFunction(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void close() {
            closeCount++;
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public boolean getBool(Record rec) {
            return true;
        }
    }
}
