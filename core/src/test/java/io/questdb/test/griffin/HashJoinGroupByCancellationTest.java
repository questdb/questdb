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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

/**
 * Cancels and fails fused hash join GROUP BY executions at the breaker checks and in the build
 * rounds that the lifecycle suites do not reach, and runs each factory again afterwards. A case
 * cancels at a breaker check that a thread makes inside one named method, so a case that stops
 * reaching that method's check fails rather than passing on a cancellation that landed elsewhere.
 * Each run asserts the build that the properties of its case ask for, so a property that did not
 * take effect fails the case too.
 */
public class HashJoinGroupByCancellationTest extends AbstractCairoTest {
    private static final String ATOM = AsyncHashJoinGroupByAtom.class.getName();
    private static final String FACTORY = AsyncHashJoinGroupByRecordCursorFactory.class.getName();
    // Each key shape and the prefix of its probe loops: the INT layout, a key staged into the build's
    // map, and the INT layout's translated SYMBOL key.
    private static final String[][] KEYS = {
            {"r.k = b.k", "aggregateInt"},
            {"r.l = b.l", "aggregateRecord"},
            {"r.s = b.s", "aggregateSymbol"}
    };

    // Workers fill the hash partitions of a parallel build in a round of tasks, one per partition.
    @Test
    public void testCancellationInsideBuildPartitionRoundWithFiberWorkers() throws Exception {
        assertCancellationInsideBuildPartitionRound(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testCancellationInsideBuildPartitionRoundWithLegacyWorkers() throws Exception {
        assertCancellationInsideBuildPartitionRound(WorkerPoolMode.LEGACY);
    }

    // A probe filter sends every frame to the filtered twin of each key shape's loop, which repeats
    // the frame's breaker check once per page frame of joined pairs, as the unfiltered loop does.
    @Test
    public void testCancellationInsideFilteredProbe() throws Exception {
        assertCancellationInsideProbe(null, false, true);
    }

    @Test
    public void testCancellationInsidePartitionedFilteredProbe() throws Exception {
        assertCancellationInsideProbe(null, true, true);
    }

    @Test
    public void testCancellationInsidePartitionedFilteredProbeWithFiberWorkers() throws Exception {
        assertCancellationInsideProbe(WorkerPoolMode.FIBER_HOST, true, true);
    }

    @Test
    public void testCancellationInsidePartitionedFilteredProbeWithLegacyWorkers() throws Exception {
        assertCancellationInsideProbe(WorkerPoolMode.LEGACY, true, true);
    }

    // A parallel build of more than one hash partition sends every frame to the partitioned probe
    // loops, which probe through classes of their own.
    @Test
    public void testCancellationInsidePartitionedProbe() throws Exception {
        assertCancellationInsideProbe(null, true, false);
    }

    @Test
    public void testCancellationInsidePartitionedProbeWithFiberWorkers() throws Exception {
        assertCancellationInsideProbe(WorkerPoolMode.FIBER_HOST, true, false);
    }

    @Test
    public void testCancellationInsidePartitionedProbeWithLegacyWorkers() throws Exception {
        assertCancellationInsideProbe(WorkerPoolMode.LEGACY, true, false);
    }

    // A parallel build copies its payload in a round of tasks, one per build frame.
    @Test
    public void testCancellationInsidePayloadCopyRound() throws Exception {
        assertCancellationInsidePayloadCopyRound(null);
    }

    @Test
    public void testCancellationInsidePayloadCopyRoundWithFiberWorkers() throws Exception {
        assertCancellationInsidePayloadCopyRound(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testCancellationInsidePayloadCopyRoundWithLegacyWorkers() throws Exception {
        assertCancellationInsidePayloadCopyRound(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFailureInsidePartitionedProbe() throws Exception {
        assertFailureInsidePartitionedProbe(null);
    }

    @Test
    public void testFailureInsidePartitionedProbeWithFiberWorkers() throws Exception {
        assertFailureInsidePartitionedProbe(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFailureInsidePartitionedProbeWithLegacyWorkers() throws Exception {
        assertFailureInsidePartitionedProbe(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFailureInsidePayloadCopyRound() throws Exception {
        assertFailureInsidePayloadCopyRound(null);
    }

    @Test
    public void testFailureInsidePayloadCopyRoundWithFiberWorkers() throws Exception {
        assertFailureInsidePayloadCopyRound(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFailureInsidePayloadCopyRoundWithLegacyWorkers() throws Exception {
        assertFailureInsidePayloadCopyRound(WorkerPoolMode.LEGACY);
    }

    // With no workers and a work stealing threshold of zero, the owner reduces each frame that does
    // not fit the full reduce queue itself, through reduceLocally(). Such a frame checks the breaker
    // before the reducer runs, as a queued one does, so the frame whose check finds the query
    // cancelled filters no row. The filter counts the rows that the reducers filtered.
    @Test
    public void testLocalReduceChecksBreakerBeforeEachFrame() throws Exception {
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 0);
        assertMemoryLeak(() -> TestUtils.execute(null, (db, compiler, ignored) -> {
            try (SqlExecutionContextImpl context = context(db, 4)) {
                db.execute("CREATE TABLE t AS (SELECT (x % 7)::INT g, x::DOUBLE v FROM long_sequence(10_000))", context);
                context.changePageFrameSizes(100, 100);
                final String sql = "SELECT g, sum(v) FROM t WHERE test_latched_counter() GROUP BY g ORDER BY g";
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    Assert.assertTrue(plan(factory, context).contains("Async Group By"));
                    final String expected = result(factory, context);
                    // The third frame that the owner reduces locally finds the query cancelled.
                    final MethodCheckCircuitBreaker breaker = new MethodCheckCircuitBreaker(
                            db, UnorderedPageFrameSequence.class.getName(), "reduceLocally", 3, true);
                    final AtomicInteger filteredAtCancel = new AtomicInteger(-1);
                    breaker.onReached = () -> filteredAtCancel.set(TestLatchedCounterFunctionFactory.getCount());
                    TestLatchedCounterFunctionFactory.reset(null);
                    context.with(breaker);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        cursor.hasNext();
                        Assert.fail("expected cancellation inside a local reduce");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isCancellation());
                    }
                    Assert.assertEquals(3, breaker.getChecks());
                    // Two frames of 100 rows ran locally before the cancelled one, and the four
                    // queued frames, which the owner drained afterwards, found the query cancelled.
                    Assert.assertEquals(200, filteredAtCancel.get());
                    Assert.assertEquals(200, TestLatchedCounterFunctionFactory.getCount());
                    context.with(new AtomicBooleanCircuitBreaker(db));
                    Assert.assertEquals(expected, result(factory, context));
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            }
        }, configuration, LOG));
    }

    // A parallel build over Parquet frames decodes them in its frame tasks and in its payload copy
    // round, and charges what it decodes to the query, as the probe does. Raising a per-query limit
    // until the query fits fails it at allocations of the build, of the steps between the build and
    // the probe, and of the probe and the merge, and each failure leaves nothing charged.
    @Test
    public void testParallelBuildMemoryLimitsAtEveryStepOverParquetAndReuse() throws Exception {
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
        assertMemoryLeak(() -> TestUtils.execute(null, (db, compiler, ignored) -> {
            try (
                    SqlExecutionContextImpl context = context(db, 4);
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE)
            ) {
                createTables(db, context);
                db.execute("ALTER TABLE r CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
                db.execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
                context.changePageFrameSizes(100, 100);
                for (String[] key : KEYS) {
                    final String sql = query(key[0], false);
                    final String expected = expectedRows(db, sql, context);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                        final PhaseObservingCircuitBreaker breaker = new PhaseObservingCircuitBreaker(db, atom);
                        context.with(breaker);
                        // The first power of two that fits sizes the sweep's steps, about 300 of them.
                        long fitting = 1024;
                        while (!isFitting(factory, context, tracker, fitting, sql)) {
                            fitting *= 2;
                        }
                        final long step = Math.max(256, fitting / 300);
                        int buildFailures = 0;
                        int copyFailures = 0;
                        int probeFailures = 0;
                        for (long limit = 1; ; limit += step) {
                            breaker.clearPhases();
                            if (isFitting(factory, context, tracker, limit, sql)) {
                                break;
                            }
                            if (!breaker.isBuilt) {
                                buildFailures++;
                            } else if (!breaker.isCopied) {
                                copyFailures++;
                            } else {
                                probeFailures++;
                            }
                        }
                        final String failures = sql + ": build " + buildFailures + ", copy " + copyFailures + ", probe " + probeFailures;
                        Assert.assertTrue(failures, buildFailures > 10);
                        Assert.assertTrue(failures, copyFailures > 0);
                        Assert.assertTrue(failures, probeFailures > 0);
                        context.with(new AtomicBooleanCircuitBreaker(db));
                        Assert.assertEquals(sql, expected, result(factory, context));
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertTrue(sql, atom.isBuiltInRounds());
                            Assert.assertTrue(sql, atom.getBuildPartitionCount() > 1);
                            Assert.assertTrue(sql, atom.isPayloadCopied());
                        }
                    }
                }
            }
        }, configuration, LOG));
    }

    private static void assertCancelled(RecordCursorFactory factory, SqlExecutionContextImpl context, String sql) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            cursor.hasNext();
            Assert.fail("expected cancellation: " + sql);
        } catch (CairoException e) {
            Assert.assertTrue(sql + ": " + e.getMessage(), e.isCancellation());
        }
    }

    private void assertCancellationInsideBuildPartitionRound(WorkerPoolMode mode) throws Exception {
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    createTables(db, context);
                    context.changePageFrameSizes(100, 100);
                    // The INT layout and a key staged into the build's map fill their partitions apart.
                    for (int key = 0; key < 2; key++) {
                        final String sql = query(KEYS[key][0], false);
                        final String expected = expectedRows(db, sql, context);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            Assert.assertTrue(plan(factory, context).contains("Hash Join Group By"));
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            for (int run = 0; run < 2; run++) {
                                final MethodCheckCircuitBreaker breaker = new MethodCheckCircuitBreaker(db, ATOM, "buildPartition", 1, true);
                                context.with(breaker);
                                assertCancelled(factory, context, sql);
                                Assert.assertTrue(sql, breaker.getChecks() > 0);
                                assertReused(db, context, factory, sql, expected);
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    Assert.assertTrue(cursor.hasNext());
                                    Assert.assertTrue(sql, atom.isBuiltInRounds());
                                    Assert.assertTrue(sql, atom.getBuildPartitionCount() > 1);
                                }
                            }
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    private void assertCancellationInsidePayloadCopyRound(@Nullable WorkerPoolMode mode) throws Exception {
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        // Every build copies its payload, so that the copy round runs.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    createTables(db, context);
                    context.changePageFrameSizes(100, 100);
                    // Native build frames, then build frames that the round's tasks decode from Parquet.
                    for (boolean isParquet : new boolean[]{false, true}) {
                        if (isParquet) {
                            db.execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
                        }
                        for (String[] key : KEYS) {
                            final String sql = query(key[0], false);
                            final String expected = expectedRows(db, sql, context);
                            try (RecordCursorFactory factory = db.select(sql, context)) {
                                Assert.assertTrue(plan(factory, context).contains("buildPayload: copied when the probe is large enough"));
                                final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                                for (int run = 0; run < 2; run++) {
                                    final MethodCheckCircuitBreaker breaker = new MethodCheckCircuitBreaker(db, ATOM, "copyFramePayload", 1, true);
                                    context.with(breaker);
                                    assertCancelled(factory, context, sql);
                                    Assert.assertTrue(sql, breaker.getChecks() > 0);
                                    Assert.assertFalse(sql, atom.isPayloadCopied());
                                    assertReused(db, context, factory, sql, expected);
                                    try (RecordCursor cursor = factory.getCursor(context)) {
                                        Assert.assertTrue(cursor.hasNext());
                                        Assert.assertTrue(sql, atom.isBuiltInRounds());
                                        Assert.assertTrue(sql, atom.isPayloadCopied());
                                    }
                                }
                            }
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    private void assertCancellationInsideProbe(@Nullable WorkerPoolMode mode, boolean isParallelBuild, boolean isFiltered) throws Exception {
        (isParallelBuild ? HashJoinBuildMode.PARALLEL : HashJoinBuildMode.SERIAL).applyProperties(node1.getConfigurationOverrides());
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    createTables(db, context);
                    context.changePageFrameSizes(100, 100);
                    for (String[] key : KEYS) {
                        final String sql = query(key[0], isFiltered);
                        final String loop = key[1] + (isParallelBuild ? "Partitioned" : "") + (isFiltered ? "Filtered" : "");
                        final String expected = expectedRows(db, sql, context);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final String plan = plan(factory, context);
                            Assert.assertTrue(plan, plan.contains("Hash Join Group By"));
                            Assert.assertEquals(plan, isFiltered, plan.contains("probeFilter"));
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            for (int run = 0; run < 2; run++) {
                                final MethodCheckCircuitBreaker breaker = new MethodCheckCircuitBreaker(db, FACTORY, loop, 1, true);
                                context.with(breaker);
                                assertCancelled(factory, context, sql);
                                Assert.assertTrue(sql + ", " + loop, breaker.getChecks() > 0);
                                assertReused(db, context, factory, sql, expected);
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    Assert.assertTrue(cursor.hasNext());
                                    Assert.assertEquals(sql, isParallelBuild, atom.isBuiltInRounds());
                                    Assert.assertEquals(sql, isParallelBuild, atom.getBuildPartitionCount() > 1);
                                }
                            }
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    // A post-join filter that throws, rather than a breaker, fails a probe over a partitioned build on
    // whichever thread reduces the frame. The filter runs on every joined pair, and its per-worker
    // copies count the pairs together.
    private void assertFailureInsidePartitionedProbe(@Nullable WorkerPoolMode mode) throws Exception {
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        // Outside dev mode, test_fault() folds to the constant true.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    createTables(db, context);
                    context.changePageFrameSizes(100, 100);
                    try {
                        for (String[] key : KEYS) {
                            final String sql = "SELECT r.g, sum(b.w) w, sum(r.v) v, count() n FROM r JOIN b ON " + key[0]
                                    + " WHERE r.v > 100 AND test_fault() ORDER BY r.g";
                            final String expected = expectedRows(db, sql, context);
                            try (RecordCursorFactory factory = db.select(sql, context)) {
                                final String plan = plan(factory, context);
                                Assert.assertTrue(plan, plan.contains("Hash Join Group By"));
                                Assert.assertTrue(plan, plan.contains("postJoinFilter: test_fault()"));
                                final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                                for (int run = 0; run < 2; run++) {
                                    // The 1,900 probe rows that pass the probe filter make 19,000 pairs.
                                    TestFaultFunctionFactory.armToFailAfter(500 + run * 700);
                                    try (RecordCursor cursor = factory.getCursor(context)) {
                                        cursor.hasNext();
                                        Assert.fail("expected the post-join filter to fail: " + sql);
                                    } catch (CairoException e) {
                                        TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected failure");
                                    } finally {
                                        TestFaultFunctionFactory.disarm();
                                    }
                                    Assert.assertEquals(sql, 1, TestFaultFunctionFactory.faultsTriggered());
                                    assertReused(db, context, factory, sql, expected);
                                    try (RecordCursor cursor = factory.getCursor(context)) {
                                        Assert.assertTrue(cursor.hasNext());
                                        Assert.assertTrue(sql, atom.isBuiltInRounds());
                                        Assert.assertTrue(sql, atom.getBuildPartitionCount() > 1);
                                    }
                                }
                            }
                        }
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                }
            }, configuration, LOG);
        });
    }

    // A Parquet decode error, rather than a breaker or the memory limit, fails a task of the payload
    // copy round. The owner corrupts the pages of the build's Parquet files once the build is frozen,
    // at the breaker check that begins the copy, so that the round's tasks fail to decode the payload
    // column, which no build task decoded. The files are restored before the factory runs again.
    private void assertFailureInsidePayloadCopyRound(@Nullable WorkerPoolMode mode) throws Exception {
        // The case rewrites files that the table's readers map, which Windows does not allow.
        Assume.assumeFalse(Os.isWindows());
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    createTables(db, context);
                    db.execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
                    context.changePageFrameSizes(100, 100);
                    for (String[] key : KEYS) {
                        final String sql = query(key[0], false);
                        final String expected = expectedRows(db, sql, context);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            for (int run = 0; run < 2; run++) {
                                final MethodCheckCircuitBreaker breaker = new MethodCheckCircuitBreaker(
                                        db, HashJoinBuildFrames.class.getName(), "beginCopy", 1, false);
                                final AtomicReference<ObjList<ParquetFile>> originals = new AtomicReference<>();
                                breaker.onReached = () -> {
                                    Assert.assertNotNull(atom.getFrozenBuild());
                                    try {
                                        originals.set(corruptParquetPages(db, "b"));
                                    } catch (IOException e) {
                                        throw new AssertionError(e);
                                    }
                                };
                                context.with(breaker);
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    cursor.hasNext();
                                    Assert.fail("expected the payload copy to fail: " + sql);
                                } catch (CairoException e) {
                                    Assert.assertFalse(sql + ": " + e.getMessage(), e.isCancellation());
                                    Assert.assertFalse(sql + ": " + e.getMessage(), e.isOutOfMemory());
                                    TestUtils.assertContains(e.getFlyweightMessage(), "ParquetPartitionDecoder.decodeRowGroup");
                                } finally {
                                    if (originals.get() != null) {
                                        restore(originals.get());
                                    }
                                }
                                Assert.assertEquals(sql, 1, breaker.getChecks());
                                Assert.assertFalse(sql, atom.isPayloadCopied());
                                assertReused(db, context, factory, sql, expected);
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    Assert.assertTrue(cursor.hasNext());
                                    Assert.assertTrue(sql, atom.isBuiltInRounds());
                                    Assert.assertTrue(sql, atom.isPayloadCopied());
                                }
                            }
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    // Checks what the failed execution left, then runs the factory with a breaker that does not
    // cancel and checks its rows.
    private static void assertReused(
            CairoEngine db,
            SqlExecutionContextImpl context,
            RecordCursorFactory factory,
            String sql,
            String expected
    ) throws Exception {
        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
        Assert.assertEquals(sql, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertNull(sql, atom.getFrozenBuild());
        Assert.assertNull(sql, context.getMemoryTracker());
        context.with(new AtomicBooleanCircuitBreaker(db));
        Assert.assertEquals(sql, expected, result(factory, context));
        Assert.assertEquals(sql, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
    }

    // Overwrites the pages of the table's Parquet files and keeps each footer, so that the next decode
    // of a column chunk fails. Returns the original contents, which restore() writes back in place:
    // the table's open readers map the files and must see the restored bytes.
    private static ObjList<ParquetFile> corruptParquetPages(CairoEngine db, String table) throws IOException {
        final Path tableDir = Paths.get(db.getConfiguration().getDbRoot().toString(), db.verifyTableName(table).getDirName());
        final ObjList<ParquetFile> originals = new ObjList<>();
        try (Stream<Path> files = Files.walk(tableDir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                if (!file.getFileName().toString().equals(TableUtils.PARQUET_PARTITION_NAME)) {
                    continue;
                }
                final byte[] contents = Files.readAllBytes(file);
                // A Parquet file ends with its footer, the footer's size and the magic, and starts with the magic.
                final int footerSize = ByteBuffer.wrap(contents, contents.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
                final byte[] garbage = new byte[contents.length - 8 - footerSize - 4];
                Arrays.fill(garbage, (byte) 0xFF);
                try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    channel.write(ByteBuffer.wrap(garbage), 4);
                }
                originals.add(new ParquetFile(file, contents));
            }
        }
        Assert.assertTrue(originals.size() > 0);
        return originals;
    }

    // The probe and the build, two day partitions each: the build repeats each of 20 keys ten times,
    // so every probe row matches ten build rows. Frames of 100 rows make 1,000 pairs a probe frame,
    // ten times the reducers' pair budget between breaker checks, which is one page frame.
    private static void createTables(CairoEngine db, SqlExecutionContextImpl context) throws Exception {
        db.execute("""
                CREATE TABLE r AS (
                    SELECT (x % 20)::INT k, (x % 20)::LONG l, ('s' || (x % 20))::SYMBOL s, (x % 5)::INT g, x::DOUBLE v,
                        timestamp_sequence('2020-01-01', 60_000_000) ts
                    FROM long_sequence(2_000)
                ) TIMESTAMP(ts) PARTITION BY DAY""", context);
        db.execute("""
                CREATE TABLE b AS (
                    SELECT (x % 20)::INT k, (x % 20)::LONG l, ('s' || (x % 20))::SYMBOL s, x * 0.5 w,
                        timestamp_sequence('2020-01-01', 600_000_000) ts
                    FROM long_sequence(200)
                ) TIMESTAMP(ts) PARTITION BY DAY""", context);
    }

    private static String expectedRows(CairoEngine db, String sql, SqlExecutionContextImpl context) throws Exception {
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = db.select(sql, context)) {
            Assert.assertFalse(plan(baseline, context).contains("Hash Join Group By"));
            return result(baseline, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }

    // Runs the fused operator under the limit and reads every row. Returns false when the limit
    // failed the execution, which must leave nothing charged and no slot taken.
    private static boolean isFitting(
            RecordCursorFactory factory,
            SqlExecutionContextImpl context,
            LimitedMemoryTracker tracker,
            long limit,
            String sql
    ) throws Exception {
        tracker.setLimit(limit);
        context.setMemoryTracker(tracker);
        // The query registry would give a registered execution a tracker of its own, and the sort
        // above the operator charges when it opens, so the sweep runs the operator alone.
        try (RecordCursor cursor = fused(factory).getCursor(context)) {
            // Opening the cursor charges nothing.
            Assert.assertEquals(sql, 0, tracker.getUsed());
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            return true;
        } catch (CairoException e) {
            Assert.assertTrue(sql + ": " + e.getFlyweightMessage(), e.isOutOfMemory());
            return false;
        } finally {
            context.setMemoryTracker(null);
            Assert.assertEquals(sql, 0, tracker.getUsed());
            Assert.assertEquals(sql, 0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        }
    }

    private static String query(String key, boolean isFiltered) {
        return "SELECT r.g, sum(b.w) w, sum(r.v) v, count() n FROM r JOIN b ON " + key
                + (isFiltered ? " WHERE r.v > 100" : "") + " ORDER BY r.g";
    }

    private static void restore(ObjList<ParquetFile> originals) throws IOException {
        for (int i = 0, n = originals.size(); i < n; i++) {
            final ParquetFile original = originals.getQuick(i);
            try (FileChannel channel = FileChannel.open(original.path(), StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(original.contents()), 0);
            }
        }
    }

    /**
     * Counts the breaker checks that threads make inside one method, on any thread, and at the given
     * one runs its hook and, when asked to, cancels the query. A breaker of a class of its own is
     * thread safe, so the workers consult it directly rather than through a copy.
     */
    private static class MethodCheckCircuitBreaker extends AtomicBooleanCircuitBreaker {
        private static final StackWalker STACK_WALKER = StackWalker.getInstance();
        private final AtomicInteger checks = new AtomicInteger();
        private final String className;
        private final boolean isCancelling;
        private final String methodName;
        private final int reachedCheck;
        // Runs at the given check, before the breaker cancels the query there.
        private volatile Runnable onReached;

        private MethodCheckCircuitBreaker(CairoEngine engine, String className, String methodName, int reachedCheck, boolean isCancelling) {
            super(engine);
            this.className = className;
            this.methodName = methodName;
            this.reachedCheck = reachedCheck;
            this.isCancelling = isCancelling;
        }

        @Override
        public boolean checkIfTripped() {
            onCheck();
            return super.checkIfTripped();
        }

        public int getChecks() {
            return checks.get();
        }

        @Override
        public int getState() {
            onCheck();
            return super.getState();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            onCheck();
            super.statefulThrowExceptionIfTrippedNoThrottle();
        }

        private boolean isInside() {
            return STACK_WALKER.walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(className)
                    && frame.getMethodName().equals(methodName)));
        }

        private void onCheck() {
            if (isInside() && checks.incrementAndGet() == reachedCheck) {
                final Runnable hook = onReached;
                if (hook != null) {
                    hook.run();
                }
                if (isCancelling) {
                    cancel();
                }
            }
        }
    }

    private record ParquetFile(Path path, byte[] contents) {
    }

    /**
     * Tells, on the query's owner thread, how far an execution got: whether a check found the build
     * frozen, and whether one found the payload copied. With no workers, the owner makes every check,
     * in the probe too, so the last check before a failure places it.
     */
    private static class PhaseObservingCircuitBreaker extends AtomicBooleanCircuitBreaker {
        private final AsyncHashJoinGroupByAtom atom;
        private final Thread owner = Thread.currentThread();
        private boolean isBuilt;
        private boolean isCopied;

        private PhaseObservingCircuitBreaker(CairoEngine engine, AsyncHashJoinGroupByAtom atom) {
            super(engine);
            this.atom = atom;
        }

        @Override
        public boolean checkIfTripped() {
            observe();
            return super.checkIfTripped();
        }

        @Override
        public int getState() {
            observe();
            return super.getState();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            observe();
            super.statefulThrowExceptionIfTrippedNoThrottle();
        }

        private void clearPhases() {
            isBuilt = false;
            isCopied = false;
        }

        private void observe() {
            if (Thread.currentThread() == owner) {
                isBuilt |= atom.getFrozenBuild() != null;
                isCopied |= atom.isPayloadCopied();
            }
        }
    }
}
