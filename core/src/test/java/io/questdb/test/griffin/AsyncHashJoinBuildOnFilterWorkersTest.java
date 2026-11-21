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
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.MultiHashJoinRecord;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.Rows;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join's build-only ON pushdown on shared query workers that run on threads and on
 * fibers: the leading conjuncts of a LEFT join's ON residual that read the build alone run over the
 * build's frames in the tasks of the build's rounds, and in the round that builds small stages at
 * once, each worker on a copy of its own when they are not thread safe; a build whose conjuncts throw
 * on a row on any worker gives them up and leaves the residual to the pairs. An execution whose build
 * ran the whole residual hands the stage's chains to the consumer, and any other execution of the
 * same factory walks the stage's matches on the workers, also right after an execution whose workers
 * still walked them as the consumer closed. Every query compares with the ordinary plan, in order.
 * {@link SlotGatedWorkStealingStrategy} holds the owner off until a worker took a slot of the build's
 * rounds, so that the workers run the pushed conjuncts.
 */
public class AsyncHashJoinBuildOnFilterWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinBuildOnFilterWorkersTest.class);
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        super.setUp();
    }

    @Test
    public void testPushdownOnFiberWorkers() throws Exception {
        assertPushdown(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testPushdownOnThreadWorkers() throws Exception {
        assertPushdown(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testWholePushedResidualOnFiberWorkers() throws Exception {
        assertWholePushedResidual(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testWholePushedResidualOnThreadWorkers() throws Exception {
        assertWholePushedResidual(WorkerPoolMode.LEGACY);
    }

    // Executes the factory, whose first stage is a LEFT join of t under an ON residual that its build
    // may run whole, and asserts that the build ran it, or not, that the rows equal the expected ones,
    // in order, and that the stage handed its chains to the consumer, so that no frame continued past
    // the cap of one tuple, or walked its matches on the workers, past the cap; and that every slot
    // came back.
    private static void assertExecution(RecordCursorFactory factory, SqlExecutionContext context, String sql, String expected, boolean isHandedOn) throws SqlException {
        final AsyncHashJoinRecordCursorFactory join = find(factory);
        Assert.assertNotNull(sql, join);
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertTrue(sql, cursor.hasNext());
            Assert.assertEquals(sql, isHandedOn, join.getAtom().getStage(0).isBuildOnFilterApplied());
        }
        Assert.assertEquals(sql, expected, result(factory, context));
        Assert.assertEquals(sql, isHandedOn, join.getAtom().getContinuationCount() == 0);
        Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(sql, 0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
    }

    private static void assertPushdown(WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, bindVariableService, configuration, LOG, (db, context) -> {
            createTables(db, context);
            context.changePageFrameSizes(256, 256);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 128);
            final String[] queries = {
                    // A regex, which is not thread safe, so each worker runs a copy of its own, then the
                    // rest of the residual over the pairs.
                    "SELECT t.k, t.v, d.w, d.vch FROM t LEFT JOIN d ON t.k = d.k AND d.vch ~ 'v1[0-9]' AND d.w > t.v % 1_000",
                    // Two LEFT stages, each with its pushdown, which build at once when both are small.
                    "SELECT t.k, t.v, d.w, e.w ew FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 = 0"
                            + " LEFT JOIN d e ON t.s = e.s AND e.vch ~ 'v2[0-9]' AND e.w < t.v",
                    // The row of key -1 overflows the cast on whichever worker meets it, and t holds no
                    // such key: the build gives the conjunct up, and the pairs run the residual.
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND (d.w * 10)::DECIMAL(5,0) > 0::DECIMAL(5,0) AND d.w > t.v % 500",
            };
            for (long minRows : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (String sql : queries) {
                    final String expected = ordinary(sql, context);
                    context.setParallelHashJoinEnabled(true);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        TestUtils.assertContains(plan(factory, context), "buildOnFilter: ");
                        final boolean isAbandoned = sql.contains("DECIMAL");
                        final boolean isRoundBuilt = minRows == 0 || join.getSpec().getStageCount() > 1;
                        // The owner waits until a worker took a slot of the build's rounds, and so of
                        // the round that builds the small stages at once: the workers run the
                        // pushed conjuncts.
                        final CountDownLatch buildSlot = new CountDownLatch(1);
                        if (isRoundBuilt) {
                            join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(buildSlot);
                        }
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(sql, cursor.hasNext());
                            for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
                                Assert.assertEquals(sql, !isAbandoned, join.getAtom().getStage(k).isBuildOnFilterApplied());
                            }
                        } finally {
                            join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                        }
                        Assert.assertEquals(sql, isRoundBuilt ? 0 : 1, buildSlot.getCount());
                        Assert.assertEquals(sql, 0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(sql, expected, result(factory, context));
                        Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                }
            }
        }));
    }

    private static void assertWholePushedResidual(WorkerPoolMode mode) throws Exception {
        // A cap of one tuple, which a task lifts to its frame's rows.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        // A window wider than the pool, so that the workers take frames past the first while the
        // owner holds it: the default dispatch limit follows the host's query workers, two on a
        // two-core host, and the owner may join both frames of such a window itself.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4 * WORKER_COUNT);
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, bindVariableService, configuration, LOG, (db, context) -> {
            createTables(db, context);
            context.changePageFrameSizes(256, 256);
            // Residuals of d alone, a comparison and a regex, which is not thread safe, so that each
            // worker runs a copy of its own. d holds fewer rows than t, so the build runs either
            // whole, and the stage hands the chains of the rows it kept to the consumer.
            for (String sql : new String[]{
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 = 0",
                    "SELECT t.k, t.v, d.w, d.vch FROM t LEFT JOIN d ON t.k = d.k AND d.vch ~ 'v1[0-9]'",
            }) {
                final String expected = ordinary(sql, context);
                context.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    assertExecution(factory, context, sql, expected, true);
                }
            }

            // The size rule over one factory: b holds no more rows than p, then more, then, after p
            // grew, fewer again. The build runs the regex in the first and the last execution,
            // which hand b's chains on, and the pairs run it in the second, which walks b's matches.
            db.execute("CREATE TABLE p AS (SELECT (x % 100)::INT k, x v, timestamp_sequence('2024-01-01', 10_000_000) ts FROM long_sequence(2_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
            db.execute("CREATE TABLE b AS (SELECT (x % 100)::INT k, x w, ('v' || x)::VARCHAR vch FROM long_sequence(1_000))", context);
            final String sized = "SELECT p.k, p.v, b.w FROM p LEFT JOIN b ON p.k = b.k AND b.vch ~ '[1-5]$'";
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sized, context)) {
                TestUtils.assertContains(plan(factory, context), "buildOnFilterPushdown: ");
                final long[] buildInserts = {0, 1_500, 0};
                final long[] probeInserts = {0, 0, 1_000};
                long bRows = 1_000;
                long pRows = 2_000;
                for (int i = 0; i < buildInserts.length; i++) {
                    if (buildInserts[i] > 0) {
                        db.execute("INSERT INTO b SELECT (x % 100)::INT, x + " + bRows + ", ('v' || (x + " + bRows + "))::VARCHAR FROM long_sequence(" + buildInserts[i] + ")", context);
                        bRows += buildInserts[i];
                    }
                    if (probeInserts[i] > 0) {
                        db.execute("INSERT INTO p SELECT (x % 100)::INT, x + " + pRows + ", timestamp_sequence('2024-01-02', 10_000_000) FROM long_sequence(" + probeInserts[i] + ")", context);
                        pRows += probeInserts[i];
                    }
                    final String expected = ordinary(sized, context);
                    assertExecution(factory, context, sized, expected, bRows <= pRows);
                }
            }

            // Execution N gives the pushdown up, as test_latched_counter() fails on d's rows, and the
            // workers walk d's matches under the whole residual: a worker holds a frame past the
            // first inside its walk, until the consumer, which read one row, closes its cursor and
            // waits for the frames in flight. Execution N + 1 then runs the pushdown and hands d's
            // chains on.
            final String latched = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND test_latched_counter() AND d.w % 3 = 0";
            final Thread owner = Thread.currentThread();
            final AtomicBoolean isFailing = new AtomicBoolean(true);
            final AtomicBoolean isClosing = new AtomicBoolean();
            final AtomicInteger heldWorkers = new AtomicInteger();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (!isFailing.get()) {
                        return true;
                    }
                    if (rec instanceof PageFrameMemoryRecord) {
                        throw CairoException.nonCritical().put("test value failure");
                    }
                    if (rec instanceof MultiHashJoinRecord && Thread.currentThread() != owner
                            && Rows.toPartitionIndex(rec.getRowId()) > 0 && !isClosing.get()) {
                        heldWorkers.incrementAndGet();
                        while (!isClosing.get()) {
                            if (isAwaitingFrames(owner)) {
                                isClosing.set(true);
                            } else {
                                Thread.onSpinWait();
                            }
                        }
                    }
                    return true;
                }
            });
            try {
                final String expected = ordinary(latched, context);
                context.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = db.select(latched, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    TestUtils.assertContains(plan(factory, context), "buildOnFilter: (test_latched_counter() and w%3=0)");
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertFalse(join.getAtom().getStage(0).isBuildOnFilterApplied());
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                        while (heldWorkers.get() == 0) {
                            Assert.assertTrue("no worker walked a frame past the first", System.nanoTime() < deadline);
                            Thread.onSpinWait();
                        }
                    }
                    Assert.assertTrue(isClosing.get());
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    isFailing.set(false);
                    assertExecution(factory, context, latched, expected, true);
                }
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        }));
    }

    private static void createTables(CairoEngine db, SqlExecutionContextImpl context) throws Exception {
        db.execute("""
                CREATE TABLE t AS (
                    SELECT
                        (x % 500)::INT k,
                        x v,
                        ('s' || (x % 40))::SYMBOL s,
                        timestamp_sequence('2024-01-01', 10_000_000) ts
                    FROM long_sequence(20_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """, context);
        // d inserts its symbols in another order than t, so equal texts have different keys, and holds
        // one row of key -1, whose w the cast overflows, between rows that pass.
        db.execute("""
                CREATE TABLE d AS (
                    SELECT
                        CASE WHEN x = 1_777 THEN -1 ELSE (x % 450)::INT END k,
                        CASE WHEN x = 1_777 THEN 100_000 ELSE x END w,
                        ('s' || (34 - x % 35))::SYMBOL s,
                        CASE WHEN x % 4 = 0 THEN NULL ELSE 'v' || x END::VARCHAR vch
                    FROM long_sequence(3_000)
                )
                """, context);
    }

    // Whether the thread waits, inside a cursor's close(), for the frames that workers still reduce.
    private static boolean isAwaitingFrames(Thread thread) {
        for (StackTraceElement frame : thread.getStackTrace()) {
            if (frame.getClassName().equals(PageFrameSequence.class.getName()) && frame.getMethodName().equals("await")) {
                return true;
            }
        }
        return false;
    }
}
