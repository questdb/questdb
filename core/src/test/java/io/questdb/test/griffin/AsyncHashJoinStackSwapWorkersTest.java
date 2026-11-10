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
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Stacks of the parallel hash join whose first join swaps its inputs, on shared query workers that
 * run on threads and on fibers: the left input, the smaller, builds on the owner or in rounds, and
 * the workers join the right input's frames, walk the first stage's matches and look each later
 * stage up with the key that the slot's own payload reader of the first stage reads, through the
 * slot's own key sinks, translating records and probes of the swapped key layouts. Every query
 * compares with the ordinary plan, in order. {@link SlotGatedWorkStealingStrategy} holds the owner
 * off until a worker took a slot, so that the checks that every slot is back after a query, a failed
 * one included, count slots that workers really took.
 */
public class AsyncHashJoinStackSwapWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinStackSwapWorkersTest.class);
    private static final String[] QUERIES = {
            // A later INT key, a translated SYMBOL key and a staged key of a LONG and a translated
            // SYMBOL, read off d's columns, which the swapped first stage holds; the later stages
            // hand their chains to the consumer.
            "SELECT d.k, d.w, t.v, e.ew, g.gw, h.gw FROM d JOIN t ON d.k = t.k LEFT JOIN e ON d.s = e.s"
                    + " JOIN g ON d.l = g.l LEFT JOIN g h ON d.l = h.l AND d.s = h.gs",
            // An ON residual on the second stage, a regex, which is not thread safe, so that every
            // worker runs a copy of its own, and a post-join filter on the last one.
            "SELECT d.k, d.w, t.v, e.ew, g.gw FROM d JOIN t ON d.k = t.k LEFT JOIN e ON d.s = e.s AND e.es ~ 'e[1-3]'"
                    + " LEFT JOIN g ON d.l = g.l WHERE g.gw IS NULL OR g.gw % 7 <> t.v % 7",
    };
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        super.setUp();
    }

    @Test
    public void testMemoryLimitsReleaseSlotsOnFiberWorkers() throws Exception {
        assertMemoryLimitsReleaseSlots(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testMemoryLimitsReleaseSlotsOnThreadWorkers() throws Exception {
        assertMemoryLimitsReleaseSlots(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testSwappedStacksOnFiberWorkers() throws Exception {
        assertSwappedStacks(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testSwappedStacksOnThreadWorkers() throws Exception {
        assertSwappedStacks(WorkerPoolMode.LEGACY);
    }

    private static void assertMemoryLimitsReleaseSlots(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            // Every stage builds in rounds, so that the limits fail the builds on the workers and the
            // owner, the translations, the probes and the tuples in turn.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 64);
            final String sql = QUERIES[0];
            final String expected = ordinary(sql, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                int failures = 0;
                for (long limit = 4; ; limit *= 2) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try {
                        Assert.assertEquals(expected, result(factory, context));
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        failures++;
                    }
                    Assert.assertTrue(join.getAtom().isSwapped());
                    Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    for (int k = 0; k < 4; k++) {
                        Assert.assertNull(join.getAtom().getStage(k).getFrozenBuild());
                    }
                }
                Assert.assertTrue(failures > 1);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                Assert.assertEquals(expected, result(factory, context));
            }
        });
    }

    private static void assertSwappedStacks(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 256);
            for (long minRows : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (int cap : new int[]{1, 1_000_000}) {
                    // A cap of one tuple leaves every frame's rest to the consumer's continuations.
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, cap);
                    for (String sql : QUERIES) {
                        final String expected = ordinary(sql, context);
                        context.setParallelHashJoinEnabled(true);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(sql, join);
                            final int stageCount = join.getSpec().getStageCount();
                            Assert.assertEquals(sql, sql.contains(" h ") ? 4 : 3, stageCount);
                            TestUtils.assertContains(plan(factory, context), "inputSwap: when the left input is smaller");
                            // The owner waits until a worker took a slot of each atom: one that builds
                            // in rounds runs round tasks on the workers, and every query runs probe
                            // tasks there.
                            final CountDownLatch roundSlot = minRows == 0 ? new CountDownLatch(1) : null;
                            final CountDownLatch probeSlot = new CountDownLatch(1);
                            join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(roundSlot);
                            join.getAtom().getPerWorkerLocks().setTestAcquireLatch(probeSlot);
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertTrue(sql, cursor.hasNext());
                                Assert.assertTrue(sql, join.getAtom().isSwapped());
                                // The first stage hashes d.
                                Assert.assertEquals(sql, 3_000, join.getAtom().getStage(0).getFrozenBuild().getRowCount());
                                for (int k = 0; k < stageCount; k++) {
                                    Assert.assertEquals(sql, minRows == 0, join.getAtom().getStage(k).isBuiltInRounds());
                                }
                            }
                            Assert.assertEquals(sql, expected, result(factory, context));
                            if (roundSlot != null) {
                                Assert.assertEquals(sql, 0, roundSlot.getCount());
                            }
                            Assert.assertEquals(sql, 0, probeSlot.getCount());
                            Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertEquals(sql, 0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                            join.getAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                        }
                    }
                }
            }
        });
    }

    private static void execute(WorkerPoolMode mode, Code code) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            // Fiber workers run the queue's tasks through the fiber dispatcher, thread workers run
            // them on their own threads.
            Assert.assertEquals(mode == WorkerPoolMode.FIBER_HOST, db.getMessageBus().getPageFrameReduceDispatcher() != null);
            try (
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT)
                            .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)
            ) {
                // d, the left input, is smaller than t, so every execution hashes d and streams t. The
                // key columns sit at other positions in every table, and e and g insert their symbols
                // in another order than d, so equal texts have different keys.
                db.execute("""
                        CREATE TABLE t AS (
                            SELECT
                                (x % 500)::INT k,
                                x v,
                                timestamp_sequence('2024-01-01', 10_000_000) ts
                            FROM long_sequence(20_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """, context);
                db.execute("""
                        CREATE TABLE d AS (
                            SELECT
                                x w,
                                ('s' || (x % 40))::SYMBOL s,
                                (x % 300)::LONG l,
                                (x % 450)::INT k,
                                timestamp_sequence('2023-12-01', 60_000_000) dts
                            FROM long_sequence(3_000)
                        ) TIMESTAMP(dts) PARTITION BY HOUR
                        """, context);
                db.execute("CREATE TABLE e AS (SELECT ('s' || (34 - x % 35))::SYMBOL s, x ew, 'e' || (x % 5) es FROM long_sequence(70))", context);
                db.execute("CREATE TABLE g AS (SELECT x * 3 gw, ('s' || (39 - x % 40))::SYMBOL gs, (x % 280)::LONG l FROM long_sequence(560))", context);
                context.changePageFrameSizes(256, 256);
                code.run(db, context);
            }
        }, configuration, LOG));
    }

    @FunctionalInterface
    private interface Code {
        void run(CairoEngine db, SqlExecutionContextImpl context) throws Exception;
    }
}
