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
 * The parallel hash join's ON residuals and post-join filters on shared query workers that run on
 * threads and on fibers: the workers run the filters over each pair, each on a copy of its own when
 * the filter is not thread safe, with symbol tables of its own, and frames whose tuples outgrow the
 * cap continue on the consumer. Every query compares with the ordinary plan, in order.
 * {@link SlotGatedWorkStealingStrategy} holds the owner off until a worker took a slot, so that the
 * checks that every slot is back after a query, a failed one included, count slots that workers
 * really took.
 */
public class AsyncHashJoinPairFilterWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinPairFilterWorkersTest.class);
    private static final String[] QUERIES = {
            // A residual and a post-join filter over SYMBOL columns of both inputs and a regex, which
            // is not thread safe, so every worker runs a copy of its own.
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND d.s <> t.s AND t.s ~ 's1[0-9]'",
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k WHERE d.w IS NULL OR d.s = 's3' OR t.s ~ 's[2-4]'",
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k AND d.w % 3 = 0 WHERE d.w IS NULL OR d.w > t.v % 1_000",
            // An INNER join with a probe filter and a post-join filter, and a SYMBOL key.
            "SELECT t.k, t.v, d.w, d.s FROM t JOIN d ON t.s = d.s WHERE t.v % 7 = 0 AND d.w % 11 <> t.v % 11",
            // Bind variables in the residual and the post-join filter.
            "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > $1 WHERE d.w IS NULL OR d.w < t.v + $2",
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
    public void testPairFiltersOnFiberWorkers() throws Exception {
        assertPairFilters(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testPairFiltersOnThreadWorkers() throws Exception {
        assertPairFilters(WorkerPoolMode.LEGACY);
    }

    private static void assertMemoryLimitsReleaseSlots(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            // Frames continue past a cap of their rows, so that the limits fail the workers' tuples
            // and the consumer's chunks in turn.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            final String sql = QUERIES[2];
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
                    Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                }
                Assert.assertTrue(failures > 0);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                Assert.assertEquals(expected, result(factory, context));
                Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
            }
        });
    }

    private static void assertPairFilters(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 700);
            bindVariableService.setLong(1, 300);
            for (int cap : new int[]{1, 1_000_000}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, cap);
                for (String sql : QUERIES) {
                    final String expected = ordinary(sql, context);
                    context.setParallelHashJoinEnabled(true);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        final String plan = plan(factory, context);
                        Assert.assertTrue(sql + '\n' + plan, plan.contains("onFilter: ") || plan.contains("postJoinFilter: "));
                        // The owner waits until a worker took a slot: the probe tasks, which run the
                        // filters, run on the workers.
                        final CountDownLatch probeSlot = new CountDownLatch(1);
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(probeSlot);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(sql, cursor.hasNext());
                        }
                        Assert.assertEquals(sql, expected, result(factory, context));
                        Assert.assertEquals(sql, 0, probeSlot.getCount());
                        Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                        // A cap of a frame's rows makes the frames continue on the consumer.
                        Assert.assertEquals(sql, cap == 1, join.getAtom().getContinuationCount() > 0);
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
                            .with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null)
            ) {
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
                // d inserts its symbols in another order than t, so equal texts have different keys.
                db.execute("""
                        CREATE TABLE d AS (
                            SELECT
                                (x % 450)::INT k,
                                x w,
                                ('s' || (34 - x % 35))::SYMBOL s
                            FROM long_sequence(3_000)
                        )
                        """, context);
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
