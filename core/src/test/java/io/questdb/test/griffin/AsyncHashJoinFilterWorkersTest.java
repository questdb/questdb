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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
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
 * The parallel hash join over filtered inputs on shared query workers that run on threads and on
 * fibers: the probe filter runs on the workers' slots, each with a copy of its own when the filter is
 * not thread safe, and the build filter runs in the build's rounds, on the workers' copies too. Every
 * query compares with the ordinary plan, in order. {@link SlotGatedWorkStealingStrategy} holds the
 * owner off until a worker took a slot, so that the checks that every slot is back after a query, a
 * failed one included, count slots that workers really took.
 */
public class AsyncHashJoinFilterWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinFilterWorkersTest.class);
    private static final String[] QUERIES = {
            // A compiled probe filter, and a regex, which compiles into no JIT code and which every
            // worker runs on a copy of its own.
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k WHERE t.v % 7 <> 0 AND t.v > 100",
            "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k WHERE t.s ~ 's1[0-9]'",
            // The same for the build, built in rounds.
            "SELECT t.k, t.v, d2.w, d2.s FROM t LEFT JOIN (SELECT * FROM d WHERE w > 500) d2 ON t.k = d2.k",
            "SELECT t.k, t.v, d2.w, d2.s FROM t LEFT JOIN (SELECT * FROM d WHERE s ~ 's2[0-9]') d2 ON t.k = d2.k",
            // Both, under an INNER join and a SYMBOL key.
            "SELECT t.k, t.v, d2.w, d2.s FROM t JOIN (SELECT * FROM d WHERE s ~ 's[1-3]') d2 ON t.s = d2.s WHERE t.s ~ 's[2-4]' AND t.v > 10",
            // Bind variables in both filters.
            "SELECT t.k, t.v, d2.w FROM t JOIN (SELECT * FROM d WHERE w > $2) d2 ON t.k = d2.k WHERE t.v > $1",
    };
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        super.setUp();
    }

    @Test
    public void testFiltersOnFiberWorkers() throws Exception {
        assertFilters(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFiltersOnThreadWorkers() throws Exception {
        assertFilters(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testMemoryLimitsReleaseSlotsOnFiberWorkers() throws Exception {
        assertMemoryLimitsReleaseSlots(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testMemoryLimitsReleaseSlotsOnThreadWorkers() throws Exception {
        assertMemoryLimitsReleaseSlots(WorkerPoolMode.LEGACY);
    }

    private static void assertFilters(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 256);
            bindVariableService.clear();
            bindVariableService.setLong(0, 1_000);
            bindVariableService.setLong(1, 700);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                context.setJitMode(jitMode);
                for (String sql : QUERIES) {
                    final String expected = ordinary(sql, context);
                    context.setParallelHashJoinEnabled(true);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        // The owner waits until a worker took a slot of each atom: the build's rounds and
                        // the probe tasks run on the workers.
                        final CountDownLatch roundSlot = new CountDownLatch(1);
                        final CountDownLatch probeSlot = new CountDownLatch(1);
                        join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(roundSlot);
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(probeSlot);
                        final HashJoinBuildStage stage = join.getAtom().getStage(0);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(sql, cursor.hasNext());
                            Assert.assertTrue(sql, stage.isBuiltInRounds());
                        }
                        Assert.assertEquals(sql, expected, result(factory, context));
                        Assert.assertEquals(sql, 0, roundSlot.getCount());
                        Assert.assertEquals(sql, 0, probeSlot.getCount());
                        Assert.assertEquals(sql, 0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                    }
                }
            }
        });
    }

    private static void assertMemoryLimitsReleaseSlots(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 256);
            final String sql = "SELECT t.k, t.v, d2.w FROM t LEFT JOIN (SELECT * FROM d WHERE s ~ 's2[0-9]') d2 ON t.k = d2.k WHERE t.s ~ 's1[0-9]'";
            final String expected = ordinary(sql, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Limits from a few bytes up fail the query at every allocation in turn: the build's
                // filtered round tasks, the payload copy and the filtered probe tasks. Each failure
                // leaves every slot back, and the first limit that admits the query reads the rows.
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
                db.execute("""
                        CREATE TABLE d AS (
                            SELECT
                                (x % 450)::INT k,
                                x w,
                                ('s' || (x % 35))::SYMBOL s
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
