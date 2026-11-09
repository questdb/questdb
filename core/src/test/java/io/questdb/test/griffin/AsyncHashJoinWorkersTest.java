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
 * The parallel hash join on shared query workers that run on threads and on fibers: builds on the
 * owner and in rounds of the join's build sequence, and probe frames joined on the workers. Every
 * query compares with the ordinary plan, in order. {@link SlotGatedWorkStealingStrategy} holds the
 * owner off until a worker took a slot of the build rounds' atom and of the probe's, so that the
 * checks that every slot is back after a query, a failed one included, count slots that workers
 * really took.
 */
public class AsyncHashJoinWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinWorkersTest.class);
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
    public void testRoundsAndProbesOnFiberWorkers() throws Exception {
        assertRoundsAndProbes(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testRoundsAndProbesOnThreadWorkers() throws Exception {
        assertRoundsAndProbes(WorkerPoolMode.LEGACY);
    }

    private static void assertMemoryLimitsReleaseSlots(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 256);
            final String sql = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k";
            final String expected = ordinary(sql, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Limits from a few bytes up fail the query at every allocation in turn: the owner's
                // build, the build's round tasks, the payload copy and the probe tasks. Each failure
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

    private static void assertRoundsAndProbes(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 256);
            for (long minRows : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (String sql : new String[]{
                        "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k",
                        "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.s = d.s",
                        "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.l = d.l AND t.s = d.s",
                        "SELECT t.k, t.v, d.w, d.s FROM t JOIN d ON t.k = d.k WHERE t.ts IN '2024-01-01'",
                        // The cursor closes with frames in flight, and a negative LIMIT counts the rows
                        // first, then reads them again.
                        "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k LIMIT 7",
                        "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k LIMIT -7",
                }) {
                    final String expected = ordinary(sql, context);
                    context.setParallelHashJoinEnabled(true);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        // The owner waits until a worker took a slot of each atom: one that builds in
                        // rounds runs round tasks on the workers, and every query runs probe tasks there.
                        final CountDownLatch roundSlot = minRows == 0 ? new CountDownLatch(1) : null;
                        final CountDownLatch probeSlot = new CountDownLatch(1);
                        join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(roundSlot);
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(probeSlot);
                        final HashJoinBuildStage stage = join.getAtom().getStage(0);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(minRows == 0, stage.isBuiltInRounds());
                        }
                        Assert.assertEquals(expected, result(factory, context));
                        if (roundSlot != null) {
                            Assert.assertEquals(0, roundSlot.getCount());
                        }
                        Assert.assertEquals(0, probeSlot.getCount());
                        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        join.getBuildRoundAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                        join.getAtom().getPerWorkerLocks().setTestAcquireLatch(null);
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
                db.execute("""
                        CREATE TABLE t AS (
                            SELECT
                                (x % 500)::INT k,
                                x v,
                                (x % 300)::LONG l,
                                ('s' || (x % 400))::SYMBOL s,
                                timestamp_sequence('2024-01-01', 10_000_000) ts
                            FROM long_sequence(20_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """, context);
                db.execute("""
                        CREATE TABLE d AS (
                            SELECT
                                (x % 450)::INT k,
                                x w,
                                (x % 300)::LONG l,
                                ('s' || (x % 350))::SYMBOL s
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
