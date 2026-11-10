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
 * The parallel hash join's build-only ON pushdown on shared query workers that run on threads and on
 * fibers: the leading conjuncts of a LEFT join's ON residual that read the build alone run over the
 * build's frames in the tasks of the build's rounds, and in the round that builds small stages at
 * once, each worker on a copy of its own when they are not thread safe; a build whose conjuncts throw
 * on a row on any worker gives them up and leaves the residual to the pairs. Every query compares with
 * the ordinary plan, in order. {@link SlotGatedWorkStealingStrategy} holds the owner off until a
 * worker took a slot of the build's rounds, so that the workers run the pushed conjuncts.
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

    private static void assertPushdown(WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            Assert.assertEquals(mode == WorkerPoolMode.FIBER_HOST, db.getMessageBus().getPageFrameReduceDispatcher() != null);
            try (
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT)
                            .with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null)
            ) {
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
            }
        }, configuration, LOG));
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
}
