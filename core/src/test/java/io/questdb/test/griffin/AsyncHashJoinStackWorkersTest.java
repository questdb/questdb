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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Stacks of the parallel hash join on shared query workers that run on threads and on fibers: the
 * workers run every stage of a probe frame nested, each on the slot's own probes, filter copies and
 * symbol tables, and build the later stages in rounds; the consumer expands the chains of the
 * stages after the last enumerated one. Every query compares with the ordinary plan, in order.
 * {@link SlotGatedWorkStealingStrategy} holds the owner off until a worker took a slot, so that the
 * checks that every slot is back after a query, a failed one included, count slots that workers
 * really took.
 */
public class AsyncHashJoinStackWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinStackWorkersTest.class);
    private static final String[] QUERIES = {
            // Three stages that hand their chains to the consumer: an INT, a SYMBOL and a staged key.
            "SELECT t.k, t.v, d.w, e.s, e.ew, g.gw FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN e ON t.s = e.s JOIN g ON t.lk = g.lk",
            // An ON residual and a post-join filter on the second stage, a regex, which is not thread
            // safe, so that every worker runs a copy of its own; the first two stages run on the
            // workers and the third hands its chains to the consumer.
            "SELECT t.k, t.v, d.w, e.ew, g.gw FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN e ON t.s = e.s AND e.es ~ 'e[1-3]' LEFT JOIN g ON t.lk = g.lk"
                    + " WHERE e.ew IS NULL OR d.w IS NULL OR e.ew % 7 <> d.w % 7",
            // An INNER first join under a probe filter, a post-join filter on the last stage, and
            // bind variables.
            "SELECT t.k, t.v, d.w, e.ew, g.gw FROM t JOIN d ON t.k = d.k JOIN e ON t.s = e.s LEFT JOIN g ON t.lk = g.lk"
                    + " WHERE t.v % 5 = 0 AND (g.gw IS NULL OR g.gw > d.w + $1)",
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
    public void testStacksOnFiberWorkers() throws Exception {
        assertStacks(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testStacksOnThreadWorkers() throws Exception {
        assertStacks(WorkerPoolMode.LEGACY);
    }

    private static void assertMemoryLimitsReleaseSlots(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            // The later stages build in rounds, and frames continue past a cap of their rows, so that
            // the limits fail the builds, the workers' tuples and the consumer's chunks in turn.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            final String sql = QUERIES[1];
            final String expected = ordinary(sql, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = db.select(sql, context)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final int failures = assertMemoryLimitSweep(
                        expected,
                        limit -> setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit),
                        () -> result(factory, context),
                        null,
                        () -> {
                            Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            for (int k = 0; k < 3; k++) {
                                Assert.assertNull(join.getAtom().getStage(k).getFrozenBuild());
                            }
                        }
                );
                Assert.assertTrue(failures > 0);
                Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
            }
        });
    }

    private static void assertStacks(WorkerPoolMode mode) throws Exception {
        execute(mode, (db, context) -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 300);
            for (long minRows : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (int cap : new int[]{1, 1_000_000}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, cap);
                    for (String sql : QUERIES) {
                        final String expected = ordinary(sql, context);
                        context.setParallelHashJoinEnabled(true);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(sql, join);
                            TestUtils.assertContains(plan(factory, context), "stages: 3");
                            // The owner waits until a worker took a slot: the probe tasks, which run
                            // every stage, run on the workers.
                            final CountDownLatch probeSlot = new CountDownLatch(1);
                            join.getAtom().getPerWorkerLocks().setTestAcquireLatch(probeSlot);
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertTrue(sql, cursor.hasNext());
                                for (int k = 0; k < 3; k++) {
                                    Assert.assertEquals(sql, minRows == 0, join.getAtom().getStage(k).isBuiltInRounds());
                                }
                            }
                            Assert.assertEquals(sql, expected, result(factory, context));
                            Assert.assertEquals(sql, 0, probeSlot.getCount());
                            Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertEquals(sql, 0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            join.getAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                        }
                    }
                }
            }
        });
    }

    private static void execute(WorkerPoolMode mode, WorkersCode code) throws Exception {
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, bindVariableService, configuration, LOG, (db, context) -> {
            db.execute("""
                    CREATE TABLE t AS (
                        SELECT
                            (x % 500)::INT k,
                            (x % 300)::LONG lk,
                            x v,
                            ('s' || (x % 40))::SYMBOL s,
                            timestamp_sequence('2024-01-01', 10_000_000) ts
                        FROM long_sequence(20_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """, context);
            // The dimensions repeat their keys, and e inserts its symbols in another order than
            // t, so equal texts have different keys.
            db.execute("CREATE TABLE d AS (SELECT (x % 450)::INT k, x w FROM long_sequence(900))", context);
            db.execute("CREATE TABLE e AS (SELECT ('s' || (34 - x % 35))::SYMBOL s, x ew, 'e' || (x % 5) es FROM long_sequence(70))", context);
            db.execute("CREATE TABLE g AS (SELECT (x % 280)::LONG lk, x * 3 gw FROM long_sequence(560))", context);
            context.changePageFrameSizes(256, 256);
            code.run(db, context);
        }));
    }
}
