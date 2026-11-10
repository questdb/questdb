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
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The probe frames and the dispatch window of a parallel hash join on shared query workers that run
 * on threads and on fibers: the first window under a LIMIT, the fan-out-aware frames and the
 * dynamic window of the tuple ring, whose entries frames in flight never share, which the ring
 * asserts. Every query compares with the ordinary plan, in order.
 */
public class AsyncHashJoinWindowWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinWindowWorkersTest.class);
    private static final int WORKER_COUNT = 4;

    @Test
    public void testWindowsOnFiberWorkers() throws Exception {
        assertWindows(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testWindowsOnThreadWorkers() throws Exception {
        assertWindows(WorkerPoolMode.LEGACY);
    }

    private static void assertWindows(WorkerPoolMode mode) throws Exception {
        // A ring budget of a frame of every row of the two-stage stars: the window starts at one
        // frame, and widens once the selective frames, the first half, show their few tuples.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 256 * 3 * Long.BYTES);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 512);
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            try (
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT)
                            .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)
            ) {
                createTables(db, context);
                context.changePageFrameSizes(1, 256);
                for (String sql : new String[]{
                        // Sparse frames, then dense ones.
                        "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k WHERE t.v % 64 = 0 OR t.v > 5_000",
                        "SELECT t.v, u.w, d.w FROM t JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v WHERE t.v % 64 = 0 OR t.v > 5_000",
                        // Fan-out under a residual and a post-join filter: frames of 512 / 4 rows.
                        "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v",
                        "SELECT t.v, u.w, d.w FROM t JOIN u ON t.k = u.k JOIN d ON t.k = d.k WHERE d.w + u.w > t.v",
                        // LIMITs: the first window, and LIMITs that it leaves as they are.
                        "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v LIMIT 3",
                        "SELECT t.v, u.w, d.w FROM t JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k LIMIT 700",
                        "SELECT t.v, u.w, d.w FROM t JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k ORDER BY ts DESC LIMIT 5",
                        "SELECT t.v, u.w, d.w FROM t LEFT JOIN u ON t.k = u.k LEFT JOIN d ON t.k = d.k AND d.w > t.v LIMIT -7",
                }) {
                    final String expected = ordinary(sql, context);
                    context.setParallelHashJoinEnabled(true);
                    try (RecordCursorFactory factory = db.select(sql, context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        for (int execution = 0; execution < 2; execution++) {
                            Assert.assertEquals(sql, expected, result(factory, context));
                            Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        }
                        if (sql.endsWith("LIMIT 3")) {
                            // The first frame's task alone went out before the first row, and joined
                            // three rows, the workers' tasks included.
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertTrue(cursor.hasNext());
                                Assert.assertEquals(sql, 1, join.getDispatchedFrameCount());
                                Assert.assertEquals(sql, 3, join.getAtom().getJoinedProbeRowCount());
                            }
                        } else if (!sql.contains("LIMIT")) {
                            // The fan-out-aware frames need no continuation.
                            Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                        }
                    } finally {
                        context.setParallelHashJoinEnabled(false);
                    }
                }
            }
        }, configuration, LOG));
    }

    private static void createTables(CairoEngine db, SqlExecutionContextImpl context) throws Exception {
        db.execute("""
                CREATE TABLE t AS (
                    SELECT (x % 50)::INT k, x v, timestamp_sequence('2024-01-01', 10_000_000) ts
                    FROM long_sequence(10_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """, context);
        db.execute("CREATE TABLE u AS (SELECT (x % 50)::INT k, x * 7 w FROM long_sequence(50))", context);
        // Four rows of d per key.
        db.execute("CREATE TABLE d AS (SELECT (x % 50)::INT k, x * 100 w FROM long_sequence(200))", context);
    }
}
