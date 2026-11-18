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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Late materialization of a parallel hash join's Parquet probe frames on shared query workers that
 * run on threads and on fibers, over the tables of {@link AsyncHashJoinLateMaterializationTest}: each
 * slot keeps its own row list and its own share of the rows its frames passed on to the consumer.
 * {@link SlotGatedWorkStealingStrategy} holds the owner off until a worker took a probe slot. A slot
 * takes frames in the scan's order, and the frames that keep every row come last, so each slot
 * decodes late every frame before them, whichever slot joins it, and the counts of decoded columns
 * are those of the owner alone.
 */
public class AsyncHashJoinLateMaterializationWorkersTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinLateMaterializationWorkersTest.class);
    // The query, the columns its probe tasks read, and the Parquet frames that decode every column,
    // those whose rows the consumer reads some of.
    private static final Object[][] QUERIES = {
            {"SELECT t.id, t.v, t.d, t.s, t.vc, t.str, t.arr, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.f = 1", 2, 6},
            {"SELECT t.id, t.v, t.d, t.s, t.vc, t.str, t.arr, d.w FROM t JOIN d ON t.k = d.k", 1, 6},
            {"SELECT t.id, t.vc, t.arr, d.w FROM t JOIN d ON t.k = d.k WHERE d.w + t.v > 0", 2, 6},
            {"SELECT t.id, t.vc, t.arr, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w < t.v", 2, AsyncHashJoinLateMaterializationTest.PARQUET_FRAMES},
            // Three rows of d2 per key leave the frames that keep every row to continuations.
            {"SELECT t.id, t.vc, t.arr, d2.w FROM t JOIN d2 ON t.k = d2.k WHERE d2.w <> t.g AND t.f = 1", 3, 6},
            {"SELECT t.id, t.vc, d.w FROM t JOIN d ON t.k = d.k WHERE t.f % 4 = 3", 2, 0},
    };
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        super.setUp();
        // Three row groups, so three Parquet frames, per partition.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
    }

    @Test
    public void testLateDecodingOnFiberWorkers() throws Exception {
        assertLateDecoding(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testLateDecodingOnThreadWorkers() throws Exception {
        assertLateDecoding(WorkerPoolMode.LEGACY);
    }

    private static void assertLateDecoding(WorkerPoolMode mode) throws Exception {
        assertMemoryLeak(() -> executeOnWorkers(mode, WORKER_COUNT, null, configuration, LOG, (db, context) -> {
            for (String ddl : AsyncHashJoinLateMaterializationTest.DDL) {
                db.execute(ddl, context);
            }
            db.execute("CREATE TABLE d2 AS (SELECT (x % 4)::INT k, x::INT w FROM long_sequence(12))", context);
            for (Object[] query : QUERIES) {
                final String sql = (String) query[0];
                final int taskColumns = (Integer) query[1];
                final int framesWithRows = (Integer) query[2];
                final String expected = ordinary(sql, context);
                context.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    final AsyncHashJoinAtom atom = join.getAtom();
                    final int probeColumns = join.getSpec().getLeftInput().getMetadata().getColumnCount();
                    for (int run = 0; run < 2; run++) {
                        // The owner waits until a worker took a probe slot.
                        final CountDownLatch probeSlot = new CountDownLatch(1);
                        atom.getPerWorkerLocks().setTestAcquireLatch(probeSlot);
                        try {
                            Assert.assertEquals(sql, expected, result(factory, context));
                        } finally {
                            atom.getPerWorkerLocks().setTestAcquireLatch(null);
                        }
                        Assert.assertEquals(sql, 0, probeSlot.getCount());
                        Assert.assertEquals(sql, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertNotNull(sql, atom.getProbeTaskColumns());
                        Assert.assertEquals(sql, taskColumns, atom.getProbeTaskColumns().size());
                        Assert.assertEquals(sql, AsyncHashJoinLateMaterializationTest.PARQUET_FRAMES, atom.getParquetProbeFrameCount());
                        Assert.assertEquals(
                                sql,
                                (long) framesWithRows * probeColumns + (long) (AsyncHashJoinLateMaterializationTest.PARQUET_FRAMES - framesWithRows) * taskColumns,
                                atom.getDecodedProbeColumnCount()
                        );
                    }
                }
            }
        }));
    }
}
