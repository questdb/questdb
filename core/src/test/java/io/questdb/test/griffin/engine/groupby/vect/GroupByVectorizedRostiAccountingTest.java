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

package io.questdb.test.griffin.engine.groupby.vect;

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Rosti;
import io.questdb.std.RostiAllocFacadeImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies the vectorized (rosti) keyed GROUP BY keeps its {@code NATIVE_ROSTI}
 * memory accounting balanced when inserting the null group grows the map.
 * <p>
 * {@code GroupByRecordCursorFactory} inserts the null-key slot once, after the drain, for
 * column-top rows. When the live keys have filled the map to its growth threshold, that
 * insert resizes the rosti, and the insert is the last operation that can: it runs after
 * all aggregation, whichever partition order the scan takes. Growth that goes unrecorded
 * makes {@code close()}'s reset over-subtract, leaving the {@code NATIVE_ROSTI} tag with a
 * negative net delta at end of run - which is how the query fuzzer's malloc fault injection
 * surfaced the original over-free in the single-worker build path, where {@code wrapUp()}
 * was not bracketed with {@code updateMemoryUsage()}.
 */
public class GroupByVectorizedRostiAccountingTest extends AbstractCairoTest {

    @Test
    public void testNullGroupInsertResizeKeepsNativeRostiBalancedColumnTopFirst() throws Exception {
        final GrowthRecordingRostiAllocFacade facade = new GrowthRecordingRostiAllocFacade();
        configOverrideRostiAllocFacade(facade);
        assertMemoryLeak(() -> {
            for (int liveKeys = 888; liveKeys <= 904; liveKeys++) {
                createColumnTopTable(liveKeys, true, sqlExecutionContext);

                final String query = "SELECT k, count() FROM tab";
                assertQuery(query).noLeakCheck().assertsPlanContaining("GroupBy vectorized: true");

                try (RecordCursorFactory factory = select(query)) {
                    drain(factory, sqlExecutionContext, liveKeys + 1);
                }

                execute("DROP TABLE tab");
            }
            assertHasResized(facade);
        });
    }

    @Test
    public void testNullGroupInsertResizeKeepsNativeRostiBalancedColumnTopLast() throws Exception {
        // The reversed partition order scans the column-top rows after the live keys have filled
        // the map. The insert lands at the end of the build either way, so both orders must record
        // the growth it causes, or close() subtracts more than was added.
        final GrowthRecordingRostiAllocFacade facade = new GrowthRecordingRostiAllocFacade();
        configOverrideRostiAllocFacade(facade);
        assertMemoryLeak(() -> {
            for (int liveKeys = 888; liveKeys <= 904; liveKeys++) {
                createColumnTopTable(liveKeys, false, sqlExecutionContext);

                final String query = "SELECT k, count() FROM tab";
                assertQuery(query).noLeakCheck().assertsPlanContaining("GroupBy vectorized: true");

                try (RecordCursorFactory factory = select(query)) {
                    drain(factory, sqlExecutionContext, liveKeys + 1);
                }

                execute("DROP TABLE tab");
            }
            assertHasResized(facade);
        });
    }

    @Test
    public void testNullGroupInsertResizeKeepsNativeRostiBalancedMultiWorker() throws Exception {
        // Same boundary sweep, but with a four-worker execution context so the build
        // runs through the multi-worker merge path. Confirms the merge path stays
        // balanced too, and that the null group survives the merge exactly once.
        final GrowthRecordingRostiAllocFacade facade = new GrowthRecordingRostiAllocFacade();
        configOverrideRostiAllocFacade(facade);
        assertMemoryLeak(() -> {
            final int workerCount = 4;
            final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "rostiAcct";
                }

                @Override
                public int getWorkerCount() {
                    return workerCount;
                }
            });
            WorkerPoolUtils.setupQueryJobs(pool, engine);
            pool.start(null);
            try (SqlExecutionContext parallelCtx = new SqlExecutionContextImpl(engine, workerCount)
                    .with(securityContext, bindVariableService, null, -1, circuitBreaker)) {
                parallelCtx.initNow();
                for (int liveKeys = 888; liveKeys <= 904; liveKeys++) {
                    createColumnTopTable(liveKeys, true, parallelCtx);

                    final String query = "SELECT k, count() FROM tab";
                    try (RecordCursorFactory factory = select(query, parallelCtx)) {
                        drain(factory, parallelCtx, liveKeys + 1);
                    }

                    execute("DROP TABLE tab", parallelCtx);
                }
                assertHasResized(facade);
            } finally {
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    private static void assertHasResized(GrowthRecordingRostiAllocFacade facade) {
        Assert.assertTrue("no sweep iteration grew the rosti, so none of them covered the insert "
                + "resize; the growth threshold moved -- re-derive the liveKeys sweep", facade.hasGrown);
    }

    // The default 1024 map capacity gives a growth threshold of 896 live keys; at exactly that
    // count adding the null group resizes the map. The caller sweeps around the boundary so one
    // iteration lands the resize regardless of small differences in the rosti growth math.
    //
    // isColumnTopFirst puts the partition written before k existed at the EARLIER timestamps, so
    // its column-top rows are scanned before the live keys fill the map; false scans them after.
    // Either way the null group is materialized from row presence alone.
    private static void createColumnTopTable(int liveKeys, boolean isColumnTopFirst, SqlExecutionContext context) throws Exception {
        final long columnTopDay = isColumnTopFirst ? 0 : 86_400_000_000L;
        final long liveKeysDay = isColumnTopFirst ? 86_400_000_000L : 0;
        execute("CREATE TABLE tab (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
        execute("INSERT INTO tab SELECT (" + columnTopDay + " + x * 1_000_000L)::timestamp, x FROM long_sequence(8)", context);
        execute("ALTER TABLE tab ADD COLUMN k INT", context);
        execute("""
                INSERT INTO tab SELECT (%d + x * 1_000_000L)::timestamp, x, x::int
                FROM long_sequence(%d)""".formatted(liveKeysDay, liveKeys), context);
    }

    private static void drain(RecordCursorFactory factory, SqlExecutionContext context, int expectedRowCount) throws Exception {
        final StringSink localSink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(context)) {
            final RecordMetadata metadata = factory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            final Record record = cursor.getRecord();
            int rowCount = 0;
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    CursorPrinter.printColumn(record, metadata, i, localSink, false);
                }
                localSink.clear();
                rowCount++;
            }
            // Balanced accounting over a result that quietly lost the null group would still pass.
            Assert.assertEquals("live keys plus the null group", expectedRowCount, rowCount);
        }
    }

    // Records whether any bracketed rosti operation actually grew the map, so a sweep that stops
    // straddling the growth threshold fails instead of passing vacuously.
    private static class GrowthRecordingRostiAllocFacade extends RostiAllocFacadeImpl {
        private volatile boolean hasGrown;

        @Override
        public void updateMemoryUsage(long pRosti, long oldSize) {
            if (Rosti.getAllocMemory(pRosti) != oldSize) {
                hasGrown = true;
            }
            super.updateMemoryUsage(pRosti, oldSize);
        }
    }
}
