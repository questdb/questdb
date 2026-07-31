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
 * <p>
 * Each sweep guards itself with {@link NullKeyInsertGrowthRostiAllocFacade}, which counts only the
 * growth the null-key insert itself causes. Counting any growth would not guard anything: the keyed
 * aggregation brackets every (page frame, aggregate) pair with {@code updateMemoryUsage()} too, and
 * the live keys alone cross the threshold at the top of the sweep.
 */
public class GroupByVectorizedRostiAccountingTest extends AbstractCairoTest {

    @Test
    public void testNullGroupInsertResizeKeepsNativeRostiBalancedColumnTopFirst() throws Exception {
        final NullKeyInsertGrowthRostiAllocFacade facade = new NullKeyInsertGrowthRostiAllocFacade();
        configOverrideRostiAllocFacade(facade);
        assertMemoryLeak(() -> {
            for (int liveKeys = 888; liveKeys <= 904; liveKeys++) {
                createColumnTopTable(liveKeys, true, false, sqlExecutionContext);

                final String query = "SELECT k, count() FROM tab";
                assertQuery(query).noLeakCheck().assertsPlanContaining("GroupBy vectorized: true");

                try (RecordCursorFactory factory = select(query)) {
                    drain(factory, sqlExecutionContext, liveKeys + 1);
                }

                execute("DROP TABLE tab");
            }
            assertNullKeyInsertResized(facade);
        });
    }

    @Test
    public void testNullGroupInsertResizeKeepsNativeRostiBalancedColumnTopLast() throws Exception {
        // The reversed partition order scans the column-top rows after the live keys have filled
        // the map. The insert lands at the end of the build either way, so both orders must record
        // the growth it causes, or close() subtracts more than was added.
        final NullKeyInsertGrowthRostiAllocFacade facade = new NullKeyInsertGrowthRostiAllocFacade();
        configOverrideRostiAllocFacade(facade);
        assertMemoryLeak(() -> {
            for (int liveKeys = 888; liveKeys <= 904; liveKeys++) {
                createColumnTopTable(liveKeys, false, false, sqlExecutionContext);

                final String query = "SELECT k, count() FROM tab";
                assertQuery(query).noLeakCheck().assertsPlanContaining("GroupBy vectorized: true");

                try (RecordCursorFactory factory = select(query)) {
                    drain(factory, sqlExecutionContext, liveKeys + 1);
                }

                execute("DROP TABLE tab");
            }
            assertNullKeyInsertResized(facade);
        });
    }

    @Test
    public void testNullGroupInsertResizeKeepsNativeRostiBalancedMultiWorker() throws Exception {
        // Same boundary sweep, but with a four-worker execution context so the build
        // runs through the multi-worker merge path. Confirms the merge path stays
        // balanced too, and that the null group survives the merge exactly once.
        final NullKeyInsertGrowthRostiAllocFacade facade = new NullKeyInsertGrowthRostiAllocFacade();
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
                    createColumnTopTable(liveKeys, true, false, parallelCtx);

                    final String query = "SELECT k, count() FROM tab";
                    try (RecordCursorFactory factory = select(query, parallelCtx)) {
                        drain(factory, parallelCtx, liveKeys + 1);
                    }

                    execute("DROP TABLE tab", parallelCtx);
                }
                assertNullKeyInsertResized(facade);
            } finally {
                pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            }
        });
    }

    private static void assertNullKeyInsertResized(NullKeyInsertGrowthRostiAllocFacade facade) {
        Assert.assertTrue("no sweep iteration grew the rosti inside the post-drain null group insert, "
                + "so none of them covered the insert resize; either the growth threshold moved, or the "
                + "insert no longer runs under a method named " + NullKeyInsertGrowthRostiAllocFacade.NULL_KEY_INSERT_METHOD
                + "() -- re-derive the liveKeys sweep", facade.hasGrownOnNullKeyInsert);
    }

    // The default 1024 map capacity gives a growth threshold of 896 live keys; at exactly that
    // count adding the null group resizes the map. The caller sweeps around the boundary so one
    // iteration lands the resize regardless of small differences in the rosti growth math.
    //
    // isColumnTopFirst puts the partition written before k existed at the EARLIER timestamps, so
    // its column-top rows are scanned before the live keys fill the map; false scans them after.
    // Either way the null group is materialized from row presence alone.
    //
    // isValueColumnTop adds v late as well, so the column-top rows carry neither a key nor a value.
    // GroupByVectorizedOomTest needs that: an aggregate that sees no value cannot create the null
    // group in its wrapUp(), which leaves the post-drain insert as the only thing that can.
    static void createColumnTopTable(int liveKeys, boolean isColumnTopFirst, boolean isValueColumnTop, SqlExecutionContext context) throws Exception {
        final long columnTopDay = isColumnTopFirst ? 0 : 86_400_000_000L;
        final long liveKeysDay = isColumnTopFirst ? 86_400_000_000L : 0;
        if (isValueColumnTop) {
            execute("CREATE TABLE tab (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
            execute("INSERT INTO tab SELECT (" + columnTopDay + " + x * 1_000_000L)::timestamp FROM long_sequence(8)", context);
            execute("ALTER TABLE tab ADD COLUMN v LONG", context);
        } else {
            execute("CREATE TABLE tab (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
            execute("INSERT INTO tab SELECT (" + columnTopDay + " + x * 1_000_000L)::timestamp, x FROM long_sequence(8)", context);
        }
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

    // Records whether the post-drain null group insert grew the map, so a sweep that stops straddling
    // the growth threshold fails instead of passing vacuously.
    //
    // Recording any growth would guard nothing. GroupByRecordCursorFactory brackets four different
    // operations with updateMemoryUsage(): the keyed aggregation of every (page frame, aggregate)
    // pair, the null group insert, each merge source, and each wrapUp(). The live keys alone cross
    // the growth threshold over the top of the sweep, so an "any growth" flag is set by the
    // aggregation whether or not the insert ever resized anything - which is what it exists to
    // detect. The other three post-drain operations run after the insert and can grow the map too,
    // so "growth after the drain" does not separate them either; the calling frame does.
    private static class NullKeyInsertGrowthRostiAllocFacade extends RostiAllocFacadeImpl {
        static final String NULL_KEY_INSERT_METHOD = "insertNullKeyConditionally";
        private volatile boolean hasGrownOnNullKeyInsert;

        @Override
        public void updateMemoryUsage(long pRosti, long oldSize) {
            if (Rosti.getAllocMemory(pRosti) != oldSize && isNullKeyInsertOnStack()) {
                hasGrownOnNullKeyInsert = true;
            }
            super.updateMemoryUsage(pRosti, oldSize);
        }

        // Walks the caller's frames rather than only its immediate caller, so splitting the insert
        // across a helper keeps the guard working. No other bracketed operation can reach this
        // facade with the insert on its stack: the insert neither aggregates, merges nor wraps up.
        private static boolean isNullKeyInsertOnStack() {
            return StackWalker.getInstance().walk(
                    frames -> frames.anyMatch(frame -> NULL_KEY_INSERT_METHOD.equals(frame.getMethodName()))
            );
        }
    }
}
