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
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Verifies the vectorized (rosti) keyed GROUP BY keeps its {@code NATIVE_ROSTI}
 * memory accounting balanced when {@code wrapUp()} grows the map.
 * <p>
 * {@code wrapUp()} inserts the null-key slot for column-top rows. When the live
 * keys have filled the map to its growth threshold, that insert resizes the
 * rosti. The single-worker build path used by the default execution context
 * (one shared query worker) did not record the resize - only the multi-worker
 * merge path bracketed {@code wrapUp()} with {@code updateMemoryUsage()}. The
 * unrecorded growth then made {@code close()}'s reset over-subtract, leaving the
 * {@code NATIVE_ROSTI} tag with a negative net delta at end of run. The query
 * fuzzer's malloc fault injection surfaced this over-free.
 */
public class GroupByVectorizedRostiAccountingTest extends AbstractCairoTest {

    @Test
    public void testWrapUpResizeKeepsNativeRostiBalanced() throws Exception {
        assertMemoryLeak(() -> {
            for (int liveKeys = 888; liveKeys <= 904; liveKeys++) {
                createColumnTopTable(liveKeys, sqlExecutionContext);

                final String query = "SELECT k, count() FROM tab";
                printSql("EXPLAIN " + query);
                TestUtils.assertContains(sink, "GroupBy vectorized: true");

                try (RecordCursorFactory factory = select(query)) {
                    drain(factory, sqlExecutionContext);
                }

                execute("DROP TABLE tab");
            }
        });
    }

    @Test
    public void testWrapUpResizeKeepsNativeRostiBalancedMultiWorker() throws Exception {
        // Same boundary sweep, but with a four-worker execution context so the build
        // runs through the multi-worker merge path. Confirms the merge path stays
        // balanced too.
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
                    createColumnTopTable(liveKeys, parallelCtx);

                    final String query = "SELECT k, count() FROM tab";
                    try (RecordCursorFactory factory = select(query, parallelCtx)) {
                        drain(factory, parallelCtx);
                    }

                    execute("DROP TABLE tab", parallelCtx);
                }
            } finally {
                pool.halt();
            }
        });
    }

    // The default 1024 map capacity gives a growth threshold of 896 live keys; at
    // exactly that count the null-key insert in wrapUp() resizes. The caller sweeps
    // around the boundary so one iteration lands the resize regardless of small
    // differences in the rosti growth math.
    private static void createColumnTopTable(int liveKeys, SqlExecutionContext context) throws Exception {
        execute("CREATE TABLE tab (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
        // A partition written before k existed: its rows read back as column tops
        // (null keys), so wrapUp() must insert the null-key slot.
        execute("INSERT INTO tab SELECT (x * 1000000L)::timestamp, x FROM long_sequence(8)", context);
        execute("ALTER TABLE tab ADD COLUMN k INT", context);
        // liveKeys distinct non-null keys in a later partition.
        execute("INSERT INTO tab SELECT (86400000000L + x * 1000000L)::timestamp, x, x::int " +
                "FROM long_sequence(" + liveKeys + ")", context);
    }

    private static void drain(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        final StringSink localSink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(context)) {
            final RecordMetadata metadata = factory.getMetadata();
            final int columnCount = metadata.getColumnCount();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnCount; i++) {
                    CursorPrinter.printColumn(record, metadata, i, localSink, false);
                }
                localSink.clear();
            }
        }
    }
}
