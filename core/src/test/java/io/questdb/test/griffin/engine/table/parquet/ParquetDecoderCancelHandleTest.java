/*******************************************************************************
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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies the plumbing that lets a parquet decoder observe a query's cancellation while it
 * decodes on the parallel-reduce path. A cold (blocking) decoder probes the handle it was
 * handed, so the handle must be safe to probe from the reduce workers and must reach the
 * decoder on every parallel-reduce query shape.
 * <p>
 * A recording {@link ParquetPartitionDecoder} installed via the configuration's decoder factory
 * captures the handle the pool forwards. The assertions would fail against the pre-fix plumbing,
 * which pushed the raw, thread-unsafe {@code SqlExecutionContext} breaker (shared across every
 * reduce worker) on the filter path only and never wired the aggregation/join paths at all.
 */
public class ParquetDecoderCancelHandleTest extends AbstractCairoTest {

    private static final AtomicInteger decodersCreated = new AtomicInteger();
    // Captures the last non-null cancel handle any parquet decoder received. Written from the
    // reduce path (owner or worker thread), read from the test thread -> AtomicReference.
    private static final AtomicReference<SqlExecutionCircuitBreaker> lastCancelHandle = new AtomicReference<>();
    private static final AtomicInteger nonNullCancelHandleCalls = new AtomicInteger();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean isSqlParallelFilterEnabled() {
                        return true;
                    }

                    @Override
                    public boolean isSqlParallelGroupByEnabled() {
                        return true;
                    }

                    @Override
                    public boolean isSqlParallelReadParquetEnabled() {
                        return true;
                    }

                    @Override
                    public ParquetPartitionDecoder newParquetPartitionDecoder() {
                        return new RecordingParquetPartitionDecoder();
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAsyncFilterOverParquetDeliversPerPoolBreaker() throws Exception {
        // Ordered reduce path (async filter). The decoder is created lazily on the first parquet
        // frame AFTER setCancelHandle runs, so this also exercises the activateDecoder re-arm.
        assertPerPoolBreakerDelivered("SELECT ts, v FROM x WHERE v > 100");
    }

    @Test
    public void testAsyncGroupByOverParquetDeliversPerPoolBreaker() throws Exception {
        // Unordered reduce path (async keyed GROUP BY) decodes cold parquet through the
        // AsyncFilterContext per-worker pools, which before the fix never received a handle.
        assertPerPoolBreakerDelivered("SELECT v % 100 AS k, count() FROM x WHERE v > 100");
    }

    private void assertPerPoolBreakerDelivered(String query) throws Exception {
        assertMemoryLeak(() -> {
            // Sanity: the engine really uses our recording decoder.
            try (ParquetPartitionDecoder probe = configuration.newParquetPartitionDecoder()) {
                Assert.assertTrue(
                        "recording decoder is not installed engine-wide",
                        probe instanceof RecordingParquetPartitionDecoder
                );
            }

            // 2000 rows at 5-minute steps span seven daily partitions; the first three are converted
            // to parquet (they are not the active partition), so the scan decodes real parquet frames.
            execute("CREATE TABLE x AS (" +
                    "  SELECT x::double v, timestamp_sequence(0, 300_000_000L) ts" +
                    "  FROM long_sequence(2000)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01-01', '1970-01-02', '1970-01-03'");

            // Drop any handle/counters captured while building/converting the partition.
            lastCancelHandle.set(null);
            nonNullCancelHandleCalls.set(0);
            decodersCreated.set(0);

            // workerCount > 0 makes the compiler pick the parallel (async) factory; the owner
            // thread work-steals and runs the reduce, which is what pushes the handle.
            try (SqlExecutionContext parallelCtx = TestUtils.createSqlExecutionCtx(engine, 4);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, parallelCtx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(parallelCtx)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // drain: forces the reduce path to decode the parquet frame
                    }
                }
            }

            final SqlExecutionCircuitBreaker handle = lastCancelHandle.get();
            Assert.assertNotNull(
                    "parquet decoder never received a cancel handle [decodersCreated=" + decodersCreated.get()
                            + ", nonNullCancelHandleCalls=" + nonNullCancelHandleCalls.get() + "]",
                    handle
            );
            // Before the fix the decoder got the raw, thread-unsafe query breaker shared across every
            // reduce worker; the fix hands each pool its own SqlExecutionCircuitBreakerWrapper view.
            Assert.assertTrue(
                    "decoder must receive a per-pool wrapper, not the raw shared query breaker (got "
                            + handle.getClass().getName() + ")",
                    handle instanceof SqlExecutionCircuitBreakerWrapper
            );
        });
    }

    private static class RecordingParquetPartitionDecoder extends ParquetPartitionDecoder {
        private RecordingParquetPartitionDecoder() {
            decodersCreated.incrementAndGet();
        }

        @Override
        public void setCancelHandle(SqlExecutionCircuitBreaker cancelHandle) {
            if (cancelHandle != null) {
                nonNullCancelHandleCalls.incrementAndGet();
                lastCancelHandle.set(cancelHandle);
            }
            super.setCancelHandle(cancelHandle);
        }
    }
}
