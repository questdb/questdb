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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression guard: a tripped circuit breaker (CANCEL QUERY / timeout) observed by the non-parallel
 * {@link io.questdb.griffin.engine.functions.table.ReadParquetRecordCursor} must keep its cancellation
 * classification and message, not be relabeled as a "Parquet file is likely corrupted" read error.
 * <p>
 * The breaker is consulted at the top of {@code switchToNextRowGroup()}, which is only ever called
 * from inside the {@code try/catch(CairoException)} of {@code hasNext()}/{@code skipRows()}. That catch
 * wraps decode errors into a {@code nonCritical()} "likely corrupted" message, and {@code nonCritical()}
 * resets the interruption flag. Without the fix, a cancellation/timeout would surface as a corrupt-file
 * error with {@code isInterruption() == false}, so wire processors that branch on the interruption flag
 * (JSON/exp/validation endpoints) would misclassify it.
 */
public class ReadParquetCancellationTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // Force the non-parallel ReadParquetRecordCursor path where the regression lives.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "false");
        // Production throttle so the first breaker consultation in switchToNextRowGroup() performs a
        // real check (testCount == 0 after resetTimer()), even on the very first row group.
        final SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 2_000_000;
            }
        };
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, config) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
        super.setUp();
        inputRoot = root;
        ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);
    }

    @Test
    public void testCancelledScanKeepsCancellationFlagAndMessage() throws Exception {
        // An explicit CANCEL QUERY surfaces via queryCancelled(), which sets BOTH the interruption and
        // cancellation flags. After dropping `|| ex.isCancellation()` the parquet catch keys on the
        // interruption flag alone; queryCancelled() still sets it, so a genuine cancellation must keep
        // its classification/message and not be relabeled "Parquet file is likely corrupted".
        // We drive cancellation through a deterministic breaker toggle rather than a cancelledFlag,
        // because QueryRegistry resets the breaker's cancelledFlag to null around query execution.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 2_000_000;
            }
        };
        final NetworkSqlExecutionCircuitBreaker cancelBreaker = new NetworkSqlExecutionCircuitBreaker(engine, config) {
            @Override
            public void statefulThrowExceptionIfTripped() {
                if (armed.get()) {
                    throw CairoException.queryCancelled();
                }
                super.statefulThrowExceptionIfTripped();
            }

            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
        ((SqlExecutionContextImpl) sqlExecutionContext).with(cancelBreaker);
        try {
            assertMemoryLeak(() -> {
                final long rows = 10;
                execute("create table x as (select" +
                        " cast(x as int) id," +
                        " timestamp_sequence(0, 1_000_000) ts" +
                        " from long_sequence(" + rows + "))");

                try (
                        Path path = new Path();
                        PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                        TableReader reader = engine.getReader("x")
                ) {
                    path.of(root).concat("x.parquet").$();
                    PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                    PartitionEncoder.encode(partitionDescriptor, path);

                    sink.clear();
                    sink.put("select * from read_parquet('x.parquet')");

                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        // Disarmed while compiling so only the drain observes the cancellation.
                        try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                            armed.set(true);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                    // drain to force switchToNextRowGroup()
                                }
                                Assert.fail("expected the parquet scan to abort on an explicit cancellation");
                            } catch (CairoException e) {
                                // Must keep the cancellation classification and message, not be relabeled corrupt.
                                TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                                TestUtils.assertNotContains(e.getFlyweightMessage(), "Parquet file is likely corrupted");
                                Assert.assertTrue("expected interruption flag to survive the parquet catch", e.isInterruption());
                                Assert.assertTrue("expected cancellation flag to survive the parquet catch", e.isCancellation());
                            }
                        }
                    }
                }
            });
        } finally {
            ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);
            cancelBreaker.close();
        }
    }

    @Test
    public void testTimedOutScanKeepsInterruptionFlagAndMessage() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " cast(x as int) id," +
                    " timestamp_sequence(0, 1_000_000) ts" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);

                sink.clear();
                sink.put("select * from read_parquet('x.parquet')");

                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    // Disarm while compiling so only the drain observes the trip.
                    circuitBreaker.setTimeout(Long.MAX_VALUE);
                    circuitBreaker.resetTimer();
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        // Arm the breaker with a fresh throttle window plus an already-elapsed timeout,
                        // so the first real check inside switchToNextRowGroup() aborts.
                        circuitBreaker.resetTimer();
                        circuitBreaker.setTimeout(-1000);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                                // drain to force switchToNextRowGroup()
                            }
                            Assert.fail("expected the parquet scan to abort on a tripped circuit breaker");
                        } catch (CairoException e) {
                            // Must keep the timeout classification and message, not be relabeled corrupt.
                            TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
                            TestUtils.assertNotContains(e.getFlyweightMessage(), "Parquet file is likely corrupted");
                            Assert.assertTrue("expected interruption flag to survive the parquet catch", e.isInterruption());
                        }
                    }
                }
            }
        });
    }
}
