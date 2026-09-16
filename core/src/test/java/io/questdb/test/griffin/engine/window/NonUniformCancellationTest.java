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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class NonUniformCancellationTest extends AbstractCairoTest {
    private static final int ROWS = 8193;
    private static final DefaultSqlExecutionCircuitBreakerConfiguration BREAKER_CONFIGURATION = new DefaultSqlExecutionCircuitBreakerConfiguration() {
        @Override
        public int getCircuitBreakerThrottle() {
            return 2_000_000;
        }
    };

    @Test
    public void testBucketSparseSelection() throws Exception {
        for (String selection : List.of("lttb(v, $1)", "lttb(v, $1, '1s')", "m4(v, $1)", "minmax(v, $1)")) {
            assertBothCancellations(selection, Phase.COPY_SELECTED, Input.LINEAR);
        }
    }

    @Test
    public void testCadence() throws Exception {
        assertCadence("cadence($1)");
    }

    @Test
    public void testCadenceSeeded() throws Exception {
        // Seed 1 produces offset 1 for stride 2: selected positions never hit a 1024 mask.
        assertCadence("cadence($1, 1)");
    }

    @Test
    public void testLttbDenseSelection() throws Exception {
        assertBucket("lttb(v, $1)");
    }

    @Test
    public void testM4DenseSelection() throws Exception {
        assertBucket("m4(v, $1)");
    }

    @Test
    public void testMinMaxDenseSelection() throws Exception {
        assertBucket("minmax(v, $1)");
    }

    @Test
    public void testSdtDenseEnumeration() throws Exception {
        assertBothCancellations("sdt(v, 0.5)", Phase.COPY_SELECTED, Input.ZIGZAG);
    }

    @Test
    public void testSdtSparseEnumeration() throws Exception {
        assertBothCancellations("sdt(v, 0.5)", Phase.COPY_SELECTED, Input.LINEAR);
    }

    private void assertBothCancellations(String selection, Phase phase, Input input) throws Exception {
        for (int mode = 0; mode < 2; mode++) {
            for (int cancel = 0; cancel < 2; cancel++) {
                assertPhaseCancellation(selection, phase, input, mode == 0, cancel == 0);
            }
        }
    }

    private void assertBucket(String selection) throws Exception {
        assertBothCancellations(selection, Phase.GENERATE, Input.SINGLE_NULL);
        assertBothCancellations(selection, Phase.COPY_SELECTED, Input.SINGLE_NULL);
        assertBothCancellations(selection, Phase.COPY_ALL, Input.LINEAR);
        // Only the final row survives. Checking emitted-row counts would never interrupt this walk.
        assertBothCancellations(selection, Phase.COPY_SELECTED, Input.NULL_PREFIX);
    }

    private void assertCadence(String selection) throws Exception {
        for (Phase phase : List.of(Phase.GENERATE, Phase.COPY_SELECTED, Phase.COPY_ALL)) {
            assertBothCancellations(selection, phase, Input.LINEAR);
        }
    }

    private void assertPhaseCancellation(String selection, Phase phase, Input input, boolean isLight, boolean isPreCancelled) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
            execute("CREATE TABLE t AS (SELECT x::TIMESTAMP ts, x::DOUBLE v FROM long_sequence(" + ROWS + ")) TIMESTAMP(ts)");
            final boolean isCadence = selection.startsWith("cadence");
            if (isCadence) {
                bindVariableService.setLong(0, phase == Phase.COPY_ALL ? 1 : 2);
            } else {
                // A smaller bucket target on NULL-free input reaches the direct ordinal copy.
                bindVariableService.setLong(0, phase == Phase.COPY_SELECTED && input == Input.LINEAR ? ROWS / 2 : ROWS);
            }
            final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
            try (
                    CancellingBreaker breaker = new CancellingBreaker();
                    NetworkSqlExecutionCircuitBreaker replacement = new NetworkSqlExecutionCircuitBreaker(engine, BREAKER_CONFIGURATION)
            ) {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursorFactory factory = select("SELECT ts, v FROM t SUBSAMPLE " + selection)) {
                    final WindowFunction function = findSelector(factory);
                    MemoryTracker tracker;
                    try (
                            RecordCursor ignored = factory.getCursor(sqlExecutionContext);
                            DirectLongList dest = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true)
                    ) {
                        tracker = sqlExecutionContext.getMemoryTracker();
                        Assert.assertNotNull(tracker);
                        dest.setMemoryTracker(tracker);
                        dest.reopen();
                        // Isolate the real function's phases, including enumeration that the identity
                        // and non-LIGHT cursor paths skip. Executor checks must not hide missing checks.
                        final InputRecord record = new InputRecord(input);
                        for (int i = 0; i < ROWS; i++) {
                            record.row = i;
                            function.pass1(record, i, null);
                        }
                        if (phase != Phase.GENERATE) {
                            function.preparePass2();
                            Assert.assertEquals(phase == Phase.COPY_ALL, function.isSelectionAllRows());
                        }
                        final long memoryBefore = tracker.getUsed();
                        breaker.arm(isPreCancelled);
                        try {
                            if (phase == Phase.GENERATE) {
                                function.preparePass2();
                            } else {
                                function.getSelectedRows(dest);
                            }
                            Assert.fail("expected cancellation [selection=" + selection + ", phase=" + phase + ']');
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isCancellation());
                            Assert.assertEquals(isPreCancelled ? 1 : 2, breaker.checks);
                            final long maxProgress = isPreCancelled ? 0 : 1024;
                            final long growth = tracker.getUsed() - memoryBefore;
                            Assert.assertTrue("native allocation after cancellation: " + growth, growth <= maxProgress * Long.BYTES);
                            if (phase != Phase.GENERATE) {
                                Assert.assertTrue("enumerated rows: " + dest.size(), dest.size() <= maxProgress);
                                if (input == Input.NULL_PREFIX) {
                                    Assert.assertEquals(0, dest.size());
                                }
                            }
                        }
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, engine.getBusyReaderCount());

                    // The old breaker stays cancelled. Reuse the factory with a different breaker,
                    // different target/stride, and the table's linear input rather than partial state.
                    Assert.assertTrue(breaker.checkIfTripped());
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(replacement);
                    bindVariableService.setLong(0, isCadence ? ROWS : 2);
                    assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t1.0
                            1970-01-01T00:00:00.008193Z\t8193.0
                            """);
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
            }
            execute("DROP TABLE t");
        });
    }

    private static WindowFunction findSelector(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof CachedWindowLightRecordCursorFactory light) {
                Assert.assertEquals(1, light.getAllWindowFunctions().size());
                return light.getAllWindowFunctions().getQuick(0);
            }
            if (current instanceof CachedWindowRecordCursorFactory cached) {
                Assert.assertEquals(1, cached.getAllWindowFunctions().size());
                return cached.getAllWindowFunctions().getQuick(0);
            }
        }
        throw new AssertionError("expected a cached window factory");
    }

    private enum Input {
        LINEAR, SINGLE_NULL, NULL_PREFIX, ZIGZAG
    }

    private enum Phase {
        GENERATE, COPY_SELECTED, COPY_ALL
    }

    private static class CancellingBreaker extends NetworkSqlExecutionCircuitBreaker {
        private int checks;
        private boolean isArmed;

        CancellingBreaker() {
            super(engine, BREAKER_CONFIGURATION);
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            if (isArmed) {
                checks++;
            }
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            if (isArmed && checks == 1) {
                cancel();
            }
        }

        void arm(boolean isPreCancelled) {
            checks = 0;
            isArmed = true;
            if (isPreCancelled) {
                cancel();
            }
        }
    }

    private static class InputRecord implements Record {
        private final Input input;
        private int row;

        private InputRecord(Input input) {
            this.input = input;
        }

        @Override
        public double getDouble(int col) {
            return switch (input) {
                case SINGLE_NULL -> row == ROWS / 2 ? Double.NaN : row + 1;
                case NULL_PREFIX -> row < ROWS - 1 ? Double.NaN : row + 1;
                case ZIGZAG -> (row & 1) * 4.0;
                default -> row + 1;
            };
        }

        @Override
        public long getTimestamp(int col) {
            return row + 1;
        }
    }
}
