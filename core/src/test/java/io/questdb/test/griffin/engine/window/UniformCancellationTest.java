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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class UniformCancellationTest extends AbstractCairoTest {
    private static final int ROWS = 8193;
    private static final DefaultSqlExecutionCircuitBreakerConfiguration BREAKER_CONFIGURATION = new DefaultSqlExecutionCircuitBreakerConfiguration() {
        @Override
        public int getCircuitBreakerThrottle() {
            // Match the server default: a second count throttle inside the selector's
            // checkpoint would defer cancellation for millions of checkpoints.
            return 2_000_000;
        }
    };

    @Test
    public void testCopyAllRowsCancelledBeforeFirstIteration() throws Exception {
        assertPhaseCancellation(Phase.COPY_ALL, true);
    }

    @Test
    public void testCopyAllRowsCancelledMidLoop() throws Exception {
        assertPhaseCancellation(Phase.COPY_ALL, false);
    }

    @Test
    public void testCopySelectionCancelledBeforeFirstIteration() throws Exception {
        assertPhaseCancellation(Phase.COPY_SELECTED, true);
    }

    @Test
    public void testCopySelectionCancelledMidLoop() throws Exception {
        assertPhaseCancellation(Phase.COPY_SELECTED, false);
    }

    @Test
    public void testGenerationCancelledBeforeFirstIteration() throws Exception {
        assertPhaseCancellation(Phase.GENERATE, true);
    }

    @Test
    public void testGenerationCancelledMidLoop() throws Exception {
        assertPhaseCancellation(Phase.GENERATE, false);
    }

    private void assertPhaseCancellation(Phase phase, boolean isPreCancelled) throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, "true");
            execute("CREATE TABLE t AS (SELECT x::TIMESTAMP ts, x v FROM long_sequence(" + ROWS + ")) TIMESTAMP(ts)");
            bindVariableService.setLong(0, phase == Phase.COPY_ALL ? ROWS : ROWS - 1);
            final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
            try (
                    CancellingBreaker breaker = new CancellingBreaker();
                    NetworkSqlExecutionCircuitBreaker replacement = new NetworkSqlExecutionCircuitBreaker(engine, BREAKER_CONFIGURATION)
            ) {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursorFactory factory = select("SELECT ts, v FROM t SUBSAMPLE uniform($1)")) {
                    final WindowFunction function = findUniform(factory);
                    MemoryTracker tracker;
                    try (
                            RecordCursor ignored = factory.getCursor(sqlExecutionContext);
                            DirectLongList dest = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true)
                    ) {
                        tracker = sqlExecutionContext.getMemoryTracker();
                        Assert.assertNotNull(tracker);
                        dest.setMemoryTracker(tracker);
                        dest.reopen();
                        // Drive the real function's phases individually after the cursor initializes
                        // it. Later executor checks must not hide a missing check in either loop.
                        // The fused select-all path skips getSelectedRows(), so call it directly too.
                        for (int i = 0; i < ROWS; i++) {
                            function.pass1(null, i, null);
                        }
                        if (phase != Phase.GENERATE) {
                            function.preparePass2();
                            Assert.assertEquals(phase == Phase.COPY_ALL, function.isSelectionAllRows());
                        }
                        final long memoryBefore = tracker.getUsed();
                        breaker.arm(isPreCancelled);
                        try {
                            switch (phase) {
                                case GENERATE -> function.preparePass2();
                                case COPY_SELECTED, COPY_ALL -> function.getSelectedRows(dest);
                            }
                            Assert.fail("expected cancellation in " + phase);
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isCancellation());
                            Assert.assertEquals(isPreCancelled ? 1 : 2, breaker.checks);
                            final long maxRowsAfterCancel = isPreCancelled ? 0 : 1024;
                            final long growth = tracker.getUsed() - memoryBefore;
                            Assert.assertTrue("native allocation after cancellation: " + growth,
                                    growth <= maxRowsAfterCancel * Long.BYTES);
                            if (phase != Phase.GENERATE) {
                                Assert.assertEquals(maxRowsAfterCancel, dest.size());
                            }
                        }
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, engine.getBusyReaderCount());

                    // Keep the old breaker cancelled. Reopen the same factory with a different
                    // breaker and target to catch stale init bindings and partial selection state.
                    Assert.assertTrue(breaker.checkIfTripped());
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(replacement);
                    bindVariableService.setLong(0, 3);
                    assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t1
                            1970-01-01T00:00:00.004097Z\t4097
                            1970-01-01T00:00:00.008193Z\t8193
                            """);
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
            }
        });
    }

    private static WindowFunction findUniform(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof CachedWindowLightRecordCursorFactory light) {
                Assert.assertEquals(1, light.getAllWindowFunctions().size());
                final WindowFunction function = light.getAllWindowFunctions().getQuick(0);
                Assert.assertEquals("uniform", function.getName());
                return function;
            }
        }
        throw new AssertionError("expected a cached LIGHT window factory");
    }

    private enum Phase {
        GENERATE,
        COPY_SELECTED,
        COPY_ALL
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
                // Make cancellation pending just AFTER a successful check. The next checkpoint
                // must observe the real flag, without relying on timing or a second thread.
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
}
