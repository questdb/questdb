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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class SleepFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCancellationAtDeadlineIsNotSwallowed() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final QueryRegistry registry = engine.getQueryRegistry();
            final SqlExecutionCircuitBreaker previousCircuitBreaker = sqlExecutionContext.getCircuitBreaker();
            ((SqlExecutionContextImpl) sqlExecutionContext).with(new AtomicBooleanCircuitBreaker(engine));
            setCurrentMicros(0);
            try (RecordCursorFactory factory = select("sleep(0.1)")) {
                final long queryId = registry.register("sleep(0.1)", sqlExecutionContext);
                try {
                    final SuspendableSleepTask task = new SuspendableSleepTask(
                            factory.getBaseFactory(),
                            sqlExecutionContext
                    );
                    Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                    Assert.assertEquals(1, runtime.drain(1));
                    Assert.assertFalse(task.isDone());
                    Assert.assertEquals(1, runtime.getParkedFiberCount());

                    setCurrentMicros(200_000);
                    Assert.assertTrue(registry.cancel(queryId, sqlExecutionContext));
                    Assert.assertEquals(1, runtime.drain(1));

                    Assert.assertTrue(task.isDone());
                    Assert.assertNotNull("cancellation at the sleep deadline must fail the query", task.error);
                    Assert.assertTrue(task.error.getMessage(), task.error.getMessage().contains("cancel"));
                } finally {
                    registry.unregister(queryId, sqlExecutionContext);
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previousCircuitBreaker);
                setCurrentMicros(-1);
                close(runtime);
            }
        });
    }

    @Test
    public void testCancellationSignalWithoutTrippedCircuitBreakerAbortsSleep() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final SqlExecutionCircuitBreaker previousCircuitBreaker = sqlExecutionContext.getCircuitBreaker();
            ((SqlExecutionContextImpl) sqlExecutionContext).with(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            final FiberCancellationSignal cancellationSignal = new FiberCancellationSignal();
            try (RecordCursorFactory factory = select("sleep(60.0)")) {
                final SuspendableSleepTask task = new SuspendableSleepTask(
                        factory.getBaseFactory(),
                        sqlExecutionContext,
                        cancellationSignal
                );
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(task.isDone());
                Assert.assertEquals(1, runtime.getParkedFiberCount());

                cancellationSignal.cancel();
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertTrue(task.isDone());
                Assert.assertNotNull(task.error);
                Assert.assertTrue(task.error.getMessage(), task.error.getMessage().contains("cancel"));
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previousCircuitBreaker);
                close(runtime);
            }
        });
    }

    @Test
    public void testNegativeSeconds() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("sleep(-1.0)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("sleep duration must be"));
                }
            }
        });
    }

    @Test
    public void testInfiniteSeconds() throws Exception {
        assertMemoryLeak(() -> {
            // 1e308 * 1e10 overflows the double range to +Infinity, so the isInfinite
            // branch (not the 24 hour cap) rejects it.
            try (RecordCursorFactory factory = select("sleep(1e308 * 1e10)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("sleep duration must be"));
                }
            }
        });
    }

    @Test
    public void testNonFiniteSeconds() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("sleep(cast('NaN' as double))")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("sleep duration must be"));
                }
            }
        });
    }

    @Test
    public void testNullSuspensionScopeFallsBackToBlockingSleep() throws Exception {
        assertMemoryLeak(() -> {
            // An embedded caller's thread never enters a suspension scope; sleep() must
            // block the thread like the legacy path instead of failing the query.
            final SuspensionScope.Mode previousMode = SuspensionScope.enter(null);
            try (RecordCursorFactory factory = select("sleep(0.05)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.getRecord().getTimestamp(0) > 0);
                    Assert.assertFalse(cursor.hasNext());
                }
            } finally {
                SuspensionScope.restore(previousMode);
            }
        });
    }

    @Test
    public void testPinnedFiberFallsBackToBlockingSleep() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            try (RecordCursorFactory factory = select("sleep(0.02)")) {
                final PinnedSleepTask task = new PinnedSleepTask(factory, sqlExecutionContext);
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertNull(task.error);
                Assert.assertTrue(task.hasRow);
                Assert.assertTrue(task.isDone());
                Assert.assertTrue(runtime.getInlineSuspendViolationCount() > 0);
            } finally {
                close(runtime);
            }
        });
    }

    @Test
    public void testSubMillisecondSecondsRoundsToZeroAndSkipsSleep() throws Exception {
        assertMemoryLeak(() -> {
            // 0.0001s * 1000 = 0.1ms, truncated to 0 by the (long) cast, so the sleep is
            // skipped entirely and the call returns the current server time near-instantly.
            long start = System.nanoTime();
            try (RecordCursorFactory factory = select("sleep(0.0001)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    long ts = cursor.getRecord().getTimestamp(0);
                    Assert.assertTrue(ts > 0);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            Assert.assertTrue("sub-millisecond sleep should be near-instant, elapsed=" + elapsedMs + "ms",
                    elapsedMs < 200);
        });
    }

    @Test
    public void testSecondsExceedingMaximum() throws Exception {
        assertMemoryLeak(() -> {
            // 24 hours + 1 second, just over the 24 hour cap.
            try (RecordCursorFactory factory = select("sleep((24 * 60 * 60 + 1)::double)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("exceeds 24 hour maximum"));
                }
            }
        });
    }

    @Test
    public void testSecondsVeryLarge() throws Exception {
        assertMemoryLeak(() -> {
            // 1e15 seconds is finite and non-negative; * 1000 is 1e18 ms (well under Long.MAX_VALUE),
            // so the long conversion does not saturate. The cap check rejects it as exceeding 24h.
            try (RecordCursorFactory factory = select("sleep(1e15)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("exceeds 24 hour maximum"));
                }
            }
        });
    }

    @Test
    public void testSleepReturnsCurrentServerTime() throws Exception {
        assertMemoryLeak(() -> {
            long start = System.nanoTime();
            try (RecordCursorFactory factory = select("sleep(0.2)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    long ts = cursor.getRecord().getTimestamp(0);
                    Assert.assertTrue("returned timestamp must be positive: " + ts, ts > 0);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            Assert.assertTrue("did not sleep long enough, elapsed=" + elapsedMs + "ms",
                    elapsedMs >= 150);
        });
    }

    @Test
    public void testZeroSeconds() throws Exception {
        assertMemoryLeak(() -> {
            long start = System.nanoTime();
            try (RecordCursorFactory factory = select("sleep(0.0)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    long ts = cursor.getRecord().getTimestamp(0);
                    Assert.assertTrue(ts > 0);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            Assert.assertTrue("zero sleep should be near-instant, elapsed=" + elapsedMs + "ms",
                    elapsedMs < 200);
        });
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(8);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static class PinnedSleepTask extends FiberTask {
        private static final ThreadLocal<PinnedSleepTask> CURRENT_TASK = new ThreadLocal<>();
        private Throwable error;
        private final RecordCursorFactory factory;
        private boolean hasRow;
        private final Object monitor = new Object();
        private final SqlExecutionContext sqlExecutionContext;

        private PinnedSleepTask(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext) {
            this.factory = factory;
            this.sqlExecutionContext = sqlExecutionContext;
        }

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            CURRENT_TASK.set(this);
            try {
                PinnedSleepTaskInitializer.initialize();
            } finally {
                CURRENT_TASK.remove();
            }
            return true;
        }

        private void runPinned() {
            synchronized (monitor) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    hasRow = cursor.hasNext();
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }

    private static class SuspendableSleepTask extends FiberTask {
        private final @Nullable FiberCancellationSignal cancellationSignal;
        private Throwable error;
        private final RecordCursorFactory factory;
        private final SqlExecutionContext sqlExecutionContext;

        private SuspendableSleepTask(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext) {
            this(factory, sqlExecutionContext, null);
        }

        private SuspendableSleepTask(
                RecordCursorFactory factory,
                SqlExecutionContext sqlExecutionContext,
                @Nullable FiberCancellationSignal cancellationSignal
        ) {
            this.cancellationSignal = cancellationSignal;
            this.factory = factory;
            this.sqlExecutionContext = sqlExecutionContext;
        }

        @Override
        public @Nullable FiberCancellationSignal getCancellationSignal() {
            return cancellationSignal;
        }

        @Override
        protected boolean runStep() {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                cursor.hasNext();
            } catch (Throwable th) {
                error = th;
            }
            return true;
        }
    }

    private static class PinnedSleepTaskInitializer {
        static {
            PinnedSleepTask.CURRENT_TASK.get().runPinned();
        }

        private static void initialize() {
        }
    }
}
