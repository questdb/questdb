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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.log.LogWriter;
import io.questdb.log.LogWriterConfig;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// Verifies the query-progress logging/metric behavior: validation failures must not
// be counted as server-side query errors, reader leaks must still be counted even
// during validation, and only interruptions may skip a full log queue.
public class QueryProgressValidationTest extends AbstractCairoTest {

    @Test
    public void testCancelledQueryDoesNotWaitForFullLogQueue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x::INT v FROM long_sequence(1_000))");
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            final SqlExecutionCircuitBreaker previousBreaker = ctx.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine) {
                @Override
                public boolean checkIfTripped() {
                    cancel();
                    return super.checkIfTripped();
                }

                @Override
                public int getState() {
                    cancel();
                    return super.getState();
                }

                @Override
                public void statefulThrowExceptionIfTrippedNoThrottle() {
                    cancel();
                    super.statefulThrowExceptionIfTrippedNoThrottle();
                }
            };
            ctx.with(breaker);
            try (
                    RecordCursorFactory factory = select("SELECT * FROM x WHERE v > 0");
                    FullLogQueue queue = new FullLogQueue()
            ) {
                final long errors = engine.getMetrics().healthMetrics().queryErrorCounter();
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final boolean isWaiting = queue.waitsForSlot(() -> {
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        cursor.hasNext();
                    } catch (Throwable th) {
                        failure.set(th);
                    }
                });
                Assert.assertFalse("a cancelled query waited for a log queue slot", isWaiting);
                queue.awaitDone();

                Assert.assertTrue(String.valueOf(failure.get()), failure.get() instanceof CairoException e && e.isCancellation());
                TestUtils.assertContains(((CairoException) failure.get()).getFlyweightMessage(), "cancelled by user");
                Assert.assertEquals(errors + 1, engine.getMetrics().healthMetrics().queryErrorCounter());
                Assert.assertEquals(queue.getDepth(), queue.drain().size());
            } finally {
                ctx.with(previousBreaker);
            }
        });
    }

    @Test
    public void testErrorWaitsForFullLogQueue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT)");
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            for (int kind = 0; kind < 3; kind++) {
                try (
                        TableReader reader = engine.getReader("x");
                        FullLogQueue queue = new FullLogQueue()
                ) {
                    final ObjList<TableReader> leaked = new ObjList<>();
                    final Throwable error;
                    final String expected;
                    switch (kind) {
                        case 0 -> {
                            error = CairoException.nonCritical().put("boom");
                            expected = "err [id=7, sql=`SELECT 'failed'`";
                        }
                        case 1 -> {
                            error = new RuntimeException("boom");
                            expected = "exception=java.lang.RuntimeException: boom";
                        }
                        default -> {
                            error = CairoException.queryCancelled();
                            leaked.add(reader);
                            expected = "brk [id=7, sql=`SELECT 'failed'`";
                        }
                    }
                    final boolean isWaiting = queue.waitsForSlot(() -> QueryProgress.logError(
                            error,
                            7,
                            "SELECT 'failed'",
                            ctx,
                            0,
                            leaked.size() > 0 ? leaked : null
                    ));
                    Assert.assertTrue("kind=" + kind + " did not wait for a log queue slot", isWaiting);
                    queue.freeSlot();
                    queue.awaitDone();

                    final ObjList<String> records = queue.drain();
                    Assert.assertEquals(queue.getDepth(), records.size());
                    TestUtils.assertContains(records.getLast(), expected);
                }
            }
        });
    }

    @Test
    public void testInterruptionDoesNotWaitForFullLogQueue() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            final String[] messages = {"cancelled by user", "timeout, query aborted", "remote disconnected, query aborted"};
            for (int kind = 0; kind < messages.length; kind++) {
                try (FullLogQueue queue = new FullLogQueue()) {
                    final long errors = engine.getMetrics().healthMetrics().queryErrorCounter();
                    final CairoException interruption = newInterruption(kind);
                    final boolean isWaiting = queue.waitsForSlot(
                            () -> QueryProgress.logError(interruption, 5, "SELECT 'interrupted'", ctx, 0)
                    );
                    Assert.assertFalse("kind=" + kind + " waited for a log queue slot", isWaiting);
                    queue.awaitDone();
                    Assert.assertEquals(errors + 1, engine.getMetrics().healthMetrics().queryErrorCounter());
                    Assert.assertEquals(queue.getDepth(), queue.drain().size());

                    QueryProgress.logError(newInterruption(kind), 6, "SELECT 'interrupted'", ctx, 0);
                    final ObjList<String> records = queue.drain();
                    Assert.assertEquals(1, records.size());
                    TestUtils.assertContains(records.getQuick(0), "err [id=6, sql=`SELECT 'interrupted'`");
                    TestUtils.assertContains(records.getQuick(0), "msg=" + messages[kind]);
                }
            }
        });
    }

    @Test
    public void testNonValidationFailureIncrementsQueryErrorMetric() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            final long before = engine.getMetrics().healthMetrics().queryErrorCounter();

            ctx.setValidationOnly(false);
            QueryProgress.logError(
                    CairoException.nonCritical().put("boom"),
                    -1,
                    "select 1",
                    ctx,
                    0
            );

            Assert.assertEquals(before + 1, engine.getMetrics().healthMetrics().queryErrorCounter());
        });
    }

    @Test
    public void testShouldLogSqlReflectsValidationOnly() {
        final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
        try {
            ctx.setValidationOnly(true);
            Assert.assertFalse(ctx.shouldLogSql());
            ctx.setValidationOnly(false);
            Assert.assertTrue(ctx.shouldLogSql());
        } finally {
            ctx.setValidationOnly(false);
        }
    }

    @Test
    public void testValidationFailureDoesNotIncrementQueryErrorMetric() throws Exception {
        assertMemoryLeak(() -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            final long before = engine.getMetrics().healthMetrics().queryErrorCounter();

            ctx.setValidationOnly(true);
            try {
                QueryProgress.logError(
                        CairoException.nonCritical().put("boom"),
                        -1,
                        "select 1",
                        ctx,
                        0
                );
            } finally {
                ctx.setValidationOnly(false);
            }

            Assert.assertEquals(before, engine.getMetrics().healthMetrics().queryErrorCounter());
        });
    }

    @Test
    public void testValidationReaderLeakStillCounted() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT)");

            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            final long before = engine.getMetrics().healthMetrics().readerLeakCounter();

            ctx.setValidationOnly(true);
            try (TableReader reader = engine.getReader("x")) {
                final ObjList<TableReader> leaked = new ObjList<>();
                leaked.add(reader);
                QueryProgress.logEnd(-1, "select * from x", ctx, 0, leaked, null);
            } finally {
                ctx.setValidationOnly(false);
            }

            Assert.assertEquals(before + 1, engine.getMetrics().healthMetrics().readerLeakCounter());
        });
    }

    private static CairoException newInterruption(int kind) {
        return switch (kind) {
            case 0 -> CairoException.queryCancelled();
            case 1 -> CairoException.queryTimedOut();
            default -> CairoException.queryDisconnected(-1);
        };
    }

    private static class FullLogQueue implements QuietCloseable {
        private final LogFactory factory = new LogFactory();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Log previous;
        private final AtomicReference<RingQueue<LogRecordUtf8Sink>> ring = new AtomicReference<>();
        private final AtomicReference<SCSequence> sequence = new AtomicReference<>();
        private Thread thread;

        FullLogQueue() {
            try {
                factory.add(new LogWriterConfig(LogLevel.ALL, (r, s, level) -> {
                    ring.set(r);
                    sequence.set(s);
                    return new LogWriter() {
                        @Override
                        public void bindProperties(LogFactory factory) {
                        }

                        @Override
                        public boolean run(@NotNull WorkerContext workerContext) {
                            return false;
                        }
                    };
                }));
                factory.bind();
                final Log filler = factory.create("filler", false);
                for (int i = 0, n = factory.getQueueDepth(); i < n; i++) {
                    filler.info().$("filler ").$(i).$();
                }
                previous = QueryProgress.swapLogForTesting(factory.create(QueryProgress.class.getName(), false));
            } catch (Throwable th) {
                factory.close();
                throw th;
            }
        }

        void awaitDone() throws InterruptedException {
            thread.join(TimeUnit.SECONDS.toMillis(60));
            Assert.assertFalse("logging thread did not finish", thread.isAlive());
            if (failure.get() != null) {
                throw new AssertionError("logging thread failed", failure.get());
            }
        }

        @Override
        public void close() {
            try {
                drain();
                if (thread != null) {
                    thread.join(TimeUnit.SECONDS.toMillis(60));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                QueryProgress.swapLogForTesting(previous);
                factory.close();
            }
        }

        ObjList<String> drain() {
            final ObjList<String> records = new ObjList<>();
            final SCSequence seq = sequence.get();
            long cursor;
            while ((cursor = seq.next()) > -1) {
                records.add(ring.get().get(cursor).toString());
                seq.done(cursor);
            }
            return records;
        }

        void freeSlot() {
            final SCSequence seq = sequence.get();
            final long cursor = seq.next();
            Assert.assertTrue(cursor > -1);
            seq.done(cursor);
        }

        int getDepth() {
            return factory.getQueueDepth();
        }

        boolean waitsForSlot(Runnable task) {
            thread = new Thread(() -> {
                try {
                    task.run();
                } catch (Throwable th) {
                    failure.set(th);
                }
            }, "full-log-queue");
            thread.start();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (thread.isAlive()) {
                boolean isClaimingSlot = false;
                for (StackTraceElement frame : thread.getStackTrace()) {
                    if ("nextBully".equals(frame.getMethodName())) {
                        isClaimingSlot = true;
                    } else if (isClaimingSlot && "logError".equals(frame.getMethodName())
                            && QueryProgress.class.getName().equals(frame.getClassName())) {
                        return true;
                    }
                }
                Assert.assertTrue("logging thread neither finished nor waited for a slot", System.nanoTime() < deadline);
                Os.pause();
            }
            return false;
        }
    }
}
