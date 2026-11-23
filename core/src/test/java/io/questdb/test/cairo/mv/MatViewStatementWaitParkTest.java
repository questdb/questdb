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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RESUME, ALTER ... WITH BASE, and a REFRESH or a view ADD COLUMN issued right after ALTER ... AS, run as a fiber
 * task, park their fiber while they wait for the view latch, the view's WAL or the view's new definition: the carrier
 * that drains the runtime returns while the statement is unfinished, and the statement completes on a later drain, on
 * any carrier. The park wakes on a 1 ms timer; a fiber that cannot park sleeps instead, and a shut down runtime or
 * timer aborts the statement with nothing written. A view ADD COLUMN holds the view's sequencer metadata while it
 * waits, and returns it on whatever carrier the statement ends.
 * <p>
 * At a statement that holds its carrier, the drain does not return while the test holds the blocker, so a watchdog
 * releases the blocker after {@link #WATCHDOG_MILLIS} and the test fails.
 */
public class MatViewStatementWaitParkTest extends AbstractCairoTest {
    private static final String BASE_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;
    // a view ADD COLUMN of the column x that X_SQL produces, which waits for an ALTER ... AS to X_SQL
    private static final String ADD_X = "ALTER MATERIALIZED VIEW v ADD COLUMN x DOUBLE";
    // the position of the view name in ALTER MATERIALIZED VIEW statements
    private static final int ALTER_VIEW_NAME_POSITION = "ALTER MATERIALIZED VIEW ".length();
    // the refusal of a view ALTER that waits for the definition event of an ALTER ... AS in the suspended WAL of v
    private static final String DEFINITION_NOT_APPLIED = "materialized view WAL is suspended with an earlier ALTER MATERIALIZED VIEW ... AS not applied, ";
    private static final Log LOG = LogFactory.getLog(MatViewStatementWaitParkTest.class);
    private static final String NEW_SQL = "SELECT ts, sym, max(price) * 10 AS price FROM base SAMPLE BY 1h";
    private static final String RESUME = "REFRESH MATERIALIZED VIEW v RESUME SKIP BACKLOG";
    // the position of the view name in RESUME, REFRESH and ALTER ... AS statements
    private static final int REFRESH_VIEW_NAME_POSITION = "REFRESH MATERIALIZED VIEW ".length();
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
    private static final String VIEW_SQL_BASE2 = "SELECT ts, sym, last(price) AS price FROM base2 SAMPLE BY 1h";
    private static final long WATCHDOG_MILLIS = 30_000;
    // a SQL that also produces the column x, the average price of the hour, which the view lacks
    private static final String X_SQL = "SELECT ts, sym, last(price) AS price, avg(price) AS x FROM base SAMPLE BY 1h";
    // The timers that the engine hands out when set, see setUpStatic().
    private static volatile TimerShards timerShardsOverride;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public TimerShards getTimerShards() {
                final TimerShards override = timerShardsOverride;
                return override != null ? override : super.getTimerShards();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAddColumnParkedOnPendingDefinitionContinuesOnAnotherCarrier() throws Exception {
        // A view ADD COLUMN right after ALTER ... AS parks on carrier A while the definition event waits in the view's
        // WAL, holding the view's sequencer metadata, and completes on carrier B once the event applied: it checks the
        // new SQL there and returns the compiler and the metadata.
        assertMemoryLeak(() -> {
            createViewWithPendingDefinition();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            final AtomicReference<Thread> waitCarrier = new AtomicReference<>();
            state.setOnDefinitionWaitForTesting(() -> waitCarrier.set(Thread.currentThread()));
            final FiberRuntime runtime = new FiberRuntime(1);
            try (
                    SequencerMetadataCounter metadataCounter = new SequencerMetadataCounter(engine, viewToken());
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(AbstractCairoTest::drainWalQueue)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(ADD_X, context);
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));

                // carrier A runs the statement until it parks, and stops draining
                final AtomicInteger carrierAMounts = new AtomicInteger();
                final Thread carrierA = new Thread(() -> {
                    runtime.initializeCarrier();
                    try {
                        carrierAMounts.set(runtime.drain(1));
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "carrier-a");
                carrierA.start();
                carrierA.join(2 * WATCHDOG_MILLIS);
                Assert.assertFalse(carrierA.isAlive());
                watchdog.assertNotFired();
                Assert.assertEquals(1, carrierAMounts.get());
                Assert.assertFalse("the statement must still wait", task.isFinished);
                Assert.assertSame(carrierA, waitCarrier.get());
                Assert.assertEquals(1, metadataCounter.get());
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                drainWalQueue();
                // carrier B resumes the statement, which checks the new SQL there
                final Thread carrierB = new Thread(() -> {
                    runtime.initializeCarrier();
                    try {
                        drainUntilFinished(runtime, task);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "carrier-b");
                carrierB.start();
                carrierB.join(2 * WATCHDOG_MILLIS);
                Assert.assertFalse(carrierB.isAlive());
                Assert.assertTrue(task.isFinished);
                Assert.assertNull(task.error);
                Assert.assertSame(carrierB, task.finishingThread);
                Assert.assertEquals(0, metadataCounter.get());
            } finally {
                state.setOnDefinitionWaitForTesting(null);
                close(runtime);
            }
            Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount());
            drainWalQueue();
            assertViewColumns("ts", "sym", "price", "x");
            execute("INSERT INTO base VALUES ('a', 3.0, '2024-01-01T00:20:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
            drainWalAndMatViewQueues();
            // the refresh recomputes the 00:00 bucket only: avg(1.0, 3.0) = 2.0
            assertViewRows("""
                    ts\tsym\tprice\tx
                    2024-01-01T00:00:00.000000Z\ta\t3.0\t2.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    """);
        });
    }

    @Test
    public void testAlterWithBaseParksWhileLatchBusy() throws Exception {
        assertMemoryLeak(() -> {
            createBasesWithView("IMMEDIATE");
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            final String alter = "ALTER MATERIALIZED VIEW v WITH BASE base2 AS (" + VIEW_SQL_BASE2 + ")";
            final LatchHolder latch = new LatchHolder(state);
            final FiberRuntime runtime = new FiberRuntime(1);
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(latch::release)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(alter, context);
                assertParks(runtime, task, watchdog);
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());

                latch.release();
                drainUntilFinished(runtime, task);
                watchdog.assertNotFired();
                Assert.assertNull(task.error);
            } finally {
                latch.release();
                close(runtime);
            }
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals("base2", engine.getDependentViewGraph().getViewDefinition(viewToken()).getBaseTableName());
            assertViewRows(BASE_ROWS);
            Assert.assertFalse(state.isLocked());
        });
    }

    @Test
    public void testFullRefreshParksWhileDefinitionPending() throws Exception {
        assertRefreshParksWhileDefinitionPending("REFRESH MATERIALIZED VIEW v FULL", "\tb\t20.0");
    }

    @Test
    public void testIncrementalRefreshParksWhileDefinitionPending() throws Exception {
        // INCREMENTAL recomputes the bucket of the row the view has not refreshed only
        assertRefreshParksWhileDefinitionPending("REFRESH MATERIALIZED VIEW v INCREMENTAL", "\tb\t2.0");
    }

    @Test
    public void testParkedResumeContinuesOnAnotherCarrier() throws Exception {
        assertMemoryLeak(() -> {
            createInvalidView();
            final MatViewState state = viewState();
            final LatchHolder latch = new LatchHolder(state);
            final FiberRuntime runtime = new FiberRuntime(1);
            final AtomicReference<Thread> latchBusyCarrier = new AtomicReference<>();
            final AtomicReference<Thread> holdingLockCarrier = new AtomicReference<>();
            state.setOnResumeLatchBusyForTesting(() -> latchBusyCarrier.set(Thread.currentThread()));
            state.setOnResumeHoldingLockForTesting(() -> holdingLockCarrier.set(Thread.currentThread()));
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(latch::release)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(RESUME, context);
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));

                // carrier A runs the statement until it parks, and stops draining
                final AtomicInteger carrierAMounts = new AtomicInteger();
                final Thread carrierA = new Thread(() -> {
                    runtime.initializeCarrier();
                    try {
                        carrierAMounts.set(runtime.drain(1));
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "carrier-a");
                carrierA.start();
                carrierA.join(2 * WATCHDOG_MILLIS);
                Assert.assertFalse(carrierA.isAlive());
                watchdog.assertNotFired();
                Assert.assertEquals(1, carrierAMounts.get());
                Assert.assertFalse("the statement must still wait", task.isFinished);
                Assert.assertSame(carrierA, latchBusyCarrier.get());

                latch.release();
                // carrier B resumes the statement, which takes the latch there
                final Thread carrierB = new Thread(() -> {
                    runtime.initializeCarrier();
                    try {
                        drainUntilFinished(runtime, task);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "carrier-b");
                carrierB.start();
                carrierB.join(2 * WATCHDOG_MILLIS);
                Assert.assertFalse(carrierB.isAlive());
                Assert.assertTrue(task.isFinished);
                Assert.assertNull(task.error);
                Assert.assertSame(carrierB, holdingLockCarrier.get());
            } finally {
                state.setOnResumeLatchBusyForTesting(null);
                state.setOnResumeHoldingLockForTesting(null);
                latch.release();
                close(runtime);
            }
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            assertResumedOnBase2();
        });
    }

    @Test
    public void testParkedWaitWakesOncePerMillisecond() throws Exception {
        // The configuration clock stands still, so the 1 ms timer of the parked wait fires only once the test moves
        // the clock by a millisecond: one more pass of the wait per millisecond, as the 1 ms sleeps of a statement
        // off a fiber.
        assertMemoryLeak(() -> {
            createInvalidView();
            execute("ALTER MATERIALIZED VIEW v ADD COLUMN extra DOUBLE");
            final MatViewState state = viewState();
            final AtomicInteger passCount = new AtomicInteger();
            final Runnable passCounter = new Runnable() {
                @Override
                public void run() {
                    passCount.incrementAndGet();
                    state.setOnResumeWalWaitForTesting(this);
                }
            };
            state.setOnResumeWalWaitForTesting(passCounter);
            final FiberRuntime runtime = new FiberRuntime(1);
            final long frozenMicros = MicrosecondClockImpl.INSTANCE.getTicks();
            setCurrentMicros(frozenMicros);
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(AbstractCairoTest::drainWalQueue)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(RESUME, context);
                assertParks(runtime, task, watchdog);
                Assert.assertEquals(1, passCount.get());
                for (int i = 0; i < 100; i++) {
                    runtime.drain(1);
                }
                Assert.assertEquals(1, passCount.get());

                setCurrentMicros(frozenMicros + 1_000);
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (passCount.get() < 2 && System.nanoTime() < deadline) {
                    runtime.drain(1);
                    Os.pause();
                }
                Assert.assertEquals(2, passCount.get());
                for (int i = 0; i < 100; i++) {
                    runtime.drain(1);
                }
                Assert.assertEquals(2, passCount.get());
                Assert.assertFalse("the statement must still wait", task.isFinished);

                setCurrentMicros(-1);
                drainWalQueue();
                drainUntilFinished(runtime, task);
                watchdog.assertNotFired();
                Assert.assertNull(task.error);
            } finally {
                setCurrentMicros(-1);
                state.setOnResumeWalWaitForTesting(null);
                close(runtime);
            }
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    """);
        });
    }

    @Test
    public void testRangeRefreshParksWhileDefinitionPending() throws Exception {
        assertRefreshParksWhileDefinitionPending(
                "REFRESH MATERIALIZED VIEW v RANGE FROM '2024-01-01' TO '2024-01-01T02:00:00.000000Z'",
                "\tb\t20.0"
        );
    }

    @Test
    public void testRefusedParkSleepsAndResumes() throws Exception {
        // A continuation pinned by a class initializer cannot park: the wait sleeps on its carrier, and the statement
        // completes once the latch frees up.
        assertMemoryLeak(() -> {
            createInvalidView();
            final MatViewState state = viewState();
            final LatchHolder latch = new LatchHolder(state);
            final FiberRuntime runtime = new FiberRuntime(1);
            final CountDownLatch isWaiting = new CountDownLatch(1);
            state.setOnResumeLatchBusyForTesting(isWaiting::countDown);
            final AtomicBoolean hasSeenRefusedPark = new AtomicBoolean();
            // The test thread drains the runtime, so another thread frees the latch once the statement waits and a
            // park of it was refused.
            final Thread releaser = new Thread(() -> {
                try {
                    if (isWaiting.await(WATCHDOG_MILLIS, TimeUnit.MILLISECONDS)) {
                        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WATCHDOG_MILLIS);
                        while (runtime.getInlineSuspendViolationCount() == 0 && System.nanoTime() < deadline) {
                            Os.pause();
                        }
                        hasSeenRefusedPark.set(runtime.getInlineSuspendViolationCount() > 0);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.release();
                }
            }, "latch-releaser");
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker()
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final PinnedStatementTask task = new PinnedStatementTask(RESUME, context);
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                releaser.start();
                Assert.assertEquals(1, runtime.drain(1));
                releaser.join(2 * WATCHDOG_MILLIS);
                Assert.assertFalse(releaser.isAlive());
                Assert.assertTrue("the statement must try to park", hasSeenRefusedPark.get());
                Assert.assertTrue(task.isFinished);
                Assert.assertNull(task.error);
            } finally {
                state.setOnResumeLatchBusyForTesting(null);
                latch.release();
                close(runtime);
            }
            Assert.assertFalse(state.isLocked());
            assertResumedOnBase2();
        });
    }

    @Test
    public void testRefusedTimerAbortsResume() throws Exception {
        // The engine's timers stopped, while the engine is not closing: the statement cannot park and aborts
        // instead of sleeping on its carrier, as on a closing server.
        assertAbortsWithNothingWritten((runtime, state) -> {
            final TimerShards stoppedTimers = new TimerShards(1, "test-stopped-timer", LOG);
            stoppedTimers.start();
            stoppedTimers.shutdown();
            timerShardsOverride = stoppedTimers;
        });
    }

    @Test
    public void testRefusedWaitBuildAbortsResume() throws Exception {
        // The runtime starts to shut down when the statement finds the latch busy: its fiber cannot build a wait, and
        // the statement aborts instead of sleeping on its carrier.
        assertAbortsWithNothingWritten((runtime, state) -> state.setOnResumeLatchBusyForTesting(runtime::beginQuiesce));
    }

    @Test
    public void testResumeParksWhileLatchBusy() throws Exception {
        assertMemoryLeak(() -> {
            createInvalidView();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            final LatchHolder latch = new LatchHolder(state);
            final FiberRuntime runtime = new FiberRuntime(1);
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(latch::release)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(RESUME, context);
                assertParks(runtime, task, watchdog);
                Assert.assertEquals(viewSeqTxn, viewSeqTxn());
                Assert.assertTrue(state.isInvalid());

                latch.release();
                drainUntilFinished(runtime, task);
                watchdog.assertNotFired();
                Assert.assertNull(task.error);
            } finally {
                latch.release();
                close(runtime);
            }
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            assertResumedOnBase2();
        });
    }

    @Test
    public void testResumeParksWhileViewWalUnapplied() throws Exception {
        assertMemoryLeak(() -> {
            createInvalidView();
            // a view WAL transaction that RESUME waits for
            execute("ALTER MATERIALIZED VIEW v ADD COLUMN extra DOUBLE");
            final FiberRuntime runtime = new FiberRuntime(1);
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(AbstractCairoTest::drainWalQueue)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(RESUME, context);
                assertParks(runtime, task, watchdog);
                Assert.assertTrue(viewState().isInvalid());

                drainWalQueue();
                drainUntilFinished(runtime, task);
                watchdog.assertNotFired();
                Assert.assertNull(task.error);
            } finally {
                close(runtime);
            }
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t2.0\tnull
                    """);
        });
    }

    @Test
    public void testShutdownWakeAbortsAddColumnParkedOnPendingDefinition() throws Exception {
        // The configuration clock stands still, so the timer does not wake the parked view ADD COLUMN after the test
        // applies the definition event; the runtime's shutdown does. The statement aborts instead of checking the new
        // SQL, writes nothing, and returns the compiler and the view's sequencer metadata.
        assertMemoryLeak(() -> {
            createViewWithPendingDefinition();
            final long viewSeqTxn = viewSeqTxn();
            final FiberRuntime runtime = new FiberRuntime(1);
            setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
            try (
                    SequencerMetadataCounter metadataCounter = new SequencerMetadataCounter(engine, viewToken());
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(AbstractCairoTest::drainWalQueue)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(ADD_X, context);
                assertParks(runtime, task, watchdog);
                Assert.assertEquals(1, metadataCounter.get());

                drainWalQueue();
                Assert.assertEquals(X_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken()).getMatViewSql());
                runtime.beginQuiesce();
                drainUntilFinished(runtime, task);
                watchdog.assertNotFired();
                assertServerClosing(task.error);
                Assert.assertEquals(0, metadataCounter.get());
            } finally {
                setCurrentMicros(-1);
                close(runtime);
            }
            Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount());
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            drainWalQueue();
            assertViewColumns("ts", "sym", "price");

            execute(ADD_X);
            drainWalQueue();
            assertViewColumns("ts", "sym", "price", "x");
        });
    }

    @Test
    public void testShutdownWakeAbortsParkedResume() throws Exception {
        // The configuration clock stands still, so the timer does not wake the parked statement after the test frees
        // the latch; the runtime's shutdown does. The statement aborts instead of taking the free latch.
        assertMemoryLeak(() -> {
            createInvalidView();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            final LatchHolder latch = new LatchHolder(state);
            final FiberRuntime runtime = new FiberRuntime(1);
            setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(latch::release)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(RESUME, context);
                assertParks(runtime, task, watchdog);

                latch.release();
                runtime.beginQuiesce();
                drainUntilFinished(runtime, task);
                watchdog.assertNotFired();
                assertServerClosing(task.error);
            } finally {
                setCurrentMicros(-1);
                latch.release();
                close(runtime);
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            Assert.assertTrue(state.isInvalid());

            execute(RESUME);
            assertResumedOnBase2();
        });
    }

    @Test
    public void testViewDroppedWhileAddColumnParkedOnPendingDefinition() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithPendingDefinition();
            assertAddColumnStopsAfterWake(
                    () -> execute("DROP MATERIALIZED VIEW v"),
                    "materialized view does not exist [view=v]"
            );
        });
    }

    @Test
    public void testViewDroppedWhileResumeParked() throws Exception {
        assertStopsAfterWake(
                "DROP MATERIALIZED VIEW v",
                "materialized view does not exist [view=v]"
        );
    }

    @Test
    public void testViewListedAndDroppedWhileAddColumnParkedOnPendingDefinition() throws Exception {
        // A dropped view does not exist, whatever stops the apply of its WAL: the statement says so, and does not name
        // the list entry.
        assertMemoryLeak(() -> {
            createViewWithPendingDefinition();
            final String dirName = viewToken().getDirName();
            try {
                assertAddColumnStopsAfterWake(
                        () -> {
                            setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, dirName);
                            execute("DROP MATERIALIZED VIEW v");
                        },
                        "materialized view does not exist [view=v]"
                );
            } finally {
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
        });
    }

    @Test
    public void testViewListedWhileAddColumnParkedOnPendingDefinition() throws Exception {
        // The parked statement reads the cairo.wal.apply.suspended.tables list on each pass of its wait, not only when
        // it starts. Once the list no longer holds the view, its WAL applies the definition event, and ADD COLUMN takes
        // the column the new SQL produces.
        assertMemoryLeak(() -> {
            createViewWithPendingDefinition();
            final String dirName = viewToken().getDirName();
            try {
                assertAddColumnStopsAfterWake(
                        () -> setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, dirName),
                        DEFINITION_NOT_APPLIED + "remove " + dirName
                                + " from cairo.wal.apply.suspended.tables and run ALTER MATERIALIZED VIEW v RESUME WAL first"
                );
            } finally {
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
            drainWalQueue();
            Assert.assertEquals(X_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken()).getMatViewSql());
            execute(ADD_X);
            drainWalQueue();
            assertViewColumns("ts", "sym", "price", "x");
        });
    }

    @Test
    public void testViewWalSuspendedWhileAddColumnParkedOnPendingDefinition() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithPendingDefinition();
            assertAddColumnStopsAfterWake(
                    () -> execute("ALTER MATERIALIZED VIEW v SUSPEND WAL"),
                    DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW v RESUME WAL first"
            );
        });
    }

    @Test
    public void testViewWalSuspendedWhileRangeParkedOnPendingDefinition() throws Exception {
        // A REFRESH ... RANGE parked on the definition event refuses at its next wake once the view's WAL is suspended,
        // as a view ALTER does: its bounds cannot wait in the refresh job.
        assertMemoryLeak(() -> {
            createViewWithPendingDefinition();
            assertParkedStatementStopsAfterWake(
                    "REFRESH MATERIALIZED VIEW v RANGE FROM '2024-01-01' TO '2024-01-01T02:00:00.000000Z'",
                    REFRESH_VIEW_NAME_POSITION,
                    0,
                    () -> execute("ALTER MATERIALIZED VIEW v SUSPEND WAL"),
                    DEFINITION_NOT_APPLIED + "run ALTER MATERIALIZED VIEW v RESUME WAL first"
            );
        });
    }

    @Test
    public void testViewWalSuspendedWhileResumeParked() throws Exception {
        assertStopsAfterWake(
                "ALTER MATERIALIZED VIEW v SUSPEND WAL",
                "materialized view WAL is suspended, run ALTER MATERIALIZED VIEW v RESUME WAL first"
        );
    }

    // A view ADD COLUMN on the view of createViewWithPendingDefinition() parks while the definition event waits in the
    // view's WAL; the action then runs on the test thread, and the parked statement stops at its next wake with the
    // refusal at the view name. It writes nothing, and returns the compiler and the view's sequencer metadata, which
    // it holds while it waits.
    private void assertAddColumnStopsAfterWake(ParkedAction action, String refusal) throws Exception {
        assertParkedStatementStopsAfterWake(ADD_X, ALTER_VIEW_NAME_POSITION, 1, action, refusal);
    }

    // The statement, a view ALTER or a REFRESH ... RANGE on the view of createViewWithPendingDefinition(), parks while
    // the definition event waits in the view's WAL; the action then runs on the test thread, and the parked statement
    // stops at its next wake with the refusal at the view name. It writes nothing, enqueues no refresh, and returns the
    // compiler and the view's sequencer metadata, of which it holds parkedMetadataCount while it waits.
    private void assertParkedStatementStopsAfterWake(
            String statement,
            int viewNamePosition,
            int parkedMetadataCount,
            ParkedAction action,
            String refusal
    ) throws Exception {
        final TableToken viewToken = viewToken();
        final FiberRuntime runtime = new FiberRuntime(1);
        try (
                SequencerMetadataCounter metadataCounter = new SequencerMetadataCounter(engine, viewToken);
                SqlExecutionContextImpl context = newContext();
                NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                Watchdog watchdog = new Watchdog(AbstractCairoTest::drainWalQueue)
        ) {
            context.with(circuitBreaker);
            circuitBreaker.resetTimer();
            final StatementTask task = new StatementTask(statement, context);
            assertParks(runtime, task, watchdog);
            Assert.assertEquals(parkedMetadataCount, metadataCounter.get());

            action.run();
            final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
            drainUntilFinished(runtime, task);
            watchdog.assertNotFired();
            Assert.assertTrue(String.valueOf(task.error), task.error instanceof SqlException);
            final SqlException e = (SqlException) task.error;
            TestUtils.assertEquals(refusal, e.getFlyweightMessage());
            Assert.assertEquals(viewNamePosition, e.getPosition());
            Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());
            assertNoRefreshQueued();
            Assert.assertEquals(0, metadataCounter.get());
        } finally {
            close(runtime);
        }
        Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount());
    }

    // The refresh queue holds no refresh task: other tasks, such as the invalidation a view drop queues, go back on it.
    private static void assertNoRefreshQueued() {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        final ObjList<MatViewRefreshTask> tasks = new ObjList<>();
        while (true) {
            final MatViewRefreshTask task = new MatViewRefreshTask();
            if (!stateStore.tryDequeueRefreshTask(task)) {
                break;
            }
            tasks.add(task);
        }
        for (int i = 0, n = tasks.size(); i < n; i++) {
            final MatViewRefreshTask task = tasks.getQuick(i);
            Assert.assertFalse(
                    "refresh task queued: " + MatViewRefreshTask.getRefreshOperationName(task.operation),
                    MatViewRefreshTask.isRefreshOperation(task.operation)
            );
            stateStore.reenqueueRefreshTask(task);
        }
    }

    // Runs RESUME on a busy latch after the action has run, which makes the statement's park fail: the statement
    // aborts with "server is closing" without waiting, writes nothing, and leaves the view invalid and the latch with
    // the test.
    private void assertAbortsWithNothingWritten(ParkFailure parkFailure) throws Exception {
        assertMemoryLeak(() -> {
            createInvalidView();
            final MatViewState state = viewState();
            final long viewSeqTxn = viewSeqTxn();
            final LatchHolder latch = new LatchHolder(state);
            final FiberRuntime runtime = new FiberRuntime(1);
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(latch::release)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                parkFailure.arm(runtime, state);
                final StatementTask task = new StatementTask(RESUME, context);
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));
                watchdog.assertNotFired();
                Assert.assertTrue(task.isFinished);
                assertServerClosing(task.error);
                Assert.assertTrue(state.isLocked());
            } finally {
                timerShardsOverride = null;
                state.setOnResumeLatchBusyForTesting(null);
                latch.release();
                close(runtime);
            }
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");

            execute(RESUME);
            assertResumedOnBase2();
        });
    }

    // Runs the task until it parks: the drain returns while the statement is unfinished.
    private static void assertParks(FiberRuntime runtime, StatementTask task, Watchdog watchdog) {
        Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
        Assert.assertEquals(1, runtime.drain(1));
        watchdog.assertNotFired();
        Assert.assertNull(task.error);
        Assert.assertFalse("the statement must still wait", task.isFinished);
    }

    // A MANUAL view whose ALTER ... AS waits in the view's WAL, and a base row the view has not refreshed. The REFRESH
    // parks until the test applies the definition, and then refreshes with the new SQL.
    private void assertRefreshParksWhileDefinitionPending(String refreshSql, String bucketB) throws Exception {
        assertMemoryLeak(() -> {
            createBasesWithView("MANUAL");
            execute("INSERT INTO base VALUES ('a', 3.0, '2024-01-01T00:20:00.000000Z')");
            drainWalQueue();
            execute("ALTER MATERIALIZED VIEW v AS (" + NEW_SQL + ")");
            final FiberRuntime runtime = new FiberRuntime(1);
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(AbstractCairoTest::drainWalQueue)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(refreshSql, context);
                assertParks(runtime, task, watchdog);
                Assert.assertEquals(VIEW_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken()).getMatViewSql());

                drainWalQueue();
                drainUntilFinished(runtime, task);
                watchdog.assertNotFired();
                Assert.assertNull(task.error);
            } finally {
                close(runtime);
            }
            drainWalAndMatViewQueues();
            Assert.assertEquals(NEW_SQL, engine.getDependentViewGraph().getViewDefinition(viewToken()).getMatViewSql());
            Assert.assertFalse(viewState().isInvalid());
            // max(price) * 10 of each hour: a 1.0 and 3.0, b 2.0
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    """.replace("\tb\t2.0", bucketB));
        });
    }

    // RESUME SKIP BACKLOG made the view valid on base2 and kept its rows; its next refresh reads base2 from there.
    private void assertResumedOnBase2() throws Exception {
        drainWalAndMatViewQueues();
        assertViewStatus("valid", "");
        assertViewRows(BASE_ROWS);
        execute("INSERT INTO base2 VALUES ('z', 30.0, '2024-01-01T02:30:00.000000Z')");
        drainWalAndMatViewQueues();
        assertViewRows(BASE_ROWS + "2024-01-01T02:00:00.000000Z\tz\t30.0\n");
    }

    private static void assertServerClosing(Throwable error) {
        Assert.assertTrue(String.valueOf(error), error instanceof CairoException);
        final CairoException e = (CairoException) error;
        TestUtils.assertEquals("query aborted, server is closing", e.getFlyweightMessage());
        Assert.assertTrue(e.isInterruption());
    }

    // RESUME parks while the view's WAL waits to apply; the statement then runs on the test thread, and the parked
    // RESUME stops at its next wake with the refusal.
    private void assertStopsAfterWake(String statement, String refusal) throws Exception {
        assertMemoryLeak(() -> {
            createInvalidView();
            execute("ALTER MATERIALIZED VIEW v ADD COLUMN extra DOUBLE");
            final MatViewState state = viewState();
            final FiberRuntime runtime = new FiberRuntime(1);
            try (
                    SqlExecutionContextImpl context = newContext();
                    NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker();
                    Watchdog watchdog = new Watchdog(AbstractCairoTest::drainWalQueue)
            ) {
                context.with(circuitBreaker);
                circuitBreaker.resetTimer();
                final StatementTask task = new StatementTask(RESUME, context);
                assertParks(runtime, task, watchdog);

                execute(statement);
                drainUntilFinished(runtime, task);
                watchdog.assertNotFired();
                Assert.assertTrue(String.valueOf(task.error), task.error instanceof SqlException);
                final SqlException e = (SqlException) task.error;
                TestUtils.assertEquals(refusal, e.getFlyweightMessage());
                Assert.assertEquals(REFRESH_VIEW_NAME_POSITION, e.getPosition());
            } finally {
                close(runtime);
            }
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount());
            assertQuery("SELECT count() FROM base2").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
        });
    }

    private void assertViewColumns(String... expected) throws Exception {
        final StringBuilder columns = new StringBuilder("column\n");
        for (String column : expected) {
            columns.append(column).append('\n');
        }
        assertQuery("SELECT \"column\" FROM table_columns('v')").noLeakCheck().noRandomAccess().returns(columns.toString());
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("v ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'v'")
                .noLeakCheck().noRandomAccess().returns("view_status\tinvalidation_reason\n" + status + '\t' + reason + '\n');
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(8);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    // Base rows a@00:10 (1.0) and b@01:10 (2.0), base2 rows x@00:20 (10.0) and y@01:20 (20.0), and a view on base
    // refreshed from them.
    private void createBasesWithView(String refreshType) throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-01T01:10:00.000000Z')
                """);
        execute("""
                INSERT INTO base2 VALUES
                ('x', 10.0, '2024-01-01T00:20:00.000000Z'),
                ('y', 20.0, '2024-01-01T01:20:00.000000Z')
                """);
        execute("CREATE MATERIALIZED VIEW v REFRESH " + refreshType + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
        if (!"IMMEDIATE".equals(refreshType)) {
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW v INCREMENTAL");
        }
        drainWalAndMatViewQueues();
        assertViewRows(BASE_ROWS);
        assertViewStatus("valid", "");
    }

    // The view of createBasesWithView(), moved to base2: invalid until RESUME SKIP BACKLOG.
    private void createInvalidView() throws Exception {
        createBasesWithView("IMMEDIATE");
        execute("ALTER MATERIALIZED VIEW v WITH BASE base2 AS (" + VIEW_SQL_BASE2 + ")");
        drainWalAndMatViewQueues();
        assertViewStatus("invalid", "base table changed");
        Assert.assertFalse(viewState().isLocked());
    }

    // The MANUAL view of createBasesWithView(), and an ALTER ... AS to X_SQL, whose definition event waits in the view's
    // WAL.
    private void createViewWithPendingDefinition() throws Exception {
        createBasesWithView("MANUAL");
        execute("ALTER MATERIALIZED VIEW v AS (" + X_SQL + ")");
        Assert.assertEquals(viewSeqTxn(), viewState().getPendingDefinitionSeqTxn());
    }

    private static void drainUntilFinished(FiberRuntime runtime, StatementTask task) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WATCHDOG_MILLIS);
        while (!task.isFinished && System.nanoTime() < deadline) {
            runtime.drain(1);
            Os.pause();
        }
        Assert.assertTrue("the statement did not finish", task.isFinished);
    }

    private static NetworkSqlExecutionCircuitBreaker newCircuitBreaker() {
        return new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }
        }) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
    }

    private static SqlExecutionContextImpl newContext() {
        return TestUtils.createSqlExecutionCtx(engine, 1);
    }

    private static long viewSeqTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken()).getSeqTxn();
    }

    private static MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private static TableToken viewToken() {
        return engine.verifyTableName("v");
    }

    @FunctionalInterface
    private interface ParkFailure {
        void arm(FiberRuntime runtime, MatViewState state);
    }

    // What the test does while a statement is parked.
    @FunctionalInterface
    private interface ParkedAction {
        void run() throws Exception;
    }

    // The view latch that the test holds, as a refresh does, and releases once, the way a refresh does.
    private static final class LatchHolder {
        private final AtomicBoolean isHeld = new AtomicBoolean();
        private final MatViewState state;

        private LatchHolder(MatViewState state) {
            this.state = state;
            Assert.assertTrue(state.tryLock());
            isHeld.set(true);
        }

        private void release() {
            if (isHeld.compareAndSet(true, false)) {
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), state.getViewDefinition().getMatViewToken(), state, false);
            }
        }
    }

    private static class PinnedStatementInitializer {
        // Class initialization pins the continuation while the statement waits.
        static {
            PinnedStatementTask.CURRENT_TASK.get().runStatement();
        }

        private static void initialize() {
        }
    }

    private static class PinnedStatementTask extends StatementTask {
        private static final ThreadLocal<PinnedStatementTask> CURRENT_TASK = new ThreadLocal<>();

        private PinnedStatementTask(String sql, SqlExecutionContextImpl context) {
            super(sql, context);
        }

        @Override
        protected boolean runStep() {
            CURRENT_TASK.set(this);
            try {
                PinnedStatementInitializer.initialize();
            } finally {
                CURRENT_TASK.remove();
            }
            return true;
        }
    }

    private static class StatementTask extends FiberTask {
        private final SqlExecutionContextImpl context;
        private final String sql;
        volatile Throwable error;
        // the thread that ran the statement's last step, the carrier the statement ended on
        volatile Thread finishingThread;
        volatile boolean isFinished;

        private StatementTask(String sql, SqlExecutionContextImpl context) {
            this.context = context;
            this.sql = sql;
        }

        @Override
        protected boolean runStep() {
            runStatement();
            return true;
        }

        void runStatement() {
            try {
                engine.execute(sql, context);
            } catch (Throwable th) {
                error = th;
            } finally {
                finishingThread = Thread.currentThread();
                isFinished = true;
            }
        }
    }

    // Runs the release after WATCHDOG_MILLIS unless the test closes it first: a statement that holds its carrier
    // keeps the test thread in the drain until then.
    private static final class Watchdog implements AutoCloseable {
        private final CountDownLatch closed = new CountDownLatch(1);
        private final AtomicBoolean hasFired = new AtomicBoolean();
        private final Thread thread;

        private Watchdog(Runnable release) {
            thread = new Thread(() -> {
                try {
                    if (!closed.await(WATCHDOG_MILLIS, TimeUnit.MILLISECONDS)) {
                        hasFired.set(true);
                        release.run();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    Path.clearThreadLocals();
                }
            }, "statement-wait-watchdog");
            thread.start();
        }

        @Override
        public void close() throws InterruptedException {
            closed.countDown();
            thread.join();
        }

        private void assertNotFired() {
            Assert.assertFalse("the statement held its carrier until the watchdog released what it waited for", hasFired.get());
        }
    }
}
