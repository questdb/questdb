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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncStreamGroupByAtom;
import io.questdb.mp.MCSequence;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.tasks.GroupByMergeShardTask;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A hash GROUP BY fills the merge queue and the test holds its only slot, so a stream GROUP BY owner
 * that helps with a foreign shard parks deterministically.
 */
public class StreamGroupByMergeCancellationTest extends AbstractTest {
    private static final int QUEUE_CAPACITY = 32;
    private static final int ROWS = 512;

    @Test
    public void testCancelledOwnerLeavesForeignMergeTasks() throws Exception {
        assertForeignMergeSurvivesOwnerCancellation(true);
    }

    @Test
    public void testOwnerCancellationDoesNotFailForeignMerge() throws Exception {
        assertForeignMergeSurvivesOwnerCancellation(false);
    }

    private static void assertForeignMergeSurvivesOwnerCancellation(boolean isCancelledAtDrain) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getGroupByMergeShardQueueCapacity() {
                    return QUEUE_CAPACITY;
                }

                @Override
                public int getGroupByShardingThreshold() {
                    return 1;
                }

                @Override
                public long getQueryContinuationWakeIntervalMillis() {
                    return TimeUnit.HOURS.toMillis(1);
                }

                @Override
                public int getSqlPageFrameMaxRows() {
                    return 128;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return 1;
                }
            };
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext setupContext = TestUtils.createSqlExecutionCtx(engine, 1)
            ) {
                final MessageBus bus = engine.getMessageBus();
                final MCSequence subSeq = bus.getGroupByMergeShardSubSeq();
                final FiberRuntime ownerRuntime = new FiberRuntime(1);
                final FiberRuntime foreignRuntime = new FiberRuntime(1);
                final FiberRuntime dispatcherRuntime = new FiberRuntime(1);
                final QueryParallelFiberDispatcher dispatcher = new QueryParallelFiberDispatcher(engine, bus, dispatcherRuntime);
                bus.setQueryParallelFiberDispatcher(dispatcher);
                final AtomicBoolean isCancelHookHit = new AtomicBoolean();
                final AtomicBooleanCircuitBreaker ownerBreaker = new AtomicBooleanCircuitBreaker(engine) {
                    @Override
                    public boolean checkIfTrippedOrYield() {
                        if (isCancelledAtDrain && bus.getGroupByMergeShardPubSeq().current() >= QUEUE_CAPACITY
                                && isCancelHookHit.compareAndSet(false, true)) {
                            cancel();
                        }
                        return super.checkIfTrippedOrYield();
                    }
                };
                ownerBreaker.setCancelledFlag(new FiberCancellationSignal());
                final AtomicBooleanCircuitBreaker foreignBreaker = new AtomicBooleanCircuitBreaker(engine);
                try (
                        SqlExecutionContext ownerContext = TestUtils.createSqlExecutionCtx(engine, 1).with(ownerBreaker);
                        SqlExecutionContext foreignContext = TestUtils.createSqlExecutionCtx(engine, 1).with(foreignBreaker)
                ) {
                    engine.execute(
                            "CREATE TABLE tab AS (SELECT x::TIMESTAMP ts, ('k' || x) key, x v FROM long_sequence(" + ROWS + ")) TIMESTAMP(ts) PARTITION BY DAY",
                            setupContext
                    );
                    final String ownerSql = "SELECT timestamp_floor('1d', ts) d, key, max(v) m FROM tab";
                    final String foreignSql = "SELECT key, max(v) m FROM tab";
                    try (
                            RecordCursorFactory ownerFactory = compiler.compile(ownerSql, ownerContext).getRecordCursorFactory();
                            RecordCursorFactory foreignFactory = compiler.compile(foreignSql, foreignContext).getRecordCursorFactory()
                    ) {
                        Assert.assertTrue(TestUtils.findAtom(ownerFactory, ownerSql) instanceof AsyncStreamGroupByAtom);
                        TestUtils.assertFactoryInTree(foreignFactory, AsyncGroupByRecordCursorFactory.class);
                        final PerWorkerLocks foreignLocks = TestUtils.findPerWorkerLocks(foreignFactory, foreignSql);
                        final Owner owner = new Owner(engine, ownerFactory, ownerContext, ownerBreaker, 2);
                        final Owner foreign = new Owner(engine, foreignFactory, foreignContext, foreignBreaker, 1);
                        int heldSlot = -1;
                        try {
                            Assert.assertEquals(LaunchResult.LAUNCHED, foreignRuntime.launch(foreign));
                            settle(foreignRuntime, foreign);
                            Assert.assertEquals(QUEUE_CAPACITY - 1, bus.getGroupByMergeShardPubSeq().current());
                            final long first = subSeq.next();
                            final GroupByMergeShardTask firstTask = bus.getGroupByMergeShardQueue().get(first);
                            final SqlExecutionCircuitBreaker foreignTaskBreaker = firstTask.getCircuitBreaker();
                            GroupByMergeShardJob.run(0, firstTask, subSeq, first, null, dispatcher);
                            heldSlot = foreignLocks.acquireSlot(0, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);

                            Assert.assertEquals(LaunchResult.LAUNCHED, ownerRuntime.launch(owner));
                            settle(ownerRuntime, owner);
                            if (isCancelledAtDrain) {
                                Assert.assertTrue(isCancelHookHit.get());
                                Assert.assertEquals("the cancelled owner took a foreign task", first, subSeq.current());
                            } else {
                                Assert.assertEquals(first + 1, subSeq.current());
                                ownerBreaker.cancel();
                                settle(ownerRuntime, owner);
                            }
                            Assert.assertFalse("owner cancellation failed a foreign merge", foreignTaskBreaker.checkIfTripped());
                            Assert.assertEquals(1, ownerRuntime.getParkedFiberCount());

                            foreignLocks.releaseSlot(heldSlot);
                            heldSlot = -1;
                            settle(ownerRuntime, owner);
                            runQueuedTasks(bus, dispatcher);
                            settle(ownerRuntime, owner);
                            settle(foreignRuntime, foreign);
                            Assert.assertTrue(owner.isDone());
                            Assert.assertTrue(String.valueOf(owner.error), owner.error instanceof CairoException e && e.isInterruption());
                            Assert.assertNotNull(owner.signal);
                            Assert.assertSame(owner.signal, owner.signalAtError);
                            Assert.assertTrue(foreign.isDone());
                            Assert.assertNull(foreign.error);
                            Assert.assertEquals(ROWS, foreign.rows);
                            Assert.assertEquals(ROWS * (ROWS + 1L) / 2, foreign.sum);
                            Assert.assertEquals(0, foreignLocks.getAcquiredSlotCount());
                        } finally {
                            if (heldSlot > -1) {
                                foreignLocks.releaseSlot(heldSlot);
                            }
                            if (!owner.isDone() || !foreign.isDone()) {
                                ownerBreaker.cancel();
                                foreignBreaker.cancel();
                                settle(ownerRuntime, owner);
                                runQueuedTasks(bus, dispatcher);
                                settle(ownerRuntime, owner);
                                settle(foreignRuntime, foreign);
                            }
                        }
                    }
                } finally {
                    closeRuntime(ownerRuntime);
                    closeRuntime(foreignRuntime);
                    bus.setQueryParallelFiberDispatcher(null);
                    dispatcher.close();
                    closeRuntime(dispatcherRuntime);
                }
            }
        });
    }

    private static void closeRuntime(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(16);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static void runQueuedTasks(MessageBus bus, QueryParallelFiberDispatcher dispatcher) {
        final MCSequence subSeq = bus.getGroupByMergeShardSubSeq();
        long cursor;
        while ((cursor = subSeq.next()) > -1) {
            GroupByMergeShardJob.run(0, bus.getGroupByMergeShardQueue().get(cursor), subSeq, cursor, null, dispatcher);
        }
    }

    private static void settle(FiberRuntime runtime, Owner owner) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!owner.isDone()) {
            if (runtime.drain(16) == 0 && runtime.getQueuedCount() == 0) {
                return;
            }
            Assert.assertTrue("runtime did not settle", System.nanoTime() < deadline);
        }
    }

    private static class Owner extends FiberTask {
        private final SqlExecutionCircuitBreaker breaker;
        private final SqlExecutionContext context;
        private final CairoEngine engine;
        private final RecordCursorFactory factory;
        private final int valueColumn;
        private Throwable error;
        private long rows;
        private FiberCancellationSignal signal;
        private FiberCancellationSignal signalAtError;
        private long sum;

        private Owner(CairoEngine engine, RecordCursorFactory factory, SqlExecutionContext context, SqlExecutionCircuitBreaker breaker, int valueColumn) {
            this.engine = engine;
            this.factory = factory;
            this.context = context;
            this.breaker = breaker;
            this.valueColumn = valueColumn;
        }

        @Override
        protected void onError(Throwable th) {
            error = th;
        }

        @Override
        protected boolean runStep() {
            SuspensionScope.enterTimerShards(engine.getTimerShards());
            SuspensionScope.enterCancellationSource(breaker);
            try (RecordCursor cursor = factory.getCursor(context)) {
                if (breaker.getCancelledFlag() instanceof FiberCancellationSignal cancelledFlag) {
                    signal = cancelledFlag;
                    SuspensionScope.enterCancellationSignal(cancelledFlag);
                }
                final Record record = cursor.getRecord();
                try {
                    while (cursor.hasNext()) {
                        rows++;
                        sum += record.getLong(valueColumn);
                    }
                } catch (Throwable th) {
                    signalAtError = SuspensionScope.getCancellationSignal();
                    throw th;
                }
            } catch (SqlException e) {
                throw new AssertionError(e);
            }
            return true;
        }
    }
}
