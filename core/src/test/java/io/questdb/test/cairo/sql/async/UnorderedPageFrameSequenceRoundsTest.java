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

package io.questdb.test.cairo.sql.async;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.std.LongList;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Rounds of tasks of a sequence that {@link UnorderedPageFrameSequence#ofRounds} binds without
 * frames, as a parallel hash join builds its stages through one: on thread workers, on fiber
 * workers, and on the owner alone, which then steals every task.
 * <p>
 * The test atom guards a slot per worker with {@link PerWorkerLocks}, and every round task takes a
 * slot as the build's round reducers do. With workers, {@link SlotGatedWorkStealingStrategy} holds
 * the owner off until a worker took a slot, so that a slot-leak check after a round, a failed one
 * included, counts slots that a worker really took.
 */
public class UnorderedPageFrameSequenceRoundsTest extends AbstractCairoTest {
    private static final UnorderedPageFrameReducer FAILING_FRAME_REDUCER = (_, _, _, _, _, _) -> {
        throw new AssertionError("the frame reducer ran a task of a round");
    };
    private static final Log LOG = LogFactory.getLog(UnorderedPageFrameSequenceRoundsTest.class);
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        super.setUp();
    }

    @Test
    public void testCancellationDuringRoundOnFiberWorkers() throws Exception {
        assertCancellationDuringRound(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testCancellationDuringRoundOnOwnerAlone() throws Exception {
        assertCancellationDuringRound(null);
    }

    @Test
    public void testCancellationDuringRoundOnThreadWorkers() throws Exception {
        assertCancellationDuringRound(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFailingTaskDrainsTheRoundOnFiberWorkers() throws Exception {
        assertFailingTaskDrainsTheRound(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testFailingTaskDrainsTheRoundOnOwnerAlone() throws Exception {
        assertFailingTaskDrainsTheRound(null);
    }

    @Test
    public void testFailingTaskDrainsTheRoundOnThreadWorkers() throws Exception {
        assertFailingTaskDrainsTheRound(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testFrameOperationsAfterOfRoundsAssert() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x, ('s' || (x % 3))::SYMBOL s, timestamp_sequence(0, 43_200_000_000) ts
                        FROM long_sequence(4)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final AtomicInteger reducedFrames = new AtomicInteger();
            final AtomicReference<String> mismatch = new AtomicReference<>();
            final UnorderedPageFrameReducer frameReducer = (_, _, _, _, sequence, _) -> {
                if (sequence.getSymbolTableSource() == null) {
                    mismatch.compareAndSet(null, "a frame task found no symbol table source");
                }
                reducedFrames.incrementAndGet();
            };
            try (
                    RecordCursorFactory factory = select("t");
                    UnorderedPageFrameSequence<RoundAtom> sequence = new UnorderedPageFrameSequence<>(
                            engine,
                            configuration,
                            engine.getMessageBus(),
                            new RoundAtom(configuration, 1),
                            frameReducer,
                            1
                    )
            ) {
                sequence.ofRounds(sqlExecutionContext);
                // A sequence that runs rounds has no frames to dispatch, prepare or bind to.
                assertFrameOperationFails(sequence::dispatchAndAwait, "has no frames to dispatch");
                assertFrameOperationFails(sequence::prepareForDispatch, "has no frames to dispatch");
                assertFrameOperationFails(
                        () -> sequence.of(factory, sqlExecutionContext, ORDER_ASC),
                        "binds to frames only after reset()"
                );
                // The refused bind opened no page frame cursor, so no table reader is held.
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertNull(sequence.getSymbolTableSource());
                Assert.assertEquals(0, reducedFrames.get());

                // The refusals leave the binding intact: rounds still run.
                final RoundReducer reducer = new RoundReducer(3);
                runRound(sequence, reducer, 3, false);
                assertEveryTaskRanOnce(reducer, 3);
                Assert.assertEquals(3, reducer.ownerTasks.get());

                // reset() ends the rounds binding, after which the sequence binds to frames.
                sequence.reset();
                sequence.of(factory, sqlExecutionContext, ORDER_ASC);
                Assert.assertNotNull(sequence.getSymbolTableSource());
                sequence.prepareForDispatch();
                // Two daily partitions, so two frames.
                Assert.assertEquals(2, sequence.getFrameCount());
                sequence.dispatchAndAwait();
                Assert.assertEquals(2, reducedFrames.get());
                Assert.assertNull(mismatch.get());

                // A sequence bound to frames runs rounds only after reset().
                assertFrameOperationFails(() -> sequence.ofRounds(sqlExecutionContext), "runs rounds only after reset()");
                Assert.assertNotNull(sequence.getSymbolTableSource());
                sequence.reset();
                Assert.assertEquals(0, engine.getBusyReaderCount());
                sequence.ofRounds(sqlExecutionContext);
                final RoundReducer afterFrames = new RoundReducer(2);
                runRound(sequence, afterFrames, 2, false);
                assertEveryTaskRanOnce(afterFrames, 2);
                sequence.reset();
            }
        });
    }

    @Test
    public void testRoundsOnFiberWorkers() throws Exception {
        assertRounds(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testRoundsOnOwnerAlone() throws Exception {
        assertRounds(null);
    }

    @Test
    public void testRoundsOnThreadWorkers() throws Exception {
        assertRounds(WorkerPoolMode.LEGACY);
    }

    private static void assertEveryTaskRanOnce(RoundReducer reducer, int taskCount) {
        Assert.assertNull(reducer.mismatch.get());
        Assert.assertEquals(taskCount, reducer.total.get());
        for (int i = 0; i < taskCount; i++) {
            Assert.assertEquals("task " + i, 1, reducer.runs.get(i));
        }
        Assert.assertEquals(0, reducer.inFlight.get());
        Assert.assertEquals(taskCount, reducer.ownerTasks.get() + reducer.workerTasks.get());
    }

    private static void assertFrameOperationFails(ThrowingRunnable operation, String message) {
        final AssertionError error = Assert.assertThrows(AssertionError.class, operation);
        TestUtils.assertContains(error.getMessage(), message);
    }

    // A round that its first task stopped, by failing or by cancelling the query, ran the first task
    // and every task that had got past its checks while the first one ran: no more than one per
    // thread and one per live fiber, far fewer than the round's tasks. The round returned only once
    // all of them were done.
    private static void assertStoppedRound(UnorderedPageFrameSequence<RoundAtom> sequence, RoundReducer reducer, int taskCount) {
        Assert.assertNull(reducer.mismatch.get());
        Assert.assertFalse(sequence.isActive());
        Assert.assertTrue(reducer.firstTask >= 0);
        Assert.assertEquals(1, reducer.runs.get(reducer.firstTask));
        final int ran = reducer.total.get();
        Assert.assertTrue("tasks that ran: " + ran, ran >= 1 && ran < taskCount / 2);
        for (int i = 0; i < taskCount; i++) {
            Assert.assertTrue("task " + i, reducer.runs.get(i) <= 1);
        }
        Assert.assertEquals(0, reducer.inFlight.get());
    }

    // Where the tasks of a round that ran to the end ran: with workers, at least the one whose slot
    // opened the owner's gate ran on a worker, inside a fiber exactly when the workers host fibers;
    // the owner alone runs every task, stealing it from the queue or reducing it in place.
    private static void assertTaskPlacement(RoundReducer reducer, int taskCount, @Nullable WorkerPoolMode mode) {
        if (mode == null) {
            Assert.assertEquals(taskCount, reducer.ownerTasks.get());
            Assert.assertEquals(0, reducer.fiberTasks.get());
        } else {
            Assert.assertTrue("no task ran on a worker", reducer.workerTasks.get() > 0);
            Assert.assertEquals(mode == WorkerPoolMode.FIBER_HOST ? reducer.workerTasks.get() : 0, reducer.fiberTasks.get());
        }
    }

    private static void executeRounds(@Nullable WorkerPoolMode mode, RoundsCode code) throws Exception {
        assertMemoryLeak(() -> {
            final int workerCount = mode == null ? 1 : WORKER_COUNT;
            TestUtils.execute(mode == null ? null : new TestWorkerPool(WORKER_COUNT, mode), (db, _, _) -> {
                // Fiber workers run the queue's tasks through the fiber dispatcher, thread workers
                // run them on their own threads.
                Assert.assertEquals(mode == WorkerPoolMode.FIBER_HOST, db.getMessageBus().getPageFrameReduceDispatcher() != null);
                final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(db);
                try (
                        SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, workerCount)
                                .with(AllowAllSecurityContext.INSTANCE, null, null, -1, breaker);
                        UnorderedPageFrameSequence<RoundAtom> sequence = new UnorderedPageFrameSequence<>(
                                db,
                                db.getConfiguration(),
                                db.getMessageBus(),
                                new RoundAtom(db.getConfiguration(), workerCount),
                                FAILING_FRAME_REDUCER,
                                workerCount
                        )
                ) {
                    code.run(db, sequence, context, breaker, mode != null);
                }
            }, configuration, LOG);
        });
    }

    private static long rowsOf(int taskIndex) {
        return 3L * taskIndex + 1;
    }

    // Runs a round of taskCount tasks. With workers, the owner waits until a worker took a slot
    // before it takes a task itself, and every slot is back once the round returns or throws.
    private static void runRound(
            UnorderedPageFrameSequence<RoundAtom> sequence,
            RoundReducer reducer,
            int taskCount,
            boolean hasWorkers
    ) {
        final LongList taskRowCounts = new LongList();
        for (int i = 0; i < taskCount; i++) {
            taskRowCounts.add(rowsOf(i));
        }
        final PerWorkerLocks locks = sequence.getAtom().getPerWorkerLocks();
        final CountDownLatch acquired = hasWorkers && taskCount > 0 ? new CountDownLatch(1) : null;
        locks.setTestAcquireLatch(acquired);
        try {
            sequence.dispatchRoundAndAwait(reducer, taskRowCounts);
        } finally {
            locks.setTestAcquireLatch(null);
            // No task of the round is queued or running any more, whether it returned or threw.
            Assert.assertEquals(0, reducer.inFlight.get());
            Assert.assertEquals(0, locks.getAcquiredSlotCount());
            if (acquired != null) {
                Assert.assertEquals("no worker took a slot", 0, acquired.getCount());
            }
        }
    }

    private void assertCancellationDuringRound(@Nullable WorkerPoolMode mode) throws Exception {
        executeRounds(mode, (db, sequence, context, breaker, hasWorkers) -> {
            // Enough tasks to fill the queue twice over.
            final int taskCount = 2 * db.getMessageBus().getUnorderedPageFrameReduceQueue().getCycle();
            for (int attempt = 0; attempt < 2; attempt++) {
                sequence.ofRounds(context);
                final RoundReducer reducer = new RoundReducer(taskCount);
                reducer.stopRound = breaker::cancel;
                reducer.isRoundStopped = breaker::checkIfTripped;
                final CairoException error = Assert.assertThrows(
                        CairoException.class,
                        () -> runRound(sequence, reducer, taskCount, hasWorkers)
                );
                Assert.assertTrue(error.getMessage(), error.isCancellation());
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, error.getInterruptionReason());
                assertStoppedRound(sequence, reducer, taskCount);

                // The cancelled binding resets, and the next one runs every task; no task of the
                // cancelled round runs after it.
                final int ran = reducer.total.get();
                breaker.reset();
                sequence.reset();
                sequence.ofRounds(context);
                final RoundReducer next = new RoundReducer(taskCount);
                runRound(sequence, next, taskCount, hasWorkers);
                assertEveryTaskRanOnce(next, taskCount);
                assertTaskPlacement(next, taskCount, mode);
                Assert.assertEquals(ran, reducer.total.get());
                sequence.reset();
            }
        });
    }

    private void assertFailingTaskDrainsTheRound(@Nullable WorkerPoolMode mode) throws Exception {
        executeRounds(mode, (db, sequence, context, _, hasWorkers) -> {
            // Enough tasks to fill the queue twice over.
            final int taskCount = 2 * db.getMessageBus().getUnorderedPageFrameReduceQueue().getCycle() + 3;
            // A CairoException keeps its message, and any other failure takes the sequence's prefix.
            final FailureAndMessage[] failures = {
                    new FailureAndMessage(() -> CairoException.nonCritical().put("injected round failure"), "injected round failure"),
                    new FailureAndMessage(() -> new IllegalStateException("injected round failure"), "unexpected reduce error: injected round failure")
            };
            for (FailureAndMessage failure : failures) {
                sequence.ofRounds(context);
                final RoundReducer reducer = new RoundReducer(taskCount);
                reducer.stopRound = () -> {
                    throw failure.failure.get();
                };
                reducer.isRoundStopped = () -> !sequence.isActive();
                final CairoException error = Assert.assertThrows(
                        CairoException.class,
                        () -> runRound(sequence, reducer, taskCount, hasWorkers)
                );
                TestUtils.assertEquals(failure.message, error.getFlyweightMessage());
                Assert.assertFalse(error.isCancellation());
                assertStoppedRound(sequence, reducer, taskCount);

                // The failed binding resets, and the next one runs every task; no task of the
                // failed round runs after it.
                final int ran = reducer.total.get();
                sequence.reset();
                sequence.ofRounds(context);
                final RoundReducer next = new RoundReducer(taskCount);
                runRound(sequence, next, taskCount, hasWorkers);
                assertEveryTaskRanOnce(next, taskCount);
                assertTaskPlacement(next, taskCount, mode);
                Assert.assertEquals(ran, reducer.total.get());
                sequence.reset();
            }
        });
    }

    private void assertRounds(@Nullable WorkerPoolMode mode) throws Exception {
        executeRounds(mode, (db, sequence, context, breaker, hasWorkers) -> {
            // More tasks than the queue holds, so that the owner also meets a full queue.
            final int manyTasks = 2 * db.getMessageBus().getUnorderedPageFrameReduceQueue().getCycle() + 3;
            final RoundAtom atom = sequence.getAtom();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0)) {
                context.setMemoryTracker(tracker);
                try {
                    long previousId = Long.MIN_VALUE;
                    for (int binding = 0; binding < 2; binding++) {
                        final int clearCount = atom.clearCount;
                        sequence.ofRounds(context);
                        // The binding takes the execution's tracker and breaker, and a new id, but
                        // no frames: there is no symbol table source, and the atom stays as it was.
                        Assert.assertSame(tracker, sequence.getMemoryTracker());
                        Assert.assertSame(breaker, sequence.getCircuitBreaker());
                        Assert.assertNull(sequence.getSymbolTableSource());
                        Assert.assertEquals(0, sequence.getFrameCount());
                        Assert.assertTrue(sequence.isActive());
                        Assert.assertTrue(sequence.getId() > previousId);
                        previousId = sequence.getId();

                        final RoundReducer empty = new RoundReducer(0);
                        runRound(sequence, empty, 0, hasWorkers);
                        assertEveryTaskRanOnce(empty, 0);

                        // A single task: with workers, the owner waits until a worker took it.
                        final RoundReducer single = new RoundReducer(1);
                        runRound(sequence, single, 1, hasWorkers);
                        assertEveryTaskRanOnce(single, 1);
                        Assert.assertEquals(hasWorkers ? 1 : 0, single.workerTasks.get());
                        assertTaskPlacement(single, 1, mode);

                        final RoundReducer many = new RoundReducer(manyTasks);
                        runRound(sequence, many, manyTasks, hasWorkers);
                        assertEveryTaskRanOnce(many, manyTasks);
                        assertTaskPlacement(many, manyTasks, mode);

                        // Rounds follow one another within a binding, each with its own reducer.
                        final RoundReducer after = new RoundReducer(5);
                        runRound(sequence, after, 5, hasWorkers);
                        assertEveryTaskRanOnce(after, 5);
                        Assert.assertEquals(previousId, sequence.getId());
                        Assert.assertEquals(manyTasks, many.total.get());
                        Assert.assertEquals(clearCount, atom.clearCount);

                        // reset() clears the atom and drops the tracker; the sequence binds again.
                        sequence.reset();
                        Assert.assertEquals(clearCount + 1, atom.clearCount);
                        Assert.assertNull(sequence.getMemoryTracker());
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        });
    }

    @FunctionalInterface
    private interface RoundsCode {
        void run(
                CairoEngine engine,
                UnorderedPageFrameSequence<RoundAtom> sequence,
                SqlExecutionContextImpl context,
                AtomicBooleanCircuitBreaker breaker,
                boolean hasWorkers
        ) throws Exception;
    }

    private record FailureAndMessage(Supplier<RuntimeException> failure, String message) {
    }

    /**
     * The atom of a round sequence: a slot per worker, which every round task takes as a hash join
     * build's round reducers take theirs, and nothing else.
     */
    private static final class RoundAtom implements StatefulAtom, PerWorkerLockOwner {
        private final PerWorkerLocks locks;
        private int clearCount;

        private RoundAtom(CairoConfiguration configuration, int workerCount) {
            this.locks = new PerWorkerLocks(configuration, workerCount);
        }

        @Override
        public void clear() {
            clearCount++;
        }

        @Override
        public PerWorkerLocks getPerWorkerLocks() {
            return locks;
        }

        private int maybeAcquire(int workerId, boolean isOwner, SqlExecutionCircuitBreaker circuitBreaker) {
            return workerId == -1 && isOwner ? -1 : locks.acquireSlot(workerId, circuitBreaker);
        }

        private void release(int slot) {
            locks.releaseSlot(slot);
        }
    }

    /**
     * Counts where each task of a round ran, and checks what a round task finds on its sequence: no
     * frames, no symbol table source, and the rows the round gave the task.
     */
    private static final class RoundReducer implements UnorderedPageFrameReducer {
        private final AtomicInteger fiberTasks = new AtomicInteger();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicReference<String> mismatch = new AtomicReference<>();
        private final AtomicInteger ownerTasks = new AtomicInteger();
        private final AtomicIntegerArray runs;
        private final AtomicInteger total = new AtomicInteger();
        private final AtomicInteger workerTasks = new AtomicInteger();
        // The index of the task that ran first.
        private volatile int firstTask = -1;
        // Set for a round that its first task stops, with stopRound, by failing or by cancelling
        // the query. Every other task that reaches its reducer meanwhile waits until isRoundStopped
        // holds, so that a first task descheduled before it stops the round cannot let the whole
        // round run first.
        private volatile BooleanSupplier isRoundStopped;
        private volatile Runnable stopRound;

        private RoundReducer(int taskCount) {
            this.runs = new AtomicIntegerArray(Math.max(taskCount, 1));
        }

        @Override
        public void reduce(
                int workerId,
                @NotNull PageFrameMemoryRecord record,
                int taskIndex,
                @NotNull SqlExecutionCircuitBreaker circuitBreaker,
                @NotNull UnorderedPageFrameSequence<?> frameSequence,
                @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
        ) {
            final RoundAtom atom = (RoundAtom) frameSequence.getAtom();
            final int slot = atom.maybeAcquire(workerId, stealingFrameSequence == frameSequence, circuitBreaker);
            inFlight.incrementAndGet();
            try {
                final boolean isFirst = total.getAndIncrement() == 0;
                if (frameSequence.getSymbolTableSource() != null) {
                    mismatch.compareAndSet(null, "a round task found a symbol table source");
                }
                if (frameSequence.getFrameCount() != 0) {
                    mismatch.compareAndSet(null, "a round task found frames");
                }
                if (frameSequence.getFrameRowCount(taskIndex) != rowsOf(taskIndex)) {
                    mismatch.compareAndSet(null, "task " + taskIndex + " found rows " + frameSequence.getFrameRowCount(taskIndex));
                }
                runs.incrementAndGet(taskIndex);
                if (workerId == -1) {
                    if (stealingFrameSequence != frameSequence) {
                        mismatch.compareAndSet(null, "the owner ran task " + taskIndex + " of another sequence");
                    }
                    ownerTasks.incrementAndGet();
                } else {
                    workerTasks.incrementAndGet();
                }
                if (Fiber.isMounted()) {
                    fiberTasks.incrementAndGet();
                }
                final Runnable stopRound = this.stopRound;
                if (stopRound != null) {
                    if (isFirst) {
                        firstTask = taskIndex;
                        stopRound.run();
                    } else {
                        while (!isRoundStopped.getAsBoolean()) {
                            Os.pause();
                        }
                    }
                }
            } finally {
                inFlight.decrementAndGet();
                atom.release(slot);
            }
        }
    }
}
