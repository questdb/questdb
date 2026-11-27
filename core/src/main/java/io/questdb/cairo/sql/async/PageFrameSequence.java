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

package io.questdb.cairo.sql.async;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.FiberLocal;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PageFrameSequence<T extends StatefulAtom> extends AbstractPageFrameSequence implements Closeable {
    private static final long HELD_NONE = -1;
    private static final AtomicLong ID_SEQ = new AtomicLong();
    private static final long LOCAL_TASK_CURSOR = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(PageFrameSequence.class);
    // A lane that holds no staged task, see stagedTasks.
    private static final int STAGED_NONE = -1;
    private static final FiberLocal<int[]> UNTRACKED_SCOPE_DEPTH = new FiberLocal<>(() -> new int[1]);
    // Runs on a worker path right after the worker takes a lane task, see onLaneTaskClaimed().
    private static volatile Runnable laneClaimHookForTesting;
    private static volatile long laneRowBudgetForTesting = -1;
    private final MillisecondClock clock;
    // The first reducer error of a completion-tracked sequence, whose owner never reads the failed task.
    private final AsyncQueryErrorState errorState;
    private final LongList frameRowCounts = new LongList();
    // The rows a worker reduces along a lane before it leaves the next task to the owner, see PageFrameReduceJob.
    private final long laneRowBudget;
    private final PageFrameReduceTaskFactory localTaskFactory;
    private final MessageBus messageBus;
    private final AtomicInteger reduceFinishedCounter = new AtomicInteger(0);
    private final AtomicInteger reduceStartedCounter = new AtomicInteger(0);
    private final PageFrameReducer reducer;
    // The result slots of a completion-tracked sequence, by task index modulo the queue capacity, made as needed.
    private final ObjList<PageFrameReduceTask> resultTasks = new ObjList<>();
    // Run table: runFirstFrames.get(t) is the first global frame index of task (row-group run) t; the run
    // spans up to the next run's first frame (or frameCount for the last). Built in buildAddressCache.
    private final IntList runFirstFrames = new IntList();
    private final byte taskType; // PageFrameReduceTask.TYPE_*
    private final WorkStealingStrategy workStealingStrategy;
    public volatile boolean done;
    private T atom;
    // Sub-frame within the held task currently being collected; the collector reads the run's sub-frames
    // one at a time (each looks like a standalone frame) while the task slot stays held.
    private int collectSubFrame;
    private SCSequence collectSubSeq;
    // Protocol indices are in task (row-group run) units; the address cache / frameRowCounts stay
    // frame-indexed. When no row group is split, taskCount == frameCount and these track frames 1:1.
    private int collectedTaskIndex = -1;
    private int dispatchStartTaskIndex;
    private PageFrameAddressCache frameAddressCache;
    private int frameCount;
    private PageFrameCursor frameCursor;
    // The collect-queue cursor (or LOCAL_TASK_CURSOR) of the task being collected sub-frame by sub-frame,
    // or HELD_NONE between tasks. Held across the run so its slot frees only after the last sub-frame.
    private long heldCollectCursor = HELD_NONE;
    private int heldSubFrameCount;
    private long id;
    private boolean isClosing;
    // A completion-tracked sequence has no collect subscriber, so its owner never holds its shard's queue. Its tasks
    // reduce into resultTasks, and the queued tasks carry only the sequence and the task index. The thread that ends
    // a task marks it in reducedTasks and frees the queue slot; the owner reads the output from the result slot once
    // the mark shows, until collect() releases it. The dispatch limit keeps at most as many tasks between the
    // collected and the dispatched ones as there are result slots.
    private boolean isCompletionTracked;
    // Makes dispatchAhead() publish nothing, as an owner that may not publish does, see setPublicationRefusedForTesting().
    private boolean isPublicationRefusedForTesting;
    // With lanes, task i reduces only once task i - laneCount is reduced, so the tasks of a lane, task index modulo the
    // lane count, reduce one at a time and in order. A stream GROUP BY accumulates a lane's frames into the lane's span
    // maps, which then add up the same rows in the same order on every run. The owner publishes a task whose lane is
    // free and stages the others, see stagedTasks. 0 when the tasks have no lanes.
    private int laneCount;
    private PageFrameMemoryRecord localRecord;
    private int localReduceCount;
    // Local reduce task used when there is no slots in the queue to dispatch tasks.
    private PageFrameReduceTask localTask;
    // Per-query native memory tracker captured from the owning SqlExecutionContext
    // at workload start. Null when no per-query limit is configured. Workers read
    // this off the task via task.getFrameSequence().getMemoryTracker() to charge
    // their allocations to the active workload.
    private MemoryTracker memoryTracker;
    // The owner's own record of the tasks it staged, by lane: a task stays here until the owner learns that a thread
    // took it, or publishes, reduces or ends it itself.
    private int[] ownerStagedTasks = new int[0];
    private boolean readyToDispatch;
    private RingQueue<PageFrameReduceTask> reduceQueue;
    // reducedTasks[i] != 0 once task i of a completion-tracked sequence is reduced, failed or skipped; set with
    // release semantics after the task's output is complete.
    private int[] reducedTasks = new int[0];
    private SqlExecutionContext registeredContext;
    private int shard;
    private SqlExecutionContext sqlExecutionContext;
    // By lane, the task bound to its result slot and counted as dispatched while the lane's previous task is in flight,
    // or STAGED_NONE. The thread that ends the previous task on a worker path takes it and reduces it next, see
    // claimLaneSuccessor(), so the lane goes on without the owner. That thread takes the task before it marks its own,
    // so once the owner sees the lane free, a task still staged is the owner's: it publishes the task, reduces it itself
    // when it may not publish, or ends it without reducing it when it stops, see ownedStagedTask().
    private int[] stagedTasks = new int[0];
    private long startTime;
    // Number of tasks (row-group runs) to dispatch; the unit of the dispatch/collect protocol.
    private int taskCount;
    private boolean uninterruptible;
    // Must be initialized from the original SQL context's circuit breaker before use.
    private SqlExecutionCircuitBreakerWrapper workStealCircuitBreaker;

    /**
     * Constructs a page frame sequence instance. The returned instance takes ownership of the input atom.
     */
    public PageFrameSequence(
            CairoEngine engine,
            CairoConfiguration configuration,
            MessageBus messageBus,
            T atom,
            PageFrameReducer reducer,
            PageFrameReduceTaskFactory localTaskFactory,
            int sharedQueryWorkerCount,
            byte taskType
    ) {
        try {
            this.atom = atom;
            this.frameAddressCache = new PageFrameAddressCache();
            this.messageBus = messageBus;
            this.reducer = reducer;
            this.clock = configuration.getMillisecondClock();
            this.localTaskFactory = localTaskFactory;
            this.workStealingStrategy = configuration.getFactoryProvider()
                    .getWorkStealingStrategy(configuration, sharedQueryWorkerCount, atom);
            this.taskType = taskType;
            this.errorState = new AsyncQueryErrorState(PageFrameReduceTask.getUnexpectedErrorMessage(taskType));
            this.workStealCircuitBreaker = new SqlExecutionCircuitBreakerWrapper(engine, configuration.getCircuitBreakerConfiguration());
            this.laneRowBudget = configuration.getSqlPageFrameMaxRows();
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    /**
     * Waits for frame sequence completion, fetches remaining pieces of the
     * frame sequence from the queues. This method is not thread safe.
     */
    public static void enterUntrackedScope() {
        UNTRACKED_SCOPE_DEPTH.get()[0]++;
    }

    public static void leaveUntrackedScope() {
        UNTRACKED_SCOPE_DEPTH.get()[0]--;
    }

    @TestOnly
    public static void setLaneClaimHookForTesting(@Nullable Runnable hook) {
        laneClaimHookForTesting = hook;
    }

    // Overrides the lane row budget of legacy workers, see getLaneRowBudget(); -1 restores the configured one.
    @TestOnly
    public static void setLaneRowBudgetForTesting(long budget) {
        laneRowBudgetForTesting = budget;
    }

    public void await() {
        LOG.debug()
                .$("awaiting completion [shard=").$(shard)
                .$(", id=").$(id)
                .$(", frameCount=").$(frameCount)
                .I$();

        // A scan abandoned mid-run (close/cancel) can leave a task held with its slot un-freed; release it
        // so the drain below accounts it and the collect-queue slot frees.
        if (heldCollectCursor != HELD_NONE) {
            finalizeHeldTask(heldCollectCursor, true);
        }

        if (isCompletionTracked) {
            awaitReducedTasks();
            return;
        }

        final MCSequence pageFrameReduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean isFiberSuspendable = isFiberSuspendable();
        final boolean canPark = dispatcher != null && isFiberSuspendable;
        while (!done) {
            // Sampled before the work checks below: a producer that signals progress after the
            // checks but before the sample would otherwise be missed and this fiber would park.
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedShardProgress = canPark ? dispatcher.getRingProgressVersion(shard) : 0;
            // First check the local task: maybe we were reducing locally and got interrupted by an exception?
            if (localTask != null && localTask.getFrameSequence() == this && dispatchStartTaskIndex == localTask.getTaskIndex() + 1) {
                collectedTaskIndex = localTask.getTaskIndex();
                localTask.collected(true);
            }

            if (dispatchStartTaskIndex == collectedTaskIndex + 1) {
                // We know that all frames were collected. We're almost done.
                if (!done) {
                    // Looks like not all the frames were dispatched, so no one reached the very last frame and
                    // reset the sequence via calling PageFrameReduceTask#collected(). Let's do it ourselves.
                    markAsDone();
                }
                break;
            }

            // We were asked to steal work from the reduce queue and beyond, as much as we can.
            boolean nothingProcessed = true;
            try {
                nothingProcessed = PageFrameReduceJob.consumeQueue(
                        reduceQueue,
                        pageFrameReduceSubSeq,
                        localRecord,
                        workStealCircuitBreaker,
                        this,
                        dispatcher
                );
            } catch (Throwable th) {
                LOG.error()
                        .$("await error [id=").$(id)
                        .$(", ex=").$(th)
                        .I$();
            }

            if (nothingProcessed) {
                long cursor = collectSubSeq.next();
                if (cursor > -1) {
                    // Discard collected items.
                    final PageFrameReduceTask task = reduceQueue.get(cursor);
                    final PageFrameSequence<?> taskFrameSequence = task.getFrameSequence();
                    if (taskFrameSequence == this) {
                        assert id == task.getFrameSequenceId() : "ids mismatch: " + id + ", " + task.getFrameSequenceId();
                        collectedTaskIndex = task.getTaskIndex();
                        task.collected(true);
                    }
                    collectSubSeq.done(cursor);
                } else if (canPark) {
                    awaitProgress(dispatcher, observedProgress, observedShardProgress, true, getBlockedShard());
                } else {
                    yieldOrPause(isFiberSuspendable);
                }
            }
        }

        // It could be the case that one of the workers reduced a task, then marked the sequence as done,
        // but haven't incremented reduce counter yet. In this case, we wait for the desired counter value.
        awaitReduced(dispatcher, isFiberSuspendable);

        if (frameCount > 0) {
            suspendCollectSubSeq();
        }
    }

    public void awaitReduced() {
        awaitReduced(messageBus.getPageFrameReduceDispatcher(), isFiberSuspendable());
    }

    @Override
    public void close() {
        Throwable cleanupFailure = null;
        isClosing = true;
        try {
            reset();
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        final PageFrameMemoryRecord localRecordToFree = localRecord;
        localRecord = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, localRecordToFree);
        final SqlExecutionCircuitBreakerWrapper circuitBreakerToFree = workStealCircuitBreaker;
        workStealCircuitBreaker = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, circuitBreakerToFree);
        final PageFrameReduceTask localTaskToFree = localTask;
        localTask = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, localTaskToFree);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, resultTasks);
        final T atomToFree = atom;
        atom = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, atomToFree);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    public void collect(long cursor, boolean forceCollect) {
        assert cursor > -1;
        // Defer finalising the task until the run's last sub-frame; until then keep the slot held so the
        // next next() can present the remaining sub-frames. forceCollect (close/abandon) finalises now.
        if (!forceCollect && collectSubFrame + 1 < heldSubFrameCount) {
            return;
        }
        finalizeHeldTask(cursor, forceCollect);
    }

    public T getAtom() {
        return atom;
    }

    // warning: the circuit breaker may be thread unsafe, so don't use it concurrently
    @Override
    public SqlExecutionCircuitBreaker getCircuitBreaker() {
        return sqlExecutionContext.getCircuitBreaker();
    }

    // The tasks handed to the reducers: the dispatched ones but those still staged, which no thread took yet.
    @TestOnly
    public int getDispatchedTaskCount() {
        int count = dispatchStartTaskIndex;
        for (int lane = 0; lane < laneCount; lane++) {
            if (Unsafe.arrayGetVolatile(stagedTasks, lane) != STAGED_NONE) {
                count--;
            }
        }
        return count;
    }

    @TestOnly
    public int getLocalReduceCount() {
        return localReduceCount;
    }

    @TestOnly
    public int getReducedTaskCount() {
        return reduceFinishedCounter.get();
    }

    public int getFrameCount() {
        return frameCount;
    }

    public long getFrameRowCount(int frameIndex) {
        return frameRowCounts.getQuick(frameIndex);
    }

    public long getId() {
        return id;
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public PageFrameAddressCache getPageFrameAddressCache() {
        return frameAddressCache;
    }

    public AtomicInteger getReduceFinishedCounter() {
        return reduceFinishedCounter;
    }

    public AtomicInteger getReduceStartedCounter() {
        return reduceStartedCounter;
    }

    public PageFrameReducer getReducer() {
        return reducer;
    }

    public int getShard() {
        return shard;
    }

    public SqlExecutionContext getSqlExecutionContext() {
        return sqlExecutionContext;
    }

    public long getStartTime() {
        return startTime;
    }

    public SymbolTableSource getSymbolTableSource() {
        return frameCursor;
    }

    public PageFrameReduceTask getTask(long cursor) {
        assert cursor > -1;
        final PageFrameReduceTask task;
        if (isCompletionTracked) {
            task = getResultTask((int) cursor);
        } else if (cursor == LOCAL_TASK_CURSOR) {
            assert localTask != null && localTask.getFrameSequence() != null;
            task = localTask;
        } else {
            task = reduceQueue.get(cursor);
        }
        // Present the current sub-frame of the run as a standalone frame to the unchanged collector.
        task.positionAtSubFrame(collectSubFrame);
        return task;
    }

    public int getTaskCount() {
        return taskCount;
    }

    public int getTaskFirstFrame(int taskIndex) {
        return runFirstFrames.getQuick(taskIndex);
    }

    public int getTaskFrameCount(int taskIndex) {
        final int next = taskIndex + 1 < taskCount ? runFirstFrames.getQuick(taskIndex + 1) : frameCount;
        return next - runFirstFrames.getQuick(taskIndex);
    }

    public byte getTaskType() {
        return taskType;
    }

    @Override
    public boolean isUninterruptible() {
        return uninterruptible;
    }

    public void markAsDone() {
        // prepare to resend the same sequence as it might be required by toTop()
        assert !done;
        done = true;
    }

    public long next() {
        return next(Integer.MAX_VALUE, false);
    }

    /**
     * This method is not thread safe. It's always invoked on a single "query owner" thread.
     * <p>
     * Returns a cursor that points either to the reduce queue or to the local reduce task.
     * The caller of this method should avoid accessing the reduce queue directly and,
     * instead, should use getTask and collect methods. <code>Long.MAX_VALUE</code> is the
     * reserved cursor value for the local reduce task case.
     *
     * <p>
     * A completion-tracked sequence returns the index of the next task once it is reduced, and getTask() returns its
     * result slot.
     *
     * @param dispatchLimit a cap for the number of in-flight tasks
     * @param countOnly     count-only task flag; used only for filter-only tasks
     * @return the next cursor value, or -1 value if the cursor failed and the caller
     * should retry, or -2 if there are no frames to dispatch
     */
    public long next(int dispatchLimit, boolean countOnly) {
        if (taskCount == 0) {
            return -2;
        }

        // Still draining the held task's row-group run: present its next sub-frame without consuming a new
        // collect-queue entry. getTask() positions the task at this sub-frame.
        if (heldCollectCursor != HELD_NONE && collectSubFrame + 1 < heldSubFrameCount) {
            collectSubFrame++;
            return heldCollectCursor;
        }

        if (isCompletionTracked) {
            return nextReduced(dispatchLimit, countOnly);
        }
        assert collectedTaskIndex < taskCount - 1;
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean isFiberSuspendable = isFiberSuspendable();
        final boolean canPark = dispatcher != null && isFiberSuspendable;
        while (true) {
            // Sampled before collectSubSeq.next() so a producer signalling progress between the
            // failed collect and the sample cannot be missed.
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedShardProgress = canPark ? dispatcher.getRingProgressVersion(shard) : 0;
            long cursor = collectSubSeq.next();
            if (cursor > -1) {
                PageFrameReduceTask task = reduceQueue.get(cursor);
                PageFrameSequence<?> thatFrameSequence = task.getFrameSequence();
                if (thatFrameSequence == this) {
                    assert id == task.getFrameSequenceId() : "ids mismatch: " + id + ", " + task.getFrameSequenceId();
                    beginCollectTask(cursor, task);
                    return cursor;
                } else {
                    // Not our task, nothing to collect. Go for another spin.
                    collectSubSeq.done(cursor);
                }
            } else if (cursor == -1) {
                if (dispatcher != null && !isActive()) {
                    if (getCancelReason() != SqlExecutionCircuitBreaker.STATE_OK) {
                        throw buildInterruptionException();
                    }
                    if (dispatchStartTaskIndex == collectedTaskIndex + 1) {
                        return -2;
                    }
                } else {
                    final int previousDispatchStartTaskIndex = dispatchStartTaskIndex;
                    if (dispatch(dispatchLimit, countOnly)
                            && (!isFiberSuspendable || dispatchStartTaskIndex != previousDispatchStartTaskIndex)) {
                        continue;
                    }
                    if (dispatcher != null && !isActive()) {
                        continue;
                    }
                }
                if (dispatchStartTaskIndex == collectedTaskIndex + 1) {
                    reduceLocally(countOnly);
                    beginCollectTask(LOCAL_TASK_CURSOR, localTask);
                    return LOCAL_TASK_CURSOR;
                }
                if (canPark) {
                    final boolean isDraining = !isActive();
                    awaitProgress(dispatcher, observedProgress, observedShardProgress, isDraining, getBlockedShard());
                    continue;
                }
                if (!isFiberSuspendable) {
                    return -1;
                }
                yieldOrPause(true);
            } else {
                Os.pause();
            }
        }
    }

    /**
     * Prepares the sequence for a query. With a collect subscriber, the owner collects tasks in order from the shard's
     * queue. Without one, the sequence is completion-tracked: its tasks reduce into its own result slots, and the
     * owner never holds the queue. Either way the owner drives it through next(), getTask() and collect().
     */
    public PageFrameSequence<T> of(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            @Nullable SCSequence collectSubSeq,
            int order
    ) throws SqlException {
        sqlExecutionContext = executionContext;
        memoryTracker = executionContext.getMemoryTracker();
        startTime = clock.getTicks();
        uninterruptible = executionContext.isUninterruptible();

        if (localRecord == null) {
            localRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
        }

        final Rnd rnd = executionContext.getAsyncRandom();
        try {
            assert frameCursor == null;
            frameCursor = base.getPageFrameCursor(executionContext, order);

            // pass one to cache page addresses
            // this has to be separate pass to ensure there no cache reads
            // while cache might be resizing
            frameAddressCache.of(base.getMetadata(), frameCursor);

            this.collectSubSeq = collectSubSeq;
            isCompletionTracked = collectSubSeq == null;
            errorState.clear();
            id = ID_SEQ.incrementAndGet();
            done = false;
            resetCancellation();
            reduceFinishedCounter.set(0);
            reduceStartedCounter.set(0);
            localReduceCount = 0;
            workStealingStrategy.of(reduceStartedCounter);
            shard = rnd.nextInt(messageBus.getPageFrameReduceShardCount());
            reduceQueue = messageBus.getPageFrameReduceQueue(shard);

            // It is essential to init the atom after we prepared sequence for dispatch.
            // If atom is to fail, we will be releasing whatever we prepared.
            atom.init(frameCursor, executionContext);
            if (UNTRACKED_SCOPE_DEPTH.get()[0] == 0) {
                executionContext.registerPageFrameSequence(this);
                registeredContext = executionContext;
            }
        } catch (TableReferenceOutOfDateException e) {
            frameCursor = Misc.freeIfCloseable(frameCursor);
            throw e;
        } catch (Throwable th) {
            // Log the OG exception as the below frame cursor close call may throw.
            LOG.error().$("could not initialize page frame sequence [error=").$(th).I$();
            frameCursor = Misc.free(frameCursor);
            throw th;
        }
        return this;
    }

    /**
     * Must be called before subsequence calls to {@link #next(int, boolean)} to count page frames and
     * initialize page frame cache and filter functions.
     */
    public void prepareForDispatch() {
        if (!readyToDispatch) {
            buildAddressCache();
            readyToDispatch = true;
        }
    }

    public void reset() {
        // reset() must be called only if there are no tasks in progress for this page frame sequence
        assert taskCount == 0 || reduceFinishedCounter.get() == dispatchStartTaskIndex;
        assert isNoTaskStaged();
        if (registeredContext != null) {
            registeredContext.unregisterPageFrameSequence(this);
            registeredContext = null;
        }

        // prepare different frame sequence using the same object instance
        frameCount = 0;
        taskCount = 0;
        dispatchStartTaskIndex = 0;
        collectedTaskIndex = -1;
        heldCollectCursor = HELD_NONE;
        collectSubFrame = 0;
        heldSubFrameCount = 0;
        readyToDispatch = false;
        // Drop the borrowed tracker reference; the provider owns the native block.
        memoryTracker = null;
        runFirstFrames.clear();
        frameRowCounts.clear();

        Throwable cleanupFailure = null;
        try {
            if (atom != null) {
                atom.clear();
            }
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        // Unfreeze the covered posting readers frozen at dispatch BEFORE the
        // address cache (which holds them) and the frame cursor (which owns them)
        // are torn down. reset() runs after the sequence has been awaited (see the
        // close() paths of the async cursors, which call await() then reset()), so
        // every worker cursor has finished and the unfreeze is race-free. A reader
        // left frozen would make its reloadConditionally() a permanent no-op and
        // break the next query against the same partition.
        final PageFrameAddressCache frameAddressCacheToFree = frameAddressCache;
        if (isClosing) {
            frameAddressCache = null;
        }
        if (frameAddressCacheToFree != null) {
            try {
                frameAddressCacheToFree.unfreezeCoveredReaders();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameAddressCacheToFree);
        final PageFrameCursor frameCursorToFree = frameCursor;
        frameCursor = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameCursorToFree);
        // collect sequence may not be set here when
        // factory is closed without using cursor
        final SCSequence collectSubSeqToRemove = collectSubSeq;
        collectSubSeq = null;
        if (collectSubSeqToRemove != null) {
            try {
                messageBus.getPageFrameCollectFanOut(shard).remove(collectSubSeqToRemove);
                LOG.debug().$("removed [seq=").$(collectSubSeqToRemove).I$();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        if (localTask != null) {
            try {
                localTask.clear();
            } catch (Throwable th) {
                if (cleanupFailure == null) {
                    cleanupFailure = th;
                } else if (cleanupFailure != th) {
                    cleanupFailure.addSuppressed(th);
                }
            }
        }
        // A cached factory keeps its result slots, so they free their memory until its next query.
        for (int i = 0, n = resultTasks.size(); i < n; i++) {
            final PageFrameReduceTask resultTask = resultTasks.getQuick(i);
            if (resultTask != null) {
                cleanupFailure = resultTask.releaseMemory(cleanupFailure);
            }
        }
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Gives the tasks of this completion-tracked sequence lanes, see {@link #laneCount}. A task covers a single frame
     * here, so the lane of a task is that of its frame.
     */
    public void setLaneCount(int laneCount) {
        assert laneCount >= 0 && taskType == PageFrameReduceTask.TYPE_GROUP_BY;
        this.laneCount = laneCount;
        stagedTasks = new int[laneCount];
        Arrays.fill(stagedTasks, STAGED_NONE);
        ownerStagedTasks = new int[laneCount];
        Arrays.fill(ownerStagedTasks, STAGED_NONE);
    }

    // An owner that may not publish, such as a fiber that holds a CPU-limited grant, reduces the tasks itself. Tests
    // turn publication off midway to reach the tasks that the owner staged while it still published.
    @TestOnly
    public void setPublicationRefusedForTesting(boolean isRefused) {
        this.isPublicationRefusedForTesting = isRefused;
    }

    /**
     * Prepares page frame sequence for retrieving the same data set again. The method
     * is not thread-safe.
     */
    public void toTop() {
        if (frameCount > 0) {
            long newId = ID_SEQ.incrementAndGet();
            LOG.debug()
                    .$("toTop [shard=").$(shard)
                    .$(", id=").$(id)
                    .$(", newId=").$(newId)
                    .I$();

            await();
            if (isCompletionTracked) {
                clearReducedTasks();
                errorState.clear();
            } else {
                messageBus.getPageFrameCollectFanOut(shard).resume(collectSubSeq);
            }

            // done is reset by method call above
            done = false;
            id = newId;
            dispatchStartTaskIndex = 0;
            collectedTaskIndex = -1;
            reduceFinishedCounter.set(0);
            reduceStartedCounter.set(0);
            localReduceCount = 0;
            workStealingStrategy.of(reduceStartedCounter);
            resetCancellation();
        }
    }

    // The task that a reducer fills for a task from the queue: its result slot when the sequence is completion-tracked,
    // the task itself otherwise.
    PageFrameReduceTask getReduceTarget(PageFrameReduceTask task) {
        return isCompletionTracked ? getResultTask(task.getTaskIndex()) : task;
    }

    // A thread that fails to reduce a task reports it here, on every path that reduces one: PageFrameReduceJob,
    // PageFrameFiberTask, PageFrameReduceDispatcher and reduceLocally(). The sequence also keeps the first error of a
    // completion-tracked task, which the cancellation makes its owner throw before it reaches the task.
    void onReducerError(PageFrameReduceTask task, Throwable th) {
        getReduceTarget(task).setErrorMsg(th);
        if (!isCompletionTracked || errorState.setError(th)) {
            cancelOnReducerError(th);
        }
    }

    // Every path that ends a task calls this before the task's queue slot goes back to the publishers, whether it
    // reduced, failed, skipped or drained the task: PageFrameReduceJob, PageFrameFiberTask, PageFrameReduceDispatcher
    // and reduceLocally(). A completion-tracked task leaves its output in its result slot, so the queued task only
    // drops its references, and the mark publishes the output to the owner.
    void onTaskReduced(PageFrameReduceTask task) {
        if (isCompletionTracked) {
            final int taskIndex = task.getTaskIndex();
            if (task != getResultTask(taskIndex)) {
                task.detach();
            }
            Unsafe.arrayPutOrdered(reducedTasks, taskIndex, 1);
        }
    }

    long getLaneRowBudget() {
        final long budget = laneRowBudgetForTesting;
        return budget > -1 ? budget : laneRowBudget;
    }

    boolean hasLanes() {
        return laneCount > 0;
    }

    // Takes the staged next task of the reduced task's lane, see stagedTasks, for the thread to reduce it next. A worker
    // path calls this before onTaskReduced() marks the task. The owner takes a staged task only once the mark shows the
    // lane free, so it never takes the task from the thread; the thread's progress signal, which follows the mark,
    // makes the owner publish or reduce a task that it staged after the thread looked. The task does not count as finished yet,
    // so await() cannot end, nor the sequence move on to its next query, while the thread holds the next task.
    @Nullable
    PageFrameReduceTask claimLaneSuccessor(int taskIndex) {
        assert laneCount > 0;
        final int successor = taskIndex + laneCount;
        return Unsafe.cas(stagedTasks, taskIndex % laneCount, successor, STAGED_NONE) ? getResultTask(successor) : null;
    }

    // Ends a lane task that a worker took when the worker fails before it reduces the task: await() waits for the task,
    // so it ends here, failed, rather than never.
    void failLaneTask(PageFrameReduceTask task, Throwable th) {
        onReducerError(task, th);
        try {
            onTaskReduced(task);
        } finally {
            reduceFinishedCounter.incrementAndGet();
        }
    }

    // A worker path calls this right after it takes a lane task, where a failure has to end the task, see failLaneTask().
    static void onLaneTaskClaimed() {
        final Runnable hook = laneClaimHookForTesting;
        if (hook != null) {
            hook.run();
        }
    }

    private void awaitReduced(PageFrameReduceDispatcher dispatcher, boolean isFiberSuspendable) {
        final boolean canPark = dispatcher != null && isFiberSuspendable;
        while (true) {
            final long observedProgress = canPark ? getProgressVersion() : 0;
            if (reduceFinishedCounter.get() >= dispatchStartTaskIndex) {
                break;
            }
            if (canPark) {
                awaitProgress(dispatcher, observedProgress, true);
            } else {
                yieldOrPause(isFiberSuspendable);
            }
        }
    }

    // Waits until every dispatched task is reduced, stealing what it can, so that no task outlives the cursor.
    private void awaitReducedTasks() {
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean isFiberSuspendable = isFiberSuspendable();
        final boolean canPark = dispatcher != null && isFiberSuspendable;
        while (true) {
            final long observedProgress = canPark ? getProgressVersion() : 0;
            skipStagedTasks();
            if (reduceFinishedCounter.get() >= dispatchStartTaskIndex) {
                break;
            }
            boolean isIdle = true;
            try {
                isIdle = PageFrameReduceJob.consumeQueue(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker, this, dispatcher);
            } catch (Throwable th) {
                LOG.error()
                        .$("await error [id=").$(id)
                        .$(", ex=").$(th)
                        .I$();
            }
            if (isIdle) {
                if (canPark) {
                    awaitProgress(dispatcher, observedProgress, true);
                } else {
                    yieldOrPause(isFiberSuspendable);
                }
            }
        }
        // The owner stopped before it collected these tasks, so their result slots let go of the output now.
        while (collectedTaskIndex + 1 < dispatchStartTaskIndex) {
            getResultTask(++collectedTaskIndex).collected(true);
        }
        if (!done) {
            markAsDone();
        }
    }

    private void beginCollectTask(long cursor, PageFrameReduceTask task) {
        heldCollectCursor = cursor;
        collectSubFrame = 0;
        heldSubFrameCount = task.getSubFrameCount();
    }

    private void buildAddressCache() {
        runFirstFrames.clear();
        taskCount = 0;
        boolean prevCustomDecode = false;
        int prevRowGroup = -1;
        int prevPartitionIndex = -1;
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            // Only custom-decode sub-frames of one row group join one task, so the decoder prepares the
            // row group's window once. Other frames stay single-frame tasks: the collector reads their
            // reduce output and never decodes them again.
            final int rowGroup = frame.getParquetRowGroup();
            final int partitionIndex = frame.getPartitionIndex();
            final boolean customDecode = rowGroup >= 0
                    && frame.getPartitionFrameState() != 0
                    && PartitionFrameState.requiresMaterialization(frame.getPartitionFrameState(), rowGroup);
            // Stream GROUP BY has one result slot per in-flight frame and keeps each frame as a task.
            final boolean isSameRun = taskType != PageFrameReduceTask.TYPE_GROUP_BY
                    && frameCount > 0
                    && customDecode
                    && prevCustomDecode
                    && rowGroup == prevRowGroup
                    && partitionIndex == prevPartitionIndex;
            if (!isSameRun) {
                runFirstFrames.add(frameCount);
                taskCount++;
            }
            prevCustomDecode = customDecode;
            prevRowGroup = rowGroup;
            prevPartitionIndex = partitionIndex;
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameAddressCache.add(frameCount++, frame);
        }

        // Covered frames decode their columns on the async workers (in
        // PageFrameMemoryPool.navigateTo) by iterating detached cursors over the
        // shared per-partition posting readers. That is only race-free if the
        // readers are positioned at the query txn, cache-warm, and FROZEN before
        // any worker decodes. The eager production decode above (driven through
        // frameAddressCache.add -> the covering page-frame cursor) already
        // positioned + warmed each reader as a side effect of its full iteration,
        // so freeze them now, before dispatch. unfreezeCoveredReaders() in reset()
        // reverses it once the sequence has been awaited.
        frameAddressCache.freezeCoveredReaders();

        // dispatch tasks only if there is anything to dispatch
        if (frameCount > 0 && isCompletionTracked) {
            clearReducedTasks();
        } else if (frameCount > 0) {
            // We need to subscribe publisher sequence before we return
            // control to the caller of this method. However, this sequence
            // will be unsubscribed asynchronously.
            messageBus.getPageFrameCollectFanOut(shard).and(collectSubSeq);
            LOG.debug()
                    .$("added [shard=").$(shard)
                    .$(", id=").$(id)
                    .$(", seqCurrent=").$(collectSubSeq.current())
                    .$(", seq=").$(collectSubSeq)
                    .I$();
        }
    }

    // Runs before dispatch and after await(), so no thread marks a task meanwhile.
    private void clearReducedTasks() {
        resultTasks.setPos(reduceQueue.getCycle());
        if (reducedTasks.length < taskCount) {
            reducedTasks = new int[Numbers.ceilPow2(taskCount)];
        } else {
            Arrays.fill(reducedTasks, 0, taskCount, 0);
        }
    }

    /**
     * This method is re-enterable. It has to be in case queue capacity or the dispatch limit is smaller
     * than number of tasks to be dispatched. When it is the case, task count published so far is
     * stored in the `dispatchStartTaskIndex` field. This method has no responsibility to deal with
     * "collect" stage hence it deals with everything to unblock the collect stage.
     *
     * @param dispatchLimit a cap for the number of in-flight tasks
     * @param countOnly     count-only task flag; used only for filter-only tasks
     * @return true if at least one task was dispatched or reduced; false otherwise
     */
    private boolean dispatch(int dispatchLimit, boolean countOnly) {
        // the sequence used to steal worker jobs
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        return dispatch0(dispatchLimit, countOnly, dispatcher, reduceSubSeq, reducePubSeq);
    }

    private boolean dispatch0(
            int dispatchLimit,
            boolean countOnly,
            PageFrameReduceDispatcher dispatcher,
            MCSequence reduceSubSeq,
            MPSequence reducePubSeq
    ) {
        boolean hasPublication = dispatcher == null || dispatcher.tryAcquirePublication();
        boolean idle = true;
        boolean dispatched = false;
        final int collectedTaskCount = collectedTaskIndex + 1;

        try {
            if (!hasPublication) {
                if (!dispatcher.isCurrentFiberOwned()) {
                    cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                }
                return false;
            }
            long cursor;
            int i = dispatchStartTaskIndex;
            OUT:
            for (; i < taskCount; i++) {
                // We cannot process work on this thread. If we do the consumer will
                // never get the executions results. Consumer only picks ready to go
                // tasks from the queue.

                while (true) {
                    final int totalDispatched = dispatchStartTaskIndex - collectedTaskCount;
                    // Treat situation when we hit the dispatch limit as if it was a full queue (-1).
                    if (totalDispatched >= dispatchLimit) {
                        cursor = -1;
                    } else {
                        cursor = reducePubSeq.next();
                        if (cursor > -1) {
                            reduceQueue.get(cursor).of(this, i, countOnly);
                            reducePubSeq.done(cursor);
                        }
                    }
                    if (cursor > -1) {
                        LOG.debug()
                                .$("dispatched [shard=").$(shard)
                                .$(", id=").$(getId())
                                .$(", taskIndex=").$(i)
                                .$(", taskCount=").$(taskCount)
                                .$(", cursor=").$(cursor)
                                .I$();
                        dispatchStartTaskIndex = i + 1;
                        dispatched = true;
                        break;
                    } else if (cursor == -1) {
                        if (!workStealingStrategy.shouldSteal(collectedTaskCount)) {
                            return dispatched;
                        }
                        // start stealing work to unload the queue
                        idle = false;
                        if (dispatcher != null) {
                            dispatcher.releasePublication();
                            hasPublication = false;
                        }
                        if (stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker)) {
                            if (reduceFinishedCounter.get() > collectedTaskCount) {
                                // We have something to collect, so let's do it!
                                return true;
                            }
                            if (dispatcher != null) {
                                hasPublication = dispatcher.tryAcquirePublication();
                                if (!hasPublication) {
                                    if (!dispatcher.isCurrentFiberOwned()) {
                                        cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                                    }
                                    return dispatched;
                                }
                            }
                            continue;
                        }
                        break OUT;
                    } else {
                        Os.pause();
                    }
                }
            }

            if (dispatcher != null && hasPublication) {
                dispatcher.releasePublication();
                hasPublication = false;
            }

            if (reduceFinishedCounter.get() > collectedTaskCount) {
                // We have something to collect, so let's do it!
                return true;
            }

            // Reduce counter is here to provide safe backoff point
            // for job stealing code. It is needed because queue is shared
            // and there is possibility of never ending stealing if we don't
            // specifically count only our items

            // join the gang to consume published tasks
            while (reduceFinishedCounter.get() < dispatchStartTaskIndex) {
                idle = false;
                // The owner may take its own published frames here, so it lets the strategy hold
                // it back first. An inactive sequence only drains, which the strategy must not hold.
                if (isActive()) {
                    workStealingStrategy.onBeforeDirectSteal();
                }
                if (stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker)) {
                    if (isActive()) {
                        continue;
                    }
                }
                break;
            }

            if (idle) {
                stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker);
            }

            return dispatched;
        } finally {
            if (dispatcher != null && hasPublication) {
                dispatcher.releasePublication();
            }
        }
    }

    // Publishes tasks up to the limit without stealing, so that the workers keep reducing while the owner works
    // through the tasks it collected.
    private void dispatchAhead(int dispatchLimit, boolean countOnly) {
        if (!isActive() || isPublicationRefusedForTesting) {
            return;
        }
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        if (dispatcher != null && !dispatcher.tryAcquirePublication()) {
            // As in dispatch0(), a quiescing dispatcher stops a query that none of its fibers runs, rather than leave
            // the owner to reduce every frame.
            if (!dispatcher.isCurrentFiberOwned()) {
                cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
            }
            return;
        }
        try {
            final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);
            final int collectedTaskCount = collectedTaskIndex + 1;
            final int limit = Math.min(dispatchLimit, resultTasks.size());
            int lane = 0;
            while (true) {
                int taskIndex = STAGED_NONE;
                // The staged tasks that the owner publishes go first: they are older than the tasks it dispatches.
                while (taskIndex < 0 && lane < laneCount) {
                    taskIndex = ownedStagedTask(lane++);
                }
                if (taskIndex < 0) {
                    taskIndex = nextTaskToDispatch(collectedTaskCount, limit, countOnly);
                    if (taskIndex < 0) {
                        return;
                    }
                    // Binding the result slot may allocate, so it comes before the queue slot: a failure must not
                    // leave a claimed slot unpublished, which would stop every consumer of the shard.
                    prepareResultTask(taskIndex, countOnly);
                }
                if (!publishTask(taskIndex, getResultTask(taskIndex).isCountOnly(), reducePubSeq)) {
                    return;
                }
                if (taskIndex < dispatchStartTaskIndex) {
                    unstageTask(taskIndex);
                } else {
                    dispatchStartTaskIndex++;
                }
            }
        } finally {
            if (dispatcher != null) {
                dispatcher.releasePublication();
            }
        }
    }

    // Finalises the held task: marks its run collected, releases its frame memory, and (for a queue task)
    // frees the collect-queue slot. Called on the run's last sub-frame and on force-collect / abandon.
    private void finalizeHeldTask(long cursor, boolean forceCollect) {
        if (isCompletionTracked) {
            final PageFrameReduceTask task = getResultTask((int) cursor);
            collectedTaskIndex = task.getTaskIndex();
            task.collected(forceCollect);
        } else if (cursor == LOCAL_TASK_CURSOR) {
            collectedTaskIndex = localTask.getTaskIndex();
            localTask.collected();
            if (done) {
                suspendCollectSubSeq();
            }
        } else {
            PageFrameReduceTask task = reduceQueue.get(cursor);
            collectedTaskIndex = task.getTaskIndex();
            task.collected(forceCollect);
            collectSubSeq.done(cursor);
            if (done) {
                suspendCollectSubSeq();
            }
        }
        heldCollectCursor = HELD_NONE;
    }

    private int getBlockedShard() {
        return reduceFinishedCounter.get() > collectedTaskIndex + 1 ? shard : -1;
    }

    private PageFrameReduceTask getResultTask(int taskIndex) {
        return resultTasks.getQuick(taskIndex % resultTasks.size());
    }

    // The task is the first of its lane, or its lane's previous task is reduced. The mark that shows it also publishes
    // what that task wrote to the lane.
    private boolean isLaneFree(int taskIndex) {
        return taskIndex < laneCount || laneCount == 0 || Unsafe.arrayGetVolatile(reducedTasks, taskIndex - laneCount) != 0;
    }

    private boolean isNoTaskStaged() {
        for (int lane = 0; lane < laneCount; lane++) {
            if (ownerStagedTasks[lane] != STAGED_NONE || Unsafe.arrayGetVolatile(stagedTasks, lane) != STAGED_NONE) {
                return false;
            }
        }
        return true;
    }

    // The completion-tracked part of next(): returns the next task in order once it is reduced. While it waits, the
    // owner publishes tasks up to the dispatch limit, steals work from its shard, reduces the task itself when the shard
    // has no room for it, or parks.
    private long nextReduced(int dispatchLimit, boolean countOnly) {
        final int taskIndex = collectedTaskIndex + 1;
        assert taskIndex < taskCount;
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final boolean isFiberSuspendable = isFiberSuspendable();
        final boolean canPark = dispatcher != null && isFiberSuspendable;
        while (true) {
            // Sampled before the checks below, so that progress signalled in between wakes the park.
            final long observedProgress = canPark ? getProgressVersion() : 0;
            // Keeps the workers busy while the caller reads this task's output.
            dispatchAhead(dispatchLimit, countOnly);
            if (!isActive()) {
                if (errorState.hasError()) {
                    throw errorState.buildException();
                }
                if (getCancelReason() != SqlExecutionCircuitBreaker.STATE_OK) {
                    throw buildInterruptionException();
                }
                // The owner stopped the sequence, as a LIMIT does: it collects the tasks in flight and no more.
                if (taskIndex == dispatchStartTaskIndex) {
                    return -2;
                }
            }
            if (Unsafe.arrayGetVolatile(reducedTasks, taskIndex) != 0) {
                beginCollectTask(taskIndex, getResultTask(taskIndex));
                return taskIndex;
            }
            if (laneCount > 0 && ownedStagedTask(taskIndex % laneCount) == taskIndex) {
                // The task is staged, its lane is free and no thread took it, yet dispatchAhead() did not publish it:
                // the owner may not publish, or the shard has no room. No thread but the owner reduces it now.
                reduceStagedLocally(taskIndex);
                continue;
            }
            if (taskIndex == dispatchStartTaskIndex) {
                // The shard has no room for the task. Stealing frees a slot; with nothing to steal, reduce it here.
                if (!stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker)) {
                    reduceLocally(countOnly);
                }
                continue;
            }
            workStealingStrategy.onBeforeDirectSteal();
            if (stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker)) {
                continue;
            }
            if (canPark) {
                awaitProgress(dispatcher, observedProgress, false);
                continue;
            }
            if (!isFiberSuspendable) {
                return -1;
            }
            yieldOrPause(true);
        }
    }

    // Binds the result slot of a completion-tracked task before the task is published or reduced locally. The dispatch
    // limit keeps the task that used the slot before collected by now.
    private PageFrameReduceTask prepareResultTask(int taskIndex, boolean countOnly) {
        final int slot = taskIndex % resultTasks.size();
        PageFrameReduceTask task = resultTasks.getQuick(slot);
        if (task == null) {
            task = localTaskFactory.getInstance();
            task.setTaskType(taskType);
            resultTasks.setQuick(slot, task);
        }
        task.of(this, taskIndex, countOnly);
        return task;
    }

    // The next task to dispatch while its lane is free, or STAGED_NONE. Stages the tasks it passes whose lane's
    // previous task is in flight, as long as their lanes hold no staged task.
    private int nextTaskToDispatch(int collectedTaskCount, int limit, boolean countOnly) {
        while (dispatchStartTaskIndex < taskCount && dispatchStartTaskIndex - collectedTaskCount < limit) {
            if (isLaneFree(dispatchStartTaskIndex)) {
                return dispatchStartTaskIndex;
            }
            if (!stageTask(dispatchStartTaskIndex, countOnly)) {
                break;
            }
        }
        return STAGED_NONE;
    }

    // The staged task of the lane when it is the owner's, or STAGED_NONE: the lane is free, so the thread that ended its
    // previous task took the staged task before it marked its own, if at all. Forgets a task that the thread took.
    private int ownedStagedTask(int lane) {
        final int taskIndex = ownerStagedTasks[lane];
        if (taskIndex == STAGED_NONE || !isLaneFree(taskIndex)) {
            return STAGED_NONE;
        }
        if (Unsafe.arrayGetVolatile(stagedTasks, lane) != taskIndex) {
            ownerStagedTasks[lane] = STAGED_NONE;
            return STAGED_NONE;
        }
        return taskIndex;
    }

    // Publishes a task bound to its result slot. Returns false when the shard's queue has no room.
    private boolean publishTask(int taskIndex, boolean countOnly, MPSequence reducePubSeq) {
        while (true) {
            final long cursor = reducePubSeq.next();
            if (cursor > -1) {
                reduceQueue.get(cursor).of(this, taskIndex, countOnly);
                reducePubSeq.done(cursor);
                return true;
            }
            if (cursor == -1) {
                return false;
            }
            Os.pause();
        }
    }

    private void reduceLocally(boolean countOnly) {
        assert dispatchStartTaskIndex < taskCount;

        final PageFrameReduceTask task;
        if (isCompletionTracked) {
            // The owner reduces only the next task to collect, so it has collected the previous task of the lane.
            assert isLaneFree(dispatchStartTaskIndex);
            task = prepareResultTask(dispatchStartTaskIndex, countOnly);
        } else {
            if (localTask == null) {
                localTask = localTaskFactory.getInstance();
                localTask.setTaskType(taskType);
            }
            localTask.of(this, dispatchStartTaskIndex, countOnly);
            task = localTask;
        }
        // Counted once the task is bound, so that await() never waits for a task that failed to bind.
        dispatchStartTaskIndex++;
        reduceLocally(task);
    }

    // Reduces a bound task on the owner's thread and ends it.
    private void reduceLocally(PageFrameReduceTask task) {
        localReduceCount++;

        final boolean isFiberSuspendable = isFiberSuspendable();
        final SuspensionScope.CarrierScope suspensionScope = isFiberSuspendable
                ? null
                : SuspensionScope.scope();
        final SuspensionScope.Mode previousMode = isFiberSuspendable
                ? null
                : SuspensionScope.enterBlocking(suspensionScope);
        final FiberCancellationSignal previousCancellationSignal = isFiberSuspendable
                ? SuspensionScope.getCancellationSignal()
                : null;
        final long previousCancellationSignalGeneration = isFiberSuspendable
                ? SuspensionScope.getCancellationSignalGeneration()
                : CancellationBinding.NO_GENERATION;
        final FiberCancellationSignal previousSupplementalCancellationSignal = isFiberSuspendable
                ? SuspensionScope.getSupplementalCancellationSignal()
                : null;
        final long previousSupplementalCancellationSignalGeneration = isFiberSuspendable
                ? SuspensionScope.getSupplementalCancellationSignalGeneration()
                : CancellationBinding.NO_GENERATION;
        if (isFiberSuspendable) {
            enterReducerCancellationScope();
        }
        try {
            LOG.debug()
                    .$("reducing locally [shard=").$(shard)
                    .$(", id=").$(id)
                    .$(", taskType=").$(taskType)
                    .$(", taskIndex=").$(task.getTaskIndex())
                    .$(", taskCount=").$(taskCount)
                    .$(", active=").$(isActive())
                    .I$();
            if (isActive()) {
                workStealCircuitBreaker.init(sqlExecutionContext.getCircuitBreaker());
                PageFrameReduceJob.reduce(localRecord, workStealCircuitBreaker, task, this, this);
            }
        } catch (Throwable th) {
            if (isReducerFailureReportable(th)) {
                LOG.error()
                        .$("local reduce error [error=").$(th)
                        .$(", id=").$(id)
                        .$(", taskType=").$(taskType)
                        .$(", taskIndex=").$(task.getTaskIndex())
                        .$(", taskCount=").$(taskCount)
                        .I$();
                // Route the error through the local task so the collector sees it via
                // task.hasError() and can re-throw the original class via task.buildError().
                // Re-throwing here would let the outer catch in the collector wrap the
                // typed exception into a generic CairoException, losing the original class.
                onReducerError(task, th);
            }
        } finally {
            if (isFiberSuspendable) {
                SuspensionScope.restoreCancellationSignal(
                        previousCancellationSignal,
                        previousCancellationSignalGeneration
                );
                SuspensionScope.enterSupplementalCancellationSignal(
                        previousSupplementalCancellationSignal,
                        previousSupplementalCancellationSignalGeneration
                );
            } else {
                SuspensionScope.restoreMode(suspensionScope, previousMode);
            }
            try {
                onTaskReduced(task);
            } finally {
                reduceFinishedCounter.incrementAndGet();
            }
        }
    }

    // Reduces the staged next task to collect on the owner's thread, see nextReduced(). The task holds its result slot
    // and counts as dispatched since the owner staged it.
    private void reduceStagedLocally(int taskIndex) {
        unstageTask(taskIndex);
        reduceLocally(getResultTask(taskIndex));
    }

    // Once the owner stops, it ends the staged tasks of the free lanes, which no thread took, without reducing them. A
    // lane whose previous task is in flight waits for that task: its thread may still take the staged task.
    private void skipStagedTasks() {
        for (int lane = 0; lane < laneCount; lane++) {
            final int taskIndex = ownedStagedTask(lane);
            if (taskIndex > -1) {
                unstageTask(taskIndex);
                try {
                    onTaskReduced(getResultTask(taskIndex));
                } finally {
                    reduceFinishedCounter.incrementAndGet();
                }
            }
        }
    }

    // Stages a task whose lane's previous task is in flight, see stagedTasks. Returns false when the lane holds a staged
    // task already.
    private boolean stageTask(int taskIndex, boolean countOnly) {
        final int lane = taskIndex % laneCount;
        if (ownerStagedTasks[lane] != STAGED_NONE) {
            return false;
        }
        prepareResultTask(taskIndex, countOnly);
        // Publishes the bound result slot to the thread that takes the task.
        Unsafe.arrayPutOrdered(stagedTasks, lane, taskIndex);
        ownerStagedTasks[lane] = taskIndex;
        dispatchStartTaskIndex++;
        return true;
    }

    private void unstageTask(int taskIndex) {
        final int lane = taskIndex % laneCount;
        ownerStagedTasks[lane] = STAGED_NONE;
        Unsafe.arrayPutOrdered(stagedTasks, lane, STAGED_NONE);
    }

    private boolean stealWork(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence reduceSubSeq,
            PageFrameMemoryRecord record,
            SqlExecutionCircuitBreakerWrapper circuitBreaker
    ) {
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean isEmpty = PageFrameReduceJob.consumeQueue(
                queue,
                reduceSubSeq,
                record,
                circuitBreaker,
                this,
                dispatcher
        );
        if (isEmpty) {
            Os.pause();
            return false;
        }
        return true;
    }

    private void suspendCollectSubSeq() {
        messageBus.getPageFrameCollectFanOut(shard).suspend(collectSubSeq);
    }
}
