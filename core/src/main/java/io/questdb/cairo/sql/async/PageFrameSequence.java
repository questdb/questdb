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
import io.questdb.cairo.sql.RecordMetadata;
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
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PageFrameSequence<T extends StatefulAtom> extends AbstractPageFrameSequence implements Closeable {
    private static final long HELD_NONE = -1;
    private static final AtomicLong ID_SEQ = new AtomicLong();
    private static final long LOCAL_TASK_CURSOR = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(PageFrameSequence.class);
    private static final FiberLocal<int[]> UNTRACKED_SCOPE_DEPTH = new FiberLocal<>(() -> new int[1]);
    private final MillisecondClock clock;
    private final LongList frameRowCounts = new LongList();
    // When true, every frame is a task of its own, so the dispatch limit bounds the number of frames
    // in flight, not just the number of tasks; see buildAddressCache().
    private final boolean isFramePerTask;
    private final PageFrameReduceTaskFactory localTaskFactory;
    private final MessageBus messageBus;
    private final AtomicInteger reduceFinishedCounter = new AtomicInteger(0);
    private final AtomicInteger reduceStartedCounter = new AtomicInteger(0);
    private final PageFrameReducer reducer;
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
    private PageFrameMemoryRecord localRecord;
    // Local reduce task used when there is no slots in the queue to dispatch tasks.
    private PageFrameReduceTask localTask;
    // Per-query native memory tracker captured from the owning SqlExecutionContext
    // at workload start. Null when no per-query limit is configured. Workers read
    // this off the task via task.getFrameSequence().getMemoryTracker() to charge
    // their allocations to the active workload.
    private MemoryTracker memoryTracker;
    private boolean readyToDispatch;
    private RingQueue<PageFrameReduceTask> reduceQueue;
    private SqlExecutionContext registeredContext;
    private int shard;
    private SqlExecutionContext sqlExecutionContext;
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
        this(engine, configuration, messageBus, atom, reducer, localTaskFactory, sharedQueryWorkerCount, taskType, false);
    }

    /**
     * Constructs a page frame sequence instance. The returned instance takes ownership of the input atom.
     *
     * @param isFramePerTask when true, every frame becomes a task of its own. Without it, the sequence
     *                       groups the sub-frames of a custom-decode row group into a single task, so
     *                       the frames in flight can outnumber the tasks that the dispatch limit
     *                       bounds. A caller that sizes per-frame state by the dispatch limit sets it
     */
    public PageFrameSequence(
            CairoEngine engine,
            CairoConfiguration configuration,
            MessageBus messageBus,
            T atom,
            PageFrameReducer reducer,
            PageFrameReduceTaskFactory localTaskFactory,
            int sharedQueryWorkerCount,
            byte taskType,
            boolean isFramePerTask
    ) {
        this.isFramePerTask = isFramePerTask;
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
            this.workStealCircuitBreaker = new SqlExecutionCircuitBreakerWrapper(engine, configuration.getCircuitBreakerConfiguration());
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

    /**
     * Publishes further tasks (not count-only) to the reduce queue while the number of tasks in
     * flight stays below {@code dispatchLimit} and the queue has room. Tasks in flight are those
     * dispatched but not yet collected by the owner, including the one the owner currently holds.
     * The method never collects, runs, or steals a task and never waits, so its only cost to the
     * owner is the publication itself. A task is either one frame or a run of sub-frames that
     * {@link #buildAddressCache()} groups together, unless the sequence makes every frame a task of
     * its own.
     * <p>
     * {@link #next(int, boolean)} publishes tasks only when none is ready to collect. When the owner
     * spends about as long reading a frame as a worker spends reducing one, the window drains and
     * the owner then waits for a whole task on every refill. To avoid that, such an owner calls
     * this method after next() returns a frame, so the workers reduce the following tasks while the
     * owner reads the current one. Passing the same limit as to next(), or a smaller one, keeps the
     * tasks in flight within what next() would allow.
     * <p>
     * Publishes nothing if the sequence is cancelled or the fiber dispatcher refuses the
     * publication, and stops early once the queue is full or another publisher takes the slot. In
     * those cases, the next call to next() dispatches, steals, reduces locally or cancels, as usual.
     * This method is not thread safe; only the query owner thread calls it, as it does next().
     *
     * @param dispatchLimit the maximum number of tasks in flight, including the one the owner holds
     */
    public void dispatchAhead(int dispatchLimit) {
        if (dispatchStartTaskIndex >= taskCount
                || dispatchStartTaskIndex - (collectedTaskIndex + 1) >= dispatchLimit
                || !isActive()) {
            return;
        }
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        if (dispatcher != null && !dispatcher.tryAcquirePublication()) {
            return;
        }
        try {
            final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);
            while (dispatchStartTaskIndex < taskCount
                    && dispatchStartTaskIndex - (collectedTaskIndex + 1) < dispatchLimit) {
                final long cursor = reducePubSeq.next();
                if (cursor < 0) {
                    // The queue is full (-1) or another publisher took the slot (-2). The owner
                    // publishes again when it takes its next frame, and next() steals or waits
                    // as usual.
                    break;
                }
                reduceQueue.get(cursor).of(this, dispatchStartTaskIndex, false);
                reducePubSeq.done(cursor);
                LOG.debug()
                        .$("dispatched ahead [shard=").$(shard)
                        .$(", id=").$(id)
                        .$(", taskIndex=").$(dispatchStartTaskIndex)
                        .$(", taskCount=").$(taskCount)
                        .$(", cursor=").$(cursor)
                        .I$();
                dispatchStartTaskIndex++;
            }
        } finally {
            if (dispatcher != null) {
                dispatcher.releasePublication();
            }
        }
    }

    public T getAtom() {
        return atom;
    }

    // warning: the circuit breaker may be thread unsafe, so don't use it concurrently
    @Override
    public SqlExecutionCircuitBreaker getCircuitBreaker() {
        return sqlExecutionContext.getCircuitBreaker();
    }

    /**
     * Returns the number of frames the sequence handed out since it was bound or rewound, either
     * published to the reduce queue or reduced locally, whether or not their tasks have run yet.
     * A task hands out all frames of its run at once.
     */
    public int getDispatchedFrameCount() {
        return dispatchStartTaskIndex < taskCount ? runFirstFrames.getQuick(dispatchStartTaskIndex) : frameCount;
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
        if (cursor == LOCAL_TASK_CURSOR) {
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
     * Opens the base factory's page frame cursor in the given order and binds the sequence to it;
     * see {@link #of(PageFrameCursor, RecordMetadata, SqlExecutionContext, SCSequence)}.
     */
    public PageFrameSequence<T> of(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            SCSequence collectSubSeq,
            int order
    ) throws SqlException {
        // Capture the memory tracker and start time before opening the cursor: opening the query's
        // outermost factory registers the query, which may bind a tracker of its own.
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        final long startTime = clock.getTicks();
        return of0(
                base.getPageFrameCursor(executionContext, order),
                base.getMetadata(),
                executionContext,
                collectSubSeq,
                memoryTracker,
                startTime
        );
    }

    /**
     * Binds the sequence to an execution over a page frame cursor that the caller has already
     * opened, and initializes the atom. The sequence dispatches and collects frames in the cursor's
     * scan order.
     * <p>
     * The sequence takes ownership of the cursor whether this method returns or throws: on failure
     * it closes the cursor right away, otherwise {@link #reset()} closes it. Callers that need to
     * inspect a cursor before deciding its role use this overload; for example, a hash join opens
     * its inputs' cursors to compare their sizes and then picks the build side.
     *
     * @param frameCursor   the open page frame cursor; the sequence takes ownership of it
     * @param metadata      the metadata of the factory that opened the cursor
     * @param collectSubSeq the subscriber sequence through which the owner collects reduced frames
     */
    public PageFrameSequence<T> of(
            PageFrameCursor frameCursor,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            SCSequence collectSubSeq
    ) throws SqlException {
        return of0(frameCursor, metadata, executionContext, collectSubSeq, executionContext.getMemoryTracker(), clock.getTicks());
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
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Lowers the maximum number of rows in the native frames that {@link #prepareForDispatch()}
     * reads from the bound page frame cursor; see {@link PageFrameCursor#setPageFrameMaxRows(long)}.
     * Call it before {@link #prepareForDispatch()}; the frames that method counts stay fixed for
     * the rest of the execution.
     */
    public void setPageFrameMaxRows(long maxRows) {
        assert !readyToDispatch : "the frames are counted already";
        frameCursor.setPageFrameMaxRows(maxRows);
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
            messageBus.getPageFrameCollectFanOut(shard).resume(collectSubSeq);

            // done is reset by method call above
            done = false;
            id = newId;
            dispatchStartTaskIndex = 0;
            collectedTaskIndex = -1;
            reduceFinishedCounter.set(0);
            reduceStartedCounter.set(0);
            workStealingStrategy.of(reduceStartedCounter);
            resetCancellation();
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
            // reduce output and never decodes them again. With isFramePerTask set, no frames are
            // grouped.
            final int rowGroup = frame.getParquetRowGroup();
            final int partitionIndex = frame.getPartitionIndex();
            final boolean customDecode = rowGroup >= 0
                    && frame.getPartitionFrameState() != 0
                    && PartitionFrameState.requiresMaterialization(frame.getPartitionFrameState(), rowGroup);
            final boolean isSameRun = !isFramePerTask
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
        if (frameCount > 0) {
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

    // Finalises the held task: marks its run collected, releases its frame memory, and (for a queue task)
    // frees the collect-queue slot. Called on the run's last sub-frame and on force-collect / abandon.
    private void finalizeHeldTask(long cursor, boolean forceCollect) {
        if (cursor == LOCAL_TASK_CURSOR) {
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

    private PageFrameSequence<T> of0(
            PageFrameCursor frameCursor,
            RecordMetadata metadata,
            SqlExecutionContext executionContext,
            SCSequence collectSubSeq,
            MemoryTracker memoryTracker,
            long startTime
    ) throws SqlException {
        assert this.frameCursor == null : "reset() the sequence before binding it again";
        this.frameCursor = frameCursor;
        sqlExecutionContext = executionContext;
        this.memoryTracker = memoryTracker;
        this.startTime = startTime;
        uninterruptible = executionContext.isUninterruptible();

        final Rnd rnd = executionContext.getAsyncRandom();
        try {
            if (localRecord == null) {
                localRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            }

            // pass one to cache page addresses
            // this has to be separate pass to ensure there no cache reads
            // while cache might be resizing
            frameAddressCache.of(metadata, frameCursor);

            this.collectSubSeq = collectSubSeq;
            id = ID_SEQ.incrementAndGet();
            done = false;
            resetCancellation();
            reduceFinishedCounter.set(0);
            reduceStartedCounter.set(0);
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
            this.frameCursor = null;
            Misc.freeIfCloseable(frameCursor);
            throw e;
        } catch (Throwable th) {
            // Log the OG exception as the below frame cursor close call may throw.
            LOG.error().$("could not initialize page frame sequence [error=").$(th).I$();
            this.frameCursor = null;
            Misc.free(frameCursor);
            throw th;
        }
        return this;
    }

    private void reduceLocally(boolean countOnly) {
        assert dispatchStartTaskIndex < taskCount;

        if (localTask == null) {
            localTask = localTaskFactory.getInstance();
            localTask.setTaskType(taskType);
        }
        localTask.of(this, dispatchStartTaskIndex++, countOnly);

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
                    .$(", taskIndex=").$(localTask.getTaskIndex())
                    .$(", taskCount=").$(taskCount)
                    .$(", active=").$(isActive())
                    .I$();
            if (isActive()) {
                workStealCircuitBreaker.init(sqlExecutionContext.getCircuitBreaker());
                PageFrameReduceJob.reduce(localRecord, workStealCircuitBreaker, localTask, this, this);
            }
        } catch (Throwable th) {
            if (isReducerFailureReportable(th)) {
                LOG.error()
                        .$("local reduce error [error=").$(th)
                        .$(", id=").$(id)
                        .$(", taskType=").$(taskType)
                        .$(", taskIndex=").$(localTask.getTaskIndex())
                        .$(", taskCount=").$(taskCount)
                        .I$();
                // Route the error through the local task so the collector sees it via
                // task.hasError() and can re-throw the original class via task.buildError().
                // Re-throwing here would let the outer catch in the collector wrap the
                // typed exception into a generic CairoException, losing the original class.
                localTask.setErrorMsg(th);
                cancelOnReducerError(th);
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
            reduceFinishedCounter.incrementAndGet();
        }
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
