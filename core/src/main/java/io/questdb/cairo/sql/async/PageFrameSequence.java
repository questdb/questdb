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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.continuation.CancellationBinding;
import io.questdb.mp.continuation.FiberCancellationSignal;
import io.questdb.mp.continuation.SuspensionScope;
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
    private static final AtomicLong ID_SEQ = new AtomicLong();
    private static final long LOCAL_TASK_CURSOR = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(PageFrameSequence.class);
    private final MillisecondClock clock;
    private final LongList frameRowCounts = new LongList();
    private final PageFrameReduceTaskFactory localTaskFactory;
    private final MessageBus messageBus;
    private final AtomicInteger reduceFinishedCounter = new AtomicInteger(0);
    private final AtomicInteger reduceStartedCounter = new AtomicInteger(0);
    private final PageFrameReducer reducer;
    private final byte taskType; // PageFrameReduceTask.TYPE_*
    private final WorkStealingStrategy workStealingStrategy;
    public volatile boolean done;
    private T atom;
    private SCSequence collectSubSeq;
    private int collectedFrameIndex = -1;
    private int dispatchStartFrameIndex;
    private PageFrameAddressCache frameAddressCache;
    private int frameCount;
    private PageFrameCursor frameCursor;
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
    private int shard;
    private SqlExecutionContext sqlExecutionContext;
    private long startTime;
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
    public void await() {
        LOG.debug()
                .$("awaiting completion [shard=").$(shard)
                .$(", id=").$(id)
                .$(", frameCount=").$(frameCount)
                .I$();

        final MCSequence pageFrameReduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean canPark = dispatcher != null && isFiberSuspendable();
        while (!done) {
            // Sampled before the work checks below: a producer that signals progress after the
            // checks but before the sample would otherwise be missed and this fiber would park.
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedGlobalProgress = canPark ? dispatcher.getProgressVersion() : 0;
            // First check the local task: maybe we were reducing locally and got interrupted by an exception?
            if (localTask != null && localTask.getFrameSequence() == this && dispatchStartFrameIndex == localTask.getFrameIndex() + 1) {
                collectedFrameIndex = localTask.getFrameIndex();
                localTask.collected(true);
            }

            if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
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
                        collectedFrameIndex = task.getFrameIndex();
                        task.collected(true);
                    }
                    collectSubSeq.done(cursor);
                    if (dispatcher != null) {
                        if (taskFrameSequence != null) {
                            dispatcher.signalProgress(taskFrameSequence);
                        } else {
                            dispatcher.signalProgress();
                        }
                    }
                } else if (canPark) {
                    awaitProgress(dispatcher, observedProgress, observedGlobalProgress, true);
                } else {
                    Os.pause();
                }
            }
        }

        // It could be the case that one of the workers reduced a page frame, then marked the task as done,
        // but haven't incremented reduce counter yet. In this case, we wait for the desired counter value.
        while (true) {
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedGlobalProgress = canPark ? dispatcher.getProgressVersion() : 0;
            if (reduceFinishedCounter.get() == dispatchStartFrameIndex) {
                break;
            }
            if (canPark) {
                awaitProgress(dispatcher, observedProgress, observedGlobalProgress, true);
            } else {
                Os.pause();
            }
        }
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
        if (cursor == LOCAL_TASK_CURSOR) {
            collectedFrameIndex = localTask.getFrameIndex();
            localTask.collected();
            return;
        }
        PageFrameReduceTask task = reduceQueue.get(cursor);
        collectedFrameIndex = task.getFrameIndex();
        task.collected(forceCollect);
        collectSubSeq.done(cursor);
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        if (dispatcher != null) {
            dispatcher.signalProgress(this);
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
        if (cursor == LOCAL_TASK_CURSOR) {
            assert localTask != null && localTask.getFrameSequence() != null;
            return localTask;
        }
        return reduceQueue.get(cursor);
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
        if (frameCount == 0) {
            return -2;
        }

        assert collectedFrameIndex < frameCount - 1;
        final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
        final boolean canPark = dispatcher != null && isFiberSuspendable();
        while (true) {
            // Sampled before collectSubSeq.next() so a producer signalling progress between the
            // failed collect and the sample cannot be missed.
            final long observedProgress = canPark ? getProgressVersion() : 0;
            final long observedGlobalProgress = canPark ? dispatcher.getProgressVersion() : 0;
            long cursor = collectSubSeq.next();
            if (cursor > -1) {
                PageFrameReduceTask task = reduceQueue.get(cursor);
                PageFrameSequence<?> thatFrameSequence = task.getFrameSequence();
                if (thatFrameSequence == this) {
                    assert id == task.getFrameSequenceId() : "ids mismatch: " + id + ", " + task.getFrameSequenceId();
                    return cursor;
                } else {
                    // Not our task, nothing to collect. Go for another spin.
                    collectSubSeq.done(cursor);
                    if (dispatcher != null) {
                        if (thatFrameSequence != null) {
                            dispatcher.signalProgress(thatFrameSequence);
                        } else {
                            dispatcher.signalProgress();
                        }
                    }
                }
            } else if (cursor == -1) {
                if (dispatcher != null && !isActive()) {
                    if (getCancelReason() != SqlExecutionCircuitBreaker.STATE_OK) {
                        throw buildInterruptionException();
                    }
                    if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
                        return -2;
                    }
                } else if (dispatch(dispatchLimit, countOnly)) {
                    // We have dispatched something, so let's try to collect it.
                    continue;
                }
                if (dispatcher != null) {
                    if (dispatcher.isCurrentFiberOwned()
                            && dispatchStartFrameIndex == collectedFrameIndex + 1) {
                        reduceLocally(countOnly);
                        return LOCAL_TASK_CURSOR;
                    }
                    if (canPark) {
                        final boolean isDraining = !isActive();
                        awaitProgress(dispatcher, observedProgress, observedGlobalProgress, isDraining);
                        continue;
                    }
                    if (!isActive()) {
                        return -1;
                    }
                }
                if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
                    reduceLocally(countOnly);
                    return LOCAL_TASK_CURSOR;
                }
                return -1;
            } else {
                Os.pause();
            }
        }
    }

    public PageFrameSequence<T> of(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            SCSequence collectSubSeq,
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
            frameAddressCache.of(base.getMetadata(), frameCursor.getColumnMapping(), frameCursor.isExternal());

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
        assert frameCount == 0 || reduceFinishedCounter.get() == dispatchStartFrameIndex;

        // prepare different frame sequence using the same object instance
        frameCount = 0;
        dispatchStartFrameIndex = 0;
        collectedFrameIndex = -1;
        readyToDispatch = false;
        // Drop the borrowed tracker reference; the provider owns the native block.
        memoryTracker = null;
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
                final PageFrameReduceDispatcher dispatcher = messageBus.getPageFrameReduceDispatcher();
                if (dispatcher != null) {
                    dispatcher.signalProgress();
                }
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

            // done is reset by method call above
            done = false;
            id = newId;
            dispatchStartFrameIndex = 0;
            collectedFrameIndex = -1;
            reduceFinishedCounter.set(0);
            reduceStartedCounter.set(0);
            workStealingStrategy.of(reduceStartedCounter);
            resetCancellation();
        }
    }

    private void buildAddressCache() {
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
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
     * than number of frames to be dispatched. When it is the case, frame count published so far is
     * stored in the `dispatchStartFrameIndex` field. This method has no responsibility to deal with
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
        final int collectedFrameCount = collectedFrameIndex + 1;

        try {
            if (!hasPublication) {
                if (!dispatcher.isCurrentFiberOwned()) {
                    cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
                }
                return false;
            }
            long cursor;
            int i = dispatchStartFrameIndex;
            OUT:
            for (; i < frameCount; i++) {
                // We cannot process work on this thread. If we do the consumer will
                // never get the executions results. Consumer only picks ready to go
                // tasks from the queue.

                while (true) {
                    final int totalDispatched = dispatchStartFrameIndex - collectedFrameCount;
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
                                .$(", frameIndex=").$(i)
                                .$(", frameCount=").$(frameCount)
                                .$(", cursor=").$(cursor)
                                .I$();
                        dispatchStartFrameIndex = i + 1;
                        dispatched = true;
                        break;
                    } else if (cursor == -1) {
                        if (!workStealingStrategy.shouldSteal(collectedFrameCount)) {
                            return dispatched;
                        }
                        // start stealing work to unload the queue
                        idle = false;
                        if (dispatcher != null) {
                            dispatcher.releasePublication();
                            hasPublication = false;
                        }
                        if (stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker)) {
                            if (reduceFinishedCounter.get() > collectedFrameCount) {
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

            if (reduceFinishedCounter.get() > collectedFrameCount) {
                // We have something to collect, so let's do it!
                return true;
            }

            // Reduce counter is here to provide safe backoff point
            // for job stealing code. It is needed because queue is shared
            // and there is possibility of never ending stealing if we don't
            // specifically count only our items

            // join the gang to consume published tasks
            while (reduceFinishedCounter.get() < dispatchStartFrameIndex) {
                idle = false;
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

    private void reduceLocally(boolean countOnly) {
        assert dispatchStartFrameIndex < frameCount;

        if (localTask == null) {
            localTask = localTaskFactory.getInstance();
            localTask.setTaskType(taskType);
        }
        localTask.of(this, dispatchStartFrameIndex++, countOnly);

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
                    .$(", frameIndex=").$(localTask.getFrameIndex())
                    .$(", frameCount=").$(frameCount)
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
                        .$(", frameIndex=").$(localTask.getFrameIndex())
                        .$(", frameCount=").$(frameCount)
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
}
