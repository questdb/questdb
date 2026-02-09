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
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PageFrameSequence<T extends StatefulAtom> implements Closeable {

    private static final AtomicLong ID_SEQ = new AtomicLong();
    private static final long LOCAL_TASK_CURSOR = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(PageFrameSequence.class);
    private final T atom;
    private final AtomicInteger cancelReason = new AtomicInteger(SqlExecutionCircuitBreaker.STATE_OK);
    private final MillisecondClock clock;
    private final PageFrameAddressCache frameAddressCache;
    private final LongList frameRowCounts = new LongList();
    private final PageFrameReduceTaskFactory localTaskFactory;
    private final MessageBus messageBus;
    private final AtomicInteger reduceFinishedCounter = new AtomicInteger(0);
    private final AtomicInteger reduceStartedCounter = new AtomicInteger(0);
    private final PageFrameReducer reducer;
    private final byte taskType; // PageFrameReduceTask.TYPE_*
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final WorkStealingStrategy workStealingStrategy;
    public volatile boolean done;
    private SCSequence collectSubSeq;
    private int collectedFrameIndex = -1;
    private int dispatchStartFrameIndex;
    private int frameCount;
    private PageFrameCursor frameCursor;
    private long id;
    private PageFrameMemoryRecord localRecord;
    // Local reduce task used when there is no slots in the queue to dispatch tasks.
    private PageFrameReduceTask localTask;
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
            this.workStealingStrategy = WorkStealingStrategyFactory.getInstance(configuration, sharedQueryWorkerCount);
            this.taskType = taskType;
            this.workStealCircuitBreaker = new SqlExecutionCircuitBreakerWrapper(engine, configuration.getCircuitBreakerConfiguration());
        } catch (Throwable th) {
            close();
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
        while (!done) {
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
                        this
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
                    if (task.getFrameSequence() == this) {
                        assert id == task.getFrameSequenceId() : "ids mismatch: " + id + ", " + task.getFrameSequenceId();
                        collectedFrameIndex = task.getFrameIndex();
                        task.collected(true);
                    }
                    collectSubSeq.done(cursor);
                } else {
                    Os.pause();
                }
            }
        }

        // It could be the case that one of the workers reduced a page frame, then marked the task as done,
        // but haven't incremented reduce counter yet. In this case, we wait for the desired counter value.
        while (reduceFinishedCounter.get() != dispatchStartFrameIndex) {
            Os.pause();
        }
    }

    public void cancel(int reason) {
        valid.compareAndSet(true, false);
        cancelReason.set(reason);
    }

    @Override
    public void close() {
        reset();
        localRecord = Misc.free(localRecord);
        workStealCircuitBreaker = Misc.free(workStealCircuitBreaker);
        localTask = Misc.free(localTask);
        Misc.free(atom);
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
    }

    public T getAtom() {
        return atom;
    }

    public int getCancelReason() {
        return cancelReason.get();
    }

    // warning: the circuit breaker may be thread unsafe, so don't use it concurrently
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

    public WorkStealingStrategy getWorkStealingStrategy() {
        return workStealingStrategy;
    }

    public boolean isActive() {
        return valid.get();
    }

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
        while (true) {
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
                }
            } else if (cursor == -1) {
                if (dispatch(dispatchLimit, countOnly)) {
                    // We have dispatched something, so let's try to collect it.
                    continue;
                }
                if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
                    // We haven't dispatched anything, and we have collected everything
                    // that was dispatched previously in this loop iteration. Use the
                    // local task to avoid being blocked in case of full reduce queue.
                    workLocally(countOnly);
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
            frameAddressCache.of(base.getMetadata(), frameCursor.getColumnIndexes(), frameCursor.isExternal());

            this.collectSubSeq = collectSubSeq;
            id = ID_SEQ.incrementAndGet();
            done = false;
            valid.set(true);
            cancelReason.set(SqlExecutionCircuitBreaker.STATE_OK);
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
        assert frameCount == 0 || reduceFinishedCounter.get() == dispatchStartFrameIndex;
        // prepare different frame sequence using the same object instance
        frameCount = 0;
        dispatchStartFrameIndex = 0;
        collectedFrameIndex = -1;
        readyToDispatch = false;
        frameRowCounts.clear();
        atom.clear();
        Misc.free(frameAddressCache);
        frameCursor = Misc.free(frameCursor);
        // collect sequence may not be set here when
        // factory is closed without using cursor
        if (collectSubSeq != null) {
            messageBus.getPageFrameCollectFanOut(shard).remove(collectSubSeq);
            LOG.debug().$("removed [seq=").$(collectSubSeq).I$();
        }
        if (localTask != null) {
            localTask.clear();
        }
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
            valid.set(true);
            cancelReason.set(SqlExecutionCircuitBreaker.STATE_OK);
        }
    }

    private void buildAddressCache() {
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            frameAddressCache.add(frameCount++, frame);
        }

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
        boolean idle = true;
        boolean dispatched = false;

        // the sequence used to steal worker jobs
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);

        final int collectedFrameCount = collectedFrameIndex + 1;

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
                cursor = totalDispatched < dispatchLimit ? reducePubSeq.next() : -1;
                if (cursor > -1) {
                    reduceQueue.get(cursor).of(this, i, countOnly);
                    LOG.debug()
                            .$("dispatched [shard=").$(shard)
                            .$(", id=").$(getId())
                            .$(", frameIndex=").$(i)
                            .$(", frameCount=").$(frameCount)
                            .$(", cursor=").$(cursor)
                            .I$();
                    reducePubSeq.done(cursor);
                    dispatchStartFrameIndex = i + 1;
                    dispatched = true;
                    break;
                } else if (cursor == -1) {
                    if (!workStealingStrategy.shouldSteal(collectedFrameCount)) {
                        return dispatched;
                    }
                    // start stealing work to unload the queue
                    idle = false;
                    if (stealWork(reduceQueue, reduceSubSeq, localRecord, workStealCircuitBreaker)) {
                        if (reduceFinishedCounter.get() > collectedFrameCount) {
                            // We have something to collect, so let's do it!
                            return true;
                        }
                        continue;
                    }
                    break OUT;
                } else {
                    Os.pause();
                }
            }
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
    }

    private boolean stealWork(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence reduceSubSeq,
            PageFrameMemoryRecord record,
            SqlExecutionCircuitBreakerWrapper circuitBreaker
    ) {
        if (PageFrameReduceJob.consumeQueue(queue, reduceSubSeq, record, circuitBreaker, this)) {
            Os.pause();
            return false;
        }
        return true;
    }

    private void workLocally(boolean countOnly) {
        assert dispatchStartFrameIndex < frameCount;

        if (localTask == null) {
            localTask = localTaskFactory.getInstance();
            localTask.setTaskType(taskType);
        }
        localTask.of(this, dispatchStartFrameIndex++, countOnly);

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
            LOG.error()
                    .$("local reduce error [error=").$(th)
                    .$(", id=").$(id)
                    .$(", taskType=").$(taskType)
                    .$(", frameIndex=").$(localTask.getFrameIndex())
                    .$(", frameCount=").$(frameCount)
                    .I$();
            int interruptReason = SqlExecutionCircuitBreaker.STATE_OK;
            if (th instanceof CairoException e) {
                interruptReason = e.getInterruptionReason();
            }
            cancel(interruptReason);
            throw th;
        } finally {
            reduceFinishedCounter.incrementAndGet();
        }
    }
}
