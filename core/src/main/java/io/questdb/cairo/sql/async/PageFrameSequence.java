/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.*;
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
    private final MillisecondClock clock;
    private final LongList frameRowCounts = new LongList();
    private final PageFrameReduceTaskFactory localTaskFactory;
    private final MessageBus messageBus;
    private final PageAddressCache pageAddressCache;
    private final AtomicInteger reduceCounter = new AtomicInteger(0);
    private final PageFrameReducer reducer;
    private final byte taskType; // PageFrameReduceTask.TYPE_*
    private final AtomicBoolean valid = new AtomicBoolean(true);
    public volatile boolean done;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private int circuitBreakerFd;
    private SCSequence collectSubSeq;
    private int collectedFrameIndex = -1;
    private int dispatchStartFrameIndex;
    private int frameCount;
    private long id;
    // Local reduce task used when there is no slots in the queue to dispatch tasks.
    private PageFrameReduceTask localTask;
    private PageFrameCursor pageFrameCursor;
    private boolean readyToDispatch;
    private PageAddressCacheRecord record;
    private RingQueue<PageFrameReduceTask> reduceQueue;
    private int shard;
    private SqlExecutionContext sqlExecutionContext;
    private long startTime;
    private boolean uninterruptible;

    public PageFrameSequence(
            CairoConfiguration configuration,
            MessageBus messageBus,
            T atom,
            PageFrameReducer reducer,
            PageFrameReduceTaskFactory localTaskFactory,
            byte taskType
    ) {
        this.pageAddressCache = new PageAddressCache(configuration);
        this.messageBus = messageBus;
        this.atom = atom;
        this.reducer = reducer;
        this.clock = configuration.getMillisecondClock();
        this.localTaskFactory = localTaskFactory;
        this.taskType = taskType;
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
            if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
                // We know that all frames were collected. We're almost done.
                if (!done) {
                    // Looks like not all the frames were dispatched, so no one reached the very last frame and
                    // reset the sequence via calling PageFrameReduceTask#collected(). Let's do it ourselves.
                    reset();
                }
                break;
            }

            // We were asked to steal work from the reduce queue and beyond, as much as we can.
            boolean nothingProcessed = true;
            try {
                nothingProcessed = PageFrameReduceJob.consumeQueue(reduceQueue, pageFrameReduceSubSeq, record, circuitBreaker, this);
            } catch (Throwable e) {
                LOG.error()
                        .$("await error [id=").$(id)
                        .$(", ex=").$(e)
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
        while (reduceCounter.get() != dispatchStartFrameIndex) {
            Os.pause();
        }
    }

    public void cancel() {
        valid.compareAndSet(true, false);
    }

    public void clear() {
        // prepare different frame sequence using the same object instance
        frameCount = 0;
        dispatchStartFrameIndex = 0;
        collectedFrameIndex = -1;
        readyToDispatch = false;
        pageAddressCache.clear();
        atom.clear();
        pageFrameCursor = Misc.freeIfCloseable(pageFrameCursor);
        // collect sequence may not be set here when
        // factory is closed without using cursor
        if (collectSubSeq != null) {
            messageBus.getPageFrameCollectFanOut(shard).remove(collectSubSeq);
            LOG.debug().$("removed [seq=").$(collectSubSeq).I$();
            collectSubSeq.clear();
        }
        if (localTask != null) {
            localTask.resetCapacities();
        }
    }

    @Override
    public void close() {
        clear();
        record = Misc.free(record);
        circuitBreaker = Misc.freeIfCloseable(circuitBreaker);
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

    public int getCircuitBreakerFd() {
        return circuitBreakerFd;
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

    public PageAddressCache getPageAddressCache() {
        return pageAddressCache;
    }

    public AtomicInteger getReduceCounter() {
        return reduceCounter;
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
        return pageFrameCursor;
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

    public boolean isActive() {
        return valid.get();
    }

    public boolean isUninterruptible() {
        return uninterruptible;
    }

    /**
     * This method is not thread safe. It's always invoked on a single "query owner" thread.
     * <p>
     * Returns a cursor that points either to the reduce queue or to the local reduce task.
     * The caller of this method should avoid accessing the reduce queue directly and,
     * instead, should use getTask and collect methods. <code>Long.MAX_VALUE</code> is the
     * reserved cursor value for the local reduce task case.
     *
     * @return the next cursor value, or -1 value if the cursor failed and the caller
     * should retry, or -2 if there are no frames to dispatch
     */
    public long next() {
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
                if (dispatch()) {
                    // We have dispatched something, so let's try to collect it.
                    continue;
                }
                if (dispatchStartFrameIndex == collectedFrameIndex + 1) {
                    // We haven't dispatched anything, and we have collected everything
                    // that was dispatched previously in this loop iteration. Use the
                    // local task to avoid being blocked in case of full reduce queue.
                    workLocally();
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
        circuitBreakerFd = executionContext.getCircuitBreaker().getFd();
        uninterruptible = executionContext.isUninterruptible();

        initRecord(executionContext.getCircuitBreaker());

        final Rnd rnd = executionContext.getAsyncRandom();
        try {
            // pass one to cache page addresses
            // this has to be separate pass to ensure there no cache reads
            // while cache might be resizing
            pageAddressCache.of(base.getMetadata());

            assert pageFrameCursor == null;
            pageFrameCursor = base.getPageFrameCursor(executionContext, order);
            this.collectSubSeq = collectSubSeq;
            id = ID_SEQ.incrementAndGet();
            done = false;
            valid.set(true);
            reduceCounter.set(0);
            shard = rnd.nextInt(messageBus.getPageFrameReduceShardCount());
            reduceQueue = messageBus.getPageFrameReduceQueue(shard);

            // It is essential to init the atom after we prepared sequence for dispatch.
            // If atom is to fail, we will be releasing whatever we prepared.
            atom.init(pageFrameCursor, executionContext);
        } catch (Throwable e) {
            pageFrameCursor = Misc.freeIfCloseable(pageFrameCursor);
            throw e;
        }
        return this;
    }

    /**
     * Must be called before subsequence calls to {@link #next()} to count page frames and
     * initialize page frame cache and filter functions.
     *
     * @throws io.questdb.cairo.DataUnavailableException when the queried partition is in cold storage
     */
    public void prepareForDispatch() {
        if (!readyToDispatch) {
            atom.initCursor();
            buildAddressCache();
            readyToDispatch = true;
        }
    }

    public void reset() {
        // prepare to resend the same sequence as it might be required by toTop()
        frameRowCounts.clear();
        assert !done;
        done = true;
    }

    /**
     * Prepares page frame sequence for retrieving the same data set again. The method
     * is not thread-safe.
     */
    public void toTop() {
        if (frameCount > 0) {
            long newId = ID_SEQ.incrementAndGet();
            LOG.debug().$("toTop [shard=").$(shard)
                    .$(", id=").$(id)
                    .$(", newId=").$(newId)
                    .I$();

            await();

            // done is reset by method call above
            done = false;
            id = newId;
            dispatchStartFrameIndex = 0;
            collectedFrameIndex = -1;
            reduceCounter.set(0);
            valid.set(true);
        }
    }

    private void buildAddressCache() {
        PageFrame frame;
        while ((frame = pageFrameCursor.next()) != null) {
            pageAddressCache.add(frameCount++, frame);
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
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
     * This method is re-enterable. It has to be in case queue capacity is smaller than number of frames to
     * be dispatched. When it is the case, frame count published so far is stored in the `frameSequence`.
     * This method has no responsibility to deal with "collect" stage hence it deals with everything to
     * unblock the collect stage.
     *
     * @return true if at least one task was dispatched or reduced; false otherwise
     */
    private boolean dispatch() {
        boolean idle = true;
        boolean dispatched = false;

        // the sequence used to steal worker jobs
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);

        long cursor;
        int i = dispatchStartFrameIndex;
        OUT:
        for (; i < frameCount; i++) {
            // We cannot process work on this thread. If we do the consumer will
            // never get the executions results. Consumer only picks ready to go
            // tasks from the queue.

            while (true) {
                cursor = reducePubSeq.next();
                if (cursor > -1) {
                    reduceQueue.get(cursor).of(this, i);
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
                    idle = false;
                    // start stealing work to unload the queue
                    if (stealWork(reduceQueue, reduceSubSeq, record, circuitBreaker)) {
                        continue;
                    }
                    break OUT;
                } else {
                    Os.pause();
                }
            }
        }

        // Reduce counter is here to provide safe backoff point
        // for job stealing code. It is needed because queue is shared
        // and there is possibility of never ending stealing if we don't
        // specifically count only our items

        // join the gang to consume published tasks
        while (reduceCounter.get() < frameCount) {
            idle = false;
            if (stealWork(reduceQueue, reduceSubSeq, record, circuitBreaker)) {
                if (isActive()) {
                    continue;
                }
            }
            break;
        }

        if (idle) {
            stealWork(reduceQueue, reduceSubSeq, record, circuitBreaker);
        }

        return dispatched;
    }

    private void initRecord(SqlExecutionCircuitBreaker executionContextCircuitBreaker) {
        if (record == null) {
            final SqlExecutionCircuitBreakerConfiguration sqlExecutionCircuitBreakerConfiguration = executionContextCircuitBreaker.getConfiguration();
            record = new PageAddressCacheRecord();
            if (sqlExecutionCircuitBreakerConfiguration != null) {
                circuitBreaker = new NetworkSqlExecutionCircuitBreaker(sqlExecutionCircuitBreakerConfiguration, MemoryTag.NATIVE_CB2);
            } else {
                circuitBreaker = NetworkSqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
            }
        }

        circuitBreaker.setFd(executionContextCircuitBreaker.getFd());
    }

    private boolean stealWork(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence reduceSubSeq,
            PageAddressCacheRecord record,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        if (PageFrameReduceJob.consumeQueue(queue, reduceSubSeq, record, circuitBreaker, this)) {
            Os.pause();
            return false;
        }
        return true;
    }

    private void workLocally() {
        assert dispatchStartFrameIndex < frameCount;

        if (localTask == null) {
            localTask = localTaskFactory.getInstance();
            localTask.setType(taskType);
        }
        localTask.of(this, dispatchStartFrameIndex++);

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
                PageFrameReduceJob.reduce(record, circuitBreaker, localTask, this, this);
            }
        } catch (Throwable e) {
            cancel();
            throw e;
        } finally {
            reduceCounter.incrementAndGet();
        }
    }
}
