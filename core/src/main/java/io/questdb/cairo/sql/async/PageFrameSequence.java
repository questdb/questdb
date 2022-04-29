/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PageFrameSequence<T extends StatefulAtom> implements Closeable {

    private static final Log LOG = LogFactory.getLog(PageFrameSequence.class);
    private static final long LOCAL_TASK_CURSOR = Long.MAX_VALUE;
    private static final AtomicLong ID_SEQ = new AtomicLong();

    public final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final AtomicInteger reduceCounter = new AtomicInteger(0);
    private final LongList frameRowCounts = new LongList();
    private final PageFrameReducer reducer;
    private final PageAddressCache pageAddressCache;
    private final MessageBus messageBus;
    private final MicrosecondClock microsecondClock;
    private long id;
    private int shard;
    private int dispatchStartFrameIndex;
    private int collectedFrameIndex = -1;
    private int frameCount;
    private Sequence collectSubSeq;
    private RingQueue<PageFrameReduceTask> reduceQueue;
    private SymbolTableSource symbolTableSource;
    private T atom;
    private PageAddressCacheRecord[] records;
    private SqlExecutionCircuitBreaker[] circuitBreakers;
    // Local reduce task used when there is no slots in the queue to dispatch tasks.
    private PageFrameReduceTask localTask;
    private final WeakAutoClosableObjectPool<PageFrameReduceTask> localTaskPool;
    private long startTimeUs;
    private long circuitBreakerFd;
    private SqlExecutionContext sqlExecutionContext;

    public PageFrameSequence(
            CairoConfiguration configuration,
            MessageBus messageBus,
            PageFrameReducer reducer,
            WeakAutoClosableObjectPool<PageFrameReduceTask> localTaskPool
    ) {
        this.pageAddressCache = new PageAddressCache(configuration);
        this.messageBus = messageBus;
        this.reducer = reducer;
        this.microsecondClock = configuration.getMicrosecondClock();
        this.localTaskPool = localTaskPool;
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
        while (doneLatch.getCount() == 0) {
            final int workerId = getWorkerId();
            final PageAddressCacheRecord rec = records[workerId];
            final SqlExecutionCircuitBreaker circuitBreaker = circuitBreakers[workerId];
            final boolean allFramesReduced = reduceCounter.get() == dispatchStartFrameIndex;
            // we were asked to steal work from the reduce queue and beyond, as much as we can
            if (PageFrameReduceJob.consumeQueue(reduceQueue, pageFrameReduceSubSeq, rec, circuitBreaker)) {
                long cursor = collectSubSeq.next();
                if (cursor > -1) {
                    // discard collect items
                    final PageFrameReduceTask tsk = reduceQueue.get(cursor);
                    if (tsk.getFrameSequence() == this) {
                        tsk.collected(true);
                    }
                    collectSubSeq.done(cursor);
                } else if (cursor == -1 && allFramesReduced) {
                    // The collect queue is empty while we know that all frames were reduced. We're almost done.
                    if (doneLatch.getCount() == 0) {
                        // Looks like not all the frames were dispatched, so no one reached the very last frame and
                        // reset the sequence via calling PageFrameReduceTask#collected(). Let's do it ourselves.
                        reset();
                    }
                    break;
                } else {
                    Os.pause();
                }
            }
        }

        assert reduceCounter.get() == dispatchStartFrameIndex;
    }

    public void clear() {
        // prepare different frame sequence using the same object instance
        frameCount = 0;
        dispatchStartFrameIndex = 0;
        collectedFrameIndex = -1;
        pageAddressCache.clear();
        symbolTableSource = Misc.free(symbolTableSource);
        // collect sequence may not be set here when
        // factory is closed without using cursor
        if (collectSubSeq != null) {
            messageBus.getPageFrameCollectFanOut(shard).remove(collectSubSeq);
            LOG.debug().$("removed [seq=").$(collectSubSeq).I$();
            collectSubSeq.clear();
        }
        if (localTask != null) {
            localTask.resetCapacities();
            localTaskPool.push(localTask);
            localTask = null;
        }
    }

    @Override
    public void close() {
        Misc.free(circuitBreakers);
    }

    public PageFrameSequence<T> of(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            Sequence collectSubSeq,
            T atom,
            int order
    ) throws SqlException {

        this.sqlExecutionContext = executionContext;
        this.startTimeUs = microsecondClock.getTicks();
        this.circuitBreakerFd = executionContext.getCircuitBreaker().getFd();

        // allow entry for 0 - main thread that is a non-worker
        initWorkerRecords(
                executionContext.getWorkerCount() + 1,
                executionContext.getCircuitBreaker()
        );

        final Rnd rnd = executionContext.getAsyncRandom();
        try {
            final PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext, order);
            final int frameCount = setupAddressCache(base, pageFrameCursor);

            // this method sets a lot of state of the page sequence
            prepareForDispatch(rnd, frameCount, pageFrameCursor, atom, collectSubSeq);

            // It is essential to init the atom after we prepared sequence for dispatch.
            // If atom is to fail, we will be releasing whatever we prepared.
            atom.init(pageFrameCursor, executionContext);

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
        } catch (Throwable e) {
            this.symbolTableSource = Misc.free(this.symbolTableSource);
            throw e;
        }
        return this;
    }

    public T getAtom() {
        return atom;
    }

    public long getCircuitBreakerFd() {
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

    public long getStartTimeUs() {
        return startTimeUs;
    }

    public SymbolTableSource getSymbolTableSource() {
        return symbolTableSource;
    }

    public boolean isActive() {
        return valid.get();
    }

    public void cancel() {
        this.valid.compareAndSet(true, false);
    }

    public void reset() {
        // prepare to resend the same sequence as it might be required by toTop()
        frameRowCounts.clear();
        assert doneLatch.getCount() == 0;
        doneLatch.countDown();
    }

    /**
     * This method is not thread safe. It's always invoked on a single "query owner" thread.
     *
     * Returns a cursor that points either to the reduce queue or to the local reduce task.
     * The caller of this method should avoid accessing the reduce queue directly and,
     * instead, should use getTask and collect methods. <code>Long.MAX_VALUE</code> is the
     * reserved cursor value for the local reduce task case.
     *
     * @return the next cursor value or one of -1 and -2 values if the cursor failed and the
     * caller should retry
     */
    public long next() {
        assert collectedFrameIndex < frameCount - 1;
        while (true) {
            long cursor = collectSubSeq.next();
            if (cursor > -1) {
                PageFrameReduceTask task = reduceQueue.get(cursor);
                PageFrameSequence<?> thatFrameSequence = task.getFrameSequence();
                if (thatFrameSequence == this) {
                    return cursor;
                } else {
                    // Not our task, nothing to collect. Go for another spin.
                    collectSubSeq.done(cursor);
                }
            } else {
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
            }
        }
    }

    /**
     * This method is re enterable. It has to be in case queue capacity is smaller than number of frames to
     * be dispatched. When it is the case, frame count published so far is stored in the `frameSequence`.
     * This method has no responsibility to deal with "collect" stage hence it deals with everything to
     * unblock the collect stage.
     *
     * @return true if at least one task was dispatched or reduced; false otherwise
     */
    private boolean dispatch() {
        final int workerId = getWorkerId();
        final PageAddressCacheRecord record = records[workerId];
        final SqlExecutionCircuitBreaker circuitBreaker = circuitBreakers[workerId];

        boolean idle = true;
        boolean dispatched = false;

        // the sequence used to steal worker jobs
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);

        long cursor;
        int i = dispatchStartFrameIndex;
        dispatchStartFrameIndex = frameCount;
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
                    dispatched = true;
                    break;
                } else {
                    idle = false;
                    // start stealing work to unload the queue
                    if (stealWork(reduceQueue, reduceSubSeq, record, circuitBreaker)) {
                        continue;
                    }
                    dispatchStartFrameIndex = i;
                    break OUT;
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

    private boolean stealWork(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence reduceSubSeq,
            PageAddressCacheRecord record,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        if (PageFrameReduceJob.consumeQueue(queue, reduceSubSeq, record, circuitBreaker)) {
            Os.pause();
            return false;
        }
        return true;
    }

    private void workLocally() {
        assert dispatchStartFrameIndex < frameCount;

        if (localTask == null) {
            localTask = localTaskPool.pop();
        }
        localTask.of(this, dispatchStartFrameIndex++);

        final int workerId = getWorkerId();
        final PageAddressCacheRecord record = records[workerId];
        final SqlExecutionCircuitBreaker circuitBreaker = circuitBreakers[workerId];
        PageFrameReduceJob.reduce(record, circuitBreaker, localTask, this);
    }

    public PageFrameReduceTask getTask(long cursor) {
        assert cursor > -1;
        if (cursor == LOCAL_TASK_CURSOR) {
            assert localTask != null && localTask.getFrameSequence() != null;
            return localTask;
        }
        return reduceQueue.get(cursor);
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

    /**
     * Prepares page frame sequence for retrieving the same data set again. The method
     * is not thread-safe.
     */
    public void toTop() {
        if (frameCount > 0) {
            LOG.debug().$("toTop [shard=").$(shard)
                    .$(", id=").$(id)
                    .I$();

            await();

            // done latch is reset by method call above
            doneLatch.reset();
            dispatchStartFrameIndex = 0;
            collectedFrameIndex = -1;
            reduceCounter.set(0);
            valid.set(true);
        }
    }

    private static int getWorkerId() {
        final Thread thread = Thread.currentThread();
        final int workerId;
        if (thread instanceof Worker) {
            workerId = ((Worker) thread).getWorkerId() + 1;
        } else {
            workerId = 0;
        }
        return workerId;
    }

    private void initWorkerRecords(
            int workerCount,
            SqlExecutionCircuitBreaker executionContextCircuitBreaker
    ) {
        if (records == null || records.length < workerCount) {
            final SqlExecutionCircuitBreakerConfiguration sqlExecutionCircuitBreakerConfiguration = executionContextCircuitBreaker.getConfiguration();
            this.records = new PageAddressCacheRecord[workerCount];
            this.circuitBreakers = new SqlExecutionCircuitBreaker[workerCount];
            for (int i = 0; i < workerCount; i++) {
                this.records[i] = new PageAddressCacheRecord();
                if (sqlExecutionCircuitBreakerConfiguration != null) {
                    this.circuitBreakers[i] = new NetworkSqlExecutionCircuitBreaker(sqlExecutionCircuitBreakerConfiguration);
                } else {
                    this.circuitBreakers[i] = NetworkSqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
                }
            }
        }

        for (int i = 0; i < workerCount; i++) {
            this.circuitBreakers[i].setFd(executionContextCircuitBreaker.getFd());
        }
    }

    private void prepareForDispatch(
            Rnd rnd,
            int frameCount,
            SymbolTableSource symbolTableSource,
            T atom,
            Sequence collectSubSeq
    ) {
        this.id = ID_SEQ.incrementAndGet();
        this.doneLatch.reset();
        this.valid.set(true);
        this.reduceCounter.set(0);
        this.shard = rnd.nextInt(messageBus.getPageFrameReduceShardCount());
        this.reduceQueue = messageBus.getPageFrameReduceQueue(shard);
        this.frameCount = frameCount;
        assert this.symbolTableSource == null;
        this.symbolTableSource = symbolTableSource;
        this.atom = atom;
        this.collectSubSeq = collectSubSeq;
    }

    private int setupAddressCache(RecordCursorFactory base, PageFrameCursor pageFrameCursor) {
        // pass one to cache page addresses
        // this has to be separate pass to ensure there no cache reads
        // while cache might be resizing
        this.pageAddressCache.of(base.getMetadata());

        PageFrame frame;
        int frameIndex = 0;
        while ((frame = pageFrameCursor.next()) != null) {
            this.pageAddressCache.add(frameIndex++, frame);
            frameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
        }
        return frameIndex;
    }
}
