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
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PageFrameSequence<T extends StatefulAtom> implements Closeable {
    private static final int OWNER_NONE = 0;
    private static final int OWNER_WORK_STEALING = 1;
    private static final int OWNER_ASYNC = 2;
    private static final Log LOG = LogFactory.getLog(PageFrameSequence.class);
    private static final AtomicLong ID_SEQ = new AtomicLong();
    public final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final AtomicInteger reduceCounter = new AtomicInteger(0);
    private final LongList frameRowCounts = new LongList();
    private final PageFrameReducer reducer;
    private final PageAddressCache pageAddressCache;
    private final MessageBus messageBus;
    private final AtomicInteger owner = new AtomicInteger(OWNER_NONE);
    private final MicrosecondClock microsecondClock;
    private long id;
    private int shard;
    private int frameCount;
    private Sequence collectSubSeq;
    private SymbolTableSource symbolTableSource;
    private T atom;
    private PageAddressCacheRecord[] records;
    // we need this to restart execution for `toTop`
    private MPSequence dispatchPubSeq;
    private RingQueue<PageFrameDispatchTask> pageFrameDispatchQueue;
    private final AtomicInteger dispatchStartIndex = new AtomicInteger();
    private SqlExecutionCircuitBreaker[] circuitBreakers;
    private long startTimeUs;
    private long circuitBreakerFd;

    public PageFrameSequence(
            CairoConfiguration configuration,
            MessageBus messageBus,
            PageFrameReducer reducer
    ) {
        this.pageAddressCache = new PageAddressCache(configuration);
        this.messageBus = messageBus;
        this.reducer = reducer;
        this.microsecondClock = configuration.getMicrosecondClock();
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

        // Collect only dispatched frames. To do that we need to stall any further dispatch.
        while (true) {
            final int dispatched = dispatchStartIndex.get();
            if (dispatchStartIndex.compareAndSet(dispatched, frameCount)) {
                frameCount = dispatched;
                break;
            }
        }

        final RingQueue<PageFrameReduceTask> queue = getPageFrameReduceQueue();
        final MCSequence pageFrameReduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        while (doneLatch.getCount() == 0) {
            final int workerId = getWorkerId();
            final PageAddressCacheRecord rec = records[workerId];
            final SqlExecutionCircuitBreaker circuitBreaker = circuitBreakers[workerId];
            // we were asked to steal work from dispatch0 queue and beyond, as much as we can
            if (PageFrameReduceJob.consumeQueue(queue, pageFrameReduceSubSeq, rec, circuitBreaker)) {
                long cursor = collectSubSeq.next();
                if (cursor > -1) {
                    // discard collect items
                    final PageFrameReduceTask tsk = queue.get(cursor);
                    if (tsk.getFrameSequence() == this) {
                        tsk.collected();
                    }
                    collectSubSeq.done(cursor);
                } else {
                    Os.pause();
                }
            }
        }
    }

    public void clear() {
        // prepare different frame sequence using the same object instance
        this.frameCount = 0;
        pageAddressCache.clear();
        symbolTableSource = Misc.free(symbolTableSource);
        // collect sequence may not be set here when
        // factory is closed without using cursor
        if (collectSubSeq != null) {
            messageBus.getPageFrameCollectFanOut(shard).remove(collectSubSeq);
            collectSubSeq.clear();
        }
        this.dispatchStartIndex.set(0);
    }

    @Override
    public void close() {
        Misc.free(circuitBreakers);
    }

    public PageFrameSequence<T> dispatch(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            Sequence collectSubSeq,
            T atom
    ) throws SqlException {

        this.startTimeUs = microsecondClock.getTicks();
        this.circuitBreakerFd = executionContext.getCircuitBreaker().getFd();

        // allow entry for 0 - main thread that is a non-worker
        initWorkerRecords(
                executionContext.getWorkerCount() + 1,
                executionContext.getCircuitBreaker()
        );

        final Rnd rnd = executionContext.getAsyncRandom();
        try {
            final PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext);
            final int frameCount = setupAddressCache(base, pageFrameCursor);

            // this method sets a lot of state of the page sequence
            prepareForDispatch(rnd, frameCount, pageFrameCursor, atom, collectSubSeq);

            // It is essential to init the atom after we prepared sequence for dispatch0.
            // If atom is to fail, we will be releasing whatever we prepared.
            atom.init(pageFrameCursor, executionContext);

            // dispatch0 message only if there is anything to dispatch0
            if (frameCount > 0) {
                dispatch0();
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

    public int getDispatchStartIndex() {
        return dispatchStartIndex.get();
    }

    public void setDispatchStartIndex(int i) {
        this.dispatchStartIndex.set(i);
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

    public RingQueue<PageFrameReduceTask> getPageFrameReduceQueue() {
        return messageBus.getPageFrameReduceQueue(shard);
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

    public long getStartTimeUs() {
        return startTimeUs;
    }

    public SymbolTableSource getSymbolTableSource() {
        return symbolTableSource;
    }

    /**
     * Async owner is allowed to enter only once. The expectation is that all frames will be dispatched
     * in the first attempt.
     *
     * @return true if this sequence dispatch0 belongs to async owner, work stealing must not touch this.
     */
    public boolean isAsyncOwner() {
        return owner.compareAndSet(OWNER_NONE, OWNER_ASYNC);
    }

    public boolean isValid() {
        return valid.get();
    }

    public void setValid(boolean valid) {
        this.valid.compareAndSet(true, valid);
    }

    /**
     * Work stealing is re-enterable, hence subsequence invocations can yield true.
     *
     * @return true is this sequence dispatch0 is owned by work stealing algo.
     */
    public boolean isWorkStealingOwner() {
        return owner.get() == OWNER_WORK_STEALING || owner.compareAndSet(OWNER_NONE, OWNER_WORK_STEALING);
    }

    public void reset() {
        // prepare to resent the same sequence
        // as it might be required by toTop()
        frameRowCounts.clear();
        assert doneLatch.getCount() == 0;
        doneLatch.countDown();
        this.owner.set(OWNER_NONE);
    }

    public void stealDispatchWork() {
        final int workerId = getWorkerId();
        final PageAddressCacheRecord rec = records[workerId];
        final SqlExecutionCircuitBreaker circuitBreaker = circuitBreakers[workerId];
        // we were asked to steal work from dispatch0 queue and beyond, as much as we can
        PageFrameDispatchJob.handleTask(this, rec, messageBus, true, circuitBreaker);

        // now we need to steal all dispatch0 work from the queue until we reach either:
        // - end of the queue
        // - find out publisher id
        final MCSequence dispatchSubSeq = messageBus.getPageFrameDispatchSubSeq();
        while (true) {
            final long cursor = dispatchSubSeq.next();
            if (cursor > -1) {
                PageFrameSequence<?> pageFrameSequence = pageFrameDispatchQueue.get(cursor).getFrameSequence();

                if (pageFrameSequence == this) {
                    dispatchSubSeq.done(cursor);
                    break;
                }

                // We are entering all frame sequences in work stealing mode, which means
                // they can end up being partially dispatched. This is done because we
                // cannot afford to enter infinite loop here
                PageFrameDispatchJob.handleTask(pageFrameSequence, rec, messageBus, true, circuitBreaker);
                dispatchSubSeq.done(cursor);
            } else {
                break;
            }
        }
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
            dispatchStartIndex.set(0);
            reduceCounter.set(0);
            long dispatchCursor;
            do {
                dispatchCursor = dispatchPubSeq.next();
                if (dispatchCursor < 0) {
                    stealDispatchWork();
                } else {
                    break;
                }
            } while (true);
            final PageFrameDispatchTask dispatchTask = pageFrameDispatchQueue.get(dispatchCursor);
            dispatchTask.of(this);
            dispatchPubSeq.done(dispatchCursor);
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

    private void dispatch0() {
        // We need to subscribe publisher sequence before we return
        // control to the caller of this method. However, this sequence
        // will be unsubscribed asynchronously.
        messageBus.getPageFrameCollectFanOut(shard).and(collectSubSeq);
        LOG.debug()
                .$("added [shard=").$(shard)
                .$(", id=").$(id)
                .$(", seqCurrent=").$(collectSubSeq.current())
                .I$();

        long dispatchCursor;
        do {
            dispatchCursor = dispatchPubSeq.next();

            if (dispatchCursor < 0) {
                stealDispatchWork();
                Os.pause();
            } else {
                break;
            }
        } while (true);

        PageFrameDispatchTask dispatchTask = pageFrameDispatchQueue.get(dispatchCursor);
        dispatchTask.of(this);
        dispatchPubSeq.done(dispatchCursor);
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
        this.frameCount = frameCount;
        assert this.symbolTableSource == null;
        this.symbolTableSource = symbolTableSource;
        this.atom = atom;
        this.collectSubSeq = collectSubSeq;
        this.dispatchPubSeq = messageBus.getPageFrameDispatchPubSeq();
        this.pageFrameDispatchQueue = messageBus.getPageFrameDispatchQueue();
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
