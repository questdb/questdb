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
    private static final Log LOG = LogFactory.getLog(PageFrameSequence.class);
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
    private int dispatchStartIndex;
    private int frameCount;
    private Sequence collectSubSeq;
    private SymbolTableSource symbolTableSource;
    private T atom;
    private PageAddressCacheRecord[] records;
    private SqlExecutionCircuitBreaker[] circuitBreakers;
    private long startTimeUs;
    private long circuitBreakerFd;
    private SqlExecutionContext sqlExecutionContext;

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

        final RingQueue<PageFrameReduceTask> queue = getPageFrameReduceQueue();
        final MCSequence pageFrameReduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        while (doneLatch.getCount() == 0) {
            final int workerId = getWorkerId();
            final PageAddressCacheRecord rec = records[workerId];
            final SqlExecutionCircuitBreaker circuitBreaker = circuitBreakers[workerId];
            // we were asked to steal work from the reduce queue and beyond, as much as we can
            if (PageFrameReduceJob.consumeQueue(queue, pageFrameReduceSubSeq, rec, circuitBreaker)) {
                long cursor = collectSubSeq.next();
                if (cursor > -1) {
                    // discard collect items
                    final PageFrameReduceTask tsk = queue.get(cursor);
                    if (tsk.getFrameSequence() == this) {
                        tsk.collected(true);
                    }
                    collectSubSeq.done(cursor);
                } else if (cursor == -1 && reduceCounter.get() == dispatchStartIndex) {
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
            LOG.debug().$("removed [seq=").$(collectSubSeq).I$();
            collectSubSeq.clear();
        }
        this.dispatchStartIndex = 0;
    }

    @Override
    public void close() {
        Misc.free(circuitBreakers);
    }

    public PageFrameSequence<T> dispatch(
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

    public SqlExecutionContext getSqlExecutionContext() {
        return sqlExecutionContext;
    }

    public long getStartTimeUs() {
        return startTimeUs;
    }

    public SymbolTableSource getSymbolTableSource() {
        return symbolTableSource;
    }

    public boolean isValid() {
        return valid.get();
    }

    public void setValid(boolean valid) {
        this.valid.compareAndSet(true, valid);
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
     * This method is re enterable. It has to be in case queue capacity is smaller than number of frames to
     * be dispatched. When it is the case, frame count published so far is stored in the `frameSequence`.     *
     * This method has no responsibility to deal with "collect" stage hence it deals with everything to
     * unblock the collect stage.
     */
    public void tryDispatch() {
        final int workerId = getWorkerId();
        final PageAddressCacheRecord record = records[workerId];
        final SqlExecutionCircuitBreaker circuitBreaker = circuitBreakers[workerId];

        boolean idle = true;
        final RingQueue<PageFrameReduceTask> queue = messageBus.getPageFrameReduceQueue(shard);

        // the sequence used to steal worker jobs
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);

        long cursor;
        int i = dispatchStartIndex;
        dispatchStartIndex = frameCount;
        OUT:
        for (; i < frameCount; i++) {
            // We cannot process work on this thread. If we do the consumer will
            // never get the executions results. Consumer only picks ready to go
            // tasks from the queue.

            while (true) {
                cursor = reducePubSeq.next();
                if (cursor > -1) {
                    queue.get(cursor).of(this, i);
                    LOG.debug()
                            .$("dispatched [shard=").$(shard)
                            .$(", id=").$(getId())
                            .$(", frameIndex=").$(i)
                            .$(", frameCount=").$(frameCount)
                            .$(", cursor=").$(cursor)
                            .I$();
                    reducePubSeq.done(cursor);
                    break;
                } else {
                    idle = false;
                    // start stealing work to unload the queue
                    if (stealWork(queue, reduceSubSeq, record, circuitBreaker)) {
                        continue;
                    }
                    dispatchStartIndex = i;
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
            if (stealWork(queue, reduceSubSeq, record, circuitBreaker)) {
                if (isValid()) {
                    continue;
                }
            }
            break;
        }

        if (idle) {
            stealWork(queue, reduceSubSeq, record, circuitBreaker);
        }
    }

    public boolean stealWork(
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
            dispatchStartIndex = 0;
            reduceCounter.set(0);
            valid.set(true);

            tryDispatch();
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
                .$(", seq=").$(collectSubSeq)
                .I$();

        tryDispatch();
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
