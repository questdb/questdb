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
import io.questdb.std.Mutable;
import io.questdb.std.Rnd;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class PageFrameSequence<T extends StatefulAtom> implements Mutable {
    private static final int OWNER_NONE = 0;
    private static final int OWNER_WORK_STEALING = 1;
    private static final int OWNER_ASYNC = 2;
    private final static Log LOG = LogFactory.getLog(PageFrameSequence.class);

    public final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final AtomicInteger reduceCounter = new AtomicInteger(0);
    private final LongList frameRowCounts = new LongList();
    private final PageFrameReducer reducer;
    private final PageAddressCache pageAddressCache;
    private final MessageBus messageBus;
    private final AtomicInteger owner = new AtomicInteger(OWNER_NONE);
    private long id;
    private int shard;
    private int frameCount;
    private SCSequence collectSubSeq;
    private PageFrameCursor pageFrameCursor;
    private T atom;
    private PageAddressCacheRecord[] records;
    // we need this to restart execution for `toTop`
    private MPSequence dispatchPubSeq;
    private RingQueue<PageFrameDispatchTask> pageFrameDispatchQueue;
    private int dispatchStartIndex = 0;

    public PageFrameSequence(CairoConfiguration configuration, MessageBus messageBus, PageFrameReducer reducer) {
        this.reducer = reducer;
        this.pageAddressCache = new PageAddressCache(configuration);
        this.messageBus = messageBus;
    }

    public void await() {
        LOG.info()
                .$("awaiting completion [shard=").$(shard)
                .$(", id=").$(id)
                .$(", frameCount=").$(frameCount)
                .I$();

        final RingQueue<PageFrameReduceTask> queue = messageBus.getPageFrameReduceQueue(shard);
        final MCSequence pageFrameReduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        while (doneLatch.getCount() == 0) {
            final PageAddressCacheRecord rec = records[getWorkerId()];
            // we were asked to steal work from dispatch queue and beyond, as much as we can
            if (PageFrameReduceJob.consumeQueue(queue, pageFrameReduceSubSeq, rec)) {
                long cursor = collectSubSeq.next();
                if (cursor > -1) {
                    // discard collect items
                    final PageFrameReduceTask tsk = queue.get(cursor);
                    if (tsk.getFrameSequence().getId() == id) {
                        tsk.collected();
                    }
                    collectSubSeq.done(cursor);
                } else {
                    LockSupport.parkNanos(1);
                }
            }
        }
    }

    @Override
    public void clear() {
        // prepare different frame sequence using the same object instance
        this.frameCount = 0;
        pageAddressCache.clear();
        pageFrameCursor = Misc.free(pageFrameCursor);
        // collect sequence mau not be set here when
        // factory is closed without using cursor
        if (collectSubSeq != null) {
            messageBus.getPageFrameCollectFanOut(shard).remove(collectSubSeq);
            collectSubSeq.clear();
        }
        this.dispatchStartIndex = 0;
    }

    public PageFrameSequence<T> dispatch(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            SCSequence collectSubSeq,
            T atom
    ) throws SqlException {

        // allow entry for 0 - main thread that is a non-worker
        initWorkerRecords(executionContext.getWorkerCount() + 1);

        final Rnd rnd = executionContext.getAsyncRandom();
        final MessageBus bus = executionContext.getMessageBus();
        // before thread begins we will need to pick a shard
        // of queues that we will interact with
        final int shard = rnd.nextInt(bus.getPageFrameReduceShardCount());
        PageFrameCursor pageFrameCursor = base.getPageFrameCursor(executionContext);

        try {
            final MPSequence dispatchPubSeq = bus.getPageFrameDispatchPubSeq();
            final RingQueue<PageFrameDispatchTask> pageFrameDispatchQueue = bus.getPageFrameDispatchQueue();

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

            prepareForDispatch(
                    shard,
                    rnd.nextLong(),
                    frameIndex,
                    pageFrameCursor,
                    atom,
                    collectSubSeq,
                    dispatchPubSeq,
                    pageFrameDispatchQueue
            );

            // It is essential to init the atom after we prepared sequence for dispatch.
            // If atom is to fail, we will be releasing whatever we prepared.
            atom.init(pageFrameCursor, executionContext);

            // dispatch message only if there is anything to dispatch
            if (frameIndex > 0) {
                long dispatchCursor;
                do {
                    dispatchCursor = dispatchPubSeq.next();
                    if (dispatchCursor < 0) {
                        stealWork();
                        LockSupport.parkNanos(1);
                    } else {
                        break;
                    }
                } while (true);

                // We need to subscribe publisher sequence before we return
                // control to the caller of this method. However, this sequence
                // will be unsubscribed asynchronously.
                bus.getPageFrameCollectFanOut(shard).and(collectSubSeq);
                LOG.info()
                        .$("added [shard=").$(shard)
                        .$(", id=").$(id)
                        .$(", seq=").$(collectSubSeq)
                        .I$();

                PageFrameDispatchTask dispatchTask = pageFrameDispatchQueue.get(dispatchCursor);
                dispatchTask.of(this);
                dispatchPubSeq.done(dispatchCursor);
            }
        } catch (Throwable e) {
            this.pageFrameCursor = Misc.free(this.pageFrameCursor);
            throw e;
        }
        return this;
    }

    public T getAtom() {
        return atom;
    }

    public int getDispatchStartIndex() {
        return dispatchStartIndex;
    }

    public void setDispatchStartIndex(int i) {
        this.dispatchStartIndex = i;
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

    public SymbolTableSource getSymbolTableSource() {
        return pageFrameCursor;
    }

    /**
     * Async owner is allowed to enter only once. The expectation is that all frames will be dispatched
     * in the first attempt.
     *
     * @return true if this sequence dispatch belongs to async owner, work stealing must not touch this.
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
     * @return true is this sequence dispatch is owned by work stealing algo.
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

    public void stealWork() {
        final PageAddressCacheRecord rec = records[getWorkerId()];
        // we were asked to steal work from dispatch queue and beyond, as much as we can
        PageFrameDispatchJob.handleTask(this, rec, messageBus, true);

        // now we need to steal all dispatch work from the queue until we reach either:
        // - end of the queue
        // - find out publisher id
        final MCSequence dispatchSubSeq = messageBus.getPageFrameDispatchSubSeq();
        while (true) {
            final long c = dispatchSubSeq.next();
            if (c > -1) {
                PageFrameSequence<?> s = pageFrameDispatchQueue.get(c).getFrameSequence();

                if (s == this) {
                    dispatchSubSeq.done(c);
                    break;
                }

                // We are entering all frame sequences in work stealing mode, which means
                // they can end up being partially dispatched. This is done because we
                // cannot afford to enter infinite loop here
                PageFrameDispatchJob.handleTask(s, rec, messageBus, true);
            } else {
                break;
            }
        }
    }

    public void toTop() {
        if (frameCount > 0) {
            LOG.info().$("toTop [shard=").$(shard)
                    .$(", id=").$(id)
                    .I$();

            await();

            // done latch is reset by method call above
            doneLatch.reset();
            dispatchStartIndex = 0;
            reduceCounter.set(0);
            this.pageFrameCursor.toTop();
            long dispatchCursor;
            do {
                dispatchCursor = dispatchPubSeq.next();
                if (dispatchCursor < 0) {
                    stealWork();
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

    private void initWorkerRecords(int workerCount) {
        if (records == null || records.length < workerCount) {
            this.records = new PageAddressCacheRecord[workerCount];
            for (int i = 0; i < workerCount; i++) {
                this.records[i] = new PageAddressCacheRecord();
            }
        }
    }

    private void prepareForDispatch(
            int shard,
            long frameSequenceId,
            int frameCount,
            PageFrameCursor pageFrameCursor,
            T atom,
            SCSequence collectSubSeq,
            MPSequence dispatchPubSeq,
            RingQueue<PageFrameDispatchTask> pageFrameDispatchQueue
    ) {
        this.id = frameSequenceId;
        this.doneLatch.reset();
        this.valid.set(true);
        this.reduceCounter.set(0);
        this.shard = shard;
        this.frameCount = frameCount;
        assert this.pageFrameCursor == null;
        this.pageFrameCursor = pageFrameCursor;
        this.atom = atom;
        this.collectSubSeq = collectSubSeq;
        this.dispatchPubSeq = dispatchPubSeq;
        this.pageFrameDispatchQueue = pageFrameDispatchQueue;
    }
}
