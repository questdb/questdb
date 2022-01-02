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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageAddressCache;
import io.questdb.cairo.sql.PageAddressCacheRecord;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.mp.*;
import io.questdb.std.LongList;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class PageFrameDispatchJob implements Job {

    private final MessageBus messageBus;
    private final MCSequence dispatchSubSeq;
    private final RingQueue<PageFrameDispatchTask> dispatchQueue;
    // work stealing records
    private final PageAddressCacheRecord[] records;

    public PageFrameDispatchJob(MessageBus messageBus, int workerCount) {
        this.messageBus = messageBus;
        this.dispatchSubSeq = messageBus.getPageFrameDispatchSubSeq();
        this.dispatchQueue = messageBus.getPageFrameDispatchQueue();

        this.records = new PageAddressCacheRecord[workerCount];
        for (int i = 0; i < workerCount; i++) {
            records[i] = new PageAddressCacheRecord();
        }
    }

    @Override
    public boolean run(int workerId) {
        final long dispatchCursor = dispatchSubSeq.next();
        if (dispatchCursor > -1) {
            final PageFrameDispatchTask tsk = dispatchQueue.get(dispatchCursor);
            final int shard = tsk.getShard();
            final PageAddressCache pageAddressCache = tsk.getPageAddressCache();
            final int frameCount = tsk.getFrameSequenceFrameCount();
            final SymbolTableSource symbolTableSource = tsk.getSymbolTableSource();
            final long frameSequenceId = tsk.getFrameSequenceId();
            final LongList frameRowCounts = tsk.getFrameSequenceFrameRowCounts();
            final Function filter = tsk.getFilter();
            final SCSequence collectSubSeq = tsk.getCollectSubSeq();
            final SOUnboundedCountDownLatch doneLatch = tsk.getFrameSequenceDoneLatch();

            final PageFrameReducer pageFrameReducer = tsk.getReducer();
            final RingQueue<PageFrameReduceTask> queue = messageBus.getPageFrameReduceQueue(shard);
            // publisher sequence to pass work to worker jobs
            final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);
            // the sequence used to steal worker jobs
            final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
            final MCSequence cleanupSubSeq = messageBus.getPageFrameCleanupSubSeq(shard);
            final PageAddressCacheRecord record = records[workerId];

            // todo: reuse
            // Reduce counter is here to provide safe backoff point
            // for job stealing code. It is needed because queue is shared
            // and there is possibility of never ending stealing if we don't
            // specifically count only our items
            final AtomicInteger framesReducedCounter = tsk.getFrameSequenceReduceCounter();

            // Failure indicator to ensure parallel jobs can safely stop
            // processing pieces of work when one of those pieces fail. Additionally,
            // failure indicator is used to provide to user
            final AtomicBoolean frameSequenceValid = tsk.getFrameSequenceValid();

            final int pageFrameQueueCapacity = queue.getCycle();

            try {
                long cursor;
                for (int i = 0; i < frameCount; i++) {
                    // We cannot process work on this thread. If we do the consumer will
                    // never get the executions results. Consumer only picks ready to go
                    // tasks from the queue.
                    while (true) {
                        cursor = reducePubSeq.next();
                        if (cursor > -1) {
                            queue.get(cursor).of(
                                    pageFrameReducer,
                                    frameSequenceId,
                                    pageAddressCache,
                                    i,
                                    frameCount,
                                    frameRowCounts,
                                    symbolTableSource,
                                    filter,
                                    framesReducedCounter,
                                    collectSubSeq,
                                    doneLatch,
                                    frameSequenceValid
                            );
                            reducePubSeq.done(cursor);
                            break;
                        } else {
                            // start stealing work to unload the queue
                            stealQueueWork(shard, queue, reduceSubSeq, cleanupSubSeq, record, pageFrameQueueCapacity);
                        }
                    }
                }

                // join the gang to consume published tasks
                while (framesReducedCounter.get() < frameCount) {
                    stealQueueWork(shard, queue, reduceSubSeq, cleanupSubSeq, record, pageFrameQueueCapacity);
                }
            } finally {
                dispatchSubSeq.done(dispatchCursor);
            }
            return true;
        }
        return false;
    }

    private void stealQueueWork(
            int shard,
            RingQueue<PageFrameReduceTask> queue,
            MCSequence reduceSubSeq,
            MCSequence cleanupSubSeq,
            PageAddressCacheRecord record,
            int pageFrameQueueCapacity
    ) {
        if (
                PageFrameReduceJob.consumeQueue(queue, reduceSubSeq, record) &&
                        PageFrameCleanupJob.consumeQueue(
                                queue,
                                cleanupSubSeq,
                                messageBus,
                                shard,
                                pageFrameQueueCapacity
                        )
        ) {
            LockSupport.parkNanos(1);
        }
    }
}
