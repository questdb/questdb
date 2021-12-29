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

import java.util.concurrent.atomic.AtomicInteger;

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
            final int frameCount = tsk.getFrameCount();
            final SymbolTableSource symbolTableSource = tsk.getSymbolTableSource();
            final long producerId = tsk.getProducerId();
            final LongList frameRowCounts = tsk.getFrameRowCounts();
            final Function filter = tsk.getFilter();
            final SCSequence collectSubSeq = tsk.getCollectSubSeq();
            final RingQueue<PageFrameReduceTask> queue = messageBus.getPageFrameReduceQueue(shard);
            // publisher sequence to pass work to worker jobs
            final MPSequence pubSeq = messageBus.getPageFrameReducePubSeq(shard);
            // the sequence used to steal worker jobs
            final MCSequence subSeq = messageBus.getPageFrameReduceSubSeq(shard);
            final PageAddressCacheRecord record = records[workerId];
            final MCSequence cleanupSubSeq = messageBus.getPageFrameCleanupSubSeq(shard);

            // todo: reuse
            final AtomicInteger framesCollectedCounter = new AtomicInteger();
            final AtomicInteger framesReducedCounter = new AtomicInteger();

            long cursor;
            for (int i = 0; i < frameCount; i++) {
                // We cannot process work on this thread. If we do the consumer will
                // never get the executions results. Consumer only picks ready to go
                // tasks from the queue.
                while (true) {
                    cursor = pubSeq.next();
                    if (cursor > -1) {
                        queue.get(cursor).of(
                                producerId,
                                pageAddressCache,
                                i,
                                frameCount,
                                frameRowCounts,
                                symbolTableSource,
                                filter,
                                framesCollectedCounter,
                                framesReducedCounter,
                                collectSubSeq
                        );
                        pubSeq.done(cursor);
                        break;
                    } else {
                        // start stealing work to unload the queue
                        cursor = subSeq.next();
                        if (cursor > -1) {
                            try {
                                PageFrameReduceJob.handleTask(
                                        record,
                                        queue.get(cursor)
                                );
                            } finally {
                                subSeq.done(cursor);
                            }
                        } else {
                            cursor = cleanupSubSeq.next();
                            if (cursor > -1) {
                                PageFrameCleanupJob.handleTask(
                                        messageBus,
                                        shard,
                                        queue.get(cursor)
                                );
                                cleanupSubSeq.done(cursor);
                            }
                        }
                    }
                }
            }

            // join the gang to consume published tasks
            while (framesReducedCounter.get() < frameCount) {
                cursor = subSeq.next();
                if (cursor > -1) {
                    try {
                        PageFrameReduceJob.handleTask(
                                record,
                                queue.get(cursor)
                        );
                    } finally {
                        subSeq.done(cursor);
                    }
                }
            }
            dispatchSubSeq.done(dispatchCursor);
            return true;
        }
        return false;
    }
}
