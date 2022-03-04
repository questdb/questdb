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
import io.questdb.cairo.sql.PageAddressCacheRecord;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class PageFrameDispatchJob implements Job {

    private final static Log LOG = LogFactory.getLog(PageFrameDispatchJob.class);

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

    public static boolean stealWork(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence reduceSubSeq,
            PageAddressCacheRecord record
    ) {
        if (PageFrameReduceJob.consumeQueue(queue, reduceSubSeq, record)) {
            LockSupport.parkNanos(1);
            return false;
        }
        return true;
    }

    @Override
    public boolean run(int workerId) {
        final long dispatchCursor = dispatchSubSeq.next();
        if (dispatchCursor > -1) {
            try {
                handleTask(
                        dispatchQueue.get(dispatchCursor).getFrameSequence(),
                        records[workerId],
                        messageBus,
                        false
                );
            } finally {
                dispatchSubSeq.done(dispatchCursor);
            }
            return true;
        }
        return false;
    }

    /**
     * In work stealing mode this method is re enterable. It has to be in case queue capacity
     * is smaller than number of frames to be dispatched. When it is the case, frame count published so far
     * is stored in the `frameSequence`. Generally speaking, sequence cannot be dispatched via normal and
     * work stealing mode at the same time. When one mode grabs, the other must yield. This is because in normal
     * mode the method will publish all frames. It has no responsibility to deal with "collect" stage hence it
     * deals with everything to unblock the collect stage.
     *
     * @param frameSequence frame sequence instance
     * @param record pass-through instance to be used by the reducer
     * @param messageBus message bus with all the wires
     * @param workStealingMode a flag, see method description
     */
    static void handleTask(
            PageFrameSequence<?> frameSequence,
            PageAddressCacheRecord record,
            MessageBus messageBus,
            boolean workStealingMode
    ) {
        boolean idle = true;
        final int shard = frameSequence.getShard();
        final RingQueue<PageFrameReduceTask> queue = messageBus.getPageFrameReduceQueue(shard);

        // the sequence used to steal worker jobs
        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);

        if ((workStealingMode && frameSequence.isWorkStealingOwner() || (!workStealingMode && frameSequence.isAsyncOwner()))) {
            final int frameCount = frameSequence.getFrameCount();
            final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);

            // Reduce counter is here to provide safe backoff point
            // for job stealing code. It is needed because queue is shared
            // and there is possibility of never ending stealing if we don't
            // specifically count only our items
            final AtomicInteger framesReducedCounter = frameSequence.getReduceCounter();

            long cursor;
            int i = frameSequence.getDispatchStartIndex();
            frameSequence.setDispatchStartIndex(frameCount);
            OUT:
            for (; i < frameCount; i++) {
                // We cannot process work on this thread. If we do the consumer will
                // never get the executions results. Consumer only picks ready to go
                // tasks from the queue.

                while (true) {
                    cursor = reducePubSeq.next();
                    if (cursor > -1) {
                        queue.get(cursor).of(frameSequence, i);
                        LOG.debug()
                                .$("dispatched [shard=").$(shard)
                                .$(", id=").$(frameSequence.getId())
                                .$(", staling=").$(workStealingMode)
                                .$(", frameIndex=").$(i)
                                .$(", frameCount=").$(frameCount)
                                .$(", cursor=").$(cursor)
                                .I$();
                        reducePubSeq.done(cursor);
                        break;
                    } else {
                        idle = false;
                        // start stealing work to unload the queue
                        if (stealWork(queue, reduceSubSeq, record) || !workStealingMode) {
                            continue;
                        }
                        frameSequence.setDispatchStartIndex(i);
                        break OUT;
                    }
                }
            }

            // join the gang to consume published tasks
            while (framesReducedCounter.get() < frameCount) {
                idle = false;
                if (stealWork(queue, reduceSubSeq, record) || !workStealingMode) {
                    continue;
                }
                break;
            }
        }

        if (idle && workStealingMode) {
            stealWork(queue, reduceSubSeq, record);
        }
    }
}
