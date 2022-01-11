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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Rnd;

public class PageFrameCleanupJob implements Job {

    private static final Log LOG = LogFactory.getLog(PageFrameCleanupJob.class);

    private final MessageBus messageBus;
    private final int shardCount;
    private final int[] shards;
    private final int pageFrameQueueCapacity;

    public PageFrameCleanupJob(MessageBus messageBus, Rnd rnd) {
        this.messageBus = messageBus;
        this.shardCount = messageBus.getPageFrameReduceShardCount();
        this.pageFrameQueueCapacity = messageBus.getConfiguration().getPageFrameQueueCapacity();

        this.shards = new int[shardCount];
        // fill shards[] with shard indexes
        for (int i = 0; i < shardCount; i++) {
            shards[i] = i;
        }

        // shuffle shard indexes such that each job has its own
        // pass order over the shared queues
        int currentIndex = shardCount;
        int randomIndex;
        while (currentIndex != 0) {
            randomIndex = (int) Math.floor(rnd.nextDouble() * currentIndex);
            currentIndex--;

            final int tmp = shards[currentIndex];
            shards[currentIndex] = shards[randomIndex];
            shards[randomIndex] = tmp;
        }
    }

    public static boolean consumeQueue(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence cleanupSubSeq,
            MessageBus messageBus,
            int shard,
            int pageFrameQueueCapacity
    ) {
        do {
            long cursor = cleanupSubSeq.next();
            if (cursor > -1) {
                final PageFrameReduceTask task = queue.get(cursor);
                try {
                    // frame index adjusted to 1-base
                    final PageFrameSequence<?> frameSequence = task.getFrameSequence();
                    final int frameIndex = task.getFrameIndex() + 1;
                    final int frameCount = frameSequence.getFrameCount();

                    LOG.info()
                            .$("cleanup [shard=").$(shard)
                            .$(", id=").$(frameSequence.getId())
                            .$(", frameIndex=").$(frameIndex)
                            .$(", frameCount=").$(frameCount)
                            .I$();

                    // We have to reset capacity only on max all queue items
                    // What we are avoiding here is resetting capacity on 1000 frames given our queue size
                    // is 32 items. If our particular producer resizes queue items to 10x of the initial size
                    // we let these sizes stick until produce starts to wind down.
                    if (frameIndex > frameCount - pageFrameQueueCapacity) {
                        task.getRows().resetCapacity();
                    }

                    // we assume that frame indexes are published in ascending order
                    // and when we see the last index, we would free up the remaining resources
                    if (frameIndex == frameCount) {
                        LOG.info()
                                .$("cleanup [shard=").$(shard)
                                .$(", id=").$(frameSequence.getId())
                                .$(", removing=").$(frameSequence.getCollectSubSeq())
                                .I$();

                        messageBus.getPageFrameCollectFanOut(shard).remove(frameSequence.getCollectSubSeq());
                        frameSequence.clear();
                    }
                } finally {
                    cleanupSubSeq.done(cursor);
                }
                return false;
            } else if (cursor == -1) {
                // queue is empty, nothing to do
                break;
            }
        } while (true);

        return true;
    }

    @Override
    public boolean run(int workerId) {
        boolean useful = false;
        for (int i = 0; i < shardCount; i++) {
            final int shard = shards[i];
            useful = !consumeQueue(
                    messageBus.getPageFrameReduceQueue(shard),
                    messageBus.getPageFrameCleanupSubSeq(shard),
                    messageBus,
                    shard,
                    pageFrameQueueCapacity
            ) || useful;
        }
        return useful;
    }
}
