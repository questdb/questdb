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
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;

public class PageFrameCleanupJob implements Job {
    private final MessageBus messageBus;
    private final int shardCount;
    private final int[] shards;

    public PageFrameCleanupJob(MessageBus messageBus, Rnd rnd) {
        this.messageBus = messageBus;
        this.shardCount = messageBus.getPageFrameReduceShardCount();

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

    @Override
    public boolean run(int workerId) {
        boolean useful = false;
        for (int i = 0; i < shardCount; i++) {
            final int shard = shards[i];
            MCSequence cleanupSubSeq = messageBus.getPageFrameCleanupSubSeq(shard);
            RingQueue<PageFrameReduceTask> queue = messageBus.getPageFrameReduceQueue(shard);
            useful = consumeQueue(queue, cleanupSubSeq, i) || useful;
        }
        return useful;
    }

    private boolean consumeQueue(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence cleanupSubSeq,
            int shard
    ) {
        long cursor = cleanupSubSeq.next();
        if (cursor > -1) {
            final PageFrameReduceTask task = queue.get(cursor);
            try {
                handleTask(messageBus, shard, task);
            } finally {
                cleanupSubSeq.done(cursor);
            }
            return true;
        }
        return false;
    }

    public static void handleTask(MessageBus messageBus, int shard, PageFrameReduceTask task) {
        if (task.getFramesCollectedCounter().get() == 0) {
            task.getPageAddressCache().clear();
            task.getFrameRowCounts().clear();
            Misc.free(task.getSymbolTableSource());
            messageBus.getPageFrameCollectFanOut(shard).remove(task.getCollectSubSeq());
        }
    }
}
