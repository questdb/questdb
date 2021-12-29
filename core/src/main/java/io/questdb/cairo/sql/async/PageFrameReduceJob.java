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
import io.questdb.cairo.sql.PageAddressCacheRecord;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.DirectLongList;
import io.questdb.std.Rnd;

import java.util.concurrent.atomic.AtomicInteger;

public class PageFrameReduceJob implements Job {

    private final PageAddressCacheRecord[] records;
    private final int[] shards;
    private final int shardCount;
    private final MessageBus messageBus;

    public PageFrameReduceJob(MessageBus bus, Rnd rnd, int workerCount) {
        this.messageBus = bus;
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

        this.records = new PageAddressCacheRecord[workerCount];
        for (int i = 0; i < workerCount; i++) {
            records[i] = new PageAddressCacheRecord();
        }
    }

    public static void filter(long frameRowCount, Function filter, PageAddressCacheRecord record, DirectLongList rows) {
        rows.clear();
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);
            if (filter.getBool(record)) {
                rows.add(r);
            }
        }
    }

    public static void handleTask(PageAddressCacheRecord record, PageFrameReduceTask task) {
        final AtomicInteger framesReducedCounter = task.getFramesReducedCounter();
        try {
            // do not release task until filtering is done
            record.of(task.getSymbolTableSource(), task.getPageAddressCache());
            record.setFrameIndex(task.getFrameIndex());
            filter(
                    task.getFrameRowCount(),
                    task.getFilter(),
                    record,
                    task.getRows()
            );
        } catch (Throwable e) {
            task.setFailed(true);
            throw e;
        } finally {
            framesReducedCounter.incrementAndGet();
        }
    }

    @Override
    public boolean run(int workerId) {
        final PageAddressCacheRecord record = records[workerId];
        boolean useful = false;
        for (int i = 0; i < shardCount; i++) {
            final int shard = shards[i];
            MCSequence subSeq = messageBus.getPageFrameReduceSubSeq(shard);
            RingQueue<PageFrameReduceTask> queue = messageBus.getPageFrameReduceQueue(shard);
            useful = consumeQueue(queue, subSeq, record) || useful;
        }
        return useful;
    }

    private boolean consumeQueue(
            RingQueue<PageFrameReduceTask> queue,
            MCSequence subSeq,
            PageAddressCacheRecord record
    ) {
        long cursor = subSeq.next();
        if (cursor > -1) {
            final PageFrameReduceTask task = queue.get(cursor);
            try {
                handleTask(record, task);
            } finally {
                subSeq.done(cursor);
            }
            return true;
        }
        return false;
    }
}
