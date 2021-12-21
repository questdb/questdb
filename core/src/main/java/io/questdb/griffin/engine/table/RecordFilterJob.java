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

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameRecord;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.Job;
import io.questdb.mp.MCSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.DirectLongList;
import io.questdb.std.Rnd;
import io.questdb.tasks.PageFrameTask;

public class RecordFilterJob implements Job {

    private final PageFrameRecord[] records;
    private final int[] shards;
    private final int shardCount;
    private final MessageBus messageBus;

    public RecordFilterJob(SqlExecutionContext executionContext) {
        this.messageBus = executionContext.getMessageBus();
        this.shardCount = messageBus.getPageFrameQueueShardCount();
        this.shards = new int[shardCount];
        // fill shards[] with shard indexes
        for (int i = 0; i < shardCount; i++) {
            shards[i] = i;
        }

        // shuffle shard indexes such that each job has its own
        // pass order over the shared queues
        final Rnd rnd = executionContext.getRandom();
        int currentIndex = shardCount;
        int randomIndex;
        while (currentIndex != 0) {
            randomIndex = (int) Math.floor(rnd.nextDouble() * currentIndex);
            currentIndex--;

            final int tmp = shards[currentIndex];
            shards[currentIndex] = shards[randomIndex];
            shards[randomIndex] = tmp;
        }

        final int n = executionContext.getWorkerCount();
        this.records = new PageFrameRecord[n];
        for (int i = 0; i < n; i++) {
            records[i] = new PageFrameRecord();
        }
    }

    public static void filter(PageFrame frame, Function filter, PageFrameRecord record, DirectLongList rows) {
        final long count = frame.getPartitionHi() - frame.getPartitionLo();
        record.setPageFrame(frame);
        for (long r = 0; r < count; r++) {
            record.setRow(r);
            if (filter.getBool(record)) {
                rows.add(r);
            }
        }
    }

    @Override
    public boolean run(int workerId) {
        final PageFrameRecord record = records[workerId];
        boolean useful = false;
        for (int i = 0; i < shardCount; i++) {
            final int shard = shards[i];
            MCSequence subSeq = messageBus.getPageFrameWorkerSubSeq(shard);
            RingQueue<PageFrameTask> queue = messageBus.getPageFrameQueue(shard);
            useful = consumeQueue(queue, subSeq, record) || useful;
        }
        return useful;
    }

    private boolean consumeQueue(RingQueue<PageFrameTask> queue, MCSequence subSeq, PageFrameRecord record) {
        long cursor = subSeq.next();
        if (cursor > -1) {
            final PageFrameTask task = queue.get(cursor);
            // do not release task until filtering is done
            final PageFrame frame = task.getPageFrame();
            final Function filter = task.getFilter();
            final DirectLongList rows = task.getRows();
            rows.clear();
            filter(frame, filter, record, rows);
            subSeq.done(cursor);
            return true;
        }
        return false;
    }
}
