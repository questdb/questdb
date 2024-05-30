/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.tasks.GroupByMergeShardTask;

import java.util.concurrent.atomic.AtomicInteger;

public class GroupByMergeShardJob extends AbstractQueueConsumerJob<GroupByMergeShardTask> {
    private static final Log LOG = LogFactory.getLog(GroupByMergeShardJob.class);

    public GroupByMergeShardJob(MessageBus messageBus) {
        super(messageBus.getGroupByMergeShardQueue(), messageBus.getGroupByMergeShardSubSeq());
    }

    public static void run(int workerId, GroupByMergeShardTask task, Sequence subSeq, long cursor) {
        final AtomicBooleanCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final AsyncGroupByAtom atom = task.getAtom();
        final int shardIndex = task.getShardIndex();

        task.clear();
        subSeq.done(cursor);

        startedCounter.incrementAndGet();

        int slotId = -1;
        try {
            if (atom.isMergeLockRequired()) {
                slotId = atom.acquire(workerId, circuitBreaker);
            }
            if (circuitBreaker.checkIfTripped()) {
                return;
            }
            atom.mergeShard(slotId, shardIndex);
        } catch (Throwable e) {
            LOG.error().$("merge shard failed [ex=").$(e).I$();
            circuitBreaker.cancel();
        } finally {
            if (atom.isMergeLockRequired()) {
                atom.release(slotId);
            }
            doneLatch.countDown();
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        final GroupByMergeShardTask task = queue.get(cursor);
        run(workerId, task, subSeq, cursor);
        return true;
    }
}
