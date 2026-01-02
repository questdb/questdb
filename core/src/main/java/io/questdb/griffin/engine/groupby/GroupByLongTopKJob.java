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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.tasks.GroupByLongTopKTask;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles parallel ORDER BY long_column LIMIT N tasks on shards from a sharded map.
 *
 * @see AsyncGroupByRecordCursorFactory
 */
public class GroupByLongTopKJob extends AbstractQueueConsumerJob<GroupByLongTopKTask> {
    private static final Log LOG = LogFactory.getLog(GroupByLongTopKJob.class);

    public GroupByLongTopKJob(MessageBus messageBus) {
        super(messageBus.getGroupByLongTopKQueue(), messageBus.getGroupByLongTopKSubSeq());
    }

    public static void run(
            int workerId,
            GroupByLongTopKTask task,
            Sequence subSeq,
            long cursor,
            AsyncGroupByAtom stealingAtom
    ) {
        final AtomicBooleanCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final AsyncGroupByAtom atom = task.getAtom();
        final Function longFunc = task.getFunction();
        final int shardIndex = task.getShardIndex();
        final int order = task.getOrder();
        final int limit = task.getLimit();

        task.clear();
        subSeq.done(cursor);

        startedCounter.incrementAndGet();

        final boolean owner = stealingAtom != null && stealingAtom == atom;
        try {
            final int slotId = atom.maybeAcquire(workerId, owner, (ExecutionCircuitBreaker) circuitBreaker);
            try {
                if (circuitBreaker.checkIfTripped()) {
                    return;
                }
                final Map shard = atom.getDestShards().getQuick(shardIndex);
                final DirectLongLongSortedList list = atom.getLongTopKList(slotId, order, limit);
                shard.getCursor().longTopK(list, longFunc);
            } finally {
                atom.release(slotId);
            }
        } catch (Throwable th) {
            LOG.error().$("long top K on shard failed [error=").$(th).I$();
            circuitBreaker.cancel();
        } finally {
            doneLatch.countDown();
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        final GroupByLongTopKTask task = queue.get(cursor);
        run(workerId, task, subSeq, cursor, null);
        return true;
    }
}
