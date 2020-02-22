/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.MessageBus;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.tasks.VectorAggregateTask;

public class GroupByNotKeyedJob implements Job {
    private final RingQueue<VectorAggregateTask> queue;
    private final Sequence subSeq;

    public GroupByNotKeyedJob(MessageBus messageBus) {
        this.queue = messageBus.getVectorAggregateQueue();
        this.subSeq = messageBus.getVectorAggregateSubSequence();
    }

    @Override
    public boolean run() {
        long cursor = subSeq.next();
        if (cursor < 0) {
            return false;
        }

        final VectorAggregateTask queueItem = queue.get(cursor);
        final VectorAggregateEntry entry = queueItem.entry;
        subSeq.done(cursor);
        return entry.run();
    }
}
