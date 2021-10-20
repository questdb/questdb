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
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.tasks.VectorAggregateTask;

public class GroupByJob extends AbstractQueueConsumerJob<VectorAggregateTask> {

    public GroupByJob(MessageBus messageBus) {
        super(messageBus.getVectorAggregateQueue(), messageBus.getVectorAggregateSubSeq());
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final VectorAggregateEntry entry = queue.get(cursor).entry;
        final boolean result = entry.run(workerId);
        subSeq.done(cursor);
        return result;
    }
}
