/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.tasks.LatestByTask;
import org.jetbrains.annotations.NotNull;

public class LatestByAllIndexedJob extends AbstractQueueConsumerJob<LatestByTask> {
    private final MessageBus messageBus;

    public LatestByAllIndexedJob(MessageBus messageBus) {
        super(messageBus.getLatestByQueue(), messageBus.getLatestBySubSeq());
        this.messageBus = messageBus;
    }

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        final QueryParallelFiberDispatcher dispatcher = messageBus.getQueryParallelFiberDispatcher();
        return dispatcher != null
                ? !dispatcher.consumeLatestBy(workerContext.carrierId())
                : super.run(workerContext);
    }

    @Override
    protected boolean doRun(long cursor, WorkerContext workerContext) {
        final LatestByTask task = queue.get(cursor);
        try {
            return task.run();
        } finally {
            subSeq.done(cursor);
        }
    }
}
