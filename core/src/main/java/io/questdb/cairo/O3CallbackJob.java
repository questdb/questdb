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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.tasks.O3CallbackTask;
import org.jetbrains.annotations.Nullable;

public class O3CallbackJob extends AbstractQueueConsumerJob<O3CallbackTask> {
    public O3CallbackJob(MessageBus messageBus) {
        super(messageBus.getO3CallbackQueue(), messageBus.getO3CallbackSubSeq());
    }

    public static void runCallbackWithCol(O3CallbackTask task, long cursor, @Nullable Sequence subSeq) {
        final int columnIndex = task.getColumnIndex();
        final int columnType = task.getColumnType();
        final long mergedTimestampsAddr = task.getMergedTimestampsAddr();
        final long valueCount = task.getValueCount();
        final TableWriter.O3ColumnUpdateMethod callbackMethod = task.getWriterCallbackMethod();
        final CountDownLatchSPI countDownLatchSPI = task.getCountDownLatchSPI();
        if (subSeq != null) {
            subSeq.done(cursor);
        }

        try {
            callbackMethod.run(
                    columnIndex,
                    columnType,
                    mergedTimestampsAddr,
                    valueCount
            );
        } finally {
            countDownLatchSPI.countDown();
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        O3CallbackTask task = queue.get(cursor);
        // copy task on stack so that publisher has fighting chance of
        // publishing all it has to the queue

        final boolean locked = task.tryLock();
        if (locked) {
            runCallbackWithCol(task, cursor, subSeq);
        } else {
            subSeq.done(cursor);
        }

        return true;
    }
}
