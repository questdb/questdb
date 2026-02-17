/*******************************************************************************
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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.tasks.ColumnTask;
import org.jetbrains.annotations.NotNull;

public class ColumnTaskJob extends AbstractQueueConsumerJob<ColumnTask> {
    public ColumnTaskJob(MessageBus messageBus) {
        super(messageBus.getColumnTaskQueue(), messageBus.getColumnTaskSubSeq());
    }

    public static void processColumnTask(ColumnTask task, long cursor, @NotNull Sequence subSeq) {
        final int columnIndex = task.getColumnIndex();
        final int columnType = task.getColumnType();
        final long timestampColumnIndex = task.getTimestampColumnIndex();
        final long lon0 = task.getLong0();
        final long long1 = task.getLong1();
        final long long2 = task.getLong2();
        final long long3 = task.getLong3();
        final long long4 = task.getLong4();
        final TableWriter.ColumnTaskHandler taskHandler = task.getTaskHandler();
        final CountDownLatchSPI countDownLatchSPI = task.getCountDownLatchSPI();
        subSeq.done(cursor);

        try {
            taskHandler.run(
                    columnIndex,
                    columnType,
                    timestampColumnIndex,
                    lon0,
                    long1,
                    long2,
                    long3,
                    long4
            );
        } finally {
            countDownLatchSPI.countDown();
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        ColumnTask task = queue.get(cursor);
        processColumnTask(task, cursor, subSeq);
        return true;
    }
}
