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

package io.questdb.cutlass.text;

import io.questdb.MessageBus;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;

import java.io.Closeable;

public class TextImportJob extends AbstractQueueConsumerJob<TextImportTask> implements Closeable {
    private TextLexer textLexer;

    public TextImportJob(MessageBus messageBus) {
        super(messageBus.getTextImportQueue(), messageBus.getTextImportSubSeq());
        this.textLexer = new TextLexer(messageBus.getConfiguration().getTextConfiguration());
    }

    public static void assignToPool(MessageBus messageBus, WorkerPool pool) {
        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            Job job = new TextImportJob(messageBus);
            pool.assign(i, job);
            pool.freeOnHalt((Closeable) job);
        }
    }

    @Override
    public void close() {
        this.textLexer = Misc.free(textLexer);
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final TextImportTask task = queue.get(cursor);
        final boolean result = task.run(textLexer);
        subSeq.done(cursor);
        return result;
    }
}
