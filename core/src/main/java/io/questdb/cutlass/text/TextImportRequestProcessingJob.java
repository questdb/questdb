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

import io.questdb.cairo.CairoEngine;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;

public class TextImportRequestProcessingJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(TextImportRequestProcessingJob.class);

    private final RingQueue<TextImportRequestTask> requestProcessQueue;
    private final Sequence requestProcessSubSeq;
    private final CairoEngine engine;
    private final int workerCount;

    public TextImportRequestProcessingJob(final CairoEngine engine, int workerCount) {
        this.engine = engine;
        this.workerCount = workerCount;
        this.requestProcessQueue = engine.getMessageBus().getTextImportRequestProcessingQueue();
        this.requestProcessSubSeq = engine.getMessageBus().getTextImportRequestProcessingSubSeq();
    }

    @Override
    protected boolean runSerially() {
        long cursor = requestProcessSubSeq.next();
        if (cursor > -1) {
            TextImportRequestTask task = requestProcessQueue.get(cursor);
            ParallelCsvFileImporter loader = new ParallelCsvFileImporter(engine, workerCount, task.getCancellationToken());
            try {
                loader.of(
                        task.getTableName(),
                        task.getFileName(),
                        task.getPartitionBy(),
                        task.getDelimiter(),
                        task.getTimestampColumnName(),
                        task.getTimestampFormat(),
                        task.isHeaderFlag()
                );
                loader.process();
            } catch (TextException e) {
                LOG.error().$((Throwable)e).$();
            } finally {
                task.setStatus(loader.getStatus());
                requestProcessSubSeq.done(cursor);
            }
            return true;
        }
        return false;
    }
}
