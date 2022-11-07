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
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TextImportJob extends AbstractQueueConsumerJob<TextImportTask> implements Closeable {
    private static final int INDEX_MERGE_LIST_CAPACITY = 64;
    private TextLexerWrapper tlw;
    private CsvFileIndexer indexer;
    private DirectCharSink utf8Sink;
    private DirectLongList mergeIndexes;
    private Path tmpPath1;
    private Path tmpPath2;
    private final long fileBufAddr;
    private long fileBufSize;

    public TextImportJob(MessageBus messageBus) {
        super(messageBus.getTextImportQueue(), messageBus.getTextImportSubSeq());
        this.tlw = new TextLexerWrapper(messageBus.getConfiguration().getTextConfiguration());
        this.fileBufSize = messageBus.getConfiguration().getSqlCopyBufferSize();
        this.fileBufAddr = Unsafe.malloc(fileBufSize, MemoryTag.NATIVE_IMPORT);
        this.indexer = new CsvFileIndexer(messageBus.getConfiguration());
        this.utf8Sink = new DirectCharSink(messageBus.getConfiguration().getTextConfiguration().getUtf8SinkSize());
        this.mergeIndexes = new DirectLongList(INDEX_MERGE_LIST_CAPACITY, MemoryTag.NATIVE_IMPORT);
        this.tmpPath1 = new Path();
        this.tmpPath2 = new Path();
    }

    public static void assignToPool(MessageBus messageBus, WorkerPool pool) {
        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            Job job = new TextImportJob(messageBus);
            pool.assign(i, job);
            pool.freeOnExit((Closeable) job);
        }
    }

    @Override
    public void close() {
        this.tlw = Misc.free(tlw);
        this.indexer = Misc.free(indexer);
        if (fileBufSize > 0) {
            Unsafe.free(fileBufAddr, fileBufSize, MemoryTag.NATIVE_IMPORT);
            fileBufSize = 0;
        }
        this.mergeIndexes = Misc.free(this.mergeIndexes);
        this.utf8Sink = Misc.free(utf8Sink);
        this.tmpPath1 = Misc.free(tmpPath1);
        this.tmpPath2 = Misc.free(tmpPath2);
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        final TextImportTask task = queue.get(cursor);
        final boolean result = task.run(tlw, indexer, utf8Sink, mergeIndexes, fileBufAddr, fileBufSize, tmpPath1, tmpPath2);
        subSeq.done(cursor);
        return result;
    }

    Path getTmpPath1() {
        return tmpPath1;
    }

    Path getTmpPath2() {
        return tmpPath2;
    }
}
