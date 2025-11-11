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

package io.questdb.cutlass.text;

import io.questdb.MessageBus;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class CopyImportJob extends AbstractQueueConsumerJob<CopyImportTask> implements Closeable {
    private static final int INDEX_MERGE_LIST_CAPACITY = 64;
    private final long fileBufAddr;
    private long fileBufSize;
    private CsvFileIndexer indexer;
    private DirectLongList mergeIndexes;
    private TextLexerWrapper tlw;
    private Path tmpPath1;
    private Path tmpPath2;
    private DirectUtf16Sink utf16Sink;
    private DirectUtf8Sink utf8Sink;
    private final Decimal256 decimal256;

    public CopyImportJob(MessageBus messageBus) {
        super(messageBus.getCopyImportQueue(), messageBus.getCopyImportSubSeq());
        try {
            this.tlw = new TextLexerWrapper(messageBus.getConfiguration().getTextConfiguration());
            this.fileBufSize = messageBus.getConfiguration().getSqlCopyBufferSize();
            this.fileBufAddr = Unsafe.malloc(fileBufSize, MemoryTag.NATIVE_IMPORT);
            this.indexer = new CsvFileIndexer(messageBus.getConfiguration());
            int utf8SinkSize = messageBus.getConfiguration().getTextConfiguration().getUtf8SinkSize();
            this.utf16Sink = new DirectUtf16Sink(utf8SinkSize);
            this.utf8Sink = new DirectUtf8Sink(utf8SinkSize);
            this.decimal256 = new Decimal256();
            this.mergeIndexes = new DirectLongList(INDEX_MERGE_LIST_CAPACITY, MemoryTag.NATIVE_IMPORT);
            this.tmpPath1 = new Path();
            this.tmpPath2 = new Path();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    public static void assignToPool(MessageBus messageBus, WorkerPool sharedPoolWrite) {
        for (int i = 0, n = sharedPoolWrite.getWorkerCount(); i < n; i++) {
            CopyImportJob job = new CopyImportJob(messageBus);
            sharedPoolWrite.assign(i, job);
            sharedPoolWrite.freeOnExit(job);
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
        this.utf16Sink = Misc.free(utf16Sink);
        this.utf8Sink = Misc.free(utf8Sink);
        this.tmpPath1 = Misc.free(tmpPath1);
        this.tmpPath2 = Misc.free(tmpPath2);
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        final CopyImportTask task = queue.get(cursor);
        final boolean result = task.run(tlw, indexer, utf16Sink, utf8Sink, decimal256, mergeIndexes, fileBufAddr, fileBufSize, tmpPath1, tmpPath2);
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
