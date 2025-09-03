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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpResponseArrayWriteState;
import io.questdb.network.SuspendEvent;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class ExportQueryProcessorState implements Mutable, Closeable {
    static long PARQUET_BUFFER_SIZE = 8192;
    final StringSink query = new StringSink();
    private final HttpConnectionContext httpConnectionContext;
    HttpResponseArrayWriteState arrayState = new HttpResponseArrayWriteState();
    int columnIndex;
    boolean columnValueFullySent = true;
    String copyID;
    long count;
    boolean countRows = false;
    RecordCursor cursor;
    char delimiter = ',';
    String fileName;
    String fmt = null;
    boolean hasNext;
    RecordMetadata metadata;
    boolean noMeta = false;
    long parquetFileAddress = 0;
    long parquetFileBuffer = 0;
    long parquetFileFd = -1;
    long parquetFileOffset = 0;
    String parquetFilePath;
    long parquetFileSize = 0;
    boolean pausedQuery = false;
    int queryState;
    Record record;
    RecordCursorFactory recordCursorFactory;
    Rnd rnd;
    long skip;
    long stop;
    SuspendEvent suspendEvent;
    boolean waitingForCopy;
    private boolean queryCacheable = false;

    public ExportQueryProcessorState(HttpConnectionContext httpConnectionContext) {
        this.httpConnectionContext = httpConnectionContext;
        clear();
    }

    @Override
    public void clear() {
        delimiter = ',';
        fileName = null;
        fmt = null;
        rnd = null;
        record = null;
        cursor = Misc.free(cursor);
        if (recordCursorFactory != null) {
            if (queryCacheable) {
                httpConnectionContext.getSelectCache().put(query, recordCursorFactory);
            } else {
                recordCursorFactory.close();
            }
            recordCursorFactory = null;
        }
        queryCacheable = false;
        query.clear();
        queryState = JsonQueryProcessorState.QUERY_SETUP_FIRST_RECORD;
        columnIndex = 0;
        skip = 0;
        stop = 0;
        count = 0;
        noMeta = false;
        countRows = false;
        pausedQuery = false;
        arrayState.clear();
        columnValueFullySent = true;
        metadata = null;
        copyID = null;
        waitingForCopy = false;
        suspendEvent = null;
        parquetFileFd = -1;
        parquetFileSize = 0;
        parquetFileOffset = 0;
        parquetFilePath = null;
        if (parquetFileBuffer != 0) {
            Unsafe.free(parquetFileBuffer, PARQUET_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            parquetFileBuffer = 0;
        }
        if (parquetFileFd != -1) {
            // Close any open file descriptor
            parquetFileFd = -1;
        }
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
        Misc.free(suspendEvent);
        if (parquetFileBuffer != 0) {
            Unsafe.free(parquetFileBuffer, PARQUET_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            parquetFileBuffer = 0;
        }
    }

    public long getFd() {
        return httpConnectionContext.getFd();
    }

    void setQueryCacheable(boolean queryCacheable) {
        this.queryCacheable = queryCacheable;
    }
}
