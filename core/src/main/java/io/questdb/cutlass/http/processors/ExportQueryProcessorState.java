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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpResponseArrayWriteState;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.HTTPSerialParquetExporter;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.model.ExportModel;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class ExportQueryProcessorState implements Mutable, Closeable {
    final StringSink fileName = new StringSink();
    final StringSink sqlText = new StringSink();
    private final CopyExportContext copyExportContext;
    private final StringSink errorMessage = new StringSink();
    private final ExportModel exportModel = new ExportModel();
    private final HttpConnectionContext httpConnectionContext;
    HttpResponseArrayWriteState arrayState = new HttpResponseArrayWriteState();
    int columnIndex;
    boolean columnValueFullySent = true;
    long copyID = -1;
    long count;
    boolean countRows = false;
    RecordCursor cursor;
    char delimiter = ',';
    boolean descending;
    boolean firstParquetWriteCall = true;
    boolean hasNext;
    RecordMetadata metadata;
    boolean noMeta = false;
    PageFrameCursor pageFrameCursor;
    long parquetFileOffset = 0;
    boolean pausedQuery = false;
    int queryState;
    Record record;
    RecordCursorFactory recordCursorFactory;
    Rnd rnd;
    boolean serialExporterInit = false;
    long skip;
    long stop;
    CopyExportRequestTask task = new CopyExportRequestTask();
    private CreateTableOperation createParquetOp;
    private int errorPosition;
    private String parquetExportTableName;
    private boolean queryCacheable = false;
    private HTTPSerialParquetExporter serialParquetExporter;
    long timeout;
    private final ParquetWriteCallback writeCallback = new ParquetWriteCallback();

    public ExportQueryProcessorState(HttpConnectionContext httpConnectionContext, CopyExportContext copyContext) {
        this.httpConnectionContext = httpConnectionContext;
        this.copyExportContext = copyContext;
        clear();
    }

    @Override
    public void clear() {
        delimiter = ',';
        fileName.clear();
        rnd = null;
        record = null;
        cursor = Misc.free(cursor);
        pageFrameCursor = Misc.free(pageFrameCursor);
        firstParquetWriteCall = true;
        if (recordCursorFactory != null) {
            if (queryCacheable) {
                httpConnectionContext.getSelectCache().put(sqlText, recordCursorFactory);
            } else {
                recordCursorFactory.close();
            }
            recordCursorFactory = null;
        }
        queryCacheable = false;
        sqlText.clear();
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
        releaseExportEntry();
        createParquetOp = Misc.free(createParquetOp);
        parquetExportTableName = null;
        parquetFileOffset = 0;
        exportModel.clear();
        errorMessage.clear();
        errorPosition = 0;
        serialExporterInit = false;
        task.clear();
        writeCallback.of(null, null);
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
        pageFrameCursor = Misc.free(pageFrameCursor);
        task = Misc.free(task);
        serialParquetExporter = null;
        writeCallback.of(null, null);
    }

    public ExportModel getExportModel() {
        return exportModel;
    }

    public long getFd() {
        return httpConnectionContext.getFd();
    }

    public String getParquetExportTableName() {
        return parquetExportTableName;
    }

    public CreateTableOperation getParquetTempTableCreate() {
        return createParquetOp;
    }

    public HttpConnectionContext getHttpConnectionContext() {
        return httpConnectionContext;
    }

    HTTPSerialParquetExporter getOrCreateSerialParquetExporter(CairoEngine engine) {
        if (serialParquetExporter == null) {
            serialParquetExporter = new HTTPSerialParquetExporter(engine);
        }
        return serialParquetExporter;
    }

    public boolean isQueryCacheable() {
        return queryCacheable;
    }

    public void setParquetExportTableName(String tableName) {
        this.parquetExportTableName = tableName;
    }

    public void setParquetTempTableCreate(CreateTableOperation createOp) {
        this.createParquetOp = createOp;
    }

    void initWriteCallback(ExportQueryProcessor processor) {
        this.writeCallback.of(processor, this);
    }

    CopyExportRequestTask.StreamWriteParquetCallBack getWriteCallback() {
        return writeCallback;
    }

    private static final class ParquetWriteCallback implements CopyExportRequestTask.StreamWriteParquetCallBack {
        private ExportQueryProcessor processor;
        private ExportQueryProcessorState state;

        void of(ExportQueryProcessor processor, ExportQueryProcessorState state) {
            this.processor = processor;
            this.state = state;
        }

        @Override
        public void onWrite(long dataPtr, long dataLen) throws Exception {
            if (processor != null && state != null) {
                processor.writeParquetData(state, dataPtr, dataLen);
            }
        }
    }

    private void releaseExportEntry() {
        if (copyID != -1) {
            CopyExportContext.ExportTaskEntry entry = copyExportContext.getEntry(copyID);
            if (entry != null) {
                copyExportContext.releaseEntry(entry);
            }
            copyID = -1;
        }
    }

    void resumeError(HttpChunkedResponse response) throws PeerIsSlowToReadException, PeerDisconnectedException {
        response.bookmark();
        response.putAscii('{')
                .putAsciiQuoted("query").putAscii(':').putQuote().escapeJsonStr(sqlText).putQuote().putAscii(',')
                .putAsciiQuoted("error").putAscii(':').putQuote().escapeJsonStr(errorMessage).putQuote().putAscii(',')
                .putAsciiQuoted("position").putAscii(':').put(errorPosition)
                .putAscii('}');
        queryState = ExportQueryProcessor.QUERY_DONE;
        response.sendChunk(true);

    }

    void setQueryCacheable(boolean queryCacheable) {
        this.queryCacheable = queryCacheable;
    }

    void storeError(int errorPosition, CharSequence errorMessage) {
        this.queryState = ExportQueryProcessor.QUERY_SEND_ERROR;
        this.errorPosition = errorPosition;
        this.errorMessage.clear();
        this.errorMessage.put(errorMessage);
    }
}
