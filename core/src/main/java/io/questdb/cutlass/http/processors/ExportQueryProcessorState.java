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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpResponseArrayWriteState;
import io.questdb.cutlass.text.CopyExportResult;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.model.ExportModel;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class ExportQueryProcessorState implements Mutable, Closeable {
    final StringSink fileName = new StringSink();
    final StringSink sqlText = new StringSink();
    private final CopyExportResult copyExportResult;
    private final StringSink errorMessage = new StringSink();
    private final ExportModel exportModel = new ExportModel();
    private final FilesFacade filesFacade;
    private final HttpConnectionContext httpConnectionContext;
    HttpResponseArrayWriteState arrayState = new HttpResponseArrayWriteState();
    int columnIndex;
    boolean columnValueFullySent = true;
    long copyID = -1;
    long count;
    boolean countRows = false;
    RecordCursor cursor;
    char delimiter = ',';
    boolean hasNext;
    RecordMetadata metadata;
    boolean noMeta = false;
    long parquetFileAddress = 0;
    long parquetFileFd = -1;
    long parquetFileOffset = 0;
    long parquetFileSize = 0;
    boolean pausedQuery = false;
    int queryState;
    Record record;
    RecordCursorFactory recordCursorFactory;
    Rnd rnd;
    long skip;
    long stop;
    boolean waitingForCopy;
    private CreateTableOperation createParquetOp;
    private int errorPosition;
    private String parquetExportTableName;
    private boolean queryCacheable = false;

    public ExportQueryProcessorState(HttpConnectionContext httpConnectionContext, FilesFacade filesFacade) {
        this.httpConnectionContext = httpConnectionContext;
        this.copyExportResult = new CopyExportResult();
        this.filesFacade = filesFacade;
        clear();
    }

    @Override
    public void clear() {
        delimiter = ',';
        fileName.clear();
        rnd = null;
        record = null;
        cursor = Misc.free(cursor);
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
        copyID = -1;
        createParquetOp = Misc.free(createParquetOp);
        parquetExportTableName = null;
        waitingForCopy = false;
        parquetFileOffset = 0;
        exportModel.clear();
        cleanupParquetState();
        copyExportResult.clear();
        errorMessage.clear();
        errorPosition = 0;
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
    }

    public ExportModel getExportModel() {
        return exportModel;
    }

    public CopyExportResult getExportResult() {
        return copyExportResult;
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

    public boolean isQueryCacheable() {
        return queryCacheable;
    }

    public void setParquetExportTableName(String tableName) {
        this.parquetExportTableName = tableName;
    }

    public void setParquetTempTableCreate(CreateTableOperation createOp) {
        this.createParquetOp = createOp;
    }

    private void cleanupParquetState() {
        if (parquetFileAddress != 0) {
            filesFacade.munmap(parquetFileAddress, parquetFileSize, MemoryTag.NATIVE_PARQUET_EXPORTER);
            parquetFileAddress = 0;
            parquetFileSize = 0;
        }
        if (parquetFileFd != -1) {
            filesFacade.close(parquetFileFd);
            parquetFileFd = -1;
        }
        copyExportResult.cleanUpTempPath(filesFacade);
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
