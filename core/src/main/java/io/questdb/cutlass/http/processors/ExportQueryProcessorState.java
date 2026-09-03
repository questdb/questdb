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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
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
import io.questdb.cutlass.parquet.HybridColumnMaterializer;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.model.ExportModel;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class ExportQueryProcessorState implements Mutable, Closeable {

    private static final long SQL_EXECUTION_OWNER_UNINITIALIZED = Long.MIN_VALUE;
    final StringSink fileName = new StringSink();
    final HybridColumnMaterializer materializer = new HybridColumnMaterializer();
    final DirectLongList materializerColumnData = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_EXPORTER);
    final StringSink sqlText = new StringSink();
    private final CopyExportContext copyExportContext;
    private final StringSink errorMessage = new StringSink();
    private final ExportModel exportModel = new ExportModel();
    private final HttpConnectionContext httpConnectionContext;
    private final ParquetWriteCallback writeCallback = new ParquetWriteCallback();
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
    ParquetExportMode parquetExportMode;
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
    long timeout;
    private CreateTableOperation createParquetOp;
    private int errorPosition;
    private boolean isSqlExecutionOwnerMounted;
    private String parquetExportTableName;
    private boolean queryCacheable = false;
    private HTTPSerialParquetExporter serialParquetExporter;
    private SqlExecutionContext sqlExecutionOwnerContext;
    private long sqlExecutionOwnerId = SQL_EXECUTION_OWNER_UNINITIALIZED;

    public ExportQueryProcessorState(HttpConnectionContext httpConnectionContext, CopyExportContext copyContext) {
        this.httpConnectionContext = httpConnectionContext;
        this.copyExportContext = copyContext;
        clear();
    }

    public void beginSqlExecutionOwner(
            CharSequence query,
            SqlExecutionContext executionContext,
            short compiledQueryType
    ) {
        if (sqlExecutionOwnerId != SQL_EXECUTION_OWNER_UNINITIALIZED) {
            throw new IllegalStateException("HTTP export SQL execution owner is already initialized");
        }
        final long ownerId = executionContext.getCairoEngine().beginSqlExecution(
                query,
                executionContext,
                compiledQueryType
        );
        sqlExecutionOwnerContext = executionContext;
        sqlExecutionOwnerId = ownerId;
        isSqlExecutionOwnerMounted = ownerId > -1;
    }

    @Override
    public void clear() {
        delimiter = ',';
        fileName.clear();
        rnd = null;
        record = null;

        // The HTTP exporter owns any materialized temporary table. Let it close the
        // task's Rust writer and drop that table while task identity is still intact.
        // All resources still disappear before the cursor unregisters its tracker.
        Throwable cleanupFailure = null;
        if (serialParquetExporter != null) {
            try {
                serialParquetExporter.clearExportResources();
            } catch (Throwable th) {
                cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
            }
        }
        cleanupFailure = Misc.clearBestEffort(cleanupFailure, task);
        final RecordCursor cursor = this.cursor;
        this.cursor = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, cursor);
        final PageFrameCursor pageFrameCursor = this.pageFrameCursor;
        this.pageFrameCursor = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, pageFrameCursor);
        cleanupFailure = Misc.clearBestEffort(cleanupFailure, materializer);
        cleanupFailure = Misc.clearBestEffort(cleanupFailure, materializerColumnData);
        firstParquetWriteCall = true;

        final RecordCursorFactory recordCursorFactory = this.recordCursorFactory;
        this.recordCursorFactory = null;
        if (recordCursorFactory != null) {
            if (queryCacheable) {
                try {
                    httpConnectionContext.getSelectCache().put(sqlText, recordCursorFactory);
                } catch (Throwable th) {
                    cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
                }
            } else {
                cleanupFailure = Misc.freeBestEffort(cleanupFailure, recordCursorFactory);
            }
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
        try {
            releaseExportEntry();
        } catch (Throwable th) {
            cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
        }
        final CreateTableOperation createParquetOp = this.createParquetOp;
        this.createParquetOp = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, createParquetOp);
        parquetExportTableName = null;
        parquetExportMode = null;
        parquetFileOffset = 0;
        exportModel.clear();
        errorMessage.clear();
        errorPosition = 0;
        serialExporterInit = false;
        writeCallback.of(null, null);
        try {
            endSqlExecutionOwner();
        } catch (Throwable th) {
            cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
        }
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    @Override
    public void close() {
        // See clear(): the exporter must release a materialized temporary table
        // while the task still carries its export identity.
        Throwable cleanupFailure = null;
        final HTTPSerialParquetExporter serialParquetExporter = this.serialParquetExporter;
        this.serialParquetExporter = null;
        if (serialParquetExporter != null) {
            try {
                serialParquetExporter.clearExportResources();
            } catch (Throwable th) {
                cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
            }
        }
        final CopyExportRequestTask task = this.task;
        this.task = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, task);
        final RecordCursor cursor = this.cursor;
        this.cursor = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, cursor);
        final PageFrameCursor pageFrameCursor = this.pageFrameCursor;
        this.pageFrameCursor = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, pageFrameCursor);
        final RecordCursorFactory recordCursorFactory = this.recordCursorFactory;
        this.recordCursorFactory = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, recordCursorFactory);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, materializer);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, materializerColumnData);
        try {
            releaseExportEntry();
        } catch (Throwable th) {
            cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
        }
        final CreateTableOperation createParquetOp = this.createParquetOp;
        this.createParquetOp = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, createParquetOp);
        writeCallback.of(null, null);
        try {
            endSqlExecutionOwner();
        } catch (Throwable th) {
            cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
        }
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    public ExportModel getExportModel() {
        return exportModel;
    }

    public long getFd() {
        return httpConnectionContext.getFd();
    }

    public HttpConnectionContext getHttpConnectionContext() {
        return httpConnectionContext;
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

    public void parkSqlExecutionOwner() {
        try {
            suspendCursorTimer();
        } finally {
            unmountSqlExecutionOwner();
        }
    }

    public void publishSqlExecutionOwner(boolean containsSecret) {
        if (sqlExecutionOwnerId > -1) {
            sqlExecutionOwnerContext.getCairoEngine().publishSqlExecutionQuery(
                    sqlExecutionOwnerId,
                    sqlText,
                    containsSecret,
                    sqlExecutionOwnerContext
            );
        }
    }

    public void resumeCursorTimer() {
        if (cursor != null) {
            cursor.resumeTimer();
        } else if (pageFrameCursor != null) {
            pageFrameCursor.resumeTimer();
        } else if (serialParquetExporter != null) {
            serialParquetExporter.resumeCursorTimer();
        }
    }

    public void resumeSqlExecutionOwner() {
        if (hasActiveSqlExecutionWork()) {
            resumeCursorTimer();
            mountSqlExecutionOwner();
        }
    }

    public void setParquetExportTableName(String tableName) {
        this.parquetExportTableName = tableName;
    }

    public void setParquetTempTableCreate(CreateTableOperation createOp) {
        this.createParquetOp = createOp;
    }

    @TestOnly
    public void setTaskAndCursorForTest(CopyExportRequestTask task, RecordCursor cursor) {
        this.cursor = cursor;
        this.task = Misc.free(this.task);
        this.task = task;
    }

    public void suspendCursorTimer() {
        if (cursor != null) {
            cursor.suspendTimer();
        } else if (pageFrameCursor != null) {
            pageFrameCursor.suspendTimer();
        } else if (serialParquetExporter != null) {
            serialParquetExporter.suspendCursorTimer();
        }
    }

    private void endSqlExecutionOwner() {
        final long ownerId = sqlExecutionOwnerId;
        try {
            if (ownerId != SQL_EXECUTION_OWNER_UNINITIALIZED) {
                sqlExecutionOwnerContext.getCairoEngine().endSqlExecution(ownerId, sqlExecutionOwnerContext);
            }
        } finally {
            isSqlExecutionOwnerMounted = false;
            sqlExecutionOwnerContext = null;
            sqlExecutionOwnerId = SQL_EXECUTION_OWNER_UNINITIALIZED;
        }
    }

    private boolean hasActiveSqlExecutionWork() {
        if (!exportModel.isParquetFormat()) {
            return cursor != null || pageFrameCursor != null;
        }
        // TEMP_TABLE and the exporter-owned cursor modes may not expose a cursor on this state.
        // They still execute query work through EXPORT_DATA. FILE_SEND_COMPLETE, DONE, and ERROR
        // are response-only states and must not reacquire admission merely to flush bytes.
        return switch (queryState) {
            case ExportQueryProcessor.QUERY_SETUP_FIRST_RECORD,
                 ExportQueryProcessor.QUERY_PARQUET_EXPORT_INIT,
                 ExportQueryProcessor.QUERY_PARQUET_SEND_HEADER,
                 ExportQueryProcessor.QUERY_PARQUET_SEND_MAGIC,
                 ExportQueryProcessor.QUERY_PARQUET_EXPORT_DATA -> true;
            default -> false;
        };
    }

    private void mountSqlExecutionOwner() {
        if (sqlExecutionOwnerId > -1 && !isSqlExecutionOwnerMounted) {
            sqlExecutionOwnerContext.getCairoEngine().mountSqlExecution(
                    sqlExecutionOwnerId,
                    sqlExecutionOwnerContext
            );
            isSqlExecutionOwnerMounted = true;
        }
    }

    private void releaseExportEntry() {
        final long copyID = this.copyID;
        this.copyID = -1;
        if (copyID != -1) {
            CopyExportContext.ExportTaskEntry entry = copyExportContext.getEntry(copyID);
            if (entry != null) {
                copyExportContext.releaseEntry(entry);
            }
        }
    }

    private void unmountSqlExecutionOwner() {
        if (sqlExecutionOwnerId > -1 && isSqlExecutionOwnerMounted) {
            sqlExecutionOwnerContext.getCairoEngine().unmountSqlExecution(
                    sqlExecutionOwnerId,
                    sqlExecutionOwnerContext
            );
            isSqlExecutionOwnerMounted = false;
        }
    }

    HTTPSerialParquetExporter getOrCreateSerialParquetExporter(
            CairoEngine engine,
            SqlExecutionContextImpl sqlExecutionContext
    ) {
        if (serialParquetExporter == null) {
            serialParquetExporter = new HTTPSerialParquetExporter(engine, sqlExecutionContext);
        }
        return serialParquetExporter;
    }

    CopyExportRequestTask.StreamWriteParquetCallBack getWriteCallback() {
        return writeCallback;
    }

    void initWriteCallback(ExportQueryProcessor processor) {
        this.writeCallback.of(processor, this);
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

    private static final class ParquetWriteCallback implements CopyExportRequestTask.StreamWriteParquetCallBack {
        private ExportQueryProcessor processor;
        private ExportQueryProcessorState state;

        @Override
        public void onWrite(long dataPtr, long dataLen) throws Exception {
            if (processor != null && state != null) {
                processor.writeParquetData(state, dataPtr, dataLen);
            }
        }

        void of(ExportQueryProcessor processor, ExportQueryProcessorState state) {
            this.processor = processor;
            this.state = state;
        }
    }
}
