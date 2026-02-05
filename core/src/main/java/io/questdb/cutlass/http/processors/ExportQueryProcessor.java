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

import io.questdb.Metrics;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.http.ActiveConnectionTracker;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.parquet.HTTPSerialParquetExporter;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.model.ExportModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Interval;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.TelemetryEvent.QUERY_RESULT_EXPORT_CSV;
import static io.questdb.TelemetryEvent.QUERY_RESULT_EXPORT_PARQUET;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.cairo.sql.RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.griffin.model.ExportModel.COPY_FORMAT_PARQUET;

public class ExportQueryProcessor implements HttpRequestProcessor, HttpRequestHandler, Closeable {
    static final int QUERY_DONE = 1;
    static final int QUERY_METADATA = 2;
    static final int QUERY_PARQUET_EXPORT_DATA = 14;
    static final int QUERY_PARQUET_EXPORT_INIT = 10;
    static final int QUERY_PARQUET_FILE_SEND_COMPLETE = 15;
    static final int QUERY_PARQUET_SEND_HEADER = 12;
    static final int QUERY_PARQUET_SEND_MAGIC = 13;
    static final int QUERY_RECORD = 5;
    static final int QUERY_RECORD_START = 4;
    static final int QUERY_RECORD_SUFFIX = 6;
    static final int QUERY_SEND_ERROR = 3;
    static final int QUERY_SETUP_FIRST_RECORD = 0;
    static final int QUERY_SUFFIX = 7;
    private static final String FILE_EXTENSION_CSV = ".csv";
    private static final String FILE_EXTENSION_PARQUET = ".parquet";
    private static final Log LOG = LogFactory.getLog(ExportQueryProcessor.class);
    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    private static final LocalValue<ExportQueryProcessorState> LV = new LocalValue<>();
    private final MillisecondClock clock;
    private final JsonQueryProcessorConfiguration configuration;
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final CairoEngine engine;
    private final StringSink errSink = new StringSink();
    private final int maxSqlRecompileAttempts;
    private final Metrics metrics;
    private final byte requiredAuthType;
    private final int sharedWorkerCount;

    public ExportQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int sharedQueryWorkerCount
    ) {
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
        this.sharedWorkerCount = sharedQueryWorkerCount;
        this.metrics = engine.getMetrics();
        this.engine = engine;
        maxSqlRecompileAttempts = engine.getConfiguration().getMaxSqlRecompileAttempts();
        requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
    }

    public void execute(
            HttpConnectionContext context,
            ExportQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            boolean isExpRequest = isExpUrl(context.getRequestHeader().getUrl());

            NetworkSqlExecutionCircuitBreaker circuitBreaker = context.getOrCreateCircuitBreaker(engine);
            SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
            circuitBreaker.setTimeout(state.timeout);
            circuitBreaker.resetTimer();
            state.recordCursorFactory = context.getSelectCache().poll(state.sqlText);
            sqlExecutionContext.with(
                    context.getSecurityContext(),
                    null,
                    null,
                    context.getFd(),
                    circuitBreaker.of(context.getFd())
            );
            sqlExecutionContext.initNow();
            if (state.recordCursorFactory == null) {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    CompiledQuery cc = compiler.compile(state.sqlText, sqlExecutionContext);
                    if (cc.getType() == CompiledQuery.SELECT || cc.getType() == CompiledQuery.EXPLAIN) {
                        state.recordCursorFactory = cc.getRecordCursorFactory();
                    } else if (isExpRequest) {
                        // Close CompiledQuery to prevent memory leak for INSERT/UPDATE/ALTER unsupported operations
                        cc.closeAllButSelect();
                        throw SqlException.$(0, "/exp endpoint only accepts SELECT");
                    }
                    state.setQueryCacheable(cc.isCacheable());
                    sqlExecutionContext.storeTelemetry(state.getExportModel().isParquetFormat() ?
                            QUERY_RESULT_EXPORT_PARQUET : QUERY_RESULT_EXPORT_CSV, TelemetryOrigin.HTTP);
                }
            } else {
                state.setQueryCacheable(true);
                sqlExecutionContext.setCacheHit(true);
                sqlExecutionContext.storeTelemetry(state.getExportModel().isParquetFormat() ?
                        QUERY_RESULT_EXPORT_PARQUET : QUERY_RESULT_EXPORT_CSV, TelemetryOrigin.HTTP);
            }

            if (state.recordCursorFactory != null) {
                try {
                    boolean runQuery = true;
                    boolean canStreamExportParquet = state.getExportModel().isParquetFormat() && CopyExportContext.canStreamExportParquet(state.recordCursorFactory);
                    final int order = state.recordCursorFactory.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
                    state.descending = order == ORDER_DESC;
                    for (int retries = 0; runQuery; retries++) {
                        try {
                            if (canStreamExportParquet) {
                                state.pageFrameCursor = state.recordCursorFactory.getPageFrameCursor(sqlExecutionContext, order);
                            } else {
                                state.cursor = state.recordCursorFactory.getCursor(sqlExecutionContext);
                            }
                            runQuery = false;
                        } catch (TableReferenceOutOfDateException e) {
                            if (retries == maxSqlRecompileAttempts) {
                                throw SqlException.$(0, e.getFlyweightMessage());
                            }
                            info(state).$safe(e.getFlyweightMessage()).$();
                            state.recordCursorFactory = Misc.free(state.recordCursorFactory);
                            CompiledQuery cc;
                            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                                cc = compiler.compile(state.sqlText, sqlExecutionContext);
                                if (cc.getType() != CompiledQuery.SELECT && isExpRequest) {
                                    // Close CompiledQuery to prevent memory leak for INSERT/UPDATE/ALTER unsupported operations
                                    cc.closeAllButSelect();
                                    throw SqlException.$(0, "/exp endpoint only accepts SELECT");
                                }
                                state.recordCursorFactory = cc.getRecordCursorFactory();
                            }
                        }
                    }
                    state.metadata = state.recordCursorFactory.getMetadata();
                    doResumeSend(context);
                } catch (CairoException e) {
                    if (state.isQueryCacheable()) {
                        state.setQueryCacheable(e.isCacheable());
                    }
                    internalError(context.getChunkedResponse(), context.getLastRequestBytesSent(), e, state);
                } catch (CairoError e) {
                    internalError(context.getChunkedResponse(), context.getLastRequestBytesSent(), e, state);
                }
            } else {
                headerNoContentDisposition(context.getChunkedResponse());
                sendConfirmation(context.getChunkedResponse());
                readyForNextRequest(context);
            }
        } catch (SqlException | ImplicitCastException e) {
            syntaxError(context.getChunkedResponse(), state, e);
            readyForNextRequest(context);
        } catch (CairoException | CairoError e) {
            internalError(context.getChunkedResponse(), context.getLastRequestBytesSent(), e, state);
            readyForNextRequest(context);
        }
    }

    @Override
    public String getName() {
        return ActiveConnectionTracker.PROCESSOR_EXPORT_HTTP;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return this;
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        ExportQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new ExportQueryProcessorState(context, engine.getCopyExportContext()));
        }

        HttpChunkedResponse response = context.getChunkedResponse();
        if (parseUrl(response, context.getRequestHeader(), state, context)) {
            execute(context, state);
        } else {
            readyForNextRequest(context);
        }
    }

    void writeParquetData(ExportQueryProcessorState state, long dataPtr, long dataLen)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (dataLen <= 0) {
            return;
        }
        HttpConnectionContext context = state.getHttpConnectionContext();
        HttpChunkedResponse response = context.getChunkedResponse();
        if (state.firstParquetWriteCall) {
            state.firstParquetWriteCall = false;
            if (!state.getExportModel().isNoDelay()) {
                header(response, state, 200);
            }
        }

        while (state.parquetFileOffset < dataLen) {
            long remainingSize = dataLen - state.parquetFileOffset;
            int sendLSize = (int) Math.min(Integer.MAX_VALUE, remainingSize);

            state.parquetFileOffset += response.writeBytes(dataPtr + state.parquetFileOffset, sendLSize);
            response.bookmark();
            response.sendChunk(false);
        }

        state.parquetFileOffset = 0;
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        ExportQueryProcessorState state = LV.get(context);
        if (state != null) {
            state.pausedQuery = pausedQuery;
            SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
            state.rnd = sqlExecutionContext.getRandom();
        }
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            doResumeSend(context);
        } catch (CairoError | CairoException e) {
            // this is something we didn't expect
            // log the exception and disconnect
            ExportQueryProcessorState state = LV.get(context);
            if (state != null) {
                logInternalError(e, state);
            }
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private static boolean isExpUrl(Utf8Sequence tok) {
        if (tok.size() != 4) {
            return false;
        }

        int i = 0;
        return (tok.byteAt(i++) | 32) == '/'
                && (tok.byteAt(i++) | 32) == 'e'
                && (tok.byteAt(i++) | 32) == 'x'
                && (tok.byteAt(i) | 32) == 'p';
    }

    private static void putDecimal128StringValue(HttpChunkedResponse response, Decimal128 decimal128, int type) {
        if (decimal128.isNull()) {
            response.putAscii("null");
        } else {
            response.putAscii('\"');
            Decimals.appendNonNull(decimal128, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
            response.putAscii('\"');
        }
    }

    private static void putDecimal16StringValue(HttpChunkedResponse response, short value, int type) {
        if (value == Decimals.DECIMAL16_NULL) {
            response.putAscii("null");
        } else {
            putDecimalLongStringValue(response, value, type);
        }
    }

    private static void putDecimal256StringValue(HttpChunkedResponse response, Decimal256 decimal256, int type) {
        if (decimal256.isNull()) {
            response.putAscii("null");
        } else {
            response.putAscii('\"');
            Decimals.appendNonNull(decimal256, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
            response.putAscii('\"');
        }
    }

    private static void putDecimal32StringValue(HttpChunkedResponse response, int value, int type) {
        if (value == Decimals.DECIMAL32_NULL) {
            response.putAscii("null");
        } else {
            putDecimalLongStringValue(response, value, type);
        }
    }

    private static void putDecimal64StringValue(HttpChunkedResponse response, long value, int type) {
        if (value == Decimals.DECIMAL64_NULL) {
            response.putAscii("null");
        } else {
            putDecimalLongStringValue(response, value, type);
        }
    }

    private static void putDecimal8StringValue(HttpChunkedResponse response, byte value, int type) {
        if (value == Decimals.DECIMAL8_NULL) {
            response.putAscii("null");
        } else {
            putDecimalLongStringValue(response, value, type);
        }
    }

    private static void putDecimalLongStringValue(HttpChunkedResponse response, long value, int type) {
        response.putAscii('\"');
        Decimals.append(value, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
        response.putAscii('\"');
    }

    private static void putGeoHashStringValue(HttpChunkedResponse response, long value, int type) {
        if (value == GeoHashes.NULL) {
            response.putAscii("null");
        } else {
            int bitFlags = GeoHashes.getBitFlags(type);
            response.putAscii('\"');
            if (bitFlags < 0) {
                GeoHashes.appendCharsUnsafe(value, -bitFlags, response);
            } else {
                GeoHashes.appendBinaryStringUnsafe(value, bitFlags, response);
            }
            response.putAscii('\"');
        }
    }

    private static void putIPv4Value(HttpChunkedResponse response, Record rec, int col) {
        final int ip = rec.getIPv4(col);
        if (ip != Numbers.IPv4_NULL) {
            Numbers.intToIPv4Sink(response, ip);
        }
    }

    private static void putInterval(HttpChunkedResponse response, Record rec, int col, int intervalType) {
        final Interval interval = rec.getInterval(col);
        if (!Interval.NULL.equals(interval)) {
            interval.toSink(response.putQuote(), intervalType);
            response.putQuote();
        }
    }

    private static void putStringOrNull(HttpChunkedResponse response, CharSequence cs) {
        if (cs != null) {
            response.putQuote().escapeCsvStr(cs).putQuote();
        }
    }

    private static void putUuidOrNull(HttpChunkedResponse response, long lo, long hi) {
        if (Uuid.isNull(lo, hi)) {
            return;
        }
        Numbers.appendUuid(lo, hi, response);
    }

    private static void putVarcharOrNull(HttpChunkedResponse response, Utf8Sequence us) {
        if (us != null) {
            response.putQuote().escapeCsvStr(us).putQuote();
        }
    }

    private static void readyForNextRequest(HttpConnectionContext context) {
        LOG.debug().$("all sent [fd=").$(context.getFd())
                .$(", lastRequestBytesSent=").$(context.getLastRequestBytesSent())
                .$(", nCompletedRequests=").$(context.getNCompletedRequests() + 1)
                .$(", totalBytesSent=").$(context.getTotalBytesSent()).I$();
    }

    private void compileParquetExport(HttpConnectionContext context, ExportQueryProcessorState state) throws SqlException {
        assert state.copyID == -1;
        CopyExportContext.ExportTaskEntry entry = null;
        try {
            SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
            var securityContext = context.getSecurityContext();
            var sqlExecutionCircuitBreaker = sqlExecutionContext.getCircuitBreaker();
            var copyExportContext = engine.getCopyExportContext();
            entry = copyExportContext.assignExportEntry(
                    securityContext,
                    state.sqlText,
                    state.fileName,
                    sqlExecutionCircuitBreaker,
                    CopyExportContext.CopyTrigger.HTTP
            );

            state.copyID = entry.getId();
            state.firstParquetWriteCall = true;
            if (state.pageFrameCursor == null) { // cannot stream parquet export
                String tableName = getTableName(state.copyID);
                state.setParquetExportTableName(tableName);
                var createOp = copyExportContext.validateAndCreateParquetExportTableOp(
                        sqlExecutionContext,
                        state.sqlText,
                        state.getExportModel().getPartitionBy(),
                        tableName,
                        "",
                        0
                );

                state.setParquetTempTableCreate(createOp);
            }
        } catch (Throwable th) {
            if (entry != null) {
                engine.getCopyExportContext().releaseEntry(entry);
                state.copyID = -1;
            }
            throw th;
        }
    }

    private boolean configureParquetOptions(
            HttpRequestHeader request,
            HttpChunkedResponse response,
            ExportQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        ExportModel exportModel = state.getExportModel();
        exportModel.setParquetDefaults(engine.getConfiguration());
        exportModel.setPartitionBy(PartitionBy.NONE);
        // this is ok to assign, the exportModel belongs to state and so is the sqlText sink
        exportModel.setSelectText(state.sqlText, 0);

        // Handle partition_by option
        DirectUtf8Sequence partitionBy = request.getUrlParam(EXPORT_PARQUET_OPTION_PARTITION_BY);
        if (partitionBy != null && partitionBy.size() > 0) {
            errSink.clear();
            errSink.put("partitionBy is temporarily not supported:").put(partitionBy);
            sendException(response, 0, errSink, state);
            return false;
        }

        // Handle compression_codec option
        DirectUtf8Sequence compressionCodec = request.getUrlParam(EXPORT_PARQUET_OPTION_COMPRESSION_CODEC);
        if (compressionCodec != null && compressionCodec.size() > 0) {
            int codec = ParquetCompression.getCompressionCodec(compressionCodec.asAsciiCharSequence());
            if (codec < 0) {
                SqlException e = SqlException.$(0, "invalid compression codec[").put(compressionCodec).put("], expected one of: ");
                ParquetCompression.addCodecNamesToException(e);
                sendException(response, 0, e.getFlyweightMessage(), state);
                return false;
            }
            exportModel.setCompressionCodec(codec);
        }

        // Handle compression_level option
        DirectUtf8Sequence compressionLevel = request.getUrlParam(EXPORT_PARQUET_OPTION_COMPRESSION_LEVEL);
        if (compressionLevel != null && compressionLevel.size() > 0) {
            try {
                int level = Numbers.parseInt(compressionLevel);
                exportModel.setCompressionLevel(level, 0);
            } catch (NumericException e) {
                errSink.clear();
                errSink.put("invalid compression level:").put(compressionLevel);
                sendException(response, 0, errSink, state);
                return false;
            }
        }

        try {
            exportModel.validCompressOptions();
        } catch (SqlException e) {
            sendException(response, 0, e.getFlyweightMessage(), state);
            return false;
        }

        // Handle row_group_size option
        DirectUtf8Sequence rowGroupSize = request.getUrlParam(EXPORT_PARQUET_OPTION_ROW_GROUP_SIZE);
        if (rowGroupSize != null && rowGroupSize.size() > 0) {
            try {
                int size = Numbers.parseInt(rowGroupSize);
                exportModel.setRowGroupSize(size);
            } catch (NumericException e) {
                errSink.clear();
                errSink.put("invalid row group size:").put(rowGroupSize);
                sendException(response, 0, errSink, state);
                return false;
            }
        }

        // Handle rmode
        DirectUtf8Sequence rmode = request.getUrlParam(EXPORT_PARQUET_OPTION_RESPONSE_MODE);
        if (rmode != null && Utf8s.equalsIgnoreCaseAscii("nodelay", rmode)) {
            exportModel.setNoDelayTo(true);
        }

        // Handle data_page_size option
        DirectUtf8Sequence dataPageSize = request.getUrlParam(EXPORT_PARQUET_OPTION_DATA_PAGE_SIZE);
        if (dataPageSize != null && dataPageSize.size() > 0) {
            try {
                int size = Numbers.parseInt(dataPageSize);
                exportModel.setDataPageSize(size);
            } catch (NumericException e) {
                errSink.clear();
                errSink.put("invalid data page size:").put(dataPageSize);
                sendException(response, 0, errSink, state);
                return false;
            }
        }

        // Handle statistics_enabled option
        DirectUtf8Sequence statisticsEnabled = request.getUrlParam(EXPORT_PARQUET_OPTION_STATISTICS_ENABLED);
        if (statisticsEnabled != null && statisticsEnabled.size() > 0) {
            boolean enabled = HttpKeywords.isTrue(statisticsEnabled);
            exportModel.setStatisticsEnabled(enabled);
        }

        // Handle parquet_version option
        DirectUtf8Sequence parquetVersion = request.getUrlParam(EXPORT_PARQUET_OPTION_PARQUET_VERSION);
        if (parquetVersion != null && parquetVersion.size() > 0) {
            try {
                int version = Numbers.parseInt(parquetVersion);
                if (version != ExportModel.PARQUET_VERSION_V1 && version != ExportModel.PARQUET_VERSION_V2) {
                    errSink.clear();
                    errSink.put("invalid parquet version: ").put(parquetVersion)
                            .put(", supported versions: ")
                            .put(ExportModel.PARQUET_VERSION_V1).put(", ")
                            .put(ExportModel.PARQUET_VERSION_V2);
                    sendException(response, 0, errSink, state);
                    return false;
                }
                exportModel.setParquetVersion(version);
            } catch (NumericException e) {
                errSink.clear();
                errSink.put("invalid parquet version:").put(parquetVersion);
                sendException(response, 0, errSink, state);
                return false;
            }
        }

        // Handle raw_array_encoding option
        DirectUtf8Sequence rawArrayEncoding = request.getUrlParam(EXPORT_PARQUET_OPTION_RAW_ARRAY_ENCODING);
        if (rawArrayEncoding != null && rawArrayEncoding.size() > 0) {
            boolean enabled = HttpKeywords.isTrue(rawArrayEncoding);
            exportModel.setRawArrayEncoding(enabled);
        }

        return true;
    }

    private void copyQueryToParquetFile(ExportQueryProcessorState state) throws Exception {
        if (state.copyID != -1) {
            CopyExportContext.ExportTaskEntry entry = null;
            boolean cleanup = true;
            try {
                SqlExecutionContextImpl sqlExecutionContext = state.getHttpConnectionContext().getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
                var copyExportContext = engine.getCopyExportContext();
                entry = copyExportContext.getEntry(state.copyID);
                HTTPSerialParquetExporter exporter = state.getOrCreateSerialParquetExporter(engine);
                if (state.serialExporterInit) {
                    exporter.of(state.task);
                    exporter.process();
                    return;
                }
                state.initWriteCallback(this);
                int nowTimestampType = sqlExecutionContext.getNowTimestampType();
                long now = sqlExecutionContext.getNow(nowTimestampType);
                state.task.of(
                        entry,
                        state.getParquetTempTableCreate(),
                        state.getParquetExportTableName(),
                        state.fileName,
                        state.getExportModel().getCompressionCodec(),
                        state.getExportModel().getCompressionLevel(),
                        state.getExportModel().getRowGroupSize(),
                        state.getExportModel().getDataPageSize(),
                        state.getExportModel().isStatisticsEnabled(),
                        state.getExportModel().getParquetVersion(),
                        state.getExportModel().isRawArrayEncoding(),
                        nowTimestampType,
                        now,
                        state.descending,
                        state.pageFrameCursor,
                        state.metadata,
                        state.getWriteCallback()
                );

                exporter.of(state.task);
                state.task.setUpStreamPartitionParquetExporter();
                state.serialExporterInit = true;
                exporter.process();
            } catch (PeerIsSlowToReadException e) {
                cleanup = false;
                throw e;
            } finally {
                if (cleanup && entry != null) {
                    engine.getCopyExportContext().releaseEntry(entry);
                    state.copyID = -1;
                }
            }
        } else {
            throw CairoException.nonCritical().put("invalid export state, cannot find export id");
        }
    }

    private LogRecord critical(ExportQueryProcessorState state) {
        return LOG.critical().$('[').$(state.getFd()).$("] ");
    }

    private void doParquetExport(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        ExportQueryProcessorState state = LV.get(context);
        final HttpChunkedResponse response = context.getChunkedResponse();

        OUT:
        while (true) {
            try {
                switch (state.queryState) {
                    case QUERY_SETUP_FIRST_RECORD:
                        state.queryState = QUERY_PARQUET_EXPORT_INIT;
                    case QUERY_PARQUET_EXPORT_INIT:
                        try {
                            compileParquetExport(context, state);
                        } catch (SqlException | CairoException e) {
                            sendException(response, e.getPosition(), e.getFlyweightMessage(), state);
                            break OUT;
                        } catch (Throwable e) {
                            sendException(response, 0, e.getMessage(), state);
                            break OUT;
                        }
                        state.queryState = QUERY_PARQUET_SEND_HEADER;
                        // fall through

                    case QUERY_PARQUET_SEND_HEADER:
                        if (state.getExportModel().isNoDelay()) {
                            try {
                                state.queryState = QUERY_PARQUET_SEND_MAGIC;
                                header(response, state, 200);
                            } catch (CairoException e) {
                                sendException(response, 0, e.getFlyweightMessage(), state);
                                break OUT;
                            }
                        }
                        state.queryState = QUERY_PARQUET_SEND_MAGIC;
                        // fall through
                    case QUERY_PARQUET_SEND_MAGIC:
                        if (state.getExportModel().isNoDelay()) {
                            try {
                                // We send first 3 bytes of parquet file, that is always "PAR" in ascii
                                // as a chunk to trigger download to open file save dialog and start
                                // background download in browsers.
                                response.put("PAR");
                                state.parquetFileOffset = 3;
                                response.bookmark();
                                state.queryState = QUERY_PARQUET_EXPORT_DATA;
                                response.sendChunk(false);
                            } catch (CairoException e) {
                                sendException(response, 0, e.getFlyweightMessage(), state);
                                break OUT;
                            }
                        }
                        state.queryState = QUERY_PARQUET_EXPORT_DATA;
                        // fall through

                    case QUERY_PARQUET_EXPORT_DATA: // export to temp parquet file or memory
                        try {
                            copyQueryToParquetFile(state);
                        } catch (SqlException | CairoException e) {
                            sendException(response, e.getPosition(), e.getFlyweightMessage(), state);
                            break OUT;
                        } catch (PeerIsSlowToReadException | PeerDisconnectedException e) {
                            throw e;
                        } catch (Throwable e) {
                            sendException(response, 0, e.getMessage(), state);
                            break OUT;
                        }
                        state.queryState = QUERY_PARQUET_FILE_SEND_COMPLETE;
                        // fall through

                    case QUERY_PARQUET_FILE_SEND_COMPLETE:
                        sendDone(response, state);
                        break OUT;
                    case QUERY_SEND_ERROR:
                        state.resumeError(response);
                        break OUT;
                    case QUERY_DONE:
                        response.done();
                        break OUT;
                    default:
                        break OUT;
                }
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                if (response.resetToBookmark()) {
                    response.sendChunk(false);
                } else {
                    info(state).$("Response buffer is too small, state=").$(state.queryState).$();
                    throw PeerDisconnectedException.INSTANCE;
                }
            }
        }

        // Clean up parquet state when done
        readyForNextRequest(context);
    }

    private void doResumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        ExportQueryProcessorState state = LV.get(context);
        if (state == null) {
            return;
        }

        NetworkSqlExecutionCircuitBreaker circuitBreaker = context.getOrCreateCircuitBreaker(engine);
        SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        sqlExecutionContext.with(context.getSecurityContext(), null, state.rnd, context.getFd(), circuitBreaker.of(context.getFd()));
        LOG.debug().$("resume [fd=").$(context.getFd()).I$();

        if (!state.pausedQuery) {
            context.resumeResponseSend();
        } else {
            state.pausedQuery = false;
        }

        final HttpChunkedResponse response = context.getChunkedResponse();

        if (state.getExportModel().isParquetFormat()) {
            doParquetExport(context);
            return;
        }

        RecordCursorFactory recFac = state.recordCursorFactory;

        OUT:
        while (true) {
            try {
                SWITCH:
                switch (state.queryState) {
                    case QUERY_SEND_ERROR:
                        state.resumeError(response);
                        break;

                    case QUERY_SETUP_FIRST_RECORD:
                        state.hasNext = state.cursor.hasNext();
                        // Advance queryState before calling header(). Why?
                        // header() call may fail, but it durably populates the headers, even if sending
                        // them fails. Header sending resumes prior to this loop, in `resumeResponseSend()` call.
                        // If we then proceed to calling `header()` again, the headers will be repopulated and
                        // sent in full again.
                        state.queryState = QUERY_METADATA;
                        header(response, state, 200);
                        // fall through
                    case QUERY_METADATA:
                        if (!state.noMeta) {
                            state.columnIndex = 0;
                            RecordMetadata metadata = recFac.getMetadata();
                            while (state.columnIndex < metadata.getColumnCount()) {
                                if (state.columnIndex > 0) {
                                    response.putAscii(state.delimiter);
                                }
                                response.putQuote().escapeCsvStr(metadata.getColumnName(state.columnIndex)).putQuote();
                                state.columnIndex++;
                                response.bookmark();
                            }
                            response.putEOL();
                        }
                        state.queryState = QUERY_RECORD_START;
                        response.bookmark();
                        // fall through
                    case QUERY_RECORD_START:
                        if (state.record == null) {
                            // check if cursor has any records
                            Record record = state.cursor.getRecord();
                            while (true) {
                                if (state.hasNext || state.cursor.hasNext()) {
                                    state.hasNext = false;
                                    state.count++;

                                    if (state.countRows && state.count > state.stop) {
                                        continue;
                                    }

                                    if (state.count > state.skip) {
                                        state.record = record;
                                        break;
                                    }
                                } else {
                                    state.queryState = QUERY_SUFFIX;
                                    break SWITCH;
                                }
                            }
                        }

                        if (state.count > state.stop) {
                            state.queryState = QUERY_SUFFIX;
                            break;
                        }

                        state.queryState = QUERY_RECORD;
                        state.columnIndex = 0;
                        // fall through
                    case QUERY_RECORD:
                        while (state.columnIndex < recFac.getMetadata().getColumnCount()) {
                            if (state.columnIndex > 0 && state.columnValueFullySent) {
                                response.putAscii(state.delimiter);
                            }
                            putValue(response, state);
                            state.columnIndex++;
                            response.bookmark();
                        }
                        state.queryState = QUERY_RECORD_SUFFIX;
                        // fall through
                    case QUERY_RECORD_SUFFIX:
                        response.putEOL();
                        state.record = null;
                        state.queryState = QUERY_RECORD_START;
                        response.bookmark();
                        break;
                    case QUERY_SUFFIX:
                        // close cursor before returning complete response
                        // this will guarantee that by the time client reads the response fully the table will be released
                        state.cursor = Misc.free(state.cursor);
                        sendDone(response, state);
                        break OUT;
                    default:
                        break OUT;
                }
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                if (response.resetToBookmark()) {
                    response.sendChunk(false);
                } else {
                    // out unit of data, column value or query is larger than response content buffer
                    info(state).$("Response buffer is too small, state=").$(state.queryState).$();
                    throw PeerDisconnectedException.INSTANCE;
                }
            }
        }
        // reached the end naturally?
        readyForNextRequest(context);
    }

    private LogRecord error(ExportQueryProcessorState state) {
        return LOG.error().$('[').$(state.getFd()).$("] ");
    }

    private @NotNull String getTableName(long copyID) {
        var sink = Misc.getThreadLocalSink();
        sink.put(engine.getConfiguration().getParquetExportTableNamePrefix());
        Numbers.appendHex(sink, copyID, true);
        return sink.toString();
    }

    private LogRecord info(ExportQueryProcessorState state) {
        return LOG.info().$('[').$(state.getFd()).$("] ");
    }

    private void internalError(
            HttpChunkedResponse response,
            long bytesSent,
            Throwable e,
            ExportQueryProcessorState state
    ) throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        logInternalError(e, state);
        if (bytesSent > 0) {
            // We already sent a partial response to the client.
            // Give up and close the connection.
            throw ServerDisconnectException.INSTANCE;
        }
        sendException(response, 0, e.getMessage(), state);
    }

    private void logInternalError(Throwable e, ExportQueryProcessorState state) {
        if (e instanceof CairoException ce) {
            if (ce.isInterruption()) {
                info(state).$("query cancelled [reason=`").$safe(ce.getFlyweightMessage())
                        .$("`, q=`").$safe(state.sqlText)
                        .$("`]").$();
            } else if (ce.isCritical()) {
                critical(state).$("error [msg=`").$safe(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$("`, q=`").$safe(state.sqlText)
                        .$("`]").$();
            } else {
                error(state).$("error [msg=`").$safe(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$("`, q=`").$safe(state.sqlText)
                        .$("`]").$();
            }
        } else if (e instanceof HttpException) {
            error(state).$("internal HTTP server error [reason=`").$safe(((HttpException) e).getFlyweightMessage())
                    .$("`, q=`").$safe(state.sqlText)
                    .$("`]").$();
        } else {
            critical(state).$("internal error [ex=").$(e)
                    .$(", q=`").$safe(state.sqlText)
                    .$("`]").$();
            // This is a critical error, so we treat it as an unhandled one.
            metrics.healthMetrics().incrementUnhandledErrors();
        }
    }

    private boolean parseUrl(
            HttpChunkedResponse response,
            HttpRequestHeader request,
            ExportQueryProcessorState state,
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Query text.
        final DirectUtf8Sequence query = request.getUrlParam(URL_PARAM_QUERY);
        if (query == null || query.size() == 0) {
            info(state).$("Empty query request received. Sending empty reply.").$();
            sendException(response, 0, "No query text", state);
            return false;
        }

        // URL params.
        long skip = 0;
        long stop = Long.MAX_VALUE;

        DirectUtf8Sequence limit = request.getUrlParam(URL_PARAM_LIMIT);
        if (limit != null) {
            int sepPos = Utf8s.indexOfAscii(limit, ',');
            try {
                if (sepPos > 0) {
                    skip = Numbers.parseLong(limit, 0, sepPos);
                    if (sepPos + 1 < limit.size()) {
                        stop = Numbers.parseLong(limit, sepPos + 1, limit.size());
                    }
                } else {
                    stop = Numbers.parseLong(limit);
                }
            } catch (NumericException ex) {
                // Skip or stop will have default value.
            }
        }
        if (stop < 0) {
            stop = 0;
        }

        if (skip < 0) {
            skip = 0;
        }

        if ((stop - skip) > configuration.getMaxQueryResponseRowLimit()) {
            stop = skip + configuration.getMaxQueryResponseRowLimit();
        }

        state.sqlText.clear();
        if (!Utf8s.utf8ToUtf16(query.lo(), query.hi(), state.sqlText)) {
            info(state).$("Bad UTF8 encoding").$();
            sendException(response, 0, "Bad UTF8 encoding in query text", state);
            return false;
        }
        DirectUtf8Sequence fileName = request.getUrlParam(URL_PARAM_FILENAME);
        state.fileName.clear();
        if (fileName != null && fileName.size() > 0) {
            state.fileName.put(fileName);
        }

        DirectUtf8Sequence delimiter = request.getUrlParam(URL_PARAM_DELIMITER);
        state.delimiter = ',';

        if (delimiter != null && delimiter.size() == 1) {
            state.delimiter = (char) delimiter.byteAt(0);
        }

        ExportModel exportModel = state.getExportModel();
        exportModel.clear();
        exportModel.setFormat(ExportModel.COPY_FORMAT_CSV);
        DirectUtf8Sequence format = request.getUrlParam(URL_PARAM_FMT);
        if (format != null && format.size() > 0) {
            if (SqlKeywords.isParquetKeyword(format.asAsciiCharSequence())) {
                exportModel.setFormat(COPY_FORMAT_PARQUET);
            } else if (!SqlKeywords.isCsvKeyword(format.asAsciiCharSequence())) {
                errSink.clear();
                errSink.put("unrecognised format [format=").put(format).put("]");
                sendException(response, 0, errSink, state);
                return false;
            }
        }
        if (exportModel.isParquetFormat()) {
            if (!configureParquetOptions(request, response, state)) {
                return false;
            }
        }

        final long defaultTimeout;
        // Timeout policy:
        // - Parquet uses the dedicated export timeout.
        // - CSV uses the circuit breaker default (query timeout). This keeps the
        //   historical behavior where CSV is bounded by general query limits,
        //   at the cost of a format-specific difference. If we later decide to
        //   unify exports under a single timeout, this is the place to change.
        if (exportModel.isParquetFormat()) {
            defaultTimeout = configuration.getExportTimeout();
        } else {
            NetworkSqlExecutionCircuitBreaker circuitBreaker = context.getOrCreateCircuitBreaker(engine);
            defaultTimeout = circuitBreaker.getDefaultMaxTime();
        }
        state.timeout = defaultTimeout;

        DirectUtf8Sequence timeoutSeq = request.getUrlParam(URL_PARAM_TIMEOUT);
        if (timeoutSeq != null) {
            try {
                long parsedTimeout = Numbers.parseLong(timeoutSeq) * 1000;
                state.timeout = parsedTimeout > 0 ? parsedTimeout : defaultTimeout;
            } catch (NumericException ex) {
                state.timeout = defaultTimeout;
            }
        }

        state.skip = skip;
        state.count = 0L;
        state.stop = stop;
        state.noMeta = HttpKeywords.isTrue(request.getUrlParam(URL_PARAM_NM));
        state.countRows = HttpKeywords.isTrue(request.getUrlParam(URL_PARAM_COUNT));
        return true;
    }

    private void putArrayValue(HttpChunkedResponse response, ExportQueryProcessorState state, Record record, int columnIdx, int columnType) {
        state.arrayState.of(response);
        var arrayView = state.arrayState.getArrayView() == null ? record.getArray(columnIdx, columnType) : state.arrayState.getArrayView();
        try {
            state.arrayState.putCharIfNew(response, '"');
            ArrayTypeDriver.arrayToJson(arrayView, response, state.arrayState);
            state.arrayState.putCharIfNew(response, '"');
            state.arrayState.clear();
            state.columnValueFullySent = true;
        } catch (Throwable e) {
            // we have to disambiguate here if this is the first attempt to send the value, which failed,
            // and we have any partial value we can send to the clint, or our state did not bookmark anything?
            state.columnValueFullySent = state.arrayState.isNothingWritten();
            state.arrayState.reset(arrayView);
            throw e;
        }
    }

    private void putValue(HttpChunkedResponse response, ExportQueryProcessorState state) {
        long l;
        final int columnType = state.metadata.getColumnType(state.columnIndex);
        final int columnIndex = state.columnIndex;
        final Record rec = state.record;
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
                response.put(rec.getBool(columnIndex));
                break;
            case ColumnType.BYTE:
                response.put((int) rec.getByte(columnIndex));
                break;
            case ColumnType.DOUBLE:
                double d = rec.getDouble(columnIndex);
                if (Numbers.isFinite(d)) {
                    response.put(d);
                }
                break;
            case ColumnType.FLOAT:
                float f = rec.getFloat(columnIndex);
                if (Numbers.isFinite(f)) {
                    response.put(f);
                }
                break;
            case ColumnType.INT:
                final int i = rec.getInt(columnIndex);
                if (i != Numbers.INT_NULL) {
                    response.put(i);
                }
                break;
            case ColumnType.LONG:
                l = rec.getLong(columnIndex);
                if (l != Numbers.LONG_NULL) {
                    response.put(l);
                }
                break;
            case ColumnType.DATE:
                l = rec.getDate(columnIndex);
                if (l != Numbers.LONG_NULL) {
                    response.putAscii('"').putISODateMillis(l).putAscii('"');
                }
                break;
            case ColumnType.TIMESTAMP:
                l = rec.getTimestamp(columnIndex);
                if (l != Numbers.LONG_NULL) {
                    response.putAscii('"').putISODate(ColumnType.getTimestampDriver(columnType), l).putAscii('"');
                }
                break;
            case ColumnType.SHORT:
                response.put(rec.getShort(columnIndex));
                break;
            case ColumnType.CHAR:
                char c = rec.getChar(columnIndex);
                if (c > 0) {
                    response.put(c);
                }
                break;
            case ColumnType.NULL:
            case ColumnType.BINARY:
            case ColumnType.RECORD:
                break;
            case ColumnType.STRING:
                putStringOrNull(response, rec.getStrA(columnIndex));
                break;
            case ColumnType.VARCHAR:
                putVarcharOrNull(response, rec.getVarcharA(columnIndex));
                break;
            case ColumnType.SYMBOL:
                putStringOrNull(response, rec.getSymA(columnIndex));
                break;
            case ColumnType.LONG256:
                rec.getLong256(columnIndex, response);
                break;
            case ColumnType.GEOBYTE:
                putGeoHashStringValue(response, rec.getGeoByte(columnIndex), columnType);
                break;
            case ColumnType.GEOSHORT:
                putGeoHashStringValue(response, rec.getGeoShort(columnIndex), columnType);
                break;
            case ColumnType.GEOINT:
                putGeoHashStringValue(response, rec.getGeoInt(columnIndex), columnType);
                break;
            case ColumnType.GEOLONG:
                putGeoHashStringValue(response, rec.getGeoLong(columnIndex), columnType);
                break;
            case ColumnType.UUID:
                putUuidOrNull(response, rec.getLong128Lo(columnIndex), rec.getLong128Hi(columnIndex));
                break;
            case ColumnType.LONG128:
                throw new UnsupportedOperationException();
            case ColumnType.IPv4:
                putIPv4Value(response, rec, columnIndex);
                break;
            case ColumnType.INTERVAL:
                putInterval(response, rec, columnIndex, columnType);
                break;
            case ColumnType.ARRAY:
                putArrayValue(response, state, rec, columnIndex, columnType);
                break;
            case ColumnType.DECIMAL8:
                putDecimal8StringValue(response, rec.getDecimal8(columnIndex), columnType);
                break;
            case ColumnType.DECIMAL16:
                putDecimal16StringValue(response, rec.getDecimal16(columnIndex), columnType);
                break;
            case ColumnType.DECIMAL32:
                putDecimal32StringValue(response, rec.getDecimal32(columnIndex), columnType);
                break;
            case ColumnType.DECIMAL64:
                putDecimal64StringValue(response, rec.getDecimal64(columnIndex), columnType);
                break;
            case ColumnType.DECIMAL128:
                rec.getDecimal128(columnIndex, decimal128);
                putDecimal128StringValue(response, decimal128, columnType);
                break;
            case ColumnType.DECIMAL256:
                rec.getDecimal256(columnIndex, decimal256);
                putDecimal256StringValue(response, decimal256, columnType);
                break;
            default:
                assert false;
        }
    }

    private void sendConfirmation(HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.putAscii("DDL Success\n");
        response.sendChunk(true);
    }

    private void sendDone(
            HttpChunkedResponse response,
            ExportQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.count > -1) {
            state.count = -1;
            response.sendChunk(true);
            return;
        }
        response.done();
    }

    private void sendException(
            HttpChunkedResponse response,
            int errorPosition,
            CharSequence errorMessage,
            ExportQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.parquetFileOffset > 0) {
            // We already sent a partial response to the client.
            // Give up and close the connection.
            LOG.error().$("partial parquet response sent, closing connection on error [fd=").$(state.getFd())
                    .$(", parquetFileOffset=").$(state.parquetFileOffset)
                    .$(", errorMessage=").$safe(errorMessage)
                    .I$();
            throw PeerDisconnectedException.INSTANCE;
        }

        state.storeError(errorPosition, errorMessage);
        response.status(400, CONTENT_TYPE_JSON);
        response.headers().setKeepAlive(configuration.getKeepAliveHeader());
        response.sendHeader();
        state.resumeError(response);
    }

    private void syntaxError(
            HttpChunkedResponse response,
            ExportQueryProcessorState state,
            FlyweightMessageContainer container
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        info(state).$("syntax-error [q=`").$safe(state.sqlText)
                .$("`, at=").$(container.getPosition())
                .$(", message=`").$safe(container.getFlyweightMessage()).$('`').I$();
        sendException(response, container.getPosition(), container.getFlyweightMessage(), state);
    }

    protected void header(
            HttpChunkedResponse response,
            ExportQueryProcessorState state,
            int statusCode
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        boolean isParquet = state.getExportModel().isParquetFormat();
        String contentType = isParquet ? CONTENT_TYPE_PARQUET : CONTENT_TYPE_CSV;
        String fileExtension = isParquet ? FILE_EXTENSION_PARQUET : FILE_EXTENSION_CSV;
        response.status(statusCode, contentType);
        if (!state.fileName.isEmpty()) {
            response.headers().putAscii("Content-Disposition: attachment; filename=\"").put(state.fileName).putAscii(fileExtension).putAscii("\"").putEOL();
        } else {
            response.headers().putAscii("Content-Disposition: attachment; filename=\"questdb-query-").put(clock.getTicks()).putAscii(fileExtension).putAscii("\"").putEOL();
        }
        response.headers().setKeepAlive(configuration.getKeepAliveHeader());
        response.sendHeader();
    }

    protected void headerNoContentDisposition(HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(200, CONTENT_TYPE_CSV);
        response.headers().setKeepAlive(configuration.getKeepAliveHeader());
        response.sendHeader();
    }
}
