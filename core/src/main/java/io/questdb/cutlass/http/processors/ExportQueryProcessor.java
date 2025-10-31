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

import io.questdb.Metrics;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
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
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.SerialParquetExporter;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.cutlass.text.CopyExportResult;
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
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.FilesFacade;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Interval;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.griffin.model.ExportModel.COPY_FORMAT_PARQUET;

public class ExportQueryProcessor implements HttpRequestProcessor, HttpRequestHandler, Closeable {
    private static final String FILE_EXTENSION_CSV = ".csv";
    private static final String FILE_EXTENSION_PARQUET = ".parquet";
    private static final Log LOG = LogFactory.getLog(ExportQueryProcessor.class);
    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    private static final LocalValue<ExportQueryProcessorState> LV = new LocalValue<>();
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final MillisecondClock clock;
    private final JsonQueryProcessorConfiguration configuration;
    private final Decimal128 decimal128 = new Decimal128();
    private final Decimal256 decimal256 = new Decimal256();
    private final CairoEngine engine;
    private final StringSink errSink = new StringSink();
    private final int maxSqlRecompileAttempts;
    private final Metrics metrics;
    private final byte requiredAuthType;
    private final SerialParquetExporter serialParquetExporter;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final CopyExportRequestTask task = new CopyExportRequestTask();
    private long timeout;

    public ExportQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int sharedQueryWorkerCount
    ) {
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
        serialParquetExporter = new SerialParquetExporter(engine);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, sharedQueryWorkerCount);
        this.circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB4);
        this.metrics = engine.getMetrics();
        this.engine = engine;
        maxSqlRecompileAttempts = engine.getConfiguration().getMaxSqlRecompileAttempts();
        requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
        Misc.free(circuitBreaker);
        Misc.free(serialParquetExporter);
    }

    public void execute(
            HttpConnectionContext context,
            ExportQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        try {
            boolean isExpRequest = isExpUrl(context.getRequestHeader().getUrl());

            circuitBreaker.setTimeout(timeout);
            circuitBreaker.resetTimer();
            state.recordCursorFactory = context.getSelectCache().poll(state.sqlText);
            state.setQueryCacheable(true);
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
                    sqlExecutionContext.storeTelemetry(cc.getType(), TelemetryOrigin.HTTP_TEXT);
                }
            } else {
                sqlExecutionContext.setCacheHit(true);
                sqlExecutionContext.storeTelemetry(CompiledQuery.SELECT, TelemetryOrigin.HTTP_TEXT);
            }

            if (state.recordCursorFactory != null) {
                try {
                    boolean runQuery = true;
                    for (int retries = 0; runQuery; retries++) {
                        try {
                            state.cursor = state.recordCursorFactory.getCursor(sqlExecutionContext);
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
                    state.setQueryCacheable(e.isCacheable());
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
        return ActiveConnectionTracker.PROCESSOR_EXPORT;
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
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        ExportQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new ExportQueryProcessorState(context, engine.getConfiguration().getFilesFacade()));
        }

        HttpChunkedResponse response = context.getChunkedResponse();
        if (parseUrl(response, context.getRequestHeader(), state)) {
            execute(context, state);
        } else {
            readyForNextRequest(context);
        }
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        ExportQueryProcessorState state = LV.get(context);
        if (state != null) {
            state.pausedQuery = pausedQuery;
            state.rnd = sqlExecutionContext.getRandom();
        }
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
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
            var securityContext = context.getSecurityContext();
            var sqlExecutionCircuitBreaker = sqlExecutionContext.getCircuitBreaker();
            var copyExportContext = engine.getCopyExportContext();
            CopyExportResult exportResult = state.getExportResult();

            entry = copyExportContext.assignExportEntry(
                    securityContext,
                    state.sqlText,
                    state.fileName,
                    sqlExecutionCircuitBreaker,
                    CopyExportContext.CopyTrigger.HTTP
            );

            state.copyID = entry.getId();
            exportResult.setCopyID(state.copyID);
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
        } catch (Throwable th) {
            if (entry != null) {
                engine.getCopyExportContext().releaseEntry(entry);
                state.clear();
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

    private void copyQueryToParquetFile(ExportQueryProcessorState state) throws SqlException {
        if (state.copyID != -1) {
            CopyExportContext.ExportTaskEntry entry = null;
            try {
                var copyExportContext = engine.getCopyExportContext();
                CopyExportResult exportResult = state.getExportResult();
                entry = copyExportContext.getEntry(state.copyID);

                task.of(
                        entry,
                        state.getParquetTempTableCreate(),
                        exportResult,
                        state.getParquetExportTableName(),
                        state.fileName,
                        state.getExportModel().getCompressionCodec(),
                        state.getExportModel().getCompressionLevel(),
                        state.getExportModel().getRowGroupSize(),
                        state.getExportModel().getDataPageSize(),
                        state.getExportModel().isStatisticsEnabled(),
                        state.getExportModel().getParquetVersion(),
                        state.getExportModel().isRawArrayEncoding()
                );

                serialParquetExporter.of(task);
                serialParquetExporter.process();
            } finally {
                if (entry != null) {
                    engine.getCopyExportContext().releaseEntry(entry);
                }
            }
        } else {
            throw CairoException.nonCritical().put("invalid export state, cannot find export id");
        }
    }

    private LogRecord critical(ExportQueryProcessorState state) {
        return LOG.critical().$('[').$(state.getFd()).$("] ");
    }

    private void doParquetExport(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException {
        ExportQueryProcessorState state = LV.get(context);
        final HttpChunkedResponse response = context.getChunkedResponse();

        OUT:
        while (true) {
            try {
                switch (state.queryState) {
                    case JsonQueryProcessorState.QUERY_SETUP_FIRST_RECORD:
                        state.queryState = JsonQueryProcessorState.QUERY_PARQUET_EXPORT_INIT;
                    case JsonQueryProcessorState.QUERY_PARQUET_EXPORT_INIT:
                        try {
                            compileParquetExport(context, state);
                        } catch (SqlException | CairoException e) {
                            sendException(response, e.getPosition(), e.getFlyweightMessage(), state);
                            break OUT;
                        } catch (Throwable e) {
                            sendException(response, 0, e.getMessage(), state);
                            break OUT;
                        }
                        state.queryState = JsonQueryProcessorState.QUERY_PARQUET_SEND_HEADER;
                        // fall through

                    case JsonQueryProcessorState.QUERY_PARQUET_SEND_HEADER:
                        if (state.getExportModel().isNoDelay()) {
                            try {
                                sendParquetHeader(response, state);
                            } catch (CairoException e) {
                                sendException(response, 0, e.getFlyweightMessage(), state);
                                break OUT;
                            }
                        }
                        state.queryState = JsonQueryProcessorState.QUERY_PARQUET_TO_PARQUET_FILE;
                        // fall through

                    case JsonQueryProcessorState.QUERY_PARQUET_TO_PARQUET_FILE:
                        try {
                            copyQueryToParquetFile(state);
                        } catch (SqlException | CairoException e) {
                            sendException(response, e.getPosition(), e.getFlyweightMessage(), state);
                            break OUT;
                        } catch (Throwable e) {
                            sendException(response, 0, e.getMessage(), state);
                            break OUT;
                        }
                        state.queryState = JsonQueryProcessorState.QUERY_PARQUET_FILE_SEND_INIT;
                        // fall through

                    case JsonQueryProcessorState.QUERY_PARQUET_FILE_SEND_INIT:
                        try {
                            initParquetFileSending(context, state);
                        } catch (CairoException e) {
                            sendException(response, 0, e.getFlyweightMessage(), state);
                            break OUT;
                        }
                        state.queryState = JsonQueryProcessorState.QUERY_PARQUET_FILE_SEND_CHUNK;
                        // fall through

                    case JsonQueryProcessorState.QUERY_PARQUET_FILE_SEND_CHUNK:
                        sendParquetFileChunk(response, state);
                        if (state.parquetFileOffset >= state.parquetFileSize) {
                            state.queryState = JsonQueryProcessorState.QUERY_PARQUET_FILE_SEND_COMPLETE;
                        }
                        break;
                    case JsonQueryProcessorState.QUERY_PARQUET_FILE_SEND_COMPLETE:
                        sendDone(response, state);
                        break OUT;
                    default:
                        break OUT;
                }
            } catch (DataUnavailableException e) {
                throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
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

    private void doResumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException {
        ExportQueryProcessorState state = LV.get(context);
        if (state == null) {
            return;
        }

        // copy random during query resume
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

        final RecordMetadata metadata = state.recordCursorFactory.getMetadata();
        final int columnCount = metadata.getColumnCount();

        OUT:
        while (true) {
            try {
                SWITCH:
                switch (state.queryState) {
                    case JsonQueryProcessorState.QUERY_SETUP_FIRST_RECORD:
                        state.hasNext = state.cursor.hasNext();
                        header(response, state, 200);
                        state.queryState = JsonQueryProcessorState.QUERY_METADATA;
                        // fall through

                    case JsonQueryProcessorState.QUERY_METADATA:
                        if (!state.noMeta) {
                            state.columnIndex = 0;
                            while (state.columnIndex < columnCount) {
                                if (state.columnIndex > 0) {
                                    response.putAscii(state.delimiter);
                                }
                                response.putQuote().escapeCsvStr(metadata.getColumnName(state.columnIndex)).putQuote();
                                state.columnIndex++;
                                response.bookmark();
                            }
                            response.putEOL();
                        }
                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_START;
                        response.bookmark();
                        // fall through
                    case JsonQueryProcessorState.QUERY_RECORD_START:
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
                                    state.queryState = JsonQueryProcessorState.QUERY_SUFFIX;
                                    break SWITCH;
                                }
                            }
                        }

                        if (state.count > state.stop) {
                            state.queryState = JsonQueryProcessorState.QUERY_SUFFIX;
                            break;
                        }

                        state.queryState = JsonQueryProcessorState.QUERY_RECORD;
                        state.columnIndex = 0;
                        // fall through
                    case JsonQueryProcessorState.QUERY_RECORD:
                        while (state.columnIndex < columnCount) {
                            if (state.columnIndex > 0 && state.columnValueFullySent) {
                                response.putAscii(state.delimiter);
                            }
                            putValue(response, state);
                            state.columnIndex++;
                            response.bookmark();
                        }

                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_SUFFIX;
                        // fall through
                    case JsonQueryProcessorState.QUERY_RECORD_SUFFIX:
                        response.putEOL();
                        state.record = null;
                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_START;
                        response.bookmark();
                        break;
                    case JsonQueryProcessorState.QUERY_SUFFIX:
                        // close cursor before returning complete response
                        // this will guarantee that by the time client reads the response fully the table will be released
                        state.cursor = Misc.free(state.cursor);
                        sendDone(response, state);
                        break OUT;
                    default:
                        break OUT;
                }
            } catch (DataUnavailableException e) {
                response.resetToBookmark();
                throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
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

    private void initParquetFileSending(HttpConnectionContext context, ExportQueryProcessorState state) throws PeerDisconnectedException, PeerIsSlowToReadException {
        Utf8Sequence pathStr = state.getExportResult().getPath();
        if (pathStr.size() == 0) {
            throw CairoException.nonCritical().put("empty table");
        }
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        Path path = Path.getThreadLocal(pathStr);
        state.parquetFileFd = ff.openRO(path.$());
        if (state.parquetFileFd < 0) {
            throw CairoException.critical(CairoException.ERRNO_FILE_DOES_NOT_EXIST)
                    .put("could not find parquet file: [path=").put(path)
                    .put(", copyId=").put(state.copyID).put(']');
        }

        state.parquetFileSize = ff.length(state.parquetFileFd);
        state.parquetFileAddress = TableUtils.mapRO(ff, state.parquetFileFd, state.parquetFileSize, MemoryTag.NATIVE_PARQUET_EXPORTER);

        // in Nodelay mode header is sent before parquet file is read
        // so no need to send it here
        if (!state.getExportModel().isNoDelay()) {
            header(context.getChunkedResponse(), state, 200);
        }
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
            ExportQueryProcessorState state
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

        if (exportModel.isParquetFormat()) { // parquet use export timeout
            timeout = configuration.getExportTimeout();
        } else { // csv use query timeout
            timeout = circuitBreaker.getDefaultMaxTime();
        }

        DirectUtf8Sequence timeoutSeq = request.getUrlParam(URL_PARAM_TIMEOUT);
        if (timeoutSeq != null) {
            try {
                timeout = Numbers.parseLong(timeoutSeq) * 1000;
                if (timeout <= 0) {
                    timeout = configuration.getExportTimeout();
                }
            } catch (NumericException ex) {
                // Skip or stop will have default value.
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
            int position,
            CharSequence message,
            ExportQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.parquetFileOffset > 0) {
            // We already sent a partial response to the client.
            // Give up and close the connection.
            LOG.error().$("partial parquet response sent, closing connection on error [fd=").$(state.getFd())
                    .$(", parquetFileOffset=").$(state.parquetFileOffset)
                    .$(", errorMessage=").$safe(message)
                    .I$();
            throw PeerDisconnectedException.INSTANCE;
        }
        headerJsonError(response);
        JsonQueryProcessorState.prepareExceptionJson(response, position, message, state.sqlText);
    }

    private void sendParquetFileChunk(HttpChunkedResponse response, ExportQueryProcessorState state) throws PeerIsSlowToReadException, PeerDisconnectedException {
        if (state.parquetFileOffset >= state.parquetFileSize) {
            return;
        }

        // This can easily overflow int with 2G+ parquet file size.
        long writeSize = state.parquetFileSize - state.parquetFileOffset;
        int sendLSize = (int) Math.min(Integer.MAX_VALUE, writeSize);
        state.parquetFileOffset += response.writeBytes(state.parquetFileAddress + state.parquetFileOffset, sendLSize);
        response.bookmark();
        if (state.parquetFileOffset < state.parquetFileSize) {
            response.sendChunk(false);
        }
    }

    private void sendParquetHeader(HttpChunkedResponse response, ExportQueryProcessorState state) throws PeerDisconnectedException, PeerIsSlowToReadException {
        header(response, state, 200);

        // We send first 3 byes of parquet file, that is always "PAR" in ascii
        // as a chunk to trigger download to open file save dialog and start background download browsers.
        response.put("PAR");
        state.parquetFileOffset = 3;
        response.bookmark();
        response.sendChunk(false);
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

    protected void headerJsonError(HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(400, CONTENT_TYPE_JSON);
        response.headers().setKeepAlive(configuration.getKeepAliveHeader());
        response.sendHeader();
    }

    protected void headerNoContentDisposition(HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(200, CONTENT_TYPE_CSV);
        response.headers().setKeepAlive(configuration.getKeepAliveHeader());
        response.sendHeader();
    }
}
