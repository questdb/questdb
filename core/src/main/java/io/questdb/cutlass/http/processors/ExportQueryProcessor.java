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
import io.questdb.griffin.model.CopyModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.ConcurrentPool;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;
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

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.griffin.model.CopyModel.COPY_FORMAT_PARQUET;

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
    private final CairoEngine engine;
    private final StringSink errSink = new StringSink();
    private final int maxSqlRecompileAttempts;
    private final Metrics metrics;
    private final ConcurrentPool<SerialParquetExporter> parquetExporterPool;
    private final byte requiredAuthType;
    private final SqlExecutionContextImpl sqlExecutionContext;
    CopyExportRequestTask task = new CopyExportRequestTask();
    private long timeout;

    public ExportQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            ConcurrentPool<SerialParquetExporter> parquetExporterPool,
            CairoEngine engine,
            int sharedQueryWorkerCount
    ) {
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
        this.parquetExporterPool = parquetExporterPool;
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, sharedQueryWorkerCount);
        this.circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB4);
        this.metrics = engine.getMetrics();
        this.engine = engine;
        maxSqlRecompileAttempts = engine.getConfiguration().getMaxSqlRecompileAttempts();
        requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
        Misc.free(circuitBreaker);
    }

    public void execute(
            HttpConnectionContext context,
            ExportQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        try {
            boolean isExpRequest = isExpUrl(context.getRequestHeader().getUrl());

            circuitBreaker.setTimeout(timeout);
            circuitBreaker.resetTimer();
            state.recordCursorFactory = context.getSelectCache().poll(state.query);
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
                CompiledQuery cc = null;
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    cc = compiler.compile(state.query, sqlExecutionContext);
                    if (cc.getType() == CompiledQuery.SELECT || cc.getType() == CompiledQuery.EXPLAIN) {
                        state.recordCursorFactory = cc.getRecordCursorFactory();
                    } else if (isExpRequest) {
                        throw SqlException.$(0, "/exp endpoint only accepts SELECT");
                    }
                    sqlExecutionContext.storeTelemetry(cc.getType(), TelemetryOrigin.HTTP_TEXT);
                } finally {
                    Misc.free(cc);
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
                            CompiledQuery cc = null;
                            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                                cc = compiler.compile(state.query, sqlExecutionContext);
                                if (cc.getType() != CompiledQuery.SELECT && isExpRequest) {
                                    throw SqlException.$(0, "/exp endpoint only accepts SELECT");
                                }
                                state.recordCursorFactory = cc.getRecordCursorFactory();
                            } finally {
                                Misc.free(cc);
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
            LV.set(context, state = new ExportQueryProcessorState(context, engine));
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

    private void cleanupParquetState(ExportQueryProcessorState state) {
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        if (state.parquetFileFd != -1) {
            ff.close(state.parquetFileFd);
            state.parquetFileFd = -1;
            state.getExportResult().cleanUpTempPath(ff);
        }
        if (state.parquetFileAddress != 0) {
            ff.munmap(state.parquetFileAddress, state.parquetFileSize, MemoryTag.NATIVE_PARQUET_EXPORTER);
            state.parquetFileAddress = 0;
        }
    }

    private boolean configureParquetOptions(
            HttpRequestHeader request,
            HttpChunkedResponse response,
            ExportQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        CopyModel copyModel = state.getCopyModel();
        copyModel.setParquetDefaults(engine.getConfiguration());
        copyModel.setPartitionBy(PartitionBy.NONE);
        copyModel.setSelectText(state.query, 0);

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
            copyModel.setCompressionCodec(codec);
        }

        // Handle compression_level option
        DirectUtf8Sequence compressionLevel = request.getUrlParam(EXPORT_PARQUET_OPTION_COMPRESSION_LEVEL);
        if (compressionLevel != null && compressionLevel.size() > 0) {
            try {
                int level = Numbers.parseInt(compressionLevel);
                copyModel.setCompressionLevel(level, 0);
            } catch (NumericException e) {
                errSink.clear();
                errSink.put("invalid compression level:").put(compressionLevel);
                sendException(response, 0, errSink, state);
                return false;
            }
        }

        try {
            copyModel.validCompressOptions();
        } catch (SqlException e) {
            sendException(response, 0, e.getFlyweightMessage(), state);
            return false;
        }

        // Handle row_group_size option
        DirectUtf8Sequence rowGroupSize = request.getUrlParam(EXPORT_PARQUET_OPTION_ROW_GROUP_SIZE);
        if (rowGroupSize != null && rowGroupSize.size() > 0) {
            try {
                int size = Numbers.parseInt(rowGroupSize);
                copyModel.setRowGroupSize(size);
            } catch (NumericException e) {
                errSink.clear();
                errSink.put("invalid row group size:").put(rowGroupSize);
                sendException(response, 0, errSink, state);
                return false;
            }
        }

        // Handle data_page_size option
        DirectUtf8Sequence dataPageSize = request.getUrlParam(EXPORT_PARQUET_OPTION_DATA_PAGE_SIZE);
        if (dataPageSize != null && dataPageSize.size() > 0) {
            try {
                int size = Numbers.parseInt(dataPageSize);
                copyModel.setDataPageSize(size);
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
            copyModel.setStatisticsEnabled(enabled);
        }

        // Handle parquet_version option
        DirectUtf8Sequence parquetVersion = request.getUrlParam(EXPORT_PARQUET_OPTION_PARQUET_VERSION);
        if (parquetVersion != null && parquetVersion.size() > 0) {
            try {
                int version = Numbers.parseInt(parquetVersion);
                if (version != CopyModel.PARQUET_VERSION_V1 && version != CopyModel.PARQUET_VERSION_V2) {
                    errSink.clear();
                    errSink.put("invalid parquet version: ").put(parquetVersion)
                            .put(", supported versions: ")
                            .put(CopyModel.PARQUET_VERSION_V1).put(", ")
                            .put(CopyModel.PARQUET_VERSION_V2);
                    sendException(response, 0, errSink, state);
                    return false;
                }
                copyModel.setParquetVersion(version);
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
            copyModel.setRawArrayEncoding(enabled);
        }

        return true;
    }

    private void copyQueryToParquetFile(HttpConnectionContext context, ExportQueryProcessorState state) throws SqlException {
        if (state.copyID == null) {
            SerialParquetExporter parquetExporter = parquetExporterPool.pop();
            if (parquetExporter == null) {
                parquetExporter = new SerialParquetExporter(engine);
            }

            CopyExportContext.ExportTaskEntry entry = null;
            try {

                var securityContext = context.getSecurityContext();

                var selectText = state.query;
                var fileName = state.fileName;
                var sqlExecutionCircuitBreaker = sqlExecutionContext.getCircuitBreaker();
                var copyContext = engine.getCopyExportContext();
                CopyExportResult exportResult = state.getExportResult();

                entry = copyContext.assignExportEntry(securityContext, selectText, fileName, sqlExecutionCircuitBreaker);
                long copyID = entry.getId();
                exportResult.setCopyID(copyID);

                var sink = Misc.getThreadLocalSink();
                sink.put(engine.getConfiguration().getParquetExportTableNamePrefix());
                Numbers.appendHex(sink, copyID, true);
                String tableName = sink.toString();

                var createOp = copyContext.validateAndCreateTableOp(
                        sqlExecutionContext,
                        selectText,
                        state.getCopyModel().getPartitionBy(),
                        tableName,
                        "",
                        0
                );

                task.of(
                        entry,
                        createOp,
                        exportResult,
                        tableName,
                        fileName,
                        state.getCopyModel().getCompressionCodec(),
                        state.getCopyModel().getCompressionLevel(),
                        state.getCopyModel().getRowGroupSize(),
                        state.getCopyModel().getDataPageSize(),
                        state.getCopyModel().isStatisticsEnabled(),
                        state.getCopyModel().getParquetVersion(),
                        state.getCopyModel().isRawArrayEncoding(),
                        state.getCopyModel().isUserSpecifiedExportOptions()
                );

                parquetExporter.of(task, sqlExecutionCircuitBreaker);
                parquetExporter.process();
            } finally {
                if (entry != null) {
                    engine.getCopyExportContext().releaseEntry(entry);
                }
                if (!engine.isClosing()) {
                    parquetExporterPool.push(parquetExporter);
                } else {
                    parquetExporter.close();
                }
            }
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
                            copyQueryToParquetFile(context, state);
                        } catch (SqlException e) {
                            sendException(response, e.getPosition(), e.getFlyweightMessage(), state);
                            break OUT;
                        } catch (CairoException e) {
                            sendException(response, e.getPosition(), e.getFlyweightMessage(), state);
                            break OUT;
                        }
                        state.queryState = JsonQueryProcessorState.QUERY_PARQUET_FILE_SEND_INIT;
                        // fall through

                    case JsonQueryProcessorState.QUERY_PARQUET_FILE_SEND_INIT:
                        try {
                            initParquetFileSending(context, state);
                        } catch (CairoException e) {
                            cleanupParquetState(state);
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
                        response.sendChunk(true);
                        sendDone(response, state);
                        break OUT;
                    default:
                        break OUT;
                }
            } catch (DataUnavailableException e) {
                throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                response.sendChunk(false);
            }
        }

        // Clean up parquet state when done
        cleanupParquetState(state);
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

        if (state.getCopyModel().isParquetFormat()) {
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

    private LogRecord info(ExportQueryProcessorState state) {
        return LOG.info().$('[').$(state.getFd()).$("] ");
    }

    private void initParquetFileSending(HttpConnectionContext context, ExportQueryProcessorState state) throws PeerDisconnectedException, PeerIsSlowToReadException {
        Path path = state.getExportResult().getPath();
        if (path.size() == 0) {
            throw CairoException.nonCritical().put("empty table");
        }
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        state.parquetFileFd = ff.openRO(path.$());
        if (state.parquetFileFd < 0) {
            throw CairoException.critical(CairoException.ERRNO_FILE_DOES_NOT_EXIST)
                    .put("could not find parquet file: [path=").put(path)
                    .put(", copyId=").put(state.copyID).put(']');
        }

        state.parquetFileSize = ff.length(state.parquetFileFd);
        state.parquetFileOffset = 0;
        state.parquetFileAddress = TableUtils.mapRO(ff, state.parquetFileFd, state.parquetFileSize, MemoryTag.NATIVE_PARQUET_EXPORTER);
        header(context.getChunkedResponse(), state, 200);
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
        if (e instanceof CairoException) {
            CairoException ce = (CairoException) e;
            if (ce.isInterruption()) {
                info(state).$("query cancelled [reason=`").$safe(((CairoException) e).getFlyweightMessage())
                        .$("`, q=`").$safe(state.query)
                        .$("`]").$();
            } else if (ce.isCritical()) {
                critical(state).$("error [msg=`").$safe(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$("`, q=`").$safe(state.query)
                        .$("`]").$();
            } else {
                error(state).$("error [msg=`").$safe(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$("`, q=`").$safe(state.query)
                        .$("`]").$();
            }
        } else if (e instanceof HttpException) {
            error(state).$("internal HTTP server error [reason=`").$safe(((HttpException) e).getFlyweightMessage())
                    .$("`, q=`").$safe(state.query)
                    .$("`]").$();
        } else {
            critical(state).$("internal error [ex=").$(e)
                    .$(", q=`").$safe(state.query)
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

        state.query.clear();
        if (!Utf8s.utf8ToUtf16(query.lo(), query.hi(), state.query)) {
            info(state).$("Bad UTF8 encoding").$();
            sendException(response, 0, "Bad UTF8 encoding in query text", state);
            return false;
        }
        DirectUtf8Sequence fileName = request.getUrlParam(URL_PARAM_FILENAME);
        state.fileName = null;
        if (fileName != null && fileName.size() > 0) {
            state.fileName = fileName.toString();
        }

        DirectUtf8Sequence delimiter = request.getUrlParam(URL_PARAM_DELIMITER);
        state.delimiter = ',';

        if (delimiter != null && delimiter.size() == 1) {
            state.delimiter = (char) delimiter.byteAt(0);
        }

        CopyModel copyModel = state.getCopyModel();
        copyModel.clear();
        copyModel.setFormat(CopyModel.COPY_FORMAT_CSV);
        DirectUtf8Sequence format = request.getUrlParam(URL_PARAM_FMT);
        if (format != null && format.size() > 0) {
            if (SqlKeywords.isParquetKeyword(format.asAsciiCharSequence())) {
                copyModel.setFormat(COPY_FORMAT_PARQUET);
            } else if (!SqlKeywords.isCsvKeyword(format.asAsciiCharSequence())) {
                errSink.clear();
                errSink.put("unrecognised format [format=").put(format).put("]");
                sendException(response, 0, errSink, state);
            }
        }
        if (copyModel.isParquetFormat()) {
            if (!configureParquetOptions(request, response, state)) {
                return false;
            }
        }

        if (copyModel.isParquetFormat()) { // parquet use export timeout
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
        headerJsonError(response);
        JsonQueryProcessorState.prepareExceptionJson(response, position, message, state.query);
    }

    private void sendParquetFileChunk(HttpChunkedResponse response, ExportQueryProcessorState state) {
        if (state.parquetFileOffset >= state.parquetFileSize) {
            return;
        }

        long writeSize = state.parquetFileSize - state.parquetFileOffset;
        state.parquetFileOffset += response.writeBytes(state.parquetFileAddress + state.parquetFileOffset, (int) Math.min(Integer.MAX_VALUE, writeSize));
        response.bookmark();
        if (state.parquetFileOffset >= state.parquetFileSize) {
            cleanupParquetState(state);
        } else {
            throw NoSpaceLeftInResponseBufferException.instance(writeSize);
        }
    }

    private void syntaxError(
            HttpChunkedResponse response,
            ExportQueryProcessorState state,
            FlyweightMessageContainer container
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        info(state).$("syntax-error [q=`").$safe(state.query)
                .$("`, at=").$(container.getPosition())
                .$(", message=`").$safe(container.getFlyweightMessage()).$('`').I$();
        sendException(response, container.getPosition(), container.getFlyweightMessage(), state);
    }

    protected void header(
            HttpChunkedResponse response,
            ExportQueryProcessorState state,
            int statusCode
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        boolean isParquet = state.getCopyModel().isParquetFormat();
        String contentType = isParquet ? CONTENT_TYPE_PARQUET : CONTENT_TYPE_CSV;
        String fileExtension = isParquet ? FILE_EXTENSION_PARQUET : FILE_EXTENSION_CSV;
        response.status(statusCode, contentType);
        if (state.fileName != null && !state.fileName.isEmpty()) {
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
