/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.QueryLogger;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.http.*;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;

public class TextQueryProcessor implements HttpRequestProcessor, Closeable {

    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    private static final LocalValue<TextQueryProcessorState> LV = new LocalValue<>();
    @SuppressWarnings("FieldMayBeFinal")
    private static Log LOG = LogFactory.getLog(TextQueryProcessor.class);
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final MillisecondClock clock;
    private final JsonQueryProcessorConfiguration configuration;
    private final int doubleScale;
    private final CairoEngine engine;
    private final int floatScale;
    private final Metrics metrics;
    private final QueryLogger queryLogger;
    private final byte requiredAuthType;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final int maxSqlRecompileAttempts;

    @TestOnly
    public TextQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount
    ) {
        this(configuration, engine, workerCount, workerCount);
    }

    public TextQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount,
            int sharedWorkerCount
    ) {
        this.configuration = configuration;
        this.floatScale = configuration.getFloatScale();
        this.clock = configuration.getClock();
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount, sharedWorkerCount);
        this.doubleScale = configuration.getDoubleScale();
        this.circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB4);
        this.metrics = engine.getMetrics();
        this.engine = engine;
        queryLogger = engine.getConfiguration().getQueryLogger();
        maxSqlRecompileAttempts = engine.getConfiguration().getMaxSqlRecompileAttempts();
        requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
        Misc.free(circuitBreaker);
    }

    public void execute(
            HttpConnectionContext context,
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        try {
            boolean isExpRequest = isExpUrl(context.getRequestHeader().getUrl());

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
            if (state.recordCursorFactory == null) {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final CompiledQuery cc = compiler.compile(state.query, sqlExecutionContext);
                    if (cc.getType() == CompiledQuery.SELECT || cc.getType() == CompiledQuery.EXPLAIN) {
                        state.recordCursorFactory = cc.getRecordCursorFactory();
                    } else if (isExpRequest) {
                        throw SqlException.$(0, "/exp endpoint only accepts SELECT");
                    }
                    queryLogger.logQuery(LOG, context.getFd(), state.query, context.getSecurityContext(), "execute-new")
                            .$(", skip: ").$(state.skip)
                            .$(", stop: ").$(state.stop)
                            .I$();
                    sqlExecutionContext.storeTelemetry(cc.getType(), TelemetryOrigin.HTTP_TEXT);
                }
            } else {
                queryLogger.logQuery(LOG, context.getFd(), state.query, context.getSecurityContext(), "execute-cached")
                        .$(", skip: ").$(state.skip)
                        .$(", stop: ").$(state.stop)
                        .I$();
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
                            info(state).$(e.getFlyweightMessage()).$();
                            state.recordCursorFactory = Misc.free(state.recordCursorFactory);
                            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                                final CompiledQuery cc = compiler.compile(state.query, sqlExecutionContext);
                                if (cc.getType() != CompiledQuery.SELECT && isExpRequest) {
                                    throw SqlException.$(0, "/exp endpoint only accepts SELECT");
                                }
                                state.recordCursorFactory = cc.getRecordCursorFactory();
                            }
                        }
                    }
                    state.metadata = state.recordCursorFactory.getMetadata();
                    header(context.getChunkedResponse(), state, 200);
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
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        TextQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new TextQueryProcessorState(context));
        }
        // new request clears random
        state.rnd = null;

        HttpChunkedResponse response = context.getChunkedResponse();
        if (parseUrl(response, context.getRequestHeader(), state)) {
            execute(context, state);
        } else {
            readyForNextRequest(context);
        }
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        TextQueryProcessorState state = LV.get(context);
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
            TextQueryProcessorState state = LV.get(context);
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

    private static void putStringOrNull(HttpChunkedResponse r, CharSequence str) {
        if (str != null) {
            r.putQuoted(str);
        }
    }

    private static void putUuidOrNull(HttpChunkedResponse response, long lo, long hi) {
        if (Uuid.isNull(lo, hi)) {
            return;
        }
        Numbers.appendUuid(lo, hi, response);
    }

    private static void readyForNextRequest(HttpConnectionContext context) {
        LOG.info().$("all sent [fd=").$(context.getFd())
                .$(", lastRequestBytesSent=").$(context.getLastRequestBytesSent())
                .$(", nCompletedRequests=").$(context.getNCompletedRequests() + 1)
                .$(", totalBytesSent=").$(context.getTotalBytesSent()).I$();
    }

    private LogRecord critical(TextQueryProcessorState state) {
        return LOG.critical().$('[').$(state.getFd()).$("] ");
    }

    private void doResumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException {
        TextQueryProcessorState state = LV.get(context);
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
        final int columnCount = state.metadata.getColumnCount();

        OUT:
        while (true) {
            try {
                SWITCH:
                switch (state.queryState) {
                    case JsonQueryProcessorState.QUERY_PREFIX:
                    case JsonQueryProcessorState.QUERY_METADATA:
                        state.columnIndex = 0;
                        state.queryState = JsonQueryProcessorState.QUERY_METADATA;
                        while (state.columnIndex < columnCount) {
                            if (state.columnIndex > 0) {
                                response.putAscii(state.delimiter);
                            }
                            response.putQuoted(state.metadata.getColumnName(state.columnIndex));
                            state.columnIndex++;
                            response.bookmark();
                        }
                        response.putEOL();
                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_START;
                        response.bookmark();
                        // fall through
                    case JsonQueryProcessorState.QUERY_RECORD_START:
                        if (state.record == null) {
                            // check if cursor has any records
                            Record record = state.cursor.getRecord();
                            while (true) {
                                if (state.cursor.hasNext()) {
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
                            if (state.columnIndex > 0) {
                                response.putAscii(state.delimiter);
                            }
                            putValue(response, state.metadata.getColumnType(state.columnIndex), state.record, state.columnIndex);
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
                    // what we have here is out unit of data, column value or query
                    // is larger that response content buffer
                    // all we can do in this scenario is to log appropriately
                    // and disconnect socket
                    info(state).$("Response buffer is too small, state=").$(state.queryState).$();
                    throw PeerDisconnectedException.INSTANCE;
                }
            }
        }
        // reached the end naturally?
        readyForNextRequest(context);
    }

    private LogRecord error(TextQueryProcessorState state) {
        return LOG.error().$('[').$(state.getFd()).$("] ");
    }

    private LogRecord info(TextQueryProcessorState state) {
        return LOG.info().$('[').$(state.getFd()).$("] ");
    }

    private void internalError(
            HttpChunkedResponse response,
            long bytesSent,
            Throwable e,
            TextQueryProcessorState state
    ) throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        logInternalError(e, state);
        if (bytesSent > 0) {
            // We already sent a partial response to the client.
            // Give up and close the connection.
            throw ServerDisconnectException.INSTANCE;
        }
        sendException(response, 0, e.getMessage(), state);
    }

    private void logInternalError(Throwable e, TextQueryProcessorState state) {
        if (e instanceof CairoException) {
            CairoException ce = (CairoException) e;
            if (ce.isInterruption()) {
                info(state).$("query cancelled [reason=`").$(((CairoException) e).getFlyweightMessage())
                        .$("`, q=`").utf8(state.query)
                        .$("`]").$();
            } else if (ce.isCritical()) {
                critical(state).$("error [msg=`").$(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$("`, q=`").utf8(state.query)
                        .$("`]").$();
            } else {
                error(state).$("error [msg=`").$(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$("`, q=`").utf8(state.query)
                        .$("`]").$();
            }
        } else if (e instanceof HttpException) {
            error(state).$("internal HTTP server error [reason=`").$(((HttpException) e).getFlyweightMessage())
                    .$("`, q=`").utf8(state.query)
                    .$("`]").$();
        } else {
            critical(state).$("internal error [ex=").$(e)
                    .$(", q=`").utf8(state.query)
                    .$("`]").$();
            // This is a critical error, so we treat it as an unhandled one.
            metrics.health().incrementUnhandledErrors();
        }
    }

    private boolean parseUrl(
            HttpChunkedResponse response,
            HttpRequestHeader request,
            TextQueryProcessorState state
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

        state.skip = skip;
        state.count = 0L;
        state.stop = stop;
        state.noMeta = Utf8s.equalsNcAscii("true", request.getUrlParam(URL_PARAM_NM));
        state.countRows = Utf8s.equalsNcAscii("true", request.getUrlParam(URL_PARAM_COUNT));
        return true;
    }

    private void putValue(HttpChunkedResponse response, int type, Record rec, int col) {
        long l;
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
                response.put(rec.getBool(col));
                break;
            case ColumnType.BYTE:
                response.put((int) rec.getByte(col));
                break;
            case ColumnType.DOUBLE:
                double d = rec.getDouble(col);
                if (d == d) {
                    response.put(d, doubleScale);
                }
                break;
            case ColumnType.FLOAT:
                float f = rec.getFloat(col);
                if (f == f) {
                    response.put(f, floatScale);
                }
                break;
            case ColumnType.INT:
                final int i = rec.getInt(col);
                if (i > Integer.MIN_VALUE) {
                    response.put(i);
                }
                break;
            case ColumnType.LONG:
                l = rec.getLong(col);
                if (l > Long.MIN_VALUE) {
                    response.put(l);
                }
                break;
            case ColumnType.DATE:
                l = rec.getDate(col);
                if (l > Long.MIN_VALUE) {
                    response.putAscii('"').putISODateMillis(l).putAscii('"');
                }
                break;
            case ColumnType.TIMESTAMP:
                l = rec.getTimestamp(col);
                if (l > Long.MIN_VALUE) {
                    response.putAscii('"').putISODate(l).putAscii('"');
                }
                break;
            case ColumnType.SHORT:
                response.put(rec.getShort(col));
                break;
            case ColumnType.CHAR:
                char c = rec.getChar(col);
                if (c > 0) {
                    response.put(c);
                }
                break;
            case ColumnType.NULL:
            case ColumnType.BINARY:
            case ColumnType.RECORD:
                break;
            case ColumnType.STRING:
                putStringOrNull(response, rec.getStr(col));
                break;
            case ColumnType.SYMBOL:
                putStringOrNull(response, rec.getSym(col));
                break;
            case ColumnType.LONG256:
                rec.getLong256(col, response);
                break;
            case ColumnType.GEOBYTE:
                putGeoHashStringValue(response, rec.getGeoByte(col), type);
                break;
            case ColumnType.GEOSHORT:
                putGeoHashStringValue(response, rec.getGeoShort(col), type);
                break;
            case ColumnType.GEOINT:
                putGeoHashStringValue(response, rec.getGeoInt(col), type);
                break;
            case ColumnType.GEOLONG:
                putGeoHashStringValue(response, rec.getGeoLong(col), type);
                break;
            case ColumnType.UUID:
                putUuidOrNull(response, rec.getLong128Lo(col), rec.getLong128Hi(col));
                break;
            case ColumnType.LONG128:
                throw new UnsupportedOperationException();
            case ColumnType.IPv4:
                putIPv4Value(response, rec, col);
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
            TextQueryProcessorState state
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
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        headerJsonError(response);
        JsonQueryProcessorState.prepareExceptionJson(response, position, message, state.query);
    }

    private void syntaxError(
            HttpChunkedResponse response,
            TextQueryProcessorState state,
            FlyweightMessageContainer container
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        info(state).$("syntax-error [q=`").utf8(state.query)
                .$("`, at=").$(container.getPosition())
                .$(", message=`").$(container.getFlyweightMessage()).$('`').I$();
        sendException(response, container.getPosition(), container.getFlyweightMessage(), state);
    }

    protected void header(
            HttpChunkedResponse response,
            TextQueryProcessorState state,
            int statusCode
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(statusCode, CONTENT_TYPE_CSV);
        if (state.fileName != null && !state.fileName.isEmpty()) {
            response.headers().putAscii("Content-Disposition: attachment; filename=\"").put(state.fileName).putAscii(".csv\"").putEOL();
        } else {
            response.headers().putAscii("Content-Disposition: attachment; filename=\"questdb-query-").put(clock.getTicks()).putAscii(".csv\"").putEOL();
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
