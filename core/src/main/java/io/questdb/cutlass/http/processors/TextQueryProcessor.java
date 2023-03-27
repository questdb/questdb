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
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.text.TextUtil;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectByteCharSequence;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class TextQueryProcessor implements HttpRequestProcessor, Closeable {

    private static final Log LOG = LogFactory.getLog(TextQueryProcessor.class);
    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    private static final LocalValue<TextQueryProcessorState> LV = new LocalValue<>();
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final MillisecondClock clock;
    private final SqlCompiler compiler;
    private final JsonQueryProcessorConfiguration configuration;
    private final int doubleScale;
    private final int floatScale;
    private final Metrics metrics;
    private final SqlExecutionContextImpl sqlExecutionContext;

    @TestOnly
    public TextQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount
    ) {
        this(configuration, engine, workerCount, workerCount, null, null);
    }

    public TextQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount,
            int sharedWorkerCount,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent
    ) {
        this.configuration = configuration;
        this.compiler = new SqlCompiler(engine, functionFactoryCache, snapshotAgent);
        this.floatScale = configuration.getFloatScale();
        this.clock = configuration.getClock();
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount, sharedWorkerCount);
        this.doubleScale = configuration.getDoubleScale();
        this.circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB4);
        this.metrics = engine.getMetrics();
    }

    @Override
    public void close() {
        Misc.free(compiler);
        Misc.free(circuitBreaker);
    }

    public void execute(
            HttpConnectionContext context,
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        try {
            boolean isExpRequest = isExpUrl(context.getRequestHeader().getUrl());

            circuitBreaker.resetTimer();
            state.recordCursorFactory = QueryCache.getThreadLocalInstance().poll(state.query);
            state.setQueryCacheable(true);
            sqlExecutionContext.with(
                    context.getCairoSecurityContext(),
                    null,
                    null,
                    context.getFd(),
                    circuitBreaker.of(context.getFd())
            );
            if (state.recordCursorFactory == null) {
                final CompiledQuery cc = compiler.compile(state.query, sqlExecutionContext);
                if (cc.getType() == CompiledQuery.SELECT || cc.getType() == CompiledQuery.EXPLAIN) {
                    state.recordCursorFactory = cc.getRecordCursorFactory();
                } else if (isExpRequest) {
                    throw SqlException.$(0, "/exp endpoint only accepts SELECT");
                }
                info(state).$("execute-new [q=`").utf8(state.query)
                        .$("`, skip: ").$(state.skip)
                        .$(", stop: ").$(state.stop)
                        .I$();
                sqlExecutionContext.storeTelemetry(cc.getType(), TelemetryOrigin.HTTP_TEXT);
            } else {
                info(state).$("execute-cached [q=`").utf8(state.query)
                        .$("`, skip: ").$(state.skip)
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
                            if (retries == TableReferenceOutOfDateException.MAX_RETRY_ATTEMPS) {
                                throw e;
                            }
                            info(state).$(e.getFlyweightMessage()).$();
                            state.recordCursorFactory = Misc.free(state.recordCursorFactory);
                            final CompiledQuery cc = compiler.compile(state.query, sqlExecutionContext);
                            if (cc.getType() != CompiledQuery.SELECT && isExpRequest) {
                                throw SqlException.$(0, "/exp endpoint only accepts SELECT");
                            }

                            state.recordCursorFactory = cc.getRecordCursorFactory();
                        }
                    }
                    state.metadata = state.recordCursorFactory.getMetadata();
                    header(context.getChunkedResponseSocket(), state, 200);
                    doResumeSend(context);
                } catch (CairoException e) {
                    state.setQueryCacheable(e.isCacheable());
                    internalError(context.getChunkedResponseSocket(), context.getLastRequestBytesSent(), e, state);
                } catch (CairoError e) {
                    internalError(context.getChunkedResponseSocket(), context.getLastRequestBytesSent(), e, state);
                }
            } else {
                headerNoContentDisposition(context.getChunkedResponseSocket());
                sendConfirmation(context.getChunkedResponseSocket());
                readyForNextRequest(context);
            }
        } catch (SqlException | ImplicitCastException e) {
            syntaxError(context.getChunkedResponseSocket(), state, e);
            readyForNextRequest(context);
        } catch (CairoException | CairoError e) {
            internalError(context.getChunkedResponseSocket(), context.getLastRequestBytesSent(), e, state);
            readyForNextRequest(context);
        }
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

        HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        if (parseUrl(socket, context.getRequestHeader(), state)) {
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

    private static boolean isExpUrl(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == '/'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i) | 32) == 'p';
    }

    private static void putGeoHashStringValue(HttpChunkedResponseSocket socket, long value, int type) {
        if (value == GeoHashes.NULL) {
            socket.put("null");
        } else {
            int bitFlags = GeoHashes.getBitFlags(type);
            socket.put('\"');
            if (bitFlags < 0) {
                GeoHashes.appendCharsUnsafe(value, -bitFlags, socket);
            } else {
                GeoHashes.appendBinaryStringUnsafe(value, bitFlags, socket);
            }
            socket.put('\"');
        }
    }

    private static void putStringOrNull(CharSink r, CharSequence str) {
        if (str != null) {
            r.encodeUtf8AndQuote(str);
        }
    }

    private static void putUuidOrNull(HttpChunkedResponseSocket socket, long lo, long hi) {
        if (Uuid.isNull(lo, hi)) {
            return;
        }
        Numbers.appendUuid(lo, hi, socket);
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
        if (state == null || state.cursor == null) {
            return;
        }

        // copy random during query resume
        sqlExecutionContext.with(context.getCairoSecurityContext(), null, state.rnd, context.getFd(), circuitBreaker.of(context.getFd()));
        LOG.debug().$("resume [fd=").$(context.getFd()).I$();

        if (!state.pausedQuery) {
            context.resumeResponseSend();
        } else {
            state.pausedQuery = false;
        }

        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
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
                                socket.put(state.delimiter);
                            }
                            socket.putQuoted(state.metadata.getColumnName(state.columnIndex));
                            state.columnIndex++;
                            socket.bookmark();
                        }
                        socket.put(Misc.EOL);
                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_START;
                        socket.bookmark();
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
                                socket.put(state.delimiter);
                            }
                            putValue(socket, state.metadata.getColumnType(state.columnIndex), state.record, state.columnIndex);
                            state.columnIndex++;
                            socket.bookmark();
                        }

                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_SUFFIX;
                        // fall through
                    case JsonQueryProcessorState.QUERY_RECORD_SUFFIX:
                        socket.put(Misc.EOL);
                        state.record = null;
                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_START;
                        socket.bookmark();
                        break;
                    case JsonQueryProcessorState.QUERY_SUFFIX:
                        sendDone(socket, state);
                        break OUT;
                    default:
                        break OUT;
                }
            } catch (DataUnavailableException e) {
                socket.resetToBookmark();
                throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                if (socket.resetToBookmark()) {
                    socket.sendChunk(false);
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

    private LogRecord info(TextQueryProcessorState state) {
        return LOG.info().$('[').$(state.getFd()).$("] ");
    }

    private void internalError(
            HttpChunkedResponseSocket socket,
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
        sendException(socket, 0, e.getMessage(), state);
    }

    private void logInternalError(Throwable e, TextQueryProcessorState state) {
        critical(state).$("Server error executing query ").utf8(state.query).$(e).$();
        // This is a critical error, so we treat it as an unhandled one.
        metrics.health().incrementUnhandledErrors();
    }

    private boolean parseUrl(
            HttpChunkedResponseSocket socket,
            HttpRequestHeader request,
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Query text.
        final DirectByteCharSequence query = request.getUrlParam("query");
        if (query == null || query.length() == 0) {
            info(state).$("Empty query request received. Sending empty reply.").$();
            sendException(socket, 0, "No query text", state);
            return false;
        }

        // URL params.
        long skip = 0;
        long stop = Long.MAX_VALUE;

        CharSequence limit = request.getUrlParam("limit");
        if (limit != null) {
            int sepPos = Chars.indexOf(limit, ',');
            try {
                if (sepPos > 0) {
                    skip = Numbers.parseLong(limit, 0, sepPos);
                    if (sepPos + 1 < limit.length()) {
                        stop = Numbers.parseLong(limit, sepPos + 1, limit.length());
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
        try {
            TextUtil.utf8Decode(query.getLo(), query.getHi(), state.query);
        } catch (Utf8Exception e) {
            info(state).$("Bad UTF8 encoding").$();
            sendException(socket, 0, "Bad UTF8 encoding in query text", state);
            return false;
        }
        CharSequence fileName = request.getUrlParam("filename");
        state.fileName = null;
        if (fileName != null && fileName.length() > 0) {
            state.fileName = fileName.toString();
        }

        DirectByteCharSequence delimiter = request.getUrlParam("delimiter");
        state.delimiter = ',';

        if (delimiter != null && delimiter.length() == 1) {
            state.delimiter = delimiter.charAt(0);
        }

        state.skip = skip;
        state.count = 0L;
        state.stop = stop;
        state.noMeta = Chars.equalsNc("true", request.getUrlParam("nm"));
        state.countRows = Chars.equalsNc("true", request.getUrlParam("count"));
        return true;
    }

    private void putValue(HttpChunkedResponseSocket socket, int type, Record rec, int col) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
                socket.put(rec.getBool(col));
                break;
            case ColumnType.BYTE:
                socket.put(rec.getByte(col));
                break;
            case ColumnType.DOUBLE:
                double d = rec.getDouble(col);
                if (d == d) {
                    socket.put(d, doubleScale);
                }
                break;
            case ColumnType.FLOAT:
                float f = rec.getFloat(col);
                if (f == f) {
                    socket.put(f, floatScale);
                }
                break;
            case ColumnType.INT:
                final int i = rec.getInt(col);
                if (i > Integer.MIN_VALUE) {
                    Numbers.append(socket, i);
                }
                break;
            case ColumnType.LONG:
                long l = rec.getLong(col);
                if (l > Long.MIN_VALUE) {
                    socket.put(l);
                }
                break;
            case ColumnType.DATE:
                l = rec.getDate(col);
                if (l > Long.MIN_VALUE) {
                    socket.put('"').putISODateMillis(l).put('"');
                }
                break;
            case ColumnType.TIMESTAMP:
                l = rec.getTimestamp(col);
                if (l > Long.MIN_VALUE) {
                    socket.put('"').putISODate(l).put('"');
                }
                break;
            case ColumnType.SHORT:
                socket.put(rec.getShort(col));
                break;
            case ColumnType.CHAR:
                char c = rec.getChar(col);
                if (c > 0) {
                    socket.put(c);
                }
                break;
            case ColumnType.NULL:
            case ColumnType.BINARY:
            case ColumnType.RECORD:
                break;
            case ColumnType.STRING:
                putStringOrNull(socket, rec.getStr(col));
                break;
            case ColumnType.SYMBOL:
                putStringOrNull(socket, rec.getSym(col));
                break;
            case ColumnType.LONG256:
                rec.getLong256(col, socket);
                break;
            case ColumnType.GEOBYTE:
                putGeoHashStringValue(socket, rec.getGeoByte(col), type);
                break;
            case ColumnType.GEOSHORT:
                putGeoHashStringValue(socket, rec.getGeoShort(col), type);
                break;
            case ColumnType.GEOINT:
                putGeoHashStringValue(socket, rec.getGeoInt(col), type);
                break;
            case ColumnType.GEOLONG:
                putGeoHashStringValue(socket, rec.getGeoLong(col), type);
                break;
            case ColumnType.UUID:
                putUuidOrNull(socket, rec.getLong128Lo(col), rec.getLong128Hi(col));
                break;
            case ColumnType.LONG128:
                throw new UnsupportedOperationException();
            default:
                assert false;
        }
    }

    private void sendConfirmation(HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.put("DDL Success\n");
        socket.sendChunk(true);
    }

    private void sendDone(
            HttpChunkedResponseSocket socket,
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.count > -1) {
            state.count = -1;
            socket.sendChunk(true);
            return;
        }
        socket.done();
    }

    private void sendException(
            HttpChunkedResponseSocket socket,
            int position,
            CharSequence message,
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        headerJsonError(socket);
        JsonQueryProcessorState.prepareExceptionJson(socket, position, message, state.query);
    }

    private void syntaxError(
            HttpChunkedResponseSocket socket,
            TextQueryProcessorState state,
            FlyweightMessageContainer container
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        info(state).$("syntax-error [q=`").utf8(state.query)
                .$("`, at=").$(container.getPosition())
                .$(", message=`").$(container.getFlyweightMessage()).$('`').I$();
        sendException(socket, container.getPosition(), container.getFlyweightMessage(), state);
    }

    protected void header(
            HttpChunkedResponseSocket socket,
            TextQueryProcessorState state,
            int statusCode
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(statusCode, "text/csv; charset=utf-8");
        if (state.fileName != null && state.fileName.length() > 0) {
            socket.headers().put("Content-Disposition: attachment; filename=\"").put(state.fileName).put(".csv\"").put(Misc.EOL);
        } else {
            socket.headers().put("Content-Disposition: attachment; filename=\"questdb-query-").put(clock.getTicks()).put(".csv\"").put(Misc.EOL);
        }
        socket.headers().setKeepAlive(configuration.getKeepAliveHeader());
        socket.sendHeader();
    }

    protected void headerJsonError(HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(400, "application/json; charset=utf-8");
        socket.headers().setKeepAlive(configuration.getKeepAliveHeader());
        socket.sendHeader();
    }

    protected void headerNoContentDisposition(HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(200, "text/csv; charset=utf-8");
        socket.headers().setKeepAlive(configuration.getKeepAliveHeader());
        socket.sendHeader();
    }
}
