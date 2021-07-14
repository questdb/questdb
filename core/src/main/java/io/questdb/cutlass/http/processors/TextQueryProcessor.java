/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.MessageBus;
import io.questdb.Telemetry;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.text.TextUtil;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectByteCharSequence;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class TextQueryProcessor implements HttpRequestProcessor, Closeable {
    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    private static final LocalValue<TextQueryProcessorState> LV = new LocalValue<>();
    private static final Log LOG = LogFactory.getLog(TextQueryProcessor.class);
    private final SqlCompiler compiler;
    private final JsonQueryProcessorConfiguration configuration;
    private final int floatScale;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final MillisecondClock clock;
    private final int doubleScale;
    private final HttpSqlExecutionInterruptor interruptor;

    public TextQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            @Nullable MessageBus messageBus,
            int workerCount
    ) {
        this(configuration, engine, messageBus, workerCount, null);
    }

    public TextQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            @Nullable MessageBus messageBus,
            int workerCount,
            @Nullable FunctionFactoryCache functionFactoryCache
    ) {
        this.configuration = configuration;
        this.compiler = new SqlCompiler(engine, messageBus, functionFactoryCache);
        this.floatScale = configuration.getFloatScale();
        this.clock = configuration.getClock();
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount, messageBus);
        this.doubleScale = configuration.getDoubleScale();
        this.interruptor = new HttpSqlExecutionInterruptor(configuration.getInterruptorConfiguration());
    }

    @Override
    public void close() {
        Misc.free(compiler);
        Misc.free(interruptor);
    }

    public void execute(
            HttpConnectionContext context,
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        try {
            state.recordCursorFactory = QueryCache.getInstance().poll(state.query);
            state.setQueryCacheable(true);
            sqlExecutionContext.with(context.getCairoSecurityContext(), null, null, context.getFd(), interruptor.of(context.getFd()));
            if (state.recordCursorFactory == null) {
                final CompiledQuery cc = compiler.compile(state.query, sqlExecutionContext);
                if (cc.getType() == CompiledQuery.SELECT) {
                    state.recordCursorFactory = cc.getRecordCursorFactory();
                }
                info(state).$("execute-new [q=`").utf8(state.query).
                        $("`, skip: ").$(state.skip).
                        $(", stop: ").$(state.stop).
                        $(']').$();
                sqlExecutionContext.storeTelemetry(cc.getType(), Telemetry.ORIGIN_HTTP_TEXT);
            } else {
                info(state).$("execute-cached [q=`").utf8(state.query).
                        $("`, skip: ").$(state.skip).
                        $(", stop: ").$(state.stop).
                        $(']').$();
                sqlExecutionContext.storeTelemetry(CompiledQuery.SELECT, Telemetry.ORIGIN_HTTP_TEXT);
            }

            if (state.recordCursorFactory != null) {
                try {
                    state.cursor = state.recordCursorFactory.getCursor(sqlExecutionContext);
                    state.metadata = state.recordCursorFactory.getMetadata();
                    header(context.getChunkedResponseSocket(), state);
                    resumeSend(context);
                } catch (CairoException e) {
                    state.setQueryCacheable(e.isCacheable());
                    internalError(context.getChunkedResponseSocket(), e, state);
                } catch (CairoError e) {
                    internalError(context.getChunkedResponseSocket(), e, state);
                }
            } else {
                headerNoContentDisposition(context.getChunkedResponseSocket());
                sendConfirmation(context.getChunkedResponseSocket());
                readyForNextRequest(context);
            }
        } catch (SqlException e) {
            syntaxError(context.getChunkedResponseSocket(), e, state);
            readyForNextRequest(context);
        } catch (CairoException | CairoError e) {
            internalError(context.getChunkedResponseSocket(), e, state);
            readyForNextRequest(context);
        }
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
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
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        TextQueryProcessorState state = LV.get(context);
        if (state == null || state.cursor == null) {
            return;
        }

        // copy random during query resume
        sqlExecutionContext.with(context.getCairoSecurityContext(), null, state.rnd, context.getFd(), interruptor.of(context.getFd()));
        LOG.debug().$("resume [fd=").$(context.getFd()).$(']').$();

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
                        for (; state.columnIndex < columnCount; state.columnIndex++) {

                            socket.bookmark();
                            if (state.columnIndex > 0) {
                                socket.put(',');
                            }
                            socket.putQuoted(state.metadata.getColumnName(state.columnIndex));
                        }
                        socket.put(Misc.EOL);
                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_START;
                        // fall through
                    case JsonQueryProcessorState.QUERY_RECORD_START:

                        if (state.record == null) {
                            // check if cursor has any records
                            state.record = state.cursor.getRecord();
                            while (true) {
                                if (state.cursor.hasNext()) {
                                    state.count++;

                                    if (state.countRows && state.count > state.stop) {
//                                        state.cancellationHandler.check();
                                        continue;
                                    }

                                    if (state.count > state.skip) {
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

                        for (; state.columnIndex < columnCount; state.columnIndex++) {
                            socket.bookmark();
                            if (state.columnIndex > 0) {
                                socket.put(',');
                            }
                            putValue(socket, state.metadata.getColumnType(state.columnIndex), state.record, state.columnIndex);
                        }

                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_SUFFIX;
                        // fall through

                    case JsonQueryProcessorState.QUERY_RECORD_SUFFIX:
                        socket.bookmark();
                        socket.put(Misc.EOL);
                        state.record = null;
                        state.queryState = JsonQueryProcessorState.QUERY_RECORD_START;
                        break;
                    case JsonQueryProcessorState.QUERY_SUFFIX:
                        sendDone(socket, state);
                        break OUT;
                    default:
                        break OUT;
                }
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

    @Override
    public void parkRequest(HttpConnectionContext context) {
        TextQueryProcessorState state = LV.get(context);
        if (state != null) {
            state.rnd = sqlExecutionContext.getRandom();
        }
    }

    private static void putStringOrNull(CharSink r, CharSequence str) {
        if (str != null) {
            r.encodeUtf8AndQuote(str);
        }
    }

    private static void readyForNextRequest(HttpConnectionContext context) {
        LOG.info().$("all sent [fd=").$(context.getFd()).$(", lastRequestBytesSent=").$(context.getLastRequestBytesSent()).$(", nCompletedRequests=").$(context.getNCompletedRequests() + 1)
                .$(", totalBytesSent=").$(context.getTotalBytesSent()).$(']').$();
    }

    private LogRecord error(TextQueryProcessorState state) {
        return LOG.error().$('[').$(state.getFd()).$("] ");
    }

    protected void header(HttpChunkedResponseSocket socket, TextQueryProcessorState state) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(200, "text/csv; charset=utf-8");
        if (state.fileName != null && state.fileName.length() > 0) {
            socket.headers().put("Content-Disposition: attachment; filename=\"").put(state.fileName).put(".csv\"").put(Misc.EOL);
        } else {
            socket.headers().put("Content-Disposition: attachment; filename=\"questdb-query-").put(clock.getTicks()).put(".csv\"").put(Misc.EOL);
        }

        socket.headers().setKeepAlive(configuration.getKeepAliveHeader());
        socket.sendHeader();
    }

    protected void headerNoContentDisposition(HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(200, "text/csv; charset=utf-8");
        socket.headers().setKeepAlive(configuration.getKeepAliveHeader());
        socket.sendHeader();
    }

    private LogRecord info(TextQueryProcessorState state) {
        return LOG.info().$('[').$(state.getFd()).$("] ");
    }

    private void internalError(
            HttpChunkedResponseSocket socket,
            Throwable e,
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        error(state).$("Server error executing query ").utf8(state.query).$(e).$();
        sendException(socket, 0, e.getMessage(), state);
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

        // Url Params.
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
        state.skip = skip;
        state.count = 0L;
        state.stop = stop;
        state.noMeta = Chars.equalsNc("true", request.getUrlParam("nm"));
        state.countRows = Chars.equalsNc("true", request.getUrlParam("count"));
        return true;
    }

    private void putValue(HttpChunkedResponseSocket socket, int type, Record rec, int col) {
        switch (type) {
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
                break;
            case ColumnType.STRING:
                putStringOrNull(socket, rec.getStr(col));
                break;
            case ColumnType.SYMBOL:
                putStringOrNull(socket, rec.getSym(col));
                break;
            case ColumnType.BINARY:
                break;
            case ColumnType.LONG256:
                rec.getLong256(col, socket);
                break;
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
        header(socket, state);
        JsonQueryProcessorState.prepareExceptionJson(socket, position, message, state.query);
    }

    private void syntaxError(
            HttpChunkedResponseSocket socket,
            SqlException sqlException,
            TextQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        info(state)
                .$("syntax-error [q=`").utf8(state.query)
                .$("`, at=").$(sqlException.getPosition())
                .$(", message=`").$(sqlException.getFlyweightMessage()).$('`')
                .$(']').$();
        sendException(socket, sqlException.getPosition(), sqlException.getFlyweightMessage(), state);
    }
}
