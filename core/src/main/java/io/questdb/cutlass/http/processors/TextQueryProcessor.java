/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.time.MillisecondClock;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

public class TextQueryProcessor implements HttpRequestProcessor, Closeable {
    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    private static final LocalValue<JsonQueryProcessorState> LV = new LocalValue<>();
    private static final Log LOG = LogFactory.getLog(TextQueryProcessor.class);
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final SqlCompiler compiler;
    private final JsonQueryProcessorConfiguration configuration;
    private final int floatScale;
    private final int doubleScale;
    private final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl();
    private final MillisecondClock clock;

    public TextQueryProcessor(JsonQueryProcessorConfiguration configuration, CairoEngine engine) {
        // todo: add scheduler
        this.configuration = configuration;
        this.compiler = new SqlCompiler(engine);
        this.floatScale = configuration.getFloatScale();
        this.doubleScale = configuration.getDoubleScale();
        this.clock = configuration.getClock();
    }

    private static void putStringOrNull(CharSink r, CharSequence str) {
        if (str != null) {
            r.encodeUtf8AndQuote(str);
        }
    }

    @Override
    public void close() {
        Misc.free(compiler);
    }

    public void execute(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            JsonQueryProcessorState state,
            HttpChunkedResponseSocket socket
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        try {
            state.recordCursorFactory = AbstractQueryContext.FACTORY_CACHE.get().poll(state.query);
            int retryCount = 0;
            do {
                sqlExecutionContext.with(context.getCairoSecurityContext(), null);
                if (state.recordCursorFactory == null) {
                    state.recordCursorFactory = compiler.compile(state.query, sqlExecutionContext);
                    cacheHits.incrementAndGet();
                    info(state).$("execute-new [q=`").$(state.query).
                            $("`, skip: ").$(state.skip).
                            $(", stop: ").$(state.stop).
                            $(']').$();
                } else {
                    cacheMisses.incrementAndGet();
                    info(state).$("execute-cached [q=`").$(state.query).
                            $("`, skip: ").$(state.skip).
                            $(", stop: ").$(state.stop).
                            $(']').$();
                }

                if (state.recordCursorFactory != null) {
                    try {
                        state.cursor = state.recordCursorFactory.getCursor(sqlExecutionContext);
                        state.metadata = state.recordCursorFactory.getMetadata();
                        header(socket, 200);
                        resumeSend(context, dispatcher);
                        break;
                    } catch (CairoError | CairoException e) {
                        // todo: investigate why we need to keep retrying to execute query when it is failing
                        //  perhaps this is unnecessary because we don't even check the type of error it is
                        //  we could be having severe hardware issues and continue trying
                        if (retryCount == 0) {
                            // todo: we want to clear cache, no need to create string to achieve this
                            AbstractQueryContext.FACTORY_CACHE.get().put(state.query.toString(), null);
                            state.recordCursorFactory = null;
                            LOG.error().$("RecordSource execution failed. ").$(e.getMessage()).$(". Retrying ...").$();
                            retryCount++;
                        } else {
                            internalError(socket, e, state);
                            break;
                        }
                    }
                } else {
                    header(socket, 200);
                    sendConfirmation(socket);
                    readyForNextRequest(context, dispatcher);
                    break;
                }
            } while (true);
        } catch (SqlException e) {
            syntaxError(socket, e, state);
            readyForNextRequest(context, dispatcher);
        } catch (CairoException | CairoError e) {
            internalError(socket, e, state);
            readyForNextRequest(context, dispatcher);
        }
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        JsonQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new JsonQueryProcessorState(context.getFd(), configuration.getConnectionCheckFrequency()));
        }
        HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        if (parseUrl(socket, context.getRequestHeader(), state)) {
            execute(context, dispatcher, state, socket);
        } else {
            readyForNextRequest(context, dispatcher);
        }
    }

    @Override
    public void resumeRecv(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) {
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        JsonQueryProcessorState state = LV.get(context);
        if (state == null || state.cursor == null) {
            return;
        }

        LOG.debug().$("resume [fd=").$(context.getFd()).$(']').$();

        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        final int columnCount = state.metadata.getColumnCount();

        OUT:
        while (true) {
            try {
                SWITCH:
                switch (state.queryState) {
                    case AbstractQueryContext.QUERY_PREFIX:
                    case AbstractQueryContext.QUERY_METADATA:
                        state.columnIndex = 0;
                        state.queryState = AbstractQueryContext.QUERY_METADATA;
                        for (; state.columnIndex < columnCount; state.columnIndex++) {

                            socket.bookmark();
                            if (state.columnIndex > 0) {
                                socket.put(',');
                            }
                            socket.putQuoted(state.metadata.getColumnName(state.columnIndex));
                        }
                        socket.put(Misc.EOL);
                        state.queryState = AbstractQueryContext.QUERY_RECORD_START;
                        // fall through
                    case AbstractQueryContext.QUERY_RECORD_START:

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
                                    state.queryState = AbstractQueryContext.QUERY_SUFFIX;
                                    break SWITCH;
                                }
                            }
                        }

                        if (state.count > state.stop) {
                            state.queryState = AbstractQueryContext.QUERY_SUFFIX;
                            break;
                        }

                        state.queryState = AbstractQueryContext.QUERY_RECORD;
                        state.columnIndex = 0;
                        // fall through
                    case AbstractQueryContext.QUERY_RECORD:

                        for (; state.columnIndex < columnCount; state.columnIndex++) {
                            socket.bookmark();
                            if (state.columnIndex > 0) {
                                socket.put(',');
                            }
                            putValue(socket, state.metadata.getColumnType(state.columnIndex), state.record, state.columnIndex);
                        }

                        state.queryState = AbstractQueryContext.QUERY_RECORD_SUFFIX;
                        // fall through

                    case AbstractQueryContext.QUERY_RECORD_SUFFIX:
                        socket.bookmark();
                        socket.put(Misc.EOL);
                        state.record = null;
                        state.queryState = AbstractQueryContext.QUERY_RECORD_START;
                        break;
                    case AbstractQueryContext.QUERY_SUFFIX:
                        sendDone(socket, state);
                        break OUT;
                    default:
                        break OUT;
                }
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                if (socket.resetToBookmark()) {
                    socket.sendChunk();
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
        readyForNextRequest(context, dispatcher);
    }

    private LogRecord error(JsonQueryProcessorState state) {
        return LOG.error().$('[').$(state.fd).$("] ");
    }

    long getCacheHits() {
        return cacheHits.longValue();
    }

    long getCacheMisses() {
        return cacheMisses.longValue();
    }

    protected void header(
            HttpChunkedResponseSocket socket,
            int status
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(status, "text/csv; charset=utf-8");
        socket.headers().put("Content-Disposition: attachment; filename=\"questdb-query-").put(clock.getTicks()).put(".csv\"").put(Misc.EOL);
        socket.headers().setKeepAlive(configuration.getKeepAliveHeader());
        socket.sendHeader();
    }

    private LogRecord info(JsonQueryProcessorState state) {
        return LOG.info().$('[').$(state.fd).$("] ");
    }

    private void internalError(
            HttpChunkedResponseSocket socket,
            Throwable e,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        error(state).$("Server error executing query ").$(state.query).$(e).$();
        sendException(socket, 0, e.getMessage(), 500, state.query);
    }

    private boolean parseUrl(
            HttpChunkedResponseSocket socket,
            HttpRequestHeader request,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Query text.
        final CharSequence query = request.getUrlParam("query");
        if (query == null || query.length() == 0) {
            info(state).$("Empty query request received. Sending empty reply.").$();
            sendException(socket, 0, "No query text", 400, state.query);
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

        state.query = query;
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

    private void readyForNextRequest(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) {
        LOG.debug().$("all sent [fd=").$(context.getFd()).$(']').$();
        context.clear();
        dispatcher.registerChannel(context, IOOperation.READ);
    }

    private void sendConfirmation(HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.put('{').putQuoted("ddl").put(':').putQuoted("OK").put('}');
        socket.sendChunk();
        socket.done();
    }

    private void sendDone(
            HttpChunkedResponseSocket socket,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.count > -1) {
            state.count = -1;
            socket.sendChunk();
        }
        socket.done();
    }

    private void sendException(
            HttpChunkedResponseSocket socket,
            int position,
            CharSequence message,
            int status,
            CharSequence query
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        header(socket, status);
        socket.put('{').
                putQuoted("query").put(':').encodeUtf8AndQuote(query == null ? "" : query).put(',').
                putQuoted("error").put(':').encodeUtf8AndQuote(message).put(',').
                putQuoted("position").put(':').put(position);
        socket.put('}');
        socket.sendChunk();
        socket.done();
    }

    private void syntaxError(
            HttpChunkedResponseSocket socket,
            SqlException sqlException,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        info(state)
                .$("syntax-error [q=`").$(state.query)
                .$("`, at=").$(sqlException.getPosition())
                .$(", message=`").$(sqlException.getFlyweightMessage()).$('`')
                .$(']').$();
        sendException(socket, sqlException.getPosition(), sqlException.getFlyweightMessage(), 400, state.query);
    }
}
