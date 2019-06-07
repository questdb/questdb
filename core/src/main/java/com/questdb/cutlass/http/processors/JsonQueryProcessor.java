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

package com.questdb.cutlass.http.processors;

import com.questdb.cairo.CairoError;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cutlass.http.*;
import com.questdb.griffin.SqlCompiler;
import com.questdb.griffin.SqlException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.network.IODispatcher;
import com.questdb.network.IOOperation;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;

import java.io.Closeable;
import java.lang.ThreadLocal;
import java.util.concurrent.atomic.AtomicLong;

public class JsonQueryProcessor implements HttpRequestProcessor, Closeable {
    static final int QUERY_DATA_SUFFIX = 7;
    static final int QUERY_RECORD_SUFFIX = 6;
    static final int QUERY_RECORD_COLUMNS = 5;
    static final int QUERY_RECORD_START = 4;
    static final int QUERY_META_SUFFIX = 3;
    static final int QUERY_METADATA = 2;
    static final int QUERY_PREFIX = 1;
    // Factory cache is thread local due to possibility of factory being
    // close by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    static final ThreadLocal<AssociativeCache<RecordCursorFactory>> FACTORY_CACHE = ThreadLocal.withInitial(() -> new AssociativeCache<>(8, 8));
    private static final LocalValue<JsonQueryProcessorState> LV = new LocalValue<>();
    private static final Log LOG = LogFactory.getLog(JsonQueryProcessor.class);
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final SqlCompiler compiler;

    public JsonQueryProcessor(CairoEngine engine) {
        // todo: add scheduler
        this.compiler = new SqlCompiler(engine);
    }

    private static void putValue(HttpChunkedResponseSocket socket, int type, Record rec, int col) {
        switch (type) {
            case ColumnType.BOOLEAN:
                socket.put(rec.getBool(col));
                break;
            case ColumnType.BYTE:
                socket.put(rec.getByte(col));
                break;
            case ColumnType.DOUBLE:
                socket.put(rec.getDouble(col), 10);
                break;
            case ColumnType.FLOAT:
                socket.put(rec.getFloat(col), 10);
                break;
            case ColumnType.INT:
                final int i = rec.getInt(col);
                if (i == Integer.MIN_VALUE) {
                    socket.put("null");
                } else {
                    Numbers.append(socket, i);
                }
                break;
            case ColumnType.LONG:
                final long l = rec.getLong(col);
                if (l == Long.MIN_VALUE) {
                    socket.put("null");
                } else {
                    socket.put(l);
                }
                break;
            case ColumnType.DATE:
                final long d = rec.getDate(col);
                if (d == Long.MIN_VALUE) {
                    socket.put("null");
                    break;
                }
                socket.put('"').putISODateMillis(d).put('"');
                break;
            case ColumnType.TIMESTAMP:
                final long t = rec.getTimestamp(col);
                if (t == Long.MIN_VALUE) {
                    socket.put("null");
                    break;
                }
                socket.put('"').putISODate(t).put('"');
                break;
            case ColumnType.SHORT:
                socket.put(rec.getShort(col));
                break;
            case ColumnType.STRING:
                putStringOrNull(socket, rec.getStr(col));
                break;
            case ColumnType.SYMBOL:
                putStringOrNull(socket, rec.getSym(col));
                break;
            case ColumnType.BINARY:
                socket.put('[');
                socket.put(']');
                break;
            default:
                assert false;
        }
    }

    private static void putStringOrNull(CharSink r, CharSequence str) {
        if (str == null) {
            r.put("null");
        } else {
            r.encodeUtf8AndQuote(str);
        }
    }

    @Override
    public void close() {
        Misc.free(compiler);
        Misc.free(FACTORY_CACHE.get());
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
    }

    @Override
    public void resumeRecv(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) {
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        JsonQueryProcessorState state = LV.get(context);
        if (state == null) {
            // todo: configure state externally
            LV.set(context, state = new JsonQueryProcessorState(context.getFd(), 1000));
        }
        HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        if (parseUrl(socket, context.getRequestHeader(), state)) {
            execute(context, dispatcher, state, socket);
        } else {
            readyForNextRequest(context, dispatcher);
        }
    }

    public boolean parseUrl(
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
        state.fetchAll = Chars.equalsNc("true", request.getUrlParam("count"));
        return true;
    }

    public void execute(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            JsonQueryProcessorState state,
            HttpChunkedResponseSocket socket
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        try {
            state.recordCursorFactory = FACTORY_CACHE.get().poll(state.query);
            int retryCount = 0;
            do {
                if (state.recordCursorFactory == null) {
                    state.recordCursorFactory = compiler.compile(state.query);
                    cacheHits.incrementAndGet();
                    info(state).$("execute-new [q=`").$(state.query).
                            $("`, skip: ").$(state.skip).
                            $(", stop: ").$(state.stop == Long.MAX_VALUE ? "MAX" : state.stop).
                            $(']').$();
                } else {
                    cacheMisses.incrementAndGet();
                    info(state).$("execute-cached [q=`").$(state.query).
                            $("`, skip: ").$(state.skip).
                            $(", stop: ").$(state.stop == Long.MAX_VALUE ? "MAX" : state.stop).
                            $(']').$();
                }

                if (state.recordCursorFactory != null) {
                    try {
                        state.cursor = state.recordCursorFactory.getCursor();
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
                            FACTORY_CACHE.get().put(state.query.toString(), null);
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

    private void sendConfirmation(HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // todo: test what happens when we cannot send a simple confirmation out
        //  it looks like on retry "send" we don't even handle this case unless the whole
        //  of the confirmation fits nicely into send buffer
        //  what happens when it doesn't?

        socket.put('{').putQuoted("ddl").put(':').putQuoted("OK").put('}');
        socket.sendChunk();
        socket.done();
    }

    private void internalError(
            HttpChunkedResponseSocket socket,
            Throwable e,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        error(state).$("Server error executing query ").$(state.query).$(e).$();
        sendException(socket, 0, e.getMessage(), 500, state.query);
    }


    protected void header(
            HttpChunkedResponseSocket socket,
            int status
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // todo: same issue as with sending confirmation
        //  what happens when header doesn't fit in buffer or there is "slow" client
        //  half way sending header
        socket.status(status, "application/json; charset=utf-8");
        // todo: configure this header externally
        socket.headers().put("Keep-Alive: timeout=5, max=10000").put(Misc.EOL);
        socket.sendHeader();
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

    private LogRecord error(JsonQueryProcessorState state) {
        return LOG.error().$('[').$(state.fd).$("] ");
    }

    private LogRecord info(JsonQueryProcessorState state) {
        return LOG.info().$('[').$(state.fd).$("] ");
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
                    case QUERY_PREFIX:
                        if (state.noMeta) {
                            socket.put('{').putQuoted("dataset").put(":[");
                            state.queryState = QUERY_RECORD_START;
                            break;
                        }
                        socket.bookmark();
                        socket.put('{').putQuoted("query").put(':').encodeUtf8AndQuote(state.query);
                        socket.put(',').putQuoted("columns").put(':').put('[');
                        state.queryState = QUERY_METADATA;
                        state.columnIndex = 0;
                        // fall through
                    case QUERY_METADATA:
                        for (; state.columnIndex < columnCount; state.columnIndex++) {

                            socket.bookmark();

                            if (state.columnIndex > 0) {
                                socket.put(',');
                            }
                            socket.put('{').
                                    putQuoted("name").put(':').putQuoted(state.metadata.getColumnName(state.columnIndex)).
                                    put(',').
                                    putQuoted("type").put(':').putQuoted(ColumnType.nameOf(state.metadata.getColumnType(state.columnIndex)));
                            socket.put('}');
                        }
                        state.queryState = QUERY_META_SUFFIX;
                        // fall through
                    case QUERY_META_SUFFIX:
                        socket.bookmark();
                        socket.put("],\"dataset\":[");
                        state.queryState = QUERY_RECORD_START;
                        // fall through
                    case QUERY_RECORD_START:

                        if (state.record == null) {
                            // check if cursor has any records
                            state.record = state.cursor.getRecord();
                            while (true) {
                                if (state.cursor.hasNext()) {
                                    state.count++;

                                    if (state.fetchAll && state.count > state.stop) {
                                        state.cancellationHandler.check();
                                        continue;
                                    }

                                    if (state.count > state.skip) {
                                        break;
                                    }
                                } else {
                                    state.queryState = QUERY_DATA_SUFFIX;
                                    break SWITCH;
                                }
                            }
                        }

                        if (state.count > state.stop) {
                            state.queryState = QUERY_DATA_SUFFIX;
                            break;
                        }

                        socket.bookmark();
                        if (state.count > state.skip + 1) {
                            socket.put(',');
                        }
                        socket.put('[');

                        state.queryState = QUERY_RECORD_COLUMNS;
                        state.columnIndex = 0;
                        // fall through
                    case QUERY_RECORD_COLUMNS:

                        for (; state.columnIndex < columnCount; state.columnIndex++) {
                            socket.bookmark();
                            if (state.columnIndex > 0) {
                                socket.put(',');
                            }
                            putValue(socket, state.metadata.getColumnType(state.columnIndex), state.record, state.columnIndex);
                        }

                        state.queryState = QUERY_RECORD_SUFFIX;
                        // fall through

                    case QUERY_RECORD_SUFFIX:
                        socket.bookmark();
                        socket.put(']');
                        state.record = null;
                        state.queryState = QUERY_RECORD_START;
                        break;
                    case QUERY_DATA_SUFFIX:
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

    private void readyForNextRequest(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher) {
        LOG.debug().$("all sent [fd=").$(context.getFd()).$(']').$();
        context.clear();
        dispatcher.registerChannel(context, IOOperation.READ);
    }

    long getCacheHits() {
        return cacheHits.longValue();
    }

    long getCacheMisses() {
        return cacheMisses.longValue();
    }

    private void sendDone(
            HttpChunkedResponseSocket socket,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.count > -1) {
            socket.bookmark();
            socket.put(']');
            socket.put(',').putQuoted("count").put(':').put(state.count);
            socket.put('}');
            state.count = -1;
            socket.sendChunk();
        }
        socket.done();
    }
}
