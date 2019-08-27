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

import com.questdb.cairo.CairoEngine;
import com.questdb.cairo.CairoError;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.Record;
import com.questdb.cutlass.http.HttpChunkedResponseSocket;
import com.questdb.cutlass.http.HttpConnectionContext;
import com.questdb.cutlass.http.HttpRequestHeader;
import com.questdb.cutlass.http.HttpRequestProcessor;
import com.questdb.griffin.SqlCompiler;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.SqlExecutionContextImpl;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.network.*;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

public class JsonQueryProcessor implements HttpRequestProcessor, Closeable {
    private static final LocalValue<JsonQueryProcessorState> LV = new LocalValue<>();
    private static final Log LOG = LogFactory.getLog(JsonQueryProcessor.class);
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final SqlCompiler compiler;
    private final JsonQueryProcessorConfiguration configuration;
    private final int floatScale;
    private final int doubleScale;
    private final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl();
    private final ObjList<ValueWriter> valueWriters = new ObjList<>();

    public JsonQueryProcessor(JsonQueryProcessorConfiguration configuration, CairoEngine engine) {
        // todo: add scheduler
        this.configuration = configuration;
        this.compiler = new SqlCompiler(engine);
        this.floatScale = configuration.getFloatScale();
        this.doubleScale = configuration.getDoubleScale();

        this.valueWriters.extendAndSet(ColumnType.BOOLEAN, this::putBooleanValue);
        this.valueWriters.extendAndSet(ColumnType.BYTE, this::putByteValue);
        this.valueWriters.extendAndSet(ColumnType.DOUBLE, this::putDoubleValue);
        this.valueWriters.extendAndSet(ColumnType.FLOAT, this::putFloatValue);
        this.valueWriters.extendAndSet(ColumnType.INT, this::putIntValue);
        this.valueWriters.extendAndSet(ColumnType.LONG, this::putLongValue);
        this.valueWriters.extendAndSet(ColumnType.DATE, this::putDateValue);
        this.valueWriters.extendAndSet(ColumnType.TIMESTAMP, this::putTimestampValue);
        this.valueWriters.extendAndSet(ColumnType.SHORT, this::putShortValue);
        this.valueWriters.extendAndSet(ColumnType.CHAR, this::putCharValue);
        this.valueWriters.extendAndSet(ColumnType.STRING, this::putStrValue);
        this.valueWriters.extendAndSet(ColumnType.SYMBOL, this::putSymValue);
        this.valueWriters.extendAndSet(ColumnType.BINARY, this::putBinValue);
        this.valueWriters.extendAndSet(ColumnType.LONG256, this::putLong256Value);
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
                        if (state.noMeta) {
                            socket.put('{').putQuoted("dataset").put(":[");
                            state.queryState = AbstractQueryContext.QUERY_RECORD_START;
                            break;
                        }
                        socket.bookmark();
                        socket.put('{').putQuoted("query").put(':').encodeUtf8AndQuote(state.query);
                        socket.put(',').putQuoted("columns").put(':').put('[');
                        state.queryState = AbstractQueryContext.QUERY_METADATA;
                        state.columnIndex = 0;
                        // fall through
                    case AbstractQueryContext.QUERY_METADATA:
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
                        state.queryState = AbstractQueryContext.QUERY_META_SUFFIX;
                        // fall through
                    case AbstractQueryContext.QUERY_META_SUFFIX:
                        socket.bookmark();
                        socket.put("],\"dataset\":[");
                        state.queryState = AbstractQueryContext.QUERY_RECORD_START;
                        // fall through
                    case AbstractQueryContext.QUERY_RECORD_START:

                        if (state.record == null) {
                            // check if cursor has any records
                            state.record = state.cursor.getRecord();
                            while (true) {
                                if (state.cursor.hasNext()) {
                                    state.count++;

                                    if (state.fetchAll && state.count > state.stop) {
//                                        state.cancellationHandler.check();
                                        continue;
                                    }

                                    if (state.count > state.skip) {
                                        break;
                                    }
                                } else {
                                    state.queryState = AbstractQueryContext.QUERY_DATA_SUFFIX;
                                    break SWITCH;
                                }
                            }
                        }

                        if (state.count > state.stop) {
                            state.queryState = AbstractQueryContext.QUERY_DATA_SUFFIX;
                            break;
                        }

                        socket.bookmark();
                        if (state.count > state.skip + 1) {
                            socket.put(',');
                        }
                        socket.put('[');

                        state.queryState = AbstractQueryContext.QUERY_RECORD_COLUMNS;
                        state.columnIndex = 0;
                        // fall through
                    case AbstractQueryContext.QUERY_RECORD_COLUMNS:

                        for (; state.columnIndex < columnCount; state.columnIndex++) {
                            socket.bookmark();
                            if (state.columnIndex > 0) {
                                socket.put(',');
                            }
                            final ValueWriter vw = valueWriters.getQuick(state.metadata.getColumnType(state.columnIndex));
                            if (vw != null) {
                                vw.write(socket, state.record, state.columnIndex);
                            }
                        }

                        state.queryState = AbstractQueryContext.QUERY_RECORD_SUFFIX;
                        // fall through

                    case AbstractQueryContext.QUERY_RECORD_SUFFIX:
                        socket.bookmark();
                        socket.put(']');
                        state.record = null;
                        state.queryState = AbstractQueryContext.QUERY_RECORD_START;
                        break;
                    case AbstractQueryContext.QUERY_DATA_SUFFIX:
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
        socket.status(status, "application/json; charset=utf-8");
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
        state.fetchAll = Chars.equalsNc("true", request.getUrlParam("count"));
        return true;
    }

    private void putLong256Value(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put('"');
        rec.getLong256(col, socket);
        socket.put('"');
    }

    private void putBinValue(HttpChunkedResponseSocket socket, Record record, int col) {
        socket.put('[');
        socket.put(']');
    }

    private void putSymValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStringOrNull(socket, rec.getSym(col));
    }

    private void putStrValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStringOrNull(socket, rec.getStr(col));
    }

    private void putCharValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        char c = rec.getChar(col);
        if (c == 0) {
            socket.put("\"\"");
        } else {
            socket.put('"').putUtf8(c).put('"');
        }
    }

    private void putShortValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getShort(col));
    }

    private void putTimestampValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long t = rec.getTimestamp(col);
        if (t == Long.MIN_VALUE) {
            socket.put("null");
            return;
        }
        socket.put('"').putISODate(t).put('"');
    }

    private void putDateValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long d = rec.getDate(col);
        if (d == Long.MIN_VALUE) {
            socket.put("null");
            return;
        }
        socket.put('"').putISODateMillis(d).put('"');
    }

    private void putLongValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long l = rec.getLong(col);
        if (l == Long.MIN_VALUE) {
            socket.put("null");
        } else {
            socket.put(l);
        }
    }

    private void putIntValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final int i = rec.getInt(col);
        if (i == Integer.MIN_VALUE) {
            socket.put("null");
        } else {
            Numbers.append(socket, i);
        }
    }

    private void putFloatValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getFloat(col), floatScale);
    }

    private void putDoubleValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getDouble(col), doubleScale);
    }

    private void putByteValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getByte(col));
    }

    private void putBooleanValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getBool(col));
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
            socket.bookmark();
            socket.put(']');
            socket.put(',').putQuoted("count").put(':').put(state.count);
            socket.put('}');
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

    @FunctionalInterface
    private interface ValueWriter {
        void write(HttpChunkedResponseSocket socket, Record rec, int col);
    }
}
