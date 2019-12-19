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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.text.TextUtil;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.IOOperation;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

public class JsonQueryProcessor implements HttpRequestProcessor, Closeable {
    private static final LocalValue<JsonQueryProcessorState> LV = new LocalValue<>();
    private static final Log LOG = LogFactory.getLog(JsonQueryProcessor.class);
    private final static AtomicLong cacheHits = new AtomicLong();
    private final static AtomicLong cacheMisses = new AtomicLong();

    private final SqlCompiler compiler;
    private final JsonQueryProcessorConfiguration configuration;
    private final int floatScale;
    private final int doubleScale;
    private final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl();
    private final ObjList<ValueWriter> valueWriters = new ObjList<>();
    private final Path path = new Path();
    private final ObjList<QueryExecutor> queryExecutors = new ObjList<>();

    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine
    ) {
        // todo: add scheduler
        this.configuration = configuration;
        this.compiler = new SqlCompiler(engine);
        this.floatScale = configuration.getFloatScale();
        this.doubleScale = configuration.getDoubleScale();
        this.valueWriters.extendAndSet(ColumnType.BOOLEAN, JsonQueryProcessor::putBooleanValue);
        this.valueWriters.extendAndSet(ColumnType.BYTE, JsonQueryProcessor::putByteValue);
        this.valueWriters.extendAndSet(ColumnType.DOUBLE, this::putDoubleValue);
        this.valueWriters.extendAndSet(ColumnType.FLOAT, this::putFloatValue);
        this.valueWriters.extendAndSet(ColumnType.INT, JsonQueryProcessor::putIntValue);
        this.valueWriters.extendAndSet(ColumnType.LONG, JsonQueryProcessor::putLongValue);
        this.valueWriters.extendAndSet(ColumnType.DATE, JsonQueryProcessor::putDateValue);
        this.valueWriters.extendAndSet(ColumnType.TIMESTAMP, JsonQueryProcessor::putTimestampValue);
        this.valueWriters.extendAndSet(ColumnType.SHORT, JsonQueryProcessor::putShortValue);
        this.valueWriters.extendAndSet(ColumnType.CHAR, JsonQueryProcessor::putCharValue);
        this.valueWriters.extendAndSet(ColumnType.STRING, JsonQueryProcessor::putStrValue);
        this.valueWriters.extendAndSet(ColumnType.SYMBOL, JsonQueryProcessor::putSymValue);
        this.valueWriters.extendAndSet(ColumnType.BINARY, JsonQueryProcessor::putBinValue);
        this.valueWriters.extendAndSet(ColumnType.LONG256, JsonQueryProcessor::putLong256Value);

        final QueryExecutor sendConfirmation = JsonQueryProcessor::sendConfirmation;
        this.queryExecutors.extendAndSet(CompiledQuery.SELECT, this::executeNewSelect);
        this.queryExecutors.extendAndSet(CompiledQuery.INSERT, this::executeInsert);
        this.queryExecutors.extendAndSet(CompiledQuery.TRUNCATE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.ALTER, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.REPAIR, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SET, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.DROP, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COPY_LOCAL, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.INSERT_AS_SELECT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COPY_REMOTE, JsonQueryProcessor::cannotCopyRemote);
    }

    private static void putStringOrNull(CharSink r, CharSequence str) {
        if (str == null) {
            r.put("null");
        } else {
            r.encodeUtf8AndQuote(str);
        }
    }

    private static void doResumeSend(
            HttpConnectionContext context,
            ObjList<ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        JsonQueryProcessorState state = LV.get(context);
        if (state == null || state.cursor == null) {
            return;
        }

        LOG.debug().$("resume [fd=").$(context.getFd()).$(']').$();

        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        final int columnCount = state.metadata.getColumnCount();

        while (true) {
            try {
                state.resume(valueWriters, socket, columnCount);
                break;
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
        readyForNextRequest(context);
    }

    private static void cannotCopyRemote(
            HttpConnectionContext context,
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws SqlException {
        throw SqlException.$(0, "copy from STDIN is not supported over REST");
    }

    private static void executeCachedSelect(
            HttpConnectionContext context,
            JsonQueryProcessorState state,
            RecordCursorFactory factory,
            RecordCursor cursor,
            CharSequence keepAliveHeader,
            ObjList<ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        cacheHits.incrementAndGet();
        info(state).$("execute-cached ").
                $("[skip: ").$(state.skip).
                $(", stop: ").$(state.stop).
                $(']').$();
        executeSelect(context, state, factory, cursor, keepAliveHeader, valueWriters);
    }

    private static void executeSelect(
            HttpConnectionContext context,
            JsonQueryProcessorState state,
            RecordCursorFactory factory,
            RecordCursor cursor,
            CharSequence keepAliveHeader,
            ObjList<ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        state.recordCursorFactory = factory;
        state.cursor = cursor;
        state.metadata = factory.getMetadata();
        header(context.getChunkedResponseSocket(), 200, keepAliveHeader);
        doResumeSend(context, valueWriters);
    }

    protected static void header(
            HttpChunkedResponseSocket socket,
            int status,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(status, "application/json; charset=utf-8");
        socket.headers().setKeepAlive(keepAliveHeader);
        socket.sendHeader();
    }

    private static LogRecord info(JsonQueryProcessorState state) {
        return LOG.info().$('[').$(state.fd).$("] ");
    }

    private static void putBinValue(HttpChunkedResponseSocket socket, Record record, int col) {
        socket.put('[');
        socket.put(']');
    }

    private static void putBooleanValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getBool(col));
    }

    private static void putByteValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getByte(col));
    }

    private static void putCharValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        char c = rec.getChar(col);
        if (c == 0) {
            socket.put("\"\"");
        } else {
            socket.put('"').putUtf8(c).put('"');
        }
    }

    private static void putDateValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long d = rec.getDate(col);
        if (d == Long.MIN_VALUE) {
            socket.put("null");
            return;
        }
        socket.put('"').putISODateMillis(d).put('"');
    }

    private static void putIntValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final int i = rec.getInt(col);
        if (i == Integer.MIN_VALUE) {
            socket.put("null");
        } else {
            Numbers.append(socket, i);
        }
    }

    private static void putLong256Value(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put('"');
        rec.getLong256(col, socket);
        socket.put('"');
    }

    private static void putLongValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long l = rec.getLong(col);
        if (l == Long.MIN_VALUE) {
            socket.put("null");
        } else {
            socket.put(l);
        }
    }

    private static void putShortValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getShort(col));
    }

    private static void putStrValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStringOrNull(socket, rec.getStr(col));
    }

    private static void putSymValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStringOrNull(socket, rec.getSym(col));
    }

    private static void putTimestampValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long t = rec.getTimestamp(col);
        if (t == Long.MIN_VALUE) {
            socket.put("null");
            return;
        }
        socket.put('"').putISODate(t).put('"');
    }

    private static void readyForNextRequest(
            HttpConnectionContext context
    ) {
        LOG.debug().$("all sent [fd=").$(context.getFd()).$(']').$();
        context.clear();
        context.getDispatcher().registerChannel(context, IOOperation.READ);
    }

    private static void sendConfirmation(
            HttpConnectionContext context,
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        header(socket, 200, keepAliveHeader);
        socket.put('{').putQuoted("ddl").put(':').putQuoted("OK").put('}');
        socket.sendChunk();
        socket.done();
        readyForNextRequest(context);
    }

    private static void sendException(
            HttpChunkedResponseSocket socket,
            int position,
            CharSequence message,
            int status,
            CharSequence query,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        header(socket, status, keepAliveHeader);
        socket.put('{').
                putQuoted("query").put(':').encodeUtf8AndQuote(query == null ? "" : query).put(',').
                putQuoted("error").put(':').encodeUtf8AndQuote(message).put(',').
                putQuoted("position").put(':').put(position);
        socket.put('}');
        socket.sendChunk();
        socket.done();
    }

    private static void syntaxError(
            HttpChunkedResponseSocket socket,
            SqlException sqlException,
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        info(state)
                .$("syntax-error [q=`").utf8(state.query)
                .$("`, at=").$(sqlException.getPosition())
                .$(", message=`").utf8(sqlException.getFlyweightMessage()).$('`')
                .$(']').$();

        sendException(
                socket,
                sqlException.getPosition(),
                sqlException.getFlyweightMessage(),
                400,
                state.query,
                keepAliveHeader
        );
    }

    @Override
    public void close() {
        Misc.free(compiler);
        Misc.free(path);
        JsonQueryProcessorState.FACTORY_CACHE.get().close();
    }

    public void execute0(
            HttpConnectionContext context,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        sqlExecutionContext.with(context.getCairoSecurityContext(), null);
        info(state).$("exec [q='").utf8(state.query).$("']").$();
        final RecordCursorFactory factory = JsonQueryProcessorState.FACTORY_CACHE.get().poll(state.query);
        try {
            if (factory != null) {
                try {
                    executeCachedSelect(
                            context,
                            state,
                            factory,
                            factory.getCursor(sqlExecutionContext),
                            configuration.getKeepAliveHeader(),
                            valueWriters
                    );
                } catch (ReaderOutOfDateException e) {
                    Misc.free(factory);
                    compileQuery(context, state);
                }
            } else {
                // new query
                compileQuery(context, state);
            }
        } catch (SqlException e) {
            syntaxError(context.getChunkedResponseSocket(), e, state, configuration.getKeepAliveHeader());
            readyForNextRequest(context);
        } catch (CairoException | CairoError e) {
            internalError(context.getChunkedResponseSocket(), e, state);
            readyForNextRequest(context);
        }
    }

    private void compileQuery(HttpConnectionContext context, JsonQueryProcessorState state) throws SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        final CompiledQuery cc = compiler.compile(state.query, sqlExecutionContext);
        queryExecutors.getQuick(cc.getType()).execute(
                context,
                state,
                cc,
                configuration.getKeepAliveHeader()
        );
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        JsonQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new JsonQueryProcessorState(context.getFd(), configuration.getConnectionCheckFrequency()));
        }
        HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        if (parseUrl(socket, context.getRequestHeader(), state, configuration.getKeepAliveHeader())) {
            execute0(context, state);
        } else {
            readyForNextRequest(context);
        }
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doResumeSend(context, this.valueWriters);
    }

    private LogRecord error(JsonQueryProcessorState state) {
        return LOG.error().$('[').$(state.fd).$("] ");
    }

    private void executeInsert(
            HttpConnectionContext context,
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final InsertStatement insertStatement = cc.getInsertStatement();
        try (InsertMethod insertMethod = insertStatement.createMethod(sqlExecutionContext)) {
            insertMethod.execute();
            insertMethod.commit();
        }
        sendConfirmation(context, state, cc, keepAliveHeader);
    }

    private void executeNewSelect(
            HttpConnectionContext context,
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        cacheMisses.incrementAndGet();
        info(state).$("execute-new ").
                $("[skip: ").$(state.skip).
                $(", stop: ").$(state.stop).
                $(']').$();
        final RecordCursorFactory factory = cc.getRecordCursorFactory();
        final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
        executeSelect(
                context,
                state,
                factory,
                cursor,
                keepAliveHeader,
                valueWriters
        );
    }

    private void internalError(
            HttpChunkedResponseSocket socket,
            Throwable e,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        error(state).$("Server error executing query ").utf8(state.query).$(e).$();
        sendException(socket, 0, e.getMessage(), 500, state.query, configuration.getKeepAliveHeader());
    }

    private boolean parseUrl(
            HttpChunkedResponseSocket socket,
            HttpRequestHeader request,
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Query text.
        final DirectByteCharSequence query = request.getUrlParam("query");
        if (query == null || query.length() == 0) {
            info(state).$("Empty query request received. Sending empty reply.").$();
            sendException(socket, 0, "No query text", 400, state.query, keepAliveHeader);
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
                    skip = Numbers.parseLong(limit, 0, sepPos) - 1;
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

        state.query.clear();
        try {
            TextUtil.utf8Decode(query.getLo(), query.getHi(), state.query);
        } catch (Utf8Exception e) {
            info(state).$("Bad UTF8 encoding").$();
            sendException(socket, 0, "Bad UTF8 encoding in query text", 400, state.query, keepAliveHeader);
            return false;
        }

        state.skip = skip;
        state.count = 0L;
        state.stop = stop;
        state.noMeta = Chars.equalsNc("true", request.getUrlParam("nm"));
        state.countRows = Chars.equalsNc("true", request.getUrlParam("count"));
        return true;
    }

    private void putDoubleValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getDouble(col), doubleScale);
    }

    private void putFloatValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getFloat(col), floatScale);
    }

    @FunctionalInterface
    private interface QueryExecutor {
        void execute(
                HttpConnectionContext context,
                JsonQueryProcessorState state,
                CompiledQuery cc,
                CharSequence keepAliveHeader
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException;
    }

    @FunctionalInterface
    interface ValueWriter {
        void write(HttpChunkedResponseSocket socket, Record rec, int col);
    }
}
