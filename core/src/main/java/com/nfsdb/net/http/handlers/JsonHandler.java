/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.net.http.handlers;

import com.nfsdb.ex.*;
import com.nfsdb.factory.JournalCachingFactory;
import com.nfsdb.factory.JournalFactoryPool;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.log.LogRecord;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.http.ChunkedResponse;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.parser.QueryCompiler;
import com.nfsdb.ql.parser.QueryError;
import com.nfsdb.std.LocalValue;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjectFactory;
import com.nfsdb.std.ThreadLocal;
import com.nfsdb.store.ColumnType;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class JsonHandler implements ContextHandler {
    private static final ThreadLocal<QueryCompiler> queryCompilerLocal = new ThreadLocal<>(new ObjectFactory<QueryCompiler>() {
        @Override
        public QueryCompiler newInstance() {
            return new QueryCompiler();
        }
    });

    private final JournalFactoryPool factoryPool;
    private final LocalValue<JsonHandlerContext> localContext = new LocalValue<>();

    public JsonHandler(JournalFactoryPool factoryPool) {
        this.factoryPool = factoryPool;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        JsonHandlerContext ctx = localContext.get(context);
        if (ctx == null) {
            localContext.set(context, ctx = new JsonHandlerContext());
        }
        ctx.fd = context.channel.getFd();

        // Query text.
        ChunkedResponse r = context.chunkedResponse();
        CharSequence query = context.request.getUrlParam("query");
        if (query == null || query.length() == 0) {
            ctx.info().$("Empty query request received. Sending empty reply.").$();
            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            r.put('{').putQuoted("query").put(':').putQuoted("").put('}');
            r.sendChunk();
            r.done();
            return;
        }

        // Url Params.
        long skip = 0;
        long stop = Long.MAX_VALUE;

        CharSequence limit = context.request.getUrlParam("limit");
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

        CharSequence withCount = context.request.getUrlParam("withCount");
        ctx.includeCount = withCount != null && Chars.equalsIgnoreCase(withCount, "true");
        ctx.query = query;
        ctx.skip = skip;
        ctx.count = 0L;
        ctx.stop = stop;

        ctx.info().$("Query: ").$(query).
                $(", skip: ").$(skip).
                $(", stop: ").$(stop).
                $(", withCount: ").$(ctx.includeCount).$();

        if (executeQuery(r, ctx) != null) {
            // Send records.
            resume(context);
        }
    }

    @Override
    public void resume(IOContext context) throws IOException {
        JsonHandlerContext ctx = localContext.get(context);

        Iterator<Record> records = ctx.records;
        if (records == null) {
            return;
        }

        RecordMetadata metadata = ctx.metadata;
        final int columnCount = metadata.getColumnCount();

        if (ctx.current == null && records.hasNext()) {
            ctx.current = records.next();
            ctx.count++;
        }

        final ChunkedResponse r = context.chunkedResponse();
        try {
            while (ctx.current != null) {
                if (ctx.count > ctx.skip) {
                    r.bookmark();
                    if (ctx.count > ctx.stop && !ctx.includeCount) {
                        break;
                    }

                    if (ctx.count <= ctx.stop) {
                        if (ctx.count > ctx.skip + 1) {
                            // Record separator.
                            r.put(',');
                        }
                        r.put('{');
                        // Columns.
                        for (int col = 0; col < columnCount; col++) {
                            RecordColumnMetadata m = metadata.getColumnQuick(col);
                            if (col > 0) {
                                r.put(',');
                            }
                            r.putQuoted(m.getName()).put(':');
                            putValue(r, m.getType(), ctx.current, col);
                        }
                        r.put('}');
                        r.sendChunk();
                    }
                }

                if (records.hasNext()) {
                    ctx.current = records.next();
                    ctx.count++;
                } else {
                    ctx.current = null;
                }
            }
            sendDone(r, ctx);
        } catch (ResponseContentBufferTooSmallException ex) {
            if (!r.resetToBookmark()) {
                ctx.error().$("IO buffer overflown but no previous bookmark found. Record ").
                        $(ctx.count - 1).$(". Aborting the query.").$();
                throw ex;
            }
            ctx.debug().$("Buffer overflow on record ").$(ctx.count - 1).$();
            r.sendChunk();
        }
    }

    private static void sendException(ChunkedResponse r, CharSequence query, int position, CharSequence message, int status) throws DisconnectedChannelException, SlowWritableChannelException {
        r.status(status, "application/json; charset=utf-8");
        r.sendHeader();
        r.put('{').
                putQuoted("query").put(':').putUtf8EscapedAndQuoted(query).put(',').
                putQuoted("error").put(':').putQuoted(message).put(',').
                putQuoted("position").put(':').put(position);
        r.put('}');
        r.sendChunk();
        r.done();
    }

    private static void putValue(CharSink sink, ColumnType type, Record rec, int col) {
        switch (type) {
            case BOOLEAN:
                sink.put(rec.getBool(col));
                break;
            case BYTE:
                sink.put(rec.get(col));
                break;
            case DOUBLE:
                sink.put(rec.getDouble(col), 10);
                break;
            case FLOAT:
                sink.put(rec.getFloat(col), 10);
                break;
            case INT:
                final int i = rec.getInt(col);
                if (i == Integer.MIN_VALUE) {
                    sink.put("null");
                    break;
                }
                Numbers.append(sink, i);
                break;
            case LONG:
            case DATE:
                final long l = rec.getLong(col);
                if (l == Long.MIN_VALUE) {
                    sink.put("null");
                    break;
                }
                sink.put(l);
                break;
            case SHORT:
                sink.put(rec.getShort(col));
                break;
            case STRING:
                putStringOrNull(sink, rec.getFlyweightStr(col));
                break;
            case SYMBOL:
                putStringOrNull(sink, rec.getSym(col));
                break;
            case BINARY:
                sink.put('[');
                sink.put(']');
                break;
            default:
                break;
        }
    }

    private static void putStringOrNull(CharSink r, CharSequence str) {
        if (str == null) {
            r.put("null");
        } else {
            r.putUtf8EscapedAndQuoted(str);
        }
    }

    @Nullable
    private Iterator<Record> executeQuery(ChunkedResponse r, JsonHandlerContext ctx) throws IOException {
        CharSequence query = ctx.query;
        try {
            // Prepare Context.
            JournalCachingFactory factory = factoryPool.get();
            ctx.factory = factory;
            ctx.recordSource = queryCompilerLocal.get().compileAndRemoveFromCache(factory, query);
            RecordCursor records = ctx.recordSource.prepareCursor(factory);

            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            r.put('{').putQuoted("query").put(':').putUtf8EscapedAndQuoted(query);
            r.put(',').putQuoted("columns").put(':').put('[');

            RecordMetadata metadata = records.getMetadata();
            ctx.metadata = metadata;
            ctx.records = records;

            // List columns.
            int columnCount = metadata.getColumnCount();

            for (int i = 0; i < columnCount; i++) {
                RecordColumnMetadata column = metadata.getColumnQuick(i);
                if (i > 0) {
                    r.put(',');
                }
                r.put('{').
                        putQuoted("name").put(':').putQuoted(column.getName()).
                        put(',').
                        putQuoted("type").put(':').putQuoted(column.getType().name());
                r.put('}');
            }
            r.put(']').put(',');
            r.putQuoted("result").put(':').put('[');
            return records;
        } catch (ParserException pex) {
            // todo: log exception properly
            ctx.info().$("Parser error executing query ").$(query).$(pex).$();
            sendException(r, query, QueryError.getPosition(), QueryError.getMessage(), 400);
        } catch (JournalException jex) {
            ctx.info().$("Server error executing query ").$(query).$(jex).$();
            sendException(r, query, 0, jex.getMessage(), 500);
        } catch (InterruptedException ex) {
            // todo: probably eliminate this
            ctx.info().$("Error executing query. Server is shutting down. Query: ").$(query).$(ex).$();
            sendException(r, query, 0, "Server is shutting down.", 500);
        }
        return null;
    }

    private void sendDone(ChunkedResponse r, JsonHandlerContext ctx) throws DisconnectedChannelException, SlowWritableChannelException {
        if (ctx.count > -1) {
            r.bookmark();
            r.put(']');
            if (ctx.count > ctx.stop && !ctx.includeCount) {
                r.put(",\"moreExist\":true");
            }

            if (ctx.includeCount) {
                r.put(',').putQuoted("totalCount").put(':').put(ctx.count);
            }
            r.put('}');
            ctx.count = -1;
            r.sendChunk();
        }
        r.done();
    }

    private static class JsonHandlerContext implements Mutable, Closeable {
        private static final Log LOG = LogFactory.getLog(JsonHandlerContext.class);
        public RecordSource recordSource;
        private CharSequence query;
        private RecordMetadata metadata;
        private Iterator<Record> records;
        private long count;
        private long skip;
        private long stop;
        private Record current;
        private boolean includeCount;
        private JournalCachingFactory factory;
        private long fd;

        @Override
        public void clear() {
            debug().$("Cleaning context").$();
            metadata = null;
            records = null;
            current = null;
            if (factory != null) {
                debug().$("Closing journal factory").$();
                factory.close();
                factory = null;
            }
            if (recordSource != null) {
                queryCompilerLocal.get().reuse(query, recordSource);
                recordSource = null;
            }
            query = null;
        }

        @Override
        public void close() throws IOException {
            debug().$("Closing context").$();
            clear();
        }

        private LogRecord debug() {
            return LOG.debug().$('[').$(fd).$("] ");
        }

        private LogRecord error() {
            return LOG.error().$('[').$(fd).$("] ");
        }

        private LogRecord info() {
            return LOG.info().$('[').$(fd).$("] ");
        }
    }
}
