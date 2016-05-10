/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.net.http.handlers;

import com.questdb.ex.*;
import com.questdb.factory.JournalCachingFactory;
import com.questdb.factory.JournalFactoryPool;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.misc.Chars;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.net.http.ChunkedResponse;
import com.questdb.net.http.ContextHandler;
import com.questdb.net.http.IOContext;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.parser.QueryCompiler;
import com.questdb.ql.parser.QueryError;
import com.questdb.std.*;
import com.questdb.std.ThreadLocal;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class QueryHandler implements ContextHandler {
    private static final ThreadLocal<QueryCompiler> COMPILER = new ThreadLocal<>(new ObjectFactory<QueryCompiler>() {
        @Override
        public QueryCompiler newInstance() {
            return new QueryCompiler();
        }
    });
    private static final ThreadLocal<AssociativeCache<RecordSource>> CACHE = new ThreadLocal<>(new ObjectFactory<AssociativeCache<RecordSource>>() {
        @Override
        public AssociativeCache<RecordSource> newInstance() {
            return new AssociativeCache<>(8, 128);
        }
    });

    private final JournalFactoryPool factoryPool;
    private final LocalValue<JsonHandlerContext> localContext = new LocalValue<>();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();

    public QueryHandler(JournalFactoryPool factoryPool) {
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

        ctx.noMeta = Chars.equalsNc("true", context.request.getUrlParam("nm"));
        ctx.query = query;
        ctx.skip = skip;
        ctx.count = 0L;
        ctx.stop = stop;
        ctx.fetchAll = Chars.equalsNc("true", context.request.getUrlParam("count"));

        ctx.info().$("Query: ").$(query).
                $(", skip: ").$(skip).
                $(", stop: ").$(stop).$();

        executeQuery(r, ctx);
        resume(context);
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public void resume(IOContext context) throws IOException {
        JsonHandlerContext ctx = localContext.get(context);
        if (ctx == null || ctx.cursor == null) {
            return;
        }

        final ChunkedResponse r = context.chunkedResponse();
        final int columnCount = ctx.metadata.getColumnCount();

        OUT:
        while (true) {
            try {
                SWITCH:
                switch (ctx.state) {
                    case PREFIX:
                        if (ctx.noMeta) {
                            r.put("{\"result\":[");
                            ctx.state = QueryState.RECORD_START;
                            break;
                        }
                        r.bookmark();
                        r.put('{').putQuoted("query").put(':').putUtf8EscapedAndQuoted(ctx.query);
                        r.put(',').putQuoted("columns").put(':').put('[');
                        ctx.state = QueryState.METADATA;
                        ctx.columnIndex = 0;
                        // fall through
                    case METADATA:
                        for (; ctx.columnIndex < columnCount; ctx.columnIndex++) {
                            RecordColumnMetadata column = ctx.metadata.getColumnQuick(ctx.columnIndex);

                            r.bookmark();

                            if (ctx.columnIndex > 0) {
                                r.put(',');
                            }
                            r.put('{').
                                    putQuoted("name").put(':').putQuoted(column.getName()).
                                    put(',').
                                    putQuoted("type").put(':').putQuoted(column.getType().name());
                            r.put('}');
                        }
                        ctx.state = QueryState.META_SUFFIX;
                        // fall through
                    case META_SUFFIX:
                        r.bookmark();
                        r.put("],\"result\":[");
                        ctx.state = QueryState.RECORD_START;
                        // fall through
                    case RECORD_START:

                        if (ctx.record == null) {
                            // check if cursor has any records
                            while (true) {
                                if (ctx.cursor.hasNext()) {
                                    ctx.record = ctx.cursor.next();
                                    ctx.count++;

                                    if (ctx.fetchAll && ctx.count > ctx.stop) {
                                        continue;
                                    }

                                    if (ctx.count > ctx.skip) {
                                        break;
                                    }
                                } else {
                                    ctx.state = QueryState.DATA_SUFFIX;
                                    break SWITCH;
                                }
                            }
                        }

                        if (ctx.count > ctx.stop) {
                            ctx.state = QueryState.DATA_SUFFIX;
                            break;
                        }

                        r.bookmark();
                        if (ctx.count > ctx.skip + 1) {
                            r.put(',');
                        }
                        r.put('[');

                        ctx.state = QueryState.RECORD_COLUMNS;
                        ctx.columnIndex = 0;
                        // fall through
                    case RECORD_COLUMNS:

                        for (; ctx.columnIndex < columnCount; ctx.columnIndex++) {
                            RecordColumnMetadata m = ctx.metadata.getColumnQuick(ctx.columnIndex);
                            r.bookmark();
                            if (ctx.columnIndex > 0) {
                                r.put(',');
                            }
                            putValue(r, m.getType(), ctx.record, ctx.columnIndex);
                        }

                        ctx.state = QueryState.RECORD_SUFFIX;
                        // fall through

                    case RECORD_SUFFIX:
                        r.bookmark();
                        r.put(']');
                        ctx.record = null;
                        ctx.state = QueryState.RECORD_START;
                        break;
                    case DATA_SUFFIX:
                        sendDone(r, ctx);
                        break OUT;
                    default:
                        break OUT;
                }
            } catch (ResponseContentBufferTooSmallException ignored) {
                if (r.resetToBookmark()) {
                    r.sendChunk();
                } else {
                    // what we have here is out unit of data, column value or query
                    // is larger that response content buffer
                    // all we can do in this scenario is to log appropriately
                    // and disconnect socket
                    ctx.info().$("Response buffer is too small, state=").$(ctx.state).$();
                    throw DisconnectedChannelException.INSTANCE;
                }
            }
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
                final long l = rec.getLong(col);
                if (l == Long.MIN_VALUE) {
                    sink.put("null");
                    break;
                }
                sink.put(l);
                break;
            case DATE:
                final long d = rec.getDate(col);
                if (d == Long.MIN_VALUE) {
                    sink.put("null");
                    break;
                }
                sink.put('"').putISODate(d).put('"');
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

    private void executeQuery(ChunkedResponse r, JsonHandlerContext ctx) throws IOException {
        try {
            // Prepare Context.
            JournalCachingFactory factory = factoryPool.get();
            ctx.factory = factory;
            ctx.recordSource = CACHE.get().poll(ctx.query);
            if (ctx.recordSource == null) {
                ctx.recordSource = COMPILER.get().compileSource(factory, ctx.query);
                cacheMisses.incrementAndGet();
            } else {
                ctx.recordSource.reset();
                cacheHits.incrementAndGet();
            }
            ctx.cursor = ctx.recordSource.prepareCursor(factory);
            ctx.metadata = ctx.cursor.getMetadata();
            ctx.state = QueryState.PREFIX;

            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
        } catch (ParserException e) {
            ctx.info().$("Parser error executing query ").$(ctx.query).$(": at (").$(QueryError.getPosition()).$(") ").$(QueryError.getMessage()).$();
            sendException(r, ctx.query, QueryError.getPosition(), QueryError.getMessage(), 400);
        } catch (JournalException e) {
            ctx.error().$("Server error executing query ").$(ctx.query).$(e).$();
            sendException(r, ctx.query, 0, e.getMessage(), 500);
        } catch (InterruptedException e) {
            ctx.error().$("Error executing query. Server is shutting down. Query: ").$(ctx.query).$(e).$();
            sendException(r, ctx.query, 0, "Server is shutting down.", 500);
        }
    }

    long getCacheHits() {
        return cacheHits.longValue();
    }

    long getCacheMisses() {
        return cacheMisses.longValue();
    }

    private void sendDone(ChunkedResponse r, JsonHandlerContext ctx) throws DisconnectedChannelException, SlowWritableChannelException {
        if (ctx.count > -1) {
            r.bookmark();
            r.put(']');
            r.put(',').putQuoted("count").put(':').put(ctx.count);
            r.put('}');
            ctx.count = -1;
            r.sendChunk();
        }
        r.done();
    }

    private enum QueryState {
        PREFIX, METADATA, META_SUFFIX, RECORD_START, RECORD_COLUMNS, RECORD_SUFFIX, DATA_SUFFIX
    }

    private static class JsonHandlerContext implements Mutable, Closeable {
        private static final Log LOG = LogFactory.getLog(JsonHandlerContext.class);
        private RecordSource recordSource;
        private CharSequence query;
        private RecordMetadata metadata;
        private RecordCursor cursor;
        private long count;
        private long skip;
        private long stop;
        private Record record;
        private JournalCachingFactory factory;
        private long fd;
        private QueryState state = QueryState.PREFIX;
        private int columnIndex;
        private boolean noMeta = false;
        private boolean fetchAll = false;

        @Override
        public void clear() {
            debug().$("Cleaning context").$();
            metadata = null;
            cursor = null;
            record = null;
            debug().$("Closing journal factory").$();
            factory = Misc.free(factory);
            if (recordSource != null) {
                CACHE.get().put(query.toString(), recordSource);
                recordSource = null;
            }
            query = null;
            state = QueryState.PREFIX;
            fetchAll = false;
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
