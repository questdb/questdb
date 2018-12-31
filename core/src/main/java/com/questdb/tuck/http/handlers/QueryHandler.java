/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.tuck.http.handlers;

import com.questdb.BootstrapEnv;
import com.questdb.ex.ResponseContentBufferTooSmallException;
import com.questdb.std.LocalValue;
import com.questdb.std.Numbers;
import com.questdb.std.ex.DisconnectedChannelException;
import com.questdb.std.ex.SlowWritableChannelException;
import com.questdb.std.str.CharSink;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.factory.Factory;
import com.questdb.tuck.http.ChunkedResponse;
import com.questdb.tuck.http.ContextHandler;
import com.questdb.tuck.http.IOContext;
import com.questdb.tuck.http.ResponseSink;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.questdb.net.http.handlers.AbstractQueryContext.*;

public class QueryHandler implements ContextHandler {

    private final Factory factory;
    private final LocalValue<QueryHandlerContext> localContext = new LocalValue<>();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final BootstrapEnv env;

    public QueryHandler(BootstrapEnv env) {
        this.factory = env.factory;
        this.env = env;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        QueryHandlerContext ctx = localContext.get(context);
        if (ctx == null) {
            localContext.set(context,
                    ctx = new QueryHandlerContext(context.getFd(), context.getServerConfiguration().getDbCyclesBeforeCancel()));
        }
        ChunkedResponse r = context.chunkedResponse();
        if (ctx.parseUrl(r, context.request)) {
            ctx.compileQuery(r, factory, cacheMisses, cacheHits);
            resume(context);
        }
    }

    @Override
    public void resume(IOContext context) throws IOException {
        QueryHandlerContext ctx = localContext.get(context);
        if (ctx == null || ctx.cursor == null) {
            return;
        }

        final ChunkedResponse r = context.chunkedResponse();
        final int columnCount = ctx.metadata.getColumnCount();

        OUT:
        while (true) {
            try {
                SWITCH:
                switch (ctx.queryState) {
                    case QUERY_PREFIX:
                        if (ctx.noMeta) {
                            r.put('{').putQuoted("dataset").put(":[");
                            ctx.queryState = QUERY_RECORD_START;
                            break;
                        }
                        r.bookmark();
                        r.put('{').putQuoted("query").put(':').encodeUtf8AndQuote(ctx.query);
                        r.put(',').putQuoted("columns").put(':').put('[');
                        ctx.queryState = QUERY_METADATA;
                        ctx.columnIndex = 0;
                        // fall through
                    case QUERY_METADATA:
                        for (; ctx.columnIndex < columnCount; ctx.columnIndex++) {
                            RecordColumnMetadata column = ctx.metadata.getColumnQuick(ctx.columnIndex);

                            r.bookmark();

                            if (ctx.columnIndex > 0) {
                                r.put(',');
                            }
                            r.put('{').
                                    putQuoted("name").put(':').putQuoted(column.getName()).
                                    put(',').
                                    putQuoted("type").put(':').putQuoted(ColumnType.nameOf(column.getType()));
                            r.put('}');
                        }
                        ctx.queryState = QUERY_META_SUFFIX;
                        // fall through
                    case QUERY_META_SUFFIX:
                        r.bookmark();
                        r.put("],\"dataset\":[");
                        ctx.queryState = QUERY_RECORD_START;
                        // fall through
                    case QUERY_RECORD_START:

                        if (ctx.record == null) {
                            // check if cursor has any records
                            while (true) {
                                if (ctx.cursor.hasNext()) {
                                    ctx.record = ctx.cursor.next();
                                    ctx.count++;

                                    if (ctx.fetchAll && ctx.count > ctx.stop) {
                                        ctx.cancellationHandler.check();
                                        continue;
                                    }

                                    if (ctx.count > ctx.skip) {
                                        break;
                                    }
                                } else {
                                    ctx.queryState = QUERY_DATA_SUFFIX;
                                    break SWITCH;
                                }
                            }
                        }

                        if (ctx.count > ctx.stop) {
                            ctx.queryState = QUERY_DATA_SUFFIX;
                            break;
                        }

                        r.bookmark();
                        if (ctx.count > ctx.skip + 1) {
                            r.put(',');
                        }
                        r.put('[');

                        ctx.queryState = QUERY_RECORD_COLUMNS;
                        ctx.columnIndex = 0;
                        // fall through
                    case QUERY_RECORD_COLUMNS:

                        for (; ctx.columnIndex < columnCount; ctx.columnIndex++) {
                            RecordColumnMetadata m = ctx.metadata.getColumnQuick(ctx.columnIndex);
                            r.bookmark();
                            if (ctx.columnIndex > 0) {
                                r.put(',');
                            }
                            putValue(r, m.getType(), ctx.record, ctx.columnIndex);
                        }

                        ctx.queryState = QUERY_RECORD_SUFFIX;
                        // fall through

                    case QUERY_RECORD_SUFFIX:
                        r.bookmark();
                        r.put(']');
                        ctx.record = null;
                        ctx.queryState = QUERY_RECORD_START;
                        break;
                    case QUERY_DATA_SUFFIX:
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
                    ctx.info().$("Response buffer is too small, state=").$(ctx.queryState).$();
                    throw DisconnectedChannelException.INSTANCE;
                }
            }
        }
    }

    @Override
    public void setupThread() {
        AbstractQueryContext.setupThread(env);
    }

    private static void putValue(ResponseSink sink, int type, Record rec, int col) {
        switch (type) {
            case ColumnType.BOOLEAN:
                sink.put(rec.getBool(col));
                break;
            case ColumnType.BYTE:
                sink.put(rec.getByte(col));
                break;
            case ColumnType.DOUBLE:
                sink.put(rec.getDouble(col), 10);
                break;
            case ColumnType.FLOAT:
                sink.put(rec.getFloat(col), 10);
                break;
            case ColumnType.INT:
                final int i = rec.getInt(col);
                if (i == Integer.MIN_VALUE) {
                    sink.put("null");
                } else {
                    Numbers.append(sink, i);
                }
                break;
            case ColumnType.LONG:
                final long l = rec.getLong(col);
                if (l == Long.MIN_VALUE) {
                    sink.put("null");
                } else {
                    sink.put(l);
                }
                break;
            case ColumnType.DATE:
                final long d = rec.getDate(col);
                if (d == Long.MIN_VALUE) {
                    sink.put("null");
                    break;
                }
                sink.put('"').putISODateMillis(d).put('"');
                break;
            case ColumnType.SHORT:
                sink.put(rec.getShort(col));
                break;
            case ColumnType.STRING:
                putStringOrNull(sink, rec.getFlyweightStr(col));
                break;
            case ColumnType.SYMBOL:
                putStringOrNull(sink, rec.getSym(col));
                break;
            case ColumnType.BINARY:
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
            r.encodeUtf8AndQuote(str);
        }
    }

    long getCacheHits() {
        return cacheHits.longValue();
    }

    long getCacheMisses() {
        return cacheMisses.longValue();
    }

    private void sendDone(ChunkedResponse r, QueryHandlerContext ctx) throws DisconnectedChannelException, SlowWritableChannelException {
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

}
