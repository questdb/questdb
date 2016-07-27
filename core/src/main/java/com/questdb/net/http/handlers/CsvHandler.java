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
 ******************************************************************************/

package com.questdb.net.http.handlers;

import com.questdb.ex.DisconnectedChannelException;
import com.questdb.ex.ResponseContentBufferTooSmallException;
import com.questdb.ex.SlowWritableChannelException;
import com.questdb.factory.JournalFactoryPool;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.net.http.ChunkedResponse;
import com.questdb.net.http.ContextHandler;
import com.questdb.net.http.IOContext;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.Record;
import com.questdb.std.CharSink;
import com.questdb.std.LocalValue;
import com.questdb.std.Mutable;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class CsvHandler implements ContextHandler {
    private final JournalFactoryPool factoryPool;
    private final LocalValue<ExportHandlerContext> localContext = new LocalValue<>();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final ServerConfiguration configuration;


    public CsvHandler(JournalFactoryPool factoryPool, ServerConfiguration configuration) {
        this.factoryPool = factoryPool;
        this.configuration = configuration;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        ExportHandlerContext ctx = localContext.get(context);
        if (ctx == null) {
            localContext.set(context, ctx = new ExportHandlerContext(context.channel.getFd(), context.getServerConfiguration().getDbCyclesBeforeCancel()));
        }
        ChunkedResponse r = context.chunkedResponse();
        if (ctx.parseUrl(r, context.request)) {
            ctx.compileQuery(r, factoryPool, cacheMisses, cacheHits);
            resume(context);
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public void resume(IOContext context) throws IOException {
        ExportHandlerContext ctx = localContext.get(context);
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
                    case METADATA:
                        for (; ctx.columnIndex < columnCount; ctx.columnIndex++) {
                            RecordColumnMetadata column = ctx.metadata.getColumnQuick(ctx.columnIndex);

                            r.bookmark();
                            if (ctx.columnIndex > 0) {
                                r.put(',');
                            }
                            r.putQuoted(column.getName());
                        }
                        r.put(Misc.EOL);
                        ctx.state = AbstractQueryContext.QueryState.RECORD_START;
                        // fall through
                    case RECORD_START:
                        if (ctx.record == null) {
                            // check if cursor has any records
                            while (true) {
                                if (ctx.cursor.hasNext()) {
                                    ctx.record = ctx.cursor.next();
                                    ctx.count++;

                                    if (ctx.count > ctx.skip) {
                                        break;
                                    }
                                } else {
                                    ctx.state = AbstractQueryContext.QueryState.DATA_SUFFIX;
                                    break SWITCH;
                                }
                            }
                        }

                        if (ctx.count > ctx.stop) {
                            ctx.state = AbstractQueryContext.QueryState.DATA_SUFFIX;
                            break;
                        }

                        ctx.state = AbstractQueryContext.QueryState.RECORD_COLUMNS;
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

                        r.bookmark();
                        r.put(Misc.EOL);
                        ctx.record = null;
                        ctx.state = AbstractQueryContext.QueryState.RECORD_START;
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

    @Override
    public void setupThread() {
        AbstractQueryContext.setupThread(configuration);
    }


    private static void putValue(CharSink sink, int type, Record rec, int col) {
        switch (type) {
            case ColumnType.BOOLEAN:
                sink.put(rec.getBool(col));
                break;
            case ColumnType.BYTE:
                sink.put(rec.get(col));
                break;
            case ColumnType.DOUBLE:
                double d = rec.getDouble(col);
                if (d == d) {
                    sink.put(d, 10);
                }
                break;
            case ColumnType.FLOAT:
                float f = rec.getFloat(col);
                if (f == f) {
                    sink.put(f, 10);
                }
                break;
            case ColumnType.INT:
                final int i = rec.getInt(col);
                if (i > Integer.MIN_VALUE) {
                    Numbers.append(sink, i);
                }
                break;
            case ColumnType.LONG:
                final long l = rec.getLong(col);
                if (l > Long.MIN_VALUE) {
                    sink.put(l);
                }
                break;
            case ColumnType.DATE:
                final long dt = rec.getDate(col);
                if (dt > Long.MIN_VALUE) {
                    sink.put('"').putISODate(dt).put('"');
                }
                break;
            case ColumnType.SHORT:
                sink.put(rec.getShort(col));
                break;
            case ColumnType.STRING:
                CharSequence cs;
                cs = rec.getFlyweightStr(col);
                if (cs != null) {
                    sink.put(cs);
                }
                break;
            case ColumnType.SYMBOL:
                cs = rec.getSym(col);
                if (cs != null) {
                    sink.put(cs);
                }
                break;
            case ColumnType.BINARY:
                break;
            default:
                break;
        }
    }

    private void sendDone(ChunkedResponse r, ExportHandlerContext ctx) throws DisconnectedChannelException, SlowWritableChannelException {
        if (ctx.count > -1) {
            ctx.count = -1;
            r.sendChunk();
        }
        r.done();
    }

    private static class ExportHandlerContext extends AbstractQueryContext implements Mutable, Closeable {
        public ExportHandlerContext(long fd, int cyclesBeforeCancel) {
            super(fd, cyclesBeforeCancel);
            state = QueryState.METADATA;
        }

        @Override
        public void clear() {
            super.clear();
            state = QueryState.METADATA;
        }

        @Override
        public void close() throws IOException {
            debug().$("Closing context").$();
            clear();
        }

        @Override
        protected void header(ChunkedResponse r, int code) throws DisconnectedChannelException, SlowWritableChannelException {
            state = QueryState.METADATA;
            r.status(code, "text/csv; charset=utf-8");
            r.headers().put("Content-Disposition: attachment; filename=\"questdb-query-").put(System.currentTimeMillis()).put(".csv\"").put(Misc.EOL);
            r.sendHeader();
        }

        @Override
        protected void sendException(ChunkedResponse r, int position, CharSequence message, int status) throws DisconnectedChannelException, SlowWritableChannelException {
            header(r, status);
            r.put("Error at(").put(position).put("): ").put(message).put(Misc.EOL);
            r.sendChunk();
            r.done();
        }
    }
}
