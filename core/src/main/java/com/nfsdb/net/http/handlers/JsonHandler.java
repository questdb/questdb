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
import org.jetbrains.annotations.Nullable;
import sun.nio.cs.ArrayEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Iterator;

public class JsonHandler implements ContextHandler {
    private static final ArrayEncoder UTF8Encoder;
    private static final ThreadLocal<QueryCompiler> queryCompilerLocal = new ThreadLocal<>(new ObjectFactory<QueryCompiler>() {
        @Override
        public QueryCompiler newInstance() {
            return new QueryCompiler();
        }
    });

    private final JournalFactoryPool factoryPool;
    private final LocalValue<$Context> localContext = new LocalValue<>();

    public JsonHandler(JournalFactoryPool factoryPool) {
        this.factoryPool = factoryPool;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        $Context ctx = localContext.get(context);
        if (ctx == null) {
            ctx = new $Context();
            localContext.set(context, ctx);
        }
        ctx.fd = context.channel.getFd();

        // Query text.
        ChunkedResponse r = context.chunkedResponse();
        CharSequence query = context.request.getUrlParam("query");
        if (query == null || query.length() == 0) {
            ctx.info().$("Empty query request received. Sending empty reply.").$();
            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            sendQuery(r, "", ctx);
            r.put("\"}");
            r.sendChunk();
            r.done();
            return;
        }

        // Url Params.
        long skip = 0;
        long stop = Long.MAX_VALUE;

        CharSequence limit = context.request.getUrlParam("limit");
        if (limit != null) {
            int sepPos = separatorPos(limit);
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
        if (executeQuery(r, ctx) == null) {
            return;
        }

        // Send records.
        resume(context);
    }

    @Override
    public void resume(IOContext context) throws IOException {
        ChunkedResponse r = context.chunkedResponse();
        $Context ctx = localContext.get(context);

        Iterator<Record> records = ctx.records;
        if (records == null) {
            records = executeQuery(r, ctx);
            if (records == null) {
                return;
            }
        }

        try {
            RecordMetadata metadata = ctx.metadata;
            int columnCount = metadata.getColumnCount();

            if (ctx.current == null && records.hasNext()) {
                ctx.current = records.next();
                ctx.count++;
            }

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
                        r.put("{");

                        // Columns.
                        for (int col = 0; col < columnCount; col++) {
                            r.put('\"');
                            stringToJson(r, metadata.getColumn(col).getName(), ctx);
                            r.put("\":");
                            putValue(ctx, r, metadata, ctx.current, col);

                            if (col < columnCount - 1) {
                                r.put(',');
                            }
                        }
                        r.put("}");
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
            ctx.debug().$("Buffer overflow on record ").$(ctx.count - 1).$();
            if (!r.resetToBookmark()) {
                ctx.error().$("IO buffer overflown but no previous bookmark found. Record ").$(ctx.count - 1)
                        .$(". Aborting the query.").$();
                throw ex;
            }
            r.sendChunk();
        }
    }

    private static int separatorPos(CharSequence str) {
        if (str == null) return -1;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ',') {
                return i;
            }
        }
        return -1;
    }

    private static void sendQuery(ChunkedResponse r, CharSequence query, $Context ctx) {
        r.put("{ \"query\": \"");
        stringToJson(r, query != null ? query : "", ctx);
        r.put("\"");
    }

    private static void sendException(ChunkedResponse r, CharSequence query, int position, CharSequence message, int status, $Context ctx) throws DisconnectedChannelException, SlowWritableChannelException {
        r.status(status, "application/json; charset=utf-8");
        r.sendHeader();
        sendQuery(r, query, ctx);
        r.put(", \"error\" : \"");
        stringToJson(r, message, ctx);
        r.put("\"");
        if (position >= 0) {
            r.put(", \"position\" : ");
            Numbers.append(r, position);
        }

        r.put("}");
        r.sendChunk();
        r.done();
    }

    private static void putValue($Context ctx, ChunkedResponse r, RecordMetadata metadata, Record rec, int col) {
        RecordColumnMetadata column = metadata.getColumn(col);
        switch (column.getType()) {
            case BOOLEAN:
                r.put(rec.getBool(col) ? "true" : "false");
                break;
            case BYTE:
                byte b = rec.get(col);
                if (b == Byte.MIN_VALUE) {
                    r.put("null");
                } else {
                    Numbers.append(r, b);
                }
                break;
            case DOUBLE:
                double d = rec.getDouble(col);
                if (Double.isNaN(d)) {
                    r.put("null");
                    break;
                }

                if (d == Double.POSITIVE_INFINITY) {
                    d = Double.MAX_VALUE;
                } else if (d == Double.NEGATIVE_INFINITY) {
                    d = Double.MIN_VALUE;
                }
                putDouble(r, d);
                break;

            case FLOAT:
                float f = rec.getFloat(col);
                if (Float.isNaN(f)) {
                    r.put("null");
                    break;
                }

                if (f == Float.POSITIVE_INFINITY) {
                    f = Float.MAX_VALUE;
                } else if (f == Float.NEGATIVE_INFINITY) {
                    f = Float.MIN_VALUE;
                }
                putDouble(r, f);
                break;
            case INT:
                int iint = rec.getInt(col);
                if (iint == Integer.MIN_VALUE) {
                    r.put("null");
                    break;
                }
                Numbers.append(r, iint);
                break;
            case LONG:
            case DATE:
                long ll = rec.getLong(col);
                if (ll == Long.MIN_VALUE) {
                    r.put("null");
                    break;
                }
                Numbers.append(r, rec.getLong(col));
                break;
            case SHORT:
                Numbers.append(r, rec.getShort(col));
                break;
            case STRING:
                sendStringOrNull(r, rec.getFlyweightStr(col), ctx);
                break;
            case SYMBOL:
                sendStringOrNull(r, rec.getSym(col), ctx);
                break;
            case BINARY:
                r.put('[');
                r.put(']');
                break;

            default:
                throw new IllegalArgumentException(String.format("Column type %s not supported", column.getType()));
        }
    }

    private static void sendStringOrNull(CharSink r, CharSequence str, $Context ctx) {
        if (str == null) {
            r.put("null");
        } else {
            r.put('\"');
            stringToJson(r, str, ctx);
            r.put('\"');
        }
    }

    private static void putDouble(ChunkedResponse r, double d) {
        Numbers.append(r, d, 10);
    }

    private static void stringToJson(CharSink r, CharSequence str, $Context ctx) {
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c < 128) {
                encodeControl(r, c);
            } else if (c < 0xD800) {
                encodeUnicode(r, ctx.encodingChar, ctx.encoded, c);
            } else {
                r.put("?");
            }
        }
    }

    private static void encodeUnicode(CharSink r, char[] charSource, byte[] encoded, char c) {
        // Encode utf-8
        charSource[0] = c;
        int len = UTF8Encoder.encode(charSource, 0, 1, encoded);
        for (int j = 0; j < len; j++) {
            r.put((char) encoded[j]);
        }
    }

    private static void encodeControl(CharSink r, char c) {
        switch (c) {
            case '\"':
            case '\\':
            case '/':
                r.put('\\');
                r.put(c);
                break;
            case '\b':
                r.put("\\b");
                break;
            case '\f':
                r.put("\\f");
                break;
            case '\n':
                r.put("\\n");
                break;
            case '\r':
                r.put("\\r");
                break;
            case '\t':
                r.put("\\t");
                break;
            default:
                r.put(c);
                break;
        }
    }

    @Nullable
    private Iterator<Record> executeQuery(ChunkedResponse r, $Context ctx) throws IOException {
        CharSequence query = ctx.query;
        try {
            // Prepare Context.
            JournalCachingFactory factory = factoryPool.get();
            ctx.factory = factory;

            ctx.recordSource = queryCompilerLocal.get().compileSource(factory, query);
            RecordCursor records = ctx.recordSource.prepareCursor(factory);

            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            sendQuery(r, query, ctx);
            RecordMetadata metadata = records.getMetadata();
            ctx.metadata = metadata;
            ctx.records = records;

            // List columns.
            int columnCount = metadata.getColumnCount();
            r.put(", \"columns\":[");

            for (int i = 0; i < columnCount; i++) {
                RecordColumnMetadata column = metadata.getColumn(i);
                r.put("{\"name\":\"");
                r.put(column.getName());
                r.put("\",\"type\":\"");
                r.put(column.getType().name());
                r.put("\"}");
                if (i < columnCount - 1) {
                    r.put(',');
                }
            }
            r.put("], \"result\":[");
            return records;
        } catch (ParserException pex) {
            ctx.info().$("Parser error executing query ").$(query).$(pex).$();
            sendException(r, query, QueryError.getPosition(), QueryError.getMessage(), 400, ctx);
        } catch (JournalException jex) {
            ctx.info().$("Server error executing query ").$(query).$(jex).$();
            sendException(r, query, -1, jex.getMessage(), 500, ctx);
        } catch (InterruptedException ex) {
            ctx.info().$("Error executing query. Server is shutting down. Query: ").$(query).$(ex).$();
            sendException(r, query, -1, "Server is shutting down.", 500, ctx);
        }
        return null;
    }

    private void sendDone(ChunkedResponse r, $Context ctx) throws DisconnectedChannelException, SlowWritableChannelException {
        if (ctx.count >= 0) {
            // Finita.
            r.bookmark();

            r.put(']');
            if (ctx.count > ctx.stop && !ctx.includeCount) {
                r.put(",\"moreExist\":true");
            }

            if (ctx.includeCount) {
                r.put(",\"totalCount\":");
                r.put(ctx.count);
            }
            r.put('}');
            ctx.count = -1;
            r.sendChunk();
        }
        r.done();
    }

    private static class $Context implements Mutable, Closeable {
        private static final Log LOG = LogFactory.getLog($Context.class);
        private final byte[] encoded = new byte[4];
        private final char[] encodingChar = new char[1];
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

    static {
        CharsetEncoder encoder = Charset.forName("utf-8").newEncoder();
        if (encoder instanceof ArrayEncoder) {
            UTF8Encoder = (ArrayEncoder) encoder;
        } else {
            UTF8Encoder = null;
        }
    }
}
