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
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.http.ChunkedResponse;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.parser.QueryCompiler;
import com.nfsdb.ql.parser.QueryError;
import com.nfsdb.std.LocalValue;
import com.nfsdb.std.Mutable;
import sun.nio.cs.ArrayEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Iterator;

public class JsonHandler implements ContextHandler {
    private static final Log LOG = LogFactory.getLog(JsonHandler.class);
    private static final ArrayEncoder UTF8Encoder;
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
            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            sendQuery(r, "", null, null);
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
                r.bookmark();

                if (ctx.count > ctx.skip) {
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
                            stringToJson(r, metadata.getColumn(col).getName(), ctx.encodingChar, ctx.encoded);
                            r.put("\":");
                            putValue(ctx.encoded, ctx.encodingChar, r, metadata, ctx.current, col);

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

            if (ctx.count >= 0) {
                // Finita.
                r.bookmark();

                r.put(']');
                if (ctx.count > ctx.stop) {
                    r.put(",\"moreExist\":true");
                }

                if (ctx.includeCount) {
                    r.put(",\"totalCount\":");
                    r.put(ctx.count);
                }
                r.put('}');
                r.sendChunk();
            }
            r.done();

        } catch (ResponseContentBufferTooSmallException ex) {
            if (!r.resetToBookmark()) {
                // Nowhere to reset!
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

    private static void sendQuery(ChunkedResponse r, CharSequence query, char[] charSource, byte[] encoded) {
        r.put("{ \"query\": \"");
        stringToJson(r, query != null ? query : "", charSource, encoded);
        r.put("\"");
    }

    private static void sendException(ChunkedResponse r, CharSequence query, int position, CharSequence message, int status, char[] charSource, byte[] encoded) throws DisconnectedChannelException, SlowWritableChannelException {
        r.status(status, "application/json; charset=utf-8");
        r.sendHeader();
        sendQuery(r, query, charSource, encoded);
        r.put(", \"error\" : \"");
        stringToJson(r, message, charSource, encoded);
        r.put("\"");
        if (position >= 0) {
            r.put(", \"position\" : ");
            Numbers.append(r, position);
        }

        r.put("}");
        r.sendChunk();
        r.done();
    }

    private static void putValue(byte[] encoded, char[] encodingChar, ChunkedResponse r, RecordMetadata metadata, Record rec, int col) {
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
                sendStringOrNull(r, rec.getFlyweightStr(col), encodingChar, encoded);
                break;
            case SYMBOL:
                sendStringOrNull(r,rec.getSym(col), encodingChar, encoded);
                break;
            case BINARY:
                r.put('[');
                r.put(']');
                break;

            default:
                throw new IllegalArgumentException(String.format("Column type %s not supported", column.getType()));
        }
    }

    private static void sendStringOrNull(CharSink r, CharSequence str, char[] charSource, byte[] encoded) {
        if (str == null) {
            r.put("null");
        } else {
            r.put('\"');
            stringToJson(r, str, charSource, encoded);
            r.put('\"');
        }
    }

    private static void putDouble(ChunkedResponse r, double d) {
        Numbers.append(r, d, 10);
    }

    private static void stringToJson(CharSink r, CharSequence str, char[] charSource, byte[] encoded) {
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c < 128) {
                encodeControl(r, c);
            } else if (c < 0xD800) {
                encodeUnicode(r, charSource, encoded, c);
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

    private Iterator<Record> executeQuery(ChunkedResponse r, $Context ctx) throws IOException {
        CharSequence query = ctx.query;
        try {
            // Prepare Context.
            JournalCachingFactory factory = factoryPool.get();
            if (factory == null) {
                // Pool exhausted.
                LOG.debug().$("[").$(ctx.fd).$("] ").$("No journal factory available. Re-scheduling...").$();
                return null;
            }
            ctx.factory = factory;

            QueryCompiler qc = new QueryCompiler();
            RecordCursor records = qc.compile(factory, query);
            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            sendQuery(r, query, ctx.encodingChar, ctx.encoded);
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
            LOG.debug().$("[").$(ctx.fd).$("] ").$("Parser error executing query ").$(query).$(pex).$();
            sendException(r, query, QueryError.getPosition(), QueryError.getMessage(), 400, ctx.encodingChar, ctx.encoded);
        } catch (JournalException jex) {
            LOG.info().$("[").$(ctx.fd).$("] ").$("Server error executing query ").$(query).$(jex).$();
            sendException(r, query, -1, jex.getMessage(), 500, ctx.encodingChar, ctx.encoded);
        } catch (InterruptedException ex) {
            LOG.info().$("[").$(ctx.fd).$("] ").$("Error executing query ").$(query).$(ex).$();
            sendException(r, query, -1, "server busy, try again later", 500, ctx.encodingChar, ctx.encoded);
        }
        return null;
    }

    static {
        CharsetEncoder encoder = Charset.forName("utf-8").newEncoder();
        if (encoder instanceof ArrayEncoder) {
            UTF8Encoder = (ArrayEncoder) encoder;
        } else {
            UTF8Encoder = null;
        }
    }

    private static class $Context implements Mutable, Closeable {
        private static final Log LOG = LogFactory.getLog(JsonHandler.class);
        public final byte[] encoded = new byte[4];
        public final char[] encodingChar = new char[1];
        public CharSequence query;
        public RecordMetadata metadata;
        public Iterator<Record> records;
        public long count;
        public long skip;
        public long stop;
        public Record current;
        public boolean includeCount;
        public JournalCachingFactory factory;
        public long fd;

        @Override
        public void clear() {
            LOG.debug().$("[").$(fd).$("] ").$("Cleaning context").$();
            query = null;
            metadata = null;
            records = null;
            current = null;
            if (factory != null) {
                LOG.debug().$("[").$(fd).$("] ").$("Closing journal factory").$();
                factory.close();
                factory = null;
            }
        }

        @Override
        public void close() throws IOException {
            LOG.debug().$("[").$(fd).$("] ").$("Closing context").$();
            clear();
        }
    }
}
