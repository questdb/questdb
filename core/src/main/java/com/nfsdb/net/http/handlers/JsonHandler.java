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
import com.nfsdb.store.ColumnType;
import sun.nio.cs.ArrayEncoder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Iterator;

public class JsonHandler implements ContextHandler {
    private static final Log LOG = LogFactory.getLog(JsonHandler.class);
    private static final ArrayEncoder UTF8Encoder;
    private final JournalFactoryPool factoryPool;
    private final LocalValue<CharSequence> queryContext = new LocalValue<>();
    private final LocalValue<RecordMetadata> metadataContext = new LocalValue<>();
    private final LocalValue<Iterator<Record>> recordsContext = new LocalValue<>();
    private final LocalValue<Long> countContext = new LocalValue<>();
    private final LocalValue<Long> skipContext = new LocalValue<>();
    private final LocalValue<Long> stopContext = new LocalValue<>();
    private final LocalValue<Record> currentContext = new LocalValue<>();
    private final LocalValue<Boolean> includeCountContext = new LocalValue<>();
    private final LocalValue<JournalCachingFactory> factoryContext = new LocalValue<>();

    public JsonHandler(JournalFactoryPool factoryPool) {
        this.factoryPool = factoryPool;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        // Reused for UTF-8 encoding.
        final byte[] encoded = context.encoded;
        final char[] encodingChar = context.encodingChar;

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
        includeCountContext.set(context, withCount != null && Chars.equalsIgnoreCase(withCount, "true"));
        queryContext.set(context, query);
        countContext.set(context, 0L);
        skipContext.set(context, skip);
        stopContext.set(context, stop);

        if (executeQuery(r, context, encoded, encodingChar) == null) {
            return;
        }

        // Send records.
        resume(context);
    }

    @Override
    public void resume(IOContext context) throws IOException {
        // Reused for UTF-8 encoding. Thread local
        final byte[] encoded = context.encoded;
        final char[] encodingChar = context.encodingChar;
        ChunkedResponse r = context.chunkedResponse();

        Iterator<Record> records = recordsContext.get(context);
        if (records == null) {
            records = executeQuery(r, context, encoded, encodingChar);
            if (records == null) {
                return;
            }
        }

        try {
            RecordMetadata metadata = metadataContext.get(context);
            int columnCount = metadata.getColumnCount();

            Record current = currentContext.get(context);
            long count = countContext.get(context);
            long skip = skipContext.get(context);
            long stop = stopContext.get(context);
            boolean includeCount = includeCountContext.get(context);

            if (current == null && records.hasNext()) {
                current = records.next();
                count++;
            }

            while (current != null) {
                r.bookmark();

                if (count > skip) {
                    if (count > stop && !includeCount) {
                        break;
                    }

                    if (count <= stop) {
                        if (count > skip + 1) {
                            // Record separator.
                            r.put(',');
                        }
                        r.put("{");

                        // Columns.
                        for (int col = 0; col < columnCount; col++) {
                            r.put('\"');
                            stringToJson(r, metadata.getColumn(col).getName(), encodingChar, encoded);
                            r.put("\":");
                            putValue(encoded, encodingChar, r, metadata, current, col);

                            if (col < columnCount - 1) {
                                r.put(',');
                            }
                        }
                        r.put("}");
                        r.sendChunk();
                    }
                }

                if (records.hasNext()) {
                    current = records.next();
                    count++;
                } else {
                    current = null;
                }
                currentContext.set(context, current);
                countContext.set(context, count);
            }

            if (count >= 0) {
                // Finita.
                r.bookmark();

                r.put(']');
                if (count > stop) {
                    r.put(",\"moreExist\":true");
                }

                if (includeCount) {
                    r.put(",\"totalCount\":");
                    r.put(count);
                }
                r.put('}');
                countContext.set(context, -1L);
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
            case SYMBOL:
                CharSequence str = column.getType() == ColumnType.STRING ? rec.getFlyweightStr(col) : rec.getSym(col);
                if (str == null) {
                    r.put("null");
                } else {
                    r.put('\"');
                    stringToJson(r, str, encodingChar, encoded);
                    r.put('\"');
                }
                break;

            case BINARY:
                r.put('[');
                r.put(']');
                break;

            default:
                throw new IllegalArgumentException(String.format("Column type %s not supported", column.getType()));
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

    private Iterator<Record> executeQuery(ChunkedResponse r, IOContext context, byte[] encoded, char[] encodingChar) throws IOException {
        CharSequence query = queryContext.get(context);
        try {
            // Prepare Context.
            JournalCachingFactory factory = factoryPool.get();
            if (factory == null) {
                // Pool exhausted.
                LOG.debug().$("No journal factory available. Re-scheduling...").$();
                return null;
            }
            factoryContext.set(context, factory);

            QueryCompiler qc = new QueryCompiler();
            RecordCursor records = qc.compile(factory, query);
            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            sendQuery(r, query, encodingChar, encoded);
            RecordMetadata metadata = records.getMetadata();
            metadataContext.set(context, metadata);
            recordsContext.set(context, records);

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
            sendException(r, query, QueryError.getPosition(), QueryError.getMessage(), 400, encodingChar, encoded);
        } catch (JournalException jex) {
            sendException(r, query, -1, jex.getMessage(), 500, encodingChar, encoded);
        } catch (InterruptedException ex) {
            LOG.info().$("Error executing query ").$(query).$(ex).$();
            sendException(r, query, -1, "server busy, try again later", 500, encodingChar, encoded);
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
}
