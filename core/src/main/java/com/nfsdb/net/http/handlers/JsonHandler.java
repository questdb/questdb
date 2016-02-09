/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net.http.handlers;

import com.nfsdb.ex.*;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.http.ChunkedResponse;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.parser.QueryCompiler;
import sun.nio.cs.ArrayEncoder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Iterator;

public class JsonHandler implements ContextHandler {
    private static final ArrayEncoder UTF8Encoder;
    private static final ThreadLocal threadLocalCharBuffer = new ThreadLocal() {
        protected char[] initialValue() {
            return new char[1];
        }
    };
    private static final ThreadLocal threadLocalByteBuffer = new ThreadLocal() {
        protected byte[] initialValue() {
            return new byte[4];
        }
    };
    private JournalReaderFactory factory;

    public JsonHandler(JournalReaderFactory journalFactory) {
        this.factory = journalFactory;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        // Reused for UTF-8 encoding.
        final byte[] encoded = (byte[]) threadLocalByteBuffer.get();
        final char[] encodingChar = (char[]) threadLocalCharBuffer.get();

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

        CharSequence withCount = context.request.getUrlParam("withCount");
        context.includeCount = withCount != null && Chars.equalsIgnoreCase(withCount, "true");

        try {
            // Prepare Context.
            QueryCompiler qc = new QueryCompiler(factory);
            RecordCursor<? extends Record> records = qc.compile(query);
            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            sendQuery(r, query, encodingChar, encoded);
            RecordMetadata metadata = records.getMetadata();
            context.metadata = metadata;
            context.records = records.iterator();
            context.count = 0;
            context.skip = skip;
            context.stop = stop;

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

            // Send records.
            resume(context);
        } catch (ParserException pex) {
            sendException(r, query, pex.getMessage(), 400, encodingChar, encoded);
        } catch (JournalException jex) {
            sendException(r, query, jex.getMessage(), 500, encodingChar, encoded);
        }
    }

    @Override
    public void resume(IOContext context) throws IOException {
        // Reused for UTF-8 encoding. Thread local
        final byte[] encoded = (byte[]) threadLocalByteBuffer.get();
        final char[] encodingChar = (char[]) threadLocalCharBuffer.get();
        ChunkedResponse r = context.chunkedResponse();

        try {
            Iterator<? extends Record> records = context.records;
            RecordMetadata metadata = context.metadata;
            int columnCount = metadata.getColumnCount();

            if (context.current == null && records.hasNext()) {
                context.current = records.next();
                context.count++;
            }

            while (context.current != null) {
                r.bookmark();

                if (context.count > context.skip) {
                    if (context.count > context.stop && !context.includeCount) {
                        break;
                    }

                    if (context.count <= context.stop) {
                        if (context.count > context.skip + 1) {
                            // Record separator.
                            r.put(',');
                        }
                        r.put("{");

                        // Columns.
                        for (int col = 0; col < columnCount; col++) {
                            r.put('\"');
                            stringToJson(r, metadata.getColumn(col).getName(), encodingChar, encoded);
                            r.put("\":");
                            putValue(encoded, encodingChar, r, metadata, context.current, col);

                            if (col < columnCount - 1) {
                                r.put(',');
                            }
                        }
                        r.put("}");
                        r.sendChunk();
                    }
                }

                if (records.hasNext()) {
                    context.current = records.next();
                    context.count++;
                } else {
                    context.current = null;
                }
            }
            r.bookmark();

            // Finita.
            r.put(']');
            if (context.count > context.stop) {
                r.put(",\"moreExist\":true");
            }

            if (context.includeCount) {
                r.put(",\"totalCount\":");
                r.put(context.count);
            }
            r.put('}');
            r.sendChunk();
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

    private static void sendException(ChunkedResponse r, CharSequence query, CharSequence message, int status, char[] charSource, byte[] encoded) throws DisconnectedChannelException, SlowWritableChannelException {
        r.status(status, "application/json; charset=utf-8");
        r.sendHeader();
        sendQuery(r, query, charSource, encoded);
        r.put(", \"error\" : \"");
        stringToJson(r, message, charSource, encoded);
        r.put("\"}");
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
            case SYMBOL:
            case STRING:
                CharSequence str = rec.getStr(col);
                if (str == null) {
                    r.put("null");
                } else {
                    r.put('\"');
                    stringToJson(r, rec.getStr(col), encodingChar, encoded);
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
                if (c != '\"' && c != '\\' && c != '/' && c != '\b' && c != '\f' && c != '\n' && c != '\r' && c != '\t') {
                    r.put(c);
                } else {
                    encodeControl(r, c);
                }
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
        if (c == '\"' || c == '\\' || c == '/') {
            r.put('\\');
            r.put(c);
            return;
        }

        if (c == '\b') {
            r.put("\\b");
        } else if (c == '\f') {
            r.put("\\f");
        } else if (c == '\n') {
            r.put("\\n");
        } else if (c == '\r') {
            r.put("\\r");
        } else if (c == '\t') {
            r.put("\\t");
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
