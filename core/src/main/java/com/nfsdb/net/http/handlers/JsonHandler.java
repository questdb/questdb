/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.net.http.handlers;
/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
import com.nfsdb.exceptions.*;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.http.ChunkedResponse;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.parser.QueryCompiler;
import sun.misc.FloatingDecimal;
import sun.nio.cs.ArrayEncoder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Iterator;

public class JsonHandler implements ContextHandler {
    private JournalReaderFactory factory;
    private static final int PAGE_SIZE = 100;
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
    private static final ThreadLocal threadLocalStringBuilder = new ThreadLocal() {
        protected StringBuilder initialValue() {
            return new StringBuilder(20);
        }
    };

    static {
        CharsetEncoder encoder = Charset.forName("utf-8").newEncoder();
        if (encoder instanceof ArrayEncoder) {
            UTF8Encoder = (ArrayEncoder)encoder;
        }
        else {
            UTF8Encoder = null;
        }
    }

    public JsonHandler(JournalReaderFactory journalFactory) {
        this.factory = journalFactory;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        // Reused for UTF-8 encoding.
        final byte[] encoded = (byte[])threadLocalByteBuffer.get();
        final char[] encodingChar = (char[])threadLocalCharBuffer.get();

        ChunkedResponse r = context.chunkedResponse();
        CharSequence query = context.request.getUrlParam("query");
        long skip = 0;
        long stop = -1;

        CharSequence limit = context.request.getUrlParam("limit");

        if (limit != null) {
            int sepPos = separatorPos(limit);
            try {
                if (sepPos > 0) {
                    skip = Numbers.parseLong(limit, 0, sepPos);
                    if (sepPos + 1 < limit.length()) {
                        stop = Numbers.parseLong(limit, sepPos + 1, limit.length());
                    }
                }
                else {
                    stop = Numbers.parseLong(limit);
                }
            } catch (NumericException ex) {
            }
        }

        if (query == null || query.length() == 0) {
            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            sendQuery(r, "", null, null);
            r.put("\"}");
            r.sendChunk();
            r.done();
            return;
        }

        QueryCompiler qc = new QueryCompiler(factory);
        try {
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
            for(int i = 0; i < columnCount; i++) {
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

            resume(context);
        }
        catch (ParserException pex) {
            sendException(r, query, pex.getMessage(), 400, encodingChar, encoded);
        }
        catch (JournalException jex) {
            sendException(r, query, jex.getMessage(), 500, encodingChar, encoded);
        }
    }

    @Override
    public void resume(IOContext context) throws IOException {
        // Reused for UTF-8 encoding. Thread local
        final byte[] encoded = (byte[]) threadLocalByteBuffer.get();
        final char[] encodingChar = (char[]) threadLocalCharBuffer.get();
        final StringBuilder sb = (StringBuilder) threadLocalStringBuilder.get();
        ChunkedResponse r = context.chunkedResponse();

        while (true) {
            try {
                // Ditch check that we're not kicked out after sending last record.
                boolean moreExists = context.stop >= 0 && context.count > context.stop;
                if (!moreExists) {
                    Iterator<? extends Record> records = context.records;
                    RecordMetadata metadata = context.metadata;
                    int columnCount = metadata.getColumnCount();

                    while (context.current != null || records.hasNext()) {
                        if (context.current == null) {
                            context.current = records.next();
                            context.count++;
                        }

                        if (context.count > context.skip) {
                            if (context.stop >= 0 && context.count > context.stop) {
                                moreExists = true;
                                break;
                            }

                            r.bookmark();
                            if (context.count > context.skip + 1) {
                                // Record separator.
                                r.put(',');
                            }
                            r.put("{");
                            for (int col = 0; col < columnCount; col++) {
                                r.put('\"');
                                stringToJson(r, metadata.getColumn(col).getName(), encodingChar, encoded);
                                r.put("\":");
                                putValue(encoded, encodingChar, sb, r, metadata, context.current, col);

                                if (col < columnCount - 1) {
                                    r.put(',');
                                }
                            }

                            r.put("}");
                        }
                        context.current = null;
                    }
                }
                r.put(']');
                if (moreExists) {
                    r.put(",\"moreExist\":true");
                }
                r.put('}');
                r.sendChunk();
                r.done();
            } catch (ResponseContentBufferTooSmallException ex) {
                if (!r.resetToBookmark()){
                    // Nowhere to reset!
                    throw ex;
                }
                r.sendChunk();
            }
        }
    }

    private static int separatorPos(CharSequence str){
        if (str == null) return -1;
        for(int i =0; i < str.length();i++){
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

    private static void putValue(byte[] encoded, char[] encodingChar, StringBuilder sb, ChunkedResponse r, RecordMetadata metadata, Record rec, int col) {
        RecordColumnMetadata column = metadata.getColumn(col);
        switch (column.getType()) {
            case BOOLEAN:
                r.put(rec.getBool(col) ? "true" : "false");
                break;
            case BYTE:
                byte b = rec.get(col);
                if (b == Byte.MIN_VALUE) {
                    r.put("null");
                }
                else {
                    Numbers.append(r, b);
                }
                break;
            case DOUBLE:
                double d = rec.getDouble(col);
                if (d == Double.NaN) {
                    r.put(null);
                    break;
                }

                if (d == Double.POSITIVE_INFINITY) {
                    d = Double.MAX_VALUE;
                }
                else if (d == Double.NEGATIVE_INFINITY) {
                    d = Double.MIN_VALUE;
                }
                putDouble(r, d, sb);
                break;

            case FLOAT:
                float f = rec.getFloat(col);
                if (f == Float.NaN) {
                    r.put(null);
                    break;
                }

               if (f == Float.POSITIVE_INFINITY) {
                   f = Float.MAX_VALUE;
                }
                else if (f == Float.NEGATIVE_INFINITY) {
                   f = Float.MIN_VALUE;
                }
                putDouble(r, f, sb);
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
                }
                else {
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
                throw new IllegalArgumentException(String.format("Column type %s not supported",column.getType()));
        }
    }

    private static void putDouble(ChunkedResponse r, double d, StringBuilder stringBuilder) {
        stringBuilder.setLength(0);
        FloatingDecimal.BinaryToASCIIConverter converter = FloatingDecimal.getBinaryToASCIIConverter(d);
        converter.appendTo(stringBuilder);
        for(int i = 0; i < stringBuilder.length(); i++){
            r.put(stringBuilder.charAt(i));
        }
    }

    private static void stringToJson(CharSink r, CharSequence str, char[] charSource, byte[] encoded) {
        for(int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c < 128) {
                if (c != '\"' && c != '\\' && c != '/' && c != '\b' && c != '\f' && c != '\n' && c != '\r' && c != '\t') {
                    r.put(c);
                } else {
                    encodeControl(r, c);
                }
            }
            else if (c < 0xD800) {
                encodeUnicode(r, charSource, encoded, c);
            }
            else {
                throw new UnsupportedCharsetException("UTF-18 characters up to 0xD800 are supported only ");
            }
        }
    }

    private static void encodeUnicode(CharSink r, char[] charSource, byte[] encoded, char c) {
        if (UTF8Encoder == null) {
            throw new UnsupportedCharsetException("UTF-18 characters up to 0x80 are supported only.");
        }
        // Encode utf-8
        charSource[0] = c;
        int len = UTF8Encoder.encode(charSource, 0, 1, encoded);
        for(int j = 0; j < len; j++){
            r.put((char)encoded[j]);
        }
    }

    private static void encodeControl(CharSink r, char c) {
        if (c == '\"' ||  c == '\\' || c == '/') {
            r.put('\\');
            r.put(c);
            return;
        }

        if (c == '\b') {
            r.put("\\b");
        }
        else if (c == '\f') {
            r.put("\\f");
        }
        else if (c == '\n') {
            r.put("\\n");
        }
        else if (c == '\r') {
            r.put("\\r");
        }
        else if (c == '\t') {
            r.put("\\t");
        }
    }
}
