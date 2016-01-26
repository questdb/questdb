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
import com.nfsdb.exceptions.DisconnectedChannelException;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.ParserException;
import com.nfsdb.exceptions.SlowWritableChannelException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.http.ChunkedResponse;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.parser.QueryCompiler;
import java.io.IOException;
import java.util.Iterator;

public class JsonHandler implements ContextHandler {
    private JournalReaderFactory factory;
    private static final int PAGE_SIZE = 100;

    public JsonHandler(JournalReaderFactory journalFactory) {
        this.factory = journalFactory;
    }

    @Override
    public void handle(IOContext context) throws IOException {
        ChunkedResponse r = context.chunkedResponse();
        CharSequence query = context.request.getUrlParam("query");

        if (query == null || query.length() == 0) {
            r.status(200, "application/json; charset=utf-8");
            r.sendHeader();
            sendQuery(r, "");
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
            sendQuery(r, query);
            RecordMetadata metadata = records.getMetadata();
            context.metadata = metadata;
            context.records = records.iterator();

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
            sendException(r, query, pex.getMessage(), 400);
        }
        catch (JournalException jex) {
            sendException(r, query, jex.getMessage(), 500);
        }
    }

    private void sendQuery(ChunkedResponse r, CharSequence query) {
        r.put("{ \"query\": \"");
        stringToJson(r, query != null ? query : "");
        r.put("\"");
    }

    private void sendException(ChunkedResponse r, CharSequence query, CharSequence message, int status) throws DisconnectedChannelException, SlowWritableChannelException {
        r.status(status, "application/json; charset=utf-8");
        r.sendHeader();
        sendQuery(r, query);
        r.put(", \"error\" : \"");
        stringToJson(r, message);
        r.put("\"}");
        r.sendChunk();
        r.done();
    }

    @Override
    public void resume(IOContext context) throws IOException {
        ChunkedResponse r = context.chunkedResponse();
        Iterator<? extends Record> records = context.records;
        RecordMetadata metadata = context.metadata;
        int columnCount = metadata.getColumnCount();
        int i = 0;
        boolean hasNext = records.hasNext();
        while (hasNext) {
            Record rec = records.next();
            r.put("{");
            for (int col = 0; col < columnCount; col++) {
                r.put('\"');
                r.put(metadata.getColumn(col).getName());
                r.put("\":");
                RecordColumnMetadata column = metadata.getColumn(col);
                switch (column.getType()) {
                    case BOOLEAN:
                        r.put(rec.getBool(col) ? "true" : "false");
                        break;
                    case BYTE:
                        Numbers.append(r, rec.get(col));
                        break;
                    case DOUBLE:
                        Numbers.append(r, rec.getDouble(col), 10);
                        break;
                    case FLOAT:
                        Numbers.append(r, rec.getFloat(col), 10);
                        break;
                    case INT:
                        Numbers.append(r, rec.getInt(col));
                        break;
                    case LONG:
                        Numbers.append(r, rec.getLong(col));
                        break;
                    case SHORT:
                        Numbers.append(r, rec.getShort(col));
                        break;
                    case SYMBOL:
                    case STRING:
                        r.put('\"');
                        stringToJson(r, rec.getStr(col));
                        r.put('\"');
                        break;
                    case BINARY:
                        r.put('[');
                        r.put(']');
                        break;
                    case DATE:
                        Dates.appendDateTime(r, rec.getDate(col));
                        break;
                }

                if (col < columnCount - 1) {
                    r.put(',');
                }
            }

            r.put("}");
            hasNext = records.hasNext();
            if (hasNext) {
                r.put(",");
            }
            if (hasNext && ++i % PAGE_SIZE == 0) {
                r.sendChunk();
            }
        }
        r.put("] }");
        r.sendChunk();
        r.done();
    }

    private void stringToJson(CharSink r, CharSequence str) {
        for(int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c != '\"' && c != '\\' && c != '/' && c != '\b' && c != '\f'  && c != '\n' && c != '\r' && c != '\t') {
                r.put(str.charAt(i));
            }
            else {
                encodeControl(r, c);
            }
        }
    }

    private void encodeControl(CharSink r, char c) {
        if (c == '\"' ||  c == '\\' || c == '/') {
            r.put("\\");
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
