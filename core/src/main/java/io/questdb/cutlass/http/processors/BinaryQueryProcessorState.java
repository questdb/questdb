/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.binary.StreamingColumnarSerializer;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

/**
 * Per-connection state for BinaryQueryProcessor.
 * Maintains query execution state across multiple resumeSend() calls.
 */
public class BinaryQueryProcessorState implements Mutable, Closeable {
    static final int QUERY_DATA = 1;
    // Query states
    static final int QUERY_PREFIX = 0;
    static final int QUERY_SUFFIX = 2;
    final BinaryQueryProcessor.BinaryDataSinkAdapter adapter;
    // Binary serialization
    final StreamingColumnarSerializer serializer = new StreamingColumnarSerializer();
    // Query text and execution
    final StringSink sqlText = new StringSink();
    // Connection context
    private final HttpConnectionContext httpConnectionContext;
    RecordCursor cursor;
    boolean headerSent = false;
    boolean queryCacheable;
    // State machine
    int queryState = QUERY_PREFIX;
    RecordCursorFactory recordCursorFactory;

    public BinaryQueryProcessorState(HttpConnectionContext httpConnectionContext) {
        this.httpConnectionContext = httpConnectionContext;
        this.adapter = new BinaryQueryProcessor.BinaryDataSinkAdapter(httpConnectionContext.getChunkedResponse());
    }

    @Override
    public void clear() {
        sqlText.clear();

        // Close cursor
        cursor = Misc.free(cursor);

        // Handle factory - return to cache or close
        if (recordCursorFactory != null) {
            if (queryCacheable) {
                httpConnectionContext.getSelectCache().put(sqlText, recordCursorFactory);
            } else {
                recordCursorFactory.close();
            }
            recordCursorFactory = null;
        }

        // Clear serializer
        serializer.clear();

        queryCacheable = false;
        queryState = QUERY_PREFIX;
        headerSent = false;
    }

    @Override
    public void close() {
        clear();
        // Close adapter
        Misc.free(adapter);
    }

    public HttpConnectionContext getHttpConnectionContext() {
        return httpConnectionContext;
    }
}
