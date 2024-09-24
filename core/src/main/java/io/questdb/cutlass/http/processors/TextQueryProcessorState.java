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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class TextQueryProcessorState implements Mutable, Closeable {
    final StringSink query = new StringSink();
    private final HttpConnectionContext httpConnectionContext;
    int columnIndex;
    long count;
    boolean countRows = false;
    RecordCursor cursor;
    char delimiter = ',';
    String fileName;
    boolean hasNext;
    RecordMetadata metadata;
    boolean noMeta = false;
    boolean pausedQuery = false;
    int queryState;
    Record record;
    RecordCursorFactory recordCursorFactory;
    Rnd rnd;
    long skip;
    long stop;
    private boolean queryCacheable = false;

    public TextQueryProcessorState(HttpConnectionContext httpConnectionContext) {
        this.httpConnectionContext = httpConnectionContext;
        clear();
    }

    @Override
    public void clear() {
        delimiter = ',';
        fileName = null;
        metadata = null;
        rnd = null;
        record = null;
        cursor = Misc.free(cursor);
        if (null != recordCursorFactory) {
            if (queryCacheable) {
                httpConnectionContext.getSelectCache().put(query, recordCursorFactory);
            } else {
                recordCursorFactory.close();
            }
            recordCursorFactory = null;
        }
        queryCacheable = false;
        query.clear();
        queryState = JsonQueryProcessorState.QUERY_SETUP_FIRST_RECORD;
        columnIndex = 0;
        skip = 0;
        stop = 0;
        count = 0;
        noMeta = false;
        countRows = false;
        pausedQuery = false;
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
    }

    public long getFd() {
        return httpConnectionContext.getFd();
    }

    void setQueryCacheable(boolean queryCacheable) {
        this.queryCacheable = queryCacheable;
    }
}
