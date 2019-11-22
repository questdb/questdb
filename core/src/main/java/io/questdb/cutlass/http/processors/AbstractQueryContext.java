/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.AssociativeCache;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;

import java.io.Closeable;


public abstract class AbstractQueryContext implements Mutable, Closeable {
    static final int QUERY_RECORD_PREFIX = 9;
    static final int QUERY_SETUP_FIRST_RECORD = 8;
    static final int QUERY_SUFFIX = 7;
    static final int QUERY_RECORD_SUFFIX = 6;
    static final int QUERY_RECORD = 5;
    static final int QUERY_RECORD_START = 4;
    static final int QUERY_METADATA_SUFFIX = 3;
    static final int QUERY_METADATA = 2;
    static final int QUERY_PREFIX = 1;
    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    static final ThreadLocal<AssociativeCache<RecordCursorFactory>> FACTORY_CACHE = ThreadLocal.withInitial(() -> new AssociativeCache<>(8, 8));
    final long fd;
    RecordCursorFactory recordCursorFactory;
    final StringSink query = new StringSink();
    RecordMetadata metadata;
    RecordCursor cursor;
    long count;
    long skip;
    long stop;
    Record record;
    int queryState = QUERY_PREFIX;
    int columnIndex;

    public AbstractQueryContext(long fd) {
        this.fd = fd;
    }

    @Override
    public void clear() {
        metadata = null;
        cursor = Misc.free(cursor);
        record = null;
        if (recordCursorFactory != null) {
            // todo: avoid toString()
            FACTORY_CACHE.get().put(query.toString(), recordCursorFactory);
            recordCursorFactory = null;
        }
        query.clear();
        queryState = QUERY_PREFIX;
        columnIndex = 0;
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
    }
}
