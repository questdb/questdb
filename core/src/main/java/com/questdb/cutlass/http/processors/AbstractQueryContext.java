/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cutlass.http.processors;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.std.AssociativeCache;
import com.questdb.std.Misc;
import com.questdb.std.Mutable;

import java.io.Closeable;


public abstract class AbstractQueryContext implements Mutable, Closeable {
    static final int QUERY_DATA_SUFFIX = 7;
    static final int QUERY_RECORD_SUFFIX = 6;
    static final int QUERY_RECORD_COLUMNS = 5;
    static final int QUERY_RECORD_START = 4;
    static final int QUERY_META_SUFFIX = 3;
    static final int QUERY_METADATA = 2;
    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    static final ThreadLocal<AssociativeCache<RecordCursorFactory>> FACTORY_CACHE = ThreadLocal.withInitial(() -> new AssociativeCache<>(8, 8));
    final long fd;
    RecordCursorFactory recordCursorFactory;
    CharSequence query;
    RecordMetadata metadata;
    RecordCursor cursor;
    long count;
    long skip;
    long stop;
    Record record;
    static final int QUERY_PREFIX = 1;
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
        query = null;
        queryState = QUERY_PREFIX;
        columnIndex = 0;
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
    }
}
