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
import com.questdb.ql.ChannelCheckCancellationHandler;
import com.questdb.std.Misc;
import com.questdb.std.Mutable;

import java.io.Closeable;


public abstract class AbstractQueryContext implements Mutable, Closeable {
    final ChannelCheckCancellationHandler cancellationHandler;
    final long fd;
    RecordCursorFactory recordCursorFactory;
    CharSequence query;
    RecordMetadata metadata;
    RecordCursor cursor;
    long count;
    long skip;
    long stop;
    Record record;
    int queryState = JsonQueryProcessor.QUERY_PREFIX;
    int columnIndex;

    public AbstractQueryContext(long fd, int cyclesBeforeCancel) {
        this.cancellationHandler = new ChannelCheckCancellationHandler(fd, cyclesBeforeCancel);
        this.fd = fd;
    }

    @Override
    public void clear() {
        metadata = null;
        cursor = Misc.free(cursor);
        record = null;
        if (recordCursorFactory != null) {
            // todo: avoid toString()
            JsonQueryProcessor.FACTORY_CACHE.get().put(query.toString(), recordCursorFactory);
            recordCursorFactory = null;
        }
        query = null;
        queryState = JsonQueryProcessor.QUERY_PREFIX;
        columnIndex = 0;
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
    }
}
