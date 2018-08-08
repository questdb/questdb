/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql;

import com.questdb.parser.sql.model.ExprNode;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.Misc;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;

public class FilteredRecordSource extends AbstractCombinedRecordSource {

    private final RecordSource delegate;
    private final VirtualColumn filter;
    private final ExprNode filterNode;
    private RecordCursor cursor;
    private Record record;

    public FilteredRecordSource(RecordSource delegate, VirtualColumn filter, ExprNode filterNode) {
        this.delegate = delegate;
        this.filter = filter;
        // this value is borrowed from pool
        // and only here to print out plan
        // so plan must be serialized in sink before compiler reused
        // this is quite fragile approach but we better not create tree of objects
        // unnecessarily
        this.filterNode = filterNode;
    }

    @Override
    public void close() {
        Misc.free(delegate);
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        this.cursor = delegate.prepareCursor(factory, cancellationHandler);
        filter.prepare(cursor.getStorageFacade());
        return this;
    }

    @Override
    public Record getRecord() {
        return delegate.getRecord();
    }

    @Override
    public Record newRecord() {
        return delegate.newRecord();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return cursor.getStorageFacade();
    }

    @Override
    public void releaseCursor() {
        this.cursor.releaseCursor();
    }

    @Override
    public void toTop() {
        this.cursor.toTop();
    }

    @Override
    public boolean hasNext() {
        while (cursor.hasNext()) {
            record = cursor.next();
            if (filter.getBool(record)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Record next() {
        return record;
    }

    @Override
    public Record recordAt(long rowId) {
        return cursor.recordAt(rowId);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        this.cursor.recordAt(record, atRowId);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return delegate.supportsRowIdAccess();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("FilteredRecordSource").put(',');
        sink.putQuoted("src").put(':').put(delegate).put(',');
        sink.putQuoted("filter").put(':').put('"').put(filterNode).put('"');
        sink.put('}');
    }
}
