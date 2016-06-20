/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.model.ExprNode;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSink;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class FilteredJournalRecordSource extends AbstractCombinedRecordSource {

    private final RecordSource delegate;
    private final VirtualColumn filter;
    private final ExprNode filterNode;
    private RecordCursor cursor;
    private Record record;

    public FilteredJournalRecordSource(RecordSource delegate, VirtualColumn filter, ExprNode filterNode) {
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
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        this.cursor = delegate.prepareCursor(factory, cancellationHandler);
        filter.prepare(cursor.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public boolean supportsRowIdAccess() {
        return delegate.supportsRowIdAccess();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return cursor.getStorageFacade();
    }

    @Override
    public Record newRecord() {
        return this.cursor.newRecord();
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
    public boolean hasNext() {
        while (cursor.hasNext()) {
            record = cursor.next();
            if (filter.getBool(record)) {
                return true;
            }
        }
        return false;
    }

    @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
    @Override
    public Record next() {
        return record;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("FilteredJournalRecordSource").put(',');
        sink.putQuoted("src").put(':').put(delegate).put(',');
        sink.putQuoted("filter").put(':').put('"').put(filterNode).put('"');
        sink.put('}');
    }
}
