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

import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.str.CharSink;
import com.questdb.store.RowCursor;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class FilteredRowSource implements RowSource, RowCursor {

    private final RowSource delegate;
    private final VirtualColumn filter;
    private final JournalRecord rec = new JournalRecord();
    private RowCursor underlying;

    public FilteredRowSource(RowSource delegate, VirtualColumn filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.delegate.configure(metadata);
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade facade, CancellationHandler cancellationHandler) {
        delegate.prepare(factory, facade, cancellationHandler);
        filter.prepare(facade);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        this.underlying = delegate.prepareCursor(slice);
        this.rec.partition = slice.partition;
        return this;
    }

    @Override
    public void toTop() {
        delegate.toTop();
    }

    @Override
    public boolean hasNext() {
        while (underlying.hasNext()) {
            rec.rowid = underlying.next();
            if (filter.getBool(rec)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long next() {
        return rec.rowid;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("FilteredRowSource").put(',');
        sink.putQuoted("rsrc").put(':').put(delegate);
        sink.put('}');
    }

    @Override
    public String toString() {
        return "FilteredRowSource{}";
    }

}
