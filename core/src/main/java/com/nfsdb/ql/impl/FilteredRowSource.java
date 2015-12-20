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

package com.nfsdb.ql.impl;

import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.RowSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.VirtualColumn;

public class FilteredRowSource extends AbstractRowSource {

    private final RowSource delegate;
    private final VirtualColumn filter;
    private RowCursor underlying;
    private JournalRecord rec;

    public FilteredRowSource(RowSource delegate, VirtualColumn filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.delegate.configure(metadata);
        this.rec = new JournalRecord(metadata);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        this.underlying = delegate.prepareCursor(slice);
        this.rec.partition = slice.partition;
        return this;
    }

    @Override
    public void reset() {
        delegate.reset();
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
    public void prepare(StorageFacade facade) {
        delegate.prepare(facade);
        filter.prepare(facade);
    }

    @Override
    public String toString() {
        return "FilteredRowSource{}";
    }
}
