/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.cst.impl.rsrc;

import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.lang.cst.*;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.lang.cst.impl.virt.VirtualColumn;

public class FilteredRowSource extends AbstractRowSource implements RecordSourceState {

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
        super.configure(metadata);
        this.delegate.configure(metadata);
        this.filter.configureSource(this);
        this.rec = new JournalRecord(metadata);
    }

    @Override
    public Record currentRecord() {
        return rec;
    }

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        this.underlying = delegate.cursor(slice);
        this.rec.partition = slice.partition;
        return this;
    }

    @Override
    public boolean hasNext() {
        while (underlying.hasNext()) {
            rec.rowid = underlying.next();
            if (filter.getBool()) {
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
    public void reset() {
        delegate.reset();
    }
}
