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

package com.nfsdb.lang.cst.impl.virt;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.lang.cst.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class VirtualColumnRecordSource extends AbstractImmutableIterator<Record> implements GenericRecordSource, RecordSourceState {
    private final RecordSource<? extends Record> delegate;
    private final RecordMetadata metadata;
    private final VirtualRecord current;


    public VirtualColumnRecordSource(RecordSource<? extends Record> delegate, List<VirtualColumn> virtualColumns) {
        this.delegate = delegate;
        RecordMetadata dm = delegate.getMetadata();
        for (int i = 0, k = virtualColumns.size(); i < k; i++) {
            virtualColumns.get(i).configure(dm, this);
        }
        this.metadata = new VirtualRecordMetadata(dm, virtualColumns);
        this.current = new VirtualRecord(this.metadata, dm.getColumnCount(), virtualColumns);
    }

    @Override
    public Record currentRecord() {
        return current;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Record next() {
        current.setBase(delegate.next());
        return current;
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
