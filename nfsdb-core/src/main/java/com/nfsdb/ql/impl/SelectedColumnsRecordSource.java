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

package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.ObjList;
import com.nfsdb.ql.GenericRecordSource;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.RecordSource;

public class SelectedColumnsRecordSource extends AbstractImmutableIterator<Record> implements GenericRecordSource {
    private final RecordSource<? extends Record> delegate;
    private final RecordMetadata metadata;
    private final SelectedColumnsRecord record;

    public SelectedColumnsRecordSource(RecordSource<? extends Record> delegate, ObjList<String> names) {
        this.delegate = delegate;
        RecordMetadata dm = delegate.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names);
        this.record = new SelectedColumnsRecord(dm, names);
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
        record.setBase(delegate.next());
        return record;
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
