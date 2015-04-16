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
import com.nfsdb.ql.GenericRecordSource;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.RecordSource;

public class TopRecordSource extends AbstractImmutableIterator<Record> implements GenericRecordSource {

    private final RecordSource<? extends Record> delegate;
    private final int count;
    private int remaining;

    public TopRecordSource(int count, RecordSource<? extends Record> delegate) {
        this.delegate = delegate;
        this.count = count;
        this.remaining = count;
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public boolean hasNext() {
        return remaining > 0 && delegate.hasNext();
    }

    @Override
    public Record next() {
        remaining--;
        return delegate.next();
    }

    @Override
    public void reset() {
        delegate.reset();
        this.remaining = count;
    }
}
