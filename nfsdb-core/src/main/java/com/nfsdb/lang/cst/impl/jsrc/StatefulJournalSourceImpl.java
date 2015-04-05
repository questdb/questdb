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

package com.nfsdb.lang.cst.impl.jsrc;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.lang.cst.*;

public class StatefulJournalSourceImpl extends AbstractImmutableIterator<Record> implements GenericRecordSource, RecordSourceState {
    private final RecordSource<? extends Record> delegate;
    private Record current;

    public StatefulJournalSourceImpl(RecordSource<? extends Record> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Record currentRecord() {
        return current;
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Record next() {
        return current = delegate.next();
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
