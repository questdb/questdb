/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.ql.*;

public class NoOpJournalRecordSource extends AbstractImmutableIterator<Record> implements JournalRecordSource<Record>, RandomAccessRecordCursor<Record> {

    private final JournalRecordSource<? extends Record> delegate;
    private RandomAccessRecordCursor<? extends Record> cursor;

    public NoOpJournalRecordSource(JournalRecordSource<? extends Record> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Record getByRowId(long rowId) {
        return cursor.getByRowId(rowId);
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public SymFacade getSymFacade() {
        return cursor.getSymFacade();
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Record next() {
        return null;
    }

    @Override
    public RandomAccessRecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.cursor = delegate.prepareCursor(factory);
        return this;
    }
}
