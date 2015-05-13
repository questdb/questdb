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

public class TopRecordSource extends AbstractImmutableIterator<Record> implements GenericRecordSource, RecordCursor<Record> {

    private final RecordSource<? extends Record> recordSource;
    private final int count;
    private RecordCursor<? extends Record> recordCursor;
    private int remaining;

    public TopRecordSource(int count, RecordSource<? extends Record> recordSource) {
        this.recordSource = recordSource;
        this.count = count;
        this.remaining = count;
    }

    @Override
    public RecordMetadata getMetadata() {
        return recordSource.getMetadata();
    }

    @Override
    public RecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory);
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
        this.remaining = count;
    }

    @Override
    public boolean hasNext() {
        return remaining > 0 && recordCursor.hasNext();
    }

    @Override
    public Record next() {
        remaining--;
        return recordCursor.next();
    }
}
