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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
public class InnerSkipNullJoinRecordSource extends AbstractImmutableIterator<Record> implements GenericRecordSource, RecordCursor<Record> {

    private final RecordSource<? extends SplitRecord> recordSource;
    private RecordCursor<? extends SplitRecord> recordCursor;
    private Record data;

    public InnerSkipNullJoinRecordSource(RecordSource<? extends SplitRecord> recordSource) {
        this.recordSource = recordSource;
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
    }

    @Override
    public SymFacade getSymFacade() {
        return recordCursor.getSymFacade();
    }

    @Override
    public boolean hasNext() {
        SplitRecord data;

        while (recordCursor.hasNext()) {
            if ((data = recordCursor.next()).hasB()) {
                this.data = data;
                return true;
            }
        }
        this.data = null;
        return false;
    }

    @Override
    public Record next() {
        return data;
    }
}
