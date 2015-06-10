/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.ql.*;

public class SelectedColumnsJournalRecordSource extends AbstractImmutableIterator<Record> implements JournalRecordSource<Record>, RandomAccessRecordCursor<Record> {
    private final JournalRecordSource<? extends Record> recordSource;
    private final RecordMetadata metadata;
    private final SelectedColumnsRecord record;
    private RandomAccessRecordCursor<? extends Record> recordCursor;

    public SelectedColumnsJournalRecordSource(JournalRecordSource<? extends Record> recordSource, ObjList<String> names) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names);
        this.record = new SelectedColumnsRecord(dm, names);
    }

    @Override
    public Record getByRowId(long rowId) {
        record.setBase(recordCursor.getByRowId(rowId));
        return record;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public SymFacade getSymFacade() {
        return recordCursor.getSymFacade();
    }

    @Override
    public boolean hasNext() {
        return recordCursor.hasNext();
    }

    @Override
    public Record next() {
        record.setBase(recordCursor.next());
        return record;
    }

    @Override
    public RandomAccessRecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory);
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
    }

    @Override
    public String toString() {
        return "SelectedColumnsRecordSource{" +
                "recordSource=" + recordSource +
                ", metadata=" + metadata +
                '}';
    }
}
