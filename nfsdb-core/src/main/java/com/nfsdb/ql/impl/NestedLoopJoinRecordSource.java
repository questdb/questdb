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

public class NestedLoopJoinRecordSource extends AbstractImmutableIterator<SplitRecord> implements RecordSource<SplitRecord>, RecordCursor<SplitRecord> {
    private final RecordSource<? extends Record> masterSource;
    private final RecordSource<? extends Record> slaveSource;
    private final SplitRecordMetadata metadata;
    private final SplitRecord record;
    private RecordCursor<? extends Record> masterCursor;
    private RecordCursor<? extends Record> slaveCursor;
    private boolean nextSlave = false;

    public NestedLoopJoinRecordSource(RecordSource<? extends Record> masterSource, RecordSource<? extends Record> slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;

        RecordMetadata mm = masterSource.getMetadata();
        this.metadata = new SplitRecordMetadata(mm, slaveSource.getMetadata());
        this.record = new SplitRecord(metadata, mm.getColumnCount());
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor<SplitRecord> prepareCursor(JournalReaderFactory factory) throws JournalException {
        masterCursor = masterSource.prepareCursor(factory);
        slaveCursor = slaveSource.prepareCursor(factory);
        return this;
    }

    @Override
    public void reset() {
        // cursor reset
        masterSource.reset();
        slaveSource.reset();
        nextSlave = false;
    }

    @Override
    public SymFacade getSymFacade() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return nextSlave || masterCursor.hasNext();
    }

    @Override
    public SplitRecord next() {
        if (!nextSlave) {
            record.setA(masterCursor.next());
            slaveSource.reset();
        }

        if (nextSlave || slaveCursor.hasNext()) {
            record.setB(slaveCursor.next());
            nextSlave = slaveCursor.hasNext();
        } else {
            record.setB(null);
            nextSlave = false;
        }
        return record;
    }
}
