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
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.ql.*;
import com.nfsdb.utils.Interval;

public class IntervalJournalRecordSource extends AbstractImmutableIterator<Record> implements JournalRecordSource<Record>, RandomAccessRecordCursor<Record> {

    private final JournalRecordSource<? extends Record> delegate;
    private final IntervalSource intervalSource;
    private final int timestampIndex;
    private RandomAccessRecordCursor<? extends Record> cursor;
    private Record record;
    private boolean needInterval = true;
    private boolean needRecord = true;
    private Interval interval;

    public IntervalJournalRecordSource(JournalRecordSource<? extends Record> delegate, IntervalSource intervalSource) {
        this.delegate = delegate;
        this.intervalSource = intervalSource;
        this.timestampIndex = delegate.getMetadata().getColumnIndex(delegate.getMetadata().getTimestampMetadata().getName());
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
    public SymFacade getSymFacade() {
        return cursor.getSymFacade();
    }

    @Override
    public boolean hasNext() {
        while (true) {

            if (needInterval) {
                if (intervalSource.hasNext()) {
                    interval = intervalSource.next();
                } else {
                    return false;
                }
            }

            if (needRecord) {
                if (cursor.hasNext()) {
                    record = cursor.next();
                } else {
                    return false;
                }
            }

            long t = record.getDate(timestampIndex);


            // interval is fully above notional partition interval, skip to next interval
            if (interval.getHi() < t) {
                needRecord = false;
                needInterval = true;
                continue;
            }

            // interval is below notional partition, skip to next partition
            if (interval.getLo() > t) {
                needRecord = true;
                needInterval = false;
                continue;
            }

            needRecord = true;
            return true;
        }
    }

    @Override
    public Record next() {
        return record;
    }

    @Override
    public RandomAccessRecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.cursor = delegate.prepareCursor(factory);
        return this;
    }

    @Override
    public void reset() {
        delegate.reset();
        intervalSource.reset();
        needInterval = true;
        needRecord = true;
    }
}
