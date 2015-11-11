/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.utils.Interval;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class IntervalJournalRecordSource extends AbstractImmutableIterator<Record> implements RecordSource<Record>, RecordCursor<Record> {

    private final RecordSource<? extends Record> delegate;
    private final IntervalSource intervalSource;
    private final int timestampIndex;
    private RecordCursor<? extends Record> cursor;
    private Record record;
    private boolean needInterval = true;
    private boolean needRecord = true;
    private Interval interval;

    public IntervalJournalRecordSource(RecordSource<? extends Record> delegate, IntervalSource intervalSource) {
        this.delegate = delegate;
        this.intervalSource = intervalSource;
        final RecordMetadata metadata = delegate.getMetadata();
        this.timestampIndex = metadata.getTimestampIndex();
    }

    @Override
    public Record getByRowId(long rowId) {
        return cursor.getByRowId(rowId);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return cursor.getStorageFacade();
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public RecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
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

    @Override
    public boolean supportsRowIdAccess() {
        return true;
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

    @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
    @Override
    public Record next() {
        return record;
    }
}
