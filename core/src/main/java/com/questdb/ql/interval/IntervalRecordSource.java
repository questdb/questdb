/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.interval;

import com.questdb.parser.sql.IntervalCompiler;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.RecordSource;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.LongList;
import com.questdb.std.Misc;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;

public class IntervalRecordSource extends AbstractCombinedRecordSource {

    private final RecordSource delegate;
    private final LongList intervals;
    private final int timestampIndex;
    private final int intervalCount;
    private RecordCursor cursor;
    private Record record;
    private boolean needRecord = true;
    private int intervalIndex = 0;

    public IntervalRecordSource(RecordSource delegate, LongList intervals, int timestampIndex) {
        this.delegate = delegate;
        this.intervals = intervals;
        this.intervalCount = intervals.size() / 2;
        this.timestampIndex = timestampIndex;
    }

    @Override
    public void close() {
        Misc.free(delegate);
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        intervalIndex = 0;
        needRecord = true;
        this.cursor = delegate.prepareCursor(factory, cancellationHandler);
        return this;
    }

    @Override
    public Record getRecord() {
        return delegate.getRecord();
    }

    @Override
    public Record newRecord() {
        return delegate.newRecord();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return cursor.getStorageFacade();
    }

    @Override
    public void releaseCursor() {
        this.cursor.releaseCursor();
    }

    @Override
    public void toTop() {
        needRecord = true;
        intervalIndex = 0;
        this.cursor.toTop();
    }

    @Override
    public boolean hasNext() {
        while (true) {

            if (intervalIndex == intervalCount) {
                return false;
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
            if (IntervalCompiler.getIntervalHi(intervals, intervalIndex) < t) {
                needRecord = false;
                intervalIndex++;
                continue;
            }

            needRecord = true;
            // interval is below notional partition, skip to next partition
            if (IntervalCompiler.getIntervalLo(intervals, intervalIndex) <= t) {
                return true;
            }
        }
    }

    @Override
    public Record next() {
        return record;
    }

    @Override
    public Record recordAt(long rowId) {
        return cursor.recordAt(rowId);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        cursor.recordAt(record, atRowId);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return delegate.supportsRowIdAccess();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("IntervalRecordSource").put(',');
        sink.putQuoted("src").put(':').put(delegate).put(',');
        sink.putQuoted("interval").put(':').put(IntervalCompiler.asIntervalStr(intervals));
        sink.put('}');
    }
}
