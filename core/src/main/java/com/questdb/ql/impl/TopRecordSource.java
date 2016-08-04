/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl;

import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSink;

public class TopRecordSource extends AbstractCombinedRecordSource {

    private final RecordSource recordSource;
    private final VirtualColumn lo;
    private final VirtualColumn hi;
    private long _top;
    private long _count;
    private RecordCursor recordCursor;

    public TopRecordSource(RecordSource recordSource, VirtualColumn lo, VirtualColumn hi) {
        this.recordSource = recordSource;
        this.lo = lo;
        this.hi = hi;
    }

    @Override
    public void close() {
        Misc.free(recordSource);
    }

    @Override
    public RecordMetadata getMetadata() {
        return recordSource.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
        this._top = lo.getLong(null);
        this._count = hi.getLong(null) - this._top;
        this.recordCursor = recordSource.prepareCursor(factory, cancellationHandler);
        return this;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return recordCursor.getStorageFacade();
    }

    @Override
    public boolean hasNext() {
        if (_top > 0) {
            return scrollToStart();
        } else {
            return _count > 0 && recordCursor.hasNext();
        }
    }

    @Override
    public Record next() {
        _count--;
        return recordCursor.next();
    }

    @Override
    public Record newRecord() {
        return recordCursor.newRecord();
    }

    @Override
    public Record recordAt(long rowId) {
        return recordCursor.recordAt(rowId);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        recordCursor.recordAt(record, atRowId);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return recordSource.supportsRowIdAccess();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("TopRecordSource").put(',');
        sink.putQuoted("low").put(':').put(lo.getLong(null)).put(',');
        sink.putQuoted("high").put(':').put(hi.getLong(null));
        sink.put('}');
    }

    private boolean scrollToStart() {
        if (_count > 0) {
            long top = this._top;
            while (top > 0 && recordCursor.hasNext()) {
                recordCursor.next();
                top--;
            }
            return (_top = top) == 0 && recordCursor.hasNext();
        }
        return false;
    }
}
