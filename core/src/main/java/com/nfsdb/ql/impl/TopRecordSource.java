/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.AbstractRecordSource;
import com.nfsdb.ql.ops.VirtualColumn;

public class TopRecordSource extends AbstractRecordSource {

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
    public Record getByRowId(long rowId) {
        return recordCursor.getByRowId(rowId);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return recordCursor.getStorageFacade();
    }

    @Override
    public RecordMetadata getMetadata() {
        return recordSource.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        this._top = lo.getLong(null);
        this._count = hi.getLong(null) - this._top;
        this.recordCursor = recordSource.prepareCursor(factory);
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
        this._top = lo.getLong(null);
        this._count = hi.getLong(null) - this._top;
    }

    @Override
    public boolean supportsRowIdAccess() {
        return recordSource.supportsRowIdAccess();
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
