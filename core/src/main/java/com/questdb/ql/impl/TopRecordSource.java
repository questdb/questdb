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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.ql.impl;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
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
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        this._top = lo.getLong(null);
        this._count = hi.getLong(null) - this._top;
        this.recordCursor = recordSource.prepareCursor(factory, cancellationHandler);
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
