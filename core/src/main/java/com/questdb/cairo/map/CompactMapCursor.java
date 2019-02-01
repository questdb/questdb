/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cairo.map;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;

public class CompactMapCursor implements RecordCursor {

    private final CompactMapRecord record;
    private long offsetHi;
    private long nextOffset;

    public CompactMapCursor(CompactMapRecord record) {
        this.record = record;
    }

    @Override
    public void close() {
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (nextOffset < offsetHi) {
            record.of(nextOffset);
            nextOffset = record.getNextRecordOffset();
            return true;
        }
        return false;
    }

    @Override
    public MapRecord newRecord() {
        return record.clone();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        assert record instanceof CompactMapRecord;
        ((CompactMapRecord) record).of(atRowId);
    }

    @Override
    public void recordAt(long rowId) {
        record.of(rowId);
    }

    @Override
    public void toTop() {
        nextOffset = 0;
    }

    void of(long offsetHi) {
        this.nextOffset = 0;
        this.offsetHi = offsetHi;
    }
}
