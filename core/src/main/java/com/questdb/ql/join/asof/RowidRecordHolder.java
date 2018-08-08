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

package com.questdb.ql.join.asof;

import com.questdb.store.Record;
import com.questdb.store.RecordCursor;

public class RowidRecordHolder implements RecordHolder {
    private RecordCursor cursor;
    private long rowid = -1;

    @Override
    public void clear() {
        this.rowid = -1;
    }

    @Override
    public Record peek() {
        return rowid == -1 ? null : cursor.recordAt(this.rowid);
    }

    @Override
    public void setCursor(RecordCursor cursor) {
        this.cursor = cursor;
    }

    public void write(Record record) {
        this.rowid = record.getRowId();
    }

    @Override
    public void close() {
    }
}
