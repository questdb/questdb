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

package com.questdb.ql.ops.col;

import com.questdb.ql.ops.AbstractVirtualColumn;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;

public class ByteRecordSourceColumn extends AbstractVirtualColumn {
    private final int index;

    public ByteRecordSourceColumn(int index, int position) {
        super(ColumnType.BYTE, position);
        this.index = index;
    }

    @Override
    public byte get(Record rec) {
        return rec.getByte(index);
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getByte(index);
    }

    @Override
    public float getFloat(Record rec) {
        return rec.getByte(index);
    }

    @Override
    public int getInt(Record rec) {
        return rec.getByte(index);
    }

    @Override
    public long getLong(Record rec) {
        return rec.getByte(index);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void prepare(StorageFacade facade) {
    }
}
