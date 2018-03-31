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

package com.questdb.griffin.engine.functions;

import com.questdb.common.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

public class RecordColumn extends AbstractFunction {
    private final int columnIndex;

    public RecordColumn(int columnIndex, int type, int position) {
        super(type, position);
        this.columnIndex = columnIndex;
    }

    @Override
    public byte get(Record rec) {
        return rec.getByte(columnIndex);
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return rec.getBin2(columnIndex);
    }

    @Override
    public boolean getBool(Record rec) {
        return rec.getBool(columnIndex);
    }

    @Override
    public long getDate(Record rec) {
        return rec.getDate(columnIndex);
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(columnIndex);
    }

    @Override
    public float getFloat(Record rec) {
        return rec.getFloat(columnIndex);
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(columnIndex);
    }

    @Override
    public long getLong(Record rec) {
        return rec.getLong(columnIndex);
    }

    @Override
    public short getShort(Record rec) {
        return rec.getShort(columnIndex);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return rec.getFlyweightStr(columnIndex);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        rec.getStr(columnIndex, sink);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return rec.getFlyweightStrB(columnIndex);
    }

    @Override
    public int getStrLen(Record rec) {
        return rec.getStrLen(columnIndex);
    }

    @Override
    public CharSequence getSym(Record rec) {
        return rec.getSym(columnIndex);
    }
}
