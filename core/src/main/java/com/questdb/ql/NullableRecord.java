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

package com.questdb.ql;

import com.questdb.std.DirectInputStream;
import com.questdb.store.Record;

import java.io.OutputStream;

public class NullableRecord implements Record {
    private final Record record;
    private boolean _null = false;
    private Record rec;

    public NullableRecord(Record record) {
        this.record = this.rec = record;
    }

    @Override
    public byte getByte(int col) {
        return rec.getByte(col);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        rec.getBin(col, s);
    }

    @Override
    public DirectInputStream getBin(int col) {
        return rec.getBin(col);
    }

    @Override
    public long getBinLen(int col) {
        return rec.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        return rec.getBool(col);
    }

    @Override
    public long getDate(int col) {
        return rec.getDate(col);
    }

    @Override
    public double getDouble(int col) {
        return rec.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        return rec.getFloat(col);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return rec.getFlyweightStr(col);
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return rec.getFlyweightStrB(col);
    }

    @Override
    public int getInt(int col) {
        return rec.getInt(col);
    }

    @Override
    public long getLong(int col) {
        return rec.getLong(col);
    }

    @Override
    public long getRowId() {
        return rec.getRowId();
    }

    @Override
    public short getShort(int col) {
        return rec.getShort(col);
    }

    @Override
    public int getStrLen(int col) {
        return rec.getStrLen(col);
    }

    @Override
    public CharSequence getSym(int col) {
        return rec.getSym(col);
    }

    public void set_null(boolean _null) {
        if (this._null != _null) {
            this.rec = _null ? NullRecord.INSTANCE : record;
            this._null = _null;
        }
    }
}
