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

package com.questdb.ql.impl.join.hash;

import com.questdb.ql.AbstractRecord;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;

import java.io.OutputStream;

public class FakeRecord extends AbstractRecord {
    private long rowId;

    @Override
    public byte get(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getBin(int col, OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int col) {
        return rowId;
    }

    @Override
    public long getRowId() {
        return rowId;
    }

    @Override
    public short getShort(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStr(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(int col, CharSink sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSym(int col) {
        throw new UnsupportedOperationException();
    }

    public FakeRecord of(long rowId) {
        this.rowId = rowId;
        return this;
    }
}
