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
import com.questdb.std.Numbers;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;

import java.io.OutputStream;

public class NullRecord implements Record {

    public static final NullRecord INSTANCE = new NullRecord();

    private NullRecord() {
    }

    @Override
    public void getBin(int col, OutputStream s) {
    }

    @Override
    public DirectInputStream getBin(int col) {
        return null;
    }

    @Override
    public long getBinLen(int col) {
        return -1L;
    }

    @Override
    public boolean getBool(int col) {
        return false;
    }

    @Override
    public byte getByte(int col) {
        return 0;
    }

    @Override
    public long getDate(int col) {
        return Numbers.LONG_NaN;
    }

    @Override
    public double getDouble(int col) {
        return Double.NaN;
    }

    @Override
    public float getFloat(int col) {
        return Float.NaN;
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return null;
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return null;
    }

    @Override
    public int getInt(int col) {
        return Numbers.INT_NaN;
    }

    @Override
    public long getLong(int col) {
        return Numbers.LONG_NaN;
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        return 0;
    }

    @Override
    public void getStr(int col, CharSink sink) {
    }

    @Override
    public int getStrLen(int col) {
        return -1;
    }

    @Override
    public CharSequence getSym(int col) {
        return null;
    }
}
