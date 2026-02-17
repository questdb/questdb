/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.sql;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.BinarySequence;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

public class DelegatingRecord implements Record {
    protected Record base;

    @Override
    public ArrayView getArray(int col, int columnType) {
        return base.getArray(col, columnType);
    }

    @Override
    public BinarySequence getBin(int col) {
        return base.getBin(col);
    }

    @Override
    public long getBinLen(int col) {
        return base.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        return base.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        return base.getByte(col);
    }

    @Override
    public char getChar(int col) {
        return base.getChar(col);
    }

    @Override
    public long getDate(int col) {
        return base.getDate(col);
    }

    @Override
    public double getDouble(int col) {
        return base.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        return base.getFloat(col);
    }

    @Override
    public byte getGeoByte(int col) {
        return base.getGeoByte(col);
    }

    @Override
    public int getGeoInt(int col) {
        return base.getGeoInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        return base.getGeoLong(col);
    }

    @Override
    public short getGeoShort(int col) {
        return base.getGeoShort(col);
    }

    @Override
    public int getIPv4(int col) {
        return base.getIPv4(col);
    }

    @Override
    public int getInt(int col) {
        return base.getInt(col);
    }

    @Override
    public Interval getInterval(int col) {
        return base.getInterval(col);
    }

    @Override
    public long getLong(int col) {
        return base.getLong(col);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        base.getLong256(col, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return base.getLong256A(col);
    }

    @Override
    public Long256 getLong256B(int col) {
        return base.getLong256B(col);
    }

    @Override
    public Record getRecord(int col) {
        return base.getRecord(col);
    }

    @Override
    public short getShort(int col) {
        return base.getShort(col);
    }

    @Override
    public CharSequence getStrA(int col) {
        return base.getStrA(col);
    }

    @Override
    public CharSequence getStrB(int col) {
        return base.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        return base.getStrLen(col);
    }

    @Override
    public CharSequence getSymA(int col) {
        return base.getSymA(col);
    }

    @Override
    public CharSequence getSymB(int col) {
        return base.getSymB(col);
    }

    @Override
    public long getTimestamp(int col) {
        return base.getTimestamp(col);
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return base.getVarcharA(col);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return base.getVarcharB(col);
    }

    @Override
    public int getVarcharSize(int col) {
        return base.getVarcharSize(col);
    }

    public void of(Record base) {
        this.base = base;
    }
}
