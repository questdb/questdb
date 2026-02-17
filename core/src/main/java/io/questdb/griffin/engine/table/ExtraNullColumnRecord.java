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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.NullRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.constants.Long256NullConstant;
import io.questdb.std.BinarySequence;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

public class ExtraNullColumnRecord implements Record {
    private final int columnSplit;
    private Record base;

    public ExtraNullColumnRecord(int columnSplit) {
        this.columnSplit = columnSplit;
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        return col < columnSplit ? base.getArray(col, columnType) : null;
    }

    public Record getBaseRecord() {
        return base;
    }

    @Override
    public BinarySequence getBin(int col) {
        return col < columnSplit ? base.getBin(col) : null;
    }

    @Override
    public long getBinLen(int col) {
        return col < columnSplit ? base.getBinLen(col) : 0;
    }

    @Override
    public boolean getBool(int col) {
        return col < columnSplit && base.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        return col < columnSplit ? base.getByte(col) : (byte) Numbers.INT_NULL;
    }

    @Override
    public char getChar(int col) {
        return col < columnSplit ? base.getChar(col) : (char) 0;
    }

    @Override
    public long getDate(int col) {
        return col < columnSplit ? base.getDate(col) : Numbers.LONG_NULL;
    }

    @Override
    public double getDouble(int col) {
        return col < columnSplit ? base.getDouble(col) : Double.NaN;
    }

    @Override
    public float getFloat(int col) {
        return col < columnSplit ? base.getFloat(col) : Float.NaN;
    }

    @Override
    public byte getGeoByte(int col) {
        return col < columnSplit ? base.getGeoByte(col) : (byte) GeoHashes.NULL;
    }

    @Override
    public int getGeoInt(int col) {
        return col < columnSplit ? base.getGeoInt(col) : (int) GeoHashes.NULL;
    }

    @Override
    public long getGeoLong(int col) {
        return col < columnSplit ? base.getGeoLong(col) : GeoHashes.NULL;
    }

    @Override
    public short getGeoShort(int col) {
        return col < columnSplit ? base.getGeoShort(col) : (short) GeoHashes.NULL;
    }

    @Override
    public int getIPv4(int col) {
        return col < columnSplit ? base.getIPv4(col) : Numbers.IPv4_NULL;
    }

    @Override
    public int getInt(int col) {
        return col < columnSplit ? base.getInt(col) : Numbers.INT_NULL;
    }

    @Override
    public Interval getInterval(int col) {
        return col < columnSplit ? base.getInterval(col) : Interval.NULL;
    }

    @Override
    public long getLong(int col) {
        return col < columnSplit ? base.getLong(col) : Numbers.LONG_NULL;
    }

    @Override
    public long getLong128Hi(int col) {
        return col < columnSplit ? base.getLong128Hi(col) : Numbers.LONG_NULL;
    }

    @Override
    public long getLong128Lo(int col) {
        return col < columnSplit ? base.getLong128Lo(col) : Numbers.LONG_NULL;
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        if (col < columnSplit) {
            base.getLong256(col, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        return col < columnSplit ? base.getLong256A(col) : Long256NullConstant.INSTANCE;
    }

    @Override
    public Long256 getLong256B(int col) {
        return col < columnSplit ? base.getLong256B(col) : Long256NullConstant.INSTANCE;
    }

    @Override
    public Record getRecord(int col) {
        return col < columnSplit ? base.getRecord(col) : NullRecord.INSTANCE;
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return col < columnSplit ? base.getShort(col) : (short) Numbers.INT_NULL;
    }

    @Override
    public CharSequence getStrA(int col) {
        return col < columnSplit ? base.getStrA(col) : null;
    }

    @Override
    public CharSequence getStrB(int col) {
        return col < columnSplit ? base.getStrB(col) : null;
    }

    @Override
    public int getStrLen(int col) {
        return col < columnSplit ? base.getStrLen(col) : 0;
    }

    @Override
    public CharSequence getSymA(int col) {
        return col < columnSplit ? base.getSymA(col) : null;
    }

    @Override
    public CharSequence getSymB(int col) {
        return col < columnSplit ? base.getSymB(col) : null;
    }

    @Override
    public long getTimestamp(int col) {
        return col < columnSplit ? base.getTimestamp(col) : Numbers.LONG_NULL;
    }

    @Override
    public long getUpdateRowId() {
        return base.getUpdateRowId();
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return col < columnSplit ? base.getVarcharA(col) : null;
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return col < columnSplit ? base.getVarcharB(col) : null;
    }

    @Override
    public int getVarcharSize(int col) {
        return col < columnSplit ? base.getVarcharSize(col) : 0;
    }

    public void of(Record record) {
        this.base = record;
    }
}
