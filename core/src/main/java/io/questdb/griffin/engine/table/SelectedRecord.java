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

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

public class SelectedRecord implements Record {
    private final IntList columnCrossIndex;
    private Record base;

    public SelectedRecord(IntList columnCrossIndex) {
        this.columnCrossIndex = columnCrossIndex;
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        return base.getArray(getColumnIndex(col), columnType);
    }

    @Override
    public BinarySequence getBin(int col) {
        return base.getBin(getColumnIndex(col));
    }

    @Override
    public long getBinLen(int col) {
        return base.getBinLen(getColumnIndex(col));
    }

    @Override
    public boolean getBool(int col) {
        return base.getBool(getColumnIndex(col));
    }

    @Override
    public byte getByte(int col) {
        return base.getByte(getColumnIndex(col));
    }

    @Override
    public char getChar(int col) {
        return base.getChar(getColumnIndex(col));
    }

    @Override
    public long getDate(int col) {
        return base.getDate(getColumnIndex(col));
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        base.getDecimal128(getColumnIndex(col), sink);
    }

    @Override
    public short getDecimal16(int col) {
        return base.getDecimal16(getColumnIndex(col));
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        base.getDecimal256(getColumnIndex(col), sink);
    }

    @Override
    public int getDecimal32(int col) {
        return base.getDecimal32(getColumnIndex(col));
    }

    @Override
    public long getDecimal64(int col) {
        return base.getDecimal64(getColumnIndex(col));
    }

    @Override
    public byte getDecimal8(int col) {
        return base.getDecimal8(getColumnIndex(col));
    }

    @Override
    public double getDouble(int col) {
        return base.getDouble(getColumnIndex(col));
    }

    @Override
    public float getFloat(int col) {
        return base.getFloat(getColumnIndex(col));
    }

    @Override
    public byte getGeoByte(int col) {
        return base.getGeoByte(getColumnIndex(col));
    }

    @Override
    public int getGeoInt(int col) {
        return base.getGeoInt(getColumnIndex(col));
    }

    @Override
    public long getGeoLong(int col) {
        return base.getGeoLong(getColumnIndex(col));
    }

    @Override
    public short getGeoShort(int col) {
        return base.getGeoShort(getColumnIndex(col));
    }

    @Override
    public int getIPv4(int col) {
        return base.getIPv4(getColumnIndex(col));
    }

    @Override
    public int getInt(int col) {
        return base.getInt(getColumnIndex(col));
    }

    @Override
    public Interval getInterval(int col) {
        return base.getInterval(getColumnIndex(col));
    }

    @Override
    public long getLong(int col) {
        return base.getLong(getColumnIndex(col));
    }

    @Override
    public long getLong128Hi(int col) {
        return base.getLong128Hi(getColumnIndex(col));
    }

    @Override
    public long getLong128Lo(int col) {
        return base.getLong128Lo(getColumnIndex(col));
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        base.getLong256(getColumnIndex(col), sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return base.getLong256A(getColumnIndex(col));
    }

    @Override
    public Long256 getLong256B(int col) {
        return base.getLong256B(getColumnIndex(col));
    }

    @Override
    public Record getRecord(int col) {
        return base.getRecord(getColumnIndex(col));
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return base.getShort(getColumnIndex(col));
    }

    @Override
    public CharSequence getStrA(int col) {
        return base.getStrA(getColumnIndex(col));
    }

    @Override
    public CharSequence getStrB(int col) {
        return base.getStrB(getColumnIndex(col));
    }

    @Override
    public int getStrLen(int col) {
        return base.getStrLen(getColumnIndex(col));
    }

    @Override
    public CharSequence getSymA(int col) {
        return base.getSymA(getColumnIndex(col));
    }

    @Override
    public CharSequence getSymB(int col) {
        return base.getSymB(getColumnIndex(col));
    }

    @Override
    public long getTimestamp(int col) {
        return base.getTimestamp(getColumnIndex(col));
    }

    @Override
    public long getUpdateRowId() {
        return base.getUpdateRowId();
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return base.getVarcharA(getColumnIndex(col));
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return base.getVarcharB(getColumnIndex(col));
    }

    @Override
    public int getVarcharSize(int col) {
        return base.getVarcharSize(getColumnIndex(col));
    }

    public void of(Record record) {
        this.base = record;
    }

    private int getColumnIndex(int columnIndex) {
        return columnCrossIndex.getQuick(columnIndex);
    }

    Record getBaseRecord() {
        return base;
    }
}
