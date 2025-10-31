/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

public class VirtualRecord implements ColumnTypes, Record, QuietCloseable {
    private final int columnCount;
    private final ObjList<? extends Function> functions;
    private Record base;

    public VirtualRecord(ObjList<? extends Function> functions) {
        this.functions = functions;
        this.columnCount = functions.size();
    }

    @Override
    public void close() {
        Misc.freeObjList(functions);
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        return getFunction(col).getArray(base);
    }

    public Record getBaseRecord() {
        return base;
    }

    @Override
    public BinarySequence getBin(int col) {
        return getFunction(col).getBin(base);
    }

    @Override
    public long getBinLen(int col) {
        return getFunction(col).getBinLen(base);
    }

    @Override
    public boolean getBool(int col) {
        return getFunction(col).getBool(base);
    }

    @Override
    public byte getByte(int col) {
        return getFunction(col).getByte(base);
    }

    @Override
    public char getChar(int col) {
        return getFunction(col).getChar(base);
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnType(int columnIndex) {
        return getFunction(columnIndex).getType();
    }

    @Override
    public long getDate(int col) {
        return getFunction(col).getDate(base);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        getFunction(col).getDecimal128(base, sink);
    }

    @Override
    public short getDecimal16(int col) {
        return getFunction(col).getDecimal16(base);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        getFunction(col).getDecimal256(base, sink);
    }

    @Override
    public int getDecimal32(int col) {
        return getFunction(col).getDecimal32(base);
    }

    @Override
    public long getDecimal64(int col) {
        return getFunction(col).getDecimal64(base);
    }

    @Override
    public byte getDecimal8(int col) {
        return getFunction(col).getDecimal8(base);
    }

    @Override
    public double getDouble(int col) {
        return getFunction(col).getDouble(base);
    }

    @Override
    public float getFloat(int col) {
        return getFunction(col).getFloat(base);
    }

    public ObjList<? extends Function> getFunctions() {
        return functions;
    }

    @Override
    public byte getGeoByte(int col) {
        return getFunction(col).getGeoByte(base);
    }

    @Override
    public int getGeoInt(int col) {
        return getFunction(col).getGeoInt(base);
    }

    @Override
    public long getGeoLong(int col) {
        return getFunction(col).getGeoLong(base);
    }

    @Override
    public short getGeoShort(int col) {
        return getFunction(col).getGeoShort(base);
    }

    @Override
    public int getIPv4(int col) {
        return getFunction(col).getIPv4(base);
    }

    @Override
    public int getInt(int col) {
        return getFunction(col).getInt(base);
    }

    @Override
    public Interval getInterval(int col) {
        return getFunction(col).getInterval(base);
    }

    @Override
    public long getLong(int col) {
        return getFunction(col).getLong(base);
    }

    @Override
    public long getLong128Hi(int col) {
        return getFunction(col).getLong128Hi(base);
    }

    @Override
    public long getLong128Lo(int col) {
        return getFunction(col).getLong128Lo(base);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        getFunction(col).getLong256(base, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return getFunction(col).getLong256A(base);
    }

    @Override
    public Long256 getLong256B(int col) {
        return getFunction(col).getLong256B(base);
    }

    @Override
    public Record getRecord(int col) {
        return getFunction(col).extendedOps().getRecord(base);
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return getFunction(col).getShort(base);
    }

    @Override
    public CharSequence getStrA(int col) {
        return getFunction(col).getStrA(base);
    }

    @Override
    public CharSequence getStrB(int col) {
        return getFunction(col).getStrB(base);
    }

    @Override
    public int getStrLen(int col) {
        return getFunction(col).getStrLen(base);
    }

    @Override
    public CharSequence getSymA(int col) {
        return getFunction(col).getSymbol(base);
    }

    @Override
    public CharSequence getSymB(int col) {
        return getFunction(col).getSymbolB(base);
    }

    @Override
    public long getTimestamp(int col) {
        return getFunction(col).getTimestamp(base);
    }

    @Override
    public long getUpdateRowId() {
        return base.getUpdateRowId();
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return getFunction(col).getVarcharA(base);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return getFunction(col).getVarcharB(base);
    }

    @Override
    public int getVarcharSize(int col) {
        return getFunction(col).getVarcharSize(base);
    }

    public void of(Record record) {
        this.base = record;
    }

    private Function getFunction(int columnIndex) {
        return functions.getQuick(columnIndex);
    }
}
