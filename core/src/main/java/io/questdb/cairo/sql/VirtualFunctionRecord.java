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
import io.questdb.griffin.engine.join.JoinRecord;
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

public class VirtualFunctionRecord implements ColumnTypes, Record, QuietCloseable {
    private final int functionCount;
    private final ObjList<? extends Function> functions;
    private final JoinRecord internalJoinRecord;
    private Record baseRecord;

    public VirtualFunctionRecord(ObjList<? extends Function> functions, int virtualColumnReservedSlots) {
        this.functions = functions;
        this.functionCount = functions.size();
        // + 1 is a placeholder for conditional timestamp. Timestamp is not assigned in this class, but
        // rather may or may not have been added to the metadata, which resolved column indexes. There are
        // several places
        this.internalJoinRecord = new JoinRecord(virtualColumnReservedSlots);
    }

    @Override
    public void close() {
        Misc.freeObjList(functions);
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        return getFunction(col).getArray(internalJoinRecord);
    }

    public Record getBaseRecord() {
        return baseRecord;
    }

    @Override
    public BinarySequence getBin(int col) {
        return getFunction(col).getBin(internalJoinRecord);
    }

    @Override
    public long getBinLen(int col) {
        return getFunction(col).getBinLen(internalJoinRecord);
    }

    @Override
    public boolean getBool(int col) {
        return getFunction(col).getBool(internalJoinRecord);
    }

    @Override
    public byte getByte(int col) {
        return getFunction(col).getByte(internalJoinRecord);
    }

    @Override
    public char getChar(int col) {
        return getFunction(col).getChar(internalJoinRecord);
    }

    @Override
    public int getColumnCount() {
        return functionCount;
    }

    @Override
    public int getColumnType(int columnIndex) {
        return getFunction(columnIndex).getType();
    }

    @Override
    public long getDate(int col) {
        return getFunction(col).getDate(internalJoinRecord);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        getFunction(col).getDecimal128(internalJoinRecord, sink);
    }

    @Override
    public short getDecimal16(int col) {
        return getFunction(col).getDecimal16(internalJoinRecord);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        getFunction(col).getDecimal256(internalJoinRecord, sink);
    }

    @Override
    public int getDecimal32(int col) {
        return getFunction(col).getDecimal32(internalJoinRecord);
    }

    @Override
    public long getDecimal64(int col) {
        return getFunction(col).getDecimal64(internalJoinRecord);
    }

    @Override
    public byte getDecimal8(int col) {
        return getFunction(col).getDecimal8(internalJoinRecord);
    }

    @Override
    public double getDouble(int col) {
        return getFunction(col).getDouble(internalJoinRecord);
    }

    @Override
    public float getFloat(int col) {
        return getFunction(col).getFloat(internalJoinRecord);
    }

    @Override
    public byte getGeoByte(int col) {
        return getFunction(col).getGeoByte(internalJoinRecord);
    }

    @Override
    public int getGeoInt(int col) {
        return getFunction(col).getGeoInt(internalJoinRecord);
    }

    @Override
    public long getGeoLong(int col) {
        return getFunction(col).getGeoLong(internalJoinRecord);
    }

    @Override
    public short getGeoShort(int col) {
        return getFunction(col).getGeoShort(internalJoinRecord);
    }

    @Override
    public int getIPv4(int col) {
        return getFunction(col).getIPv4(internalJoinRecord);
    }

    @Override
    public int getInt(int col) {
        return getFunction(col).getInt(internalJoinRecord);
    }

    public JoinRecord getInternalJoinRecord() {
        return internalJoinRecord;
    }

    @Override
    public Interval getInterval(int col) {
        return getFunction(col).getInterval(internalJoinRecord);
    }

    @Override
    public long getLong(int col) {
        return getFunction(col).getLong(internalJoinRecord);
    }

    @Override
    public long getLong128Hi(int col) {
        return getFunction(col).getLong128Hi(internalJoinRecord);
    }

    @Override
    public long getLong128Lo(int col) {
        return getFunction(col).getLong128Lo(internalJoinRecord);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        getFunction(col).getLong256(internalJoinRecord, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return getFunction(col).getLong256A(internalJoinRecord);
    }

    @Override
    public Long256 getLong256B(int col) {
        return getFunction(col).getLong256B(internalJoinRecord);
    }

    @Override
    public Record getRecord(int col) {
        return getFunction(col).extendedOps().getRecord(internalJoinRecord);
    }

    @Override
    public long getRowId() {
        return baseRecord.getRowId();
    }

    @Override
    public short getShort(int col) {
        return getFunction(col).getShort(internalJoinRecord);
    }

    @Override
    public CharSequence getStrA(int col) {
        return getFunction(col).getStrA(internalJoinRecord);
    }

    @Override
    public CharSequence getStrB(int col) {
        return getFunction(col).getStrB(internalJoinRecord);
    }

    @Override
    public int getStrLen(int col) {
        return getFunction(col).getStrLen(internalJoinRecord);
    }

    @Override
    public CharSequence getSymA(int col) {
        return getFunction(col).getSymbol(internalJoinRecord);
    }

    @Override
    public CharSequence getSymB(int col) {
        return getFunction(col).getSymbolB(internalJoinRecord);
    }

    @Override
    public long getTimestamp(int col) {
        return getFunction(col).getTimestamp(internalJoinRecord);
    }

    @Override
    public long getUpdateRowId() {
        return baseRecord.getUpdateRowId();
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return getFunction(col).getVarcharA(internalJoinRecord);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return getFunction(col).getVarcharB(internalJoinRecord);
    }

    @Override
    public int getVarcharSize(int col) {
        return getFunction(col).getVarcharSize(internalJoinRecord);
    }

    public void of(Record record) {
        this.baseRecord = record;
        this.internalJoinRecord.of(this, record);
    }

    private Function getFunction(int columnIndex) {
        return functions.getQuick(columnIndex);
    }
}
