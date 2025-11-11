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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public final class NullConstant implements ConstantFunction, FunctionExtension {

    public static final NullConstant NULL = new NullConstant();


    private final int type;

    private NullConstant() {
        this.type = ColumnType.NULL;
    }

    @Override
    public FunctionExtension extendedOps() {
        return this;
    }

    @Override
    public ArrayView getArray(Record rec) {
        return ArrayConstant.NULL;
    }

    @Override
    public int getArrayLength() {
        return TableUtils.NULL_LEN;
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return NullBinConstant.INSTANCE.getBin(null);
    }

    @Override
    public long getBinLen(Record rec) {
        return NullBinConstant.INSTANCE.getBinLen(null);
    }

    @Override
    public boolean getBool(Record rec) {
        return BooleanConstant.FALSE.getBool(null);
    }

    @Override
    public byte getByte(Record rec) {
        return ByteConstant.ZERO.getByte(null);
    }

    @Override
    public char getChar(Record rec) {
        return CharConstant.ZERO.getChar(null);
    }

    @Override
    public long getDate(Record rec) {
        return DateConstant.NULL.getDate(null);
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        sink.ofRawNull();
    }

    @Override
    public short getDecimal16(Record rec) {
        return Decimals.DECIMAL16_NULL;
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        sink.ofRawNull();
    }

    @Override
    public int getDecimal32(Record rec) {
        return Decimals.DECIMAL32_NULL;
    }

    @Override
    public long getDecimal64(Record rec) {
        return Decimals.DECIMAL64_NULL;
    }

    @Override
    public byte getDecimal8(Record rec) {
        return Decimals.DECIMAL8_NULL;
    }

    @Override
    public double getDouble(Record rec) {
        return DoubleConstant.NULL.getDouble(null);
    }

    @Override
    public float getFloat(Record rec) {
        return FloatConstant.NULL.getFloat(null);
    }

    @Override
    public byte getGeoByte(Record rec) {
        return GeoHashes.BYTE_NULL;
    }

    @Override
    public int getGeoInt(Record rec) {
        return GeoHashes.INT_NULL;
    }

    @Override
    public long getGeoLong(Record rec) {
        return GeoHashes.NULL;
    }

    @Override
    public short getGeoShort(Record rec) {
        return GeoHashes.SHORT_NULL;
    }

    @Override
    public int getIPv4(Record rec) {
        return IPv4Constant.NULL.getIPv4(null);
    }

    @Override
    public int getInt(Record rec) {
        return IntConstant.NULL.getInt(null);
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        return Interval.NULL;
    }

    @Override
    public long getLong(Record rec) {
        return LongConstant.NULL.getLong(null);
    }

    @Override
    public long getLong128Hi(Record rec) {
        return Numbers.LONG_NULL;
    }

    @Override
    public long getLong128Lo(Record rec) {
        return Numbers.LONG_NULL;
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        // intentionally left empty
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return Long256NullConstant.INSTANCE.getLong256A(null);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return Long256NullConstant.INSTANCE.getLong256B(null);
    }

    @Override
    public Record getRecord(Record rec) {
        return null;
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(Record rec) {
        return ShortConstant.ZERO.getShort(null);
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return StrConstant.NULL.getStrA(null);
    }

    @Override
    public CharSequence getStrA(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return StrConstant.NULL.getStrB(null);
    }

    @Override
    public CharSequence getStrB(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(Record rec) {
        return StrConstant.NULL.getStrLen(null);
    }

    @Override
    public int getStrLen(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return SymbolConstant.NULL.getSymbol(null);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return SymbolConstant.NULL.getSymbolB(null);
    }

    @Override
    public long getTimestamp(Record rec) {
        return TimestampConstant.TIMESTAMP_MICRO_NULL.getTimestamp(null);
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return null;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return null;
    }

    @Override
    public int getVarcharSize(Record rec) {
        return VarcharConstant.NULL.getVarcharSize(null);
    }

    @Override
    public boolean isNullConstant() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("null");
    }
}
