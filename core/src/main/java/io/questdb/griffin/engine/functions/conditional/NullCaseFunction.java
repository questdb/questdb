/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The CASE whose every branch is NULL. Its getters return what {@link NullConstant} returns,
 * so a cast of it, as in {@code (CASE WHEN x > 1 THEN null END)::SYMBOL}, reads NULL.
 */
public record NullCaseFunction(ObjList<Function> args) implements CaseFunction {

    @Override
    public ArrayView getArray(Record rec) {
        return NullConstant.NULL.getArray(rec);
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return NullConstant.NULL.getBin(rec);
    }

    @Override
    public long getBinLen(Record rec) {
        return NullConstant.NULL.getBinLen(rec);
    }

    @Override
    public boolean getBool(Record rec) {
        return NullConstant.NULL.getBool(rec);
    }

    @Override
    public byte getByte(Record rec) {
        return NullConstant.NULL.getByte(rec);
    }

    @Override
    public char getChar(Record rec) {
        return NullConstant.NULL.getChar(rec);
    }

    @Override
    public long getDate(Record rec) {
        return NullConstant.NULL.getDate(rec);
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        NullConstant.NULL.getDecimal128(rec, sink);
    }

    @Override
    public short getDecimal16(Record rec) {
        return NullConstant.NULL.getDecimal16(rec);
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        NullConstant.NULL.getDecimal256(rec, sink);
    }

    @Override
    public int getDecimal32(Record rec) {
        return NullConstant.NULL.getDecimal32(rec);
    }

    @Override
    public long getDecimal64(Record rec) {
        return NullConstant.NULL.getDecimal64(rec);
    }

    @Override
    public byte getDecimal8(Record rec) {
        return NullConstant.NULL.getDecimal8(rec);
    }

    @Override
    public double getDouble(Record rec) {
        return NullConstant.NULL.getDouble(rec);
    }

    @Override
    public float getFloat(Record rec) {
        return NullConstant.NULL.getFloat(rec);
    }

    @Override
    public byte getGeoByte(Record rec) {
        return NullConstant.NULL.getGeoByte(rec);
    }

    @Override
    public int getGeoInt(Record rec) {
        return NullConstant.NULL.getGeoInt(rec);
    }

    @Override
    public long getGeoLong(Record rec) {
        return NullConstant.NULL.getGeoLong(rec);
    }

    @Override
    public short getGeoShort(Record rec) {
        return NullConstant.NULL.getGeoShort(rec);
    }

    @Override
    public int getIPv4(Record rec) {
        return NullConstant.NULL.getIPv4(rec);
    }

    @Override
    public int getInt(Record rec) {
        return NullConstant.NULL.getInt(rec);
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        return NullConstant.NULL.getInterval(rec);
    }

    @Override
    public long getLong(Record rec) {
        return NullConstant.NULL.getLong(rec);
    }

    @Override
    public long getLong128Hi(Record rec) {
        return NullConstant.NULL.getLong128Hi(rec);
    }

    @Override
    public long getLong128Lo(Record rec) {
        return NullConstant.NULL.getLong128Lo(rec);
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        NullConstant.NULL.getLong256(rec, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return NullConstant.NULL.getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return NullConstant.NULL.getLong256B(rec);
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(Record rec) {
        return NullConstant.NULL.getShort(rec);
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return NullConstant.NULL.getStrA(rec);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return NullConstant.NULL.getStrB(rec);
    }

    @Override
    public int getStrLen(Record rec) {
        return NullConstant.NULL.getStrLen(rec);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return NullConstant.NULL.getSymbol(rec);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return NullConstant.NULL.getSymbolB(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return NullConstant.NULL.getTimestamp(rec);
    }

    @Override
    public int getType() {
        return ColumnType.NULL;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(Record rec) {
        return NullConstant.NULL.getVarcharA(rec);
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        return NullConstant.NULL.getVarcharB(rec);
    }

    @Override
    public int getVarcharSize(Record rec) {
        return NullConstant.NULL.getVarcharSize(rec);
    }
}
