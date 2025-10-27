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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
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

public record NullCaseFunction(ObjList<Function> args) implements CaseFunction {

    @Override
    public ArrayView getArray(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BinarySequence getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getDecimal16(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDecimal32(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDecimal64(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getDecimal8(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getGeoByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getGeoInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getGeoLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getGeoShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getIPv4(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong128Hi(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong128Lo(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long256 getLong256A(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long256 getLong256B(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTimestamp(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getType() {
        return ColumnType.NULL;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }
}
