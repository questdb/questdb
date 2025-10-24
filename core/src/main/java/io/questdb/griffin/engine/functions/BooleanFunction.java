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

package io.questdb.griffin.engine.functions;

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
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.NotNull;

public abstract class BooleanFunction implements Function {
    protected static final Utf8String UTF_8_FALSE = new Utf8String("false");
    protected static final Utf8String UTF_8_TRUE = new Utf8String("true");

    @Override
    public ArrayView getArray(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final BinarySequence getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(Record rec) {
        return (byte) (getBool(rec) ? 1 : 0);
    }

    @Override
    public char getChar(Record rec) {
        return getBool(rec) ? 'T' : 'F';
    }

    @Override
    public long getDate(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public final void getDecimal128(Record rec, Decimal128 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final short getDecimal16(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void getDecimal256(Record rec, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getDecimal32(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDecimal64(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final byte getDecimal8(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public float getFloat(Record rec) {
        return getBool(rec) ? 1 : 0;
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
    public final int getIPv4(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Record rec) {
        return getBool(rec) ? 1 : 0;
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
    public final void getLong256(Record rec, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Long256 getLong256A(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Long256 getLong256B(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(Record rec) {
        return (short) (getBool(rec) ? 1 : 0);
    }

    @Override
    public final CharSequence getStrA(Record rec) {
        return getStr0(rec);
    }

    @Override
    public final CharSequence getStrB(Record rec) {
        return getStr0(rec);
    }

    @Override
    public final int getStrLen(Record rec) {
        return getStr0(rec).length();
    }

    @Override
    public final CharSequence getSymbol(Record rec) {
        return getStr0(rec);
    }

    @Override
    public final CharSequence getSymbolB(Record rec) {
        return getStr0(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public final int getType() {
        return ColumnType.BOOLEAN;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return getVarchar0(rec);
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return getVarchar0(rec);
    }

    @Override
    public final int getVarcharSize(Record rec) {
        return getVarchar0(rec).size();
    }

    protected String getStr0(Record rec) {
        return getBool(rec) ? "true" : "false";
    }

    protected Utf8Sequence getVarchar0(Record rec) {
        return getBool(rec) ? UTF_8_TRUE : UTF_8_FALSE;
    }
}
