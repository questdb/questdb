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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public abstract class TimestampFunction implements Function {
    protected TimestampDriver timestampDriver;
    protected int timestampType;

    public TimestampFunction(int timestampType) {
        assert ColumnType.isTimestamp(timestampType);
        this.timestampType = timestampType;
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
    }

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
    public final boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDate(Record rec) {
        final long value = getTimestamp(rec);
        return timestampDriver.toDate(value);
    }

    @Override
    public final double getDouble(Record rec) {
        final long val = getTimestamp(rec);
        return val != Numbers.LONG_NULL ? val : Double.NaN;
    }

    @Override
    public final float getFloat(Record rec) {
        final long val = getTimestamp(rec);
        return val != Numbers.LONG_NULL ? val : Float.NaN;
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
    public final int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getLong(Record rec) {
        return getTimestamp(rec);
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
    public final short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final CharSequence getStrA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final CharSequence getStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final CharSequence getSymbol(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final CharSequence getSymbolB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getType() {
        return timestampType;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }

    public void setType(int type) {
        this.timestampType = type;
        this.timestampDriver = ColumnType.getTimestampDriver(type);
    }
}
