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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

public class UndefinedFunction implements ScalarFunction {
    public static UndefinedFunction INSTANCE = new UndefinedFunction();

    @Override
    public final BinarySequence getBin(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean getBool(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final char getChar(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDate(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(io.questdb.cairo.sql.Record rec) {
        return getByte(rec);
    }

    @Override
    public float getFloat(io.questdb.cairo.sql.Record rec) {
        return getByte(rec);
    }

    @Override
    public byte getGeoByte(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getGeoInt(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getGeoLong(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getGeoShort(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getIPv4(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(io.questdb.cairo.sql.Record rec) {
        return getByte(rec);
    }

    @Override
    public long getLong(io.questdb.cairo.sql.Record rec) {
        return getByte(rec);
    }

    @Override
    public long getLong128Hi(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong128Lo(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void getLong256(io.questdb.cairo.sql.Record rec, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Long256 getLong256A(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Long256 getLong256B(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(io.questdb.cairo.sql.Record rec) {
        return getByte(rec);
    }

    @Override
    public final CharSequence getStrA(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final CharSequence getStrB(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getStrLen(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final CharSequence getSymbol(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final CharSequence getSymbolB(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getTimestamp(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getType() {
        return ColumnType.UNDEFINED;
    }

    @Override
    public Utf8Sequence getVarcharA(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Utf8Sequence getVarcharB(io.questdb.cairo.sql.Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }
}
