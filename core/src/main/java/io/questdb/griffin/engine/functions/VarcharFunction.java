/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.SqlUtil;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.*;

public abstract class VarcharFunction implements ScalarFunction {
    private final StringSink utf16sinkA = new StringSink();
    private final StringSink utf16sinkB = new StringSink();

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
    public long getDate(Record rec) {
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
    public final int getIPv4(Record rec) {
        final Utf8Sequence value = getVarcharA(rec);
        return SqlUtil.implicitCastStrAsIPv4(value != null ? value.asAsciiCharSequence() : null);
    }

    @Override
    public int getInt(Record rec) {
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
    public final short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(Record rec, Utf8Sink utf8Sink) {
        utf8Sink.put(getVarcharA(rec));
    }

    @Override
    public CharSequence getStr(Record rec) {
        utf16sinkA.clear();
        Utf8s.utf8ToUtf16(getVarcharA(rec), utf16sinkA);
        return utf16sinkA;
    }

    @Override
    public void getStr(Record rec, Utf16Sink utf16Sink) {
        Utf8s.utf8ToUtf16(getVarcharA(rec), utf16Sink);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        utf16sinkB.clear();
        Utf8s.utf8ToUtf16(getVarcharA(rec), utf16sinkB);
        return utf16sinkB;
    }

    @Override
    public int getStrLen(Record rec) {
        return getStr(rec).length();
    }

    @Override
    public final CharSequence getSymbol(Record rec) {
        return getStr(rec);
    }

    @Override
    public final CharSequence getSymbolB(Record rec) {
        return getStrB(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getType() {
        return ColumnType.VARCHAR;
    }
}
