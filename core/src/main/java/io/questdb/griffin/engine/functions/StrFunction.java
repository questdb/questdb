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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.griffin.SqlUtil;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public abstract class StrFunction implements ScalarFunction {
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
        return SqlUtil.implicitCastStrAsByte(getStr(rec));
    }

    @Override
    public final char getChar(Record rec) {
        return SqlUtil.implicitCastStrAsChar(getStr(rec));
    }

    @Override
    public final long getDate(Record rec) {
        return SqlUtil.implicitCastStrAsDate(getStr(rec));
    }

    @Override
    public final double getDouble(Record rec) {
        return SqlUtil.implicitCastStrAsDouble(getStr(rec));
    }

    @Override
    public final float getFloat(Record rec) {
        return SqlUtil.implicitCastStrAsFloat(getStr(rec));
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
        return SqlUtil.implicitCastStrAsIPv4(getStr(rec));
    }

    @Override
    public final int getInt(Record rec) {
        return SqlUtil.implicitCastStrAsInt(getStr(rec));
    }

    @Override
    public final long getLong(Record rec) {
        return SqlUtil.implicitCastStrAsLong(getStr(rec));
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
    public final void getLong256(Record rec, CharSink sink) {
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
    public final short getShort(Record rec) {
        return SqlUtil.implicitCastStrAsShort(getStr(rec));
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        sink.put(getStr(rec));
    }

    @Override
    public int getStrLen(Record rec) {
        final CharSequence str = getStr(rec);
        return str == null ? TableUtils.NULL_LEN : str.length();
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
    public final long getTimestamp(Record rec) {
        return SqlUtil.implicitCastStrAsTimestamp(getStr(rec));
    }

    @Override
    public final int getType() {
        return ColumnType.STRING;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }
}
