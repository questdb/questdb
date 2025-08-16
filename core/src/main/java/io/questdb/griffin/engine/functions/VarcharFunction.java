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
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlUtil;
import io.questdb.std.BinarySequence;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

public abstract class VarcharFunction implements Function {
    private final StringSink utf16SinkA = new StringSink();
    private final StringSink utf16SinkB = new StringSink();

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
        return SqlUtil.implicitCastVarcharAsByte(getVarcharA(rec));
    }

    @Override
    public char getChar(Record rec) {
        return SqlUtil.implicitCastVarcharAsChar(getVarcharA(rec));
    }

    @Override
    public long getDate(Record rec) {
        return SqlUtil.implicitCastStrAsDate(getStrA(rec));
    }

    @Override
    public double getDouble(Record rec) {
        return SqlUtil.implicitCastVarcharAsDouble(getVarcharA(rec));
    }

    @Override
    public float getFloat(Record rec) {
        return SqlUtil.implicitCastVarcharAsFloat(getVarcharA(rec));
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
        return SqlUtil.implicitCastVarcharAsInt(getVarcharA(rec));
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(Record rec) {
        return SqlUtil.implicitCastVarcharAsLong(getVarcharA(rec));
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
        return SqlUtil.implicitCastVarcharAsShort(getVarcharA(rec));
    }

    @Override
    public CharSequence getStrA(Record rec) {
        Utf8Sequence utf8seq = getVarcharA(rec);
        if (utf8seq == null) {
            return null;
        }
        if (utf8seq.isAscii()) {
            return utf8seq.asAsciiCharSequence();
        }
        utf16SinkA.clear();
        Utf8s.utf8ToUtf16(utf8seq, utf16SinkA);
        return utf16SinkA;
    }

    @Override
    public CharSequence getStrB(Record rec) {
        Utf8Sequence utf8seq = getVarcharB(rec);
        if (utf8seq == null) {
            return null;
        }
        if (utf8seq.isAscii()) {
            return utf8seq.asAsciiCharSequence();
        }
        utf16SinkB.clear();
        Utf8s.utf8ToUtf16(utf8seq, utf16SinkB);
        return utf16SinkB;
    }

    @Override
    public int getStrLen(Record rec) {
        return TableUtils.lengthOf(getStrA(rec));
    }

    @Override
    public final CharSequence getSymbol(Record rec) {
        return getStrA(rec);
    }

    @Override
    public final CharSequence getSymbolB(Record rec) {
        return getStrB(rec);
    }

    @Override
    public final long getTimestamp(Record rec) {
        return NanosTimestampDriver.INSTANCE.implicitCastVarchar(getVarcharA(rec));
    }

    @Override
    public final int getType() {
        return ColumnType.VARCHAR;
    }

    @Override
    public int getVarcharSize(Record rec) {
        Utf8Sequence utf8seq = getVarcharA(rec);
        if (utf8seq == null) {
            return TableUtils.NULL_LEN;
        }
        return utf8seq.size();
    }
}
