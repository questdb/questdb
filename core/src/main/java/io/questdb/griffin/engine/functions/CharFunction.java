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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public abstract class CharFunction implements ScalarFunction {
    private final StringSink utf16SinkA = new StringSink();
    private final StringSink utf16SinkB = new StringSink();

    private final Utf8StringSink utf8SinkA = new Utf8StringSink();
    private final Utf8StringSink utf8SinkB = new Utf8StringSink();

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
    public final long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(Record rec) {
        return getChar(rec);
    }

    @Override
    public float getFloat(Record rec) {
        return getChar(rec);
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
        return getChar(rec);
    }

    @Override
    public long getLong(Record rec) {
        return getChar(rec);
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
        return (short) getChar(rec);
    }

    @Override
    public final CharSequence getStrA(Record rec) {
        final char value = getChar(rec);
        if (value == 0) {
            return null;
        }
        utf16SinkA.clear();
        utf16SinkA.put(value);
        return utf16SinkA;
    }

    @Override
    public final CharSequence getStrB(Record rec) {
        final char value = getChar(rec);
        if (value == 0) {
            return null;
        }
        utf16SinkB.clear();
        utf16SinkB.put(value);
        return utf16SinkB;
    }

    @Override
    public final int getStrLen(Record rec) {
        final char value = getChar(rec);
        if (value == 0) {
            return TableUtils.NULL_LEN;
        }
        return 1;
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
    public long getTimestamp(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getType() {
        return ColumnType.CHAR;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        final char value = getChar(rec);
        if (value != 0) {
            utf8SinkA.clear();
            utf8SinkA.put(getChar(rec));
            return utf8SinkA;
        }
        return null;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        final char value = getChar(rec);
        if (value != 0) {
            utf8SinkB.clear();
            utf8SinkB.put(getChar(rec));
            return utf8SinkB;
        }
        return null;
    }

    @Override
    public final int getVarcharSize(Record rec) {
        final char value = getChar(rec);
        if (value == 0) {
            return TableUtils.NULL_LEN;
        }
        utf8SinkA.clear();
        utf8SinkA.put(getChar(rec));
        return utf8SinkA.size();
    }
}
