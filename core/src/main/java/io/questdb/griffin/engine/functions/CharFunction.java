/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public abstract class CharFunction implements ScalarFunction {
    private final int position;
    private final StringSink sinkA = new StringSink();
    private final StringSink sinkB = new StringSink();

    public CharFunction(int position) {
        this.position = position;
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
    public int getInt(Record rec) {
        return getChar(rec);
    }

    @Override
    public long getLong(Record rec) {
        return getChar(rec);
    }

    @Override
    public void getLong256(Record rec, CharSink sink) {
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
    public int getPosition() {
        return position;
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
    public final CharSequence getStr(Record rec) {
        final char value = getChar(rec);
        if (value == 0) {
            return null;
        }
        sinkA.clear();
        sinkA.put(value);
        return sinkA;
    }

    @Override
    public final void getStr(Record rec, CharSink sink) {
        final char value = getChar(rec);
        if (value > 0) {
            sink.put(value);
        }
    }

    @Override
    public final CharSequence getStrB(Record rec) {
        final char value = getChar(rec);
        if (value == 0) {
            return null;
        }
        sinkB.clear();
        sinkB.put(value);
        return sinkB;
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
}
