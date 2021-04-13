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
import io.questdb.griffin.UnsupportedConversionException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public abstract class StrFunction implements ScalarFunction {
    private final int position;

    public StrFunction(int position) {
        this.position = position;
    }

    @Override
    public final BinarySequence getBin(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Binary");
    }

    @Override
    public long getBinLen(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Binary");
    }

    @Override
    public final boolean getBool(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Bool");
    }

    @Override
    public final byte getByte(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Byte");
    }

    @Override
    public final char getChar(Record rec) {
        CharSequence val = getStr(rec);
        if (val.length() == 1) return val.charAt(0);

        throw UnsupportedConversionException.instance().put("cannot convert String to Char");
    }

    @Override
    public final long getDate(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Date");
    }

    @Override
    public final double getDouble(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Double");
    }

    @Override
    public final float getFloat(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Float");
    }

    @Override
    public final int getInt(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Int");
    }

    @Override
    public final long getLong(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Long");
    }

    @Override
    public final void getLong256(Record rec, CharSink sink) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Long256");
    }

    @Override
    public final Long256 getLong256A(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Long256");
    }

    @Override
    public final Long256 getLong256B(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Long256");
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public final RecordCursorFactory getRecordCursorFactory() {
        throw UnsupportedConversionException.instance().put("cannot convert String to RecordCursorFactory");
    }

    @Override
    public final short getShort(Record rec) {
        throw UnsupportedConversionException.instance().put("cannot convert String to Short");
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
        throw UnsupportedConversionException.instance().put("cannot convert String to Timestamp");
    }

    @Override
    public final int getType() {
        return ColumnType.STRING;
    }
}
