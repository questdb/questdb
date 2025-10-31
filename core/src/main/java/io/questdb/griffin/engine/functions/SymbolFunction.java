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
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Symbol API allows record cursor consumers to store "int" value of symbol function
 * and then retrieve CharSequence values via SymbolTable. Symbol Table is typically
 * populated by function dynamically, in that values that have not yet been returned via
 * getInt() are not cached.*
 */
public abstract class SymbolFunction implements Function, SymbolTable {
    private final Utf8StringSink utf8SinkA = new Utf8StringSink();
    private final Utf8StringSink utf8SinkB = new Utf8StringSink();

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
        CharSequence value = getSymbol(rec);
        return value == null ? 0 : value.charAt(0);
    }

    @Override
    public final long getDate(Record rec) {
        throw new UnsupportedOperationException();
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
    public final double getDouble(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final float getFloat(Record rec) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getLong(Record rec) {
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

    @Nullable
    public StaticSymbolTable getStaticSymbolTable() {
        return null;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return getSymbol(rec);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getSymbolB(rec);
    }

    @Override
    public final int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getTimestamp(Record rec) {
        return NanosTimestampDriver.INSTANCE.implicitCast(getSymbol(rec), ColumnType.SYMBOL);
    }

    @Override
    public final int getType() {
        return ColumnType.SYMBOL;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        final CharSequence cs = getStrA(rec);
        if (cs != null) {
            utf8SinkA.clear();
            utf8SinkA.put(cs);
            return utf8SinkA;
        }
        return null;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        final CharSequence cs = getStrB(rec);
        if (cs != null) {
            utf8SinkB.clear();
            utf8SinkB.put(cs);
            return utf8SinkB;
        }
        return null;
    }

    @Override
    public final int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }

    public abstract boolean isSymbolTableStatic();

    /**
     * A clone of function's symbol table to enable concurrent SQL execution.
     * During such execution symbol table clones will be assigned to individual executing
     * thread.
     *
     * @return clone of symbol table
     */
    public SymbolTable newSymbolTable() {
        return null;
    }

    @Override
    public boolean supportsParallelism() {
        return false;
    }
}
