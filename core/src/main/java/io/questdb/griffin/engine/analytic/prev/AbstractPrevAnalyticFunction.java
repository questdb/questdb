/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.analytic.prev;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.str.CharSink;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.io.Closeable;
import java.io.IOException;

public abstract class AbstractPrevAnalyticFunction implements AnalyticFunction, Closeable {
    protected final long bufPtr;
    protected final VirtualColumn valueColumn;
    protected boolean nextNull = true;
    protected boolean closed = false;

    public AbstractPrevAnalyticFunction(VirtualColumn valueColumn) {
        this.valueColumn = valueColumn;
        // buffer where "current" value is kept
        this.bufPtr = Unsafe.malloc(8);
    }

    @Override
    public void add(Record record) {
    }

    @Override
    public byte get() {
        return nextNull ? 0 : Unsafe.getUnsafe().getByte(bufPtr);
    }

    @Override
    public boolean getBool() {
        return !nextNull && Unsafe.getUnsafe().getByte(bufPtr) == 1;
    }

    @Override
    public long getDate() {
        return getLong();
    }

    @Override
    public double getDouble() {
        return nextNull ? Double.NaN : Unsafe.getUnsafe().getDouble(bufPtr);
    }

    @Override
    public float getFloat() {
        return nextNull ? Float.NaN : Unsafe.getUnsafe().getFloat(bufPtr);
    }

    public CharSequence getStr() {
        throw new UnsupportedOperationException();
    }

    public CharSequence getStrB() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt() {
        return nextNull ? (valueColumn.getType() == ColumnType.SYMBOL ? SymbolTable.VALUE_IS_NULL : Numbers.INT_NaN) : Unsafe.getUnsafe().getInt(bufPtr);
    }

    @Override
    public long getLong() {
        return nextNull ? Numbers.LONG_NaN : Unsafe.getUnsafe().getLong(bufPtr);
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueColumn;
    }

    @Override
    public short getShort() {
        return nextNull ? 0 : (short) Unsafe.getUnsafe().getInt(bufPtr);
    }

    @Override
    public void getStr(CharSink sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSym() {
        return nextNull ? null : valueColumn.getSymbolTable().value(getInt());
    }

    @Override
    public SymbolTable getSymbolTable() {
        return valueColumn.getSymbolTable();
    }

    @Override
    public int getType() {
        return AnalyticFunction.STREAM;
    }

    @Override
    public void prepare(RecordCursor cursor) {
        valueColumn.prepare(cursor.getStorageFacade());
    }

    @Override
    public void reset() {
        nextNull = true;
    }

    @Override
    public void toTop() {
        this.nextNull = true;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        Unsafe.free(bufPtr, 8);
        closed = true;
    }
}
