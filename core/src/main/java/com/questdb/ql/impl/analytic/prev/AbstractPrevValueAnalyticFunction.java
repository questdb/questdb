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

package com.questdb.ql.impl.analytic.prev;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public abstract class AbstractPrevValueAnalyticFunction implements AnalyticFunction, Closeable {
    protected final ColumnType valueType;
    protected final int valueIndex;
    protected final long bufPtr;
    private final RecordColumnMetadata valueMetadata;
    protected boolean nextNull = true;
    protected boolean closed = false;
    private StorageFacade storageFacade;

    public AbstractPrevValueAnalyticFunction(RecordMetadata parentMetadata, String columnName, String alias) {
        // value column particulars
        this.valueIndex = parentMetadata.getColumnIndex(columnName);
        RecordColumnMetadata m = parentMetadata.getColumn(columnName);
        this.valueType = m.getType();

        // buffer where "current" value is kept
        this.bufPtr = Unsafe.getUnsafe().allocateMemory(8);
        // metadata
        this.valueMetadata = new RecordColumnMetadataImpl(alias == null ? columnName : alias, valueType);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        Unsafe.getUnsafe().freeMemory(bufPtr);
        closed = true;
    }

    @Override
    public byte get() {
        return nextNull ? 0 : Unsafe.getUnsafe().getByte(bufPtr);
    }

    @Override
    public void getBin(OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen() {
        throw new UnsupportedOperationException();
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

    @Override
    public CharSequence getFlyweightStr() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStrB() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt() {
        return nextNull ? (valueType == ColumnType.SYMBOL ? SymbolTable.VALUE_IS_NULL : Numbers.INT_NaN) : Unsafe.getUnsafe().getInt(bufPtr);
    }

    @Override
    public long getLong() {
        return nextNull ? Numbers.LONG_NaN : Unsafe.getUnsafe().getLong(bufPtr);
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueMetadata;
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
    public CharSequence getStr() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSym() {
        return nextNull ? null : storageFacade.getSymbolTable(valueIndex).value(getInt());
    }

    @Override
    public SymbolTable getSymbolTable() {
        return storageFacade.getSymbolTable(valueIndex);
    }

    @Override
    public void reset() {
        nextNull = true;
    }

    @Override
    public void setStorageFacade(StorageFacade storageFacade) {
        this.storageFacade = storageFacade;
    }
}
