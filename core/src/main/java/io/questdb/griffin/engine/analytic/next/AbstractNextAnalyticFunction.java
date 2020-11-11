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

package io.questdb.griffin.engine.analytic.next;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.NullRecord;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.MemoryPages;
import com.questdb.std.str.CharSink;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.io.Closeable;

public abstract class AbstractNextAnalyticFunction implements AnalyticFunction, Closeable {

    protected final MemoryPages pages;
    private final VirtualColumn valueColumn;
    private long offset;
    private Record record;
    private Record next;
    private RecordCursor baseCursor;

    public AbstractNextAnalyticFunction(int pageSize, VirtualColumn valueColumn) {
        this.pages = new MemoryPages(pageSize);
        this.valueColumn = valueColumn;
    }

    @Override
    public void close() {
        Misc.free(pages);
    }

    @Override
    public byte get() {
        return valueColumn.get(next);
    }

    @Override
    public boolean getBool() {
        return valueColumn.getBool(next);
    }

    @Override
    public long getDate() {
        return valueColumn.getDate(next);
    }

    @Override
    public double getDouble() {
        return valueColumn.getDouble(next);
    }

    @Override
    public float getFloat() {
        return valueColumn.getFloat(next);
    }

    public CharSequence getStr() {
        return valueColumn.getFlyweightStr(next);
    }

    public CharSequence getStrB() {
        return valueColumn.getFlyweightStrB(next);
    }

    @Override
    public int getInt() {
        return next == NullRecord.INSTANCE && valueColumn.getType() == ColumnType.SYMBOL ? SymbolTable.VALUE_IS_NULL : valueColumn.getInt(next);
    }

    @Override
    public long getLong() {
        return valueColumn.getLong(next);
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueColumn;
    }

    @Override
    public short getShort() {
        return valueColumn.getShort(next);
    }

    @Override
    public void getStr(CharSink sink) {
        valueColumn.getStr(next, sink);
    }

    @Override
    public int getStrLen() {
        return valueColumn.getStrLen(next);
    }

    @Override
    public String getSym() {
        return (next instanceof NullRecord) ? null : valueColumn.getSymbolTable().value(getInt());
    }

    @Override
    public SymbolTable getSymbolTable() {
        return valueColumn.getSymbolTable();
    }

    @Override
    public int getType() {
        return AnalyticFunction.TWO_PASS;
    }

    @Override
    public void prepare(RecordCursor cursor) {
        this.baseCursor = cursor;
        this.record = cursor.newRecord();
        valueColumn.prepare(cursor.getStorageFacade());
        this.offset = 0;
    }

    @Override
    public void prepareFor(Record rec) {
        long rowid = Unsafe.getUnsafe().getLong(pages.addressOf(this.offset));
        if (rowid == -1) {
            next = NullRecord.INSTANCE;
        } else {
            baseCursor.recordAt(record, rowid);
            next = record;
        }
        this.offset += 8;
    }

    @Override
    public void reset() {
        pages.clear();
    }

    @Override
    public void toTop() {
        this.offset = 0;
    }
}
