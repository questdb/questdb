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

package io.questdb.griffin.engine.analytic;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.NullRecord;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.str.CharSink;
import com.questdb.store.SymbolTable;

import java.io.Closeable;
import java.io.IOException;

public abstract class AbstractOrderedAnalyticFunction implements AnalyticFunction, Closeable {

    protected final DirectMap map;
    private final VirtualColumn valueColumn;
    protected boolean closed = false;
    private Record out;
    private Record record;
    private RecordCursor cursor;

    public AbstractOrderedAnalyticFunction(int pageSize, VirtualColumn valueColumn) {
        this.map = new DirectMap(pageSize, 1, MapUtils.ROWID_MAP_VALUES);
        this.valueColumn = valueColumn;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        Misc.free(map);
        closed = true;
    }

    @Override
    public byte get() {
        return valueColumn.get(out);
    }

    @Override
    public boolean getBool() {
        return valueColumn.getBool(out);
    }

    @Override
    public long getDate() {
        return valueColumn.getDate(out);
    }

    @Override
    public double getDouble() {
        return valueColumn.getDouble(out);
    }

    @Override
    public float getFloat() {
        return valueColumn.getFloat(out);
    }

    @Override
    public CharSequence getStr() {
        return valueColumn.getFlyweightStr(out);
    }

    @Override
    public CharSequence getStrB() {
        return valueColumn.getFlyweightStrB(out);
    }

    @Override
    public int getInt() {
        return valueColumn.getInt(out);
    }

    @Override
    public long getLong() {
        return valueColumn.getLong(out);
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueColumn;
    }

    @Override
    public short getShort() {
        return valueColumn.getShort(out);
    }

    @Override
    public void getStr(CharSink sink) {
        valueColumn.getStr(out, sink);
    }

    @Override
    public int getStrLen() {
        return valueColumn.getStrLen(out);
    }

    @Override
    public String getSym() {
        return valueColumn.getSym(out);
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
        this.record = cursor.newRecord();
        this.cursor = cursor;
        valueColumn.prepare(cursor.getStorageFacade());
    }

    @Override
    public void prepareFor(Record record) {
        DirectMap.KeyWriter kw = map.keyWriter();
        kw.putLong(record.getRowId());
        DirectMapValues values = map.getValues(kw);
        long row;
        if (values == null || (row = values.getLong(0)) == -1) {
            out = NullRecord.INSTANCE;
        } else {
            cursor.recordAt(this.record, row);
            out = this.record;
        }
    }

    @Override
    public void reset() {
        map.clear();
    }
}
