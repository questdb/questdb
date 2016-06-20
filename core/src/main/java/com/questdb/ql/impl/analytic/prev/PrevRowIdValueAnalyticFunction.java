/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package com.questdb.ql.impl.analytic.prev;

import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class PrevRowIdValueAnalyticFunction extends AbstractPrevValueAnalyticFunction implements Closeable {
    private final DirectMap map;
    private final ObjList<VirtualColumn> partitionBy;
    private RecordCursor parent;

    public PrevRowIdValueAnalyticFunction(int pageSize, ObjList<VirtualColumn> partitionBy, VirtualColumn valueColumn) {
        super(valueColumn);
        this.partitionBy = partitionBy;
        this.map = new DirectMap(pageSize, partitionBy.size(), MapUtils.toTypeList(ColumnType.LONG));
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        super.close();
        Misc.free(map);
    }

    @Override
    public byte get() {
        return nextNull ? 0 : valueColumn.get(getParentRecord());
    }

    @Override
    public void getBin(OutputStream s) {
        if (nextNull) {
            return;
        }
        valueColumn.getBin(getParentRecord(), s);
    }

    @Override
    public DirectInputStream getBin() {
        return nextNull ? null : valueColumn.getBin(getParentRecord());
    }

    @Override
    public long getBinLen() {
        return nextNull ? 0 : valueColumn.getBinLen(getParentRecord());
    }

    @Override
    public boolean getBool() {
        return !nextNull && valueColumn.getBool(getParentRecord());
    }

    @Override
    public long getDate() {
        return nextNull ? Numbers.LONG_NaN : valueColumn.getDate(getParentRecord());
    }

    @Override
    public double getDouble() {
        return nextNull ? Double.NaN : valueColumn.getDouble(getParentRecord());
    }

    @Override
    public float getFloat() {
        return nextNull ? Float.NaN : valueColumn.getFloat(getParentRecord());
    }

    @Override
    public CharSequence getFlyweightStr() {
        return nextNull ? null : valueColumn.getFlyweightStr(getParentRecord());
    }

    @Override
    public CharSequence getFlyweightStrB() {
        return nextNull ? null : valueColumn.getFlyweightStrB(getParentRecord());
    }

    @Override
    public int getInt() {
        return nextNull ? Numbers.INT_NaN : valueColumn.getInt(getParentRecord());
    }

    @Override
    public long getLong() {
        return nextNull ? Numbers.LONG_NaN : valueColumn.getLong(getParentRecord());
    }

    @Override
    public short getShort() {
        return nextNull ? 0 : valueColumn.getShort(getParentRecord());
    }

    @Override
    public void getStr(CharSink sink) {
        if (nextNull) {
            sink.put((CharSequence) null);
        } else {
            valueColumn.getStr(getParentRecord(), sink);
        }
    }

    @Override
    public CharSequence getStr() {
        return nextNull ? null : valueColumn.getStr(getParentRecord());
    }

    @Override
    public int getStrLen() {
        return nextNull ? -1 : valueColumn.getStrLen(getParentRecord());
    }

    @Override
    public String getSym() {
        return nextNull ? null : valueColumn.getSym(getParentRecord());
    }

    @Override
    public void prepare(RecordCursor cursor) {
        parent = cursor;
    }

    @Override
    public void reset() {
        super.reset();
        map.clear();
    }

    @Override
    public void scroll(Record record) {
        DirectMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            MapUtils.writeVirtualColumn(kw, record, partitionBy.getQuick(i));
        }

        DirectMapValues values = map.getOrCreateValues(kw);
        if (values.isNew()) {
            nextNull = true;
        } else {
            nextNull = false;
            Unsafe.getUnsafe().putLong(bufPtr, values.getLong(0));
        }
        values.putLong(0, record.getRowId());
    }

    private Record getParentRecord() {
        return parent.recordAt(Unsafe.getUnsafe().getLong(bufPtr));
    }
}
