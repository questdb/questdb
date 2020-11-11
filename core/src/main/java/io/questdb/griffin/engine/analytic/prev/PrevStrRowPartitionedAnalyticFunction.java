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

import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.ql.impl.map.DirectMapValues;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.ObjList;
import com.questdb.std.str.CharSink;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.IOException;

public class PrevStrRowPartitionedAnalyticFunction extends AbstractPrevAnalyticFunction implements Closeable {
    private final DirectMap map;
    private final ObjList<VirtualColumn> partitionBy;
    private RecordCursor parent;

    public PrevStrRowPartitionedAnalyticFunction(int pageSize, ObjList<VirtualColumn> partitionBy, VirtualColumn valueColumn) {
        super(valueColumn);
        this.partitionBy = partitionBy;
        this.map = new DirectMap(pageSize, partitionBy.size(), MapUtils.toTypeList(ColumnType.LONG));
    }

    @Override
    public byte get() {
        return nextNull ? 0 : valueColumn.get(getParentRecord());
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
    public CharSequence getStr() {
        return nextNull ? null : valueColumn.getFlyweightStr(getParentRecord());
    }

    @Override
    public CharSequence getStrB() {
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
            return;
        }
        valueColumn.getStr(getParentRecord(), sink);
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
    public void toTop() {
        super.toTop();
        map.clear();
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
    public void prepareFor(Record record) {
        DirectMapValues values = MapUtils.getMapValues(map, record, partitionBy);
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
