/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.analytic.prev;

import com.questdb.ql.map.*;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.ThreadLocal;
import com.questdb.std.Unsafe;
import com.questdb.store.ColumnType;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.Record;

import java.io.Closeable;
import java.io.IOException;

public class PrevPartitionedAnalyticFunction extends AbstractPrevAnalyticFunction implements Closeable {
    private final static ThreadLocal<VirtualColumnTypeResolver> tlPartitionByTypeResolver = new VirtualColumnTypeResolver.ResolverThreadLocal();
    private final static ThreadLocal<VirtualColumnTypeResolver2> tlVirtualColumnResolver = new ThreadLocal<>(VirtualColumnTypeResolver2::new);
    private final DirectMap map;
    private final ObjList<VirtualColumn> partitionBy;

    public PrevPartitionedAnalyticFunction(int pageSize, ObjList<VirtualColumn> partitionBy, VirtualColumn valueColumn) {
        super(valueColumn);
        this.partitionBy = partitionBy;
        this.map = new DirectMap(pageSize, tlPartitionByTypeResolver.get().of(partitionBy), tlVirtualColumnResolver.get().of(valueColumn));
    }

    @Override
    public void prepareFor(Record record) {
        DirectMapValues values = MapUtils.getMapValues(map, record, partitionBy);
        if (values.isNew()) {
            nextNull = true;
            store(record, values);
        } else {
            nextNull = false;
            switch (valueColumn.getType()) {
                case ColumnType.BOOLEAN:
                    Unsafe.getUnsafe().putByte(bufPtr, values.getByte(0));
                    values.putByte(0, (byte) (valueColumn.getBool(record) ? 1 : 0));
                    break;
                case ColumnType.BYTE:
                    Unsafe.getUnsafe().putByte(bufPtr, values.getByte(0));
                    values.putByte(0, valueColumn.get(record));
                    break;
                case ColumnType.DOUBLE:
                    Unsafe.getUnsafe().putDouble(bufPtr, values.getDouble(0));
                    values.putDouble(0, valueColumn.getDouble(record));
                    break;
                case ColumnType.FLOAT:
                    Unsafe.getUnsafe().putFloat(bufPtr, values.getFloat(0));
                    values.putFloat(0, valueColumn.getFloat(record));
                    break;
                case ColumnType.SYMBOL:
                case ColumnType.INT:
                    Unsafe.getUnsafe().putInt(bufPtr, values.getInt(0));
                    values.putInt(0, valueColumn.getInt(record));
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                    Unsafe.getUnsafe().putLong(bufPtr, values.getLong(0));
                    values.putLong(0, valueColumn.getLong(record));
                    break;
                case ColumnType.SHORT:
                    Unsafe.getUnsafe().putShort(bufPtr, values.getShort(0));
                    values.putShort(0, valueColumn.getShort(record));
                    break;
                default:
                    throw new JournalRuntimeException("Unsupported type: " + valueColumn.getType());
            }
        }
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

    private void store(Record record, DirectMapValues values) {
        switch (valueColumn.getType()) {
            case ColumnType.BOOLEAN:
                values.putByte(0, (byte) (valueColumn.getBool(record) ? 1 : 0));
                break;
            case ColumnType.BYTE:
                values.putByte(0, valueColumn.get(record));
                break;
            case ColumnType.DOUBLE:
                values.putDouble(0, valueColumn.getDouble(record));
                break;
            case ColumnType.FLOAT:
                values.putFloat(0, valueColumn.getFloat(record));
                break;
            case ColumnType.SYMBOL:
            case ColumnType.INT:
                values.putInt(0, valueColumn.getInt(record));
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
                values.putLong(0, valueColumn.getLong(record));
                break;
            case ColumnType.SHORT:
                values.putShort(0, valueColumn.getShort(record));
                break;
            default:
                throw new JournalRuntimeException("Unsupported type: " + valueColumn.getType());
        }

    }

    private static class VirtualColumnTypeResolver2 implements ColumnTypeResolver {
        private VirtualColumn column;

        @Override
        public int count() {
            return 1;
        }

        @Override
        public int getColumnType(int index) {
            assert index == 0;
            return column.getType();
        }

        public VirtualColumnTypeResolver2 of(VirtualColumn column) {
            this.column = column;
            return this;
        }
    }
}
