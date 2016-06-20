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

package com.questdb.ql.impl.map;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.ql.Record;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.LongMetadata;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

public class MapUtils {
    public static final ObjList<ColumnType> ROWID_MAP_VALUES = new ObjList<>(1);
    public static final CollectionRecordMetadata ROWID_RECORD_METADATA = new CollectionRecordMetadata().add(LongMetadata.INSTANCE);
    private static final ThreadLocal<ObjList<ColumnType>> tlTypeList = new ThreadLocal<ObjList<ColumnType>>() {
        @Override
        protected ObjList<ColumnType> initialValue() {
            return new ObjList<>(1);
        }
    };

    private MapUtils() {
    }

    public static void putRecord(DirectMap.KeyWriter w, Record r, int columnIndex, ColumnType columnType) {
        switch (columnType) {
            case BOOLEAN:
                w.putBoolean(r.getBool(columnIndex));
                break;
            case BYTE:
                w.putByte(r.get(columnIndex));
                break;
            case DOUBLE:
                w.putDouble(r.getDouble(columnIndex));
                break;
            case INT:
                w.putInt(r.getInt(columnIndex));
                break;
            case LONG:
                w.putLong(r.getLong(columnIndex));
                break;
            case SHORT:
                w.putShort(r.getShort(columnIndex));
                break;
            case FLOAT:
                w.putFloat(r.getFloat(columnIndex));
                break;
            case STRING:
                w.putStr(r.getFlyweightStr(columnIndex));
                break;
            case SYMBOL:
                w.putInt(r.getInt(columnIndex));
                break;
            case BINARY:
                w.putBin(r.getBin(columnIndex));
                break;
            case DATE:
                w.putLong(r.getDate(columnIndex));
                break;
            default:
                throw new JournalRuntimeException("Unsupported type: " + columnType);
        }
    }

    public static ObjList<ColumnType> toTypeList(ColumnType type) {
        ObjList<ColumnType> l = tlTypeList.get();
        l.clear();
        l.add(type);
        return l;
    }

    public static ObjList<ColumnType> toTypeList(ColumnType type1, ColumnType type2) {
        ObjList<ColumnType> l = tlTypeList.get();
        l.clear();
        l.add(type1);
        l.add(type2);
        return l;
    }

    public static void writeVirtualColumn(DirectMap.KeyWriter w, Record r, VirtualColumn vc) {
        switch (vc.getType()) {
            case BOOLEAN:
                w.putBoolean(vc.getBool(r));
                break;
            case BYTE:
                w.putByte(vc.get(r));
                break;
            case DOUBLE:
                w.putDouble(vc.getDouble(r));
                break;
            case INT:
                w.putInt(vc.getInt(r));
                break;
            case LONG:
                w.putLong(vc.getLong(r));
                break;
            case SHORT:
                w.putShort(vc.getShort(r));
                break;
            case FLOAT:
                w.putFloat(vc.getFloat(r));
                break;
            case STRING:
                w.putStr(vc.getFlyweightStr(r));
                break;
            case SYMBOL:
                w.putInt(vc.getInt(r));
                break;
            case BINARY:
                w.putBin(vc.getBin(r));
                break;
            case DATE:
                w.putLong(vc.getDate(r));
                break;
            default:
                throw new JournalRuntimeException("Unsupported type: " + vc.getType());
        }
    }

    static {
        ROWID_MAP_VALUES.add(ColumnType.LONG);
    }
}
