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

package com.questdb.ql.map;

import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.ql.LongMetadata;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.Record;

public class MapUtils {
    public static final IntList ROWID_MAP_VALUES = new IntList(1);
    public static final CollectionRecordMetadata ROWID_RECORD_METADATA = new CollectionRecordMetadata().add(LongMetadata.INSTANCE);

    private MapUtils() {
    }

    public static DirectMapValues getMapValues(DirectMap map, Record rec, ObjList<VirtualColumn> partitionBy) {
        final DirectMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            writeVirtualColumn(kw, rec, partitionBy.getQuick(i));
        }
        return map.getOrCreateValues();
    }

    private static void writeVirtualColumn(DirectMap.KeyWriter w, Record r, VirtualColumn vc) {
        switch (vc.getType()) {
            case ColumnType.BOOLEAN:
                w.putBool(vc.getBool(r));
                break;
            case ColumnType.BYTE:
                w.putByte(vc.get(r));
                break;
            case ColumnType.DOUBLE:
                w.putDouble(vc.getDouble(r));
                break;
            case ColumnType.INT:
                w.putInt(vc.getInt(r));
                break;
            case ColumnType.LONG:
                w.putLong(vc.getLong(r));
                break;
            case ColumnType.SHORT:
                w.putShort(vc.getShort(r));
                break;
            case ColumnType.FLOAT:
                w.putFloat(vc.getFloat(r));
                break;
            case ColumnType.STRING:
                w.putStr(vc.getFlyweightStr(r));
                break;
            case ColumnType.SYMBOL:
                w.putInt(vc.getInt(r));
                break;
            case ColumnType.BINARY:
                w.putBin(vc.getBin(r));
                break;
            case ColumnType.DATE:
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
