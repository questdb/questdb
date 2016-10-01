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

package com.questdb.ql.impl.join.asof;

import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.impl.map.DirectMap;
import com.questdb.std.IntHashSet;
import com.questdb.std.IntList;
import com.questdb.store.ColumnType;

final class RecordUtils {
    private RecordUtils() {
    }

    static DirectMap.KeyWriter createKey(DirectMap map, Record record, IntHashSet indices, IntList types) {
        DirectMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = indices.size(); i < n; i++) {
            int idx = indices.get(i);
            switch (types.getQuick(i)) {
                case ColumnType.INT:
                    kw.putInt(record.getInt(idx));
                    break;
                case ColumnType.LONG:
                    kw.putLong(record.getLong(idx));
                    break;
                case ColumnType.FLOAT:
                    kw.putFloat(record.getFloat(idx));
                    break;
                case ColumnType.DOUBLE:
                    kw.putDouble(record.getDouble(idx));
                    break;
                case ColumnType.BOOLEAN:
                    kw.putBool(record.getBool(idx));
                    break;
                case ColumnType.BYTE:
                    kw.putByte(record.get(idx));
                    break;
                case ColumnType.SHORT:
                    kw.putShort(record.getShort(idx));
                    break;
                case ColumnType.DATE:
                    kw.putLong(record.getDate(idx));
                    break;
                case ColumnType.STRING:
                    kw.putStr(record.getFlyweightStr(idx));
                    break;
                case ColumnType.SYMBOL:
                    // this is key field
                    // we have to write out string rather than int
                    // because master int values for same strings can be different
                    kw.putStr(record.getSym(idx));
                    break;
                default:
                    break;
            }
        }
        return kw;
    }

    static void copyFixed(int columnType, Record record, int column, long address) {
        switch (columnType) {
            case ColumnType.INT:
            case ColumnType.SYMBOL:
                // write out int as symbol value
                // need symbol facade to resolve back to string
                Unsafe.getUnsafe().putInt(address, record.getInt(column));
                break;
            case ColumnType.LONG:
                Unsafe.getUnsafe().putLong(address, record.getLong(column));
                break;
            case ColumnType.FLOAT:
                Unsafe.getUnsafe().putFloat(address, record.getFloat(column));
                break;
            case ColumnType.DOUBLE:
                Unsafe.getUnsafe().putDouble(address, record.getDouble(column));
                break;
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                Unsafe.getUnsafe().putByte(address, record.get(column));
                break;
            case ColumnType.SHORT:
                Unsafe.getUnsafe().putShort(address, record.getShort(column));
                break;
            case ColumnType.DATE:
                Unsafe.getUnsafe().putLong(address, record.getDate(column));
                break;
            default:
                break;
        }
    }

}
