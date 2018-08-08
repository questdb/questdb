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

package com.questdb.ql.join.asof;

import com.questdb.std.Unsafe;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;

final class RecordUtils {
    private RecordUtils() {
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
                Unsafe.getUnsafe().putByte(address, record.getByte(column));
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
