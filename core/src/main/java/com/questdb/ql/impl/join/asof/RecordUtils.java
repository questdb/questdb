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
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.ql.impl.join.asof;

import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.std.IntHashSet;
import com.questdb.std.ObjList;
import com.questdb.store.ColumnType;

final class RecordUtils {
    private RecordUtils() {
    }

    static MultiMap.KeyWriter createKey(MultiMap map, Record record, IntHashSet indices, ObjList<ColumnType> types) {
        MultiMap.KeyWriter kw = map.keyWriter();
        for (int i = 0, n = indices.size(); i < n; i++) {
            int idx = indices.get(i);
            switch (types.getQuick(i)) {
                case INT:
                    kw.putInt(record.getInt(idx));
                    break;
                case LONG:
                    kw.putLong(record.getLong(idx));
                    break;
                case FLOAT:
                    kw.putFloat(record.getFloat(idx));
                    break;
                case DOUBLE:
                    kw.putDouble(record.getDouble(idx));
                    break;
                case BOOLEAN:
                    kw.putBoolean(record.getBool(idx));
                    break;
                case BYTE:
                    kw.putByte(record.get(idx));
                    break;
                case SHORT:
                    kw.putShort(record.getShort(idx));
                    break;
                case DATE:
                    kw.putLong(record.getDate(idx));
                    break;
                case STRING:
                    kw.putStr(record.getFlyweightStr(idx));
                    break;
                case SYMBOL:
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

    static void copyFixed(ColumnType type, Record record, int column, long address) {
        switch (type) {
            case INT:
            case SYMBOL:
                // write out int as symbol value
                // need symbol facade to resolve back to string
                Unsafe.getUnsafe().putInt(address, record.getInt(column));
                break;
            case LONG:
                Unsafe.getUnsafe().putLong(address, record.getLong(column));
                break;
            case FLOAT:
                Unsafe.getUnsafe().putFloat(address, record.getFloat(column));
                break;
            case DOUBLE:
                Unsafe.getUnsafe().putDouble(address, record.getDouble(column));
                break;
            case BOOLEAN:
            case BYTE:
                Unsafe.getUnsafe().putByte(address, record.get(column));
                break;
            case SHORT:
                Unsafe.getUnsafe().putShort(address, record.getShort(column));
                break;
            case DATE:
                Unsafe.getUnsafe().putLong(address, record.getDate(column));
                break;
            default:
                break;
        }
    }

}
