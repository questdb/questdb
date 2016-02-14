/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.impl.join.asof;

import com.nfsdb.misc.Unsafe;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.impl.map.MultiMap;
import com.nfsdb.std.IntHashSet;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;

final class RecordUtils {
    private RecordUtils() {
    }

    public static void copyFixed(ColumnType type, Record record, int column, long address) {
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

    public static MultiMap.KeyWriter createKey(MultiMap map, Record record, IntHashSet indices, ObjList<ColumnType> types) {
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

}
