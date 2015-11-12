/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.impl.join.hash;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.impl.map.MultiMap;
import com.nfsdb.storage.ColumnType;

final public class KeyWriterHelper {
    private KeyWriterHelper() {
    }

    public static void setKey(MultiMap.KeyWriter key, Record r, ColumnType columnType, int columnIndex) {
        switch (columnType) {
            case BOOLEAN:
                key.putBoolean(r.getBool(columnIndex));
                break;
            case BYTE:
                key.putByte(r.get(columnIndex));
                break;
            case DOUBLE:
                key.putDouble(r.getDouble(columnIndex));
                break;
            case INT:
                key.putInt(r.getInt(columnIndex));
                break;
            case LONG:
                key.putLong(r.getLong(columnIndex));
                break;
            case SHORT:
                key.putShort(r.getShort(columnIndex));
                break;
            case FLOAT:
                key.putFloat(r.getFloat(columnIndex));
                break;
            case STRING:
                key.putStr(r.getFlyweightStr(columnIndex));
                break;
            case SYMBOL:
                key.putStr(r.getSym(columnIndex));
                break;
            case BINARY:
                key.putBin(r.getBin(columnIndex));
                break;
            case DATE:
                key.putLong(r.getDate(columnIndex));
                break;
            default:
                throw new JournalRuntimeException("Unsupported type: " + columnType);
        }
    }
}
