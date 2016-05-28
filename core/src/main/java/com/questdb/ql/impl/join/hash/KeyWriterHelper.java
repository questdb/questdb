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

package com.questdb.ql.impl.join.hash;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.ql.Record;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.store.ColumnType;

final public class KeyWriterHelper {
    private KeyWriterHelper() {
    }

    public static void setKey(MultiMap.KeyWriter key, Record r, int columnIndex, ColumnType columnType) {
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
                key.putInt(r.getInt(columnIndex));
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
