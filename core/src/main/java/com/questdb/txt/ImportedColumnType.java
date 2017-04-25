/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.txt;

import com.questdb.std.CharSequenceIntHashMap;

import static com.questdb.store.ColumnType.*;

public final class ImportedColumnType {

    private static final CharSequenceIntHashMap nameTypeMap = new CharSequenceIntHashMap();

    private ImportedColumnType() {
    }


    public static int importedColumnTypeOf(CharSequence name) {
        return nameTypeMap.get(name);
    }

    static {
        nameTypeMap.put("BOOLEAN", BOOLEAN);
        nameTypeMap.put("BYTE", BYTE);
        nameTypeMap.put("DOUBLE", DOUBLE);
        nameTypeMap.put("FLOAT", FLOAT);
        nameTypeMap.put("INT", INT);
        nameTypeMap.put("LONG", LONG);
        nameTypeMap.put("SHORT", SHORT);
        nameTypeMap.put("STRING", STRING);
        nameTypeMap.put("SYMBOL", SYMBOL);
        nameTypeMap.put("DATE_ISO", DATE);
        nameTypeMap.put("DATE_1", DATE);
        nameTypeMap.put("DATE_2", DATE);
        nameTypeMap.put("DATE_3", DATE);
    }
}
