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

package com.questdb.txt;

import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.store.ColumnType;

public final class ImportedColumnType {

    public static final int BOOLEAN = ColumnType.BOOLEAN;
    public static final int BYTE = ColumnType.BYTE;
    public static final int DOUBLE = ColumnType.DOUBLE;
    public static final int FLOAT = ColumnType.FLOAT;
    public static final int INT = ColumnType.INT;
    public static final int LONG = ColumnType.LONG;
    public static final int SHORT = ColumnType.SHORT;
    public static final int STRING = ColumnType.STRING;
    public static final int SYMBOL = ColumnType.SYMBOL;
    public static final int BINARY = ColumnType.BINARY;
    // extra dates
    public static final int DATE_ISO = 2048;
    public static final int DATE_1 = 2049;
    public static final int DATE_2 = 2050;
    public static final int DATE_3 = 2051;

    private static final CharSequenceIntHashMap nameTypeMap = new CharSequenceIntHashMap();

    private ImportedColumnType() {
    }

    public static int columnTypeOf(int importedType) {
        switch (importedType) {
            case DATE_ISO:
            case DATE_1:
            case DATE_2:
            case DATE_3:
                return ColumnType.DATE;
            default:
                return importedType;
        }
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
        nameTypeMap.put("BINARY", BINARY);
        nameTypeMap.put("DATE_ISO", DATE_ISO);
        nameTypeMap.put("DATE_1", DATE_1);
        nameTypeMap.put("DATE_2", DATE_2);
        nameTypeMap.put("DATE_3", DATE_3);
    }
}
