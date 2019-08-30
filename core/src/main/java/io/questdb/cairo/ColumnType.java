/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package io.questdb.cairo;

import io.questdb.griffin.TypeEx;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Long256;
import io.questdb.std.str.StringSink;

public final class ColumnType {
    public static final int BOOLEAN = 0;
    public static final int BYTE = 1;
    public static final int SHORT = 2;
    public static final int INT = 3;
    public static final int LONG = 4;
    public static final int FLOAT = 5;
    public static final int DOUBLE = 6;
    public static final int STRING = 7;
    public static final int SYMBOL = 8;
    public static final int BINARY = 9;
    public static final int DATE = 10;
    public static final int PARAMETER = 11;
    public static final int TIMESTAMP = 12;
    public static final int CHAR = 13;
    public static final int LONG256 = 14;
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();
    private static final CharSequenceIntHashMap nameTypeMap = new CharSequenceIntHashMap();
    private static final ThreadLocal<StringSink> caseConverterBuffer = ThreadLocal.withInitial(StringSink::new);

    static {
        typeNameMap.put(BOOLEAN, "BOOLEAN");
        typeNameMap.put(BYTE, "BYTE");
        typeNameMap.put(DOUBLE, "DOUBLE");
        typeNameMap.put(FLOAT, "FLOAT");
        typeNameMap.put(INT, "INT");
        typeNameMap.put(LONG, "LONG");
        typeNameMap.put(SHORT, "SHORT");
        typeNameMap.put(CHAR, "CHAR");
        typeNameMap.put(STRING, "STRING");
        typeNameMap.put(SYMBOL, "SYMBOL");
        typeNameMap.put(BINARY, "BINARY");
        typeNameMap.put(DATE, "DATE");
        typeNameMap.put(PARAMETER, "PARAMETER");
        typeNameMap.put(TIMESTAMP, "TIMESTAMP");
        typeNameMap.put(TypeEx.CURSOR, "CURSOR");
        typeNameMap.put(LONG256, "LONG256");

        nameTypeMap.put("BOOLEAN", BOOLEAN);
        nameTypeMap.put("BYTE", BYTE);
        nameTypeMap.put("DOUBLE", DOUBLE);
        nameTypeMap.put("FLOAT", FLOAT);
        nameTypeMap.put("INT", INT);
        nameTypeMap.put("LONG", LONG);
        nameTypeMap.put("SHORT", SHORT);
        nameTypeMap.put("CHAR", CHAR);
        nameTypeMap.put("STRING", STRING);
        nameTypeMap.put("SYMBOL", SYMBOL);
        nameTypeMap.put("BINARY", BINARY);
        nameTypeMap.put("DATE", DATE);
        nameTypeMap.put("PARAMETER", PARAMETER);
        nameTypeMap.put("TIMESTAMP", TIMESTAMP);
        nameTypeMap.put("CURSOR", TypeEx.CURSOR);
        nameTypeMap.put("LONG256", ColumnType.LONG256);
    }

    private ColumnType() {
    }

    public static int columnTypeOf(CharSequence name) {
        StringSink b = caseConverterBuffer.get();
        b.clear();
        for (int i = 0, n = name.length(); i < n; i++) {
            b.put(Character.toUpperCase(name.charAt(i)));
        }
        return nameTypeMap.get(b);
    }

    public static String nameOf(int columnType) {
        final int index = typeNameMap.keyIndex(columnType);
        if (index > -1) {
            return "unknown";
        }
        return typeNameMap.valueAt(index);
    }

    public static int pow2SizeOf(int columnType) {
        switch (columnType) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                return 0;
            case ColumnType.DOUBLE:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return 3;
            case ColumnType.FLOAT:
            case ColumnType.INT:
            case ColumnType.SYMBOL:
                return 2;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
                return 1;
            case ColumnType.LONG256:
                return 8;
            default:
                assert false : "Cannot request power of 2 for " + nameOf(columnType);
                return -1;
        }
    }

    public static int sizeOf(int columnType) {
        switch (columnType) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                return 1;
            case ColumnType.DOUBLE:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return 8;
            case ColumnType.LONG256:
                return Long256.BYTES;
            case ColumnType.FLOAT:
            case ColumnType.INT:
            case ColumnType.SYMBOL:
                return 4;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
                return 2;
            case ColumnType.PARAMETER:
            case ColumnType.STRING:
            case ColumnType.BINARY:
                return 0;
            default:
                return -1;
        }
    }
}
