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
    public static final int CHAR = 3;
    public static final int INT = 4;
    public static final int LONG = 5;
    public static final int FLOAT = 6;
    public static final int DOUBLE = 7;
    public static final int STRING = 8;
    public static final int SYMBOL = 9;
    public static final int BINARY = 10;
    public static final int DATE = 11;
    public static final int TIMESTAMP = 12;
    public static final int LONG256 = 13;
    public static final int PARAMETER = 14;
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();
    private static final CharSequenceIntHashMap nameTypeMap = new CharSequenceIntHashMap();
    private static final ThreadLocal<StringSink> caseConverterBuffer = ThreadLocal.withInitial(StringSink::new);
    private static int[] TYPE_SIZE_POW2 = new int[ColumnType.PARAMETER + 1];
    private static int[] TYPE_SIZE = new int[ColumnType.PARAMETER + 1];

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

        TYPE_SIZE_POW2[ColumnType.BOOLEAN] = 0;
        TYPE_SIZE_POW2[ColumnType.BYTE] = 0;
        TYPE_SIZE_POW2[ColumnType.SHORT] = 1;
        TYPE_SIZE_POW2[ColumnType.CHAR] = 1;
        TYPE_SIZE_POW2[ColumnType.FLOAT] = 2;
        TYPE_SIZE_POW2[ColumnType.INT] = 2;
        TYPE_SIZE_POW2[ColumnType.SYMBOL] = 2;
        TYPE_SIZE_POW2[ColumnType.DOUBLE] = 3;
        TYPE_SIZE_POW2[ColumnType.LONG] = 3;
        TYPE_SIZE_POW2[ColumnType.DATE] = 3;
        TYPE_SIZE_POW2[ColumnType.TIMESTAMP] = 3;
        TYPE_SIZE_POW2[ColumnType.LONG256] = 8;

        TYPE_SIZE[ColumnType.BOOLEAN] = Byte.BYTES;
        TYPE_SIZE[ColumnType.BYTE] = Byte.BYTES;
        TYPE_SIZE[ColumnType.SHORT] = Short.BYTES;
        TYPE_SIZE[ColumnType.CHAR] = Character.BYTES;
        TYPE_SIZE[ColumnType.FLOAT] = Float.BYTES;
        TYPE_SIZE[ColumnType.INT] = Integer.BYTES;
        TYPE_SIZE[ColumnType.SYMBOL] = Integer.BYTES;
        TYPE_SIZE[ColumnType.DOUBLE] = Double.BYTES;
        TYPE_SIZE[ColumnType.LONG] = Long.BYTES;
        TYPE_SIZE[ColumnType.DATE] = Long.BYTES;
        TYPE_SIZE[ColumnType.TIMESTAMP] = Long.BYTES;
        TYPE_SIZE[ColumnType.LONG256] = Long256.BYTES;
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
        return TYPE_SIZE_POW2[columnType];
    }

    public static int sizeOf(int columnType) {
        if (columnType < 0 || columnType > ColumnType.PARAMETER) {
            return -1;
        }
        return TYPE_SIZE[columnType];
    }
}
