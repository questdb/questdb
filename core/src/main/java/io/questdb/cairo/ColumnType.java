/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.griffin.TypeEx;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Long256;
import io.questdb.std.LowerCaseAsciiCharSequenceIntHashMap;

public final class ColumnType {
    // column type version as written to the metadata file
    public static final int VERSION = 416;

    public static final int BOOLEAN = 0;
    public static final int BYTE = 1;
    public static final int SHORT = 2;
    public static final int CHAR = 3;
    public static final int INT = 4;
    public static final int LONG = 5;
    public static final int DATE = 6;
    public static final int TIMESTAMP = 7;
    public static final int FLOAT = 8;
    public static final int DOUBLE = 9;
    public static final int STRING = 10;
    public static final int SYMBOL = 11;
    public static final int LONG256 = 12;
    public static final int BINARY = 13;
    public static final int PARAMETER = 14;
    public static final int MAX = PARAMETER;
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();
    private static final LowerCaseAsciiCharSequenceIntHashMap nameTypeMap = new LowerCaseAsciiCharSequenceIntHashMap();
    private static final int[] TYPE_SIZE_POW2 = new int[ColumnType.PARAMETER + 1];
    private static final int[] TYPE_SIZE = new int[ColumnType.PARAMETER + 1];

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

        nameTypeMap.put("boolean", BOOLEAN);
        nameTypeMap.put("byte", BYTE);
        nameTypeMap.put("double", DOUBLE);
        nameTypeMap.put("float", FLOAT);
        nameTypeMap.put("int", INT);
        nameTypeMap.put("long", LONG);
        nameTypeMap.put("short", SHORT);
        nameTypeMap.put("char", CHAR);
        nameTypeMap.put("string", STRING);
        nameTypeMap.put("symbol", SYMBOL);
        nameTypeMap.put("binary", BINARY);
        nameTypeMap.put("date", DATE);
        nameTypeMap.put("parameter", PARAMETER);
        nameTypeMap.put("timestamp", TIMESTAMP);
        nameTypeMap.put("cursor", TypeEx.CURSOR);
        nameTypeMap.put("long256", ColumnType.LONG256);

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
        return nameTypeMap.get(name);
    }

    public static String nameOf(int columnType) {
        final int index = typeNameMap.keyIndex(columnType);
        if (index > -1) {
            return "unknown";
        }
        return typeNameMap.valueAtQuick(index);
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
