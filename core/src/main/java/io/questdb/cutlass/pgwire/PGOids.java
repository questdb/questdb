/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;

public class PGOids {

    public static final int PG_VARCHAR = 1043;
    public static final int PG_TIMESTAMP = 1114;
    public static final int PG_FLOAT8 = 701;
    public static final int PG_FLOAT4 = 700;
    public static final int PG_INT4 = 23;
    public static final int PG_INT2 = 21;
    public static final int PG_INT8 = 20;
    public static final int PG_BOOL = 16;
    public static final int PG_CHAR = 18;
    public static final int PG_DATE = 1082;
    public static final int PG_BYTEA = 17;
    private static final IntList TYPE_OIDS = new IntList();
    public static final IntList PG_TYPE_OIDS = new IntList();
    public static final IntIntHashMap PG_TYPE_TO_SIZE_MAP = new IntIntHashMap();
    public static final CharSequence[] PG_TYPE_TO_NAME = new CharSequence[11];

    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_FLOAT8 = ((PG_FLOAT8 >> 24) & 0xff) | ((PG_FLOAT8 << 8) & 0xff0000) | ((PG_FLOAT8 >> 8) & 0xff00) | ((PG_FLOAT8 << 24) & 0xff000000);
    public static final int X_B_PG_FLOAT8 = 1 | X_PG_FLOAT8;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_FLOAT4 = ((PG_FLOAT4 >> 24) & 0xff) | ((PG_FLOAT4 << 8) & 0xff0000) | ((PG_FLOAT4 >> 8) & 0xff00) | ((PG_FLOAT4 << 24) & 0xff000000);
    public static final int X_B_PG_FLOAT4 = 1 | X_PG_FLOAT4;
    public static final int X_PG_INT4 = ((PG_INT4 >> 24) & 0xff) | ((PG_INT4 << 8) & 0xff0000) | ((PG_INT4 >> 8) & 0xff00) | ((PG_INT4 << 24) & 0xff000000);
    public static final int X_B_PG_INT4 = 1 | X_PG_INT4;
    public static final int X_PG_INT8 = ((PG_INT8 >> 24) & 0xff) | ((PG_INT8 << 8) & 0xff0000) | ((PG_INT8 >> 8) & 0xff00) | ((PG_INT8 << 24) & 0xff000000);
    public static final int X_B_PG_INT8 = 1 | X_PG_INT8;
    public static final int X_PG_INT2 = ((PG_INT2 >> 24) & 0xff) | ((PG_INT2 << 8) & 0xff0000) | ((PG_INT2 >> 8) & 0xff00) | ((PG_INT2 << 24) & 0xff000000);
    public static final int X_B_PG_INT2 = 1 | X_PG_INT2;
    public static final int X_PG_CHAR = ((PG_CHAR >> 24) & 0xff) | ((PG_CHAR << 8) & 0xff0000) | ((PG_CHAR >> 8) & 0xff00) | ((PG_CHAR << 24) & 0xff000000);
    public static final int X_B_PG_CHAR = 1 | X_PG_CHAR;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_DATE = ((PG_DATE >> 24) & 0xff) | ((PG_DATE << 8) & 0xff0000) | ((PG_DATE >> 8) & 0xff00) | ((PG_DATE << 24) & 0xff000000);
    public static final int X_B_PG_DATE = 1 | X_PG_DATE;
    public static final int X_PG_BOOL = ((PG_BOOL >> 24) & 0xff) | ((PG_BOOL << 8) & 0xff0000) | ((PG_BOOL >> 8) & 0xff00) | ((PG_BOOL << 24) & 0xff000000);
    public static final int X_B_PG_BOOL = 1 | X_PG_BOOL;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_TIMESTAMP = ((PG_TIMESTAMP >> 24) & 0xff) | ((PG_TIMESTAMP << 8) & 0xff0000) | ((PG_TIMESTAMP >> 8) & 0xff00) | ((PG_TIMESTAMP << 24) & 0xff000000);
    public static final int X_B_PG_TIMESTAMP = 1 | X_PG_TIMESTAMP;
    public static final int X_PG_BYTEA = ((PG_BYTEA >> 24) & 0xff) | ((PG_BYTEA << 8) & 0xff0000) | ((PG_BYTEA >> 8) & 0xff00) | ((PG_BYTEA << 24) & 0xff000000);
    public static final int X_B_PG_BYTEA = 1 | X_PG_BYTEA;
    public static final int PG_CATALOG_OID = 11;
    public static final int PG_CLASS_OID = 1259;
    public static final int PG_PUBLIC_OID = 2200;
    public static final int PG_NAMESPACE_OID = 2615;

    public static final int BINARY_TYPE_INT = (1 << 31) | ColumnType.INT;
    public static final int BINARY_TYPE_BYTE = (1 << 31) | ColumnType.BYTE;
    public static final int BINARY_TYPE_SHORT = (1 << 31) | ColumnType.SHORT;
    public static final int BINARY_TYPE_LONG = (1 << 31) | ColumnType.LONG;
    public static final int BINARY_TYPE_DOUBLE = (1 << 31) | ColumnType.DOUBLE;
    public static final int BINARY_TYPE_FLOAT = (1 << 31) | ColumnType.FLOAT;
    public static final int BINARY_TYPE_DATE = (1 << 31) | ColumnType.DATE;
    public static final int BINARY_TYPE_TIMESTAMP = (1 << 31) | ColumnType.TIMESTAMP;
    public static final int BINARY_TYPE_BINARY = (1 << 31) | ColumnType.BINARY;

    public static final int BINARY_TYPE_STRING = (1 << 31) | ColumnType.STRING;
    public static final int BINARY_TYPE_SYMBOL = (1 << 31) | ColumnType.SYMBOL;
    public static final int BINARY_TYPE_BOOLEAN = (1 << 31) | ColumnType.BOOLEAN;
    public static final int BINARY_TYPE_LONG256 = (1 << 31) | ColumnType.LONG256;
    public static final int BINARY_TYPE_CHAR = (1 << 31) | ColumnType.CHAR;

    static int toColumnBinaryType(short code, int type) {
        return (((int) code) << 31) | type;
    }

    static int toParamBinaryType(short code, int type) {
        return code | type;
    }

    static short getColumnBinaryFlag(int type) {
        return (short) ((type >>> 31) & 0xff);
    }

    static int toColumnType(int type) {
        // clear format flag
        return type & (~(1 << 31));
    }

    static int toParamType(int type) {
        // clear format flag
        return type & (~1);
    }

    public static int getTypeOid(int type) {
        return TYPE_OIDS.getQuick(ColumnType.tagOf(type));
    }

    static {
        TYPE_OIDS.extendAndSet(ColumnType.STRING, PG_VARCHAR); // VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.TIMESTAMP, PG_TIMESTAMP); // TIMESTAMP
        TYPE_OIDS.extendAndSet(ColumnType.DOUBLE, PG_FLOAT8); // FLOAT8
        TYPE_OIDS.extendAndSet(ColumnType.FLOAT, PG_FLOAT4); // FLOAT4
        TYPE_OIDS.extendAndSet(ColumnType.INT, PG_INT4); // INT4
        TYPE_OIDS.extendAndSet(ColumnType.SHORT, PG_INT2); // INT2
        TYPE_OIDS.extendAndSet(ColumnType.CHAR, PG_CHAR);
        TYPE_OIDS.extendAndSet(ColumnType.SYMBOL, PG_VARCHAR); // NAME
        TYPE_OIDS.extendAndSet(ColumnType.LONG, PG_INT8); // INT8
        TYPE_OIDS.extendAndSet(ColumnType.BYTE, PG_INT2); // INT2
        TYPE_OIDS.extendAndSet(ColumnType.BOOLEAN, PG_BOOL); // BOOL
        TYPE_OIDS.extendAndSet(ColumnType.DATE, PG_TIMESTAMP); // DATE
        TYPE_OIDS.extendAndSet(ColumnType.BINARY, PG_BYTEA); // BYTEA
        TYPE_OIDS.extendAndSet(ColumnType.LONG256, PG_VARCHAR); // VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.GEOBYTE, PG_VARCHAR); // VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.GEOSHORT, PG_VARCHAR); // VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.GEOINT, PG_VARCHAR); // VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.GEOLONG, PG_VARCHAR); // VARCHAR

        PG_TYPE_OIDS.add(PG_VARCHAR);
        PG_TYPE_OIDS.add(PG_TIMESTAMP);
        PG_TYPE_OIDS.add(PG_FLOAT8);
        PG_TYPE_OIDS.add(PG_FLOAT4);
        PG_TYPE_OIDS.add(PG_INT4);
        PG_TYPE_OIDS.add(PG_INT2);
        PG_TYPE_OIDS.add(PG_CHAR);
        PG_TYPE_OIDS.add(PG_INT8);
        PG_TYPE_OIDS.add(PG_BOOL);
        PG_TYPE_OIDS.add(PG_BYTEA);
        PG_TYPE_OIDS.add(PG_DATE);

        PG_TYPE_TO_SIZE_MAP.put(PG_FLOAT8, Double.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_FLOAT4, Float.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_INT4, Integer.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_INT2, Short.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_CHAR, Character.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_INT8, Long.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_BOOL, Byte.BYTES);

        PG_TYPE_TO_NAME[0] = "varchar";
        PG_TYPE_TO_NAME[1] = "timestamp";
        PG_TYPE_TO_NAME[2] = "float8";
        PG_TYPE_TO_NAME[3] = "float4";
        PG_TYPE_TO_NAME[4] = "int4";
        PG_TYPE_TO_NAME[5] = "int2";
        PG_TYPE_TO_NAME[6] = "char";
        PG_TYPE_TO_NAME[7] = "int8";
        PG_TYPE_TO_NAME[8] = "bool";
        PG_TYPE_TO_NAME[9] = "binary";
        PG_TYPE_TO_NAME[10] = "date";
    }
}
