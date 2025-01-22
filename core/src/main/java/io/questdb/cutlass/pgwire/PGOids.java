/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.IntShortHashMap;
import io.questdb.std.Numbers;

public class PGOids {

    public static final int BINARY_TYPE_BINARY = (1 << 31) | ColumnType.BINARY;
    public static final int BINARY_TYPE_BOOLEAN = (1 << 31) | ColumnType.BOOLEAN;
    public static final int BINARY_TYPE_BYTE = (1 << 31) | ColumnType.BYTE;
    public static final int BINARY_TYPE_CHAR = (1 << 31) | ColumnType.CHAR;
    public static final int BINARY_TYPE_DATE = (1 << 31) | ColumnType.DATE;
    public static final int BINARY_TYPE_DOUBLE = (1 << 31) | ColumnType.DOUBLE;
    public static final int BINARY_TYPE_FLOAT = (1 << 31) | ColumnType.FLOAT;
    public static final int BINARY_TYPE_INT = (1 << 31) | ColumnType.INT;
    public static final int BINARY_TYPE_INTERVAL = (1 << 31) | ColumnType.INTERVAL;
    public static final int BINARY_TYPE_LONG = (1 << 31) | ColumnType.LONG;
    public static final int BINARY_TYPE_LONG256 = (1 << 31) | ColumnType.LONG256;
    public static final int BINARY_TYPE_SHORT = (1 << 31) | ColumnType.SHORT;
    public static final int BINARY_TYPE_STRING = (1 << 31) | ColumnType.STRING;
    public static final int BINARY_TYPE_SYMBOL = (1 << 31) | ColumnType.SYMBOL;
    public static final int BINARY_TYPE_TIMESTAMP = (1 << 31) | ColumnType.TIMESTAMP;
    public static final int BINARY_TYPE_UUID = (1 << 31) | ColumnType.UUID;
    public static final int BINARY_TYPE_VARCHAR = (1 << 31) | ColumnType.VARCHAR;
    public static final int PG_BOOL = 16;
    public static final int PG_BYTEA = 17;
    public static final int PG_CATALOG_OID = 11;
    public static final int PG_CHAR = 1042;
    public static final int PG_CLASS_OID = 1259;
    public static final int PG_DATE = 1082;
    public static final int PG_FLOAT4 = 700;
    public static final int PG_FLOAT8 = 701;
    public static final int PG_INT2 = 21;
    public static final int PG_INT4 = 23;
    public static final int PG_INT8 = 20;
    public static final int PG_INTERNAL = 2281;
    public static final int PG_NAMESPACE_OID = 2615;
    public static final int PG_OID = 26;
    public static final int PG_PUBLIC_OID = 2200;
    public static final int PG_TIMESTAMP = 1114;
    public static final int PG_TIMESTAMP_TZ = 1184;
    public static final IntList PG_TYPE_OIDS = new IntList();
    public static final IntList PG_TYPE_PROC_OIDS = new IntList();
    public static final char[] PG_TYPE_TO_CATEGORY = new char[14];
    public static final CharSequence[] PG_TYPE_TO_DEFAULT = new CharSequence[14];
    public static final short[] PG_TYPE_TO_LENGTH = new short[14];
    public static final CharSequence[] PG_TYPE_TO_NAME = new CharSequence[14];
    public static final CharSequence[] PG_TYPE_TO_PROC_NAME = new CharSequence[14];
    public static final CharSequence[] PG_TYPE_TO_PROC_SRC = new CharSequence[14];
    public static final IntShortHashMap PG_TYPE_TO_SIZE_MAP = new IntShortHashMap();
    public static final int PG_UNSPECIFIED = 0;
    public static final int PG_UUID = 2950;
    public static final int PG_VARCHAR = 1043;
    public static final int PG_VOID = 2278;
    public static final int X_PG_BOOL = ((PG_BOOL >> 24) & 0xff) | ((PG_BOOL << 8) & 0xff0000) | ((PG_BOOL >> 8) & 0xff00) | ((PG_BOOL << 24) & 0xff000000);
    public static final int X_B_PG_BOOL = 1 | X_PG_BOOL;
    public static final int X_PG_BYTEA = ((PG_BYTEA >> 24) & 0xff) | ((PG_BYTEA << 8) & 0xff0000) | ((PG_BYTEA >> 8) & 0xff00) | ((PG_BYTEA << 24) & 0xff000000);
    public static final int X_B_PG_BYTEA = 1 | X_PG_BYTEA;
    public static final int X_PG_CHAR = ((PG_CHAR >> 24) & 0xff) | ((PG_CHAR << 8) & 0xff0000) | ((PG_CHAR >> 8) & 0xff00) | ((PG_CHAR << 24) & 0xff000000);
    public static final int X_B_PG_CHAR = 1 | X_PG_CHAR;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_DATE = ((PG_DATE >> 24) & 0xff) | ((PG_DATE << 8) & 0xff0000) | ((PG_DATE >> 8) & 0xff00) | ((PG_DATE << 24) & 0xff000000);
    public static final int X_B_PG_DATE = 1 | X_PG_DATE;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_FLOAT4 = ((PG_FLOAT4 >> 24) & 0xff) | ((PG_FLOAT4 << 8) & 0xff0000) | ((PG_FLOAT4 >> 8) & 0xff00) | ((PG_FLOAT4 << 24) & 0xff000000);
    public static final int X_B_PG_FLOAT4 = 1 | X_PG_FLOAT4;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_FLOAT8 = ((PG_FLOAT8 >> 24) & 0xff) | ((PG_FLOAT8 << 8) & 0xff0000) | ((PG_FLOAT8 >> 8) & 0xff00) | ((PG_FLOAT8 << 24) & 0xff000000);
    public static final int X_B_PG_FLOAT8 = 1 | X_PG_FLOAT8;
    public static final int X_PG_INT2 = ((PG_INT2 >> 24) & 0xff) | ((PG_INT2 << 8) & 0xff0000) | ((PG_INT2 >> 8) & 0xff00) | ((PG_INT2 << 24) & 0xff000000);
    public static final int X_B_PG_INT2 = 1 | X_PG_INT2;
    public static final int X_PG_INT4 = ((PG_INT4 >> 24) & 0xff) | ((PG_INT4 << 8) & 0xff0000) | ((PG_INT4 >> 8) & 0xff00) | ((PG_INT4 << 24) & 0xff000000);
    public static final int X_B_PG_INT4 = 1 | X_PG_INT4;
    public static final int X_PG_INT8 = ((PG_INT8 >> 24) & 0xff) | ((PG_INT8 << 8) & 0xff0000) | ((PG_INT8 >> 8) & 0xff00) | ((PG_INT8 << 24) & 0xff000000);
    public static final int X_B_PG_INT8 = 1 | X_PG_INT8;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_TIMESTAMP = ((PG_TIMESTAMP >> 24) & 0xff) | ((PG_TIMESTAMP << 8) & 0xff0000) | ((PG_TIMESTAMP >> 8) & 0xff00) | ((PG_TIMESTAMP << 24) & 0xff000000);
    public static final int X_B_PG_TIMESTAMP = 1 | X_PG_TIMESTAMP;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_TIMESTAMP_TZ = ((PG_TIMESTAMP_TZ >> 24) & 0xff) | ((PG_TIMESTAMP_TZ << 8) & 0xff0000) | ((PG_TIMESTAMP_TZ >> 8) & 0xff00) | ((PG_TIMESTAMP_TZ << 24) & 0xff000000);
    public static final IntShortHashMap X_PG_TYPE_TO_SIZE_MAP = new IntShortHashMap();
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_UUID = ((PG_UUID >> 24) & 0xff) | ((PG_UUID << 8) & 0xff0000) | ((PG_UUID >> 8) & 0xff00) | ((PG_UUID << 24) & 0xff000000);
    public static final int X_B_PG_UUID = 1 | X_PG_UUID;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_VOID = ((PG_VOID >> 24) & 0xff) | ((PG_VOID << 8) & 0xff0000) | ((PG_VOID >> 8) & 0xff00) | ((PG_VOID << 24) & 0xff000000);
    private static final int CHAR_ATT_TYP_MOD = 5; // CHAR(n) in PostgreSQL has n+4 as type modifier
    private static final IntList TYPE_OIDS = new IntList();
    private static final int X_CHAR_ATT_TYP_MOD = Numbers.bswap(CHAR_ATT_TYP_MOD);

    public static int getAttTypMod(int pgOidType) {
        // PG_CHAR is the only type that has type modifier
        // for all other types it is -1 = no value

        // we use a simple branch here, if we add more types with typmod, we should consider using a map
        // but it's an overkill for just one type.
        // if you modify this method, make sure to update also getXAttTypMod.
        if (pgOidType == PG_CHAR) {
            return CHAR_ATT_TYP_MOD;
        }
        return -1;
    }

    public static short getColumnBinaryFlag(int type) {
        return (short) ((type >>> 31) & 0xff);
    }

    public static int getTypeOid(int type) {
        return TYPE_OIDS.getQuick(ColumnType.tagOf(type));
    }

    public static int getXAttTypMod(int pgOidType) {
        // see getAttTypMod for explanation
        if (pgOidType == PG_CHAR) {
            return X_CHAR_ATT_TYP_MOD;
        }
        return -1;
    }

    public static int toColumnBinaryType(short code, int type) {
        return (((int) code) << 31) | type;
    }

    public static int toColumnType(int type) {
        // clear format flag
        return type & (~(1 << 31));
    }

    public static int toParamBinaryType(short code, int type) {
        return code | type;
    }

    public static int toParamType(int type) {
        // clear format flag
        return type & (~1);
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
        TYPE_OIDS.extendAndSet(ColumnType.UUID, PG_UUID); // UUID
        TYPE_OIDS.extendAndSet(ColumnType.IPv4, PG_VARCHAR); //IPv4
        TYPE_OIDS.extendAndSet(ColumnType.VARCHAR, PG_VARCHAR); // VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.INTERVAL, PG_VARCHAR); // VARCHAR

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
        PG_TYPE_OIDS.add(PG_UUID);
        PG_TYPE_OIDS.add(PG_INTERNAL);
        PG_TYPE_OIDS.add(PG_OID);

        // these values are taken from PostgreSQL pg_proc view
        PG_TYPE_PROC_OIDS.add(2432);
        PG_TYPE_PROC_OIDS.add(2474);
        PG_TYPE_PROC_OIDS.add(2426);
        PG_TYPE_PROC_OIDS.add(2424);
        PG_TYPE_PROC_OIDS.add(2406);
        PG_TYPE_PROC_OIDS.add(2404);
        PG_TYPE_PROC_OIDS.add(2434);
        PG_TYPE_PROC_OIDS.add(2408);
        PG_TYPE_PROC_OIDS.add(2436);
        PG_TYPE_PROC_OIDS.add(2412);
        PG_TYPE_PROC_OIDS.add(2568);
        PG_TYPE_PROC_OIDS.add(2961);
        PG_TYPE_PROC_OIDS.add(0); // INTERNAL
        PG_TYPE_PROC_OIDS.add(2418); // OID

        // Fixed-size types only since variable size types have size -1 in PostgreSQL and -1 this happens
        // to be a marker for 'no value' in this map.
        // Note: PG_CHAR is fixed size in QuestDB, but variable size in PostgreSQL. PostgreSQL uses char(n) and
        // when n is not specified it is as it was 1, but the type is still technically variable size.
        // The actual 'n' value is communicated via type modifier, not the type itself.
        PG_TYPE_TO_SIZE_MAP.put(PG_FLOAT8, (short) Double.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_FLOAT4, (short) Float.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_INT4, (short) Integer.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_INT2, (short) Short.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_INT8, (short) Long.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_BOOL, (short) Byte.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_TIMESTAMP, (short) Long.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_UUID, (short) (Long.BYTES * 2));
        PG_TYPE_TO_SIZE_MAP.put(PG_TIMESTAMP, (short) Long.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_TIMESTAMP_TZ, (short) Long.BYTES);
        PG_TYPE_TO_SIZE_MAP.put(PG_DATE, (short) Long.BYTES);

        // The same table with big endian values, keep in sync with PG_TYPE_TO_SIZE_MAP
        // This is to avoid converting endianness on every value read.
        // Note: No entry value is -1, which is the same regardless of endianness since it's all 1s.
        X_PG_TYPE_TO_SIZE_MAP.put(PG_FLOAT8, Numbers.bswap((short) Double.BYTES));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_FLOAT4, Numbers.bswap((short) Float.BYTES));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_INT4, Numbers.bswap((short) Integer.BYTES));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_INT2, Numbers.bswap((short) Short.BYTES));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_INT8, Numbers.bswap((short) Long.BYTES));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_BOOL, Numbers.bswap((short) Byte.BYTES));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_UUID, Numbers.bswap((short) (Long.BYTES * 2)));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_TIMESTAMP, Numbers.bswap((short) Long.BYTES));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_TIMESTAMP_TZ, Numbers.bswap((short) Long.BYTES));
        X_PG_TYPE_TO_SIZE_MAP.put(PG_DATE, Numbers.bswap((short) Long.BYTES));

        PG_TYPE_TO_NAME[0] = "varchar";
        PG_TYPE_TO_NAME[1] = "timestamp";
        PG_TYPE_TO_NAME[2] = "float8";
        PG_TYPE_TO_NAME[3] = "float4";
        PG_TYPE_TO_NAME[4] = "int4";
        PG_TYPE_TO_NAME[5] = "int2";
        PG_TYPE_TO_NAME[6] = "bpchar";
        PG_TYPE_TO_NAME[7] = "int8";
        PG_TYPE_TO_NAME[8] = "bool";
        PG_TYPE_TO_NAME[9] = "binary";
        PG_TYPE_TO_NAME[10] = "date";
        PG_TYPE_TO_NAME[11] = "uuid";
        PG_TYPE_TO_NAME[12] = "internal";
        PG_TYPE_TO_NAME[13] = "oid";

        for (int i = 0, n = PG_TYPE_TO_NAME.length; i < n; i++) {
            PG_TYPE_TO_PROC_NAME[i] = PG_TYPE_TO_NAME[i] + "_recv";
            PG_TYPE_TO_PROC_SRC[i] = PG_TYPE_TO_NAME[i] + "recv";
        }

        PG_TYPE_TO_CATEGORY[0] = 'S';
        PG_TYPE_TO_CATEGORY[1] = 'D';
        PG_TYPE_TO_CATEGORY[2] = 'N';
        PG_TYPE_TO_CATEGORY[3] = 'N';
        PG_TYPE_TO_CATEGORY[4] = 'N';
        PG_TYPE_TO_CATEGORY[5] = 'N';
        PG_TYPE_TO_CATEGORY[6] = 'Z';
        PG_TYPE_TO_CATEGORY[7] = 'N';
        PG_TYPE_TO_CATEGORY[8] = 'B';
        PG_TYPE_TO_CATEGORY[9] = 'U';
        PG_TYPE_TO_CATEGORY[10] = 'D';
        PG_TYPE_TO_CATEGORY[11] = 'U';
        PG_TYPE_TO_CATEGORY[12] = 'P';
        PG_TYPE_TO_CATEGORY[13] = 'N';

        PG_TYPE_TO_LENGTH[0] = -1;
        PG_TYPE_TO_LENGTH[1] = 8;
        PG_TYPE_TO_LENGTH[2] = 8;
        PG_TYPE_TO_LENGTH[3] = 4;
        PG_TYPE_TO_LENGTH[4] = 4;
        PG_TYPE_TO_LENGTH[5] = 2;
        PG_TYPE_TO_LENGTH[6] = 2;
        PG_TYPE_TO_LENGTH[7] = 8;
        PG_TYPE_TO_LENGTH[8] = 1;
        PG_TYPE_TO_LENGTH[9] = -1;
        PG_TYPE_TO_LENGTH[10] = 8;
        PG_TYPE_TO_LENGTH[11] = 16;
        PG_TYPE_TO_LENGTH[12] = 8;
        PG_TYPE_TO_LENGTH[13] = 4;

        PG_TYPE_TO_DEFAULT[0] = null;
        PG_TYPE_TO_DEFAULT[1] = null;
        PG_TYPE_TO_DEFAULT[2] = null;
        PG_TYPE_TO_DEFAULT[3] = null;
        PG_TYPE_TO_DEFAULT[4] = null;
        PG_TYPE_TO_DEFAULT[5] = "0";
        PG_TYPE_TO_DEFAULT[6] = "0";
        PG_TYPE_TO_DEFAULT[7] = null;
        PG_TYPE_TO_DEFAULT[8] = "false";
        PG_TYPE_TO_DEFAULT[9] = null;
        PG_TYPE_TO_DEFAULT[10] = null;
        PG_TYPE_TO_DEFAULT[11] = null;
        PG_TYPE_TO_DEFAULT[12] = null;
        PG_TYPE_TO_DEFAULT[13] = null;
    }
}
