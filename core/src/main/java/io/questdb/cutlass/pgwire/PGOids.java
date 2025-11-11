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
import io.questdb.std.IntList;
import io.questdb.std.IntShortHashMap;
import io.questdb.std.Numbers;

public class PGOids {

    public static final int BINARY_TYPE_ARRAY = (1 << 31) | ColumnType.ARRAY;
    public static final int BINARY_TYPE_ARRAY_STRING = (1 << 31) | ColumnType.ARRAY_STRING;
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
    public static final int BINARY_TYPE_DECIMAL8 = (1 << 31) | ColumnType.DECIMAL8;
    public static final int BINARY_TYPE_DECIMAL16 = (1 << 31) | ColumnType.DECIMAL16;
    public static final int BINARY_TYPE_DECIMAL32 = (1 << 31) | ColumnType.DECIMAL32;
    public static final int BINARY_TYPE_DECIMAL64 = (1 << 31) | ColumnType.DECIMAL64;
    public static final int BINARY_TYPE_DECIMAL128 = (1 << 31) | ColumnType.DECIMAL128;
    public static final int BINARY_TYPE_DECIMAL256 = (1 << 31) | ColumnType.DECIMAL256;
    /**
     * We cannot know in advance the actual type of the decimal, so we make a default one that
     * should be large enough to hold decimals.
     * Users should prefer using the text format when possible.
     */
    public static final int DECIMAL_BIND_TYPE = ColumnType.getDecimalType(76, 38);
    public static final int PG_ARR_BOOL = 1000;
    public static final int PG_ARR_BYTEA = 1001;
    public static final int PG_ARR_DATE = 1182;
    public static final int PG_ARR_FLOAT4 = 1021;
    public static final int PG_ARR_FLOAT8 = 1022;
    public static final int PG_ARR_INET = 1041;
    public static final int PG_ARR_INT2 = 1005;
    public static final int PG_ARR_INT4 = 1007;
    public static final int PG_ARR_INT8 = 1016;
    public static final int PG_ARR_INTERVAL = 1187;
    public static final int PG_ARR_JSONB = 3807;
    public static final int PG_ARR_NUMERIC = 1231;
    public static final int PG_ARR_TEXT = 1009;
    public static final int PG_ARR_TIME = 1183;
    public static final int PG_ARR_TIMESTAMP = 1115;
    public static final int PG_ARR_TIMESTAMP_TZ = 1185;
    public static final int PG_ARR_UUID = 2951;
    public static final int PG_ARR_VARCHAR = 1015;
    public static final int PG_BOOL = 16;
    public static final int PG_BYTEA = 17;
    public static final int PG_CATALOG_OID = 11;
    public static final int PG_CHAR = 1042;
    public static final int PG_CLASS_OID = 1259;
    public static final int PG_DATE = 1082;
    public static final int PG_FLOAT4 = 700;
    public static final int PG_FLOAT8 = 701;
    public static final int PG_INET = 869;
    public static final int PG_INT2 = 21;
    public static final int PG_INT4 = 23;
    public static final int PG_INT8 = 20;
    public static final int PG_INTERNAL = 2281;
    public static final int PG_INTERVAL = 1186;
    public static final int PG_JSONB = 3802;
    public static final int PG_NAMESPACE_OID = 2615;
    public static final int PG_NUMERIC = 1700;
    public static final int PG_OID = 26;
    public static final int PG_PUBLIC_OID = 2200;
    public static final int PG_TIME = 1083;
    public static final int PG_TIMESTAMP = 1114;
    public static final int PG_TIMESTAMP_TZ = 1184;
    public static final IntList PG_TYPE_OIDS = new IntList();
    public static final IntList PG_TYPE_PROC_OIDS = new IntList();
    public static final char[] PG_TYPE_TO_CATEGORY = new char[16];
    public static final CharSequence[] PG_TYPE_TO_DEFAULT = new CharSequence[16];
    public static final short[] PG_TYPE_TO_LENGTH = new short[16];
    public static final CharSequence[] PG_TYPE_TO_NAME = new CharSequence[16];
    public static final CharSequence[] PG_TYPE_TO_PROC_NAME = new CharSequence[16];
    public static final CharSequence[] PG_TYPE_TO_PROC_SRC = new CharSequence[16];
    public static final IntShortHashMap PG_TYPE_TO_SIZE_MAP = new IntShortHashMap();
    public static final int PG_UNSPECIFIED = 0;
    public static final int PG_UUID = 2950;
    public static final int PG_VARCHAR = 1043;
    public static final int PG_VOID = 2278;
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_BOOL = ((PG_ARR_BOOL >> 24) & 0xff) | ((PG_ARR_BOOL << 8) & 0xff0000) | ((PG_ARR_BOOL >> 8) & 0xff00) | ((PG_ARR_BOOL << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_BYTEA = ((PG_ARR_BYTEA >> 24) & 0xff) | ((PG_ARR_BYTEA << 8) & 0xff0000) | ((PG_ARR_BYTEA >> 8) & 0xff00) | ((PG_ARR_BYTEA << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_DATE = ((PG_ARR_DATE >> 24) & 0xff) | ((PG_ARR_DATE << 8) & 0xff0000) | ((PG_ARR_DATE >> 8) & 0xff00) | ((PG_ARR_DATE << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_FLOAT4 = ((PG_ARR_FLOAT4 >> 24) & 0xff) | ((PG_ARR_FLOAT4 << 8) & 0xff0000) | ((PG_ARR_FLOAT4 >> 8) & 0xff00) | ((PG_ARR_FLOAT4 << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_FLOAT8 = ((PG_ARR_FLOAT8 >> 24) & 0xff) | ((PG_ARR_FLOAT8 << 8) & 0xff0000) | ((PG_ARR_FLOAT8 >> 8) & 0xff00) | ((PG_ARR_FLOAT8 << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_INET = ((PG_ARR_INET >> 24) & 0xff) | ((PG_ARR_INET << 8) & 0xff0000) | ((PG_ARR_INET >> 8) & 0xff00) | ((PG_ARR_INET << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_INT2 = ((PG_ARR_INT2 >> 24) & 0xff) | ((PG_ARR_INT2 << 8) & 0xff0000) | ((PG_ARR_INT2 >> 8) & 0xff00) | ((PG_ARR_INT2 << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_INT4 = ((PG_ARR_INT4 >> 24) & 0xff) | ((PG_ARR_INT4 << 8) & 0xff0000) | ((PG_ARR_INT4 >> 8) & 0xff00) | ((PG_ARR_INT4 << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_INT8 = ((PG_ARR_INT8 >> 24) & 0xff) | ((PG_ARR_INT8 << 8) & 0xff0000) | ((PG_ARR_INT8 >> 8) & 0xff00) | ((PG_ARR_INT8 << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_INTERVAL = ((PG_ARR_INTERVAL >> 24) & 0xff) | ((PG_ARR_INTERVAL << 8) & 0xff0000) | ((PG_ARR_INTERVAL >> 8) & 0xff00) | ((PG_ARR_INTERVAL << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_JSONB = ((PG_ARR_JSONB >> 24) & 0xff) | ((PG_ARR_JSONB << 8) & 0xff0000) | ((PG_ARR_JSONB >> 8) & 0xff00) | ((PG_ARR_JSONB << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_NUMERIC = ((PG_ARR_NUMERIC >> 24) & 0xff) | ((PG_ARR_NUMERIC << 8) & 0xff0000) | ((PG_ARR_NUMERIC >> 8) & 0xff00) | ((PG_ARR_NUMERIC << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_TEXT = ((PG_ARR_TEXT >> 24) & 0xff) | ((PG_ARR_TEXT << 8) & 0xff0000) | ((PG_ARR_TEXT >> 8) & 0xff00) | ((PG_ARR_TEXT << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_TIME = ((PG_ARR_TIME >> 24) & 0xff) | ((PG_ARR_TIME << 8) & 0xff0000) | ((PG_ARR_TIME >> 8) & 0xff00) | ((PG_ARR_TIME << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_TIMESTAMP = ((PG_ARR_TIMESTAMP >> 24) & 0xff) | ((PG_ARR_TIMESTAMP << 8) & 0xff0000) | ((PG_ARR_TIMESTAMP >> 8) & 0xff00) | ((PG_ARR_TIMESTAMP << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_TIMESTAMP_TZ = ((PG_ARR_TIMESTAMP_TZ >> 24) & 0xff) | ((PG_ARR_TIMESTAMP_TZ << 8) & 0xff0000) | ((PG_ARR_TIMESTAMP_TZ >> 8) & 0xff00) | ((PG_ARR_TIMESTAMP_TZ << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_UUID = ((PG_ARR_UUID >> 24) & 0xff) | ((PG_ARR_UUID << 8) & 0xff0000) | ((PG_ARR_UUID >> 8) & 0xff00) | ((PG_ARR_UUID << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_ARR_VARCHAR = ((PG_ARR_VARCHAR >> 24) & 0xff) | ((PG_ARR_VARCHAR << 8) & 0xff0000) | ((PG_ARR_VARCHAR >> 8) & 0xff00) | ((PG_ARR_VARCHAR << 24) & 0xff000000);
    public static final int X_PG_BOOL = ((PG_BOOL >> 24) & 0xff) | ((PG_BOOL << 8) & 0xff0000) | ((PG_BOOL >> 8) & 0xff00) | ((PG_BOOL << 24) & 0xff000000);
    public static final int X_PG_BYTEA = ((PG_BYTEA >> 24) & 0xff) | ((PG_BYTEA << 8) & 0xff0000) | ((PG_BYTEA >> 8) & 0xff00) | ((PG_BYTEA << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_CHAR = ((PG_CHAR >> 24) & 0xff) | ((PG_CHAR << 8) & 0xff0000) | ((PG_CHAR >> 8) & 0xff00) | ((PG_CHAR << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_DATE = ((PG_DATE >> 24) & 0xff) | ((PG_DATE << 8) & 0xff0000) | ((PG_DATE >> 8) & 0xff00) | ((PG_DATE << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_FLOAT4 = ((PG_FLOAT4 >> 24) & 0xff) | ((PG_FLOAT4 << 8) & 0xff0000) | ((PG_FLOAT4 >> 8) & 0xff00) | ((PG_FLOAT4 << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_FLOAT8 = ((PG_FLOAT8 >> 24) & 0xff) | ((PG_FLOAT8 << 8) & 0xff0000) | ((PG_FLOAT8 >> 8) & 0xff00) | ((PG_FLOAT8 << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_INET = ((PG_INET >> 24) & 0xff) | ((PG_INET << 8) & 0xff0000) | ((PG_INET >> 8) & 0xff00) | ((PG_INET << 24) & 0xff000000);
    public static final int X_PG_INT2 = ((PG_INT2 >> 24) & 0xff) | ((PG_INT2 << 8) & 0xff0000) | ((PG_INT2 >> 8) & 0xff00) | ((PG_INT2 << 24) & 0xff000000);
    public static final int X_PG_INT4 = ((PG_INT4 >> 24) & 0xff) | ((PG_INT4 << 8) & 0xff0000) | ((PG_INT4 >> 8) & 0xff00) | ((PG_INT4 << 24) & 0xff000000);
    public static final int X_PG_INT8 = ((PG_INT8 >> 24) & 0xff) | ((PG_INT8 << 8) & 0xff0000) | ((PG_INT8 >> 8) & 0xff00) | ((PG_INT8 << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_INTERVAL = ((PG_INTERVAL >> 24) & 0xff) | ((PG_INTERVAL << 8) & 0xff0000) | ((PG_INTERVAL >> 8) & 0xff00) | ((PG_INTERVAL << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_JSONB = ((PG_JSONB >> 24) & 0xff) | ((PG_JSONB << 8) & 0xff0000) | ((PG_JSONB >> 8) & 0xff00) | ((PG_JSONB << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_NUMERIC = ((PG_NUMERIC >> 24) & 0xff) | ((PG_NUMERIC << 8) & 0xff0000) | ((PG_NUMERIC >> 8) & 0xff00) | ((PG_NUMERIC << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_TIME = ((PG_TIME >> 24) & 0xff) | ((PG_TIME << 8) & 0xff0000) | ((PG_TIME >> 8) & 0xff00) | ((PG_TIME << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_TIMESTAMP = ((PG_TIMESTAMP >> 24) & 0xff) | ((PG_TIMESTAMP << 8) & 0xff0000) | ((PG_TIMESTAMP >> 8) & 0xff00) | ((PG_TIMESTAMP << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_TIMESTAMP_TZ = ((PG_TIMESTAMP_TZ >> 24) & 0xff) | ((PG_TIMESTAMP_TZ << 8) & 0xff0000) | ((PG_TIMESTAMP_TZ >> 8) & 0xff00) | ((PG_TIMESTAMP_TZ << 24) & 0xff000000);
    public static final IntShortHashMap X_PG_TYPE_TO_SIZE_MAP = new IntShortHashMap();
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_UUID = ((PG_UUID >> 24) & 0xff) | ((PG_UUID << 8) & 0xff0000) | ((PG_UUID >> 8) & 0xff00) | ((PG_UUID << 24) & 0xff000000);
    @SuppressWarnings("NumericOverflow")
    public static final int X_PG_VOID = ((PG_VOID >> 24) & 0xff) | ((PG_VOID << 8) & 0xff0000) | ((PG_VOID >> 8) & 0xff00) | ((PG_VOID << 24) & 0xff000000);
    private static final int CHAR_ATT_TYP_MOD = 5; // CHAR(n) in PostgreSQL has n+4 as type modifier
    private static final IntList TYPE_ARR_OIDS = new IntList();
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

    public static int getTypeOid(int type) {
        if (!ColumnType.isArray(type)) {
            return TYPE_OIDS.getQuick(ColumnType.tagOf(type));
        }
        int elType = ColumnType.decodeArrayElementType(type);
        return TYPE_ARR_OIDS.getQuick(elType);
    }

    public static int getXAttTypMod(int pgOidType) {
        // see getAttTypMod for explanation
        if (pgOidType == PG_CHAR) {
            return X_CHAR_ATT_TYP_MOD;
        }
        return -1;
    }

    /**
     * Returns PostgreSQL element type OID for given PostgreSQL array type OID.
     * <p>
     * When a given array type OID is not supported, 0 is returned.
     *
     * @param pgOid PostgreSQL array type OID
     * @return PostgreSQL element type OID
     */
    public static int pgArrayToElementType(int pgOid) {
        return switch (pgOid) {
            case PG_ARR_FLOAT8 -> PG_FLOAT8;
            case PG_ARR_INT8 -> PG_INT8;
            default -> 0;
        };
    }

    /**
     * Returns PostgreSQL array type OID for given PostgreSQL type OID.
     * <p>
     * When a given type OID is not supported as an array, 0 is returned.
     *
     * @param pgOid PostgreSQL type OID
     * @return PostgreSQL array type OID
     */
    public static int pgToArrayOid(int pgOid) {
        return switch (pgOid) {
            case PG_BOOL -> PG_ARR_BOOL;
            case PG_INT2 -> PG_ARR_INT2;
            case PG_INT4 -> PG_ARR_INT4;
            case PG_INT8 -> PG_ARR_INT8;
            case PG_FLOAT4 -> PG_ARR_FLOAT4;
            case PG_FLOAT8 -> PG_ARR_FLOAT8;
            case PG_TIMESTAMP -> PG_ARR_TIMESTAMP;
            case PG_TIMESTAMP_TZ -> PG_ARR_TIMESTAMP_TZ;
            case PG_VARCHAR -> PG_ARR_VARCHAR;
            default -> 0;
        };
    }

    public static int toColumnBinaryType(short code, int type) {
        return (((int) code) << 31) | type;
    }

    public static int toColumnType(int type) {
        // clear format flag
        return type & (~(1 << 31));
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
        // We represent QuestDB native DATE type as TIMESTAMP in PostgreSQL
        // Why? QuestDB DATE type has millisecond precision, while PostgreSQL DATE type has 'day' precision.
        // This is a workaround to avoid data loss when transferring data from QuestDB to PostgreSQL.
        TYPE_OIDS.extendAndSet(ColumnType.DATE, PG_TIMESTAMP); // TIMESTAMP, not PG_DATE! (this intentional)
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
        TYPE_OIDS.extendAndSet(ColumnType.ARRAY_STRING, PG_VARCHAR); // ARRAY_STRING is a hack, we send results as VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.DECIMAL8, PG_NUMERIC); // NUMERIC
        TYPE_OIDS.extendAndSet(ColumnType.DECIMAL16, PG_NUMERIC); // NUMERIC
        TYPE_OIDS.extendAndSet(ColumnType.DECIMAL32, PG_NUMERIC); // NUMERIC
        TYPE_OIDS.extendAndSet(ColumnType.DECIMAL64, PG_NUMERIC); // NUMERIC
        TYPE_OIDS.extendAndSet(ColumnType.DECIMAL128, PG_NUMERIC); // NUMERIC
        TYPE_OIDS.extendAndSet(ColumnType.DECIMAL256, PG_NUMERIC); // NUMERIC

        TYPE_ARR_OIDS.extendAndSet(ColumnType.DOUBLE, PG_ARR_FLOAT8); // FLOAT8[]
        TYPE_ARR_OIDS.extendAndSet(ColumnType.LONG, PG_ARR_INT8); // INT8[]

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
        PG_TYPE_OIDS.add(PG_ARR_FLOAT8);
        PG_TYPE_OIDS.add(PG_NUMERIC);

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
        PG_TYPE_PROC_OIDS.add(2400); // ARRAY
        PG_TYPE_PROC_OIDS.add(3823); // NUMERIC

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
        PG_TYPE_TO_NAME[14] = "_float8";
        PG_TYPE_TO_NAME[15] = "numeric";

        // array are excluded since all arrays are handled by the same function
        for (int i = 0, n = PG_TYPE_TO_NAME.length; i < n; i++) {
            int pgOid = PG_TYPE_OIDS.getQuick(i);
            boolean isArr = pgArrayToElementType(pgOid) != 0;
            if (!isArr) {
                PG_TYPE_TO_PROC_NAME[i] = PG_TYPE_TO_NAME[i] + "_recv";
                PG_TYPE_TO_PROC_SRC[i] = PG_TYPE_TO_NAME[i] + "recv";
            } else {
                // array types are handled by the same function in PostgreSQL
                PG_TYPE_TO_PROC_NAME[i] = "array_recv";
                PG_TYPE_TO_PROC_SRC[i] = "array_recv"; // intentionally the same as name (=with underscore), that's how it is in PostgreSQL
            }
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
        PG_TYPE_TO_CATEGORY[14] = 'A';
        PG_TYPE_TO_CATEGORY[15] = 'N';

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
        PG_TYPE_TO_LENGTH[14] = -1;
        PG_TYPE_TO_LENGTH[15] = -1;

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
        PG_TYPE_TO_DEFAULT[14] = null;
        PG_TYPE_TO_DEFAULT[15] = null;
    }
}
