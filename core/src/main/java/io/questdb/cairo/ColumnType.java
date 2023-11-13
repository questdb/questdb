/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.*;
import io.questdb.std.str.StringSink;

// ColumnType is always a 32bit int with layout:
//
// | PGWire format | Extra type information  | Tag    |
// +---------------+-------------------------+--------+
// |    1 bit      |        23 bits          | 8 bits |
// +------------------+----------------------+--------+
// |              0|0000000 00000000 00000000|00000000|
// |              0|               1 <-- 16th isGeoHash
// |              0|              1  <-- 17th isDesignatedTimestamp
public final class ColumnType {
    public static final int DYNAMIC_TYPE_SIZE = 0; // visible for test only
    // geohash bits <-> backing primitive types bit boundaries
    public static final int GEOBYTE_MAX_BITS = 7;
    public static final int GEOBYTE_MIN_BITS = 1;
    public static final int GEOINT_MAX_BITS = 31;
    public static final int GEOINT_MIN_BITS = 16;
    public static final int GEOLONG_MAX_BITS = 60;
    public static final int GEOLONG_MIN_BITS = 32;
    public static final int GEOSHORT_MAX_BITS = 15;
    public static final int GEOSHORT_MIN_BITS = 8;
    public static final int MIGRATION_VERSION = 427;
    public static final int NOT_STORED_TYPE_SIZE = -1; // visible for test only
    public static final int OVERLOAD_NONE = 10000; // akin to infinite distance
    // Column type discriminants (tags) are numeric byte values, ordered:
    // - from UNDEFINED: index 0, lack of type, an internal parsing concept.
    //          functions cannot accept UNDEFINED (signature not supported).
    // - to NULL: index must be last, codebase relies on this.
    public static final byte UNDEFINED = 0;                    // 0
    public static final byte BOOLEAN = UNDEFINED + 1;          // 1 not numeric
    public static final byte BYTE = BOOLEAN + 1;               // 2 numeric
    public static final byte SHORT = BYTE + 1;                 // 3 numeric
    public static final byte CHAR = SHORT + 1;                 // 4 not numeric
    public static final byte INT = CHAR + 1;                   // 5 numeric
    public static final byte LONG = INT + 1;                   // 6 numeric
    public static final byte DATE = LONG + 1;                  // 7 numeric
    public static final byte TIMESTAMP = DATE + 1;             // 8 numeric
    public static final byte FLOAT = TIMESTAMP + 1;            // 9 numeric
    public static final byte DOUBLE = FLOAT + 1;               // 10 numeric
    public static final byte STRING = DOUBLE + 1;              // 11
    public static final byte SYMBOL = STRING + 1;              // 12
    public static final byte LONG256 = SYMBOL + 1;             // 13
    public static final byte GEOBYTE = LONG256 + 1;            // 14 internal only
    public static final byte GEOSHORT = GEOBYTE + 1;           // 15 internal only
    public static final byte GEOINT = GEOSHORT + 1;            // 16 internal only
    public static final byte GEOLONG = GEOINT + 1;             // 17 internal only
    public static final byte BINARY = GEOLONG + 1;             // 18
    public static final byte UUID = BINARY + 1;                // 19
    public static final byte CURSOR = UUID + 1;                // 20 internal only
    public static final byte VAR_ARG = CURSOR + 1;             // 21 internal only
    public static final byte RECORD = VAR_ARG + 1;             // 22 internal only
    public static final byte GEOHASH = RECORD + 1;             // 23 internal only, used generically in function signature to resolve overloads
    public static final byte LONG128 = GEOHASH + 1;            // 24 limited support, few tests only
    public static final byte IPv4 = LONG128 + 1;               // 25
    public static final byte REGCLASS = IPv4 + 1;              // 26 pg-wire only
    public static final byte REGPROCEDURE = REGCLASS + 1;      // 27 pg-wire only
    public static final byte ARRAY_STRING = REGPROCEDURE + 1;  // 28 pg-wire only
    public static final byte NULL = ARRAY_STRING + 1;          // 29 internal only, NULL is ALWAYS last
    private static final short[] TYPE_SIZE = new short[NULL + 1];
    private static final short[] TYPE_SIZE_POW2 = new short[TYPE_SIZE.length];
    // slightly bigger than needed to make it a power of 2
    public static final short OVERLOAD_PRIORITY_N = (short) Math.pow(2.0, Numbers.msb(NULL) + 1.0);
    public static final int[] OVERLOAD_PRIORITY_MATRIX = new int[OVERLOAD_PRIORITY_N * OVERLOAD_PRIORITY_N]; // NULL to any is 0
    // column type version as written to the metadata file
    public static final int VERSION = 426;
    static final short[] GEO_TYPE_SIZE_POW2;
    private static final LowerCaseCharSequenceIntHashMap NAME2TYPE = new LowerCaseCharSequenceIntHashMap();
    private static final int OVERLOAD_MAX = -1; // akin to no distance
    private static final byte[][] OVERLOAD_PRIORITY;
    private static final int TAG_SIZE = Byte.SIZE;
    private static final IntObjHashMap<String> TYPE2NAME = new IntObjHashMap<>();
    private static final int TYPE_FLAG_DESIGNATED_TIMESTAMP = (1 << 17);
    private static final int TYPE_FLAG_GEOHASH = (1 << 16);

    private ColumnType() {
    }

    public static int getGeoHashBits(int type) {
        return (byte) ((type >> TAG_SIZE) & 0xFF);
    }

    public static int getGeoHashTypeWithBits(int bits) {
        assert bits > 0 && bits <= GEOLONG_MAX_BITS;
        // this logic relies on GeoHash type value to be clustered together
        return mkGeoHashType(bits, (byte) (GEOBYTE + pow2SizeOfBits(bits)));
    }

    public static boolean isAssignableFrom(int fromType, int toType) {
        return isToSameOrWider(fromType, toType) || isNarrowingCast(fromType, toType);
    }

    public static boolean isBinary(int columnType) {
        return columnType == BINARY;
    }

    public static boolean isBoolean(int columnType) {
        return columnType == BOOLEAN;
    }

    public static boolean isBuiltInWideningCast(int fromType, int toType) {
        // This method returns true when a cast is not needed from type to type
        // because of the way typed functions are implemented.
        // For example IntFunction has getDouble() method implemented and does not need
        // additional wrap function to CAST to double.
        // This is usually case for widening conversions.
        return (fromType >= BYTE && toType >= BYTE && toType <= DOUBLE && fromType < toType) || fromType == NULL
                // char can be short and short can be char for symmetry
                || (fromType == CHAR && toType == SHORT) || (fromType == TIMESTAMP && toType == LONG);
    }

    public static boolean isChar(int columnType) {
        return columnType == CHAR;
    }

    public static boolean isCursor(int columnType) {
        return columnType == CURSOR;
    }

    public static boolean isDesignatedTimestamp(int type) {
        return (type & TYPE_FLAG_DESIGNATED_TIMESTAMP) != 0;
    }

    public static boolean isDouble(int columnType) {
        return columnType == DOUBLE;
    }

    public static boolean isGeoHash(int columnType) {
        return (columnType & TYPE_FLAG_GEOHASH) != 0;
    }

    public static boolean isInt(int columnType) {
        return columnType == INT;
    }

    public static boolean isNull(int columnType) {
        return columnType == NULL;
    }

    public static boolean isString(int columnType) {
        return columnType == STRING;
    }

    public static boolean isSymbol(int columnType) {
        return columnType == SYMBOL;
    }

    public static boolean isSymbolOrString(int columnType) {
        return columnType == SYMBOL || columnType == STRING;
    }

    public static boolean isTimestamp(int columnType) {
        return columnType == TIMESTAMP;
    }

    public static boolean isToSameOrWider(int fromType, int toType) {
        return ((toType == fromType || tagOf(fromType) == tagOf(toType)) && (getGeoHashBits(fromType) >= getGeoHashBits(toType) || getGeoHashBits(fromType) == 0)) || isBuiltInWideningCast(fromType, toType) || isStringCast(fromType, toType) || isGeoHashWideningCast(fromType, toType) || isImplicitParsingCast(fromType, toType) || isIPv4Cast(fromType, toType);
    }

    public static boolean isUndefined(int columnType) {
        return columnType == UNDEFINED;
    }

    public static boolean isVariableLength(int columnType) {
        return columnType == STRING || columnType == BINARY;
    }

    public static String nameOf(int columnType) {
        int index = TYPE2NAME.keyIndex(columnType);
        return index < 0 ? TYPE2NAME.valueAtQuick(index) : "unknown";
    }

    public static int overloadDistance(byte fromType, byte toType) {
        assert toType != UNDEFINED : "Undefined not supported in overloads";
        return OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * tagOf(fromType) + tagOf(toType)];
    }

    public static int pow2SizeOf(int columnType) {
        return TYPE_SIZE_POW2[tagOf(columnType)];
    }

    public static short pow2SizeOfBits(int bits) {
        assert bits <= GEOLONG_MAX_BITS;
        return GEO_TYPE_SIZE_POW2[bits];
    }

    public static int setDesignatedTimestampBit(int type, boolean designated) {
        if (designated) {
            return type | TYPE_FLAG_DESIGNATED_TIMESTAMP;
        } else {
            return type & ~(TYPE_FLAG_DESIGNATED_TIMESTAMP);
        }
    }

    public static int sizeOf(int columnType) {
        byte tag = tagOf(columnType);
        return tag < TYPE_SIZE.length ? TYPE_SIZE[tag] : NOT_STORED_TYPE_SIZE;
    }

    public static byte tagOf(int type) {
        return (byte) (type & 0xFF);
    }

    public static byte tagOf(CharSequence name) {
        return (byte) NAME2TYPE.get(name);
    }

    public static int typeOf(CharSequence name) {
        return NAME2TYPE.get(name);
    }

    public static int variableColumnLengthBytes(int columnType) {
        if (columnType == STRING) {
            return Integer.BYTES;
        }
        assert columnType == BINARY;
        return Long.BYTES;
    }

    private static boolean isGeoHashWideningCast(int fromType, int toType) {
        final int toTag = tagOf(toType);
        final int fromTag = tagOf(fromType);
        return (fromTag == GEOLONG && toTag == GEOINT) || (fromTag == GEOLONG && toTag == GEOSHORT) || (fromTag == GEOLONG && toTag == GEOBYTE) || (fromTag == GEOINT && toTag == GEOSHORT) || (fromTag == GEOINT && toTag == GEOBYTE) || (fromTag == GEOSHORT && toTag == GEOBYTE);
    }

    private static boolean isIPv4Cast(int fromType, int toType) {
        return (fromType == STRING && toType == IPv4);
    }

    private static boolean isImplicitParsingCast(int fromType, int toType) {
        final int toTag = tagOf(toType);
        return (fromType == CHAR && toTag == GEOBYTE && getGeoHashBits(toType) < 6) || (fromType == STRING && toTag == GEOBYTE) || (fromType == STRING && toTag == GEOSHORT) || (fromType == STRING && toTag == GEOINT) || (fromType == STRING && toTag == GEOLONG) || (fromType == STRING && toTag == TIMESTAMP) || (fromType == SYMBOL && toTag == TIMESTAMP) || (fromType == STRING && toTag == LONG256);
    }

    private static boolean isNarrowingCast(int fromType, int toType) {
        return (fromType == DOUBLE && (toType == FLOAT || (toType >= BYTE && toType <= LONG))) || (fromType == FLOAT && toType >= BYTE && toType <= LONG) || (fromType == LONG && toType >= BYTE && toType <= INT) || (fromType == INT && toType >= BYTE && toType <= SHORT) || (fromType == SHORT && toType == BYTE) || (fromType == CHAR && toType == BYTE) || (fromType == STRING && toType == BYTE) || (fromType == STRING && toType == SHORT) || (fromType == STRING && toType == INT) || (fromType == STRING && toType == LONG) || (fromType == STRING && toType == DATE) || (fromType == STRING && toType == TIMESTAMP) || (fromType == STRING && toType == FLOAT) || (fromType == STRING && toType == DOUBLE) || (fromType == STRING && toType == CHAR) || (fromType == STRING && toType == UUID);
    }

    private static boolean isStringCast(int fromType, int toType) {
        return (fromType == STRING && toType == SYMBOL) || (fromType == SYMBOL && toType == STRING) || (fromType == CHAR && toType == SYMBOL) || (fromType == CHAR && toType == STRING) || (fromType == UUID && toType == STRING);
    }

    private static int mkGeoHashType(int bits, byte baseType) {
        return (baseType & ~(0xFF << TAG_SIZE)) | (bits << TAG_SIZE) | TYPE_FLAG_GEOHASH; // bit 16 is GeoHash flag
    }

    private static short pow2(short value) {
        switch (value) {
            case NOT_STORED_TYPE_SIZE:
            case DYNAMIC_TYPE_SIZE:
                return -1;
            default:
                return (short) Numbers.msb(value);
        }
    }

    static {
        assert MIGRATION_VERSION >= VERSION;
        // type overload priority matrix:
        OVERLOAD_PRIORITY = new byte[][]{
                /* 0 UNDEFINED     */  {DOUBLE, FLOAT, STRING, LONG, TIMESTAMP, DATE, INT, CHAR, SHORT, BYTE, BOOLEAN}
                /* 1  BOOLEAN      */, {BOOLEAN}
                /* 2  BYTE         */, {BYTE, SHORT, INT, LONG, FLOAT, DOUBLE}
                /* 3  SHORT        */, {SHORT, INT, LONG, FLOAT, DOUBLE}
                /* 4  CHAR         */, {CHAR, STRING}
                /* 5  INT          */, {INT, LONG, FLOAT, DOUBLE, TIMESTAMP, DATE}
                /* 6  LONG         */, {LONG, DOUBLE, TIMESTAMP, DATE}
                /* 7  DATE         */, {DATE, TIMESTAMP, LONG}
                /* 8  TIMESTAMP    */, {TIMESTAMP, LONG, DATE}
                /* 9  FLOAT        */, {FLOAT, DOUBLE}
                /* 10 DOUBLE       */, {DOUBLE}
                /* 11 STRING       */, {STRING, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE}
                /* 12 SYMBOL       */, {SYMBOL, STRING}
                /* 13 LONG256      */, {LONG256}
                /* 14 GEOBYTE      */, {GEOBYTE, GEOSHORT, GEOINT, GEOLONG, GEOHASH}
                /* 15 GEOSHORT     */, {GEOSHORT, GEOINT, GEOLONG, GEOHASH}
                /* 16 GEOINT       */, {GEOINT, GEOLONG, GEOHASH}
                /* 17 GEOLONG      */, {GEOLONG, GEOHASH}
                /* 18 BINARY       */, {BINARY}
                /* 19 UUID         */, {UUID, STRING}
        };
        // see testOverloadPriorityMatrix for a view of the matrix
        for (byte fromTag = UNDEFINED; fromTag < NULL; fromTag++) { // distance NULL to any type is 0
            OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * fromTag + UNDEFINED] = OVERLOAD_NONE;
            for (byte toTag = BOOLEAN; toTag <= NULL; toTag++) {
                int value = toTag == VAR_ARG ? OVERLOAD_MAX : OVERLOAD_NONE;
                if (fromTag < OVERLOAD_PRIORITY.length) {
                    byte[] priority = OVERLOAD_PRIORITY[fromTag];
                    for (int i = 0; i < priority.length; i++) {
                        if (priority[i] == toTag) {
                            value = i;
                            break;
                        }
                    }
                }
                OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * fromTag + toTag] = value;
            }
        }
        OVERLOAD_PRIORITY_MATRIX[VAR_ARG] = OVERLOAD_NONE;
        // When null used as func arg, default to string as function factory arg
        OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * NULL + STRING] = OVERLOAD_MAX;
        // Do the same for symbol -> avoids weird null behaviour
        OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * NULL + SYMBOL] = OVERLOAD_MAX;

        TYPE_SIZE[CURSOR] = NOT_STORED_TYPE_SIZE;
        TYPE_SIZE[VAR_ARG] = NOT_STORED_TYPE_SIZE;
        TYPE_SIZE[RECORD] = NOT_STORED_TYPE_SIZE;
        TYPE_SIZE[UNDEFINED] = NOT_STORED_TYPE_SIZE;
        TYPE_SIZE[BOOLEAN] = Byte.BYTES;
        TYPE_SIZE[BYTE] = Byte.BYTES;
        TYPE_SIZE[SHORT] = Short.BYTES;
        TYPE_SIZE[CHAR] = Character.BYTES;
        TYPE_SIZE[INT] = Integer.BYTES;
        TYPE_SIZE[LONG] = Long.BYTES;
        TYPE_SIZE[DATE] = Long.BYTES;
        TYPE_SIZE[TIMESTAMP] = Long.BYTES;
        TYPE_SIZE[FLOAT] = Float.BYTES;
        TYPE_SIZE[DOUBLE] = Double.BYTES;
        TYPE_SIZE[STRING] = DYNAMIC_TYPE_SIZE;
        TYPE_SIZE[SYMBOL] = Integer.BYTES;
        TYPE_SIZE[LONG256] = Long256.BYTES;
        TYPE_SIZE[GEOBYTE] = Byte.BYTES;
        TYPE_SIZE[GEOSHORT] = Short.BYTES;
        TYPE_SIZE[GEOINT] = Integer.BYTES;
        TYPE_SIZE[GEOLONG] = Long.BYTES;
        TYPE_SIZE[BINARY] = DYNAMIC_TYPE_SIZE;
        TYPE_SIZE[UUID] = Uuid.BYTES;
        TYPE_SIZE[LONG128] = Long128.BYTES;
        TYPE_SIZE[IPv4] = Integer.BYTES;
        TYPE_SIZE[NULL] = DYNAMIC_TYPE_SIZE; // storage size is column dependent

        TYPE_SIZE_POW2[UNDEFINED] = pow2(TYPE_SIZE[UNDEFINED]);
        TYPE_SIZE_POW2[BOOLEAN] = pow2(TYPE_SIZE[BOOLEAN]);
        TYPE_SIZE_POW2[BYTE] = pow2(TYPE_SIZE[BYTE]);
        TYPE_SIZE_POW2[SHORT] = pow2(TYPE_SIZE[SHORT]);
        TYPE_SIZE_POW2[CHAR] = pow2(TYPE_SIZE[CHAR]);
        TYPE_SIZE_POW2[FLOAT] = pow2(TYPE_SIZE[FLOAT]);
        TYPE_SIZE_POW2[INT] = pow2(TYPE_SIZE[INT]);
        TYPE_SIZE_POW2[IPv4] = pow2(TYPE_SIZE[IPv4]);
        TYPE_SIZE_POW2[SYMBOL] = pow2(TYPE_SIZE[SYMBOL]);
        TYPE_SIZE_POW2[DOUBLE] = pow2(TYPE_SIZE[DOUBLE]);
        TYPE_SIZE_POW2[STRING] = pow2(TYPE_SIZE[STRING]);
        TYPE_SIZE_POW2[LONG] = pow2(TYPE_SIZE[LONG]);
        TYPE_SIZE_POW2[DATE] = pow2(TYPE_SIZE[DATE]);
        TYPE_SIZE_POW2[TIMESTAMP] = pow2(TYPE_SIZE[TIMESTAMP]);
        TYPE_SIZE_POW2[LONG256] = pow2(TYPE_SIZE[LONG256]);
        TYPE_SIZE_POW2[GEOBYTE] = pow2(TYPE_SIZE[GEOBYTE]);
        TYPE_SIZE_POW2[GEOSHORT] = pow2(TYPE_SIZE[GEOSHORT]);
        TYPE_SIZE_POW2[GEOINT] = pow2(TYPE_SIZE[GEOINT]);
        TYPE_SIZE_POW2[GEOLONG] = pow2(TYPE_SIZE[GEOLONG]);
        TYPE_SIZE_POW2[BINARY] = pow2(TYPE_SIZE[BINARY]);
        TYPE_SIZE_POW2[CURSOR] = pow2(TYPE_SIZE[CURSOR]);
        TYPE_SIZE_POW2[VAR_ARG] = pow2(TYPE_SIZE[VAR_ARG]);
        TYPE_SIZE_POW2[RECORD] = pow2(TYPE_SIZE[RECORD]);
        TYPE_SIZE_POW2[NULL] = pow2(TYPE_SIZE[NULL]);
        TYPE_SIZE_POW2[LONG128] = pow2(TYPE_SIZE[LONG128]);
        TYPE_SIZE_POW2[UUID] = pow2(TYPE_SIZE[UUID]);

        GEO_TYPE_SIZE_POW2 = new short[GEOLONG_MAX_BITS + 1];
        for (int bits = 1; bits <= GEOLONG_MAX_BITS; bits++) {
            GEO_TYPE_SIZE_POW2[bits] = (short) Numbers.msb(Numbers.ceilPow2(((bits + Byte.SIZE) & -Byte.SIZE)) >> 3);
        }
        StringSink sink = new StringSink();
        for (int b = 1; b <= GEOLONG_MAX_BITS; b++) {
            sink.clear();
            if (b % 5 != 0) {
                sink.put("GEOHASH(").put(b).put("b)");
            } else {
                sink.put("GEOHASH(").put(b / 5).put("c)");
            }
            String name = sink.toString();
            int type = getGeoHashTypeWithBits(b);
            NAME2TYPE.put(name, type);
            TYPE2NAME.put(type, name);
        }

        TYPE2NAME.put(UNDEFINED, "UNDEFINED");
        TYPE2NAME.put(BOOLEAN, "BOOLEAN");
        TYPE2NAME.put(BYTE, "BYTE");
        TYPE2NAME.put(SHORT, "SHORT");
        TYPE2NAME.put(CHAR, "CHAR");
        TYPE2NAME.put(INT, "INT");
        TYPE2NAME.put(LONG, "LONG");
        TYPE2NAME.put(DATE, "DATE");
        TYPE2NAME.put(TIMESTAMP, "TIMESTAMP");
        TYPE2NAME.put(FLOAT, "FLOAT");
        TYPE2NAME.put(DOUBLE, "DOUBLE");
        TYPE2NAME.put(STRING, "STRING");
        TYPE2NAME.put(SYMBOL, "SYMBOL");
        TYPE2NAME.put(LONG256, "LONG256");
        TYPE2NAME.put(GEOBYTE, "GEOBYTE");
        TYPE2NAME.put(GEOSHORT, "GEOSHORT");
        TYPE2NAME.put(GEOINT, "GEOINT");
        TYPE2NAME.put(GEOLONG, "GEOLONG");
        TYPE2NAME.put(BINARY, "BINARY");
        TYPE2NAME.put(UUID, "UUID");
        TYPE2NAME.put(CURSOR, "CURSOR");
        TYPE2NAME.put(VAR_ARG, "VARARG");
        TYPE2NAME.put(RECORD, "RECORD");
        TYPE2NAME.put(GEOHASH, "GEOHASH");
        TYPE2NAME.put(LONG128, "LONG128");
        TYPE2NAME.put(IPv4, "IPv4");
        TYPE2NAME.put(REGCLASS, "regclass");
        TYPE2NAME.put(REGPROCEDURE, "regprocedure");
        TYPE2NAME.put(ARRAY_STRING, "text[]");
        TYPE2NAME.put(NULL, "NULL");

        NAME2TYPE.put("undefined", UNDEFINED);
        NAME2TYPE.put("boolean", BOOLEAN);
        NAME2TYPE.put("byte", BYTE);
        NAME2TYPE.put("short", SHORT);
        NAME2TYPE.put("smallint", SHORT);
        NAME2TYPE.put("char", CHAR);
        NAME2TYPE.put("int", INT);
        NAME2TYPE.put("long", LONG);
        NAME2TYPE.put("bigint", LONG);
        NAME2TYPE.put("date", DATE);
        NAME2TYPE.put("timestamp", TIMESTAMP);
        NAME2TYPE.put("float", FLOAT);
        NAME2TYPE.put("real", FLOAT);
        NAME2TYPE.put("double", DOUBLE);
        NAME2TYPE.put("string", STRING);
        NAME2TYPE.put("bytea", STRING);
        NAME2TYPE.put("varchar", STRING);
        NAME2TYPE.put("text", STRING);
        NAME2TYPE.put("symbol", SYMBOL);
        NAME2TYPE.put("long256", LONG256);
        NAME2TYPE.put("geobyte", GEOBYTE);
        NAME2TYPE.put("geoshort", GEOSHORT);
        NAME2TYPE.put("geoint", GEOINT);
        NAME2TYPE.put("geolong", GEOLONG);
        NAME2TYPE.put("binary", BINARY);
        NAME2TYPE.put("uuid", UUID);
        NAME2TYPE.put("cursor", CURSOR);
        NAME2TYPE.put("vararg", VAR_ARG);
        NAME2TYPE.put("record", RECORD);
        NAME2TYPE.put("geohash", GEOHASH);
        NAME2TYPE.put("long128", LONG128);
        NAME2TYPE.put("IPv4", IPv4);
        NAME2TYPE.put("regclass", REGCLASS);
        NAME2TYPE.put("regprocedure", REGPROCEDURE);
        NAME2TYPE.put("text[]", ARRAY_STRING);
        NAME2TYPE.put("null", NULL);
    }
}
