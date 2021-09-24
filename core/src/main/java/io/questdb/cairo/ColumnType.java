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

import io.questdb.std.IntObjHashMap;
import io.questdb.std.Long256;
import io.questdb.std.LowerCaseAsciiCharSequenceIntHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;

// ColumnType layout - 32bit
//
// | PGWire format | Extra type information | Type discriminant (tag) |
// +---------------+------------------------+-------------------------+
// |    1 bit      |        23 bits         |         8 bits          |
// +---------------+------------------------+-------------------------+

/**
 * Column types as numeric (integer) values
 */
public final class ColumnType {
    // column type version as written to the metadata file
    public static final int VERSION = 421;

    public static final short UNDEFINED = 0;
    public static final short BOOLEAN = 1;
    public static final short BYTE = 2;
    public static final short SHORT = 3;
    public static final short CHAR = 4;
    public static final short INT = 5;
    public static final short LONG = 6;
    public static final short DATE = 7;
    public static final short TIMESTAMP = 8;
    public static final short FLOAT = 9;
    public static final short DOUBLE = 10;
    public static final short STRING = 11;
    public static final short SYMBOL = 12;
    public static final short LONG256 = 13;
    public static final short GEOBYTE = 14;
    public static final short GEOSHORT = 15;
    public static final short GEOINT = 16;
    public static final short GEOLONG = 17;
    public static final short BINARY = 18;
    public static final short PARAMETER = 19;
    public static final short CURSOR = 20;
    public static final short VAR_ARG = 21;
    public static final short RECORD = 22;
    // This type is not stored, it is used on function arguments to resolve overloads.
    // We also build overload matrix, which logic relies on the fact GEOHASH value has to be
    // inside the MAX type value.
    public static final short GEOHASH = 23;
    public static final short NULL = 24;

    // Overload matrix algo depends on the fact that MAX == NULL
    public static final short MAX = NULL;
    public static final short TYPES_SIZE = MAX + 1;
    private static final int[] TYPE_SIZE_POW2 = new int[TYPES_SIZE];
    private static final int[] TYPE_SIZE = new int[TYPES_SIZE];
    public static final int GEO_HASH_MAX_BITS_LENGTH;
    public static final int NO_OVERLOAD = 10000;
    static final int[] GEO_TYPE_SIZE_POW2;
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();
    private static final LowerCaseAsciiCharSequenceIntHashMap nameTypeMap = new LowerCaseAsciiCharSequenceIntHashMap();
    // For function overload the priority is taken from left to right
    private static final short[][] overloadPriority = {
            /* 0 UNDEFINED  */  {DOUBLE, FLOAT, STRING, LONG, TIMESTAMP, DATE, INT, CHAR, SHORT, BYTE, BOOLEAN}
            /* 1  BOOLEAN   */, {BOOLEAN}
            /* 2  BYTE      */, {BYTE, SHORT, INT, LONG, FLOAT, DOUBLE}
            /* 3  SHORT     */, {SHORT, INT, LONG, FLOAT, DOUBLE}
            /* 4  CHAR      */, {CHAR, STRING}
            /* 5  INT       */, {INT, LONG, FLOAT, DOUBLE, TIMESTAMP, DATE}
            /* 6  LONG      */, {LONG, DOUBLE, TIMESTAMP, DATE}
            /* 7  DATE      */, {DATE, TIMESTAMP, LONG}
            /* 8  TIMESTAMP */, {TIMESTAMP, LONG}
            /* 9  FLOAT     */, {FLOAT, DOUBLE}
            /* 10 DOUBLE    */, {DOUBLE}
            /* 11 STRING    */, {} // STRING can be cast to TIMESTAMP, but it's handled in a special way
            /* 12 SYMBOL    */, {SYMBOL, STRING}
            /* 13 LONG256   */, {LONG256}
            /* 14 GEOBYTE   */, {GEOBYTE, GEOSHORT, GEOINT, GEOLONG, GEOHASH}
            /* 15 GEOSHORT  */, {GEOSHORT, GEOINT, GEOLONG, GEOHASH}
            /* 16 GEOINT    */, {GEOINT, GEOLONG, GEOHASH}
            /* 17 GEOLONG   */, {GEOLONG, GEOHASH}
            /* 18 BINARY    */, {BINARY}
    };

    // this value has to be larger than MAX type and be power of 2
    private static final int OVERLOAD_MATRIX_SIZE = 32;
    private static final int[] overloadPriorityMatrix;
    private static final int TYPE_FLAG_GEO_HASH = (1 << 16);
    private static final int TYPE_FLAG_DESIGNATED_TIMESTAMP = (1 << 17);
    private static final int BITS_OFFSET = 8;

    private ColumnType() {
    }

    public static int getGeoHashBits(int type) {
        return (byte) ((type >> BITS_OFFSET) & 0xFF);
    }

    public static int getGeoHashTypeWithBits(int bits) {
        assert bits > 0 && bits <= GEO_HASH_MAX_BITS_LENGTH;
        // this logic relies on GeoHash type value to be clustered together
        return mkGeoHashType(bits, (short) (GEOBYTE + pow2SizeOfBits(bits)));
    }

    public static boolean isBinary(int columnType) {
        return columnType == BINARY;
    }

    public static boolean isBoolean(int columnType) {
        return columnType == ColumnType.BOOLEAN;
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
        return (columnType & TYPE_FLAG_GEO_HASH) != 0;
    }

    public static boolean isInt(int columnType) {
        return columnType == ColumnType.INT;
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

    public static boolean isUndefined(int columnType) {
        return columnType == UNDEFINED;
    }

    public static boolean isVariableLength(int columnType) {
        return columnType == STRING || columnType == BINARY;
    }

    public static String nameOf(int columnType) {
        final int index = typeNameMap.keyIndex(columnType);
        if (index > -1) {
            return "unknown";
        }
        return typeNameMap.valueAtQuick(index);
    }

    public static int overloadDistance(short from, short to) {
        final int fromTag = tagOf(from);
        final int toTag = tagOf(to);
        // Functions cannot accept UNDEFINED type (signature is not supported)
        // this check is just in case
        assert toTag > UNDEFINED : "Undefined not supported in overloads";
        return overloadPriorityMatrix[OVERLOAD_MATRIX_SIZE * fromTag + toTag];
    }

    public static int pow2SizeOf(int columnType) {
        return TYPE_SIZE_POW2[tagOf(columnType)];
    }

    public static int pow2SizeOfBits(int bits) {
        assert bits <= GEO_HASH_MAX_BITS_LENGTH;
        return GEO_TYPE_SIZE_POW2[bits];
    }

    public static int setDesignatedTimestampBit(int tsType, boolean designated) {
        if (designated) {
            return tsType | TYPE_FLAG_DESIGNATED_TIMESTAMP;
        } else {
            return tsType & ~(TYPE_FLAG_DESIGNATED_TIMESTAMP);
        }
    }

    public static int sizeOf(int columnType) {
        short tag = tagOf(columnType); // tagOf
        if (tag < TYPES_SIZE) {
            return TYPE_SIZE[tag];
        }
        return -1;
    }

    public static short tagOf(int type) {
        return (short) (type & 0xFF);
    }

    public static short tagOf(CharSequence name) {
        return (short) nameTypeMap.get(name);
    }

    public static long truncateGeoHashBits(long value, int fromBits, int toBits) {
        return value >> (fromBits - toBits);
    }

    // This method used by row copier assembler
    public static long truncateGeoHashTypes(long value, int fromType, int toType) {
        final int fromBits = getGeoHashBits(fromType);
        final int toBits = getGeoHashBits(toType);
        assert fromBits >= toBits;
        return truncateGeoHashBits(value, fromBits, toBits);
    }

    private static int mkGeoHashType(int bits, short baseType) {
        return (baseType & ~(0xFF << BITS_OFFSET)) | (bits << BITS_OFFSET) | TYPE_FLAG_GEO_HASH; // bit 16 is GeoHash flag
    }

    private static short indexOf(short[] list, short value) {
        for (short i = 0; i < list.length; i++) {
            if (list[i] == value) {
                return i;
            }
        }
        return -1;
    }

    static {
        overloadPriorityMatrix = new int[OVERLOAD_MATRIX_SIZE * OVERLOAD_MATRIX_SIZE];
        for (short i = UNDEFINED; i < MAX; i++) {
            for (short j = BOOLEAN; j < MAX; j++) {
                if (i < overloadPriority.length) {
                    int index = indexOf(overloadPriority[i], j);
                    overloadPriorityMatrix[OVERLOAD_MATRIX_SIZE * i + j] = index != -1 ? index : NO_OVERLOAD;
                } else {
                    overloadPriorityMatrix[OVERLOAD_MATRIX_SIZE * i + j] = NO_OVERLOAD;
                }
            }
        }
    }

    static {
        GEO_HASH_MAX_BITS_LENGTH = 60;
        GEO_TYPE_SIZE_POW2 = new int[GEO_HASH_MAX_BITS_LENGTH + 1];
        for (int bits = 1; bits <= GEO_HASH_MAX_BITS_LENGTH; bits++) {
            GEO_TYPE_SIZE_POW2[bits] = Numbers.msb(Numbers.ceilPow2(((bits + Byte.SIZE) & -Byte.SIZE)) >> 3);
        }

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
        typeNameMap.put(LONG256, "LONG256");
        typeNameMap.put(CURSOR, "CURSOR");
        typeNameMap.put(RECORD, "RECORD");
        typeNameMap.put(VAR_ARG, "VARARG");
        typeNameMap.put(GEOHASH, "GEOHASH");

        StringSink sink = new StringSink();

        for (int b = 1; b <= GEO_HASH_MAX_BITS_LENGTH; b++) {
            sink.clear();

            if (b % 5 != 0) {
                sink.put("GEOHASH(").put(b).put("b)");
            } else {
                sink.put("GEOHASH(").put(b / 5).put("c)");
            }
            typeNameMap.put(getGeoHashTypeWithBits(b), sink.toString());
        }

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
        nameTypeMap.put("cursor", CURSOR);
        nameTypeMap.put("long256", LONG256);
        nameTypeMap.put("geohash", GEOHASH);
        nameTypeMap.put("text", STRING);
        nameTypeMap.put("smallint", SHORT);
        nameTypeMap.put("bigint", LONG);
        nameTypeMap.put("real", FLOAT);
        nameTypeMap.put("bytea", STRING);

        TYPE_SIZE_POW2[UNDEFINED] = -1;
        TYPE_SIZE_POW2[BOOLEAN] = 0;
        TYPE_SIZE_POW2[BYTE] = 0;
        TYPE_SIZE_POW2[SHORT] = 1;
        TYPE_SIZE_POW2[CHAR] = 1;
        TYPE_SIZE_POW2[FLOAT] = 2;
        TYPE_SIZE_POW2[INT] = 2;
        TYPE_SIZE_POW2[SYMBOL] = 2;
        TYPE_SIZE_POW2[DOUBLE] = 3;
        TYPE_SIZE[STRING] = -1;
        TYPE_SIZE_POW2[LONG] = 3;
        TYPE_SIZE_POW2[DATE] = 3;
        TYPE_SIZE_POW2[TIMESTAMP] = 3;
        TYPE_SIZE_POW2[LONG256] = 5;
        TYPE_SIZE_POW2[GEOBYTE] = 0;
        TYPE_SIZE_POW2[GEOSHORT] = 1;
        TYPE_SIZE_POW2[GEOINT] = 2;
        TYPE_SIZE_POW2[GEOLONG] = 3;
        TYPE_SIZE_POW2[BINARY] = 2;
        TYPE_SIZE_POW2[PARAMETER] = -1;
        TYPE_SIZE_POW2[CURSOR] = -1;
        TYPE_SIZE_POW2[VAR_ARG] = -1;
        TYPE_SIZE_POW2[RECORD] = -1;
        TYPE_SIZE_POW2[NULL] = -1;

        TYPE_SIZE[UNDEFINED] = -1;
        TYPE_SIZE[BOOLEAN] = Byte.BYTES;
        TYPE_SIZE[BYTE] = Byte.BYTES;
        TYPE_SIZE[SHORT] = Short.BYTES;
        TYPE_SIZE[CHAR] = Character.BYTES;
        TYPE_SIZE[FLOAT] = Float.BYTES;
        TYPE_SIZE[INT] = Integer.BYTES;
        TYPE_SIZE[SYMBOL] = Integer.BYTES;
        TYPE_SIZE[STRING] = 0;
        TYPE_SIZE[DOUBLE] = Double.BYTES;
        TYPE_SIZE[LONG] = Long.BYTES;
        TYPE_SIZE[DATE] = Long.BYTES;
        TYPE_SIZE[TIMESTAMP] = Long.BYTES;
        TYPE_SIZE[LONG256] = Long256.BYTES;
        TYPE_SIZE[GEOBYTE] = Byte.BYTES;
        TYPE_SIZE[GEOSHORT] = Short.BYTES;
        TYPE_SIZE[GEOINT] = Integer.BYTES;
        TYPE_SIZE[GEOLONG] = Long.BYTES;
        TYPE_SIZE[BINARY] = 0;
        TYPE_SIZE[PARAMETER] = -1;
        TYPE_SIZE[CURSOR] = -1;
        TYPE_SIZE[VAR_ARG] = -1;
        TYPE_SIZE[RECORD] = -1;
        TYPE_SIZE[NULL] = 0;
    }
}
