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

package io.questdb.cairo;

import io.questdb.std.*;
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
    public static final int GEOBYTE_MAX_BITS = 7;
    // geohash bits <-> backing primitive types bit boundaries
    public static final int GEOBYTE_MIN_BITS = 1;
    public static final int GEOINT_MAX_BITS = 31;
    public static final int GEOINT_MIN_BITS = 16;
    public static final int GEOLONG_MAX_BITS = 60;
    public static final int GEOLONG_MIN_BITS = 32;
    public static final int GEOSHORT_MAX_BITS = 15;
    public static final int GEOSHORT_MIN_BITS = 8;
    public static final int LEGACY_VAR_SIZE_AUX_SHL = 3;
    public static final int MIGRATION_VERSION = 427;
    public static final short OVERLOAD_FULL = -1; // akin to no distance
    public static final short OVERLOAD_NONE = 10000; // akin to infinite distance
    // our type system is absolutely ordered ranging
    // - from UNDEFINED: index 0, represents lack of type, an internal parsing concept.
    // - to NULL: index must be last, other parts of the codebase rely on this fact.
    public static final short UNDEFINED = 0;                    // = 0
    public static final short BOOLEAN = UNDEFINED + 1;          // = 1
    public static final short BYTE = BOOLEAN + 1;               // = 2
    public static final short SHORT = BYTE + 1;                 // = 3
    public static final short CHAR = SHORT + 1;                 // = 4
    public static final short INT = CHAR + 1;                   // = 5
    public static final short LONG = INT + 1;                   // = 6
    public static final short DATE = LONG + 1;                  // = 7
    public static final short TIMESTAMP = DATE + 1;             // = 8
    public static final short FLOAT = TIMESTAMP + 1;            // = 9
    public static final short DOUBLE = FLOAT + 1;               // = 10
    public static final short STRING = DOUBLE + 1;              // = 11
    public static final short SYMBOL = STRING + 1;              // = 12
    public static final short LONG256 = SYMBOL + 1;             // = 13
    public static final short GEOBYTE = LONG256 + 1;            // = 14
    public static final short GEOSHORT = GEOBYTE + 1;           // = 15
    public static final short GEOINT = GEOSHORT + 1;            // = 16
    public static final short GEOLONG = GEOINT + 1;             // = 17
    public static final short BINARY = GEOLONG + 1;             // = 18
    public static final short UUID = BINARY + 1;                // = 19
    public static final short CURSOR = UUID + 1;                // = 20
    public static final short VAR_ARG = CURSOR + 1;             // = 21
    public static final short RECORD = VAR_ARG + 1;             // = 22
    // GEOHASH is not stored. It is used on function
    // arguments to resolve overloads. We also build
    // overload matrix, which logic relies on GEOHASH
    // value >UUID and <MAX.
    public static final short GEOHASH = RECORD + 1;             // = 23
    public static final short LONG128 = GEOHASH + 1;            // = 24  Limited support, few tests only
    public static final short IPv4 = LONG128 + 1;               // = 25
    public static final short VARCHAR = IPv4 + 1;               // = 26
    // PG specific types to work with 3rd party software
    // with canned catalogue queries:
    // REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER
    public static final short REGCLASS = VARCHAR + 1;           // = 27;
    public static final short REGPROCEDURE = REGCLASS + 1;      // = 28;
    public static final short ARRAY_STRING = REGPROCEDURE + 1;  // = 29;
    public static final short PARAMETER = ARRAY_STRING + 1;     // = 30;
    public static final short INTERVAL = PARAMETER + 1;         // = 31
    public static final short NULL = INTERVAL + 1;              // = 32; ALWAYS the last
    private static final short[] TYPE_SIZE = new short[NULL + 1];
    private static final short[] TYPE_SIZE_POW2 = new short[TYPE_SIZE.length];
    // slightly bigger than needed to make it a power of 2
    private static final short OVERLOAD_PRIORITY_N = (short) Math.pow(2.0, Numbers.msb(NULL) + 1.0);
    private static final int[] OVERLOAD_PRIORITY_MATRIX = new int[OVERLOAD_PRIORITY_N * OVERLOAD_PRIORITY_N]; // NULL to any is 0
    public static final int VARCHAR_AUX_SHL = 4;
    // column type version as written to the metadata file
    public static final int VERSION = 426;
    static final int[] GEO_TYPE_SIZE_POW2;
    private static final boolean ALLOW_DEFAULT_STRING_CHANGE = false;
    private static final int BITS_OFFSET = 8;
    private static final short[][] OVERLOAD_PRIORITY;
    private static final int TYPE_FLAG_DESIGNATED_TIMESTAMP = (1 << 17);
    private static final int TYPE_FLAG_GEO_HASH = (1 << 16);
    private static final LowerCaseAsciiCharSequenceIntHashMap nameTypeMap = new LowerCaseAsciiCharSequenceIntHashMap();
    private static final IntHashSet nonPersistedTypes = new IntHashSet();
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();

    private ColumnType() {
    }

    public static boolean defaultStringImplementationIsUtf8() {
        return Chars.equals(nameOf(STRING), "VARCHAR");
    }

    public static ColumnTypeDriver getDriver(int columnType) {
        switch (columnType) {
            case STRING:
                return StringTypeDriver.INSTANCE;
            case BINARY:
                return BinaryTypeDriver.INSTANCE;
            case VARCHAR:
                return VarcharTypeDriver.INSTANCE;
            default:
                throw CairoException.critical(0).put("no driver for type: ").put(columnType);
        }
    }

    public static int getGeoHashBits(int type) {
        return (byte) ((type >> BITS_OFFSET) & 0xFF);
    }

    public static int getGeoHashTypeWithBits(int bits) {
        assert bits > 0 && bits <= GEOLONG_MAX_BITS;
        // this logic relies on GeoHash type value to be clustered together
        return mkGeoHashType(bits, (short) (GEOBYTE + pow2SizeOfBits(bits)));
    }

    public static int getWalDataColumnShl(int columnType, boolean designatedTimestamp) {
        if (columnType == ColumnType.TIMESTAMP && designatedTimestamp) {
            return 4; // 128 bit column
        }
        return pow2SizeOf(columnType);
    }

    public static boolean isAssignableFrom(int fromType, int toType) {
        return isToSameOrWider(fromType, toType) || isNarrowingCast(fromType, toType);
    }

    public static boolean isBinary(int columnType) {
        return columnType == BINARY;
    }

    public static boolean isBoolean(int columnType) {
        return columnType == ColumnType.BOOLEAN;
    }

    public static boolean isBuiltInWideningCast(int fromType, int toType) {
        // This method returns true when a cast is not needed from type to type
        // because of the way typed functions are implemented.
        // For example IntFunction has getDouble() method implemented and does not need
        // additional wrap function to CAST to double.
        // This is usually case for widening conversions.
        return (fromType >= BYTE && toType >= BYTE && toType <= DOUBLE && fromType < toType) || fromType == NULL
                // char can be short and short can be char for symmetry
                || (fromType == CHAR && toType == SHORT)
                // Same with bytes and bools
                || (fromType == BYTE && toType == BOOLEAN)
                || (fromType == TIMESTAMP && toType == LONG);
    }

    public static boolean isChar(int columnType) {
        return columnType == CHAR;
    }

    public static boolean isComparable(int columnType) {
        return columnType != BINARY && columnType != INTERVAL;
    }

    public static boolean isCursor(int columnType) {
        return columnType == CURSOR;
    }

    public static boolean isDesignatedTimestamp(int columnType) {
        return tagOf(columnType) == TIMESTAMP && (columnType & TYPE_FLAG_DESIGNATED_TIMESTAMP) != 0;
    }

    public static boolean isDouble(int columnType) {
        return columnType == DOUBLE;
    }

    public static boolean isFixedSize(int columnType) {
        // specified explicitly
        switch (columnType) {
            case INT:
            case LONG:
            case BOOLEAN:
            case BYTE:
            case TIMESTAMP:
            case DATE:
            case DOUBLE:
            case CHAR:
            case SHORT:
            case FLOAT:
            case LONG128:
            case LONG256:
            case GEOBYTE:
            case GEOSHORT:
            case GEOINT:
            case GEOLONG:
            case UUID:
            case IPv4:
                return true;
            default:
                return false;
        }
    }

    public static boolean isGeoHash(int columnType) {
        return (columnType & TYPE_FLAG_GEO_HASH) != 0;
    }

    public static boolean isInt(int columnType) {
        return columnType == ColumnType.INT;
    }

    public static boolean isInterval(int columnType) {
        return columnType == INTERVAL;
    }

    public static boolean isNull(int columnType) {
        return columnType == NULL;
    }

    public static boolean isPersisted(int columnType) {
        return nonPersistedTypes.excludes(columnType);
    }

    public static boolean isString(int columnType) {
        return columnType == STRING;
    }

    public static boolean isSymbol(int columnType) {
        return columnType == SYMBOL;
    }

    public static boolean isSymbolOrString(int columnType) {
        return columnType == SYMBOL || columnType == STRING || columnType == VARCHAR;
    }

    public static boolean isTimestamp(int columnType) {
        return columnType == TIMESTAMP;
    }

    public static boolean isToSameOrWider(int fromType, int toType) {
        return (tagOf(fromType) == tagOf(toType) && (getGeoHashBits(fromType) == 0 || getGeoHashBits(fromType) >= getGeoHashBits(toType)))
                || isBuiltInWideningCast(fromType, toType)
                || isStringCast(fromType, toType)
                || isVarcharCast(fromType, toType)
                || isGeoHashWideningCast(fromType, toType)
                || isImplicitParsingCast(fromType, toType)
                || isIPv4Cast(fromType, toType);
    }

    public static boolean isUndefined(int columnType) {
        return columnType == UNDEFINED;
    }

    public static boolean isVarSize(int columnType) {
        return columnType == STRING || columnType == BINARY || columnType == VARCHAR;
    }

    public static boolean isVarchar(int columnType) {
        return columnType == VARCHAR;
    }

    public static void makeUtf16DefaultString() {
        if (ALLOW_DEFAULT_STRING_CHANGE) {
            typeNameMap.put(STRING, "STRING");
            nameTypeMap.put("STRING", STRING);
            typeNameMap.put(VARCHAR, "VARCHAR");
            nameTypeMap.put("VARCHAR", VARCHAR);
        }
    }

    public static void makeUtf8DefaultString() {
        if (ALLOW_DEFAULT_STRING_CHANGE) {
            typeNameMap.put(VARCHAR, "STRING");
            nameTypeMap.put("STRING", VARCHAR);
            typeNameMap.put(STRING, "VARCHAR");
            nameTypeMap.put("VARCHAR", STRING);
        }
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
        return OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * fromTag + toTag];
    }

    public static int pow2SizeOf(int columnType) {
        return TYPE_SIZE_POW2[tagOf(columnType)];
    }

    public static int pow2SizeOfBits(int bits) {
        assert bits <= GEOLONG_MAX_BITS;
        return GEO_TYPE_SIZE_POW2[bits];
    }

    public static void resetStringToDefault() {
        makeUtf16DefaultString();
    }

    public static int setDesignatedTimestampBit(int tsType, boolean designated) {
        if (designated) {
            return tsType | TYPE_FLAG_DESIGNATED_TIMESTAMP;
        } else {
            return tsType & ~(TYPE_FLAG_DESIGNATED_TIMESTAMP);
        }
    }

    public static int sizeOf(int columnType) {
        short tag = tagOf(columnType);
        if (tag < TYPE_SIZE.length) {
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

    public static int typeOf(CharSequence name) {
        return nameTypeMap.get(name);
    }

    private static boolean isGeoHashWideningCast(int fromType, int toType) {
        final int toTag = tagOf(toType);
        final int fromTag = tagOf(fromType);
        return (fromTag == GEOLONG && toTag == GEOINT) || (fromTag == GEOLONG && toTag == GEOSHORT) || (fromTag == GEOLONG && toTag == GEOBYTE) || (fromTag == GEOINT && toTag == GEOSHORT) || (fromTag == GEOINT && toTag == GEOBYTE) || (fromTag == GEOSHORT && toTag == GEOBYTE);
    }

    private static boolean isIPv4Cast(int fromType, int toType) {
        return (fromType == STRING || fromType == VARCHAR) && toType == IPv4;
    }

    private static boolean isImplicitParsingCast(int fromType, int toType) {
        final int toTag = tagOf(toType);
        switch (fromType) {
            case CHAR:
                return toTag == GEOBYTE && getGeoHashBits(toType) < 6;
            case STRING:
            case VARCHAR:
                return toTag == GEOBYTE || toTag == GEOSHORT || toTag == GEOINT
                        || toTag == GEOLONG || toTag == TIMESTAMP || toTag == LONG256;
            case SYMBOL:
                return toTag == TIMESTAMP;
            default:
                return false;
        }
    }

    private static boolean isNarrowingCast(int fromType, int toType) {
        return (fromType == DOUBLE && (toType == FLOAT || (toType >= BYTE && toType <= LONG)))
                || (fromType == FLOAT && toType >= BYTE && toType <= LONG)
                || (fromType == LONG && toType >= BYTE && toType <= INT)
                || (fromType == INT && toType >= BYTE && toType <= SHORT)
                || (fromType == SHORT && toType == BYTE)
                || (fromType == CHAR && toType == BYTE)
                || ((fromType == STRING || fromType == VARCHAR) && toType == BYTE)
                || ((fromType == STRING || fromType == VARCHAR) && toType == SHORT)
                || ((fromType == STRING || fromType == VARCHAR) && toType == INT)
                || ((fromType == STRING || fromType == VARCHAR) && toType == LONG)
                || ((fromType == STRING || fromType == VARCHAR) && toType == DATE)
                || ((fromType == STRING || fromType == VARCHAR) && toType == TIMESTAMP)
                || ((fromType == STRING || fromType == VARCHAR) && toType == FLOAT)
                || ((fromType == STRING || fromType == VARCHAR) && toType == DOUBLE)
                || ((fromType == STRING || fromType == VARCHAR) && toType == CHAR)
                || ((fromType == STRING || fromType == VARCHAR) && toType == UUID);
    }

    private static boolean isStringCast(int fromType, int toType) {
        return (fromType == STRING && toType == SYMBOL)
                || (fromType == SYMBOL && toType == STRING)
                || (fromType == CHAR && toType == SYMBOL)
                || (fromType == CHAR && toType == STRING)
                || (fromType == UUID && toType == STRING);
    }

    private static boolean isVarcharCast(int fromType, int toType) {
        return (fromType == STRING && toType == VARCHAR)
                || (fromType == VARCHAR && toType == SYMBOL)
                || (fromType == VARCHAR && toType == STRING)
                || (fromType == SYMBOL && toType == VARCHAR)
                || (fromType == CHAR && toType == VARCHAR)
                || (fromType == UUID && toType == VARCHAR);
    }

    private static int mkGeoHashType(int bits, short baseType) {
        return (baseType & ~(0xFF << BITS_OFFSET)) | (bits << BITS_OFFSET) | TYPE_FLAG_GEO_HASH; // bit 16 is GeoHash flag
    }

    static {
        assert MIGRATION_VERSION >= VERSION;
        // Overload priority is used (indirectly) to route argument type to correct function signature.
        // The argument type keys the array (see comments in the array initialized). This type has to match
        // the numeric value of the type text. Values are then picked in left-to-right order. Signature types
        // on the left are used only if none of signature types on the right exist.
        //
        // All types must be mentioned at all times.
        OVERLOAD_PRIORITY = new short[][]{
                /* 0 UNDEFINED  */  {DOUBLE, FLOAT, STRING, VARCHAR, LONG, TIMESTAMP, DATE, INT, CHAR, SHORT, BYTE, BOOLEAN}
                /* 1  BOOLEAN   */, {BOOLEAN}
                /* 2  BYTE      */, {BYTE, SHORT, INT, LONG, FLOAT, DOUBLE}
                /* 3  SHORT     */, {SHORT, INT, LONG, FLOAT, DOUBLE}
                /* 4  CHAR      */, {CHAR, STRING, VARCHAR}
                /* 5  INT       */, {INT, LONG, FLOAT, DOUBLE, TIMESTAMP, DATE}
                /* 6  LONG      */, {LONG, DOUBLE, TIMESTAMP, DATE}
                /* 7  DATE      */, {DATE, TIMESTAMP, LONG}
                /* 8  TIMESTAMP */, {TIMESTAMP, LONG, DATE}
                /* 9  FLOAT     */, {FLOAT, DOUBLE}
                /* 10 DOUBLE    */, {DOUBLE}
                /* 11 STRING    */, {STRING, VARCHAR, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE, TIMESTAMP, DATE}
                /* 12 SYMBOL    */, {SYMBOL, STRING, VARCHAR, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE, TIMESTAMP, DATE}
                /* 13 LONG256   */, {LONG256}
                /* 14 GEOBYTE   */, {GEOBYTE, GEOSHORT, GEOINT, GEOLONG, GEOHASH}
                /* 15 GEOSHORT  */, {GEOSHORT, GEOINT, GEOLONG, GEOHASH}
                /* 16 GEOINT    */, {GEOINT, GEOLONG, GEOHASH}
                /* 17 GEOLONG   */, {GEOLONG, GEOHASH}
                /* 18 BINARY    */, {BINARY}
                /* 19 UUID      */, {UUID, STRING, VARCHAR}
                /* 20 CURSOR    */, {CURSOR}
                /* 21 unused    */, {}
                /* 22 unused    */, {}
                /* 23 unused    */, {}
                /* 24 LONG128   */, {LONG128}
                /* 25 IPv4      */, {IPv4}
                /* 26 VARCHAR   */, {VARCHAR, STRING, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE, TIMESTAMP, DATE}
                /* 27 unused    */, {}
                /* 28 unused    */, {}
                /* 29 unused    */, {}
                /* 30 unused    */, {}
                /* 31 INTERVAL  */, {INTERVAL, STRING}
                /* 32 NULL      */, {VARCHAR, STRING, DOUBLE, FLOAT, LONG, INT}
        };
        for (short fromTag = UNDEFINED; fromTag < NULL; fromTag++) {
            for (short toTag = BOOLEAN; toTag <= NULL; toTag++) {
                short value = OVERLOAD_NONE;
                short[] priority = OVERLOAD_PRIORITY[fromTag];
                for (short i = 0; i < priority.length; i++) {
                    if (priority[i] == toTag) {
                        value = i;
                        break;
                    }
                }
                OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * fromTag + toTag] = value;
            }
        }
        // When null used as func arg, default to string as function factory arg to avoid weird behaviour
        OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * NULL + STRING] = OVERLOAD_FULL;
        // Do the same for symbol -> avoids weird null behaviour
        OVERLOAD_PRIORITY_MATRIX[OVERLOAD_PRIORITY_N * NULL + SYMBOL] = OVERLOAD_FULL;

        GEO_TYPE_SIZE_POW2 = new int[GEOLONG_MAX_BITS + 1];
        for (int bits = 1; bits <= GEOLONG_MAX_BITS; bits++) {
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
        typeNameMap.put(VARCHAR, "VARCHAR");
        typeNameMap.put(SYMBOL, "SYMBOL");
        typeNameMap.put(BINARY, "BINARY");
        typeNameMap.put(DATE, "DATE");
        typeNameMap.put(PARAMETER, "PARAMETER");
        typeNameMap.put(TIMESTAMP, "TIMESTAMP");
        typeNameMap.put(LONG256, "LONG256");
        typeNameMap.put(UUID, "UUID");
        typeNameMap.put(LONG128, "LONG128");
        typeNameMap.put(CURSOR, "CURSOR");
        typeNameMap.put(RECORD, "RECORD");
        typeNameMap.put(VAR_ARG, "VARARG");
        typeNameMap.put(GEOHASH, "GEOHASH");
        typeNameMap.put(REGCLASS, "regclass");
        typeNameMap.put(REGPROCEDURE, "regprocedure");
        typeNameMap.put(ARRAY_STRING, "text[]");
        typeNameMap.put(IPv4, "IPv4");
        typeNameMap.put(INTERVAL, "INTERVAL");

        nameTypeMap.put("boolean", BOOLEAN);
        nameTypeMap.put("byte", BYTE);
        nameTypeMap.put("double", DOUBLE);
        nameTypeMap.put("float", FLOAT);
        nameTypeMap.put("int", INT);
        nameTypeMap.put("long", LONG);
        nameTypeMap.put("short", SHORT);
        nameTypeMap.put("char", CHAR);
        nameTypeMap.put("string", STRING);
        nameTypeMap.put("varchar", VARCHAR);
        nameTypeMap.put("symbol", SYMBOL);
        nameTypeMap.put("binary", BINARY);
        nameTypeMap.put("date", DATE);
        nameTypeMap.put("parameter", PARAMETER);
        nameTypeMap.put("timestamp", TIMESTAMP);
        nameTypeMap.put("cursor", CURSOR);
        nameTypeMap.put("long256", LONG256);
        nameTypeMap.put("uuid", UUID);
        nameTypeMap.put("long128", LONG128);
        nameTypeMap.put("geohash", GEOHASH);
        nameTypeMap.put("text", STRING);
        nameTypeMap.put("smallint", SHORT);
        nameTypeMap.put("bigint", LONG);
        nameTypeMap.put("real", FLOAT);
        nameTypeMap.put("bytea", STRING);
        nameTypeMap.put("regclass", REGCLASS);
        nameTypeMap.put("regprocedure", REGPROCEDURE);
        nameTypeMap.put("text[]", ARRAY_STRING);
        nameTypeMap.put("IPv4", IPv4);
        nameTypeMap.put("interval", INTERVAL);

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
            typeNameMap.put(type, name);
            nameTypeMap.put(name, type);
        }

        TYPE_SIZE_POW2[UNDEFINED] = -1;
        TYPE_SIZE_POW2[BOOLEAN] = 0;
        TYPE_SIZE_POW2[BYTE] = 0;
        TYPE_SIZE_POW2[SHORT] = 1;
        TYPE_SIZE_POW2[CHAR] = 1;
        TYPE_SIZE_POW2[FLOAT] = 2;
        TYPE_SIZE_POW2[INT] = 2;
        TYPE_SIZE_POW2[IPv4] = 2;
        TYPE_SIZE_POW2[SYMBOL] = 2;
        TYPE_SIZE_POW2[DOUBLE] = 3;
        TYPE_SIZE_POW2[STRING] = -1;
        TYPE_SIZE_POW2[VARCHAR] = -1;
        TYPE_SIZE_POW2[LONG] = 3;
        TYPE_SIZE_POW2[DATE] = 3;
        TYPE_SIZE_POW2[TIMESTAMP] = 3;
        TYPE_SIZE_POW2[LONG256] = 5;
        TYPE_SIZE_POW2[GEOBYTE] = 0;
        TYPE_SIZE_POW2[GEOSHORT] = 1;
        TYPE_SIZE_POW2[GEOINT] = 2;
        TYPE_SIZE_POW2[GEOLONG] = 3;
        TYPE_SIZE_POW2[BINARY] = -1;
        TYPE_SIZE_POW2[PARAMETER] = -1;
        TYPE_SIZE_POW2[CURSOR] = -1;
        TYPE_SIZE_POW2[VAR_ARG] = -1;
        TYPE_SIZE_POW2[RECORD] = -1;
        TYPE_SIZE_POW2[NULL] = -1;
        TYPE_SIZE_POW2[LONG128] = 4;
        TYPE_SIZE_POW2[UUID] = 4;
        TYPE_SIZE_POW2[INTERVAL] = 4;

        TYPE_SIZE[UNDEFINED] = -1;
        TYPE_SIZE[BOOLEAN] = Byte.BYTES;
        TYPE_SIZE[BYTE] = Byte.BYTES;
        TYPE_SIZE[SHORT] = Short.BYTES;
        TYPE_SIZE[CHAR] = Character.BYTES;
        TYPE_SIZE[FLOAT] = Float.BYTES;
        TYPE_SIZE[INT] = Integer.BYTES;
        TYPE_SIZE[IPv4] = Integer.BYTES;
        TYPE_SIZE[SYMBOL] = Integer.BYTES;
        TYPE_SIZE[STRING] = 0;
        TYPE_SIZE[VARCHAR] = 0;
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
        TYPE_SIZE[UUID] = 2 * Long.BYTES;
        TYPE_SIZE[NULL] = 0;
        TYPE_SIZE[LONG128] = 2 * Long.BYTES;
        TYPE_SIZE[INTERVAL] = 2 * Long.BYTES;

        nonPersistedTypes.add(UNDEFINED);
        nonPersistedTypes.add(INTERVAL);
        nonPersistedTypes.add(PARAMETER);
        nonPersistedTypes.add(CURSOR);
        nonPersistedTypes.add(VAR_ARG);
        nonPersistedTypes.add(RECORD);
        nonPersistedTypes.add(NULL);
        nonPersistedTypes.add(REGCLASS);
        nonPersistedTypes.add(REGPROCEDURE);
        nonPersistedTypes.add(ARRAY_STRING);
    }
}
