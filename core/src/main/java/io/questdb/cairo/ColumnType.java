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

import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Chars;
import io.questdb.std.Decimals;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Long256;
import io.questdb.std.LowerCaseAsciiCharSequenceIntHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;

// ColumnType layout - 32bit
//
// | Handling bit | Extra type information | Timestamp Flag | GeoHash Flag | Extra type information | Type discriminant (tag) |
// +--------------+------------------------+----------------+--------------+------------------------+-------------------------+
// |    1 bit     |        13 bits         |     1 bit      |    1 bit     |         8 bits         |         8 bits          |
// +--------------+------------------------+----------------+--------------+------------------------+-------------------------+
//
// Handling bit:
//   Skip column use case:
//       The top bit is set for columns that should be skipped.
//       I.e. `if (columnType < 0) { skip }`.
//   PG Wire Format use case:
//       Reserved for bit-shifting operations as part of `PGOids` to
//       determine if a PG Wire column should be handled as text or binary.
//       Also see `bindSelectColumnFormats` and `bindVariableTypes` in
//       `PGConnectionContext`.

/**
 * Column types as numeric (integer) values
 */
public final class ColumnType {
    public static final int ARRAY_NDIMS_LIMIT = 32; // inclusive
    public static final String[] ARRAY_DIM_SUFFIX = new String[ARRAY_NDIMS_LIMIT + 1];
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
    public static final short ARRAY = VARCHAR + 1;              // = 27
    // Similarly to GeoHash, Decimal is separated in 2 kinds of type:
    //  - Stored ones with the number of bits used in the suffix (selected from the precision
    // through getStorageSize).
    //  - Unstored one that is used as a surrogate to resolve decimal functions.
    //
    // Stored decimal uses the Extra type information to store the precision
    // and scale, giving this layout:
    //        31        30~24     23~16       15~8                7~0
    // +--------------+--------+----------+-----------+-------------------------+
    // | Handling bit | Scale  | Reserved | Precision | Type discriminant (tag) |
    // +--------------+--------+----------+-----------+-------------------------+
    // |    1 bit     | 8 bits |  7 bits  |  8 bits   |         8 bits          |
    // +--------------+--------+----------+-----------+-------------------------+
    public static final short DECIMAL8 = ARRAY + 1;     // = 28;
    public static final short DECIMAL16 = DECIMAL8 + 1;    // = 29;
    public static final short DECIMAL32 = DECIMAL16 + 1;   // = 30;
    public static final short DECIMAL64 = DECIMAL32 + 1;   // = 31;
    public static final short DECIMAL128 = DECIMAL64 + 1;  // = 32;
    public static final short DECIMAL256 = DECIMAL128 + 1; // = 33;
    public static final short DECIMAL = DECIMAL256 + 1;    // = 34;
    // PG specific types to work with 3rd party software
    // with canned catalogue queries:
    // REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER
    public static final short REGCLASS = DECIMAL + 1;            // = 35;
    public static final short REGPROCEDURE = REGCLASS + 1;     // = 36;
    public static final short ARRAY_STRING = REGPROCEDURE + 1; // = 37;
    public static final short PARAMETER = ARRAY_STRING + 1;    // = 38;
    public static final short INTERVAL = PARAMETER + 1;        // = 39;
    public static final short NULL = INTERVAL + 1;          // = 40; ALWAYS the last
    private static final short[] TYPE_SIZE = new short[NULL + 1];
    private static final short[] TYPE_SIZE_POW2 = new short[TYPE_SIZE.length];
    // slightly bigger than needed to make it a power of 2
    private static final short OVERLOAD_PRIORITY_N = (short) Math.pow(2.0, Numbers.msb(NULL) + 1.0);
    private static final int[] OVERLOAD_PRIORITY_MATRIX = new int[OVERLOAD_PRIORITY_N * OVERLOAD_PRIORITY_N]; // NULL to any is 0
    public static final int INTERVAL_RAW = INTERVAL;
    public static final int INTERVAL_TIMESTAMP_MICRO = INTERVAL | 1 << 17;
    public static final int INTERVAL_TIMESTAMP_NANO = INTERVAL | 1 << 18;
    public static final int DECIMAL_DEFAULT_TYPE_TAG = DECIMAL64;
    public static final int DECIMAL_DEFAULT_TYPE = getDecimalType(18, 3);
    public static final int TIMESTAMP_MICRO = TIMESTAMP;
    public static final int TIMESTAMP_NANO = 1 << 18 | TIMESTAMP;
    public static final int VARCHAR_AUX_SHL = 4;
    // column type version as written to the metadata file
    public static final int VERSION = 426;
    static final int[] GEO_TYPE_SIZE_POW2;
    private static final boolean ALLOW_DEFAULT_STRING_CHANGE = false;
    private static final int ARRAY_ELEMTYPE_FIELD_MASK = 0x3F;
    private static final int ARRAY_ELEMTYPE_FIELD_POS = 8;
    private static final int ARRAY_NDIMS_FIELD_MASK = ARRAY_NDIMS_LIMIT - 1;
    private static final int ARRAY_NDIMS_FIELD_POS = 14;
    private static final int BYTE_BITS = 8;
    private static final short[][] OVERLOAD_PRIORITY;
    private static final int TYPE_FLAG_ARRAY_WEAK_DIMS = (1 << 19);
    private static final int TYPE_FLAG_DESIGNATED_TIMESTAMP = (1 << 17);
    private static final int TYPE_FLAG_GEO_HASH = (1 << 16);
    private static final IntHashSet arrayTypeSet = new IntHashSet();
    private static final LowerCaseAsciiCharSequenceIntHashMap nameTypeMap = new LowerCaseAsciiCharSequenceIntHashMap();
    private static final IntHashSet nonPersistedTypes = new IntHashSet();
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();

    private ColumnType() {
    }

    public static int commonWideningType(int typeA, int typeB) {
        return (typeA == typeB && typeA != SYMBOL) ? typeA
                : (isStringyType(typeA) && isStringyType(typeB)) ? STRING
                : (isStringyType(typeA) && isParseableType(typeB)) ? typeA
                : (isStringyType(typeB) && isParseableType(typeA)) ? typeB

                // NULL casts to any other nullable type, except for symbols which can't cross symbol tables.
                : ((typeA == NULL) && isCastableFromNull(typeB) && (typeB != SYMBOL)) ? typeB
                : ((typeB == NULL) && isCastableFromNull(typeA) && (typeA != SYMBOL)) ? typeA

                // cast long and timestamp to timestamp in unions instead of longs.
                : ((isTimestamp(typeA)) && (typeB == LONG)) ? typeA
                : ((typeA == LONG) && (isTimestamp(typeB))) ? typeB
                : (isTimestamp(typeA) && (isTimestamp(typeB))) ? getHigherPrecisionTimestampType(typeA, typeB)

                // cast long and date to date in unions instead of longs.
                : ((typeA == LONG) && (typeB == DATE)) ? DATE
                : ((typeA == DATE) && (typeB == LONG)) ? DATE

                // Varchars take priority over strings, but strings over most types.
                : (typeA == VARCHAR || typeB == VARCHAR) ? VARCHAR
                : ((typeA == STRING) || (typeB == STRING)) ? STRING

                // cast booleans vs anything other than varchars to strings.
                : ((typeA == BOOLEAN) || (typeB == BOOLEAN)) ? STRING

                : (isToSameOrWider(typeB, typeA) && typeA != SYMBOL && typeA != CHAR) ? typeA
                : (isToSameOrWider(typeA, typeB) && typeB != SYMBOL && typeB != CHAR) ? typeB
                : STRING;
    }

    public static int decodeArrayDimensionality(int encodedType) {
        final int dims = ColumnType.decodeWeakArrayDimensionality(encodedType);
        assert dims > 0;
        return dims;
    }

    /**
     * Returns the int constant denoting the type of the elements in an array of the given encoded type.
     */
    public static short decodeArrayElementType(int encodedType) {
        if (ColumnType.isNull(encodedType)) {
            return ColumnType.NULL;
        }
        assert ColumnType.isArray(encodedType) : "typeTag of encodedType is not ARRAY";
        return (short) ((encodedType >> ARRAY_ELEMTYPE_FIELD_POS) & ARRAY_ELEMTYPE_FIELD_MASK);
    }

    /**
     * Returns the number of dimensions for the given array type or -1 in case of an array with weak dimensionality,
     * e.g. array type of bind variable.
     */
    public static int decodeWeakArrayDimensionality(int encodedType) {
        if (ColumnType.isNull(encodedType)) {
            return 0;
        }
        assert ColumnType.isArray(encodedType) : "typeTag of encodedType is not ARRAY";
        if ((encodedType & TYPE_FLAG_ARRAY_WEAK_DIMS) != 0) {
            return -1;
        }
        return ((encodedType >> ARRAY_NDIMS_FIELD_POS) & ARRAY_NDIMS_FIELD_MASK) + 1;
    }

    public static boolean defaultStringImplementationIsUtf8() {
        return Chars.equals(nameOf(STRING), "VARCHAR");
    }

    public static int encodeArrayType(short elemType, int nDims) {
        return encodeArrayType(elemType, nDims, true);
    }

    /**
     * Encodes the array type tag from the element type tag and dimensionality.
     * <br>
     * The encoded type is laid out as follows:
     * <pre>
     *     31~20      19        18~14       13~8           7~0
     * +----------+----------+----------+-----------+------------------+
     * | Reserved | WeakDims |  nDims   | elemType  | ColumnType.ARRAY |
     * +----------+----------+----------+-----------+------------------+
     * |          |  1 bit   |  5 bits  |  6 bits   |      8 bits      |
     * +----------+----------+----------+-----------+------------------+
     * </pre>
     * <p>
     * WeakDims bit (19): When set, indicates the dimensionality is tentative and
     * can be updated based on actual data. This is useful for PostgreSQL wire
     * protocol where type information doesn't include array dimensions.
     *
     * @param elemType one of the supported array element type tags.
     * @param nDims    dimensionality, from 1 to {@value ARRAY_NDIMS_LIMIT}.
     */
    public static int encodeArrayType(int elemType, int nDims, boolean checkSupportedElementTypes) {
        assert nDims >= 1 && nDims <= ARRAY_NDIMS_LIMIT : "nDims out of range: " + nDims;
        assert !checkSupportedElementTypes || (isSupportedArrayElementType(elemType) || elemType == UNDEFINED)
                : "not supported as array element type: " + nameOf(elemType);

        nDims--; // 0 == one dimension
        return (nDims & ARRAY_NDIMS_FIELD_MASK) << ARRAY_NDIMS_FIELD_POS
                | (elemType & ARRAY_ELEMTYPE_FIELD_MASK) << ARRAY_ELEMTYPE_FIELD_POS
                | ARRAY;
    }

    /**
     * Encodes an array type with weak dimensionality. The dimensionality is still
     * encoded but marked as tentative and can be updated based on actual data.
     * This is useful for PostgreSQL wire protocol where type information doesn't
     * include array dimensions.
     * <p>
     * The number of dimensions of this type is undefined, so the decoded number on
     * dimensions for the returned column type will be -1.
     */
    public static int encodeArrayTypeWithWeakDims(short elemType, boolean checkSupportedElementTypes) {
        return encodeArrayType(elemType, 1, checkSupportedElementTypes) | TYPE_FLAG_ARRAY_WEAK_DIMS;
    }

    /**
     * Extracts the precision from a decimal type.
     *
     * @param type is the decimal type to extract the precision from
     * @return the precision as an int
     */
    public static int getDecimalPrecision(int type) {
        return (type >>> 8) & 0xFF;
    }

    /**
     * Extracts the scale from a decimal type.
     *
     * @param type is the decimal type to extract the scale from
     * @return the scale as an int
     */
    public static int getDecimalScale(int type) {
        return (type >>> 18) & 0xFF;
    }

    /**
     * Generate a decimal type from a given precision and scale.
     * It will choose the proper subtype (DECIMAL8, DECIMAL16, etc.) from the precision, depending on the amount
     * of storage needed to store a number with the given precision.
     *
     * @param precision to be encoded in the decimal type
     * @param scale     to be encoded in the decimal type
     * @return the generated type as an int
     */
    public static int getDecimalType(int precision, int scale) {
        assert precision > 0 && precision <= Decimals.MAX_PRECISION;
        assert scale >= 0 && scale <= Decimals.MAX_SCALE;
        int size = Decimals.getStorageSizePow2(precision);
        // Construct the type following the layout described earlier.
        // DECIMAL8-256 needs to be clustered together for this to work.
        return ((scale & 0xFF) << 18) | ((precision & 0xFF) << 8) | (DECIMAL8 + size);
    }

    /**
     * Encode a decimal type from a given tag, precision and scale.
     *
     * @param tag       to be encoded in the decimal type
     * @param precision to be encoded in the decimal type
     * @param scale     to be encoded in the decimal type
     * @return the generated type as an int
     */
    public static int getDecimalType(int tag, int precision, int scale) {
        assert precision > 0 && precision <= Decimals.MAX_PRECISION;
        assert scale >= 0 && scale <= Decimals.MAX_SCALE;
        // Construct the type following the layout described earlier.
        // DECIMAL8-256 needs to be clustered together for this to work.
        return ((scale & 0xFF) << 18) | ((precision & 0xFF) << 8) | tag;
    }

    public static ColumnTypeDriver getDriver(int columnType) {
        return switch (tagOf(columnType)) {
            case STRING -> StringTypeDriver.INSTANCE;
            case BINARY -> BinaryTypeDriver.INSTANCE;
            case VARCHAR -> VarcharTypeDriver.INSTANCE;
            case ARRAY -> ArrayTypeDriver.INSTANCE;
            default -> throw CairoException.critical(0).put("no driver for type: ").put(columnType);
        };
    }

    public static int getGeoHashBits(int type) {
        return (byte) ((type >> BYTE_BITS) & 0xFF);
    }

    public static int getGeoHashTypeWithBits(int bits) {
        assert bits > 0 && bits <= GEOLONG_MAX_BITS;
        // this logic relies on GeoHash type value to be clustered together
        return mkGeoHashType(bits, (short) (GEOBYTE + pow2SizeOfBits(bits)));
    }

    public static int getHigherPrecisionTimestampType(int left, int right) {
        int leftPriority = getTimestampTypePriority(left);
        int rightPriority = getTimestampTypePriority(right);
        // Return the timestamp type with higher precision using explicit priority
        return leftPriority >= rightPriority ? left : right;
    }

    public static TimestampDriver getTimestampDriver(int timestampType) {
        final short tag = tagOf(timestampType);
        // null and UNDEFINED use MicrosTimestamp
        if (tag == NULL || tag == UNDEFINED) {
            return MicrosTimestampDriver.INSTANCE;
        }
        assert tag == TIMESTAMP;

        return switch (timestampType) {
            case TIMESTAMP_MICRO -> MicrosTimestampDriver.INSTANCE;
            case TIMESTAMP_NANO -> NanosTimestampDriver.INSTANCE;
            default -> throw new UnsupportedOperationException();
        };
    }

    /**
     * Determines the implicit conversion rule from the other columnTypes to the Timestamp type.
     * <p>
     * This conversion rule is consistent with the implementation of the
     * {@link io.questdb.cairo.sql.Function#getTimestamp(Record)} of functions.
     * <p>
     * Conversion rules: <ul>
     * <li> TIMESTAMP types: returned as-is to preserve existing precision
     * <li> DATE types: converted to {@link #TIMESTAMP_MICRO}
     * <li> String types (VARCHAR, STRING, SYMBOL): converted to {@link #TIMESTAMP_NANO}
     * for maximum precision when parsing timestamp strings
     * <li> Other types (LONG, INT, etc.): return {@link #UNDEFINED}, the caller should
     * determine the appropriate timestamp type based on context
     * </ul>
     *
     * @param type the input column type to convert
     * @return the appropriate timestamp type for the input column type, or {@link #UNDEFINED}
     * for numeric types where the caller should determine the timestamp type
     */
    public static int getTimestampType(int type) {
        return switch (tagOf(type)) {
            case TIMESTAMP -> type;
            case VARCHAR, STRING, SYMBOL -> TIMESTAMP_NANO;
            case DATE -> TIMESTAMP_MICRO;
            // Long, Int etc.
            default -> UNDEFINED;
        };
    }

    public static int getWalDataColumnShl(int columnType, boolean designatedTimestamp) {
        if (ColumnType.isTimestamp(columnType) && designatedTimestamp) {
            return 4; // 128 bit column
        }
        return pow2SizeOf(columnType);
    }

    /**
     * Is an N-dimensional array type.
     */
    public static boolean isArray(int columnType) {
        return ColumnType.tagOf(columnType) == ColumnType.ARRAY;
    }

    /**
     * Checks if an array type has weak dimensionality, meaning the dimensionality
     * is tentative and can be updated based on actual data.
     */
    public static boolean isArrayWithWeakDims(int columnType) {
        return isArray(columnType) && (columnType & TYPE_FLAG_ARRAY_WEAK_DIMS) != 0;
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
        fromType = tagOf(fromType);
        toType = tagOf(toType);
        return (fromType >= BYTE && toType >= BYTE && toType <= DOUBLE && fromType < toType) || fromType == NULL
                // char can be short and short can be char for symmetry
                || (fromType == CHAR && toType == SHORT)
                // Same with bytes and booleans
                || (fromType == BYTE && toType == BOOLEAN)
                || (fromType == TIMESTAMP && toType == LONG)
                || (fromType == DATE && toType == LONG)
                || (fromType == STRING && (toType >= BYTE && toType <= DOUBLE));
    }

    /**
     * Checks if a type can be cast from NULL to the specified type.
     */
    public static boolean isCastableFromNull(int columnType) {
        return tagOf(columnType) != CHAR;
    }

    public static boolean isChar(int columnType) {
        return columnType == CHAR;
    }

    public static boolean isComparable(int columnType) {
        short typeTag = tagOf(columnType);
        return typeTag != BINARY && typeTag != INTERVAL && typeTag != ARRAY;
    }

    public static boolean isCursor(int columnType) {
        return columnType == CURSOR;
    }

    public static boolean isDecimal(int type) {
        final short tag = tagOf(type);
        return tag >= DECIMAL8 && tag <= DECIMAL;
    }

    public static boolean isDecimalType(int colType) {
        return colType >= DECIMAL8 && colType <= DECIMAL256;
    }

    public static boolean isDesignatedTimestamp(int columnType) {
        return tagOf(columnType) == TIMESTAMP && (columnType & TYPE_FLAG_DESIGNATED_TIMESTAMP) != 0;
    }

    public static boolean isDouble(int columnType) {
        return columnType == DOUBLE;
    }

    public static boolean isFixedSize(int columnType) {
        // specified explicitly
        return switch (columnType) {
            case INT, LONG, BOOLEAN, BYTE, TIMESTAMP_MICRO, TIMESTAMP_NANO, DATE, DOUBLE, CHAR, SHORT, FLOAT, LONG128,
                 LONG256, GEOBYTE, GEOSHORT, GEOINT, GEOLONG, UUID, IPv4, DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64,
                 DECIMAL128, DECIMAL256 -> true;
            default -> false;
        };
    }

    public static boolean isGenericType(int columnType) {
        return isGeoHash(columnType) || isArray(columnType);
    }

    public static boolean isGeoHash(int columnType) {
        return (columnType & TYPE_FLAG_GEO_HASH) != 0;
    }

    public static boolean isGeoType(int colType) {
        return colType >= GEOBYTE && colType <= GEOLONG;
    }

    public static boolean isInt(int columnType) {
        return columnType == ColumnType.INT;
    }

    public static boolean isInterval(int columnType) {
        return tagOf(columnType) == INTERVAL;
    }

    public static boolean isNull(int columnType) {
        return columnType == NULL;
    }

    public static boolean isParseableType(int colType) {
        return isTimestamp(colType) || colType == LONG256;
    }

    public static boolean isPersisted(int columnType) {
        return nonPersistedTypes.excludes(columnType);
    }

    public static boolean isString(int columnType) {
        return columnType == STRING;
    }

    public static boolean isStringyType(int colType) {
        return colType == VARCHAR || colType == STRING;
    }

    public static boolean isSupportedArrayElementType(int typeTag) {
        return arrayTypeSet.contains(typeTag);
    }

    public static boolean isSymbol(int columnType) {
        return columnType == SYMBOL;
    }

    public static boolean isSymbolOrString(int columnType) {
        return columnType == SYMBOL || columnType == STRING;
    }

    public static boolean isSymbolOrStringOrVarchar(int columnType) {
        return columnType == SYMBOL || columnType == STRING || columnType == VARCHAR;
    }

    public static boolean isTimestamp(int columnType) {
        return ColumnType.tagOf(columnType) == TIMESTAMP;
    }

    public static boolean isTimestampMicro(int timestampType) {
        return timestampType == TIMESTAMP_MICRO;
    }

    public static boolean isTimestampNano(int timestampType) {
        return timestampType == TIMESTAMP_NANO;
    }

    public static boolean isToSameOrWider(int fromType, int toType) {
        final short fromTag = tagOf(fromType);
        final short toTag = tagOf(toType);
        return (fromTag == toTag && !isArray(fromType) && (getGeoHashBits(fromType) == 0 || getGeoHashBits(fromType) >= getGeoHashBits(toType)))
                || isBuiltInWideningCast(fromType, toType)
                || isStringCast(fromType, toType)
                || isVarcharCast(fromType, toType)
                || isGeoHashWideningCast(fromType, toType)
                || isImplicitParsingCast(fromType, toType)
                || isIPv4Cast(fromType, toType)
                || isArrayCast(fromType, toType)
                || (isDecimalType(toTag) && isDecimalType(fromTag));
    }

    public static boolean isUndefined(int columnType) {
        return columnType == UNDEFINED || isUndefinedArray(columnType);
    }

    public static boolean isVarSize(int columnType) {
        return columnType == STRING
                || columnType == BINARY
                || columnType == VARCHAR
                || tagOf(columnType) == ARRAY;
    }

    public static boolean isVarchar(int columnType) {
        return columnType == VARCHAR;
    }

    public static boolean isVarcharOrString(int columnType) {
        return columnType == VARCHAR || columnType == STRING;
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
        if (type == -1) {
            return (short) type;
        }
        return (short) (type & 0xFF);
    }

    public static short tagOf(CharSequence name) {
        return tagOf(nameTypeMap.get(name));
    }

    public static int typeOf(CharSequence name) {
        return nameTypeMap.get(name);
    }

    private static void addArrayTypeName(StringSink sink, short type) {
        sink.clear();
        sink.put(nameOf(type));
        for (int d = 1; d <= ARRAY_NDIMS_LIMIT; d++) {
            sink.put("[]");
            int arrayType = encodeArrayType(type, d, false);
            String name = sink.toString();
            typeNameMap.put(arrayType, name);
            nameTypeMap.put(name, arrayType);
        }
    }

    private static int getTimestampTypePriority(int timestampType) {
        assert tagOf(timestampType) == TIMESTAMP || timestampType == UNDEFINED;
        return switch (timestampType) {
            case TIMESTAMP_MICRO -> 1;
            case TIMESTAMP_NANO -> 2;
            default -> 0;
        };

    }

    private static boolean isArrayCast(int fromType, int toType) {
        return isArray(fromType) && isArray(toType)
                && decodeArrayElementType(fromType) == decodeArrayElementType(toType)
                && !isArrayWithWeakDims(fromType) && !isArrayWithWeakDims(toType)
                && decodeWeakArrayDimensionality(fromType) == decodeWeakArrayDimensionality(toType);
    }

    private static boolean isGeoHashWideningCast(int fromType, int toType) {
        final int toTag = tagOf(toType);
        final int fromTag = tagOf(fromType);
        // Deliberate fallthrough in all case branches!
        switch (fromTag) {
            case GEOLONG:
                if (toTag == GEOINT) {
                    return true;
                }
            case GEOINT:
                if (toTag == GEOSHORT) {
                    return true;
                }
            case GEOSHORT:
                if (toTag == GEOBYTE) {
                    return true;
                }
            default:
                return false;
        }
    }

    private static boolean isIPv4Cast(int fromType, int toType) {
        return (fromType == STRING || fromType == VARCHAR) && toType == IPv4;
    }

    private static boolean isImplicitParsingCast(int fromType, int toType) {
        final int toTag = tagOf(toType);
        return switch (fromType) {
            case CHAR -> toTag == GEOBYTE && getGeoHashBits(toType) < 6;
            case STRING, VARCHAR -> switch (toTag) {
                case GEOBYTE, GEOSHORT, GEOINT, GEOLONG, TIMESTAMP, LONG256 -> true;
                default -> false;
            };
            case SYMBOL -> toTag == TIMESTAMP;
            default -> false;
        };
    }

    private static boolean isNarrowingCast(int fromType, int toType) {
        final boolean isTargetDecimal = isDecimal(toType);
        return (fromType == DOUBLE && (toType == FLOAT || (toType >= BYTE && toType <= LONG)))
                || (fromType == FLOAT && ((toType >= BYTE && toType <= LONG) || toType == DATE || isTimestamp(toType)))
                || (fromType == LONG && toType >= BYTE && toType <= INT)
                || (fromType == DATE && toType >= BYTE && toType <= INT)
                || (isTimestamp(fromType) && ((toType >= BYTE && toType <= INT) || toType == DATE))
                || (fromType == INT && toType >= BYTE && toType <= SHORT)
                || (fromType == SHORT && toType == BYTE)
                || (fromType == CHAR && toType == BYTE)
                || (fromType >= BYTE && fromType <= LONG && isTargetDecimal)
                || isStringyType(fromType) && (
                toType == BYTE ||
                        toType == SHORT ||
                        toType == INT ||
                        toType == LONG ||
                        toType == DATE ||
                        toType == TIMESTAMP_MICRO ||
                        toType == TIMESTAMP_NANO ||
                        toType == FLOAT ||
                        toType == DOUBLE ||
                        toType == CHAR ||
                        toType == UUID ||
                        ColumnType.isArray(toType) ||
                        isTargetDecimal);
    }

    private static boolean isStringCast(int fromType, int toType) {
        return (fromType == STRING && toType == SYMBOL)
                || (fromType == SYMBOL && toType == STRING)
                || (fromType == CHAR && toType == SYMBOL)
                || (fromType == CHAR && toType == STRING)
                || (fromType == UUID && toType == STRING);
    }

    // Both arrays with undefined element types and arrays with weak dimensionality are considered undefined.
    private static boolean isUndefinedArray(int columnType) {
        return tagOf(columnType) == ARRAY
                && (decodeArrayElementType(columnType) == UNDEFINED || (columnType & TYPE_FLAG_ARRAY_WEAK_DIMS) != 0);
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
        return (baseType & ~(0xFF << BYTE_BITS)) | (bits << BYTE_BITS) | TYPE_FLAG_GEO_HASH; // bit 16 is GeoHash flag
    }

    static {
        assert MIGRATION_VERSION >= VERSION;
        // Overload priority is used (indirectly) to route argument type to correct function signature.
        // The argument type keys the array (see comments in the array initialized). This type has to match
        // the numeric value of the type text. Values are then picked in left-to-right order. Signature types
        // on the left are used only if none of signature types on the right exist.
        //
        // All types must be mentioned at all times.
        //
        /// Note that the overload rule here must align with the corresponding function implementation, or specific
        /// rules specified by {@link io.questdb.griffin.FunctionParser}, which add explicit cast function(like uuid -> string).
        /// For instance, in {@link io.questdb.griffin.engine.functions.SymbolFunction},
        /// apart from getChar(), getStr(), getTimestamp(), getVarchar(), and getInt(),
        /// all other getxxx methods throw an UnSupportException. Therefore, the Symbol datatype only supports
        /// overloading by STRING, VARCHAR, CHAR, INT, and TIMESTAMP.

        OVERLOAD_PRIORITY = new short[][]{
                /* 0 UNDEFINED   */  {DOUBLE, FLOAT, STRING, VARCHAR, LONG, TIMESTAMP, DATE, INT, CHAR, SHORT, BYTE, BOOLEAN}
                /* 1  BOOLEAN    */, {BOOLEAN}
                /* 2  BYTE       */, {BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL}
                /* 3  SHORT      */, {SHORT, INT, LONG, FLOAT, DOUBLE, CHAR, DECIMAL}
                /* 4  CHAR       */, {CHAR, STRING, VARCHAR, SHORT, INT, LONG, FLOAT, DOUBLE}
                /* 5  INT        */, {INT, LONG, FLOAT, DOUBLE, TIMESTAMP, DATE, DECIMAL}
                /* 6  LONG       */, {LONG, DOUBLE, TIMESTAMP, DATE, DECIMAL}
                /* 7  DATE       */, {DATE, TIMESTAMP, LONG, DOUBLE}
                /* 8  TIMESTAMP  */, {TIMESTAMP, LONG, DATE, DOUBLE}
                /* 9  FLOAT      */, {FLOAT, DOUBLE}
                /* 10 DOUBLE     */, {DOUBLE}
                /* 11 STRING     */, {STRING, VARCHAR, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE, TIMESTAMP, DATE, SYMBOL, IPv4}
                /* 12 SYMBOL     */, {SYMBOL, STRING, VARCHAR, CHAR, INT, TIMESTAMP}
                /* 13 LONG256    */, {LONG256, LONG}
                /* 14 GEOBYTE    */, {GEOBYTE, GEOSHORT, GEOINT, GEOLONG, GEOHASH}
                /* 15 GEOSHORT   */, {GEOSHORT, GEOINT, GEOLONG, GEOHASH}
                /* 16 GEOINT     */, {GEOINT, GEOLONG, GEOHASH}
                /* 17 GEOLONG    */, {GEOLONG, GEOHASH}
                /* 18 BINARY     */, {BINARY}
                /* 19 UUID       */, {UUID, STRING}
                /* 20 CURSOR     */, {CURSOR}
                /* 21 unused     */, {}
                /* 22 unused     */, {}
                /* 23 unused     */, {}
                /* 24 LONG128    */, {LONG128}
                /* 25 IPv4       */, {IPv4, STRING, VARCHAR}
                /* 26 VARCHAR    */, {VARCHAR, STRING, CHAR, DOUBLE, LONG, INT, FLOAT, SHORT, BYTE, TIMESTAMP, DATE, SYMBOL, IPv4}
                /* 27 ARRAY      */, {ARRAY}
                /* 28 DECIMAL8   */, {DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL}
                /* 29 DECIMAL16  */, {DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL}
                /* 30 DECIMAL32  */, {DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL}
                /* 31 DECIMAL64  */, {DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL}
                /* 32 DECIMAL128 */, {DECIMAL128, DECIMAL256, DECIMAL}
                /* 33 DECIMAL256 */, {DECIMAL256, DECIMAL}
                /* 34 DECIMAL    */, {}
                /* 35 unused     */, {}
                /* 36 unused     */, {}
                /* 37 unused     */, {}
                /* 38 unused     */, {}
                /* 39 INTERVAL   */, {INTERVAL, STRING}
                /* 40 NULL       */, {VARCHAR, STRING, DOUBLE, FLOAT, LONG, INT}
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
        typeNameMap.put(ARRAY, "ARRAY");
        typeNameMap.put(SYMBOL, "SYMBOL");
        typeNameMap.put(BINARY, "BINARY");
        typeNameMap.put(DATE, "DATE");
        typeNameMap.put(PARAMETER, "PARAMETER");
        typeNameMap.put(TIMESTAMP_MICRO, "TIMESTAMP");
        typeNameMap.put(TIMESTAMP_NANO, "TIMESTAMP_NS");
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
        typeNameMap.put(INTERVAL_RAW, "INTERVAL");
        typeNameMap.put(INTERVAL_TIMESTAMP_MICRO, "INTERVAL");
        typeNameMap.put(INTERVAL_TIMESTAMP_NANO, "INTERVAL");
        typeNameMap.put(DECIMAL, "DECIMAL");
        typeNameMap.put(NULL, "NULL");

//        arrayTypeSet.add(BOOLEAN);
//        arrayTypeSet.add(BYTE);
//        arrayTypeSet.add(SHORT);
//        arrayTypeSet.add(INT);
//        arrayTypeSet.add(LONG);
//        arrayTypeSet.add(DATE);
//        arrayTypeSet.add(TIMESTAMP);
//        arrayTypeSet.add(FLOAT);
        arrayTypeSet.add(DOUBLE);
//        arrayTypeSet.add(LONG256);
//        arrayTypeSet.add(UUID);
//        arrayTypeSet.add(IPv4);

        nameTypeMap.put("boolean", BOOLEAN);
        nameTypeMap.put("byte", BYTE);
        nameTypeMap.put("double", DOUBLE);
        nameTypeMap.put("float", FLOAT);
        nameTypeMap.put("int", INT);
        nameTypeMap.put("integer", INT);
        nameTypeMap.put("long", LONG);
        nameTypeMap.put("short", SHORT);
        nameTypeMap.put("char", CHAR);
        nameTypeMap.put("string", STRING);
        nameTypeMap.put("varchar", VARCHAR);
        nameTypeMap.put("array", ARRAY);
        nameTypeMap.put("symbol", SYMBOL);
        nameTypeMap.put("binary", BINARY);
        nameTypeMap.put("date", DATE);
        nameTypeMap.put("parameter", PARAMETER);
        nameTypeMap.put("timestamp", TIMESTAMP_MICRO);
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
        nameTypeMap.put("interval", INTERVAL_TIMESTAMP_MICRO);
        nameTypeMap.put("timestamp_ns", TIMESTAMP_NANO);
        nameTypeMap.put("decimal", DECIMAL);

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
        TYPE_SIZE_POW2[ARRAY] = -1;
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
        TYPE_SIZE_POW2[DECIMAL8] = 0;
        TYPE_SIZE_POW2[DECIMAL16] = 1;
        TYPE_SIZE_POW2[DECIMAL32] = 2;
        TYPE_SIZE_POW2[DECIMAL64] = 3;
        TYPE_SIZE_POW2[DECIMAL128] = 4;
        TYPE_SIZE_POW2[DECIMAL256] = 5;
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
        TYPE_SIZE[ARRAY] = 0;
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
        TYPE_SIZE[DECIMAL8] = Byte.BYTES;
        TYPE_SIZE[DECIMAL16] = Short.BYTES;
        TYPE_SIZE[DECIMAL32] = Integer.BYTES;
        TYPE_SIZE[DECIMAL64] = Long.BYTES;
        TYPE_SIZE[DECIMAL128] = 2 * Long.BYTES;
        TYPE_SIZE[DECIMAL256] = 4 * Long.BYTES;
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

        addArrayTypeName(sink, ColumnType.BOOLEAN);
        addArrayTypeName(sink, ColumnType.BYTE);
        addArrayTypeName(sink, ColumnType.SHORT);
        addArrayTypeName(sink, ColumnType.INT);
        addArrayTypeName(sink, ColumnType.LONG);
        addArrayTypeName(sink, ColumnType.FLOAT);
        addArrayTypeName(sink, ColumnType.DOUBLE);
        addArrayTypeName(sink, ColumnType.LONG256);
        addArrayTypeName(sink, ColumnType.VARCHAR);
        addArrayTypeName(sink, ColumnType.STRING);
        addArrayTypeName(sink, ColumnType.IPv4);
        addArrayTypeName(sink, ColumnType.TIMESTAMP);
        addArrayTypeName(sink, ColumnType.UUID);
        addArrayTypeName(sink, ColumnType.DATE);

        sink.clear();
        for (int i = 0, n = ARRAY_NDIMS_LIMIT + 1; i < n; i++) {
            ARRAY_DIM_SUFFIX[i] = sink.toString();
            sink.put("[]");
        }

        // Stored decimals
        for (int precision = 1; precision <= Decimals.MAX_PRECISION; precision++) {
            for (int scale = 0; scale <= Decimals.MAX_SCALE; scale++) {
                int type = getDecimalType(precision, scale);
                sink.clear();
                sink.put("DECIMAL(").put(precision).put(',').put(scale).put(")");
                String name = sink.toString();
                typeNameMap.put(type, name);
                nameTypeMap.put(name, type);
            }
        }
    }
}
