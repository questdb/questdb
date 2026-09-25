/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.GeoHashes;
import io.questdb.griffin.TypeConstant;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public final class Constants {
    private static final ObjList<TypeConstant> doubleArrayTypeConstants = new ObjList<>();
    private static final ObjList<ConstantFunction> geoNullConstants = new ObjList<>();
    private static final ObjList<ConstantFunction> nullDoubleArrayConstants = new ObjList<>();
    private static final IntObjHashMap<TypeConstant> typeConstants = new IntObjHashMap<>(32);

    public static ConstantFunction getGeoHashConstant(long hash, int bits) {
        final int type = ColumnType.getGeoHashTypeWithBits(bits);
        return getGeoHashConstantWithType(hash, type);
    }

    @NotNull
    public static ConstantFunction getGeoHashConstantWithType(long hash, int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE -> new GeoByteConstant((byte) hash, type);
            case ColumnType.GEOSHORT -> new GeoShortConstant((short) hash, type);
            case ColumnType.GEOINT -> new GeoIntConstant((int) hash, type);
            default -> new GeoLongConstant(hash, type);
        };
    }

    /**
     * The NULL constant of a geohash type with {@code bits} bits, cached per bit count.
     */
    public static ConstantFunction getGeoHashNullConstant(int bits) {
        return geoNullConstants.getQuick(bits);
    }

    /**
     * The NULL constant of an array type, cached for up to ten dimensions. The cache holds
     * DOUBLE arrays and is keyed by dimensionality alone, as it always has been.
     */
    public static ConstantFunction getNullArrayConstant(int columnType) {
        final int dims = ColumnType.decodeArrayDimensionality(columnType);
        if (dims <= nullDoubleArrayConstants.size()) {
            return nullDoubleArrayConstants.getQuick(dims - 1);
        }
        return new NullArrayConstant(columnType);
    }

    public static ConstantFunction getNullConstant(int columnType) {
        return switch (ColumnTypeTag.of(columnType)) {
            case BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, STRING, SYMBOL, LONG256,
                 GEOBYTE, GEOSHORT, GEOINT, GEOLONG, BINARY, UUID, LONG128, IPv4, VARCHAR, ARRAY,
                 DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, INTERVAL, UNKNOWN ->
                    ColumnType.getTypeDriver(columnType).getNullConstant(columnType);
            // pseudo tags have no value of their own; a NULL of one of them is the untyped NULL.
            // VARCHAR_SLICE is served by the VARCHAR driver elsewhere, but its NULL has always been
            // the untyped one, and stays so.
            case UNDEFINED, CURSOR, VAR_ARG, RECORD, GEOHASH, DECIMAL, REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER,
                 VARCHAR_SLICE, NULL -> NullConstant.NULL;
        };
    }

    public static TypeConstant getTypeConstant(int columnType) {
        if (ColumnType.isArray(columnType)) {
            if (ColumnType.decodeArrayElementType(columnType) == ColumnType.DOUBLE) {
                // dimension is 1-based, list offset is 0-based
                final int dims = ColumnType.decodeArrayDimensionality(columnType);
                if (dims <= doubleArrayTypeConstants.size()) {
                    return doubleArrayTypeConstants.get(dims - 1);
                }
                return new ArrayTypeConstant(columnType);
            }
            throw new UnsupportedOperationException();
        }
        // GEOHASH takes a different path, no need to extract tag
        return typeConstants.get(columnType);
    }

    static {
        typeConstants.put(ColumnType.INT, IntTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.STRING, StrTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.SYMBOL, SymbolTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.LONG, LongTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.DATE, DateTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.TIMESTAMP_MICRO, TimestampTypeConstant.TIMESTAMP_MS_CONSTANT);
        typeConstants.put(ColumnType.TIMESTAMP_NANO, TimestampTypeConstant.TIMESTAMP_NS_CONSTANT);
        typeConstants.put(ColumnType.BYTE, ByteTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.SHORT, ShortTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.CHAR, CharTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.BOOLEAN, BooleanTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.DOUBLE, DoubleTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.FLOAT, FloatTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.BINARY, BinTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.LONG256, Long256TypeConstant.INSTANCE);
        typeConstants.put(ColumnType.REGCLASS, RegClassTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.REGPROCEDURE, RegProcedureTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.ARRAY_STRING, StringArrayTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.UUID, UuidTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.IPv4, IPv4TypeConstant.INSTANCE);
        typeConstants.put(ColumnType.VARCHAR, VarcharTypeConstant.INSTANCE);
        typeConstants.put(ColumnType.INTERVAL_RAW, IntervalTypeConstant.RAW_INSTANCE);
        typeConstants.put(ColumnType.INTERVAL_TIMESTAMP_MICRO, IntervalTypeConstant.TIMESTAMP_MICRO_INSTANCE);
        typeConstants.put(ColumnType.INTERVAL_TIMESTAMP_NANO, IntervalTypeConstant.TIMESTAMP_NANO_INSTANCE);


        // pre-populate double array types up to 10 dimensions
        for (int i = 0; i < 10; i++) {
            doubleArrayTypeConstants.add(
                    new ArrayTypeConstant(
                            ColumnType.encodeArrayType(ColumnType.DOUBLE, i + 1)
                    )
            );
        }

        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            geoNullConstants.extendAndSet(b, getGeoHashConstant(GeoHashes.NULL, b));
        }

        for (int i = 0; i < 10; i++) {
            nullDoubleArrayConstants.add(
                    new NullArrayConstant(
                            ColumnType.encodeArrayType(ColumnType.DOUBLE, i + 1)
                    )
            );
        }
    }
}
