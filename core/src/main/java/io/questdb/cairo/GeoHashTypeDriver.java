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


package io.questdb.cairo;

import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.GeoByteConstant;
import io.questdb.griffin.engine.functions.constants.GeoIntConstant;
import io.questdb.griffin.engine.functions.constants.GeoLongConstant;
import io.questdb.griffin.engine.functions.constants.GeoShortConstant;
import io.questdb.std.Vect;

/**
 * Type driver for the geohash family: GEOBYTE, GEOSHORT, GEOINT and GEOLONG are one type
 * stored at four widths, so they share one class with one instance per tag. The number of
 * bits is part of the encoded column type and is passed as an argument where a method
 * needs it. NULL is -1 at every width. A NULL constant is typed by the encoded bit count,
 * from the {@link Constants} cache; a bare tag (no bits) yields the tag's untyped NULL constant.
 * <p>
 * Every width travels on PostgreSQL wire as its text. The GEOHASH pseudo tag names the widths
 * in function signatures and in CAST, with its bits (GeoHashTypeConstant), so a width has no
 * signature character and no type constant of its own and is no CAST target.
 */
public final class GeoHashTypeDriver extends FixedSizeTypeDriver {
    public static final GeoHashTypeDriver GEOBYTE = new GeoHashTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.GEOBYTE,
                    PhysicalDescriptor.Movement.W1,
                    PhysicalDescriptor.Arithmetic.I8,
                    PhysicalDescriptor.Accessor.GEOBYTE,
                    NullPolicy.SENTINEL,
                    WireKind.GEOBYTE,
                    RelationKind.GEO,
                    8,
                    new short[]{ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.GEOHASH},
                    PgTypeOids.PG_VARCHAR,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    GeoHashes.NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            columnType -> {
                final int bits = ColumnType.getGeoHashBits(columnType);
                return bits != 0 ? Constants.getGeoHashNullConstant(bits) : GeoByteConstant.NULL;
            },
            (columnIndex, columnType) -> GeoByteColumn.newInstance(columnIndex, columnType),
            (dataMem, auxMem) -> () -> dataMem.putByte(GeoHashes.BYTE_NULL),
            (addr, count) -> Vect.memset(addr, count, GeoHashes.BYTE_NULL)
    );
    public static final GeoHashTypeDriver GEOINT = new GeoHashTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.GEOINT,
                    PhysicalDescriptor.Movement.W4,
                    PhysicalDescriptor.Arithmetic.I32,
                    PhysicalDescriptor.Accessor.GEOINT,
                    NullPolicy.SENTINEL,
                    WireKind.GEOINT,
                    RelationKind.GEO,
                    32,
                    new short[]{ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.GEOHASH},
                    PgTypeOids.PG_VARCHAR,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    GeoHashes.NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            columnType -> {
                final int bits = ColumnType.getGeoHashBits(columnType);
                return bits != 0 ? Constants.getGeoHashNullConstant(bits) : GeoIntConstant.NULL;
            },
            (columnIndex, columnType) -> GeoIntColumn.newInstance(columnIndex, columnType),
            (dataMem, auxMem) -> () -> dataMem.putInt(GeoHashes.INT_NULL),
            (addr, count) -> Vect.setMemoryInt(addr, GeoHashes.INT_NULL, count)
    );
    public static final GeoHashTypeDriver GEOLONG = new GeoHashTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.GEOLONG,
                    PhysicalDescriptor.Movement.W8,
                    PhysicalDescriptor.Arithmetic.I64,
                    PhysicalDescriptor.Accessor.GEOLONG,
                    NullPolicy.SENTINEL,
                    WireKind.GEOLONG,
                    RelationKind.GEO,
                    64,
                    new short[]{ColumnType.GEOLONG, ColumnType.GEOHASH},
                    PgTypeOids.PG_VARCHAR,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    GeoHashes.NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            columnType -> {
                final int bits = ColumnType.getGeoHashBits(columnType);
                return bits != 0 ? Constants.getGeoHashNullConstant(bits) : GeoLongConstant.NULL;
            },
            (columnIndex, columnType) -> GeoLongColumn.newInstance(columnIndex, columnType),
            (dataMem, auxMem) -> () -> dataMem.putLong(GeoHashes.NULL),
            (addr, count) -> Vect.setMemoryLong(addr, GeoHashes.NULL, count)
    );
    public static final GeoHashTypeDriver GEOSHORT = new GeoHashTypeDriver(
            new TypeFacts(
                    ColumnTypeTag.GEOSHORT,
                    PhysicalDescriptor.Movement.W2,
                    PhysicalDescriptor.Arithmetic.I16,
                    PhysicalDescriptor.Accessor.GEOSHORT,
                    NullPolicy.SENTINEL,
                    WireKind.GEOSHORT,
                    RelationKind.GEO,
                    16,
                    new short[]{ColumnType.GEOSHORT, ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.GEOHASH},
                    PgTypeOids.PG_VARCHAR,
                    FunctionFactoryDescriptor.NO_SIGNATURE_CHAR,
                    0,
                    GeoHashes.NULL,
                    CastTarget.NEVER,
                    ColumnType.UNKNOWN_NAME
            ),
            columnType -> {
                final int bits = ColumnType.getGeoHashBits(columnType);
                return bits != 0 ? Constants.getGeoHashNullConstant(bits) : GeoShortConstant.NULL;
            },
            (columnIndex, columnType) -> GeoShortColumn.newInstance(columnIndex, columnType),
            (dataMem, auxMem) -> () -> dataMem.putShort(GeoHashes.SHORT_NULL),
            (addr, count) -> Vect.setMemoryShort(addr, GeoHashes.SHORT_NULL, count)
    );
    // by bit count: GEOHASH(<n>c) for a multiple of 5 bits, GEOHASH(<n>b) otherwise
    private static final String[] NAMES = new String[ColumnType.GEOLONG_MAX_BITS + 1];

    private GeoHashTypeDriver(
            TypeFacts facts,
            NullConstantSource nullConstantSource,
            ColumnFunctionFactory columnFunctionFactory,
            NullAppenderFactory nullAppenderFactory,
            NullFiller nullFiller
    ) {
        super(
                facts,
                (service, index, columnType, position) -> {
                    service.setGeoHash(index, columnType);
                    return columnType;
                },
                nullConstantSource,
                columnType -> null,
                columnFunctionFactory,
                nullAppenderFactory,
                nullFiller
        );
    }

    /**
     * Named by the encoded bit count; a bare tag, which carries no bits, has no name.
     */
    @Override
    public String getName(int columnType) {
        final int bits = ColumnType.getGeoHashBits(columnType);
        if (bits < 1 || bits > ColumnType.GEOLONG_MAX_BITS || columnType != ColumnType.getGeoHashTypeWithBits(bits)) {
            return ColumnType.UNKNOWN_NAME;
        }
        return NAMES[bits];
    }

    static {
        for (int bits = 1; bits <= ColumnType.GEOLONG_MAX_BITS; bits++) {
            NAMES[bits] = bits % 5 != 0 ? "GEOHASH(" + bits + "b)" : "GEOHASH(" + bits / 5 + "c)";
        }
    }
}
