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
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.cairo.TypeDriver;
import io.questdb.griffin.TypeConstant;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public final class Constants {
    private static final ObjList<TypeConstant> doubleArrayTypeConstants = new ObjList<>();
    private static final ObjList<ConstantFunction> geoNullConstants = new ObjList<>();
    private static final ObjList<ConstantFunction> nullDoubleArrayConstants = new ObjList<>();
    // type constants of the pseudo types that CAST accepts; pseudo types have no type driver
    private static final IntObjHashMap<TypeConstant> pseudoTypeConstants = new IntObjHashMap<>();

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
     * The NULL constant of an array type, cached for up to ten dimensions. The cache holds DOUBLE
     * arrays and is keyed by dimensionality alone, so an array of another element type with up to
     * ten dimensions gets the DOUBLE array NULL.
     */
    public static ConstantFunction getNullArrayConstant(int columnType) {
        final int dims = ColumnType.decodeArrayDimensionality(columnType);
        if (dims <= nullDoubleArrayConstants.size()) {
            return nullDoubleArrayConstants.getQuick(dims - 1);
        }
        return new NullArrayConstant(columnType);
    }

    /**
     * The NULL constant of {@code columnType}, from its type driver. A pseudo type and
     * VARCHAR_SLICE get the untyped {@link NullConstant#NULL}; a number that is no tag throws.
     */
    public static ConstantFunction getNullConstant(int columnType) {
        final TypeDriver driver = PhysicalDescriptor.storedTypeDriverOf(columnType);
        if (driver != null) {
            return driver.getNullConstant(columnType);
        }
        // a number that is no tag has no NULL: getTypeDriver() throws for it
        if (ColumnTypeTag.of(columnType) == ColumnTypeTag.UNKNOWN) {
            return ColumnType.getTypeDriver(columnType).getNullConstant(columnType);
        }
        return NullConstant.NULL;
    }

    /**
     * The type constant of an array type, as {@link TypeDriver#getTypeConstant(int)} answers it for
     * arrays: DOUBLE arrays only, cached for up to ten dimensions; any other element type throws.
     */
    public static TypeConstant getArrayTypeConstant(int columnType) {
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

    /**
     * The type constant that CAST uses for {@code columnType}, or null when no type name resolves
     * to it. A real type's driver supplies it; this class holds the constants of the pseudo types
     * REGCLASS, REGPROCEDURE and ARRAY_STRING. GEOHASH and DECIMAL casts resolve elsewhere.
     */
    public static TypeConstant getTypeConstant(int columnType) {
        final TypeDriver driver = ColumnType.findTypeDriver(columnType);
        return driver != null ? driver.getTypeConstant(columnType) : pseudoTypeConstants.get(columnType);
    }

    static {
        pseudoTypeConstants.put(ColumnType.REGCLASS, RegClassTypeConstant.INSTANCE);
        pseudoTypeConstants.put(ColumnType.REGPROCEDURE, RegProcedureTypeConstant.INSTANCE);
        pseudoTypeConstants.put(ColumnType.ARRAY_STRING, StringArrayTypeConstant.INSTANCE);


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
