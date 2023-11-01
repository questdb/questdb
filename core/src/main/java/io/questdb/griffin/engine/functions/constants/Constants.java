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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.griffin.TypeConstant;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public final class Constants {
    private static final ObjList<ConstantFunction> geoNullConstants = new ObjList<>();
    private static final ObjList<ConstantFunction> nullConstants = new ObjList<>();
    private static final ObjList<TypeConstant> typeConstants = new ObjList<>();

    public static ConstantFunction getGeoHashConstant(long hash, int bits) {
        final int type = ColumnType.getGeoHashTypeWithBits(bits);
        return getGeoHashConstantWithType(hash, type);
    }

    @NotNull
    public static ConstantFunction getGeoHashConstantWithType(long hash, int type) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE:
                return new GeoByteConstant((byte) hash, type);
            case ColumnType.GEOSHORT:
                return new GeoShortConstant((short) hash, type);
            case ColumnType.GEOINT:
                return new GeoIntConstant((int) hash, type);
            default:
                return new GeoLongConstant(hash, type);
        }
    }

    public static ConstantFunction getNullConstant(int columnType) {
        int typeTag = ColumnType.tagOf(columnType);
        if (typeTag >= ColumnType.GEOBYTE &&
                typeTag <= ColumnType.GEOLONG) {
            int bits = ColumnType.getGeoHashBits(columnType);
            if (bits != 0) {
                return geoNullConstants.get(bits);
            }
        }
        return nullConstants.getQuick(typeTag);
    }

    public static TypeConstant getTypeConstant(int columnType) {
        // GEOHASH takes a different path, no need to extract tag
        return typeConstants.getQuick(columnType);
    }

    static {
        nullConstants.extendAndSet(ColumnType.INT, IntConstant.NULL);
        nullConstants.extendAndSet(ColumnType.STRING, StrConstant.NULL);
        nullConstants.extendAndSet(ColumnType.SYMBOL, SymbolConstant.NULL);
        nullConstants.extendAndSet(ColumnType.LONG, LongConstant.NULL);
        nullConstants.extendAndSet(ColumnType.DATE, DateConstant.NULL);
        nullConstants.extendAndSet(ColumnType.TIMESTAMP, TimestampConstant.NULL);
        nullConstants.extendAndSet(ColumnType.BYTE, ByteConstant.ZERO);
        nullConstants.extendAndSet(ColumnType.SHORT, ShortConstant.ZERO);
        nullConstants.extendAndSet(ColumnType.CHAR, CharConstant.ZERO);
        nullConstants.extendAndSet(ColumnType.BOOLEAN, BooleanConstant.FALSE);
        nullConstants.extendAndSet(ColumnType.DOUBLE, DoubleConstant.NULL);
        nullConstants.extendAndSet(ColumnType.FLOAT, FloatConstant.NULL);
        nullConstants.extendAndSet(ColumnType.BINARY, NullBinConstant.INSTANCE);
        nullConstants.extendAndSet(ColumnType.LONG256, Long256NullConstant.INSTANCE);
        nullConstants.extendAndSet(ColumnType.GEOBYTE, GeoByteConstant.NULL);
        nullConstants.extendAndSet(ColumnType.GEOSHORT, GeoShortConstant.NULL);
        nullConstants.extendAndSet(ColumnType.GEOINT, GeoIntConstant.NULL);
        nullConstants.extendAndSet(ColumnType.LONG128, Long128Constant.NULL);
        nullConstants.extendAndSet(ColumnType.GEOLONG, GeoLongConstant.NULL);
        nullConstants.extendAndSet(ColumnType.UUID, UuidConstant.NULL);
        nullConstants.extendAndSet(ColumnType.IPv4, IPv4Constant.NULL);

        typeConstants.extendAndSet(ColumnType.INT, IntTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.STRING, StrTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.SYMBOL, SymbolTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.LONG, LongTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.DATE, DateTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.TIMESTAMP, TimestampTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.BYTE, ByteTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.SHORT, ShortTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.CHAR, CharTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.BOOLEAN, BooleanTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.DOUBLE, DoubleTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.FLOAT, FloatTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.BINARY, BinTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.LONG256, Long256TypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.REGCLASS, RegClassTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.REGPROCEDURE, RegProcedureTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.ARRAY_STRING, StringArrayTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.UUID, UuidTypeConstant.INSTANCE);
        typeConstants.extendAndSet(ColumnType.IPv4, IPv4TypeConstant.INSTANCE);

        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            geoNullConstants.extendAndSet(b, getGeoHashConstant(GeoHashes.NULL, b));
        }
    }
}
