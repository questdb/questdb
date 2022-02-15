/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.*;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CastGeoHashToGeoHashFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        // GeoHashes are of different lengths
        // and can be cast to lower precision
        // for example cast(cast('questdb' as geohash(6c)) as geohash(5c))
        return "cast(Gg)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function value = args.getQuick(0);
        int srcType = value.getType();
        int targetType = args.getQuick(1).getType();
        return newInstance(position, value, srcType, targetType);
    }

    public static Function newInstance(int position, Function value, int srcType, int targetType) throws SqlException {
        int srcBitsPrecision = ColumnType.getGeoHashBits(srcType);
        int targetBitsPrecision = ColumnType.getGeoHashBits(targetType);
        int shift = srcBitsPrecision - targetBitsPrecision;
        if (shift > 0) {
            if (value.isConstant()) {
                long val = GeoHashes.getGeoLong(srcType, value, null);
                // >> shift will take care of NULL value -1
                return Constants.getGeoHashConstantWithType(val >> shift, targetType);
            }

            final Function result = castFunc(shift, targetType, value, srcType);
            if (result != null) {
                return result;
            }
        } else if (shift == 0) {
            return value;
        }

        // check if this is a null of different bit count
        if (value.isConstant() && GeoHashes.getGeoLong(value.getType(), value, null) == GeoHashes.NULL) {
            return Constants.getNullConstant(targetType);
        }

        throw SqlException.position(position)
                .put("CAST cannot decrease precision from GEOHASH(")
                .put(srcBitsPrecision)
                .put("b) to GEOHASH(")
                .put(targetBitsPrecision)
                .put("b)");
    }

    private static Function castFunc(int shift, int targetType, Function value, int srcType) {
        switch (ColumnType.tagOf(srcType)) {
            case ColumnType.GEOBYTE:
                return new CastByteFunc(shift, targetType, value);
            case ColumnType.GEOSHORT:
                switch (ColumnType.tagOf(targetType)) {
                    case ColumnType.GEOBYTE:
                        return new CastShortToByteFunc(shift, targetType, value);
                    case ColumnType.GEOSHORT:
                        return new CastShortFunc(shift, targetType, value);
                }
                break;
            case ColumnType.GEOINT:
                switch (ColumnType.tagOf(targetType)) {
                    case ColumnType.GEOBYTE:
                        return new CastIntToByteFunc(shift, targetType, value);
                    case ColumnType.GEOSHORT:
                        return new CastIntToShortFunc(shift, targetType, value);
                    case ColumnType.GEOINT:
                        return new CastIntFunc(shift, targetType, value);
                }
                break;
            default:
                switch (ColumnType.tagOf(targetType)) {
                    case ColumnType.GEOBYTE:
                        return new CastLongToByteFunc(shift, targetType, value);
                    case ColumnType.GEOSHORT:
                        return new CastLongToShortFunc(shift, targetType, value);
                    case ColumnType.GEOINT:
                        return new CastLongToIntFunc(shift, targetType, value);
                    case ColumnType.GEOLONG:
                        return new CastLongFunc(shift, targetType, value);
                }
        }
        return null;
    }

    private static class CastLongFunc extends GeoLongFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastLongFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public long getGeoLong(Record rec) {
            return value.getGeoLong(rec) >> shift;
        }
    }

    private static class CastLongToByteFunc extends GeoByteFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastLongToByteFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) (value.getGeoLong(rec) >> shift);
        }
    }

    private static class CastLongToShortFunc extends GeoShortFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastLongToShortFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getGeoShort(Record rec) {
            return (short) (value.getGeoLong(rec) >> shift);
        }
    }

    private static class CastLongToIntFunc extends GeoIntFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastLongToIntFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public int getGeoInt(Record rec) {
            return (int) (value.getGeoLong(rec) >> shift);
        }
    }

    private static class CastIntFunc extends GeoIntFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastIntFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }


        @Override
        public int getGeoInt(Record rec) {
            return value.getGeoInt(rec) >> shift;
        }
    }

    private static class CastIntToByteFunc extends GeoByteFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastIntToByteFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) (value.getGeoInt(rec) >> shift);
        }
    }

    private static class CastIntToShortFunc extends GeoShortFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastIntToShortFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getGeoShort(Record rec) {
            return (short) (value.getGeoInt(rec) >> shift);
        }
    }

    private static class CastShortFunc extends GeoShortFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastShortFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getGeoShort(Record rec) {
            return (short) (value.getGeoShort(rec) >> shift);
        }
    }

    private static class CastShortToByteFunc extends GeoByteFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastShortToByteFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) (value.getGeoShort(rec) >> shift);
        }
    }

    private static class CastByteFunc extends GeoByteFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastByteFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) (value.getGeoByte(rec) >> shift);
        }
    }
}
