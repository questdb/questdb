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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.AbstractGeoHashFunction;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class GeoHashFromCoordinatesFunctionFactory implements FunctionFactory {

    private static final String SYMBOL = "make_geohash";
    private static final String SIGNATURE = SYMBOL + "(DDi)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {

        final Function bitsArg = args.get(2);
        int bits = bitsArg.getInt(null);
        if (bits < 1 || bits > ColumnType.GEOLONG_MAX_BITS) {
            throw SqlException.$(argPositions.getQuick(2), "precision must be in [1..60] range");
        }

        int type = ColumnType.getGeoHashTypeWithBits(bits);

        final Function lonArg = args.get(0);
        final Function latArg = args.get(1);
        final boolean isLonConst = lonArg.isConstant();
        final boolean isLatConst = latArg.isConstant();

        if (isLonConst && isLatConst) {

            double lon = lonArg.getDouble(null);
            if (lon < -180.0 || lon > 180.0) {
                throw SqlException.$(argPositions.getQuick(0), "longitude must be in [-180.0..180.0] range");
            }

            double lat = latArg.getDouble(null);
            if (lat < -90.0 || lat > 90.0) {
                throw SqlException.$(argPositions.getQuick(1), "latitude must be in [-90.0..90.0] range");
            }

            return Constants.getGeoHashConstantWithType(GeoHashes.fromCoordinatesDegUnsafe(lat, lon, bits), type);
        } else {
            return new FromCoordinatesFixedBitsFunction(lonArg, latArg, bits);
        }
    }

    private static class FromCoordinatesFixedBitsFunction extends AbstractGeoHashFunction implements BinaryFunction {
        private final int bits;
        private final Function lat;
        private final Function lon;

        public FromCoordinatesFixedBitsFunction(Function lon, Function lat, int bits) {
            super(ColumnType.getGeoHashTypeWithBits(bits));
            this.lon = lon;
            this.lat = lat;
            this.bits = bits;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) getLongValue(rec);
        }

        @Override
        public int getGeoInt(Record rec) {
            return (int) getLongValue(rec);
        }

        @Override
        public long getGeoLong(Record rec) {
            return getLongValue(rec);
        }

        @Override
        public short getGeoShort(Record rec) {
            return (short) getLongValue(rec);
        }

        @Override
        public Function getLeft() {
            return lat;
        }

        @Override
        public Function getRight() {
            return lon;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SYMBOL).val('(').val(lon).val(',').val(lat).val(',').val(bits).val(')');
        }

        private long getLongValue(Record rec) {
            try {
                double lon = this.lon.getDouble(rec);
                double lat = this.lat.getDouble(rec);
                return GeoHashes.fromCoordinatesDeg(lat, lon, bits);
            } catch (NumericException e) {
                return GeoHashes.NULL;
            }
        }

    }
}
