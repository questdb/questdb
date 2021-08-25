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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GeoHashFunction;
import io.questdb.griffin.engine.functions.constants.GeoHashConstant;
import io.questdb.griffin.engine.functions.rnd.RndGeoHashFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class GeoHashFromCoordinatesFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "make_geohash(DDi)";
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
        if (bits < 1 || bits > GeoHashes.MAX_BITS_LENGTH) {
            throw SqlException.$(argPositions.getQuick(2), "precision must be in [1..60] range");
        }

        int type = ColumnType.geohashWithPrecision(bits);

        final Function lonArg = args.get(0);
        final Function latArg = args.get(1);

        final boolean isLonConst = lonArg.isConstant() || lonArg.isRuntimeConstant();
        final boolean isLatConst = latArg.isConstant() || latArg.isRuntimeConstant();

        if (isLonConst && isLatConst) {

            double lon = lonArg.getDouble(null);
            if (lon < -180.0 || lon > 180.0) {
                throw SqlException.$(argPositions.getQuick(0), "longitude must be in [-180.0..180.0] range");
            }

            double lat = latArg.getDouble(null);
            if (lat < -90.0 || lat > 90.0) {
                throw SqlException.$(argPositions.getQuick(1), "latitude must be in [-90.0..90.0] range");
            }

            try {
                return GeoHashConstant.newInstance(GeoHashes.fromCoordinates(lat, lon, bits), type);
            } catch (NumericException e) {
                return GeoHashConstant.newInstance(GeoHashes.NULL, type);
            }
        } else {
            return new FromCoordinatesFixedBitsFunction(lonArg, latArg, bits);
        }
    }

    private static class FromCoordinatesFixedBitsFunction extends GeoHashFunction implements Function {
        private final Function lon;
        private final Function lat;
        private final int bits;

        public FromCoordinatesFixedBitsFunction(Function lon, Function lat, int bits) {
            super(ColumnType.geohashWithPrecision(bits));
            this.lon = lon;
            this.lat = lat;
            this.bits = bits;
        }

        private long getLongValue(Record rec) {
            try {
                double lon = this.lon.getDouble(rec);
                double lat = this.lat.getDouble(rec);
                return GeoHashes.fromCoordinates(lat, lon, bits);
            } catch (NumericException e) {
                return GeoHashes.NULL;
            }
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            return (byte) getLongValue(rec);
        }

        @Override
        public short getGeoHashShort(Record rec) {
            return (short) getLongValue(rec);
        }

        @Override
        public int getGeoHashInt(Record rec) {
            return (int) getLongValue(rec);
        }

        @Override
        public long getGeoHashLong(Record rec) {
            return getLongValue(rec);
        }
    }
}
