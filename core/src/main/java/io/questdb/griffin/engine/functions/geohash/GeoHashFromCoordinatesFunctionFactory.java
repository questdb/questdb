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
        int bits = args.getQuick(2).getInt(null);
        if (bits < 1 || bits > GeoHashes.MAX_BITS_LENGTH) {
            throw SqlException.$(argPositions.getQuick(0), "precision must be in [1..60] range");
        }
        return new FromCoordinatesFunction(args.get(0), args.get(1), bits);
    }

    private static class FromCoordinatesFunction extends GeoHashFunction implements Function {
        private final Function lon;
        private final Function lat;
        private final int bits;

        public FromCoordinatesFunction(Function lon, Function lat, int bits) {
            // Because of this, I cannot postpone the value fetching from the argument.
            // And bits should always be literal value.
            // So the case with bits from a column was discarded.
            super(ColumnType.geohashWithPrecision(bits));
            this.lon = lon;
            this.lat = lat;
            this.bits = bits;
        }

        private long getLongValue(Record rec) {
            try {
                double lon = this.lon.getDouble(rec);
                double lat = this.lat.getDouble(rec);
                return GeoHashes.fromCoordinates(lat, lon, this.bits);
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
