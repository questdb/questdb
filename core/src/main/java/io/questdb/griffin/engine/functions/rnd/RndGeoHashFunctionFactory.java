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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GeoHashFunction;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndGeoHashFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_geohash(i)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) throws SqlException {
        int bits = args.getQuick(0).getInt(null);
        if (bits < 1 || bits > GeoHashes.MAX_BITS_LENGTH) {
            throw SqlException.$(argPositions.getQuick(0), "precision must be in [1..60] range");
        }
        return new RndFunction(bits);
    }

    private static class RndFunction extends GeoHashFunction implements Function {

        private final int bits;
        private Rnd rnd;

        public RndFunction(int bits) {
            super(ColumnType.geohashWithPrecision(bits));
            this.bits = bits;
        }

        private long getLongRnd() {
            double x = rnd.nextDouble() * 180.0 - 90.0;
            double y = rnd.nextDouble() * 360.0 - 180.0;
            try {
                return GeoHashes.fromCoordinates(x, y, this.bits);
            } catch (NumericException e) {
                // Should never happen
                return GeoHashes.NULL;
            }
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            return (byte) getLongRnd();
        }

        @Override
        public short getGeoHashShort(Record rec) {
            return (short) getLongRnd();
        }

        @Override
        public int getGeoHashInt(Record rec) {
            return (int) getLongRnd();
        }

        @Override
        public long getGeoHashLong(Record rec) {
            return getLongRnd();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }
    }
}
