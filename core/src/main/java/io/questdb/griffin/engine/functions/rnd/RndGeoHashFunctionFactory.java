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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GeoHashFunction;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndGeoHashFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_geohash(i)";
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int bits = args.getQuick(0).getInt(null);
        if (bits < 1 || bits > 60) {
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

        @Override
        public byte getByte(Record rec) {
            return (byte) getLong(rec);
        }

        @Override
        public short getShort(Record rec) {
            return (short) getLong(rec);
        }

        @Override
        public int getInt(Record rec) {
            return (int) getLong(rec);
        }

        @Override
        public long getLong(Record rec) {
            double x = rnd.nextDouble() * 180.0 - 90.0;
            if (x > 90.0) {
                x = Double.longBitsToDouble(Double.doubleToLongBits(90) - 1);
            }
            double y = rnd.nextDouble() * 360.0 - 180.0;
            if (y > 180.0) {
                y = Double.longBitsToDouble(Double.doubleToLongBits(180) - 1);
            }
            assert x >= -90.0 && x <= 90.0;
            assert y >= -180.0 && y <= 180.0;
            return GeoHashNative.fromCoordinates(x, y, this.bits);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }
    }
}
