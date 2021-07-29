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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.GeoHashExtra;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GeoHashFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.GeoHashConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CastGeoHashToGeoHashFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        // Geohashes are of different lenghts
        // and can be casted to lower precision
        // for example cast(cast('questdb' as geohash(6c)) as geohash(5c))
        return "cast(Gg)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function value = args.getQuick(0);
        int srcType = value.getType();
        int targetType = args.getQuick(1).getType();
        int srcBitsPrecision = GeoHashExtra.getBitsPrecision(srcType);
        int targetBitsPrecision = GeoHashExtra.getBitsPrecision(targetType);
        int shift = srcBitsPrecision - targetBitsPrecision;
        if (shift > 0) {
            if (value.isConstant()) {
                long val = value.getLong(null);
                // >> shift will take care of NULL value -1
                return GeoHashConstant.newInstance(val >> shift, targetType);
            }
            return new CastFunc(shift, targetType, value);
        }
        if (srcBitsPrecision == targetBitsPrecision) {
            return value;
        }
        throw SqlException.position(position)
                .put("CAST cannot decrease precision from GEOHASH(")
                .put(srcBitsPrecision)
                .put("b) to GEOHASH(")
                .put(targetBitsPrecision)
                .put("b)");
    }

    private static class CastFunc extends GeoHashFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public long getLong(Record rec) {
            // >> shift will take care of NULL value -1
            return value.getLong(rec) >> shift;
        }
    }
}
