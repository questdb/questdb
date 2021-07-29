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
import io.questdb.griffin.engine.functions.constants.GeoHashConstant;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.std.*;

public class CastStrToGeoHashFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Sg)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function value = args.getQuick(0);
        int argPosition = argPositions.getQuick(0);
        int targetTypep = args.getQuick(1).getType();
        if (value.isConstant()) {
            try {
                return GeoHashConstant.newInstance(
                        getGeoHashImpl(targetTypep, value.getStr(null), argPosition),
                        targetTypep
                );
            } catch (NumericException e) {
                throw SqlException.position(argPosition).put("invalid GEOHASH symbol");
            }
        }
        return new Func(targetTypep, value, argPosition);
    }

    private static long getGeoHashImpl(int targetTypep, CharSequence value, int position) throws SqlException, NumericException {
        if (value == null || value.length() == 0) {
            return GeoHashExtra.NULL;
        }
        int actualBits = value.length() * 5;
        int typepBits = GeoHashExtra.getBitsPrecision(targetTypep);
        if (actualBits < typepBits) {
            throw SqlException.position(position).put("string is too short to cast to chosen GEOHASH precision");
        }
        return GeoHashNative.fromString(value) >>> (actualBits - typepBits);
    }

    private static class Func extends GeoHashFunction implements UnaryFunction {
        private final Function value;
        private final int position;

        public Func(int targetTypep, Function value, int position) {
            super(targetTypep);
            this.value = value;
            this.position = position;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public long getLong(Record rec) {
            try {
                return getGeoHashImpl(GeoHashExtra.getBitsPrecision(typep), value.getStr(rec), position);
            } catch (SqlException | NumericException e) {
                return GeoHashExtra.NULL;
            }
        }
    }
}
