/*******************************************************************************
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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.cairo.ColumnType.GEOLONG_MAX_BITS;

public class CastVarcharToGeoHashFunctionFactory implements FunctionFactory {

    public static Function newInstance(int argPosition, int geoType, Function value) throws SqlException {
        if (value.isConstant()) {
            final int bits = ColumnType.getGeoHashBits(geoType);
            assert bits > 0 && bits < GEOLONG_MAX_BITS + 1;
            return Constants.getGeoHashConstantWithType(
                    parseGeoHash(value.getVarcharA(null), argPosition, bits),
                    geoType
            );
        }
        return new Func(geoType, value, argPosition);
    }

    @Override
    public String getSignature() {
        return "cast(Øg)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function value = args.getQuick(0);
        int argPosition = argPositions.getQuick(0);
        int geoType = args.getQuick(1).getType();
        if (value.isConstant()) {
            final int bits = ColumnType.getGeoHashBits(geoType);
            assert bits > 0 && bits < GEOLONG_MAX_BITS + 1;
            return Constants.getGeoHashConstantWithType(
                    parseGeoHash(value.getVarcharA(null), argPosition, bits),
                    geoType
            );
        }
        return new Func(geoType, value, argPosition);
    }

    private static long parseGeoHash(Utf8Sequence value, int position, int typeBits) throws SqlException {
        if (value == null || value.size() == 0) {
            return GeoHashes.NULL;
        }
        return CastStrToGeoHashFunctionFactory.parseGeoHash(value.asAsciiCharSequence(), position, typeBits);
    }

    private static class Func extends AbstractCastToGeoHashFunction {

        public Func(int geoType, Function arg, int position) {
            super(geoType, arg, position);
        }

        @Override
        protected long getGeoHashLong0(Record rec) {
            try {
                return parseGeoHash(arg.getVarcharA(rec), position, bitsPrecision);
            } catch (SqlException e) {
                return GeoHashes.NULL;
            }
        }
    }
}
