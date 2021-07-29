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

import static io.questdb.griffin.engine.functions.geohash.GeoHashNative.MAX_BITS_LENGTH;

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
        int geoType = args.getQuick(1).getType();
        if (value.isConstant()) {
            try {
                int bitsPrecision = GeoHashExtra.getBitsPrecision(geoType);
                assert bitsPrecision > 0 && bitsPrecision < MAX_BITS_LENGTH + 1;
                return GeoHashConstant.newInstance(
                        getGeoHashImpl(value.getStr(null), argPosition, bitsPrecision),
                        geoType
                );
            } catch (NumericException e) {
                // throw SqlException if string literal is invalid geohash
                // runtime parsing errors will result in NULL geohash
                throw SqlException.position(argPosition).put("invalid GEOHASH");
            }
        }
        return new Func(geoType, args.getQuick(0), argPosition);
    }

    private static long getGeoHashImpl(CharSequence value, int position, int typeBits) throws SqlException, NumericException {
        if (value == null || value.length() == 0) {
            return GeoHashExtra.NULL;
        }
        int actualBits = value.length() * 5;
        if (actualBits < typeBits) {
            throw SqlException.position(position).put("string is too short to cast to chosen GEOHASH precision");
        }
        // Don't parse full string, it can be over 12 chars and result in overflow
        int parseChars = (typeBits - 1) / 5 + 1;
        long lvalue = GeoHashNative.fromString(value, parseChars);
        return lvalue >>> (parseChars * 5 - typeBits);
    }

    public static class Func extends GeoHashFunction implements UnaryFunction {
        private final Function arg;
        private final int position;
        private final int bitsPrecision;

        public Func(int geoType, Function arg, int position) {
            super(geoType);
            this.arg = arg;
            this.position = position;
            this.bitsPrecision = GeoHashExtra.getBitsPrecision(geoType);
            assert this.bitsPrecision > 0 && this.bitsPrecision < MAX_BITS_LENGTH + 1;
        }

        @Override
        public Function getArg() {
            return arg;
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
            try {
                return getGeoHashImpl(arg.getStr(rec), position, bitsPrecision);
            } catch (SqlException | NumericException e) {
                return GeoHashExtra.NULL;
            }
        }
    }
}
