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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GeoByteFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

import static io.questdb.cairo.ColumnType.GEOLONG_MAX_BITS;

public class CastStrToGeoHashFunctionFactory implements FunctionFactory {
    public static Function newInstance(int argPosition, int geoType, Function value) throws SqlException {
        if (value.isConstant()) {
            final int bits = ColumnType.getGeoHashBits(geoType);
            assert bits > 0 && bits < GEOLONG_MAX_BITS + 1;
            return Constants.getGeoHashConstantWithType(
                    getGeoHashImpl(value.getStr(null), argPosition, bits),
                    geoType
            );
        }
        return new Func(geoType, value, argPosition);
    }

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
        return newInstance(argPosition, geoType, value);
    }

    private static long getGeoHashImpl(CharSequence value, int position, int typeBits) throws SqlException {
        if (value == null || value.length() == 0) {
            return GeoHashes.NULL;
        }
        int actualBits = value.length() * 5;
        if (actualBits < typeBits) {
            throw SqlException.position(position)
                    .put("string is too short to cast to chosen GEOHASH precision [len=").put(value.length())
                    .put(", precision=").put(typeBits).put(']');
        }
        // Don't parse full string, it can be over 12 chars and result in overflow
        int parseChars = (typeBits - 1) / 5 + 1;
        try {
            long lvalue = GeoHashes.fromString(value, 0, parseChars);
            return lvalue >>> (parseChars * 5 - typeBits);
        } catch (NumericException e) {
            // throw SqlException if string literal is invalid geohash
            // runtime parsing errors will result in NULL geohash
            throw SqlException.position(position).put("invalid GEOHASH");
        }
    }

    public static class Func extends GeoByteFunction implements UnaryFunction {
        private final Function arg;
        private final int bitsPrecision;
        private final int position;

        public Func(int geoType, Function arg, int position) {
            super(geoType);
            this.arg = arg;
            this.position = position;
            this.bitsPrecision = ColumnType.getGeoHashBits(geoType);
            assert this.bitsPrecision > 0 && this.bitsPrecision < GEOLONG_MAX_BITS + 1;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public byte getGeoByte(Record rec) {
            assert bitsPrecision < 8;
            return (byte) getGeoHashLong0(rec);
        }

        @Override
        public int getGeoInt(Record rec) {
            assert bitsPrecision >= 16 && bitsPrecision < 32;
            return (int) getGeoHashLong0(rec);
        }

        @Override
        public long getGeoLong(Record rec) {
            assert bitsPrecision >= 32;
            return getGeoHashLong0(rec);
        }

        @Override
        public short getGeoShort(Record rec) {
            assert bitsPrecision >= 8 && bitsPrecision < 16;
            return (short) getGeoHashLong0(rec);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val("::geohash");
        }

        private long getGeoHashLong0(Record rec) {
            try {
                return getGeoHashImpl(arg.getStr(rec), position, bitsPrecision);
            } catch (SqlException e) {
                return GeoHashes.NULL;
            }
        }
    }
}
