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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GeoHashFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.GeoHashConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import static io.questdb.cairo.GeoHashes.getGeoLong;
import static io.questdb.cairo.GeoHashes.sizeOf;

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
        int srcBitsPrecision = GeoHashes.getBitsPrecision(srcType);
        int targetBitsPrecision = GeoHashes.getBitsPrecision(targetType);
        int shift = srcBitsPrecision - targetBitsPrecision;
        if (shift > 0) {
            if (value.isConstant()) {
                long val = getGeoLong(srcType, value, null);
                // >> shift will take care of NULL value -1
                return GeoHashConstant.newInstance(val >> shift, targetType);
            }
            return castFunc(shift, targetType, value, srcType);
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

    private Function castFunc(int shift, int targetType, Function value, int srcType) {
        switch (sizeOf(srcType)) {
            default:
                return new CastLongFunc(shift, targetType, value);
            case Byte.BYTES:
                return new CastByteFunc(shift, targetType, value);
            case Short.BYTES:
                return new CastShortFunc(shift, targetType, value);
            case Integer.BYTES:
                return new CastIntFunc(shift, targetType, value);
        }
    }

    private static class CastLongFunc extends GeoHashFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastLongFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            assert ColumnType.sizeOf(getType()) == 1;
            return (byte) (value.getGeoHashLong(rec) >> shift);
        }

        @Override
        public short getGeoHashShort(Record rec) {
            assert ColumnType.sizeOf(getType()) == 2;
            return (short) (value.getGeoHashLong(rec) >> shift);
        }

        @Override
        public int getGeoHashInt(Record rec) {
            assert ColumnType.sizeOf(getType()) == 4;
            return (int) (value.getGeoHashLong(rec) >> shift);
        }

        @Override
        public long getGeoHashLong(Record rec) {
            assert ColumnType.sizeOf(getType()) == 8;
            return value.getGeoHashLong(rec) >> shift;
        }
    }

    private static class CastIntFunc extends GeoHashFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastIntFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            assert ColumnType.sizeOf(getType()) == 1;
            return (byte) (value.getGeoHashInt(rec) >> shift);
        }

        @Override
        public short getGeoHashShort(Record rec) {
            assert ColumnType.sizeOf(getType()) == 2;
            return (short) (value.getGeoHashInt(rec) >> shift);
        }

        @Override
        public int getGeoHashInt(Record rec) {
            assert ColumnType.sizeOf(getType()) == 4;
            return value.getGeoHashInt(rec) >> shift;
        }

        @Override
        public long getGeoHashLong(Record rec) {
            throw new UnsupportedOperationException();
        }
    }

    private static class CastShortFunc extends GeoHashFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastShortFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            assert ColumnType.sizeOf(getType()) == 1;
            return (byte) (value.getGeoHashShort(rec) >> shift);
        }

        @Override
        public short getGeoHashShort(Record rec) {
            assert ColumnType.sizeOf(getType()) == 2;
            return (short) (value.getGeoHashShort(rec) >> shift);
        }

        @Override
        public int getGeoHashInt(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getGeoHashLong(Record rec) {
            throw new UnsupportedOperationException();
        }
    }

    private static class CastByteFunc extends GeoHashFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastByteFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            assert ColumnType.sizeOf(getType()) == 1;
            return (byte) (value.getGeoHashByte(rec) >> shift);
        }

        @Override
        public short getGeoHashShort(Record rec) {
            return (byte) (value.getGeoHashShort(rec) >> shift);
        }

        @Override
        public int getGeoHashInt(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getGeoHashLong(Record rec) {
            throw new UnsupportedOperationException();
        }
    }
}
