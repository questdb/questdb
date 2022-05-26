/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.engine.functions.*;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class CastGeoHashToGeoHashFunctionFactory implements FunctionFactory {
    public static Function newInstance(int position, Function value, int srcType, int targetType) throws SqlException {
        int srcBitsPrecision = ColumnType.getGeoHashBits(srcType);
        int targetBitsPrecision = ColumnType.getGeoHashBits(targetType);
        int shift = srcBitsPrecision - targetBitsPrecision;
        if (shift > 0) {
            if (value.isConstant()) {
                long val = GeoHashes.getGeoLong(srcType, value, null);
                // >> shift will take care of NULL value -1
                return Constants.getGeoHashConstantWithType(val >> shift, targetType);
            }

            final Function result = castFunc(shift, targetType, value, srcType);
            if (result != null) {
                return result;
            }
        } else if (shift == 0) {
            return value;
        }

        // check if this is a null of different bit count
        if (value.isConstant() && GeoHashes.getGeoLong(value.getType(), value, null) == GeoHashes.NULL) {
            return Constants.getNullConstant(targetType);
        }

        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                throw SqlException.position(position)
                        .put("CAST cannot decrease precision from GEOHASH(")
                        .put(srcBitsPrecision)
                        .put("b) to GEOHASH(")
                        .put(targetBitsPrecision)
                        .put("b)");
            case ColumnType.STRING:
                switch (ColumnType.tagOf(srcType)) {
                    case ColumnType.GEOBYTE:
                        if (srcBitsPrecision % 5 == 0) {
                            return new CastGeoByteToStrCharsFunc(value, srcBitsPrecision / 5);
                        }
                        return new CastGeoByteToStrBitsFunc(value, srcBitsPrecision);
                    case ColumnType.GEOSHORT:
                        if (srcBitsPrecision % 5 == 0) {
                            return new CastGeoShortToStrCharsFunc(value, srcBitsPrecision / 5);
                        }
                        return new CastGeoShortToStrBitsFunc(value, srcBitsPrecision);
                }
            default:
                throw SqlException.position(position)
                        .put("cannot cast GEOHASH(")
                        .put(srcBitsPrecision)
                        .put("b) to ")
                        .put(ColumnType.nameOf(targetType));
        }
    }

    @Override
    public String getSignature() {
        // GeoHashes are of different lengths
        // and can be cast to lower precision
        // for example cast(cast('questdb' as geohash(6c)) as geohash(5c))
        return "cast(Gg)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function value = args.getQuick(0);
        int srcType = value.getType();
        int targetType = args.getQuick(1).getType();
        return newInstance(position, value, srcType, targetType);
    }

    private static Function castFunc(int shift, int targetType, Function value, int srcType) {
        switch (ColumnType.tagOf(srcType)) {
            case ColumnType.GEOBYTE:
                if (ColumnType.tagOf(targetType) == ColumnType.GEOBYTE) {
                    return new CastByteFunc(shift, targetType, value);
                }
                break;
            case ColumnType.GEOSHORT:
                switch (ColumnType.tagOf(targetType)) {
                    case ColumnType.GEOBYTE:
                        return new CastShortToByteFunc(shift, targetType, value);
                    case ColumnType.GEOSHORT:
                        return new CastShortFunc(shift, targetType, value);
                }
                break;
            case ColumnType.GEOINT:
                switch (ColumnType.tagOf(targetType)) {
                    case ColumnType.GEOBYTE:
                        return new CastIntToByteFunc(shift, targetType, value);
                    case ColumnType.GEOSHORT:
                        return new CastIntToShortFunc(shift, targetType, value);
                    case ColumnType.GEOINT:
                        return new CastIntFunc(shift, targetType, value);
                }
                break;
            default:
                switch (ColumnType.tagOf(targetType)) {
                    case ColumnType.GEOBYTE:
                        return new CastLongToByteFunc(shift, targetType, value);
                    case ColumnType.GEOSHORT:
                        return new CastLongToShortFunc(shift, targetType, value);
                    case ColumnType.GEOINT:
                        return new CastLongToIntFunc(shift, targetType, value);
                    case ColumnType.GEOLONG:
                        return new CastLongFunc(shift, targetType, value);
                }
        }
        return null;
    }

    private static class CastLongFunc extends GeoLongFunction implements UnaryFunction {
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
        public long getGeoLong(Record rec) {
            return value.getGeoLong(rec) >> shift;
        }
    }

    private static class CastLongToByteFunc extends GeoByteFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastLongToByteFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) (value.getGeoLong(rec) >> shift);
        }
    }

    private static class CastLongToShortFunc extends GeoShortFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastLongToShortFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getGeoShort(Record rec) {
            return (short) (value.getGeoLong(rec) >> shift);
        }
    }

    private static class CastLongToIntFunc extends GeoIntFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastLongToIntFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public int getGeoInt(Record rec) {
            return (int) (value.getGeoLong(rec) >> shift);
        }
    }

    private static class CastIntFunc extends GeoIntFunction implements UnaryFunction {
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
        public int getGeoInt(Record rec) {
            return value.getGeoInt(rec) >> shift;
        }
    }

    private static class CastIntToByteFunc extends GeoByteFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastIntToByteFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) (value.getGeoInt(rec) >> shift);
        }
    }

    private static class CastIntToShortFunc extends GeoShortFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastIntToShortFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public short getGeoShort(Record rec) {
            return (short) (value.getGeoInt(rec) >> shift);
        }
    }

    private static class CastShortFunc extends GeoShortFunction implements UnaryFunction {
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
        public short getGeoShort(Record rec) {
            return (short) (value.getGeoShort(rec) >> shift);
        }
    }

    private static class CastShortToByteFunc extends GeoByteFunction implements UnaryFunction {
        private final Function value;
        private final int shift;

        public CastShortToByteFunc(int shift, int targetType, Function value) {
            super(targetType);
            this.value = value;
            this.shift = shift;
        }

        @Override
        public Function getArg() {
            return value;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return (byte) (value.getGeoShort(rec) >> shift);
        }
    }

    private static class CastByteFunc extends GeoByteFunction implements UnaryFunction {
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
        public byte getGeoByte(Record rec) {
            return (byte) (value.getGeoByte(rec) >> shift);
        }
    }

    private static class CastGeoByteToStrCharsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoByteToStrCharsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoByte(rec);
        }

        @Override
        protected void print(long value, CharSink sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoShortToStrCharsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoShortToStrCharsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoShort(rec);
        }

        @Override
        protected void print(long value, CharSink sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoByteToStrBitsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoByteToStrBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoByte(rec);
        }

        @Override
        protected void print(long value, CharSink sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoShortToStrBitsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoShortToStrBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoShort(rec);
        }

        @Override
        protected void print(long value, CharSink sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
        }
    }

    private static abstract class AbstractCastGeoByteToStrFunction extends StrFunction implements UnaryFunction {
        protected final Function value;
        protected final int bits;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public AbstractCastGeoByteToStrFunction(Function value, int bits) {
            this.value = value;
            this.bits = bits;
        }

        @Override
        public final Function getArg() {
            return value;
        }

        @Override
        public final CharSequence getStr(Record rec) {
            return toSink(getValue(rec), sinkA);
        }

        @Override
        public final CharSequence getStrB(Record rec) {
            return toSink(getValue(rec), sinkB);
        }

        protected abstract long getValue(Record rec);

        protected abstract void print(long value, CharSink sink);

        private StringSink toSink(long value, StringSink sink) {
            sink.clear();
            if (value == GeoHashes.NULL) {
                sink.put("null");
            } else {
                print(value, sink);
            }
            return sink;
        }
    }
}
