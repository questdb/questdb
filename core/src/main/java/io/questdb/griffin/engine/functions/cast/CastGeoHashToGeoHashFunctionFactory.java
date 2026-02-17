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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.*;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;

public class CastGeoHashToGeoHashFunctionFactory implements FunctionFactory {

    @NotNull
    public static Function getGeoByteToStrCastFunction(Function value, int srcBitsPrecision) {
        if (srcBitsPrecision % 5 == 0) {
            return new CastGeoByteToStrCharsFunc(value, srcBitsPrecision / 5);
        }
        return new CastGeoByteToStrBitsFunc(value, srcBitsPrecision);
    }

    // TODO: getGeo*ToVarcharCastFunction methods are currently unused due to the lack
    // of support for cast(geohash_col as VARCHAR). Issue that tracks this:
    // https://github.com/questdb/questdb/issues/4262

    @NotNull
    public static Function getGeoByteToVarcharCastFunction(Function value, int srcBitsPrecision) {
        if (srcBitsPrecision % 5 == 0) {
            return new CastGeoByteToVarcharCharsFunc(value, srcBitsPrecision / 5);
        }
        return new CastGeoByteToVarcharBitsFunc(value, srcBitsPrecision);
    }

    @NotNull
    public static Function getGeoIntToStrCastFunction(Function value, int srcBitsPrecision) {
        if (srcBitsPrecision % 5 == 0) {
            return new CastGeoIntToStrCharsFunc(value, srcBitsPrecision / 5);
        }
        return new CastGeoIntToStrBitsFunc(value, srcBitsPrecision);
    }

    @NotNull
    public static Function getGeoIntToVarcharCastFunction(Function value, int srcBitsPrecision) {
        if (srcBitsPrecision % 5 == 0) {
            return new CastGeoIntToVarcharCharsFunc(value, srcBitsPrecision / 5);
        }
        return new CastGeoIntToVarcharBitsFunc(value, srcBitsPrecision);
    }

    @NotNull
    public static Function getGeoLongToStrCastFunction(Function value, int srcBitsPrecision) {
        if (srcBitsPrecision % 5 == 0) {
            return new CastGeoLongToStrCharsFunc(value, srcBitsPrecision / 5);
        }
        return new CastGeoLongToStrBitsFunc(value, srcBitsPrecision);
    }

    @NotNull
    public static Function getGeoLongToVarcharCastFunction(Function value, int srcBitsPrecision) {
        if (srcBitsPrecision % 5 == 0) {
            return new CastGeoLongToVarcharCharsFunc(value, srcBitsPrecision / 5);
        }
        return new CastGeoLongToVarcharBitsFunc(value, srcBitsPrecision);
    }

    @NotNull
    public static Function getGeoShortToStrCastFunction(Function value, int srcBitsPrecision) {
        if (srcBitsPrecision % 5 == 0) {
            return new CastGeoShortToStrCharsFunc(value, srcBitsPrecision / 5);
        }
        return new CastGeoShortToStrBitsFunc(value, srcBitsPrecision);
    }

    @NotNull
    public static Function getGeoShortToVarcharCastFunction(Function value, int srcBitsPrecision) {
        if (srcBitsPrecision % 5 == 0) {
            return new CastGeoShortToVarcharCharsFunc(value, srcBitsPrecision / 5);
        }
        return new CastGeoShortToVarcharBitsFunc(value, srcBitsPrecision);
    }

    public static Function newInstance(int position, Function value, int toType, int fromType) throws SqlException {
        int fromBits = ColumnType.getGeoHashBits(fromType);
        int toBits = ColumnType.getGeoHashBits(toType);
        int shift = fromBits - toBits;
        if (shift > 0) {
            if (value.isConstant()) {
                long val = GeoHashes.getGeoLong(fromType, value, null);
                // >> shift will take care of NULL value -1
                return Constants.getGeoHashConstantWithType(val >> shift, toType);
            }

            final Function result = getCastGeoHashToGeoHashFunction(value, toType, fromType, shift);
            if (result != null) {
                return result;
            }
        } else if (shift == 0) {
            return value;
        }

        // check if this is a null of different bit count
        if (value.isConstant() && GeoHashes.getGeoLong(value.getType(), value, null) == GeoHashes.NULL) {
            return Constants.getNullConstant(toType);
        }

        switch (ColumnType.tagOf(toType)) {
            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                throw SqlException.position(position)
                        .put("CAST cannot narrow values from GEOHASH(")
                        .put(fromBits)
                        .put("b) to GEOHASH(")
                        .put(toBits)
                        .put("b)");
            case ColumnType.STRING:
                switch (ColumnType.tagOf(fromType)) {
                    case ColumnType.GEOBYTE:
                        return getGeoByteToStrCastFunction(value, fromBits);
                    case ColumnType.GEOSHORT:
                        return getGeoShortToStrCastFunction(value, fromBits);
                }
            default:
                throw SqlException.position(position)
                        .put("cannot cast GEOHASH(")
                        .put(fromBits)
                        .put("b) to ")
                        .put(ColumnType.nameOf(toType));
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
                                SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function value = args.getQuick(0);
        int srcType = value.getType();
        int targetType = args.getQuick(1).getType();
        return newInstance(position, value, targetType, srcType);
    }

    private static Function getCastGeoHashToGeoHashFunction(Function value, int toType, int fromType, int shift) {
        switch (ColumnType.tagOf(fromType)) {
            case ColumnType.GEOBYTE:
                if (ColumnType.tagOf(toType) == ColumnType.GEOBYTE) {
                    return new CastByteFunc(shift, toType, value);
                }
                break;
            case ColumnType.GEOSHORT:
                switch (ColumnType.tagOf(toType)) {
                    case ColumnType.GEOBYTE:
                        return new CastShortToByteFunc(shift, toType, value);
                    case ColumnType.GEOSHORT:
                        return new CastGeoShortFunction(shift, toType, value);
                }
                break;
            case ColumnType.GEOINT:
                switch (ColumnType.tagOf(toType)) {
                    case ColumnType.GEOBYTE:
                        return new CastIntToByteFunc(shift, toType, value);
                    case ColumnType.GEOSHORT:
                        return new CastGeoIntToGeoShortFunction(shift, toType, value);
                    case ColumnType.GEOINT:
                        return new CastIntFunc(shift, toType, value);
                }
                break;
            default:
                switch (ColumnType.tagOf(toType)) {
                    case ColumnType.GEOBYTE:
                        return new CastLongToByteFunc(shift, toType, value);
                    case ColumnType.GEOSHORT:
                        return new CastGeoLongToGeoShortFunction(shift, toType, value);
                    case ColumnType.GEOINT:
                        return new CastLongToIntFunc(shift, toType, value);
                    case ColumnType.GEOLONG:
                        return new CastLongFunc(shift, toType, value);
                }
        }
        return null;
    }

    private static abstract class AbstractCastGeoByteToStrFunction extends StrFunction implements UnaryFunction {
        protected final int bits;
        protected final Function value;
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
        public final CharSequence getStrA(Record rec) {
            return toSink(getValue(rec), sinkA);
        }

        @Override
        public final CharSequence getStrB(Record rec) {
            return toSink(getValue(rec), sinkB);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::string");
        }

        private StringSink toSink(long value, StringSink sink) {
            sink.clear();
            if (value == GeoHashes.NULL) {
                sink.put("null");
            } else {
                print(value, sink);
            }
            return sink;
        }

        protected abstract long getValue(Record rec);

        protected abstract void print(long value, Utf16Sink sink);
    }

    private static abstract class AbstractCastGeoByteToVarcharFunction extends VarcharFunction implements UnaryFunction {
        protected final int bits;
        protected final Function value;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public AbstractCastGeoByteToVarcharFunction(Function value, int bits) {
            this.value = value;
            this.bits = bits;
        }

        @Override
        public final Function getArg() {
            return value;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return toSink(getValue(rec), sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return toSink(getValue(rec), sinkB);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value).val("::varchar");
        }

        private Utf8StringSink toSink(long value, Utf8StringSink sink) {
            sink.clear();
            if (value == GeoHashes.NULL) {
                sink.putAscii("null");
            } else {
                print(value, sink);
            }
            return sink;
        }

        protected abstract long getValue(Record rec);

        protected abstract void print(long value, CharSink<?> sink);
    }

    private static class CastByteFunc extends GeoByteFunction implements UnaryFunction {
        private final int shift;
        private final Function value;

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

    private static class CastGeoByteToStrBitsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoByteToStrBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoByte(rec);
        }

        @Override
        protected void print(long value, Utf16Sink sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
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
        protected void print(long value, Utf16Sink sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoByteToVarcharBitsFunc extends AbstractCastGeoByteToVarcharFunction implements UnaryFunction {

        public CastGeoByteToVarcharBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoByte(rec);
        }

        @Override
        protected void print(long value, CharSink<?> sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoByteToVarcharCharsFunc extends AbstractCastGeoByteToVarcharFunction implements UnaryFunction {

        public CastGeoByteToVarcharCharsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoByte(rec);
        }

        @Override
        protected void print(long value, CharSink<?> sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    public static class CastGeoIntToGeoShortFunction extends GeoShortFunction implements UnaryFunction {
        private final int shift;
        private final Function value;

        public CastGeoIntToGeoShortFunction(int shift, int targetType, Function value) {
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

    private static class CastGeoIntToStrBitsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoIntToStrBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoInt(rec);
        }

        @Override
        protected void print(long value, Utf16Sink sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoIntToStrCharsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoIntToStrCharsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoInt(rec);
        }

        @Override
        protected void print(long value, Utf16Sink sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoIntToVarcharBitsFunc extends AbstractCastGeoByteToVarcharFunction implements UnaryFunction {

        public CastGeoIntToVarcharBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoInt(rec);
        }

        @Override
        protected void print(long value, CharSink<?> sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoIntToVarcharCharsFunc extends AbstractCastGeoByteToVarcharFunction implements UnaryFunction {

        public CastGeoIntToVarcharCharsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoInt(rec);
        }

        @Override
        protected void print(long value, CharSink<?> sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    public static class CastGeoLongToGeoShortFunction extends GeoShortFunction implements UnaryFunction {
        private final int shift;
        private final Function value;

        public CastGeoLongToGeoShortFunction(int shift, int targetType, Function value) {
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

    private static class CastGeoLongToStrBitsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoLongToStrBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoLong(rec);
        }

        @Override
        protected void print(long value, Utf16Sink sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoLongToStrCharsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoLongToStrCharsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoLong(rec);
        }

        @Override
        protected void print(long value, Utf16Sink sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoLongToVarcharBitsFunc extends AbstractCastGeoByteToVarcharFunction implements UnaryFunction {

        public CastGeoLongToVarcharBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoLong(rec);
        }

        @Override
        protected void print(long value, CharSink<?> sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoLongToVarcharCharsFunc extends AbstractCastGeoByteToVarcharFunction implements UnaryFunction {

        public CastGeoLongToVarcharCharsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoLong(rec);
        }

        @Override
        protected void print(long value, CharSink<?> sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    public static class CastGeoShortFunction extends GeoShortFunction implements UnaryFunction {
        private final int shift;
        private final Function value;

        public CastGeoShortFunction(int shift, int targetType, Function value) {
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

    private static class CastGeoShortToStrBitsFunc extends AbstractCastGeoByteToStrFunction implements UnaryFunction {

        public CastGeoShortToStrBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoShort(rec);
        }

        @Override
        protected void print(long value, Utf16Sink sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
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
        protected void print(long value, Utf16Sink sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoShortToVarcharBitsFunc extends AbstractCastGeoByteToVarcharFunction implements UnaryFunction {

        public CastGeoShortToVarcharBitsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoShort(rec);
        }

        @Override
        protected void print(long value, CharSink<?> sink) {
            GeoHashes.appendBinaryStringUnsafe(value, bits, sink);
        }
    }

    private static class CastGeoShortToVarcharCharsFunc extends AbstractCastGeoByteToVarcharFunction implements UnaryFunction {

        public CastGeoShortToVarcharCharsFunc(Function value, int bits) {
            super(value, bits);
        }

        @Override
        protected long getValue(Record rec) {
            return this.value.getGeoShort(rec);
        }

        @Override
        protected void print(long value, CharSink<?> sink) {
            GeoHashes.appendCharsUnsafe(value, bits, sink);
        }
    }

    private static class CastIntFunc extends GeoIntFunction implements UnaryFunction {
        private final int shift;
        private final Function value;

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
        private final int shift;
        private final Function value;

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

    private static class CastLongFunc extends GeoLongFunction implements UnaryFunction {
        private final int shift;
        private final Function value;

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
        private final int shift;
        private final Function value;

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

    private static class CastLongToIntFunc extends GeoIntFunction implements UnaryFunction {
        private final int shift;
        private final Function value;

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

    private static class CastShortToByteFunc extends GeoByteFunction implements UnaryFunction {
        private final int shift;
        private final Function value;

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
}
