/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.cast.CastDoubleToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.griffin.engine.functions.decimal.Decimal256Function;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class GreatestNumericFunctionFactory implements FunctionFactory {
    private static final ThreadLocal<IntHashSet> tlSet = ThreadLocal.withInitial(IntHashSet::new);

    @Override
    public String getSignature() {
        return "greatest(V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final IntHashSet counters = tlSet.get();
        counters.clear();
        final int argCount;
        if (args == null || (argCount = args.size()) == 0) {
            throw SqlException.$(position, "at least one argument is required by GREATEST(V)");
        }
        boolean allNull = true;
        for (int i = 0; i < argCount; i++) {
            final Function arg = args.getQuick(i);
            final int type = arg.getType();

            switch (ColumnType.tagOf(type)) {
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.SHORT:
                case ColumnType.BYTE:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                case ColumnType.DECIMAL8:
                case ColumnType.DECIMAL16:
                case ColumnType.DECIMAL32:
                case ColumnType.DECIMAL64:
                case ColumnType.DECIMAL128:
                case ColumnType.DECIMAL256:
                    allNull = false;
                case ColumnType.NULL:
                    counters.add(type);
                    continue;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("unsupported type: ").put(ColumnType.nameOf(type));
            }
        }
        if (allNull) {
            return NullConstant.NULL;
        }

        // have to copy, args is mutable
        final Function retVal = getGreatestFunction(new ObjList<>(args), new IntList(argPositions), counters);
        if (retVal != null) {
            return retVal;
        }
        throw SqlException.position(argPositions.getQuick(0)).put("unexpected argument types");
    }

    private static @NotNull Function getDecimalGreatestFunction(ObjList<Function> args, IntList argPositions) throws SqlException {
        // We need to find the maximum scale/precision combination.
        int precision = 1;
        int scale = 0;

        for (int i = 0, n = args.size(); i < n; i++) {
            int type = args.getQuick(i).getType();
            final int r = DecimalUtil.getTypePrecisionScale(type);
            int argPrecision = Numbers.decodeLowShort(r);
            int argScale = Numbers.decodeHighShort(r);
            int finalScale = Math.max(scale, argScale);
            precision = Math.max(precision - scale, argPrecision - argScale) + finalScale;
            scale = finalScale;
        }

        final int type = ColumnType.getDecimalType(Math.min(precision, Decimals.MAX_PRECISION), scale);
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL128 -> new GreatestDecimal128RecordFunction(type, scale, args);
            case ColumnType.DECIMAL256 -> new GreatestDecimal256RecordFunction(type, scale, args, argPositions);
            default -> new GreatestDecimal64RecordFunction(type, scale, args);
        };
    }

    private static @Nullable Function getGreatestFunction(ObjList<Function> args, IntList argPositions, IntHashSet set) throws SqlException {
        if (set.contains(ColumnType.DOUBLE)) {
            return new GreatestDoubleRecordFunction(args);
        }

        if (set.contains(ColumnType.FLOAT)) {
            return new CastDoubleToFloatFunctionFactory.CastDoubleToFloatFunction(new GreatestDoubleRecordFunction(args));
        }

        // Decimals are able to represent numbers that are between -10⁷⁶-1 and 10⁷⁶-1, so we put them after double/float.
        for (int i = 0, n = args.size(); i < n; i++) {
            if (ColumnType.isDecimal(args.getQuick(i).getType())) {
                return getDecimalGreatestFunction(args, argPositions);
            }
        }

        if (set.contains(ColumnType.TIMESTAMP_NANO)) {
            if (set.contains(ColumnType.DATE) || set.contains(ColumnType.TIMESTAMP_MICRO)) {
                return new GreatestTimestampRecordFunction(args, ColumnType.TIMESTAMP_NANO);
            }
            return new CastLongToTimestampFunctionFactory.Func(new GreatestLongRecordFunction(args), ColumnType.TIMESTAMP_NANO);
        }

        if (set.contains(ColumnType.TIMESTAMP_MICRO)) {
            if (set.contains(ColumnType.DATE)) {
                return new GreatestTimestampRecordFunction(args, ColumnType.TIMESTAMP_MICRO);
            }
            return new CastLongToTimestampFunctionFactory.Func(new GreatestLongRecordFunction(args), ColumnType.TIMESTAMP_MICRO);
        }

        if (set.contains(ColumnType.DATE)) {
            return new CastLongToDateFunctionFactory.CastLongToDateFunction(new GreatestLongRecordFunction(args));
        }

        if (set.contains(ColumnType.LONG)) {
            return new GreatestLongRecordFunction(args);
        }

        if (set.contains(ColumnType.INT)) {
            return new CastLongToIntFunctionFactory.CastLongToIntFunction(new GreatestLongRecordFunction(args));
        }

        if (set.contains(ColumnType.SHORT)) {
            return new CastLongToShortFunctionFactory.CastLongToShortFunction(new GreatestLongRecordFunction(args));
        }

        if (set.contains(ColumnType.BYTE)) {
            return new CastLongToByteFunctionFactory.CastLongToByteFunction(new GreatestLongRecordFunction(args));
        }

        return null;
    }

    private static class GreatestDecimal128RecordFunction extends Decimal128Function implements MultiArgFunction {
        private final ObjList<Function> args;
        private final Decimal128 decimal128 = new Decimal128();
        private final Decimal128 greatest = new Decimal128();
        private final int n;
        private final int scale;

        public GreatestDecimal128RecordFunction(int type, int scale, ObjList<Function> args) {
            super(type);
            this.scale = scale;
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getDecimal128Hi(Record rec) {
            greatest.ofNull();
            for (int i = 0; i < n; i++) {
                Function arg = args.getQuick(i);
                // We need to cast everything to a decimal128 to be able to compare against the lowest value
                DecimalUtil.load(decimal128, arg, rec);
                if (!decimal128.isNull()) {
                    if (decimal128.getScale() != scale) {
                        decimal128.rescale(scale);
                    }
                    if (decimal128.compareTo(greatest) > 0) {
                        greatest.copyFrom(decimal128);
                    }
                }
            }
            return greatest.getHigh();
        }

        @Override
        public long getDecimal128Lo(Record rec) {
            return greatest.getLow();
        }

        @Override
        public String getName() {
            return "greatest[DECIMAL]";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class GreatestDecimal256RecordFunction extends Decimal256Function implements MultiArgFunction {
        private final IntList argPositions;
        private final ObjList<Function> args;
        private final Decimal256 decimal256 = new Decimal256();
        private final Decimal256 greatest = new Decimal256();
        private final int n;
        private final int scale;

        public GreatestDecimal256RecordFunction(int type, int scale, ObjList<Function> args, IntList argPositions) {
            super(type);
            this.scale = scale;
            this.args = args;
            this.argPositions = argPositions;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getDecimal256HH(Record rec) {
            greatest.ofNull();
            for (int i = 0; i < n; i++) {
                Function arg = args.getQuick(i);
                // We need to cast everything to a decimal256 to be able to compare against the lowest value
                DecimalUtil.load(decimal256, arg, rec);
                if (!decimal256.isNull()) {
                    if (decimal256.getScale() != scale) {
                        try {
                            decimal256.rescale(scale);
                        } catch (NumericException ex) {
                            throw ImplicitCastException.inconvertibleValue(decimal256, arg.getType(), type)
                                    .position(argPositions.getQuick(i));
                        }
                    }
                    if (decimal256.compareTo(greatest) > 0) {
                        greatest.copyFrom(decimal256);
                    }
                }
            }
            return greatest.getHh();
        }

        @Override
        public long getDecimal256HL(Record rec) {
            return greatest.getHl();
        }

        @Override
        public long getDecimal256LH(Record rec) {
            return greatest.getLh();
        }

        @Override
        public long getDecimal256LL(Record rec) {
            return greatest.getLl();
        }

        @Override
        public String getName() {
            return "greatest[DECIMAL]";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class GreatestDecimal64RecordFunction extends Decimal64Function implements MultiArgFunction {
        private final ObjList<Function> args;
        private final Decimal64 decimal64 = new Decimal64();
        private final Decimal64 greatest = new Decimal64();
        private final int n;
        private final int scale;

        public GreatestDecimal64RecordFunction(int type, int scale, ObjList<Function> args) {
            super(type);
            this.scale = scale;
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public short getDecimal16(Record rec) {
            compute(rec);
            return (short) greatest.getValue();
        }

        @Override
        public int getDecimal32(Record rec) {
            compute(rec);
            return (int) greatest.getValue();
        }

        @Override
        public long getDecimal64(Record rec) {
            compute(rec);
            return greatest.getValue();
        }

        @Override
        public byte getDecimal8(Record rec) {
            compute(rec);
            return (byte) greatest.getValue();
        }

        @Override
        public String getName() {
            return "greatest[DECIMAL]";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        /**
         * Compute the greatest value from the list of given functions and store it in {@link #greatest}
         *
         * @param rec Record use to load values from functions
         */
        private void compute(Record rec) {
            greatest.ofNull();
            for (int i = 0; i < n; i++) {
                Function arg = args.getQuick(i);
                // We need to cast everything to a decimal64 to be able to compare against the lowest value
                DecimalUtil.load(decimal64, arg, rec);
                if (!decimal64.isNull()) {
                    if (decimal64.getScale() != scale) {
                        decimal64.rescale(scale);
                    }
                    if (decimal64.compareTo(greatest) > 0) {
                        greatest.copyFrom(decimal64);
                    }
                }
            }
        }
    }

    private static class GreatestDoubleRecordFunction extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final Decimal256 decimal256 = new Decimal256();
        private final int n;
        private final StringSink sink = new StringSink();

        public GreatestDoubleRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            double value = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                final Function arg = args.getQuick(i);
                final double v = load(arg, rec);
                if (!Numbers.isNull(v)) {
                    value = Math.max(value, v);
                }
            }
            return value;
        }

        @Override
        public String getName() {
            return "greatest[DOUBLE]";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private double load(Function arg, Record rec) {
            final int type = arg.getType();
            if (ColumnType.isDecimal(type)) {
                DecimalUtil.load(decimal256, arg, rec, type);
                if (decimal256.isNull()) {
                    return Double.NEGATIVE_INFINITY;
                }
                sink.clear();
                sink.put(decimal256);
                return Numbers.parseDouble(sink);
            }
            return arg.getDouble(rec);
        }
    }

    private static class GreatestLongRecordFunction extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;

        public GreatestLongRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            long value = args.getQuick(0).getLong(rec);
            for (int i = 1; i < n; i++) {
                value = Math.max(value, args.getQuick(i).getLong(rec));
            }
            return value;
        }

        @Override
        public String getName() {
            return "greatest[LONG]";
        }
    }

    private static class GreatestTimestampRecordFunction extends TimestampFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;
        private final IntList timestampTypes;

        public GreatestTimestampRecordFunction(ObjList<Function> args, int timestampType) {
            super(timestampType);
            this.args = args;
            this.n = args.size();
            timestampTypes = new IntList(n);
            timestampTypes.setPos(n);
            for (int i = 0; i < n; i++) {
                timestampTypes.setQuick(i, ColumnType.getTimestampType(args.getQuick(i).getType()));
            }
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public String getName() {
            return "greatest[TIMESTAMP]";
        }

        @Override
        public long getTimestamp(Record rec) {
            long value = timestampDriver.from(args.getQuick(0).getTimestamp(rec), timestampTypes.getQuick(0));
            for (int i = 1; i < n; i++) {
                value = Math.max(value, timestampDriver.from(args.getQuick(i).getTimestamp(rec), timestampTypes.getQuick(i)));
            }
            return value;
        }
    }
}
