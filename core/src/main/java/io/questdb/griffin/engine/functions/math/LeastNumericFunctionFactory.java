/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.ResultTypes;
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
import io.questdb.griffin.engine.functions.decimal.Decimal128LoaderFunctionFactory;
import io.questdb.griffin.engine.functions.decimal.Decimal256Function;
import io.questdb.griffin.engine.functions.decimal.Decimal256LoaderFunctionFactory;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.griffin.engine.functions.decimal.Decimal64LoaderFunctionFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;

public class LeastNumericFunctionFactory implements FunctionFactory {
    @Override
    public int getResultType(IntList argTypes) {
        return ResultTypes.numericExtreme(argTypes);
    }

    @Override
    public String getSignature() {
        return "least(V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args == null || args.size() == 0) {
            throw SqlException.$(position, "at least one argument is required by LEAST(V)");
        }
        for (int i = 0, n = args.size(); i < n; i++) {
            final int type = args.getQuick(i).getType();
            if (!ResultTypes.isNumericExtremeArgument(type)) {
                throw SqlException.position(argPositions.getQuick(i)).put("unsupported type: ").put(ColumnType.nameOf(type));
            }
        }
        final IntList argTypes = ResultTypes.argTypes(args);
        final int type = getResultType(argTypes);
        if (type == ColumnType.NULL) {
            return NullConstant.NULL;
        }
        final boolean isTimestampConverted = argTypes.contains(ColumnType.DATE)
                || (type == ColumnType.TIMESTAMP_NANO && argTypes.contains(ColumnType.TIMESTAMP_MICRO));
        // have to copy, args is mutable
        final ObjList<Function> ownArgs = new ObjList<>(args);
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.DOUBLE -> new LeastDoubleRecordFunction(ownArgs);
            case ColumnType.FLOAT ->
                    new CastDoubleToFloatFunctionFactory.CastDoubleToFloatFunction(new LeastDoubleRecordFunction(ownArgs));
            case ColumnType.TIMESTAMP -> isTimestampConverted
                    ? new LeastTimestampRecordFunction(ownArgs, type)
                    : new CastLongToTimestampFunctionFactory.Func(new LeastLongRecordFunction(ownArgs), type);
            case ColumnType.DATE ->
                    new CastLongToDateFunctionFactory.CastLongToDateFunction(new LeastLongRecordFunction(ownArgs));
            case ColumnType.LONG -> new LeastLongRecordFunction(ownArgs);
            case ColumnType.INT ->
                    new CastLongToIntFunctionFactory.CastLongToIntFunction(new LeastLongRecordFunction(ownArgs));
            case ColumnType.SHORT ->
                    new CastLongToShortFunctionFactory.CastLongToShortFunction(new LeastLongRecordFunction(ownArgs));
            case ColumnType.BYTE ->
                    new CastLongToByteFunctionFactory.CastLongToByteFunction(new LeastLongRecordFunction(ownArgs));
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    new LeastDecimal64RecordFunction(type, ColumnType.getDecimalScale(type), ownArgs);
            case ColumnType.DECIMAL128 ->
                    new LeastDecimal128RecordFunction(type, ColumnType.getDecimalScale(type), ownArgs);
            case ColumnType.DECIMAL256 ->
                    new LeastDecimal256RecordFunction(type, ColumnType.getDecimalScale(type), ownArgs, new IntList(argPositions));
            default -> throw SqlException.position(argPositions.getQuick(0)).put("unexpected argument types");
        };
    }

    private static class LeastDecimal128RecordFunction extends Decimal128Function implements MultiArgFunction {
        private final int[] argScales;
        private final ObjList<Function> args;
        private final Decimal128 decimal128 = new Decimal128();
        private final int n;
        private final int scale;

        public LeastDecimal128RecordFunction(int type, int scale, ObjList<Function> args) {
            super(type);
            this.scale = scale;
            this.n = args.size();
            this.args = new ObjList<>(n);
            for (int i = 0; i < n; i++) {
                this.args.add(Decimal128LoaderFunctionFactory.getInstance(args.getQuick(i)));
            }
            this.argScales = new int[n];
            for (int i = 0; i < n; i++) {
                this.argScales[i] = ColumnType.getDecimalScale(args.getQuick(i).getType());
            }
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            sink.ofRawNull();
            for (int i = 0; i < n; i++) {
                Function arg = args.getQuick(i);
                arg.getDecimal128(rec, decimal128);
                if (!decimal128.isNull()) {
                    decimal128.setScale(argScales[i]);
                    if (argScales[i] != scale) {
                        decimal128.rescale(scale);
                    }
                    if (sink.isNull() || decimal128.compareTo(sink) < 0) {
                        sink.copyFrom(decimal128);
                    }
                }
            }
        }

        @Override
        public String getName() {
            return "least[DECIMAL]";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class LeastDecimal256RecordFunction extends Decimal256Function implements MultiArgFunction {
        private final IntList argPositions;
        private final int[] argScales;
        private final ObjList<Function> args;
        private final Decimal256 decimal256 = new Decimal256();
        private final int n;
        private final int scale;

        public LeastDecimal256RecordFunction(int type, int scale, ObjList<Function> args, IntList argPositions) {
            super(type);
            this.scale = scale;
            this.argPositions = argPositions;
            this.n = args.size();
            this.args = new ObjList<>(n);
            for (int i = 0; i < n; i++) {
                this.args.add(Decimal256LoaderFunctionFactory.getInstance(args.getQuick(i)));
            }
            this.argScales = new int[n];
            for (int i = 0; i < n; i++) {
                this.argScales[i] = ColumnType.getDecimalScale(args.getQuick(i).getType());
            }
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.ofRawNull();
            for (int i = 0; i < n; i++) {
                Function arg = args.getQuick(i);
                arg.getDecimal256(rec, decimal256);
                if (!decimal256.isNull()) {
                    decimal256.setScale(argScales[i]);
                    if (argScales[i] != scale) {
                        try {
                            decimal256.rescale(scale);
                        } catch (NumericException ex) {
                            throw ImplicitCastException.inconvertibleValue(
                                            decimal256,
                                            Decimal256LoaderFunctionFactory.getParent(arg).getType(),
                                            type)
                                    .position(argPositions.getQuick(i));
                        }
                    }
                    if (sink.isNull() || decimal256.compareTo(sink) < 0) {
                        sink.copyFrom(decimal256);
                    }
                }
            }
        }

        @Override
        public String getName() {
            return "least[DECIMAL]";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class LeastDecimal64RecordFunction extends Decimal64Function implements MultiArgFunction {
        private final int[] argScales;
        private final ObjList<Function> args;
        private final Decimal64 decimal64 = new Decimal64();
        private final Decimal64 least = new Decimal64();
        private final int n;
        private final int scale;

        public LeastDecimal64RecordFunction(int type, int scale, ObjList<Function> args) {
            super(type);
            this.scale = scale;
            this.n = args.size();
            this.args = new ObjList<>(n);
            for (int i = 0; i < n; i++) {
                this.args.add(Decimal64LoaderFunctionFactory.getInstance(args.getQuick(i)));
            }
            this.argScales = new int[n];
            for (int i = 0; i < n; i++) {
                this.argScales[i] = ColumnType.getDecimalScale(args.getQuick(i).getType());
            }
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public short getDecimal16(Record rec) {
            compute(rec);
            // narrowing the 64-bit null sentinel would yield 0
            return least.isNull() ? Decimals.DECIMAL16_NULL : (short) least.getValue();
        }

        @Override
        public int getDecimal32(Record rec) {
            compute(rec);
            return least.isNull() ? Decimals.DECIMAL32_NULL : (int) least.getValue();
        }

        @Override
        public long getDecimal64(Record rec) {
            compute(rec);
            return least.getValue();
        }

        @Override
        public byte getDecimal8(Record rec) {
            compute(rec);
            return least.isNull() ? Decimals.DECIMAL8_NULL : (byte) least.getValue();
        }

        @Override
        public String getName() {
            return "least[DECIMAL]";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        /**
         * Compute the least value from the list of given functions and store it in {@link #least}
         *
         * @param rec Record use to load values from functions
         */
        private void compute(Record rec) {
            least.ofRawNull();
            for (int i = 0; i < n; i++) {
                Function arg = args.getQuick(i);
                decimal64.ofRaw(arg.getDecimal64(rec));
                if (!decimal64.isNull()) {
                    decimal64.setScale(argScales[i]);
                    if (argScales[i] != scale) {
                        decimal64.rescale(scale);
                    }
                    if (least.isNull() || decimal64.compareTo(least) < 0) {
                        least.copyFrom(decimal64);
                    }
                }
            }
        }
    }

    private static class LeastDoubleRecordFunction extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final Decimal256 decimal256 = new Decimal256();
        private final int n;
        private final StringSink sink = new StringSink();

        public LeastDoubleRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
            for (int i = 0; i < n; i++) {
                if (ColumnType.isDecimal(this.args.getQuick(i).getType())) {
                    this.args.setQuick(i, Decimal256LoaderFunctionFactory.getInstance(this.args.getQuick(i)));
                }
            }
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            double value = Double.POSITIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                final Function arg = args.getQuick(i);
                final double v = load(arg, rec);
                if (!Numbers.isNull(v)) {
                    value = Math.min(value, v);
                }
            }
            return value;
        }

        @Override
        public String getName() {
            return "least[DOUBLE]";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private double load(Function arg, Record rec) {
            final int type = arg.getType();
            if (ColumnType.tagOf(type) == ColumnType.DECIMAL256) {
                arg.getDecimal256(rec, decimal256);
                if (decimal256.isNull()) {
                    return Double.NaN;
                }
                sink.clear();
                decimal256.setScale(ColumnType.getDecimalScale(type));
                sink.put(decimal256);
                return Numbers.parseDouble(sink);
            }
            return arg.getDouble(rec);
        }
    }

    private static class LeastLongRecordFunction extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;

        public LeastLongRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            long value = Long.MAX_VALUE;
            boolean foundValidValue = false;
            for (int i = 0; i < n; i++) {
                final long v = args.getQuick(i).getLong(rec);
                if (v != Numbers.LONG_NULL) {
                    foundValidValue = true;
                    value = Math.min(value, v);
                }
            }
            return foundValidValue ? value : Numbers.LONG_NULL;
        }

        @Override
        public String getName() {
            return "least[LONG]";
        }
    }

    private static class LeastTimestampRecordFunction extends TimestampFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;
        private final IntList timestampTypes;

        public LeastTimestampRecordFunction(ObjList<Function> args, int timestampType) {
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
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public String getName() {
            return "least[TIMESTAMP]";
        }

        @Override
        public long getTimestamp(Record rec) {
            long value = Long.MAX_VALUE;
            boolean foundValidValue = false;
            for (int i = 0; i < n; i++) {
                final long v = load(rec, i);
                if (v != Numbers.LONG_NULL) {
                    foundValidValue = true;
                    value = Math.min(value, v);
                }
            }
            return foundValidValue ? value : Numbers.LONG_NULL;
        }

        private long load(Record rec, int index) {
            final Function arg = args.getQuick(index);
            final int timestampType = timestampTypes.getQuick(index);
            return timestampType == ColumnType.UNDEFINED
                    ? arg.getLong(rec)
                    : timestampDriver.from(arg.getTimestamp(rec), timestampType);
        }
    }
}
