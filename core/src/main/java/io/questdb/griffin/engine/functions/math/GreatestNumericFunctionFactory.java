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

public class GreatestNumericFunctionFactory implements FunctionFactory {
    @Override
    public int getResultType(IntList argTypes) {
        return ResultTypes.numericExtreme(argTypes);
    }

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
        if (args == null || args.size() == 0) {
            throw SqlException.$(position, "at least one argument is required by GREATEST(V)");
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
            case ColumnType.DOUBLE -> new GreatestDoubleRecordFunction(ownArgs);
            case ColumnType.FLOAT ->
                    new CastDoubleToFloatFunctionFactory.CastDoubleToFloatFunction(new GreatestDoubleRecordFunction(ownArgs));
            case ColumnType.TIMESTAMP -> isTimestampConverted
                    ? new GreatestTimestampRecordFunction(ownArgs, type)
                    : new CastLongToTimestampFunctionFactory.Func(new GreatestLongRecordFunction(ownArgs), type);
            case ColumnType.DATE ->
                    new CastLongToDateFunctionFactory.CastLongToDateFunction(new GreatestLongRecordFunction(ownArgs));
            case ColumnType.LONG -> new GreatestLongRecordFunction(ownArgs);
            case ColumnType.INT ->
                    new CastLongToIntFunctionFactory.CastLongToIntFunction(new GreatestLongRecordFunction(ownArgs));
            case ColumnType.SHORT ->
                    new CastLongToShortFunctionFactory.CastLongToShortFunction(new GreatestLongRecordFunction(ownArgs));
            case ColumnType.BYTE ->
                    new CastLongToByteFunctionFactory.CastLongToByteFunction(new GreatestLongRecordFunction(ownArgs));
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    new GreatestDecimal64RecordFunction(type, ColumnType.getDecimalScale(type), ownArgs);
            case ColumnType.DECIMAL128 ->
                    new GreatestDecimal128RecordFunction(type, ColumnType.getDecimalScale(type), ownArgs);
            case ColumnType.DECIMAL256 ->
                    new GreatestDecimal256RecordFunction(type, ColumnType.getDecimalScale(type), ownArgs, new IntList(argPositions));
            default -> throw SqlException.position(argPositions.getQuick(0)).put("unexpected argument types");
        };
    }

    private static class GreatestDecimal128RecordFunction extends Decimal128Function implements MultiArgFunction {
        private final int[] argScales;
        private final ObjList<Function> args;
        private final Decimal128 decimal128 = new Decimal128();
        private final int n;
        private final int scale;

        public GreatestDecimal128RecordFunction(int type, int scale, ObjList<Function> args) {
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
                    if (decimal128.compareTo(sink) > 0) {
                        sink.copyFrom(decimal128);
                    }
                }
            }
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
        private final int[] argScales;
        private final ObjList<Function> args;
        private final Decimal256 decimal256 = new Decimal256();
        private final int n;
        private final int scale;

        public GreatestDecimal256RecordFunction(int type, int scale, ObjList<Function> args, IntList argPositions) {
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
                    if (decimal256.compareTo(sink) > 0) {
                        sink.copyFrom(decimal256);
                    }
                }
            }
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
        private final int[] argScales;
        private final ObjList<Function> args;
        private final Decimal64 decimal64 = new Decimal64();
        private final Decimal64 greatest = new Decimal64();
        private final int n;
        private final int scale;

        public GreatestDecimal64RecordFunction(int type, int scale, ObjList<Function> args) {
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
            return greatest.isNull() ? Decimals.DECIMAL16_NULL : (short) greatest.getValue();
        }

        @Override
        public int getDecimal32(Record rec) {
            compute(rec);
            return greatest.isNull() ? Decimals.DECIMAL32_NULL : (int) greatest.getValue();
        }

        @Override
        public long getDecimal64(Record rec) {
            compute(rec);
            return greatest.getValue();
        }

        @Override
        public byte getDecimal8(Record rec) {
            compute(rec);
            return greatest.isNull() ? Decimals.DECIMAL8_NULL : (byte) greatest.getValue();
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
            greatest.ofRawNull();
            for (int i = 0; i < n; i++) {
                Function arg = args.getQuick(i);
                decimal64.ofRaw(arg.getDecimal64(rec));
                if (!decimal64.isNull()) {
                    decimal64.setScale(argScales[i]);
                    if (argScales[i] != scale) {
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

    private static class GreatestLongRecordFunction extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;

        public GreatestLongRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> args() {
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
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public String getName() {
            return "greatest[TIMESTAMP]";
        }

        @Override
        public long getTimestamp(Record rec) {
            long value = load(rec, 0);
            for (int i = 1; i < n; i++) {
                value = Math.max(value, load(rec, i));
            }
            return value;
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
