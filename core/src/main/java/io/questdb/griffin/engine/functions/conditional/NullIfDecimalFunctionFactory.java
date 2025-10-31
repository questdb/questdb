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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.griffin.engine.functions.decimal.Decimal16Function;
import io.questdb.griffin.engine.functions.decimal.Decimal256Function;
import io.questdb.griffin.engine.functions.decimal.Decimal256LoaderFunctionFactory;
import io.questdb.griffin.engine.functions.decimal.Decimal32Function;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.griffin.engine.functions.decimal.Decimal8Function;
import io.questdb.griffin.engine.functions.decimal.ToDecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class NullIfDecimalFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "nullif(ΞΞ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function left = args.getQuick(0);
        Function right = args.getQuick(1);
        right = DecimalUtil.maybeRescaleDecimalConstant(
                right,
                sqlExecutionContext.getDecimal256(),
                sqlExecutionContext.getDecimal128(),
                ColumnType.getDecimalPrecision(left.getType()),
                ColumnType.getDecimalScale(left.getType())
        );
        args.setQuick(1, right);

        final int leftType = left.getType();
        final int rightType = right.getType();
        final int leftTag = ColumnType.tagOf(leftType);
        final int rightTag = ColumnType.tagOf(rightType);
        final int leftScale = ColumnType.getDecimalScale(leftType);
        final int rightScale = ColumnType.getDecimalScale(rightType);
        if (leftTag == rightTag && leftScale == rightScale) {
            return switch (leftTag) {
                case ColumnType.DECIMAL8 -> new UnscaledDecimal8Func(left, right);
                case ColumnType.DECIMAL16 -> new UnscaledDecimal16Func(left, right);
                case ColumnType.DECIMAL32 -> new UnscaledDecimal32Func(left, right);
                case ColumnType.DECIMAL64 -> new UnscaledDecimal64Func(left, right);
                case ColumnType.DECIMAL128 -> new UnscaledDecimal128Func(left, right);
                default -> new UnscaledDecimal256Func(left, right);
            };
        }

        return new Func(args.getQuick(0), args.getQuick(1));
    }

    private static class Func extends ToDecimalFunction implements BinaryFunction {
        private final Function arg0;
        private final Function arg1;
        private final Decimal256 decimalRight = new Decimal256();
        private final int leftScale;
        private final int rightScale;

        public Func(Function arg0, Function arg1) {
            super(arg0.getType());
            this.arg0 = Decimal256LoaderFunctionFactory.getInstance(arg0);
            this.leftScale = ColumnType.getDecimalScale(arg0.getType());
            this.arg1 = Decimal256LoaderFunctionFactory.getInstance(arg1);
            this.rightScale = ColumnType.getDecimalScale(arg1.getType());
        }

        @Override
        public Function getLeft() {
            return arg0;
        }

        @Override
        public Function getRight() {
            return arg1;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        public boolean store(Record record) {
            arg0.getDecimal256(record, decimal);
            decimal.setScale(leftScale);
            arg1.getDecimal256(record, decimalRight);
            decimalRight.setScale(rightScale);
            return decimal.compareTo(decimalRight) != 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg0).val(',').val(arg1).val(')');
        }
    }

    private static class UnscaledDecimal128Func extends Decimal128Function implements BinaryFunction {
        private final Function arg0;
        private final Function arg1;
        private final Decimal128 decimalRight = new Decimal128();

        public UnscaledDecimal128Func(Function arg0, Function arg1) {
            super(arg0.getType());
            this.arg0 = arg0;
            this.arg1 = arg1;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            arg0.getDecimal128(rec, sink);
            arg1.getDecimal128(rec, decimalRight);
            if (Decimal128.compare(sink, decimalRight) == 0) {
                sink.ofRawNull();
            }
        }

        @Override
        public Function getLeft() {
            return arg0;
        }

        @Override
        public Function getRight() {
            return arg1;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg0).val(',').val(arg1).val(')');
        }
    }

    private static class UnscaledDecimal16Func extends Decimal16Function implements BinaryFunction {
        private final Function arg0;
        private final Function arg1;

        public UnscaledDecimal16Func(Function arg0, Function arg1) {
            super(arg0.getType());
            this.arg0 = arg0;
            this.arg1 = arg1;
        }

        @Override
        public short getDecimal16(Record rec) {
            var left = arg0.getDecimal16(rec);
            var right = arg1.getDecimal16(rec);
            return left == right ? Decimals.DECIMAL16_NULL : left;
        }

        @Override
        public Function getLeft() {
            return arg0;
        }

        @Override
        public Function getRight() {
            return arg1;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg0).val(',').val(arg1).val(')');
        }
    }

    private static class UnscaledDecimal256Func extends Decimal256Function implements BinaryFunction {
        private final Function arg0;
        private final Function arg1;
        private final Decimal256 decimalRight = new Decimal256();

        public UnscaledDecimal256Func(Function arg0, Function arg1) {
            super(arg0.getType());
            this.arg0 = arg0;
            this.arg1 = arg1;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            arg0.getDecimal256(rec, sink);
            arg1.getDecimal256(rec, decimalRight);
            if (Decimal256.compare(sink, decimalRight) == 0) {
                sink.ofRawNull();
            }
        }

        @Override
        public Function getLeft() {
            return arg0;
        }

        @Override
        public Function getRight() {
            return arg1;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg0).val(',').val(arg1).val(')');
        }
    }

    private static class UnscaledDecimal32Func extends Decimal32Function implements BinaryFunction {
        private final Function arg0;
        private final Function arg1;

        public UnscaledDecimal32Func(Function arg0, Function arg1) {
            super(arg0.getType());
            this.arg0 = arg0;
            this.arg1 = arg1;
        }

        @Override
        public int getDecimal32(Record rec) {
            var left = arg0.getDecimal32(rec);
            var right = arg1.getDecimal32(rec);
            return left == right ? Decimals.DECIMAL32_NULL : left;
        }

        @Override
        public Function getLeft() {
            return arg0;
        }

        @Override
        public Function getRight() {
            return arg1;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg0).val(',').val(arg1).val(')');
        }
    }

    private static class UnscaledDecimal64Func extends Decimal64Function implements BinaryFunction {
        private final Function arg0;
        private final Function arg1;

        public UnscaledDecimal64Func(Function arg0, Function arg1) {
            super(arg0.getType());
            this.arg0 = arg0;
            this.arg1 = arg1;
        }

        @Override
        public long getDecimal64(Record rec) {
            var left = arg0.getDecimal64(rec);
            var right = arg1.getDecimal64(rec);
            return left == right ? Decimals.DECIMAL64_NULL : left;
        }

        @Override
        public Function getLeft() {
            return arg0;
        }

        @Override
        public Function getRight() {
            return arg1;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg0).val(',').val(arg1).val(')');
        }
    }

    private static class UnscaledDecimal8Func extends Decimal8Function implements BinaryFunction {
        private final Function arg0;
        private final Function arg1;

        public UnscaledDecimal8Func(Function arg0, Function arg1) {
            super(arg0.getType());
            this.arg0 = arg0;
            this.arg1 = arg1;
        }

        @Override
        public byte getDecimal8(Record rec) {
            var left = arg0.getDecimal8(rec);
            var right = arg1.getDecimal8(rec);
            return left == right ? Decimals.DECIMAL8_NULL : left;
        }

        @Override
        public Function getLeft() {
            return arg0;
        }

        @Override
        public Function getRight() {
            return arg1;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(arg0).val(',').val(arg1).val(')');
        }
    }
}
