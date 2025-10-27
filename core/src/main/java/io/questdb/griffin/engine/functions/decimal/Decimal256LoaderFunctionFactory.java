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

package io.questdb.griffin.engine.functions.decimal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;

public final class Decimal256LoaderFunctionFactory {
    /**
     * Returns a function that loads values from a source function into a DECIMAL256 type.
     * This factory creates specialized loader functions for each supported source type,
     * converting the raw values to 256-bit decimal representation.
     * <p>
     * For smaller decimal types (DECIMAL8/16/32/64/128), the raw bits are extracted and
     * sign-extended to fill the full 256 bits. For integer types (BYTE/SHORT/INT/LONG)
     * and temporal types (DATE/TIMESTAMP), values are converted with NULL checking where
     * applicable.
     * <p>
     * When the source is already DECIMAL256, the function is returned as-is.
     *
     * @param from the source function to load values from
     * @return a function that performs the type conversion to DECIMAL256 or the original function
     */
    public static Function getInstance(Function from) {
        return switch (ColumnType.tagOf(from.getType())) {
            case ColumnType.DECIMAL8 -> new FuncDecimal8(from);
            case ColumnType.DECIMAL16 -> new FuncDecimal16(from);
            case ColumnType.DECIMAL32 -> new FuncDecimal32(from);
            case ColumnType.DECIMAL64 -> new FuncDecimal64(from);
            case ColumnType.DECIMAL128 -> new FuncDecimal128(from);
            case ColumnType.DECIMAL256 -> from;
            case ColumnType.BYTE -> new FuncByte(from);
            case ColumnType.SHORT -> new FuncShort(from);
            case ColumnType.INT -> new FuncInt(from);
            case ColumnType.LONG -> new FuncLong(from);
            case ColumnType.DATE -> new FuncDate(from);
            case ColumnType.TIMESTAMP -> new FuncTimestamp(from);
            default -> from;
        };
    }

    /**
     * Returns the parent function of the given function if it is a loader function.
     * Otherwise, returns the given function.
     */
    public static Function getParent(Function function) {
        var clazz = function.getClass();
        if (clazz.isMemberClass() && clazz.getEnclosingClass() == Decimal256LoaderFunctionFactory.class) {
            return ((UnaryFunction) function).getArg();
        }
        return function;
    }

    private static int buildDecimalType(int fromDecimalType) {
        int precision = ColumnType.getDecimalPrecision(fromDecimalType);
        int scale = ColumnType.getDecimalScale(fromDecimalType);
        return ColumnType.getDecimalType(ColumnType.DECIMAL256, precision, scale);
    }

    private static class FuncByte extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncByte(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL256, 3, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            byte v = arg.getByte(rec);
            sink.ofRaw(v);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDate extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncDate(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL256, 19, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            long v = arg.getDate(rec);
            if (v == Numbers.LONG_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(v);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDecimal128 extends Decimal256Function implements UnaryFunction {
        private final Function arg;
        private final Decimal128 decimal128 = new Decimal128();

        public FuncDecimal128(Function arg) {
            super(buildDecimalType(arg.getType()));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(decimal128.getHigh(), decimal128.getLow());
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDecimal16 extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncDecimal16(Function arg) {
            super(buildDecimalType(arg.getType()));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            short v = arg.getDecimal16(rec);
            if (v == Decimals.DECIMAL16_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(v);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDecimal32 extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncDecimal32(Function arg) {
            super(buildDecimalType(arg.getType()));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            int v = arg.getDecimal32(rec);
            if (v == Decimals.DECIMAL32_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(v);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDecimal64 extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncDecimal64(Function arg) {
            super(buildDecimalType(arg.getType()));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            long v = arg.getDecimal64(rec);
            if (v == Decimals.DECIMAL64_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(v);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDecimal8 extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncDecimal8(Function arg) {
            super(buildDecimalType(arg.getType()));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            byte v = arg.getDecimal8(rec);
            if (v == Decimals.DECIMAL8_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(v);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncInt extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncInt(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL256, 10, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            int v = arg.getInt(rec);
            if (v == Numbers.INT_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(v);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncLong extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncLong(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL256, 19, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            long v = arg.getLong(rec);
            if (v == Numbers.LONG_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(v);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncShort extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncShort(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL256, 5, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            short v = arg.getShort(rec);
            sink.ofRaw(v);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncTimestamp extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public FuncTimestamp(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL256, 19, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            long v = arg.getTimestamp(rec);
            if (v == Numbers.LONG_NULL) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(v);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }
}
