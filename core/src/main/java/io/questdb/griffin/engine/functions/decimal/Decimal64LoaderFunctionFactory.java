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

public final class Decimal64LoaderFunctionFactory {
    /**
     * Returns a function that loads values from a source function into a DECIMAL64 type.
     * This factory creates specialized loader functions for each supported source type,
     * converting the raw values to 64-bit decimal representation.
     * <p>
     * For decimal source types (DECIMAL8/16/32/128/256), the raw bits are extracted and
     * sign-extended as needed to fill 64 bits. For integer types (BYTE/SHORT/INT/LONG)
     * and temporal types (DATE/TIMESTAMP), values are converted with NULL checking where
     * applicable.
     * <p>
     * When the source is already DECIMAL64, the function is returned as-is.
     *
     * @param from the source function to load values from
     * @return a function that performs the type conversion to DECIMAL64 or the original function
     */
    public static Function getInstance(Function from) {
        return switch (ColumnType.tagOf(from.getType())) {
            case ColumnType.DECIMAL8 -> new FuncDecimal8(from);
            case ColumnType.DECIMAL16 -> new FuncDecimal16(from);
            case ColumnType.DECIMAL32 -> new FuncDecimal32(from);
            case ColumnType.DECIMAL64 -> from;
            case ColumnType.DECIMAL128 -> new FuncDecimal128(from);
            case ColumnType.DECIMAL256 -> new FuncDecimal256(from);
            case ColumnType.BYTE -> new FuncByte(from);
            case ColumnType.SHORT -> new FuncShort(from);
            case ColumnType.INT -> new FuncInt(from);
            case ColumnType.LONG -> new FuncLong(from);
            case ColumnType.DATE -> new FuncDate(from);
            case ColumnType.TIMESTAMP -> new FuncTimestamp(from);
            default -> from;
        };
    }

    private static int buildDecimalType(int fromDecimalType) {
        int precision = ColumnType.getDecimalPrecision(fromDecimalType);
        int scale = ColumnType.getDecimalScale(fromDecimalType);
        return ColumnType.getDecimalType(ColumnType.DECIMAL64, precision, scale);
    }

    private static class FuncByte extends Decimal64Function implements UnaryFunction {
        private final Function arg;

        public FuncByte(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL64, 3, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
            return arg.getByte(rec);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDate extends Decimal64Function implements UnaryFunction {
        private final Function arg;

        public FuncDate(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL64, 19, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
            long v = arg.getDate(rec);
            if (v == Numbers.LONG_NULL) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return v;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDecimal128 extends Decimal64Function implements UnaryFunction {
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
        public long getDecimal64(Record rec) {
            arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return decimal128.getLow();
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

    private static class FuncDecimal16 extends Decimal64Function implements UnaryFunction {
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
        public long getDecimal64(Record rec) {
            short v = arg.getDecimal16(rec);
            if (v == Decimals.DECIMAL16_NULL) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return v;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDecimal256 extends Decimal64Function implements UnaryFunction {
        private final Function arg;
        private final Decimal256 decimal256 = new Decimal256();

        public FuncDecimal256(Function arg) {
            super(buildDecimalType(arg.getType()));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
            arg.getDecimal256(rec, decimal256);
            if (decimal256.isNull()) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return decimal256.getLl();
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

    private static class FuncDecimal32 extends Decimal64Function implements UnaryFunction {
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
        public long getDecimal64(Record rec) {
            int v = arg.getDecimal32(rec);
            if (v == Decimals.DECIMAL32_NULL) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return v;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncDecimal8 extends Decimal64Function implements UnaryFunction {
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
        public long getDecimal64(Record rec) {
            byte v = arg.getDecimal8(rec);
            if (v == Decimals.DECIMAL8_NULL) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return v;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncInt extends Decimal64Function implements UnaryFunction {
        private final Function arg;

        public FuncInt(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL64, 10, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
            int v = arg.getInt(rec);
            if (v == Numbers.INT_NULL) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return v;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncLong extends Decimal64Function implements UnaryFunction {
        private final Function arg;

        public FuncLong(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL64, 19, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
            long v = arg.getLong(rec);
            if (v == Numbers.LONG_NULL) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return v;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncShort extends Decimal64Function implements UnaryFunction {
        private final Function arg;

        public FuncShort(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL64, 5, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
            return arg.getShort(rec);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }

    private static class FuncTimestamp extends Decimal64Function implements UnaryFunction {
        private final Function arg;

        public FuncTimestamp(Function arg) {
            super(ColumnType.getDecimalType(ColumnType.DECIMAL64, 19, 0));
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
            long v = arg.getTimestamp(rec);
            if (v == Numbers.LONG_NULL) {
                return Decimals.DECIMAL64_NULL;
            } else {
                return v;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
        }
    }
}
