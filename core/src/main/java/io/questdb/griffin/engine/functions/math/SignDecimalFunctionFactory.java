/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal8Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class SignDecimalFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "sign(Ξ)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        var arg = args.getQuick(0);
        final int type = arg.getType();
        // All functions return DECIMAL8 (precision=1, scale=0) but read different input types
        switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8:
                return new Decimal8Func(arg);
            case ColumnType.DECIMAL16:
                return new Decimal16Func(arg);
            case ColumnType.DECIMAL32:
                return new Decimal32Func(arg);
            case ColumnType.DECIMAL64:
                return new Decimal64Func(arg);
            case ColumnType.DECIMAL128:
                return new Decimal128Func(arg);
            default:
                return new Decimal256Func(arg);
        }
    }

    // Function for DECIMAL128 input
    private static class Decimal128Func extends Decimal8Function implements UnaryFunction {
        final Function function;
        private final Decimal128 decimal128 = new Decimal128();

        public Decimal128Func(Function function) {
            super(ColumnType.getDecimalType(1, 0));
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public byte getDecimal8(Record rec) {
            function.getDecimal128(rec, decimal128);

            if (decimal128.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }

            // Return sign: -1, 0, or 1
            if (decimal128.isZero()) {
                return 0;
            } else if (decimal128.isNegative()) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public String getName() {
            return "sign";
        }
    }

    // Function for DECIMAL16 input
    private static class Decimal16Func extends Decimal8Function implements UnaryFunction {
        final Function function;

        public Decimal16Func(Function function) {
            super(ColumnType.getDecimalType(1, 0));
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public byte getDecimal8(Record rec) {
            short value = function.getDecimal16(rec);

            // Check for NULL
            if (value == Decimals.DECIMAL16_NULL) {
                return Decimals.DECIMAL8_NULL;
            }

            // Return sign: -1, 0, or 1
            if (value == 0) {
                return 0;
            } else if (value < 0) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public String getName() {
            return "sign";
        }
    }

    // Function for DECIMAL256 input
    private static class Decimal256Func extends Decimal8Function implements UnaryFunction {
        final Function function;
        private final Decimal256 decimal256 = new Decimal256();

        public Decimal256Func(Function function) {
            super(ColumnType.getDecimalType(1, 0));
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public byte getDecimal8(Record rec) {
            function.getDecimal256(rec, decimal256);
            if (decimal256.isNull()) {
                return Decimals.DECIMAL8_NULL;
            }

            // Return sign: -1, 0, or 1
            if (decimal256.isZero()) {
                return 0;
            } else if (decimal256.isNegative()) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public String getName() {
            return "sign";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    // Function for DECIMAL32 input
    private static class Decimal32Func extends Decimal8Function implements UnaryFunction {
        final Function function;

        public Decimal32Func(Function function) {
            super(ColumnType.getDecimalType(1, 0));
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public byte getDecimal8(Record rec) {
            int value = function.getDecimal32(rec);

            // Check for NULL
            if (value == Decimals.DECIMAL32_NULL) {
                return Decimals.DECIMAL8_NULL;
            }

            // Return sign: -1, 0, or 1
            if (value == 0) {
                return 0;
            } else if (value < 0) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public String getName() {
            return "sign";
        }
    }

    // Function for DECIMAL64 input
    private static class Decimal64Func extends Decimal8Function implements UnaryFunction {
        final Function function;

        public Decimal64Func(Function function) {
            super(ColumnType.getDecimalType(1, 0));
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public byte getDecimal8(Record rec) {
            long value = function.getDecimal64(rec);

            // Check for NULL
            if (value == Decimals.DECIMAL64_NULL) {
                return Decimals.DECIMAL8_NULL;
            }

            // Return sign: -1, 0, or 1
            if (value == 0) {
                return 0;
            } else if (value < 0) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public String getName() {
            return "sign";
        }
    }

    // Function for DECIMAL8 input
    private static class Decimal8Func extends Decimal8Function implements UnaryFunction {
        final Function function;

        public Decimal8Func(Function function) {
            super(ColumnType.getDecimalType(1, 0));
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public byte getDecimal8(Record rec) {
            byte value = function.getDecimal8(rec);

            // Check for NULL
            if (value == Decimals.DECIMAL8_NULL) {
                return Decimals.DECIMAL8_NULL;
            }

            // Return sign: -1, 0, or 1
            if (value == 0) {
                return 0;
            } else if (value < 0) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public String getName() {
            return "sign";
        }
    }
}