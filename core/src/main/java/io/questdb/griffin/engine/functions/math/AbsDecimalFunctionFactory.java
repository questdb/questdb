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
import io.questdb.griffin.engine.functions.decimal.Decimal128Function;
import io.questdb.griffin.engine.functions.decimal.Decimal16Function;
import io.questdb.griffin.engine.functions.decimal.Decimal256Function;
import io.questdb.griffin.engine.functions.decimal.Decimal32Function;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.griffin.engine.functions.decimal.Decimal8Function;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class AbsDecimalFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "abs(Ξ)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        var arg = args.getQuick(0);
        final int type = arg.getType();
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8 -> new Decimal8Func(type, arg);
            case ColumnType.DECIMAL16 -> new Decimal16Func(type, arg);
            case ColumnType.DECIMAL32 -> new Decimal32Func(type, arg);
            case ColumnType.DECIMAL64 -> new Decimal64Func(type, arg);
            case ColumnType.DECIMAL128 -> new Decimal128Func(type, arg);
            default -> new Decimal256Func(type, arg);
        };
    }

    private static class Decimal128Func extends Decimal128Function implements UnaryFunction {
        final Function function;

        public Decimal128Func(int type, Function function) {
            super(type);
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            function.getDecimal128(rec, sink);
            if (sink.isNegative()) {
                sink.negate();
            }
        }

        @Override
        public String getName() {
            return "abs";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class Decimal16Func extends Decimal16Function implements UnaryFunction {
        final Function function;

        public Decimal16Func(int type, Function function) {
            super(type);
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public short getDecimal16(Record rec) {
            short value = function.getDecimal16(rec);
            return value < 0 ? (short) -value : value;
        }

        @Override
        public String getName() {
            return "abs";
        }
    }

    private static class Decimal256Func extends Decimal256Function implements UnaryFunction {
        final Function function;

        public Decimal256Func(int type, Function function) {
            super(type);
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            function.getDecimal256(rec, sink);
            if (sink.isNegative()) {
                sink.negate();
            }
        }

        @Override
        public String getName() {
            return "abs";
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class Decimal32Func extends Decimal32Function implements UnaryFunction {
        final Function function;

        public Decimal32Func(int type, Function function) {
            super(type);
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public int getDecimal32(Record rec) {
            int value = function.getDecimal32(rec);
            return value < 0 ? -value : value;
        }

        @Override
        public String getName() {
            return "abs";
        }
    }

    private static class Decimal64Func extends Decimal64Function implements UnaryFunction {
        final Function function;

        public Decimal64Func(int type, Function function) {
            super(type);
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public long getDecimal64(Record rec) {
            long value = function.getDecimal64(rec);
            return value < 0 ? -value : value;
        }

        @Override
        public String getName() {
            return "abs";
        }
    }

    private static class Decimal8Func extends Decimal8Function implements UnaryFunction {
        final Function function;

        public Decimal8Func(int type, Function function) {
            super(type);
            this.function = function;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public byte getDecimal8(Record rec) {
            byte value = function.getDecimal8(rec);
            return value < 0 ? (byte) -value : value;
        }

        @Override
        public String getName() {
            return "abs";
        }
    }
}
