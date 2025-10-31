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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
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
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class NegDecimalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "-(Ξ)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function arg = args.getQuick(0);
        final int type = arg.getType();

        return switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8 -> new Decimal8Func(arg);
            case ColumnType.DECIMAL16 -> new Decimal16Func(arg);
            case ColumnType.DECIMAL32 -> new Decimal32Func(arg);
            case ColumnType.DECIMAL64 -> new Decimal64Func(arg);
            case ColumnType.DECIMAL128 -> new Decimal128Func(arg);
            default -> new Decimal256Func(arg);
        };
    }

    private static class Decimal128Func extends Decimal128Function implements UnaryFunction {
        private final Function arg;

        public Decimal128Func(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            arg.getDecimal128(rec, sink);
            sink.negate();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('-').val(arg);
        }
    }

    private static class Decimal16Func extends Decimal16Function implements UnaryFunction {
        private final Function arg;

        public Decimal16Func(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public short getDecimal16(Record rec) {
            final short argValue = arg.getDecimal16(rec);
            if (argValue != Decimals.DECIMAL16_NULL) {
                return (short) -argValue;
            }
            return Decimals.DECIMAL16_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('-').val(arg);
        }
    }

    private static class Decimal256Func extends Decimal256Function implements UnaryFunction {
        private final Function arg;

        public Decimal256Func(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            arg.getDecimal256(rec, sink);
            sink.negate();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('-').val(arg);
        }
    }

    private static class Decimal32Func extends Decimal32Function implements UnaryFunction {
        private final Function arg;

        public Decimal32Func(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getDecimal32(Record rec) {
            final int argValue = arg.getDecimal32(rec);
            if (argValue != Decimals.DECIMAL32_NULL) {
                return -argValue;
            }
            return Decimals.DECIMAL32_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('-').val(arg);
        }
    }

    private static class Decimal64Func extends Decimal64Function implements UnaryFunction {
        private final Function arg;

        public Decimal64Func(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
            final long argValue = arg.getDecimal64(rec);
            if (!Decimal64.isNull(argValue)) {
                return -argValue;
            }
            return Decimals.DECIMAL64_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('-').val(arg);
        }
    }

    private static class Decimal8Func extends Decimal8Function implements UnaryFunction {
        private final Function arg;

        public Decimal8Func(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public byte getDecimal8(Record rec) {
            final byte argValue = arg.getDecimal8(rec);
            if (argValue != Decimals.DECIMAL8_NULL) {
                return (byte) -argValue;
            }
            return Decimals.DECIMAL8_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('-').val(arg);
        }
    }
}
