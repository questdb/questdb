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
import io.questdb.griffin.engine.functions.Decimal128Function;
import io.questdb.griffin.engine.functions.Decimal16Function;
import io.questdb.griffin.engine.functions.Decimal256Function;
import io.questdb.griffin.engine.functions.Decimal32Function;
import io.questdb.griffin.engine.functions.Decimal64Function;
import io.questdb.griffin.engine.functions.Decimal8Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
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
        switch (ColumnType.tagOf(type)) {
            case ColumnType.DECIMAL8:
                return new Decimal8Func(type, arg);
            case ColumnType.DECIMAL16:
                return new Decimal16Func(type, arg);
            case ColumnType.DECIMAL32:
                return new Decimal32Func(type, arg);
            case ColumnType.DECIMAL64:
                return new Decimal64Func(type, arg);
            case ColumnType.DECIMAL128:
                return new Decimal128Func(type, arg);
            default:
                return new Decimal256Func(type, arg);
        }
    }

    private static class Decimal128Func extends Decimal128Function implements UnaryFunction {
        final Decimal128 decimal128 = new Decimal128();
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
        public long getDecimal128Hi(Record rec) {
            long hi = function.getDecimal128Hi(rec);
            long lo = function.getDecimal128Lo(rec);
            decimal128.of(hi, lo, 0);
            if (decimal128.isNegative()) {
                decimal128.negate();
            }
            return decimal128.getHigh();
        }

        @Override
        public long getDecimal128Lo(Record rec) {
            return decimal128.getLow();
        }

        @Override
        public String getName() {
            return "abs";
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
        final Decimal256 decimal256 = new Decimal256();
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
        public long getDecimal256HH(Record rec) {
            long hh = function.getDecimal256HH(rec);
            long hl = function.getDecimal256HL(rec);
            long lh = function.getDecimal256LH(rec);
            long ll = function.getDecimal256LL(rec);
            decimal256.of(hh, hl, lh, ll, 0);
            if (decimal256.isNegative()) {
                decimal256.negate();
            }
            return decimal256.getHh();
        }

        @Override
        public long getDecimal256HL(Record rec) {
            return decimal256.getHl();
        }

        @Override
        public long getDecimal256LH(Record rec) {
            return decimal256.getLh();
        }

        @Override
        public long getDecimal256LL(Record rec) {
            return decimal256.getLl();
        }


        @Override
        public String getName() {
            return "abs";
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
