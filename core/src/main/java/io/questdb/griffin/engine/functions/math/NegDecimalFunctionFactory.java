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
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
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
        final int precision = ColumnType.getDecimalPrecision(type);

        switch (Decimals.getStorageSizePow2(precision)) {
            case 0:
                return new Decimal8Func(arg);
            case 1:
                return new Decimal16Func(arg);
            case 2:
                return new Decimal32Func(arg);
            case 3:
                return new Decimal64Func(arg);
            case 4:
                return new Decimal128Func(arg);
            default:
                return new Decimal256Func(arg);
        }
    }

    private static class Decimal128Func extends DecimalFunction implements UnaryFunction {
        private final Function arg;
        private long low;

        public Decimal128Func(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal128Hi(Record rec) {
            final long argHigh = arg.getDecimal128Hi(rec);
            final long argLow = arg.getDecimal128Lo(rec);
            long high;
            if (!Decimal128.isNull(argHigh, argLow)) {
                low = ~argLow + 1;
                high = ~argHigh + (low == 0 ? 1 : 0);
            } else {
                high = Decimals.DECIMAL128_HI_NULL;
                low = Decimals.DECIMAL128_LO_NULL;
            }
            return high;
        }

        @Override
        public long getDecimal128Lo(Record rec) {
            return low;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('-').val(arg);
        }
    }

    private static class Decimal16Func extends DecimalFunction implements UnaryFunction {
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

    private static class Decimal256Func extends DecimalFunction implements UnaryFunction {
        private final Function arg;
        private long hl;
        private long lh;
        private long ll;

        public Decimal256Func(Function arg) {
            super(arg.getType());
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal256HH(Record rec) {
            final long argHh = arg.getDecimal256HH(rec);
            final long argHl = arg.getDecimal256HL(rec);
            final long argLh = arg.getDecimal256LH(rec);
            final long argLl = arg.getDecimal256LL(rec);
            long hh;
            if (!Decimal256.isNull(argHh, argHl, argLh, argLl)) {
                ll = ~argLl + 1;
                long c = ll == 0L ? 1L : 0L;
                lh = ~argLh + c;
                c = (c == 1L && lh == 0L) ? 1L : 0L;
                hl = ~argHl + c;
                c = (c == 1L && hl == 0L) ? 1L : 0L;
                hh = ~argHh + c;
            } else {
                hh = Decimals.DECIMAL256_HH_NULL;
                hl = Decimals.DECIMAL256_HL_NULL;
                lh = Decimals.DECIMAL256_LH_NULL;
                ll = Decimals.DECIMAL256_LL_NULL;
            }
            return hh;
        }

        @Override
        public long getDecimal256HL(Record rec) {
            return hl;
        }

        @Override
        public long getDecimal256LH(Record rec) {
            return lh;
        }

        @Override
        public long getDecimal256LL(Record rec) {
            return ll;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('-').val(arg);
        }
    }

    private static class Decimal32Func extends DecimalFunction implements UnaryFunction {
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

    private static class Decimal64Func extends DecimalFunction implements UnaryFunction {
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

    private static class Decimal8Func extends DecimalFunction implements UnaryFunction {
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
