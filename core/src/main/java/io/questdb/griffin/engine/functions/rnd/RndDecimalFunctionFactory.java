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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndDecimalFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "rnd_decimal(iii)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int precision = args.getQuick(0).getInt(null);
        final int scale = args.getQuick(1).getInt(null);
        final int nullRate = args.getQuick(2).getInt(null);

        if (nullRate < 0) {
            throw SqlException.$(argPositions.getQuick(2), "invalid NULL rate");
        }

        if (scale >= 0 && precision > 0 && precision >= scale && precision <= Decimals.MAX_PRECISION) {
            final int decimalType = ColumnType.getDecimalType(precision, scale);
            return switch (ColumnType.tagOf(decimalType)) {
                case ColumnType.DECIMAL8 -> new Decimal8Func(decimalType, nullRate);
                case ColumnType.DECIMAL16 -> new Decimal16Func(decimalType, nullRate);
                case ColumnType.DECIMAL32 -> new Decimal32Func(decimalType, nullRate);
                case ColumnType.DECIMAL64 -> new Decimal64Func(decimalType, nullRate);
                case ColumnType.DECIMAL128 -> new Decimal128Func(decimalType, nullRate);
                default -> new Decimal256Func(decimalType, nullRate);
            };
        }

        throw SqlException.$(position, "invalid precision and scale: ").put(precision).put(", ").put(scale);
    }

    private static abstract class BaseFunc extends DecimalFunction {
        protected final int nanRate;
        protected Rnd rnd;

        public BaseFunc(int type, int nanRate) {
            super(type);
            this.nanRate = nanRate + 1;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getDecimal16(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getDecimal32(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDecimal64(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte getDecimal8(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isRandom() {
            return true;
        }

        @Override
        public boolean shouldMemoize() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_decimal(")
                    .val(ColumnType.getDecimalPrecision(type))
                    .val(',').val(ColumnType.getDecimalScale(type))
                    .val(',').val(nanRate - 1)
                    .val(')');
        }
    }

    private static class Decimal128Func extends BaseFunc {
        private final long highRange;

        public Decimal128Func(int type, int nanRate) {
            super(type, nanRate);
            final int highPrecision = ColumnType.getDecimalPrecision(type) - Numbers.getPrecision(Long.MAX_VALUE) - 1;
            this.highRange = highPrecision > 0 ? Numbers.getMaxValue(highPrecision) : 0;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if ((rnd.nextInt() % nanRate) == 1) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(
                        highRange > 0 ? rnd.nextPositiveLong() % highRange : 0,
                        rnd.nextLong()
                );
            }
        }
    }

    private static class Decimal16Func extends BaseFunc {
        private final short range;

        public Decimal16Func(int type, int nanRate) {
            super(type, nanRate);
            this.range = (short) Numbers.getMaxValue(ColumnType.getDecimalPrecision(type));
        }

        @Override
        public short getDecimal16(Record rec) {
            if ((rnd.nextInt() % nanRate) == 1) {
                return Decimals.DECIMAL16_NULL;
            }
            return (short) (rnd.nextPositiveInt() % range);
        }
    }

    private static class Decimal256Func extends BaseFunc {
        private final long hhRange;
        private final long hlRange;

        public Decimal256Func(int type, int nanRate) {
            super(type, nanRate);
            final int maxLongPrecision = Numbers.getPrecision(Long.MAX_VALUE);
            final int precision = ColumnType.getDecimalPrecision(type);
            final int hhPrecision = Math.max(precision - 3 * maxLongPrecision - 1, 0);
            this.hhRange = hhPrecision > 0 ? Numbers.getMaxValue(hhPrecision) : 0;
            final int hlPrecision = Math.max(precision - 2 * maxLongPrecision - 1, 0);
            hlRange = hlPrecision > 0 ? Numbers.getMaxValue(hlPrecision) : 0;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            if ((rnd.nextInt() % nanRate) == 1) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(
                        hhRange > 0 ? rnd.nextPositiveLong() % hhRange : 0,
                        hlRange > 0 ? rnd.nextPositiveLong() % hlRange : 0,
                        rnd.nextLong(),
                        rnd.nextLong()
                );
            }
        }
    }

    private static class Decimal32Func extends BaseFunc {
        private final int range;

        public Decimal32Func(int type, int nanRate) {
            super(type, nanRate);
            this.range = (int) Numbers.getMaxValue(ColumnType.getDecimalPrecision(type));
        }

        @Override
        public int getDecimal32(Record rec) {
            if ((rnd.nextInt() % nanRate) == 1) {
                return Decimals.DECIMAL32_NULL;
            }
            return rnd.nextPositiveInt() % range;
        }
    }

    private static class Decimal64Func extends BaseFunc {
        private final long range;

        public Decimal64Func(int type, int nanRate) {
            super(type, nanRate);
            this.range = Numbers.getMaxValue(ColumnType.getDecimalPrecision(type));
        }

        @Override
        public long getDecimal64(Record rec) {
            if ((rnd.nextInt() % nanRate) == 1) {
                return Decimals.DECIMAL64_NULL;
            }
            return rnd.nextPositiveLong() % range;
        }
    }

    private static class Decimal8Func extends BaseFunc {
        private final byte range;

        public Decimal8Func(int type, int nanRate) {
            super(type, nanRate);
            this.range = (byte) Numbers.getMaxValue(ColumnType.getDecimalPrecision(type));
        }

        @Override
        public byte getDecimal8(Record rec) {
            if ((rnd.nextInt() % nanRate) == 1) {
                return Decimals.DECIMAL8_NULL;
            }
            return (byte) (rnd.nextPositiveInt() % range);
        }
    }
}
