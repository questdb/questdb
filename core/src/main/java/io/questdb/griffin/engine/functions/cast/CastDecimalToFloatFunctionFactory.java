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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.decimal.Decimal64LoaderFunctionFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class CastDecimalToFloatFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Ξf)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arg = args.getQuick(0);
        return switch (ColumnType.tagOf(arg.getType())) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    new CastDecimalToFloatFunctionFactory.Func64(Decimal64LoaderFunctionFactory.getInstance(arg));
            case ColumnType.DECIMAL128 -> new CastDecimalToFloatFunctionFactory.Func128(arg);
            default -> new CastDecimalToFloatFunctionFactory.Func(arg);
        };
    }

    private static class Func extends AbstractCastToFloatFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sink = new StringSink();

        public Func(Function value) {
            super(value);
            int type = value.getType();
            fromScale = ColumnType.getDecimalScale(type);
            fromPrecision = ColumnType.getDecimalPrecision(type);
        }

        public float getFloat(Record rec) {
            arg.getDecimal256(rec, decimal256);
            if (decimal256.isNull()) {
                return Float.NaN;
            }
            sink.clear();
            Decimal256.toSink(
                    sink,
                    decimal256.getHh(),
                    decimal256.getHl(),
                    decimal256.getLh(),
                    decimal256.getLl(),
                    fromScale,
                    fromPrecision
            );
            return Numbers.parseFloat(sink);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class Func128 extends AbstractCastToFloatFunction {
        private final Decimal128 decimal128 = new Decimal128();
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sink = new StringSink();

        public Func128(Function value) {
            super(value);
            int type = arg.getType();
            this.fromScale = ColumnType.getDecimalScale(type);
            this.fromPrecision = ColumnType.getDecimalPrecision(type);
        }

        public float getFloat(Record rec) {
            arg.getDecimal128(rec, decimal128);
            if (decimal128.isNull()) {
                return Float.NaN;
            }
            sink.clear();
            Decimal128.toSink(sink, decimal128.getHigh(), decimal128.getLow(), fromScale, fromPrecision);
            return Numbers.parseFloat(sink);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class Func64 extends AbstractCastToFloatFunction {
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sink = new StringSink();

        public Func64(Function value) {
            super(value);
            int type = arg.getType();
            this.fromPrecision = ColumnType.getDecimalPrecision(type);
            this.fromScale = ColumnType.getDecimalScale(type);
        }

        public float getFloat(Record rec) {
            long v = arg.getDecimal64(rec);
            if (Decimal64.isNull(v)) {
                return Float.NaN;
            }
            sink.clear();
            Decimal64.toSink(sink, v, fromScale, fromPrecision);
            return Numbers.parseFloat(sink);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
