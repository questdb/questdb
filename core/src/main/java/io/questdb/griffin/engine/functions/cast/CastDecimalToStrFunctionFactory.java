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
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.decimal.Decimal64LoaderFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class CastDecimalToStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Ξs)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            Decimal256 d = sqlExecutionContext.getDecimal256();
            Decimal128 decimal128 = sqlExecutionContext.getDecimal128();
            DecimalUtil.load(d, decimal128, arg, null);
            if (d.isNull()) {
                return StrConstant.NULL;
            }
            final StringSink sink = Misc.getThreadLocalSink();
            sink.put(d);
            return new StrConstant(Chars.toString(sink));
        }
        int tag = ColumnType.tagOf(arg.getType());
        return switch (tag) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    new Func64(Decimal64LoaderFunctionFactory.getInstance(arg));
            case ColumnType.DECIMAL128 -> new Func128(arg);
            case ColumnType.DECIMAL256 -> new Func(arg);
            default -> throw SqlException.$(position, "invalid type for cast to string: " + ColumnType.nameOf(tag));
        };
    }

    public static class Func extends AbstractCastToStrFunction {
        private final Decimal256 decimal256 = new Decimal256();
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public Func(Function arg) {
            super(arg);
            int type = arg.getType();
            this.fromScale = ColumnType.getDecimalScale(type);
            this.fromPrecision = ColumnType.getDecimalPrecision(type);
        }

        @Override
        public CharSequence getStrA(Record rec) {
            arg.getDecimal256(rec, decimal256);
            if (!decimal256.isNull()) {
                sinkA.clear();
                Decimal256.toSink(
                        sinkA,
                        decimal256.getHh(),
                        decimal256.getHl(),
                        decimal256.getLh(),
                        decimal256.getLl(),
                        fromScale,
                        fromPrecision
                );
                return sinkA;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            arg.getDecimal256(rec, decimal256);
            if (!decimal256.isNull()) {
                sinkB.clear();
                Decimal256.toSink(
                        sinkB,
                        decimal256.getHh(),
                        decimal256.getHl(),
                        decimal256.getLh(),
                        decimal256.getLl(),
                        fromScale,
                        fromPrecision
                );
                return sinkB;
            }
            return null;
        }
    }

    public static class Func128 extends AbstractCastToStrFunction {
        private final Decimal128 decimal128 = new Decimal128();
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public Func128(Function arg) {
            super(arg);
            int type = arg.getType();
            this.fromScale = ColumnType.getDecimalScale(type);
            this.fromPrecision = ColumnType.getDecimalPrecision(type);
        }

        @Override
        public CharSequence getStrA(Record rec) {
            arg.getDecimal128(rec, decimal128);
            if (!decimal128.isNull()) {
                sinkA.clear();
                Decimal128.toSink(sinkA, decimal128.getHigh(), decimal128.getLow(), fromScale, fromPrecision);
                return sinkA;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            arg.getDecimal128(rec, decimal128);
            if (!decimal128.isNull()) {
                sinkB.clear();
                Decimal128.toSink(sinkB, decimal128.getHigh(), decimal128.getLow(), fromScale, fromPrecision);
                return sinkB;
            }
            return null;
        }
    }

    public static class Func64 extends AbstractCastToStrFunction {
        private final int fromPrecision;
        private final int fromScale;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public Func64(Function arg) {
            super(arg);
            int type = arg.getType();
            this.fromPrecision = ColumnType.getDecimalPrecision(type);
            this.fromScale = ColumnType.getDecimalScale(type);
        }

        @Override
        public CharSequence getStrA(Record rec) {
            long v = arg.getDecimal64(rec);
            if (!Decimal64.isNull(v)) {
                sinkA.clear();
                Decimal64.toSink(sinkA, v, fromScale, fromPrecision);
                return sinkA;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            long v = arg.getDecimal64(rec);
            if (!Decimal64.isNull(v)) {
                sinkB.clear();
                Decimal64.toSink(sinkB, v, fromScale, fromPrecision);
                return sinkB;
            }
            return null;
        }
    }
}
