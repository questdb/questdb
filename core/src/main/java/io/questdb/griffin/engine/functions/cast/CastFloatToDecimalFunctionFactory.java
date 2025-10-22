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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class CastFloatToDecimalFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(Fξ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final int targetType = args.getQuick(1).getType();
        return switch (ColumnType.tagOf(targetType)) {
            case ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32, ColumnType.DECIMAL64 ->
                    new Func64(args.getQuick(0), targetType, argPositions.getQuick(0));
            case ColumnType.DECIMAL128 -> new Func128(args.getQuick(0), targetType, argPositions.getQuick(0));
            default -> new Func(args.getQuick(0), targetType, argPositions.getQuick(0));
        };
    }

    private static class Func extends AbstractCastToDecimalFunction {
        private final StringSink sink = new StringSink();

        public Func(Function value, int targetType, int position) {
            super(value, targetType, position);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        protected boolean cast(Record rec) {
            float f = this.arg.getFloat(rec);
            if (Numbers.isNull(f)) {
                return false;
            }
            sink.clear();
            sink.put(f);
            try {
                decimal.ofString(sink, 0, sink.length(), precision, scale, false, true);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(sink, ColumnType.FLOAT, type).position(position);
            }
            return true;
        }
    }

    private static class Func128 extends AbstractCastToDecimal128Function {
        private final StringSink sink = new StringSink();

        public Func128(Function value, int targetType, int position) {
            super(value, targetType, position);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        protected boolean cast(Record rec) {
            float f = this.arg.getFloat(rec);
            if (Numbers.isNull(f)) {
                return false;
            }
            sink.clear();
            sink.put(f);
            try {
                decimal.ofString(sink, 0, sink.length(), precision, scale, false, true);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(sink, ColumnType.FLOAT, type).position(position);
            }
            return true;
        }
    }

    private static class Func64 extends AbstractCastToDecimal64Function {
        private final StringSink sink = new StringSink();

        public Func64(Function value, int targetType, int position) {
            super(value, targetType, position);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        protected boolean cast(Record rec) {
            float f = this.arg.getFloat(rec);
            if (Numbers.isNull(f)) {
                return false;
            }
            sink.clear();
            sink.put(f);
            try {
                decimal.ofString(sink, 0, sink.length(), precision, scale, false, true);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(sink, ColumnType.FLOAT, type).position(position);
            }
            return true;
        }
    }
}
