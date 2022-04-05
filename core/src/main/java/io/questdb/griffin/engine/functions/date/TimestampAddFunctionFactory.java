/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;

public class TimestampAddFunctionFactory implements FunctionFactory {

    private static final ObjList<LongAddIntFunction> addFunctions = new ObjList<>();
    private static final int addFunctionsMax;

    @Override
    public String getSignature() {
        return "dateadd(AIN)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {

        Function period = args.getQuick(0);
        Function interval = args.getQuick(1);
        if (period.isConstant()) {
            char periodValue = period.getChar(null);
            if (periodValue < addFunctionsMax) {
                LongAddIntFunction func = addFunctions.getQuick(periodValue);
                if (func != null) {
                    if (interval.isConstant()) {
                        if (interval.getInt(null) != Numbers.INT_NaN) {
                            return new AddLongIntVarConstFunction(args.getQuick(2), interval.getInt(null), func);
                        }
                        return TimestampConstant.NULL;
                    }
                    return new AddLongIntVarVarFunction(args.getQuick(2), args.getQuick(1), func);
                }
            }
            return TimestampConstant.NULL;
        }
        return new DateAddFunc(args.getQuick(2), args.getQuick(0), args.getQuick(1));
    }

    @FunctionalInterface
    private interface LongAddIntFunction {
        long add(long a, int b);
    }

    private static class AddLongIntVarVarFunction extends TimestampFunction implements BinaryFunction {
        private final Function left;
        private final Function right;
        private final LongAddIntFunction func;

        public AddLongIntVarVarFunction(Function left, Function right, LongAddIntFunction func) {
            this.left = left;
            this.right = right;
            this.func = func;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return func.add(l, r);
        }

        @Override
        public boolean isRuntimeConstant() {
            return false;
        }
    }

    private static class AddLongIntVarConstFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;
        private final int interval;
        private final LongAddIntFunction func;

        public AddLongIntVarConstFunction(Function left, int right, LongAddIntFunction func) {
            this.arg = left;
            this.interval = right;
            this.func = func;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long l = arg.getTimestamp(rec);
            if (l == Numbers.LONG_NaN) {
                return Numbers.LONG_NaN;
            }
            return func.add(l, interval);
        }

        @Override
        public boolean isRuntimeConstant() {
            return false;
        }
    }

    private static class DateAddFunc extends TimestampFunction implements TernaryFunction {
        final Function left;
        final Function center;
        final Function right;

        public DateAddFunc(Function left, Function center, Function right) {
            this.left = left;
            this.center = center;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getCenter() {
            return center;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final char c = center.getChar(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.addPeriod(l, c, r);
        }
    }

    static {
        addFunctions.extendAndSet('s', Timestamps::addSeconds);
        addFunctions.extendAndSet('m', Timestamps::addMinutes);
        addFunctions.extendAndSet('h', Timestamps::addHours);
        addFunctions.extendAndSet('d', Timestamps::addDays);
        addFunctions.extendAndSet('w', Timestamps::addWeeks);
        addFunctions.extendAndSet('M', Timestamps::addMonths);
        addFunctions.extendAndSet('y', Timestamps::addYear);
        addFunctionsMax = addFunctions.size();
    }
}
