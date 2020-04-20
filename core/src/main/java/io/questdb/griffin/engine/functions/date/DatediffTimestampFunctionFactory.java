/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.microtime.Timestamps;

public class DatediffTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "datediff(ANN)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {

        Function period = args.getQuick(0);
        if (period.isConstant()) {
            Function start = args.getQuick(1);
            Function end = args.getQuick(2);
            char periodValue = period.getChar(null);
            if (periodValue == 's') {
                if (start.isConstant() && start.getTimestamp(null) != Numbers.LONG_NaN) {
                    long startValue = start.getLong(null);
                    return new DateDiffFuncSecondConstantTimestampConstant(position, args.getQuick(2), startValue);
                }
                if (end.isConstant() && end.getTimestamp(null) != Numbers.LONG_NaN) {
                    long endValue = end.getLong(null);
                    return new DateDiffFuncSecondConstantTimestampConstant(position, args.getQuick(1), endValue);
                }
                return new DateDiffFuncSecondConstant(position, args.getQuick(1), args.getQuick(2));
            }
            return new TimestampConstant(position, Numbers.LONG_NaN);
        }
        return new DateDiffFunc(position, args.getQuick(0), args.getQuick(1), args.getQuick(2));
}


    private static class DateDiffFuncSecondConstant extends LongFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public DateDiffFuncSecondConstant(int position, Function left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
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
        public long getLong(Record rec) {
            final long l = left.getTimestamp(rec);
            final long r = right.getTimestamp(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.LONG_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.getPeriodBetween('s', l, r);
        }
    }

    private static class DateDiffFuncSecondConstantTimestampConstant extends LongFunction implements UnaryFunction {
        private final Function arg;
        private final long constantTime;

        public DateDiffFuncSecondConstantTimestampConstant(int position, Function left, long right) {
            super(position);
            this.arg = left;
            this.constantTime = right;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong(Record rec) {
            final long l = arg.getTimestamp(rec);
            if (l == Numbers.LONG_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.getPeriodBetween('s', l, constantTime);
        }
    }


private static class DateDiffFunc extends LongFunction implements TernaryFunction {
    final Function left;
    final Function center;
    final Function right;

    public DateDiffFunc(int position, Function left, Function center, Function right) {
        super(position);
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
    public long getLong(Record rec) {
        final char l = left.getChar(rec);
        final long c = center.getTimestamp(rec);
        final long r = right.getTimestamp(rec);
        if (c == Numbers.LONG_NaN || r == Numbers.LONG_NaN) {
            return Numbers.LONG_NaN;
        }
        return Timestamps.getPeriodBetween(l, c, r);
    }
}
}
