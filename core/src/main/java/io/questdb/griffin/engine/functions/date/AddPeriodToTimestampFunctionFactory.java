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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.microtime.Timestamps;

public class AddPeriodToTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "add_period(NAI)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {

        Function period = args.getQuick(1);
        if (period.isConstant()) {
            char periodValue = period.getChar(null);
            if (periodValue == 's') {
                return new AddLongFuncSecondConstant(position, args.getQuick(0), args.getQuick(2));
            }
            if (periodValue == 'm') {
                return new AddLongFuncMinuteConstant(position, args.getQuick(0), args.getQuick(2));
            }
            if (periodValue == 'h') {
                return new AddLongFuncHourConstant(position, args.getQuick(0), args.getQuick(2));
            }
            if (periodValue == 'd') {
                return new AddLongFuncDayConstant(position, args.getQuick(0), args.getQuick(2));
            }
            if (periodValue == 'w') {
                return new AddLongFuncWeekConstant(position, args.getQuick(0), args.getQuick(2));
            }
            if (periodValue == 'M') {
                return new AddLongFuncMonthConstant(position, args.getQuick(0), args.getQuick(2));
            }
            if (periodValue == 'y') {
                return new AddLongFuncYearConstant(position, args.getQuick(0), args.getQuick(2));
            }
            return new AddLongFunc(position, args.getQuick(0), args.getQuick(1), args.getQuick(2));
        }

        return new AddLongFunc(position, args.getQuick(0), args.getQuick(1), args.getQuick(2));
    }

    private static class AddLongFuncSecondConstant extends TimestampFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddLongFuncSecondConstant(int position, Function left, Function right) {
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
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.addSeconds(l,r);
        }
    }

    private static class AddLongFuncMinuteConstant extends TimestampFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddLongFuncMinuteConstant(int position, Function left, Function right) {
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
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.addMinutes(l,r);
        }
    }

    private static class AddLongFuncHourConstant extends TimestampFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddLongFuncHourConstant(int position, Function left, Function right) {
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
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.addHours(l,r);
        }
    }

    private static class AddLongFuncDayConstant extends TimestampFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddLongFuncDayConstant(int position, Function left, Function right) {
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
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.addDays(l,r);
        }
    }
    
    
    private static class AddLongFuncWeekConstant extends TimestampFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddLongFuncWeekConstant(int position, Function left, Function right) {
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
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.addWeeks(l,r);
        }
    }

    private static class AddLongFuncMonthConstant extends TimestampFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddLongFuncMonthConstant(int position, Function left, Function right) {
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
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.addMonths(l,r);
        }
    }

    private static class AddLongFuncYearConstant extends TimestampFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddLongFuncYearConstant(int position, Function left, Function right) {
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
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final int r = right.getInt(rec);
            if (l == Numbers.LONG_NaN || r == Numbers.INT_NaN) {
                return Numbers.LONG_NaN;
            }
            return Timestamps.addYear(l,r);
        }
    }
    
    private static class AddLongFunc extends TimestampFunction implements TernaryFunction {
        final Function left;
        final Function center;
        final Function right;

        public AddLongFunc(int position, Function left, Function center, Function right) {
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
        public Function getCenter() {return center;}

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
            return Timestamps.addPeriod(l,c,r);
        }
    }
}
