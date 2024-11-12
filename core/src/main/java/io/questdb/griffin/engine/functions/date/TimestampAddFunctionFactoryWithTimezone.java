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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;

public class TimestampAddFunctionFactoryWithTimezone implements FunctionFactory {

    private static final ObjList<LongAddIntFunction> addFunctions = new ObjList<>();
    private static final int addFunctionsMax;

    @Override
    public String getSignature() {
        return "dateadd(AINS)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function period = args.getQuick(0);
        Function interval = args.getQuick(1);
        Function timeZone = args.getQuick(3);
        if (period.isConstant()) {
            char periodValue = period.getChar(null);
            if (periodValue < addFunctionsMax) {
                LongAddIntFunction func = addFunctions.getQuick(periodValue);
                if (func != null) {
                    if (interval.isConstant()) {
                        if (timeZone.isConstant()) {
                            if (interval.getInt(null) != Numbers.INT_NULL) {
                                return new AddLongIntVarConstFunction(args.getQuick(2), interval.getInt(null), func, periodValue, timeZone.getStrA(null));
                            }
                            return TimestampConstant.NULL;
                        } else {
                            return new AddLongIntVarVarConstFunction(args.getQuick(2), args.getQuick(3), interval.getInt(null), func, periodValue);
                        }
                    } else {
                        return new AddLongIntVarVarVarFunction(args.getQuick(2), args.getQuick(3), args.getQuick(1), func, periodValue);
                    }
                }
            }
            return TimestampConstant.NULL;
        }
        return new DateAddFunc(args.getQuick(2), args.getQuick(1), args.getQuick(0), args.getQuick(3));
    }

    @FunctionalInterface
    private interface LongAddIntFunction {
        long add(long a, int b);
    }

    private static class AddLongIntVarConstFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;
        private final LongAddIntFunction func;
        private final int interval;
        private final char periodSymbol;
        private final CharSequence timeZone;

        public AddLongIntVarConstFunction(Function left, int right, LongAddIntFunction func, char periodSymbol, CharSequence timeZone) {
            this.arg = left;
            this.interval = right;
            this.func = func;
            this.periodSymbol = periodSymbol;
            this.timeZone = timeZone;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = arg.getTimestamp(rec);
            if (timestamp == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }

            try {
                long timeZoneTime = timeZone != null ? Timestamps.toTimezone(timestamp, TimestampFormatUtils.EN_LOCALE, timeZone) : timestamp;
                long addedTime = func.add(timeZoneTime, interval);
                long utcTime = timeZone != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, timeZone) : addedTime;
                return utcTime;
            } catch (NumericException e) {
                return func.add(timestamp, interval);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(periodSymbol).val("',").val(interval).val(',').val(arg).val(',').val(timeZone).val(')');
        }
    }

    private static class AddLongIntVarVarConstFunction extends TimestampFunction implements BinaryFunction {
        private final LongAddIntFunction func;
        private final Function left;
        private final char periodSymbol;
        private final Function right;
        private final int interval;

        public AddLongIntVarVarConstFunction(Function left, Function right, int interval, LongAddIntFunction func, char periodSymbol) {
            this.left = left;
            this.right = right;
            this.interval = interval;
            this.func = func;
            this.periodSymbol = periodSymbol;
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
            final long timestamp = left.getTimestamp(rec);
            final CharSequence tz = right.getStrA(rec);
            if (timestamp == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }

            try {
                long timeZoneTime = tz != null ? Timestamps.toTimezone(timestamp, TimestampFormatUtils.EN_LOCALE, tz) : timestamp;
                long addedTime = func.add(timeZoneTime, interval);
                long utcTime = tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
                return utcTime;
            } catch (NumericException e) {
                return func.add(timestamp, interval);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(periodSymbol).val("',").val(interval).val(',').val(left).val(',').val(right).val(')');
        }
    }

    private static class AddLongIntVarVarVarFunction extends TimestampFunction implements TernaryFunction {
        private final LongAddIntFunction func;
        private final Function left;
        private final Function center;
        private final char periodSymbol;
        private final Function right;

        public AddLongIntVarVarVarFunction(Function left, Function center, Function right, LongAddIntFunction func, char periodSymbol) {
            this.left = left;
            this.center = center;
            this.right = right;
            this.func = func;
            this.periodSymbol = periodSymbol;
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
            final long timestamp = left.getTimestamp(rec);
            final int periodValue = right.getInt(rec);
            final CharSequence tz = center.getStrA(rec);
            if (timestamp == Numbers.LONG_NULL || periodValue == Numbers.INT_NULL) {
                return Numbers.LONG_NULL;
            }

            try {
                long timeZoneTime = tz != null ? Timestamps.toTimezone(timestamp, TimestampFormatUtils.EN_LOCALE, tz) : timestamp;
                long addedTime = func.add(timeZoneTime, periodValue);
                long utcTime = tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
                return utcTime;
            } catch (NumericException e) {
                return func.add(timestamp, periodValue);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(periodSymbol).val("',").val(center).val(',').val(left).val(',').val(right).val(')');
        }
    }

    private static class DateAddFunc extends TimestampFunction implements QuaternaryFunction {
        private final Function func0;
        private final Function func1;
        private final Function func2;
        private final Function func3;

        public DateAddFunc(Function func0, Function func1, Function func2, Function func3) {
            this.func0 = func0;
            this.func1 = func1;
            this.func2 = func2;
            this.func3 = func3;
        }

        @Override
        public Function getFunc0() {
            return func0;
        }

        @Override
        public Function getFunc1() {
            return func1;
        }


        @Override
        public Function getFunc2() {
            return func2;
        }

        @Override
        public Function getFunc3() {
            return func3;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = func0.getTimestamp(rec);
            final int periodOccurance = func1.getInt(rec);
            final char period = func2.getChar(rec);
            final CharSequence tz = func3.getStrA(rec);
            if (timestamp == Numbers.LONG_NULL || periodOccurance == Numbers.INT_NULL) {
                return Numbers.LONG_NULL;
            }

            try {
                long timeZoneTime = tz != null ? Timestamps.toTimezone(timestamp, TimestampFormatUtils.EN_LOCALE, tz) : timestamp;
                long addedTime = Timestamps.addPeriod(timeZoneTime, period, periodOccurance);
                long utcTime = tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
                return utcTime;
            } catch (NumericException e) {
                return Timestamps.addPeriod(timestamp, period, periodOccurance);
            }
        }
    }

    static {
        addFunctions.extendAndSet('u', Timestamps::addMicros);
        addFunctions.extendAndSet('T', Timestamps::addMillis);
        addFunctions.extendAndSet('s', Timestamps::addSeconds);
        addFunctions.extendAndSet('m', Timestamps::addMinutes);
        addFunctions.extendAndSet('h', Timestamps::addHours);
        addFunctions.extendAndSet('d', Timestamps::addDays);
        addFunctions.extendAndSet('w', Timestamps::addWeeks);
        addFunctions.extendAndSet('M', Timestamps::addMonths);
        addFunctions.extendAndSet('y', Timestamps::addYears);
        addFunctionsMax = addFunctions.size();
    }
}
