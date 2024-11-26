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

public class TimestampAddWithTimezoneFunctionFactory implements FunctionFactory {

    private static final ObjList<LongAddIntFunction> addFunctions = new ObjList<>();
    private static final int addFunctionsMax;

    @Override
    public String getSignature() {
        return "dateadd(AINS)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function periodFunc = args.getQuick(0);
        Function intervalFunc = args.getQuick(1);
        Function timestampFunc = args.getQuick(2);
        Function tzFunc = args.getQuick(3);
        if (periodFunc.isConstant()) {
            char period = periodFunc.getChar(null);
            if (period < addFunctionsMax) {
                LongAddIntFunction periodAddFunc = addFunctions.getQuick(period);
                if (periodAddFunc != null) {
                    if (intervalFunc.isConstant()) {
                        if (tzFunc.isConstant()) {
                            if (intervalFunc.getInt(null) != Numbers.INT_NULL) {
                                return new AddLongIntVarConstFunction(timestampFunc, intervalFunc.getInt(null), periodAddFunc, period, tzFunc.getStrA(null));
                            }
                            return TimestampConstant.NULL;
                        } else {
                            return new AddLongIntVarVarConstFunction(timestampFunc, intervalFunc.getInt(null), periodAddFunc, period, tzFunc);
                        }
                    } else {
                        return new AddLongIntVarVarVarFunction(timestampFunc, intervalFunc, periodAddFunc, period, tzFunc);
                    }
                }
            }
            return TimestampConstant.NULL;
        }
        return new DateAddFunc(timestampFunc, intervalFunc, periodFunc, tzFunc);
    }

    @FunctionalInterface
    private interface LongAddIntFunction {
        long add(long a, int b);
    }

    private static class AddLongIntVarConstFunction extends TimestampFunction implements UnaryFunction {
        private final Function timestampFunc;
        private final LongAddIntFunction periodAddFunc;
        private final int interval;
        private final char period;
        private final CharSequence tz;

        public AddLongIntVarConstFunction(Function timestampFunc, int interval, LongAddIntFunction periodAddFunc, char period, CharSequence tz) {
            this.timestampFunc = timestampFunc;
            this.interval = interval;
            this.periodAddFunc = periodAddFunc;
            this.period = period;
            this.tz = tz;
        }

        @Override
        public Function getArg() {
            return timestampFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            if (timestamp == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }

            try {
                long tzTime = tz != null ? Timestamps.toTimezone(timestamp, TimestampFormatUtils.EN_LOCALE, tz) : timestamp;
                long addedTime = periodAddFunc.add(tzTime, interval);
                return tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
            } catch (NumericException e) {
                return periodAddFunc.add(timestamp, interval);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(interval).val(',').val(timestampFunc).val(',').val(tz).val(')');
        }
    }

    private static class AddLongIntVarVarConstFunction extends TimestampFunction implements BinaryFunction {
        private final LongAddIntFunction periodAddFunc;
        private final Function timestampFunc;
        private final char period;
        private final Function tzFunc;
        private final int interval;

        public AddLongIntVarVarConstFunction(Function timestampFunc, int interval, LongAddIntFunction periodAddFunc, char period, Function tzFunc) {
            this.timestampFunc = timestampFunc;
            this.tzFunc = tzFunc;
            this.interval = interval;
            this.periodAddFunc = periodAddFunc;
            this.period = period;
        }

        @Override
        public Function getLeft() {
            return timestampFunc;
        }

        @Override
        public Function getRight() {
            return tzFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            final CharSequence tz = tzFunc.getStrA(rec);
            if (timestamp == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }

            try {
                long tzTime = tz != null ? Timestamps.toTimezone(timestamp, TimestampFormatUtils.EN_LOCALE, tz) : timestamp;
                long addedTime = periodAddFunc.add(tzTime, interval);
                return tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
            } catch (NumericException e) {
                return periodAddFunc.add(timestamp, interval);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(interval).val(',').val(timestampFunc).val(',').val(tzFunc).val(')');
        }
    }

    private static class AddLongIntVarVarVarFunction extends TimestampFunction implements TernaryFunction {
        private final LongAddIntFunction periodAddFunc;
        private final Function timestampFunc;
        private final Function tzFunc;
        private final char period;
        private final Function intervalFunc;

        public AddLongIntVarVarVarFunction(Function timestampFunc, Function intervalFunc, LongAddIntFunction periodAddFunc, char period, Function tzFunc) {
            this.timestampFunc = timestampFunc;
            this.tzFunc = tzFunc;
            this.intervalFunc = intervalFunc;
            this.periodAddFunc = periodAddFunc;
            this.period = period;
        }

        @Override
        public Function getLeft() {
            return timestampFunc;
        }

        @Override
        public Function getCenter() {
            return tzFunc;
        }

        @Override
        public Function getRight() {
            return intervalFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            final int periodValue = intervalFunc.getInt(rec);
            final CharSequence tz = tzFunc.getStrA(rec);
            if (timestamp == Numbers.LONG_NULL || periodValue == Numbers.INT_NULL) {
                return Numbers.LONG_NULL;
            }

            try {
                long tzTime = tz != null ? Timestamps.toTimezone(timestamp, TimestampFormatUtils.EN_LOCALE, tz) : timestamp;
                long addedTime = periodAddFunc.add(tzTime, periodValue);
                return tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
            } catch (NumericException e) {
                return periodAddFunc.add(timestamp, periodValue);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(timestampFunc).val(',').val(intervalFunc).val(tzFunc).val(',').val(')');
        }
    }

    private static class DateAddFunc extends TimestampFunction implements QuaternaryFunction {
        private final Function timestampFunc;
        private final Function intervalFunc;
        private final Function periodFunc;
        private final Function tzFunc;

        public DateAddFunc(Function timestampFunc, Function intervalFunc, Function periodFunc, Function tzFunc) {
            this.timestampFunc = timestampFunc;
            this.intervalFunc = intervalFunc;
            this.periodFunc = periodFunc;
            this.tzFunc = tzFunc;
        }

        @Override
        public Function getFunc0() {
            return timestampFunc;
        }

        @Override
        public Function getFunc1() {
            return intervalFunc;
        }


        @Override
        public Function getFunc2() {
            return periodFunc;
        }

        @Override
        public Function getFunc3() {
            return tzFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            final int periodOccurrence = intervalFunc.getInt(rec);
            final char period = periodFunc.getChar(rec);
            final CharSequence tz = tzFunc.getStrA(rec);
            if (timestamp == Numbers.LONG_NULL || periodOccurrence == Numbers.INT_NULL) {
                return Numbers.LONG_NULL;
            }

            try {
                long tzTime = tz != null ? Timestamps.toTimezone(timestamp, TimestampFormatUtils.EN_LOCALE, tz) : timestamp;
                long addedTime = Timestamps.addPeriod(tzTime, period, periodOccurrence);
                return tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
            } catch (NumericException e) {
                return Timestamps.addPeriod(timestamp, period, periodOccurrence);
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
