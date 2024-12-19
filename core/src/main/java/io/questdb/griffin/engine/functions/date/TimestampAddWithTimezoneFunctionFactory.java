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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;

public class TimestampAddWithTimezoneFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "dateadd(AINS)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function periodFunc = args.getQuick(0);
        Function strideFunc = args.getQuick(1);
        Function timestampFunc = args.getQuick(2);
        Function tzFunc = args.getQuick(3);
        int stride;

        if (periodFunc.isConstant()) {
            char period = periodFunc.getChar(null);
            LongAddIntFunction periodAddFunc = lookupAddFunction(period, argPositions.getQuick(0));
            if (strideFunc.isConstant()) {
                if ((stride = strideFunc.getInt(null)) != Numbers.INT_NULL) {
                    if (tzFunc.isConstant()) {
                        return new TimestampAddConstConstVarConst(period, periodAddFunc, stride, timestampFunc, tzFunc.getStrA(null));
                    }
                    return new TimestampAddConstConstVarVar(period, periodAddFunc, stride, timestampFunc, tzFunc);
                } else {
                    throw SqlException.$(argPositions.getQuick(1), "`null` is not a valid stride");
                }
            }
            return new TimestampAddConstVarVarVar(period, periodAddFunc, strideFunc, timestampFunc, tzFunc);
        }
        return new TimestampAddFunc(periodFunc, strideFunc, timestampFunc, tzFunc);
    }


    private LongAddIntFunction lookupAddFunction(char period, int periodPos) throws SqlException {
        switch (period) {
            case 'u':
                return Timestamps::addMicros;
            case 'T':
                return Timestamps::addMillis;
            case 's':
                return Timestamps::addSeconds;
            case 'm':
                return Timestamps::addMinutes;
            case 'h':
                return Timestamps::addHours;
            case 'd':
                return Timestamps::addDays;
            case 'w':
                return Timestamps::addWeeks;
            case 'M':
                return Timestamps::addMonths;
            case 'y':
                return Timestamps::addYears;
            default:
                throw SqlException.$(periodPos, "invalid time period unit '").put(period).put("'");
        }
    }

    @FunctionalInterface
    private interface LongAddIntFunction {
        long add(long a, int b);
    }

    private static class TimestampAddConstConstVarConst extends TimestampFunction implements UnaryFunction {
        private final char period;
        private final LongAddIntFunction periodAddFunction;
        private final int stride;
        private final Function timestampFunc;
        private final CharSequence tz;

        public TimestampAddConstConstVarConst(char period, LongAddIntFunction periodAddFunction, int stride, Function timestampFunc, CharSequence tz) {
            this.period = period;
            this.periodAddFunction = periodAddFunction;
            this.stride = stride;
            this.timestampFunc = timestampFunc;
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
                long addedTime = periodAddFunction.add(tzTime, stride);
                return tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
            } catch (NumericException e) {
                return periodAddFunction.add(timestamp, stride);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(stride).val(',').val(timestampFunc).val(',').val(tz).val(')');
        }
    }

    private static class TimestampAddConstConstVarVar extends TimestampFunction implements BinaryFunction {
        private final char period;
        private final LongAddIntFunction periodAddFunction;
        private final int stride;
        private final Function timestampFunc;
        private final Function tzFunc;

        public TimestampAddConstConstVarVar(char period, LongAddIntFunction periodAddFunction, int stride, Function timestampFunc, Function tzFunc) {
            this.tzFunc = tzFunc;
            this.period = period;
            this.periodAddFunction = periodAddFunction;
            this.stride = stride;
            this.timestampFunc = timestampFunc;
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
                long addedTime = periodAddFunction.add(tzTime, stride);
                return tz != null ? Timestamps.toUTC(addedTime, TimestampFormatUtils.EN_LOCALE, tz) : addedTime;
            } catch (NumericException e) {
                return periodAddFunction.add(timestamp, stride);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(stride).val(',').val(timestampFunc).val(',').val(tzFunc).val(')');
        }
    }

    private static class TimestampAddConstVarVarVar extends TimestampFunction implements TernaryFunction {
        private final char period;
        private final LongAddIntFunction periodAddFunc;
        private final Function strideFunc;
        private final Function timestampFunc;
        private final Function tzFunc;

        public TimestampAddConstVarVarVar(char period, LongAddIntFunction periodAddFunc, Function strideFunc, Function timestampFunc, Function tzFunc) {
            this.tzFunc = tzFunc;
            this.period = period;
            this.periodAddFunc = periodAddFunc;
            this.strideFunc = strideFunc;
            this.timestampFunc = timestampFunc;
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
            return strideFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            final int periodValue = strideFunc.getInt(rec);
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
            sink.val("dateadd('").val(period).val("',").val(timestampFunc).val(',').val(strideFunc).val(tzFunc).val(',').val(')');
        }
    }

    private static class TimestampAddFunc extends TimestampFunction implements QuaternaryFunction {
        private final Function periodFunc;
        private final Function strideFunc;
        private final Function timestampFunc;
        private final Function tzFunc;

        public TimestampAddFunc(Function periodFunc, Function strideFunc, Function timestampFunc, Function tzFunc) {
            this.timestampFunc = timestampFunc;
            this.strideFunc = strideFunc;
            this.periodFunc = periodFunc;
            this.tzFunc = tzFunc;
        }

        @Override
        public Function getFunc0() {
            return timestampFunc;
        }

        @Override
        public Function getFunc1() {
            return strideFunc;
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
            final int periodOccurrence = strideFunc.getInt(rec);
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
}
