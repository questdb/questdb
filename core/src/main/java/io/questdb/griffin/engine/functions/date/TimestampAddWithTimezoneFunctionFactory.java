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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;

public class TimestampAddWithTimezoneFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "dateadd(AINS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function periodFunc = args.getQuick(0);
        Function strideFunc = args.getQuick(1);
        Function timestampFunc = args.getQuick(2);
        Function tzFunc = args.getQuick(3);
        int stride;
        int timestampType = ColumnType.getTimestampType(timestampFunc.getType());
        timestampType = ColumnType.getHigherPrecisionTimestampType(timestampType, ColumnType.TIMESTAMP_MICRO);
        if (periodFunc.isConstant() && tzFunc.isConstant()) {
            // validate timezone and parse timezone into rules, that provide the offset by timestamp
            final TimeZoneRules timeZoneRules;
            try {
                timeZoneRules = ColumnType.getTimestampDriver(timestampType).getTimezoneRules(DateLocaleFactory.EN_LOCALE, tzFunc.getStrA(null));
            } catch (CairoException e) {
                throw SqlException.position(argPositions.getQuick(3)).put(e.getFlyweightMessage());
            }

            final char period = periodFunc.getChar(null);
            final TimestampDriver.TimestampAddMethod periodAddFunc = ColumnType.getTimestampDriver(timestampType).getAddMethod(period);
            if (periodAddFunc == null) {
                throw SqlException.$(argPositions.getQuick(0), "invalid time period [unit=").put(period).put(']');
            }

            if (strideFunc.isConstant()) {
                if ((stride = strideFunc.getInt(null)) != Numbers.INT_NULL) {
                    return new TimestampAddConstConstVarConst(period, periodAddFunc, stride, timestampFunc, timeZoneRules, tzFunc, timestampType);
                } else {
                    throw SqlException.$(argPositions.getQuick(1), "`null` is not a valid stride");
                }
            }

            return new TimestampAddConstVarVarConst(
                    period,
                    periodAddFunc,
                    strideFunc,
                    argPositions.getQuick(1),
                    timestampFunc,
                    timeZoneRules,
                    tzFunc,
                    timestampType
            );
        }

        return new TimestampAddFunc(
                periodFunc,
                argPositions.getQuick(0),
                strideFunc,
                argPositions.getQuick(1),
                timestampFunc,
                tzFunc,
                argPositions.getQuick(3),
                timestampType
        );
    }

    private static long compute(long timestamp, TimeZoneRules timeZoneRules, int stride, TimestampDriver.TimestampAddMethod periodAddFunction) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        long offset = timeZoneRules.getOffset(timestamp);
        long localTimestamp = periodAddFunction.add(timestamp + offset, stride);
        return localTimestamp - timeZoneRules.getLocalOffset(localTimestamp);
    }

    private static class TimestampAddConstConstVarConst extends TimestampFunction implements UnaryFunction {
        private final char period;
        private final TimestampDriver.TimestampAddMethod periodAddFunction;
        private final int stride;
        private final TimeZoneRules timeZoneRules;
        private final Function timestampFunc;
        private final Function tzFunc;

        public TimestampAddConstConstVarConst(
                char period,
                TimestampDriver.TimestampAddMethod periodAddFunction,
                int stride,
                Function timestampFunc,
                TimeZoneRules timeZoneRules,
                Function tzFunc,
                int timestampType
        ) {
            super(timestampType);
            this.period = period;
            this.periodAddFunction = periodAddFunction;
            this.stride = stride;
            this.timestampFunc = timestampFunc;
            this.timeZoneRules = timeZoneRules;
            this.tzFunc = tzFunc;
        }

        @Override
        public Function getArg() {
            return timestampFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            return compute(timestampFunc.getTimestamp(rec), timeZoneRules, stride, periodAddFunction);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(stride).val(',').val(timestampFunc).val(',').val(tzFunc).val(')');
        }
    }

    private static class TimestampAddConstVarVarConst extends TimestampFunction implements TernaryFunction {
        private final char period;
        private final TimestampDriver.TimestampAddMethod periodAddFunc;
        private final Function strideFunc;
        private final int stridePosition;
        private final TimeZoneRules timeZoneRules;
        private final Function timestampFunc;
        private final Function tzFunc;

        public TimestampAddConstVarVarConst(
                char period,
                TimestampDriver.TimestampAddMethod periodAddFunc,
                Function strideFunc,
                int stridePosition,
                Function timestampFunc,
                TimeZoneRules timeZoneRules,
                Function tzFunc,
                int timestampType
        ) {
            super(timestampType);
            this.period = period;
            this.periodAddFunc = periodAddFunc;
            this.strideFunc = strideFunc;
            this.stridePosition = stridePosition;
            this.timestampFunc = timestampFunc;
            this.timeZoneRules = timeZoneRules;
            this.tzFunc = tzFunc;
        }

        @Override
        public Function getCenter() {
            return tzFunc;
        }

        @Override
        public Function getLeft() {
            return timestampFunc;
        }

        @Override
        public Function getRight() {
            return strideFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            final int stride = strideFunc.getInt(rec);

            if (stride == Numbers.INT_NULL) {
                throw CairoException.nonCritical().position(stridePosition).put("`null` is not a valid stride");
            }
            return compute(timestamp, timeZoneRules, stride, periodAddFunc);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(period).val("',").val(timestampFunc).val(',').val(strideFunc).val(',').val(tzFunc).val(')');
        }
    }

    private static class TimestampAddFunc extends TimestampFunction implements QuaternaryFunction {
        private final Function periodFunc;
        private final int periodPosition;
        private final Function strideFunc;
        private final int stridePosition;
        private final Function timestampFunc;
        private final int timezonePosition;
        private final Function tzFunc;

        public TimestampAddFunc(
                Function periodFunc,
                int periodPosition,
                Function strideFunc,
                int stridePosition,
                Function timestampFunc,
                Function tzFunc,
                int timezonePosition,
                int timestampType
        ) {
            super(timestampType);
            this.periodFunc = periodFunc;
            this.periodPosition = periodPosition;
            this.strideFunc = strideFunc;
            this.stridePosition = stridePosition;
            this.timestampFunc = timestampFunc;
            this.tzFunc = tzFunc;
            this.timezonePosition = timezonePosition;
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
            final int stride = strideFunc.getInt(rec);
            final char period = periodFunc.getChar(rec);
            final CharSequence tz = tzFunc.getStrA(rec);

            // validation
            if (timestamp == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }

            if (stride == Numbers.INT_NULL) {
                throw CairoException.nonCritical().position(stridePosition).put("`null` is not a valid stride");
            }

            if (tz == null) {
                throw CairoException.nonCritical().position(timezonePosition).put("NULL timezone");
            }
            final TimeZoneRules timeZoneRules;
            try {
                timeZoneRules = timestampDriver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, tz);
            } catch (CairoException e) {
                throw e.position(timezonePosition);
            }
            final TimestampDriver.TimestampAddMethod periodAddFunc = timestampDriver.getAddMethod(period);
            if (periodAddFunc == null) {
                throw CairoException.nonCritical().position(periodPosition).put("invalid period [period=").put(period).put(']');
            }
            return compute(timestamp, timeZoneRules, stride, periodAddFunc);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("dateadd('").val(periodFunc).val("',").val(strideFunc).val(',').val(timestampFunc).val(',').val(tzFunc).val(')');
        }
    }
}
