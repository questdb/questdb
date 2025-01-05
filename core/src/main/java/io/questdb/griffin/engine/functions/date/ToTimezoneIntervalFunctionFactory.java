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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Numbers;
import io.questdb.std.Misc;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.Interval;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public class ToTimezoneIntervalFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "to_timezone(ΔS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function interval = args.getQuick(0);
        Function timezone = args.getQuick(1);

        if (timezone.isConstant()) {
            final TimeZoneRules timeZoneRules;
            try {
                final CharSequence tz = timezone.getStrA(null);
                if (tz != null) {
                    timeZoneRules = Timestamps.getTimezoneRules(TimestampFormatUtils.EN_LOCALE, tz);
                    return new OffsetIntervalFromRulesFunction(interval, timeZoneRules);

                } else {
                    throw SqlException.$(argPositions.getQuick(1), "timezone must not be null");
                }

            } catch (NumericException e) {
                throw SqlException.$(argPositions.getQuick(1), "invalid timezone name");
            }
        } else {
            return new ToTimezoneIntervalFunctionVar(interval, timezone);
        }
    }

//    @NotNull
//    static IntervalFunction getIntervalFunction(IntList argPositions, Function interval, Function timezone) throws SqlException {
//        final CharSequence tz = timezone.getStrA(null);
//        if (tz != null) {
//            final int hi = tz.length();
//            final long l = Timestamps.parseOffset(tz, 0, hi);
//            if (l == Long.MIN_VALUE) {
//                try {
//                    return new OffsetIntervalFromRulesFunction(
//                            interval,
//                            TimestampFormatUtils.EN_LOCALE.getZoneRules(
//                                    Numbers.decodeLowInt(TimestampFormatUtils.EN_LOCALE.matchZone(tz, 0, hi)), RESOLUTION_MICROS
//                            )
//                    );
//                } catch (NumericException e) {
//                    Misc.free(interval);
//                    throw SqlException.$(argPositions.getQuick(1), "invalid timezone name");
//                }
//            } else {
//                return new OffsetIntervalFunctionFromOffset(
//                        interval,
//                        Numbers.decodeLowInt(l) * Timestamps.MINUTE_MICROS
//                );
//            }
//        }
//        throw SqlException.$(argPositions.getQuick(1), "timezone must not be null");
//    }

    private static class ToTimezoneIntervalFunctionVar extends IntervalFunction implements BinaryFunction {
        private final Interval interval = new Interval();
        private final Function intervalFunction;
        private final Function timezone;

        public ToTimezoneIntervalFunctionVar(Function intervalFunction, Function timezone) {
            this.intervalFunction = intervalFunction;
            this.timezone = timezone;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            final long timestampLo = intervalFunction.getInterval(rec).getLo();
            final long timestampHi = intervalFunction.getInterval(rec).getHi();

            try {
                final TimeZoneRules timeZoneRules;
                final CharSequence tz = timezone.getStrA(rec);
                if (tz != null) {
                    timeZoneRules = Timestamps.getTimezoneRules(TimestampFormatUtils.EN_LOCALE, tz);
                    interval.of(
                            timestampLo + timeZoneRules.getOffset(timestampLo), timestampHi + timeZoneRules.getOffset(timestampHi)
                    );
                    return interval;
                } else {
                    throw CairoException.nonCritical().put("timezone must not be null");
                }
            } catch (NumericException e) {
                throw CairoException.nonCritical().put("invalid timezone name");
            }
        }

        @Override
        public Function getLeft() {
            return intervalFunction;
        }

        @Override
        public String getName() {
            return "to_timezone";
        }

        @Override
        public Function getRight() {
            return timezone;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("to_timezone(").val(intervalFunction).val(',').val(timezone).val(')');
        }
    }
}