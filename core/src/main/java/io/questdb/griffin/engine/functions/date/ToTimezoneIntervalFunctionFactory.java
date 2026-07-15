/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import org.jetbrains.annotations.NotNull;

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
        final Function intervalFunc = args.getQuick(0);
        final Function timezoneFunc = args.getQuick(1);
        final int timezonePos = argPositions.getQuick(1);
        final int intervalType = intervalFunc.getType();
        final TimestampDriver timestampDriver = IntervalUtils.getTimestampDriverByIntervalType(intervalType);

        if (timezoneFunc.isConstant()) {
            return toTimezoneConstFunction(intervalFunc, timezoneFunc, timezonePos, timestampDriver, intervalType);
        } else if (timezoneFunc.isRuntimeConstant()) {
            return new RuntimeConstFunc(intervalFunc, timezoneFunc, timezonePos, timestampDriver, intervalType);
        } else {
            return new Func(intervalFunc, timezoneFunc, timestampDriver, intervalType);
        }
    }

    @NotNull
    private static IntervalFunction toTimezoneConstFunction(
            Function intervalFunc,
            Function timezoneFunc,
            int timezonePos,
            TimestampDriver timestampDriver,
            int intervalType
    ) throws SqlException {
        final CharSequence tz = timezoneFunc.getStrA(null);
        if (tz != null) {
            final int hi = tz.length();
            final long l = Dates.parseOffset(tz, 0, hi);
            if (l == Long.MIN_VALUE) {
                try {
                    return new ConstRulesFunc(
                            intervalFunc,
                            DateLocaleFactory.EN_LOCALE.getZoneRules(
                                    Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, hi)), timestampDriver.getTZRuleResolution()
                            ),
                            intervalType
                    );
                } catch (NumericException e) {
                    Misc.free(intervalFunc);
                    throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                }
            } else {
                return new OffsetFunc(
                        intervalFunc,
                        timestampDriver.fromMinutes(Numbers.decodeLowInt(l)),
                        intervalType
                );
            }
        }
        throw SqlException.$(timezonePos, "timezone must not be null");
    }

    private abstract static class AbstractFunc extends IntervalFunction implements FunctionExtension {
        protected final Interval interval = new Interval();
        protected final Function intervalFunc;

        protected AbstractFunc(Function intervalFunc, int intervalType) {
            super(intervalType);
            this.intervalFunc = intervalFunc;
        }

        @Override
        public FunctionExtension extendedOps() {
            return this;
        }

        @Override
        public int getArrayLength() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return "to_timezone";
        }

        @Override
        public Record getRecord(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStrA(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStrB(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }
    }

    private static class ConstRulesFunc extends AbstractFunc implements UnaryFunction {
        private final TimeZoneRules tzRules;

        public ConstRulesFunc(Function intervalFunc, TimeZoneRules tzRules, int intervalType) {
            super(intervalFunc, intervalType);
            this.tzRules = tzRules;
        }

        @Override
        public Function getArg() {
            return intervalFunc;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            final Interval src = intervalFunc.getInterval(rec);
            final long lo = src.getLo();
            final long hi = src.getHi();
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                return Interval.NULL;
            }
            return interval.of(lo + tzRules.getOffset(lo), hi + tzRules.getOffset(hi));
        }
    }

    private static class Func extends AbstractFunc implements BinaryFunction {
        private final TimestampDriver timestampDriver;
        private final Function timezoneFunc;

        public Func(Function intervalFunc, Function timezoneFunc, TimestampDriver timestampDriver, int intervalType) {
            super(intervalFunc, intervalType);
            this.timezoneFunc = timezoneFunc;
            this.timestampDriver = timestampDriver;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            final Interval src = intervalFunc.getInterval(rec);
            final long lo = src.getLo();
            final long hi = src.getHi();
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                return Interval.NULL;
            }
            final CharSequence tz = timezoneFunc.getStrA(rec);
            if (tz == null) {
                return interval.of(lo, hi);
            }
            try {
                return interval.of(
                        timestampDriver.toTimezone(lo, DateLocaleFactory.EN_LOCALE, tz),
                        timestampDriver.toTimezone(hi, DateLocaleFactory.EN_LOCALE, tz)
                );
            } catch (NumericException e) {
                return interval.of(lo, hi);
            }
        }

        @Override
        public Function getLeft() {
            return intervalFunc;
        }

        @Override
        public Function getRight() {
            return timezoneFunc;
        }
    }

    private static class OffsetFunc extends AbstractFunc implements UnaryFunction {
        private final long offset;

        public OffsetFunc(Function intervalFunc, long offset, int intervalType) {
            super(intervalFunc, intervalType);
            this.offset = offset;
        }

        @Override
        public Function getArg() {
            return intervalFunc;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            final Interval src = intervalFunc.getInterval(rec);
            final long lo = src.getLo();
            final long hi = src.getHi();
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                return Interval.NULL;
            }
            return interval.of(lo + offset, hi + offset);
        }
    }

    private static class RuntimeConstFunc extends AbstractFunc implements BinaryFunction {
        private final TimestampDriver timestampDriver;
        private final Function timezoneFunc;
        private final int timezonePos;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public RuntimeConstFunc(Function intervalFunc, Function timezoneFunc, int timezonePos, TimestampDriver timestampDriver, int intervalType) {
            super(intervalFunc, intervalType);
            this.timezoneFunc = timezoneFunc;
            this.timezonePos = timezonePos;
            this.timestampDriver = timestampDriver;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            final Interval src = intervalFunc.getInterval(rec);
            final long lo = src.getLo();
            final long hi = src.getHi();
            if (lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL) {
                return Interval.NULL;
            }
            if (tzRules != null) {
                return interval.of(lo + tzRules.getOffset(lo), hi + tzRules.getOffset(hi));
            }
            return interval.of(lo + tzOffset, hi + tzOffset);
        }

        @Override
        public Function getLeft() {
            return intervalFunc;
        }

        @Override
        public Function getRight() {
            return timezoneFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence tz = timezoneFunc.getStrA(null);
            if (tz == null) {
                throw SqlException.$(timezonePos, "timezone must not be null");
            }

            final int hi = tz.length();
            final long l = Dates.parseOffset(tz, 0, hi);
            if (l == Long.MIN_VALUE) {
                try {
                    tzRules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                            Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, hi)), timestampDriver.getTZRuleResolution()
                    );
                    tzOffset = 0;
                } catch (NumericException e) {
                    throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                }
            } else {
                tzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                tzRules = null;
            }
        }
    }
}
