/*+*****************************************************************************
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.ResultTypes;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
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

public class ToUTCTimestampFunctionFactory implements FunctionFactory, MonotonicTimestampFunctionFactory {
    public static final String NAME = "to_utc";

    @Override
    public int getResultType(IntList argTypes) {
        return ResultTypes.timestampAtLeastMicros(argTypes.getQuick(0));
    }

    @Override
    public String getSignature() {
        return NAME + "(NS)";
    }

    @Override
    public int getTimestampArgumentIndex(FunctionExpression call, ConstantArguments arguments) {
        return call.argumentAt(1) instanceof ConstantExpression ? 0 : -1;
    }

    @Override
    public int invertTimestampInterval(FunctionExpression call, Interval io, boolean isTimestampArgMonotonic, ConstantArguments arguments) throws SqlException {
        final int timestampType = call.getDataType();
        final CharSequence tz = arguments.constant(call.argumentAt(1)).getStrA(null);
        final TimeZoneRules rules = ToTimezoneTimestampFunctionFactory.constantZoneRules(tz, call.argumentAt(1).getPosition(), timestampType);
        return rules != null
                ? MonotonicTimestampFunction.invertZoneOffsetShift(io, rules, ColumnType.getTimestampDriver(timestampType), 1)
                : MonotonicTimestampFunction.invertConstantShift(io, fixedOffset(tz, timestampType),
                MonotonicTimestampFunction.shiftInputCeiling(isTimestampArgMonotonic, timestampType));
    }

    @Override
    public boolean isConstructionDeferrable(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration) throws SqlException {
        final Function timezoneFunc = args.getQuick(1);
        if (timezoneFunc.isConstant()) {
            ToTimezoneTimestampFunctionFactory.constantZoneRules(timezoneFunc.getStrA(null), argPositions.getQuick(1), ResultTypes.timestampAtLeastMicros(args.getQuick(0).getType()));
        }
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function timestampFunc = args.getQuick(0);
        final Function timezoneFunc = args.getQuick(1);
        final int timezonePos = argPositions.getQuick(1);
        int timestampType = ColumnType.getTimestampType(timestampFunc.getType());
        timestampType = ColumnType.getHigherPrecisionTimestampType(timestampType, ColumnType.TIMESTAMP_MICRO);
        if (timezoneFunc.isConstant()) {
            final Function function = toUTCConstFunction(timestampFunc, timezoneFunc, timezonePos, timestampType);
            args.setQuick(0, null);
            args.setQuick(1, null);
            try {
                Misc.free(timezoneFunc);
                return function;
            } catch (Throwable th) {
                Misc.free(function, th);
                throw th;
            }
        } else if (timezoneFunc.isRuntimeConstant()) {
            return new RuntimeConstFunc(timestampFunc, timezoneFunc, timezonePos, timestampType);
        } else {
            return new Func(timestampFunc, timezoneFunc, timestampType);
        }
    }

    private static long fixedOffset(CharSequence tz, int timestampType) {
        return ColumnType.getTimestampDriver(timestampType).fromMinutes(-Numbers.decodeLowInt(Dates.parseOffset(tz, 0, tz.length())));
    }

    @NotNull
    private static TimestampFunction toUTCConstFunction(
            Function timestampFunc,
            Function timezoneFunc,
            int timezonePos,
            int timestampType
    ) throws SqlException {
        final CharSequence tz = timezoneFunc.getStrA(null);
        final TimeZoneRules rules = ToTimezoneTimestampFunctionFactory.constantZoneRules(tz, timezonePos, timestampType);
        if (rules != null) {
            return new ConstRulesFunc(timestampFunc, rules, timestampType);
        }
        return new OffsetTimestampFunction(timestampFunc, fixedOffset(tz, timestampType), timestampType);
    }

    private static class ConstRulesFunc extends AbstractTimestampShiftFunction implements UnaryFunction, MonotonicTimestampFunction {
        private final TimeZoneRules tzRules;

        public ConstRulesFunc(Function timestampFunc, TimeZoneRules tzRules, int timestampType) {
            super(timestampFunc, timestampType);
            this.tzRules = tzRules;
        }

        @Override
        public Function getArg() {
            return timestampFunc;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Function getTimestampArg() {
            return timestampFunc;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            return MonotonicTimestampFunction.invertZoneOffsetShift(io, tzRules, timestampDriver, 1);
        }

        @Override
        protected long shift(Record rec, long timestamp) {
            final long offset = tzRules.getLocalOffset(timestamp);
            return timestamp - offset;
        }
    }

    private static class Func extends AbstractTimestampShiftFunction implements BinaryFunction {
        private final Function timezoneFunc;

        public Func(Function timestampFunc, Function timezoneFunc, int timestampType) {
            super(timestampFunc, timestampType);
            this.timezoneFunc = timezoneFunc;
        }

        @Override
        public Function getLeft() {
            return timestampFunc;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Function getRight() {
            return timezoneFunc;
        }

        @Override
        protected long shift(Record rec, long timestampValue) {
            try {
                final CharSequence tz = timezoneFunc.getStrA(rec);
                return tz != null ? timestampDriver.toUTC(timestampValue, DateLocaleFactory.EN_LOCALE, tz) : timestampValue;
            } catch (NumericException e) {
                return timestampValue;
            }
        }
    }

    private static class RuntimeConstFunc extends AbstractTimestampShiftFunction implements BinaryFunction {
        private final Function timezoneFunc;
        private final int timezonePos;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public RuntimeConstFunc(Function timestampFunc, Function timezoneFunc, int timezonePos, int timestampType) {
            super(timestampFunc, timestampType);
            this.timezoneFunc = timezoneFunc;
            this.timezonePos = timezonePos;
        }

        @Override
        public Function getLeft() {
            return timestampFunc;
        }

        @Override
        public String getName() {
            return NAME;
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

        @Override
        protected long shift(Record rec, long timestamp) {
            if (tzRules != null) {
                final long offset = tzRules.getLocalOffset(timestamp);
                return timestamp - offset;
            }
            return timestamp - tzOffset;
        }
    }
}
