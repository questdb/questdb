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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import org.jetbrains.annotations.NotNull;

public class ToTimezoneTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "to_timezone(NS)";
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
            return toTimezoneConstFunction(timestampFunc, timezoneFunc, timezonePos, timestampType);
        } else if (timezoneFunc.isRuntimeConstant()) {
            return new RuntimeConstFunc(timestampFunc, timezoneFunc, timezonePos, timestampType);
        } else {
            return new Func(timestampFunc, timezoneFunc, timestampType);
        }
    }

    @NotNull
    private static TimestampFunction toTimezoneConstFunction(
            Function timestampFunc,
            Function timezoneFunc,
            int timezonePos,
            int timestampType
    ) throws SqlException {
        final CharSequence tz = timezoneFunc.getStrA(null);
        if (tz != null) {
            TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
            final int hi = tz.length();
            final long l = Dates.parseOffset(tz, 0, hi);
            if (l == Long.MIN_VALUE) {
                try {
                    return new ConstRulesFunc(
                            timestampFunc,
                            DateLocaleFactory.EN_LOCALE.getZoneRules(
                                    Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, hi)), timestampDriver.getTZRuleResolution()
                            ),
                            timestampType
                    );
                } catch (NumericException e) {
                    Misc.free(timestampFunc);
                    throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                }
            } else {
                return new OffsetTimestampFunction(
                        timestampFunc,
                        timestampDriver.fromMinutes(Numbers.decodeLowInt(l)),
                        timestampType
                );
            }
        }
        throw SqlException.$(timezonePos, "timezone must not be null");
    }

    private static class ConstRulesFunc extends TimestampFunction implements UnaryFunction {
        private final Function timestampFunc;
        private final TimeZoneRules tzRules;

        public ConstRulesFunc(Function timestampFunc, TimeZoneRules tzRules, int timestampType) {
            super(timestampType);
            this.timestampFunc = timestampFunc;
            this.tzRules = tzRules;
        }

        @Override
        public Function getArg() {
            return timestampFunc;
        }

        @Override
        public String getName() {
            return "to_timezone";
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            return timestamp + tzRules.getOffset(timestamp);
        }
    }

    private static class Func extends TimestampFunction implements BinaryFunction {
        private final Function timestampFunc;
        private final Function timezoneFunc;

        public Func(Function timestampFunc, Function timezoneFunc, int timestampType) {
            super(timestampType);
            this.timestampFunc = timestampFunc;
            this.timezoneFunc = timezoneFunc;
        }

        @Override
        public Function getLeft() {
            return timestampFunc;
        }

        @Override
        public String getName() {
            return "to_timezone";
        }

        @Override
        public Function getRight() {
            return timezoneFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestampValue = timestampFunc.getTimestamp(rec);
            try {
                final CharSequence tz = timezoneFunc.getStrA(rec);
                return tz != null ? timestampDriver.toTimezone(timestampValue, DateLocaleFactory.EN_LOCALE, tz) : timestampValue;
            } catch (NumericException e) {
                return timestampValue;
            }
        }
    }

    private static class RuntimeConstFunc extends TimestampFunction implements BinaryFunction {
        private final Function timestampFunc;
        private final Function timezoneFunc;
        private final int timezonePos;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public RuntimeConstFunc(Function timestampFunc, Function timezoneFunc, int timezonePos, int timestampType) {
            super(timestampType);
            this.timestampFunc = timestampFunc;
            this.timezoneFunc = timezoneFunc;
            this.timezonePos = timezonePos;
        }

        @Override
        public Function getLeft() {
            return timestampFunc;
        }

        @Override
        public String getName() {
            return "to_timezone";
        }

        @Override
        public Function getRight() {
            return timezoneFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestamp = timestampFunc.getTimestamp(rec);
            if (tzRules != null) {
                return timestamp + tzRules.getOffset(timestamp);
            }
            return timestamp + tzOffset;
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
