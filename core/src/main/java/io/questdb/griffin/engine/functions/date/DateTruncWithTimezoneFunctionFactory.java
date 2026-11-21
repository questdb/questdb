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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;

public class DateTruncWithTimezoneFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "date_trunc(sNS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function kindFunction = args.getQuick(0);
        final CharSequence kind = kindFunction.getStrA(null);
        final Function timestampFunc = args.getQuick(1);
        final Function tzFunc = args.getQuick(2);
        int timestampType = ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(timestampFunc.getType()), ColumnType.TIMESTAMP_MICRO);

        if (kind == null) {
            throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
        }

        final String unit = resolveUnit(kind, argPositions.getQuick(0));

        if (!tzFunc.isConstant()) {
            throw SqlException.$(argPositions.getQuick(2), "const expected");
        }

        final TimeZoneRules timeZoneRules;
        try {
            timeZoneRules = ColumnType.getTimestampDriver(timestampType).getTimezoneRules(DateLocaleFactory.EN_LOCALE, tzFunc.getStrA(null));
        } catch (CairoException e) {
            throw SqlException.position(argPositions.getQuick(2)).put(e.getFlyweightMessage());
        }

        final TimestampDriver.TimestampFloorMethod floor = ColumnType.getTimestampDriver(timestampType).getTimestampFloorMethod(unit);
        final String tzStr = Chars.toString(tzFunc.getStrA(null));

        return new DateTruncWithTzFunction(timestampFunc, tzFunc, unit, floor, timeZoneRules, tzStr, timestampType);
    }

    private static String resolveUnit(CharSequence kind, int position) throws SqlException {
        if (isTimeUnit(kind, "nanosecond")) {
            return "nanosecond";
        } else if (isTimeUnit(kind, "microsecond")) {
            return "microsecond";
        } else if (isTimeUnit(kind, "millisecond")) {
            return "millisecond";
        } else if (isTimeUnit(kind, "second")) {
            return "second";
        } else if (isTimeUnit(kind, "minute")) {
            return "minute";
        } else if (isTimeUnit(kind, "hour")) {
            return "hour";
        } else if (isTimeUnit(kind, "day")) {
            return "day";
        } else if (isTimeUnit(kind, "week")) {
            return "week";
        } else if (isTimeUnit(kind, "month")) {
            return "month";
        } else if (isTimeUnit(kind, "quarter")) {
            return "quarter";
        } else if (isTimeUnit(kind, "year")) {
            return "year";
        } else if (isTimeUnit(kind, "decade")) {
            return "decade";
        } else if (Chars.equals(kind, "century") || Chars.equals(kind, "centuries")) {
            return "century";
        } else if (isTimeUnit(kind, "millennium")) {
            return "millennium";
        } else {
            throw SqlException.$(position, "invalid unit '").put(kind).put('\'');
        }
    }

    private static boolean isTimeUnit(CharSequence arg, String constant) {
        if (Chars.startsWith(arg, constant)) {
            int argLen = arg.length();
            int constLen = constant.length();
            if (argLen == constLen) {
                return true;
            } else if (argLen == constLen + 1) {
                return arg.charAt(argLen - 1) == 's';
            }
        }
        return false;
    }

    private static class DateTruncWithTzFunction extends TimestampFunction implements BinaryFunction {
        private final TimestampDriver.TimestampFloorMethod floor;
        private final TimeZoneRules timeZoneRules;
        private final Function timestampFunc;
        private final Function tzFunc;
        private final String tzStr;
        private final String unit;

        public DateTruncWithTzFunction(
                Function timestampFunc,
                Function tzFunc,
                String unit,
                TimestampDriver.TimestampFloorMethod floor,
                TimeZoneRules timeZoneRules,
                String tzStr,
                int timestampType
        ) {
            super(timestampType);
            this.timestampFunc = timestampFunc;
            this.tzFunc = tzFunc;
            this.unit = unit;
            this.floor = floor;
            this.timeZoneRules = timeZoneRules;
            this.tzStr = tzStr;
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
            long timestamp = timestampFunc.getTimestamp(rec);
            if (timestamp == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            long offset = timeZoneRules.getOffset(timestamp);
            long localTimestamp = floor.floor(timestamp + offset);
            return localTimestamp - timeZoneRules.getLocalOffset(localTimestamp);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("date_trunc('").val(unit).val("',").val(timestampFunc).val(",'").val(tzStr).val("')");
        }
    }
}
