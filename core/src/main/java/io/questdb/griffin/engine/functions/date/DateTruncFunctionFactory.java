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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class DateTruncFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "date_trunc(sN)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function kindFunction = args.getQuick(0);
        CharSequence kind = kindFunction.getStrA(null);
        Function innerFunction = args.getQuick(1);
        if (kind == null) {
            throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
        } else if (isTimeUnit(kind, "microsecond")) {
            // timestamps are in microseconds internally, there is nothing to truncate
            return innerFunction;
        } else if (isTimeUnit(kind, "millisecond")) {
            return new TimestampFloorFunctions.TimestampFloorMSFunction(innerFunction);
        } else if (isTimeUnit(kind, "second")) {
            return new TimestampFloorFunctions.TimestampFloorSSFunction(innerFunction);
        } else if (isTimeUnit(kind, "minute")) {
            return new TimestampFloorFunctions.TimestampFloorMIFunction(innerFunction);
        } else if (isTimeUnit(kind, "hour")) {
            return new TimestampFloorFunctions.TimestampFloorHHFunction(innerFunction);
        } else if (isTimeUnit(kind, "day")) {
            return new TimestampFloorFunctions.TimestampFloorDDFunction(innerFunction);
        } else if (isTimeUnit(kind, "week")) {
            return new TimestampFloorFunctions.TimestampFloorDayOfWeekFunction(innerFunction);
        } else if (isTimeUnit(kind, "month")) {
            return new TimestampFloorFunctions.TimestampFloorMMFunction(innerFunction);
        } else if (isTimeUnit(kind, "quarter")) {
            return new TimestampFloorFunctions.TimestampFloorQuarterFunction(innerFunction);
        } else if (isTimeUnit(kind, "year")) {
            return new TimestampFloorFunctions.TimestampFloorYYYYFunction(innerFunction);
        } else if (isTimeUnit(kind, "decade")) {
            return new TimestampFloorFunctions.TimestampFloorDecadeFunction(innerFunction);
        } else if (Chars.equals(kind, "century") || Chars.equals(kind, "centuries")) {
            return new TimestampFloorFunctions.TimestampFloorCenturyFunction(innerFunction);
        } else if (isTimeUnit(kind, "millennium")) {
            return new TimestampFloorFunctions.TimestampFloorMillenniumFunction(innerFunction);
        } else {
            throw SqlException.$(argPositions.getQuick(0), "invalid unit '").put(kind).put('\'');
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
}
