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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.ResultTypes;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunctionFactory;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.ObjList;

public class DateTruncFunctionFactory implements FunctionFactory, MonotonicTimestampFunctionFactory {
    @Override
    public int getResultType(IntList argTypes) {
        final int type = argTypes.getQuick(1);
        return ColumnType.isTimestamp(type) ? ResultTypes.timestampAtLeastMicros(type) : ColumnType.UNDEFINED;
    }

    @Override
    public String getSignature() {
        return "date_trunc(sN)";
    }

    @Override
    public int getTimestampArgumentIndex(FunctionExpression call, ConstantArguments arguments) {
        return 1;
    }

    @Override
    public int invertTimestampInterval(FunctionExpression call, Interval io, boolean isTimestampArgMonotonic, ConstantArguments arguments) throws SqlException {
        return TimestampFloorFunctions.invertFloor(io, ColumnType.getTimestampDriver(call.getDataType()),
                unit(arguments.constant(call.argumentAt(0)).getStrA(null), call.argumentAt(0).getPosition()));
    }

    @Override
    public boolean isConstructionDeferrable(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration) throws SqlException {
        final String unit = unit(args.getQuick(0).getStrA(null), argPositions.getQuick(0));
        return !isIdentity(unit, timestampType(args.getQuick(1)));
    }

    @Override
    public boolean isIdentity(FunctionExpression call, ConstantArguments arguments) throws SqlException {
        return isIdentity(unit(arguments.constant(call.argumentAt(0)).getStrA(null), call.argumentAt(0).getPosition()), call.getDataType());
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final String unit = unit(args.getQuick(0).getStrA(null), argPositions.getQuick(0));
        final Function innerFunction = args.getQuick(1);
        final int timestampType = timestampType(innerFunction);
        // optimize, nothing to truncate
        if (isIdentity(unit, timestampType)) {
            return innerFunction;
        }
        return new TimestampFloorFunctions.TimestampFloorFunction(innerFunction, unit, timestampType);
    }

    private static boolean isIdentity(String unit, int timestampType) {
        return unit.equals("nanosecond") && ColumnType.isTimestampNano(timestampType)
                || unit.equals("microsecond") && ColumnType.isTimestampMicro(timestampType);
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

    private static int timestampType(Function innerFunction) {
        return ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(innerFunction.getType()), ColumnType.TIMESTAMP_MICRO);
    }

    private static String unit(CharSequence kind, int position) throws SqlException {
        if (kind == null) {
            throw SqlException.position(position).put("invalid unit 'null'");
        }
        if (isTimeUnit(kind, "nanosecond")) {
            return "nanosecond";
        }
        if (isTimeUnit(kind, "microsecond")) {
            return "microsecond";
        }
        if (isTimeUnit(kind, "millisecond")) {
            return "millisecond";
        }
        if (isTimeUnit(kind, "second")) {
            return "second";
        }
        if (isTimeUnit(kind, "minute")) {
            return "minute";
        }
        if (isTimeUnit(kind, "hour")) {
            return "hour";
        }
        if (isTimeUnit(kind, "day")) {
            return "day";
        }
        if (isTimeUnit(kind, "week")) {
            return "week";
        }
        if (isTimeUnit(kind, "month")) {
            return "month";
        }
        if (isTimeUnit(kind, "quarter")) {
            return "quarter";
        }
        if (isTimeUnit(kind, "year")) {
            return "year";
        }
        if (isTimeUnit(kind, "decade")) {
            return "decade";
        }
        if (Chars.equals(kind, "century") || Chars.equals(kind, "centuries")) {
            return "century";
        }
        if (isTimeUnit(kind, "millennium")) {
            return "millennium";
        }
        throw SqlException.$(position, "invalid unit '").put(kind).put('\'');
    }
}
