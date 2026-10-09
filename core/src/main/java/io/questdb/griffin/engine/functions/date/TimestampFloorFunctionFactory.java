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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.ResultTypes;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunctionFactory;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class TimestampFloorFunctionFactory implements FunctionFactory, MonotonicTimestampFunctionFactory {
    public static final String NAME = "timestamp_floor";

    @Override
    public int getResultType(IntList argTypes) {
        return ResultTypes.timestampAtLeastMicros(argTypes.getQuick(1));
    }

    @Override
    public String getSignature() {
        return NAME + "(sN)";
    }

    @Override
    public int getTimestampArgumentIndex(FunctionExpression call, ConstantArguments arguments) {
        return 1;
    }

    @Override
    public int invertTimestampInterval(FunctionExpression call, Interval io, boolean isTimestampArgMonotonic, ConstantArguments arguments) throws SqlException {
        final CharSequence str = arguments.constant(call.argumentAt(0)).getStrA(null);
        final int stride = stride(str);
        final String unit = unitName(stride > 0 ? unit(str) : 1, str, call.argumentAt(0).getPosition());
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(call.getDataType());
        return stride > 1 ? TimestampFloorFunctions.invertFloorWithStride(io, timestampDriver, unit, stride)
                : TimestampFloorFunctions.invertFloor(io, timestampDriver, unit);
    }

    @Override
    public boolean isConstructionDeferrable(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration) throws SqlException {
        final CharSequence str = args.getQuick(0).getStrA(null);
        final char c = stride(str) > 0 ? unit(str) : 1;
        return switch (c) {
            case 'M', 'y', 'w', 'd', 'h', 'm', 's', 'T', 'U', 'n' -> true;
            default -> throw invalidUnit(c, str, argPositions.getQuick(0));
        };
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final CharSequence str = args.getQuick(0).getStrA(null);
        final int stride = stride(str);
        final char c = stride > 0 ? unit(str) : 1;
        Function arg = args.getQuick(1);
        int timestampType = ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(arg.getType()), ColumnType.TIMESTAMP_MICRO);
        return createFloorFunction(arg, unitName(c, str, argPositions.getQuick(0)), stride, timestampType);
    }

    private static Function createFloorFunction(Function arg, String unit, int stride, int timestampType) {
        if (stride > 1) {
            return new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, unit, stride, timestampType);
        } else {
            return new TimestampFloorFunctions.TimestampFloorFunction(arg, unit, timestampType);
        }
    }

    private static SqlException invalidUnit(char unit, CharSequence str, int position) {
        return unit == 0
                ? SqlException.position(position).put("invalid unit 'null'")
                : SqlException.position(position).put("invalid unit '").put(str).put('\'');
    }

    private static int stride(CharSequence str) {
        if (str == null || str.length() < 2) {
            return 1;
        }
        try {
            return Numbers.parseInt(str, 0, str.length() - 1);
        } catch (NumericException e) {
            return 0;
        }
    }

    /**
     * The last letter of a unit spelling: 0 for a null spelling, 1 for an empty one.
     */
    private static char unit(CharSequence str) {
        if (str == null) {
            return 0;
        }
        return str.isEmpty() ? 1 : str.charAt(str.length() - 1);
    }

    private static String unitName(char c, CharSequence str, int position) throws SqlException {
        return switch (c) {
            case 'M' -> "month";
            case 'y' -> "year";
            case 'w' -> "week";
            case 'd' -> "day";
            case 'h' -> "hour";
            case 'm' -> "minute";
            case 's' -> "second";
            case 'T' -> "millisecond";
            case 'U' -> "microsecond";
            case 'n' -> "nanosecond";
            default -> throw invalidUnit(c, str, position);
        };
    }
}
