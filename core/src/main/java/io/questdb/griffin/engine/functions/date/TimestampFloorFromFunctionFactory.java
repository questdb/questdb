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
import io.questdb.std.ObjList;
import io.questdb.std.datetime.CommonUtils;


/**
 * Floors timestamps with modulo relative to a timestamp from 1970-01-01.
 * Takes a stride (i.e. 5d), the timestamp to round, and the offset timestamp.
 */
public class TimestampFloorFromFunctionFactory implements FunctionFactory, MonotonicTimestampFunctionFactory {

    @Override
    public int getResultType(IntList argTypes) {
        return ResultTypes.timestampAtLeastMicros(argTypes.getQuick(1));
    }

    @Override
    public String getSignature() {
        return TimestampFloorFunctionFactory.NAME + "(sNn)";
    }

    @Override
    public int getTimestampArgumentIndex(FunctionExpression call, ConstantArguments arguments) {
        return 1;
    }

    @Override
    public int invertTimestampInterval(FunctionExpression call, Interval io, boolean isTimestampArgMonotonic, ConstantArguments arguments) throws SqlException {
        final CharSequence str = arguments.constant(call.argumentAt(0)).getStrA(null);
        final int position = call.argumentAt(0).getPosition();
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(call.getDataType());
        return TimestampFloorOffsetFunction.invert(io, timestampDriver, CommonUtils.getStrideUnit(str, position), CommonUtils.getStrideMultiple(str, position),
                floorOrigin(timestampDriver, arguments.constant(call.argumentAt(2)).getTimestamp(null), call.argumentAt(2).getDataType()));
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
        final int stride = CommonUtils.getStrideMultiple(str, argPositions.getQuick(0));
        final char unit = CommonUtils.getStrideUnit(str, argPositions.getQuick(0));
        final Function timestampFunc = args.getQuick(1);
        int timestampType = ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(timestampFunc.getType()), ColumnType.TIMESTAMP_MICRO);
        final long from = floorOrigin(ColumnType.getTimestampDriver(timestampType), args.getQuick(2).getTimestamp(null), args.getQuick(2).getType());
        return new TimestampFloorOffsetFunction(TimestampFloorFunctionFactory.NAME, timestampFunc, unit, stride, from, timestampType);
    }

    static long floorOrigin(TimestampDriver timestampDriver, long from, int fromType) {
        return from == Numbers.LONG_NULL ? 0 : timestampDriver.from(from, ColumnType.getTimestampType(fromType));
    }
}
