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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class TimestampFloorFunctionFactory implements FunctionFactory {
    public static final String NAME = "timestamp_floor";

    @Override
    public String getSignature() {
        return NAME + "(sN)";
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
        int stride = 1;
        char c = 0;
        if (str != null) {
            if (str.length() == 1) {
                c = str.charAt(0);
            } else if (str.length() > 1) {
                c = str.charAt(str.length() - 1);
                try {
                    stride = Numbers.parseInt(str, 0, str.length() - 1);
                    if (stride <= 0) {
                        c = 1;
                    }
                } catch (NumericException ignored) {
                    c = 1;
                }
            } else {
                c = 1; // report it as an empty unit rather than null
            }
        }
        Function arg = args.getQuick(1);
        int timestampType = ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(arg.getType()), ColumnType.TIMESTAMP_MICRO);
        switch (c) {
            case 'M':
                return createFloorFunction(arg, "month", stride, timestampType);
            case 'y':
                return createFloorFunction(arg, "year", stride, timestampType);
            case 'w':
                return createFloorFunction(arg, "week", stride, timestampType);
            case 'd':
                return createFloorFunction(arg, "day", stride, timestampType);
            case 'h':
                return createFloorFunction(arg, "hour", stride, timestampType);
            case 'm':
                return createFloorFunction(arg, "minute", stride, timestampType);
            case 's':
                return createFloorFunction(arg, "second", stride, timestampType);
            case 'T':
                return createFloorFunction(arg, "millisecond", stride, timestampType);
            case 'U':
                return createFloorFunction(arg, "microsecond", stride, timestampType);
            case 'n':
                return createFloorFunction(arg, "nanosecond", stride, timestampType);
            case 0:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
            default:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit '").put(str).put('\'');
        }
    }

    private static Function createFloorFunction(Function arg, String unit, int stride, int timestampType) {
        if (stride > 1) {
            return new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, unit, stride, timestampType);
        } else {
            return new TimestampFloorFunctions.TimestampFloorFunction(arg, unit, timestampType);
        }
    }
}
