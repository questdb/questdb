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
        int timestampType = ColumnType.getTimestampType(arg.getType(), configuration);
        switch (c) {
            case 'M':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "month", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "month", timestampType);
            case 'y':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "year", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "year", timestampType);
            case 'w':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "week", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "week", timestampType);
            case 'd':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "day", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "day", timestampType);
            case 'h':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "hour", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "hour", timestampType);
            case 'm':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "minute", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "minute", timestampType);
            case 's':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "second", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "second", timestampType);
            case 'T':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "millisecond", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "millisecond", timestampType);
            case 'U':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(arg, "microsecond", stride, timestampType) :
                        new TimestampFloorFunctions.TimestampFloorFunction(arg, "microsecond", timestampType);
            case 0:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
            default:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit '").put(str).put('\'');
        }
    }
}
