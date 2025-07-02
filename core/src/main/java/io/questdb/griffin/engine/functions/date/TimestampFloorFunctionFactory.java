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
        switch (c) {
            case 'M':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "month", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "month", ColumnType.TIMESTAMP_MICRO);
            case 'y':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "year", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "year", ColumnType.TIMESTAMP_MICRO);
            case 'w':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "week", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "week", ColumnType.TIMESTAMP_MICRO);
            case 'd':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "day", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "day", ColumnType.TIMESTAMP_MICRO);
            case 'h':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "hour", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "hour", ColumnType.TIMESTAMP_MICRO);
            case 'm':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "minute", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "minute", ColumnType.TIMESTAMP_MICRO);
            case 's':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "second", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "second", ColumnType.TIMESTAMP_MICRO);
            case 'T':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "millisecond", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "millisecond", ColumnType.TIMESTAMP_MICRO);
            case 'U':
                return stride > 1 ?
                        new TimestampFloorFunctions.TimestampFloorWithStrideFunction(args.getQuick(1), "microsecond", stride, ColumnType.TIMESTAMP_MICRO) :
                        new TimestampFloorFunctions.TimestampFloorFunction(args.getQuick(1), "microsecond", ColumnType.TIMESTAMP_MICRO);
            case 0:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
            default:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit '").put(str).put('\'');
        }
    }
}
