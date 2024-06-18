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
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;


/**
 * Floors timestamps with modulo relative to an offset from 1970-01-01.
 * Takes a stride (i.e 5d), the timestamp to round, and the offset timestamp.
 */
public class TimestampFloorOffsetFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp_floor_offset(sNn)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final CharSequence str = args.getQuick(0).getStrA(null);
        final int stride = Timestamps.getStrideMultiple(str);
        final char unit = Timestamps.getStrideUnit(str);
        final Function timestamp = args.getQuick(1);
        final long offset = args.getQuick(2).getTimestamp(null);

        switch (unit) {
            case 'M':
                return new TimestampFloorFunctions.TimestampFloorMMFunction(timestamp, stride, offset);
            case 'y':
                return new TimestampFloorFunctions.TimestampFloorYYYYFunction(timestamp, stride, offset);
            case 'w':
                return new TimestampFloorFunctions.TimestampFloorWWFunction(timestamp, stride, offset);
            case 'd':
                return new TimestampFloorFunctions.TimestampFloorDDFunction(timestamp, stride, offset);
//            case 'h':
//                return new TimestampFloorFunctions.TimestampFloorHHFunction(timestamp, stride, offset);
//            case 'm':
//                return new TimestampFloorFunctions.TimestampFloorMIFunction(timestamp, stride, offset);
//            case 's':
//                return new TimestampFloorFunctions.TimestampFloorSSFunction(timestamp, stride, offset);
            case 'T':
                return new TimestampFloorFunctions.TimestampFloorMSFunction(timestamp, stride, offset);
//            case 'U':
//                return new TimestampFloorFunctions.TimestampFloorMCFunction(timestamp, stride, offset);
            case 0:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
            default:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid unit '").put(str).put('\'');
        }
    }
}
