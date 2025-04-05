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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;


/**
 * Floors timestamps with modulo relative to a timestamp from 1970-01-01, as
 * well as an offset from the epoch start.
 * Meant to be used in SAMPLE BY to GROUP BY SQL rewrite.
 */
public class TimestampFloorFromOffsetFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "timestamp_floor(sNnS)";
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
        final int stride = Timestamps.getStrideMultiple(str);
        final char unit = Timestamps.getStrideUnit(str);
        final Function timestampFunc = args.getQuick(1);
        long from = args.getQuick(2).getTimestamp(null);
        if (from == Numbers.LONG_NULL) {
            from = 0;
        }
        final Function offsetFunc = args.getQuick(3);
        final int offsetPos = argPositions.getQuick(3);

        long offset = 0;
        if (offsetFunc.isConstant()) {
            final CharSequence offsetStr = offsetFunc.getStrA(null);
            if (offsetStr != null) {
                final long val = Timestamps.parseOffset(offsetStr);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(argPositions.getQuick(0), "invalid offset: ").put(offsetStr);
                }
                offset = Numbers.decodeLowInt(val) * Timestamps.MINUTE_MICROS;
            }

            switch (unit) {
                case 'M':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetMMFunction(timestampFunc, stride, from + offset);
                case 'y':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetYYYYFunction(timestampFunc, stride, from + offset);
                case 'w':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetWWFunction(timestampFunc, stride, from + offset);
                case 'd':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetDDFunction(timestampFunc, stride, from + offset);
                case 'h':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetHHFunction(timestampFunc, stride, from + offset);
                case 'm':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetMIFunction(timestampFunc, stride, from + offset);
                case 's':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetSSFunction(timestampFunc, stride, from + offset);
                case 'T':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetMSFunction(timestampFunc, stride, from + offset);
                case 'U':
                    return new TimestampFloorOffsetFunctions.TimestampFloorConstOffsetMCFunction(timestampFunc, stride, from + offset);
                case 0:
                    throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
                default:
                    throw SqlException.position(argPositions.getQuick(0)).put("invalid unit '").put(str).put('\'');
            }
        }

        if (offsetFunc.isRuntimeConstant()) {
            switch (unit) {
                case 'M':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetMMFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 'y':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetYYYYFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 'w':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetWWFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 'd':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetDDFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 'h':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetHHFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 'm':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetMIFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 's':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetSSFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 'T':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetMSFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 'U':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetMCFunction(timestampFunc, stride, from, offsetFunc, offsetPos);
                case 0:
                    throw SqlException.position(argPositions.getQuick(0)).put("invalid unit 'null'");
                default:
                    throw SqlException.position(argPositions.getQuick(0)).put("invalid unit '").put(str).put('\'');
            }
        }

        throw SqlException.$(argPositions.getQuick(3), "const or runtime const expected");
    }
}
