/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

public class TimestampFloorFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp_floor(sN)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final char c = args.getQuick(0).getChar(null);
        switch (c) {
            case 'd':
                return new TimestampFloorFunctions.TimestampFloorDDFunction(args.getQuick(1));
            case 'M':
                return new TimestampFloorFunctions.TimestampFloorMMFunction(args.getQuick(1));
            case 'y':
                return new TimestampFloorFunctions.TimestampFloorYYYYFunction(args.getQuick(1));
            case 'h':
                return new TimestampFloorFunctions.TimestampFloorHHFunction(args.getQuick(1));
            case 'm':
                return new TimestampFloorFunctions.TimestampFloorMIFunction(args.getQuick(1));
            case 's':
                return new TimestampFloorFunctions.TimestampFloorSSFunction(args.getQuick(1));
            case 'T':
                return new TimestampFloorFunctions.TimestampFloorMSFunction(args.getQuick(1));
            case 0:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid kind 'null'");
            default:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid kind '").put(c).put('\'');
        }
    }
}
