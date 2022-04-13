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

public class DateTruncFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "date_trunc(sN)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function kindFunction = args.getQuick(0);
        CharSequence kind = kindFunction.getStr(null);
        Function innerFunction = args.getQuick(1);
        if (kind == null) {
            throw SqlException.position(argPositions.getQuick(0)).put("invalid kind 'null'");
        } else if ("microseconds".contentEquals(kind)) {
            // timestamps are in microseconds internally, there is nothing to truncate
            return innerFunction;
        } else if ("milliseconds".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorMSFunction(innerFunction);
        } else if ("second".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorSSFunction(innerFunction);
        } else if ("minute".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorMIFunction(innerFunction);
        } else if ("hour".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorHHFunction(innerFunction);
        } else if ("day".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorDDFunction(innerFunction);
        } else if ("week".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorDayOfWeekFunction(innerFunction);
        } else if ("month".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorMMFunction(innerFunction);
        } else if ("quarter".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorQuarterFunction(innerFunction);
        } else if ("year".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorYYYYFunction(innerFunction);
        } else if ("decade".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorDecadeFunction(innerFunction);
        } else if ("century".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorCenturyFunction(innerFunction);
        } else if ("millennium".contentEquals(kind)) {
            return new TimestampFloorFunctions.TimestampFloorMillenniumFunction(innerFunction);
        } else {
            throw SqlException.position(argPositions.getQuick(0)).put("invalid kind '").put(kind).put('\'');
        }
    }
}
