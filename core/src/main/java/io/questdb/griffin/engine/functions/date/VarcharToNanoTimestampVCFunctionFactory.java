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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateLocale;

public final class VarcharToNanoTimestampVCFunctionFactory extends ToTimestampVCFunctionFactory {
    private final static String NAME = "to_timestamp_ns";

    @Override
    public String getSignature() {
        return "to_timestamp_ns(Øs)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arg = args.getQuick(0);
        final CharSequence pattern = args.getQuick(1).getStrA(null);
        if (pattern == null) {
            throw SqlException.$(argPositions.getQuick(1), "pattern is required");
        }
        DateLocale defaultDateLocale = configuration.getDefaultDateLocale();
        if (arg.isConstant()) {
            return evaluateConstant(arg, pattern, defaultDateLocale, ColumnType.TIMESTAMP_NANO);
        } else {
            if ("en".equals(defaultDateLocale.getName()) || (defaultDateLocale.getName() != null && defaultDateLocale.getName().startsWith("en-"))) {
                return new VarcharToTimestampVCFunctionFactory.ToAsciiTimestampFunc(arg, pattern, defaultDateLocale, ColumnType.TIMESTAMP_NANO, NAME);
            }
            return new Func(arg, pattern, defaultDateLocale, ColumnType.TIMESTAMP_NANO, NAME);
        }
    }
}
