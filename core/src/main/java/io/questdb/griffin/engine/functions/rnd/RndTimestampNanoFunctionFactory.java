/*******************************************************************************
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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class RndTimestampNanoFunctionFactory implements FunctionFactory {
    private static final String NAME = "rnd_timestamp_ns";
    private static final String SIGNATURE = NAME + "(nni)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function arg = args.getQuick(0);
        Function arg2 = args.getQuick(1);
        TimestampDriver driver = NanosTimestampDriver.INSTANCE;
        final long lo = driver.from(arg.getTimestamp(null), arg.getType());
        final long hi = driver.from(arg2.getTimestamp(null), arg2.getType());
        final int nanRate = args.getQuick(2).getInt(null);

        if (nanRate < 0) {
            throw SqlException.$(argPositions.getQuick(2), "invalid NaN rate");
        }

        if (lo < hi) {
            return new RndTimestampFunctionFactory.Func(NAME, lo, hi, nanRate, ColumnType.TIMESTAMP_NANO);
        }

        throw SqlException.$(position, "invalid range");
    }
}
