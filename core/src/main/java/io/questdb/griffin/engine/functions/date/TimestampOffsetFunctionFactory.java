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
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;

import static io.questdb.std.datetime.microtime.Timestamps.MINUTE_MICROS;

public class TimestampOffsetFunctionFactory implements FunctionFactory {
    private static final Function ZERO = TimestampConstant.newInstance(0);

    @Override
    public String getSignature() {
        return "timestamp_offset(s)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function func = args.getQuick(0);
        if (func.isConstant()) {
            final CharSequence offset = func.getStrA(null);
            if (offset == null) {
                return ZERO;
            }
            final long val = Timestamps.parseOffset(offset);
            if (val == Numbers.LONG_NULL) {
                // bad value for offset
                throw SqlException.$(argPositions.getQuick(0), "invalid offset: ").put(offset);
            }
            return TimestampConstant.newInstance(Numbers.decodeLowInt(val) * MINUTE_MICROS);
        }
        // TODO
        return null;
    }
}
