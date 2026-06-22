/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class YearFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "year(N)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        return new YearFunction(arg, ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType())));
    }

    public static final class YearFunction extends IntFunction implements UnaryFunction, MonotonicTimestampFunction {
        private final Function arg;
        private final TimestampDriver driver;

        public YearFunction(Function arg, TimestampDriver driver) {
            super();
            this.arg = arg;
            this.driver = driver;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getTimestamp(rec);
            return driver.getYear(value);
        }

        @Override
        public String getName() {
            return "year";
        }

        @Override
        public Function getTimestampArg() {
            return arg;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            // An out-of-range year maps to an empty or unbounded interval, never NONE:
            // the grade must not depend on the bound, else the runtime path (which drops
            // the filter on an EXACT probe) would return wrong rows.
            long lo = io.getLo();
            long hi = io.getHi();
            if (lo != Numbers.LONG_NULL) {
                if (lo < 1) {
                    lo = Numbers.LONG_NULL;
                } else if (lo > 300_000) {
                    io.of(Long.MAX_VALUE, Numbers.LONG_NULL);
                    return EXACT;
                } else {
                    final long start = driver.addYears(0, (int) (lo - 1970));
                    if (driver.getYear(start) != lo) {
                        io.of(Long.MAX_VALUE, Numbers.LONG_NULL);
                        return EXACT;
                    }
                    lo = start;
                }
            }
            if (hi != Long.MAX_VALUE) {
                if (hi < 1) {
                    io.of(Long.MAX_VALUE, Numbers.LONG_NULL);
                    return EXACT;
                } else if (hi > 300_000) {
                    hi = Long.MAX_VALUE;
                } else {
                    final long nextStart = driver.addYears(0, (int) (hi + 1 - 1970));
                    if (driver.getYear(nextStart) != hi + 1) {
                        hi = Long.MAX_VALUE;
                    } else {
                        hi = nextStart - 1;
                    }
                }
            }
            io.of(lo, hi);
            return EXACT;
        }
    }
}
