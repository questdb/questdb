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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class TimestampShuffleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp_shuffle(nn)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function arg = args.getQuick(0);
        Function arg2 = args.getQuick(1);
        int argType = ColumnType.getTimestampType(arg.getType());
        int arg2Type = ColumnType.getTimestampType(arg2.getType());
        int timestampType = ColumnType.getHigherPrecisionTimestampType(argType, arg2Type);
        timestampType = ColumnType.getHigherPrecisionTimestampType(timestampType, ColumnType.TIMESTAMP_MICRO);
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        long start = driver.from(arg.getTimestamp(null), argType);
        long end = driver.from(arg2.getTimestamp(null), arg2Type);
        if (start == Numbers.LONG_NULL || end == Numbers.LONG_NULL) {
            return driver.getTimestampConstantNull();
        }

        if (start <= end) {
            return new TimestampShuffleFunction(start, end, timestampType);
        } else {
            return new TimestampShuffleFunction(end, start, timestampType);
        }
    }

    private static class TimestampShuffleFunction extends TimestampFunction {
        private final long end;
        private final long start;
        private Rnd rnd;

        public TimestampShuffleFunction(long start, long end, int columnType) {
            super(columnType);
            this.start = start;
            this.end = end;
        }

        @Override
        public long getTimestamp(Record rec) {
            return start + rnd.nextPositiveLong() % (end - start);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            rnd = executionContext.getRandom();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("timestamp_shuffle(").val(start).val(',').val(end).val(')');
        }

        @Override
        public void toTop() {
            rnd.reset();
        }
    }
}
