/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class TimestampShuffleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp_shuffle(nn)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        final long start = args.getQuick(0).getTimestamp(null);
        final long end = args.getQuick(1).getTimestamp(null);
        if (start == Numbers.LONG_NaN || end == Numbers.LONG_NaN) {
            return new TimestampConstant(args.getQuick(0).getPosition(), Numbers.LONG_NaN);
        }
        return new TimestampShuffleFunction(position, start, end);
    }

    private static class TimestampShuffleFunction extends TimestampFunction {
        private final long start;
        private final long end;
        private Rnd rnd;

        public TimestampShuffleFunction(int position, long start, long end) {
            super(position);
            this.start = start;
            this.end = end;
        }

        @Override
        public void close() {
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
        public void toTop() {
            rnd.reset();
        }
    }
}
