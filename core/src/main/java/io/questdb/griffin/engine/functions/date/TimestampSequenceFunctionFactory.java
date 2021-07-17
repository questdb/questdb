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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class TimestampSequenceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp_sequence(NL)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        if (args.getQuick(0).isConstant()) {
            final long start = args.getQuick(0).getTimestamp(null);
            if (start == Numbers.LONG_NaN) {
                return TimestampConstant.NULL;
            }
            return new TimestampSequenceFunction(start, args.getQuick(1));
        } else {
            return new TimestampSequenceVariableFunction(args.getQuick(0), args.getQuick(1));
        }
    }

    private static class TimestampSequenceFunction extends TimestampFunction {
        private final Function longIncrement;
        private final long start;
        private long next;

        public TimestampSequenceFunction(long start, Function longIncrement) {
            this.start = start;
            this.next = start;
            this.longIncrement = longIncrement;
        }

        @Override
        public void close() {
        }

        @Override
        public long getTimestamp(Record rec) {
            final long result = next;
            next += longIncrement.getLong(rec);
            return result;
        }

        @Override
        public void toTop() {
            next = start;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            longIncrement.init(symbolTableSource, executionContext);
        }
    }

    private static final class TimestampSequenceVariableFunction extends TimestampFunction {
        private final Function longIncrement;
        private final Function start;
        private long next;

        public TimestampSequenceVariableFunction(Function start, Function longIncrement) {
            this.start = start;
            this.next = 0;
            this.longIncrement = longIncrement;
        }

        @Override
        public void close() {
            Misc.free(start);
            Misc.free(longIncrement);
        }

        @Override
        public long getTimestamp(Record rec) {
            final long result = next;
            next += longIncrement.getLong(rec);
            return result + start.getLong(rec);
        }

        @Override
        public void toTop() {
            next = 0;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            start.init(symbolTableSource, executionContext);
            longIncrement.init(symbolTableSource, executionContext);
        }
    }
}
