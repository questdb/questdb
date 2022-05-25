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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.Long128Function;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class IndexedTimestampSequenceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "indexed_timestamp_sequence(lNl)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final long indexStart = args.getQuick(0).getLong(null);
        final long start = args.getQuick(1).getTimestamp(null);
        if (start == Numbers.LONG_NaN) {
            return TimestampConstant.NULL;
        }
        return new IndexedTimestampSequenceFunction(indexStart, start, args.getQuick(2));
    }

    private static class IndexedTimestampSequenceFunction extends Long128Function {
        private final Function longIncrement;
        private final long start;
        private final long indexStart;
        private final Long128Impl long128a = new Long128Impl();
        private final Long128Impl long128b = new Long128Impl();
        private long next;
        private long indexNext;

        public IndexedTimestampSequenceFunction(long indexStart, long start, Function longIncrement) {
            this.indexNext = this.indexStart = indexStart;
            this.next = this.start = start;
            this.longIncrement = longIncrement;
        }

        @Override
        public void close() {
        }

        @Override
        public void getLong128(Record rec, CharSink sink) {
            Long128Impl v = (Long128Impl) getLong128A(rec);
            v.toSink(sink);
        }

        @Override
        public Long128 getLong128A(Record rec) {
            long128a.setAll(next, indexNext);

            next += longIncrement.getLong(rec);
            indexNext++;

            return long128a;
        }

        @Override
        public Long128 getLong128B(Record rec) {
            long128b.setAll(next, indexNext);

            next += longIncrement.getLong(rec);
            indexNext++;

            return long128b;
        }

        @Override
        public void toTop() {
            next = start;
            indexNext = indexStart;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            longIncrement.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }
    }
}
