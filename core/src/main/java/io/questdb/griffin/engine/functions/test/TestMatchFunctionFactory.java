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

package io.questdb.griffin.engine.functions.test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

public class TestMatchFunctionFactory implements FunctionFactory {

    private static final String SIGNATURE = "test_match()";
    private static final AtomicInteger closeCount = new AtomicInteger();
    private static final AtomicInteger openCounter = new AtomicInteger();
    private static final AtomicInteger topCounter = new AtomicInteger();

    public static boolean assertAPI(SqlExecutionContext executionContext) {
        return openCounter.get() > 0 && openCounter.get() >= closeCount.get() && topCounter.get() > 0
                // consider both single-threaded and parallel filter cases
                && (closeCount.get() == 1 || closeCount.get() == executionContext.getSharedQueryWorkerCount() + 1);
    }

    public static void clear() {
        openCounter.set(0);
        topCounter.set(0);
        closeCount.set(0);
    }

    public static boolean isClosed() {
        return closeCount.get() == 1;
    }

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new TestMatchFunction();
    }

    private static class TestMatchFunction extends BooleanFunction {

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        @Override
        public boolean getBool(Record rec) {
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext sqlExecutionContext) {
            openCounter.incrementAndGet();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }

        @Override
        public void toTop() {
            assert openCounter.get() > 0;
            topCounter.incrementAndGet();
        }
    }
}
