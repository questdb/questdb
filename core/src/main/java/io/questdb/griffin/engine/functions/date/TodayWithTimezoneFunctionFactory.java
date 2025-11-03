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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class TodayWithTimezoneFunctionFactory implements FunctionFactory {
    private static final String SIGNATURE = "today(S)";

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
    ) {
        final Function tzFunc = args.getQuick(0);
        if (tzFunc.isConstant() || tzFunc.isRuntimeConstant()) {
            return new RuntimeConstFunc(sqlExecutionContext.getIntervalFunctionType(), tzFunc);
        }
        return new Func(sqlExecutionContext.getIntervalFunctionType(), tzFunc);
    }

    private static class Func extends AbstractDayIntervalWithTimezoneFunction {
        private long now;

        public Func(int intervalType, Function tzFunc) {
            super(intervalType, tzFunc);
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            return calculateInterval(now, tzFunc.getStrA(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            now = executionContext.getNow(timestampDriver.getTimestampType());
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }

        @Override
        protected int shiftFromToday() {
            return 0;
        }
    }

    private static class RuntimeConstFunc extends AbstractDayIntervalWithTimezoneFunction {

        public RuntimeConstFunc(int intervalType, Function tzFunc) {
            super(intervalType, tzFunc);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            calculateInterval(executionContext.getNow(timestampDriver.getTimestampType()), tzFunc.getStrA(null));
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }

        @Override
        protected int shiftFromToday() {
            return 0;
        }
    }
}
