/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.MillisecondClock;

public class TestSleepFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "sleep(l)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function arg = args.getQuick(0);
        long sleepMillis = arg.getLong(null);

        return new Func(configuration, sleepMillis);
    }

    static class Func extends BooleanFunction {

        private final MillisecondClock clock;
        private final long sleepMillis;
        private SqlExecutionCircuitBreaker circuitBreaker;

        public Func(CairoConfiguration configuration, long sleepMillis) {
            clock = configuration.getMillisecondClock();
            this.sleepMillis = sleepMillis;
        }

        @Override
        public boolean getBool(Record rec) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            long sleepStart = clock.getTicks();
            while ((clock.getTicks() - sleepStart) < sleepMillis) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                Os.sleep(1);
            }

            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            circuitBreaker = executionContext.getCircuitBreaker();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("sleep(").val(sleepMillis).val(')');
        }
    }
}
