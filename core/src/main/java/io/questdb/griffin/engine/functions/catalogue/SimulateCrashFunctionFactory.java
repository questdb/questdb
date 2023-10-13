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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class SimulateCrashFunctionFactory implements FunctionFactory {

    private static final CairoErrorFunction CairoErrorInstance = new CairoErrorFunction();
    private static final CairoExceptionFunction CairoExceptionInstance = new CairoExceptionFunction();
    private static final SimulateCrashFunction CrashInstance = new SimulateCrashFunction();
    private static final DoNothingInstance Dummy = new DoNothingInstance();
    private static final OutOfMemoryFunction OutOfMemoryInstance = new OutOfMemoryFunction();

    @Override
    public String getSignature() {
        return "simulate_crash(a)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {

        if (configuration.getSimulateCrashEnabled()) {
            char killType = args.get(0).getChar(null);
            switch (killType) {
                case '0':
                    return CrashInstance;
                case 'M':
                    return OutOfMemoryInstance;
                case 'C':
                    return CairoErrorInstance;
                case 'D':
                    return CairoExceptionInstance;
            }
        }
        return Dummy;
    }

    private static class CairoErrorFunction extends BooleanFunction {

        @Override
        public boolean getBool(Record rec) {
            throw new CairoError("simulated cairo error");
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            executionContext.getSecurityContext().authorizeAdminAction();
        }

        @Override
        public boolean isReadThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("simulate_crash(jvm)");
        }
    }

    private static class CairoExceptionFunction extends BooleanFunction {

        @Override
        public boolean getBool(Record rec) {
            throw CairoException.critical(1).put("simulated cairo exception");
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            executionContext.getSecurityContext().authorizeAdminAction();
        }

        @Override
        public boolean isReadThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("simulate_crash(cairoException)");
        }
    }

    private static class DoNothingInstance extends BooleanFunction {
        @Override
        public boolean getBool(Record rec) {
            return false;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            executionContext.getSecurityContext().authorizeAdminAction();
        }

        @Override
        public boolean isReadThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("simulate_crash(jvm)");
        }
    }

    private static class OutOfMemoryFunction extends BooleanFunction {
        @Override
        public boolean getBool(Record rec) {
            throw new OutOfMemoryError("simulate_crash('M')");
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            executionContext.getSecurityContext().authorizeAdminAction();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("simulate_crash(oom)");
        }
    }

    private static class SimulateCrashFunction extends BooleanFunction {
        @Override
        public boolean getBool(Record rec) {
            Unsafe.getUnsafe().getLong(0L);
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            executionContext.getSecurityContext().authorizeAdminAction();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("simulate_crash(dummy)");
        }
    }
}
