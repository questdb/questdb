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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cutlass.http.processors.WarningsProcessor;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class SimulateWarningsFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "simulate_warnings(SS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        if (configuration.isDevModeEnabled()) {
            final String tag = args.get(0).getStrA(null).toString();
            final String warning = args.get(1).getStrA(null).toString();
            return new SimulateWarningsFunction(tag, warning);
        }
        return BooleanConstant.FALSE;
    }

    private static class SimulateWarningsFunction extends BooleanFunction {
        final String tag;
        final String warning;

        SimulateWarningsFunction(String tag, String warning) {
            this.tag = tag;
            this.warning = warning;
        }

        @Override
        public boolean getBool(Record rec) {
            WarningsProcessor.override(tag, warning);
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            executionContext.getSecurityContext().authorizeSystemAdmin();
            super.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("simulate_warnings(").val(tag).val(", ").val(warning).val(")");
        }
    }
}
