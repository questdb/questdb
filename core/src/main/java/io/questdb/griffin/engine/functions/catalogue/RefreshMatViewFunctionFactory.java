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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewGraph;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class RefreshMatViewFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "refresh_mat_view(S)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(args.get(0));
    }

    private static class Func extends BooleanFunction implements UnaryFunction {
        private final Function function;
        private SqlExecutionContext executionContext;
        private MatViewGraph matViewGraph;

        public Func(Function arg) {
            this.function = arg;
        }

        @Override
        public void close() {
            super.close();
            this.executionContext = null;
        }

        @Override
        public Function getArg() {
            return function;
        }

        @Override
        public boolean getBool(Record rec) {
            if (executionContext == null) {
                return false;
            }

            CharSequence viewName = function.getStrA(rec);
            if (viewName == null) {
                return false;
            }
            TableToken token = executionContext.getTableTokenIfExists(viewName);
            if (token == null) {
                return false;
            }

            matViewGraph.refresh(token);
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            matViewGraph = executionContext.getCairoEngine().getMaterializedViewGraph();
            this.executionContext = executionContext;
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("refresh_mat_view('").val(function).val("')");
        }
    }
}
