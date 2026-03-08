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

package io.questdb.griffin.engine.functions.activity;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CancelQueryFunctionFactory implements FunctionFactory {
    private static final String NAME = "cancel_query";
    private static final String SIGNATURE = NAME + "(L)";

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
    ) throws SqlException {
        if (args.get(0).isConstant() && args.get(0).getLong(null) < 0) {
            throw SqlException.$(argPositions.getQuick(0), "non-negative integer literal expected as query id");
        }
        return new Func(sqlExecutionContext.getCairoEngine().getQueryRegistry(), args.getQuick(0), position);
    }

    private static class Func extends BooleanFunction {
        private final int position;
        private final Function queryIdFunc;
        private final QueryRegistry queryRegistry;
        private SqlExecutionContext executionContext;

        public Func(QueryRegistry queryRegistry, Function queryIdFunc, int position) {
            this.queryRegistry = queryRegistry;
            this.queryIdFunc = queryIdFunc;
            this.position = position;
        }

        @Override
        public boolean getBool(Record rec) {
            long queryId = queryIdFunc.getLong(rec);
            if (queryId < 0) {
                return false;
            }
            try {
                return queryRegistry.cancel(queryId, executionContext);
            } catch (CairoException e) {
                e.position(position);
                throw e;
            }
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            this.executionContext = executionContext;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME).val('(').val(queryIdFunc).val(')');
        }
    }
}
