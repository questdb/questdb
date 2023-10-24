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

package io.questdb.griffin.engine.analytic;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.VirtualFunctionDirectSymbolRecordCursor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/*
 * Factory implements select with analytic functions that support streaming, that is:
 * - they don't specify order by or order by is the same as underlying query
 * - all functions and their framing clause do support stream-ed processing (single pass)
 */
public class AnalyticRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<AnalyticFunction> analyticFunctions;
    private final int analyticFunctionsCount;
    private final RecordCursorFactory base;

    private final AnalyticRecordCursor cursor;
    private final ObjList<Function> functions;
    private boolean closed = false;

    public AnalyticRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions
    ) {
        super(metadata);
        this.base = base;
        this.functions = functions;

        analyticFunctions = new ObjList<AnalyticFunction>();
        for (int i = 0, n = functions.size(); i < n; i++) {
            Function func = functions.getQuick(i);
            if (func instanceof AnalyticFunction) {
                analyticFunctions.add((AnalyticFunction) func);
            }
        }
        analyticFunctionsCount = analyticFunctions.size();

        //random access is not supported because analytic function value depends on the window/frame context and can't be computed from single row alone
        //e.g. even though we might be able to skip to a rowId, we'd still need to compute values for all the rows in between
        this.cursor = new AnalyticRecordCursor(functions, false);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        cursor.of(baseCursor, executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Analytic");
        sink.optAttr("functions", analyticFunctions, true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private void resetFunctions() {
        for (int i = 0, n = analyticFunctions.size(); i < n; i++) {
            analyticFunctions.getQuick(i).reset();
        }
    }

    @Override
    protected void _close() {
        if (closed) {
            return;
        }
        Misc.free(base);
        Misc.free(cursor);
        Misc.freeObjList(functions);
        closed = true;
    }

    class AnalyticRecordCursor extends VirtualFunctionDirectSymbolRecordCursor {

        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;

        public AnalyticRecordCursor(ObjList<Function> functions, boolean supportsRandomAccess) {
            super(functions, supportsRandomAccess);
        }

        @Override
        public void close() {
            if (isOpen) {
                super.close();
                resetFunctions(); // calls close on map within RowNumber
                isOpen = false;
            }
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTripped();
            boolean hasNext = super.hasNext();
            if (hasNext) {
                for (int i = 0; i < analyticFunctionsCount; i++) {
                    analyticFunctions.getQuick(i).computeNext(baseCursor.getRecord());
                }
            }
            return hasNext;
        }

        @Override
        public void toTop() {
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
            baseCursor.toTop();
        }

        private void of(RecordCursor base, SqlExecutionContext context) throws SqlException {
            super.of(base);
            circuitBreaker = context.getCircuitBreaker();
            if (!isOpen) {
                reopen(functions, context);
                isOpen = true;
            }
        }

        private void reopen(ObjList<Function> list, SqlExecutionContext context) throws SqlException {
            for (int i = 0, n = list.size(); i < n; i++) {
                Function function = list.getQuick(i);
                function.init(baseCursor, context);
                
                if (function instanceof Reopenable) {
                    ((Reopenable) function).reopen();
                }
            }
        }
    }
}
