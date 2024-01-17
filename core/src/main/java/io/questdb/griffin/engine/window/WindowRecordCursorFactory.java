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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.VirtualFunctionDirectSymbolRecordCursor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Factory implements select with window functions that support streaming, that is:
 * - they don't specify order by or order by is the same as underlying query
 * - all functions and their framing clause do support stream-ed processing (single pass)
 */
public class WindowRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final WindowRecordCursor cursor;
    private final ObjList<Function> functions;
    private final ObjList<WindowFunction> windowFunctions;
    private final int windowFunctionsCount;
    private boolean closed = false;

    public WindowRecordCursorFactory(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions
    ) {
        super(metadata);
        this.base = base;
        this.functions = functions;

        windowFunctions = new ObjList<>();
        for (int i = 0, n = functions.size(); i < n; i++) {
            Function func = functions.getQuick(i);
            if (func instanceof WindowFunction) {
                windowFunctions.add((WindowFunction) func);
            }
        }
        windowFunctionsCount = windowFunctions.size();

        // random access is not supported because window function value depends on the window/frame
        // context and can't be computed from single row alone, e.g. even though we might be able
        // to skip to a rowId, we'd still need to compute values for all the rows in between
        this.cursor = new WindowRecordCursor(functions, false);
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
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // window functions normally depends on other rows in the window/frame, so we can't just jump to an arbitrary position
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Window");
        sink.optAttr("functions", windowFunctions, true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private void resetFunctions() {
        for (int i = 0, n = windowFunctions.size(); i < n; i++) {
            windowFunctions.getQuick(i).reset();
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

    class WindowRecordCursor extends VirtualFunctionDirectSymbolRecordCursor {

        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;

        public WindowRecordCursor(ObjList<Function> functions, boolean supportsRandomAccess) {
            super(functions, supportsRandomAccess);
            this.isOpen = true;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                super.close();
                resetFunctions();
                isOpen = false;
            }
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTripped();
            boolean hasNext = super.hasNext();
            if (hasNext) {
                for (int i = 0; i < windowFunctionsCount; i++) {
                    windowFunctions.getQuick(i).computeNext(baseCursor.getRecord());
                }
            }
            return hasNext;
        }

        @Override
        public void skipRows(Counter rowCount) throws DataUnavailableException {
            // we can't skip to an arbitrary result set point because current window function value might depend
            // on values in other rows that could be located anywhere
            RecordCursor.skipRows(this, rowCount);
        }

        @Override
        public void toTop() {
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
            baseCursor.toTop();
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            super.of(baseCursor);
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                reopen(functions);
                isOpen = true;
            }
            Function.init(functions, baseCursor, executionContext);
        }

        private void reopen(ObjList<Function> list) {
            for (int i = 0, n = list.size(); i < n; i++) {
                Function function = list.getQuick(i);

                if (function instanceof Reopenable) {
                    ((Reopenable) function).reopen();
                }
            }
        }
    }
}
