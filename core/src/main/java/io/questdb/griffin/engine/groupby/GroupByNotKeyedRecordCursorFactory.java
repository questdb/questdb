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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class GroupByNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {

    protected final RecordCursorFactory base;
    private final GroupByNotKeyedRecordCursor cursor;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final SimpleMapValue simpleMapValue;
    private final VirtualRecord virtualRecordA;

    public GroupByNotKeyedRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            RecordCursorFactory base,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            int valueCount
    ) {
        super(groupByMetadata);
        try {
            this.simpleMapValue = new SimpleMapValue(valueCount);
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            this.virtualRecordA = new VirtualRecordNoRowid(groupByFunctions);
            this.virtualRecordA.of(simpleMapValue);

            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            boolean earlyExitSupported = true;
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                earlyExitSupported &= groupByFunctions.getQuick(0).isEarlyExitSupported();
            }

            if (earlyExitSupported) {
                this.cursor = new EarlyExitGroupByNotKeyedRecordCursor(updater);
            } else {
                this.cursor = new GroupByNotKeyedRecordCursor(updater);
            }
        } catch (Throwable e) {
            Misc.freeObjList(groupByFunctions);
            throw e;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            return cursor.of(baseCursor, executionContext);
        } catch (Throwable e) {
            Misc.free(baseCursor);
            throw e;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("GroupBy");
        sink.meta("vectorized").val(false);
        sink.optAttr("values", groupByFunctions, true);
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

    @Override
    protected void _close() {
        Misc.freeObjList(groupByFunctions);
        Misc.free(base);
    }

    private class EarlyExitGroupByNotKeyedRecordCursor extends GroupByNotKeyedRecordCursor {

        public EarlyExitGroupByNotKeyedRecordCursor(GroupByFunctionsUpdater groupByFunctionsUpdater) {
            super(groupByFunctionsUpdater);
        }

        @Override
        public boolean earlyExit() {
            boolean earlyExit = true;
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                earlyExit &= groupByFunctions.getQuick(i).earlyExit(simpleMapValue);
            }
            return earlyExit;
        }
    }

    private class GroupByNotKeyedRecordCursor implements NoRandomAccessRecordCursor {

        private static final int INIT_DONE = 2;
        private static final int INIT_FIRST_RECORD_DONE = 1;
        private static final int INIT_PENDING = 0;

        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        // hold on to reference of base cursor here
        // because we use it as symbol table source for the functions
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private int initState;
        private int recordsRemaining = 1;

        public GroupByNotKeyedRecordCursor(GroupByFunctionsUpdater groupByFunctionsUpdater) {
            this.groupByFunctionsUpdater = groupByFunctionsUpdater;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (recordsRemaining > 0) {
                counter.add(recordsRemaining);
                recordsRemaining = 0;
            }
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
            Misc.clearObjList(groupByFunctions);
        }

        public boolean earlyExit() {
            return false; // no early exit support here
        }

        @Override
        public Record getRecord() {
            return virtualRecordA;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return (SymbolTable) groupByFunctions.getQuick(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (initState != INIT_DONE) {
                final Record baseRecord = baseCursor.getRecord();
                if (initState != INIT_FIRST_RECORD_DONE) {
                    if (baseCursor.hasNext()) {
                        groupByFunctionsUpdater.updateNew(simpleMapValue, baseRecord);
                    } else {
                        groupByFunctionsUpdater.updateEmpty(simpleMapValue);
                    }
                    initState = INIT_FIRST_RECORD_DONE;
                }
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    groupByFunctionsUpdater.updateExisting(simpleMapValue, baseRecord);
                    if (earlyExit()) {
                        break;
                    }
                }
                toTop();
                initState = INIT_DONE;
            }
            return recordsRemaining-- > 0;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return ((SymbolFunction) groupByFunctions.getQuick(columnIndex)).newSymbolTable();
        }

        public RecordCursor of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            circuitBreaker = executionContext.getCircuitBreaker();
            initState = INIT_PENDING;
            Function.init(groupByFunctions, baseCursor, executionContext);
            toTop();
            return this;
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            recordsRemaining = 1;
            GroupByUtils.toTop(groupByFunctions);
        }
    }
}
