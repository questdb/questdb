/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class GroupByRecordCursorFactory extends AbstractRecordCursorFactory {

    protected final RecordCursorFactory base;
    private final GroupByRecordCursor cursor;
    private final ObjList<Function> recordFunctions;
    private final ObjList<GroupByFunction> groupByFunctions;
    // this sink is used to copy recordKeyMap keys to dataMap
    private final RecordSink mapSink;

    public GroupByRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions
    ) {
        super(groupByMetadata);
        // sink will be storing record columns to map key
        try {
            this.mapSink = RecordSinkFactory.getInstance(asm, base.getMetadata(), listColumnFilter, false);
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            this.recordFunctions = recordFunctions;
            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            this.cursor = new GroupByRecordCursor(recordFunctions, updater, keyTypes, valueTypes, configuration);
        } catch (Throwable e) {
            Misc.freeObjList(recordFunctions);
            throw e;
        }
    }

    @Override
    protected void _close() {
        Misc.freeObjList(recordFunctions);
        Misc.free(base);
        Misc.free(cursor);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        final RecordCursor baseCursor = base.getCursor(executionContext);

        try {
            Function.init(recordFunctions, baseCursor, executionContext);
            cursor.of(baseCursor, circuitBreaker);
            // init all record function for this cursor, in case functions require metadata and/or symbol tables
            return cursor;
        } catch (Throwable e) {
            baseCursor.close();
            throw e;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("GroupByRecord");
        sink.meta("vectorized").val(false);
        sink.attr("groupByFunctions").val(groupByFunctions);
        sink.attr("recordFunctions").val(recordFunctions);
        sink.child(base);
    }

    class GroupByRecordCursor extends VirtualFunctionSkewedSymbolRecordCursor {
        private final Map dataMap;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private boolean isOpen;

        public GroupByRecordCursor(
                ObjList<Function> functions,
                GroupByFunctionsUpdater groupByFunctionsUpdater,
                @Transient @NotNull ArrayColumnTypes keyTypes,
                @Transient @NotNull ArrayColumnTypes valueTypes,
                CairoConfiguration configuration
        ) {
            super(functions);
            this.dataMap = MapFactory.createMap(configuration, keyTypes, valueTypes);
            this.groupByFunctionsUpdater = groupByFunctionsUpdater;
            this.isOpen = true;
        }

        public void of(RecordCursor baseCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            try {
                if (!isOpen) {
                    isOpen = true;
                    dataMap.reopen();
                }
                final Record baseRecord = baseCursor.getRecord();
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    final MapKey key = dataMap.withKey();
                    mapSink.copy(baseRecord, key);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        groupByFunctionsUpdater.updateNew(value, baseRecord);
                    } else {
                        groupByFunctionsUpdater.updateExisting(value, baseRecord);
                    }
                }
                super.of(baseCursor, dataMap.getCursor());
            } catch (Throwable e) {
                close();
                throw e;
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(dataMap);
                Misc.clearObjList(groupByFunctions);
                super.close();
            }
        }
    }
}
