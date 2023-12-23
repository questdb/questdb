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
    private final ObjList<GroupByFunction> groupByFunctions;
    private final ObjList<Function> keyFunctions;
    // this sink is used to copy recordKeyMap keys to dataMap
    private final RecordSink mapSink;
    private final ObjList<Function> recordFunctions;

    public GroupByRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> keyFunctions,
            ObjList<Function> recordFunctions
    ) {
        super(groupByMetadata);
        try {
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            this.keyFunctions = keyFunctions;
            this.recordFunctions = recordFunctions;
            // sink will be storing record columns to map key
            this.mapSink = RecordSinkFactory.getInstance(asm, base.getMetadata(), listColumnFilter, keyFunctions, false);
            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            this.cursor = new GroupByRecordCursor(recordFunctions, updater, keyTypes, valueTypes, configuration);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public static ObjList<String> getKeys(ObjList<Function> recordFunctions, RecordMetadata metadata) {
        ObjList<String> keyFuncs = null;
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            if (!(recordFunctions.get(i) instanceof GroupByFunction)) {
                if (keyFuncs == null) {
                    keyFuncs = new ObjList<>();
                }
                keyFuncs.add(metadata.getColumnName(i));
            }
        }
        return keyFuncs;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            // init all record functions for this cursor, in case functions require metadata and/or symbol tables
            Function.init(recordFunctions, baseCursor, executionContext);
            cursor.of(baseCursor, executionContext);
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
    public void toPlan(PlanSink sink) {
        sink.type("GroupBy");
        sink.meta("vectorized").val(false);
        sink.optAttr("keys", getKeys(recordFunctions, getMetadata()));
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
        Misc.freeObjList(recordFunctions); // groupByFunctions are included in recordFunctions
        Misc.freeObjList(keyFunctions);
        Misc.free(base);
        Misc.free(cursor);
    }

    class GroupByRecordCursor extends VirtualFunctionSkewedSymbolRecordCursor {
        private final Map dataMap;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isDataMapBuilt;
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

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (!isDataMapBuilt) {
                buildDataMap();
            }
            baseCursor.calculateSize(circuitBreaker, counter);
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

        @Override
        public boolean hasNext() {
            if (!isDataMapBuilt) {
                buildDataMap();
            }
            return super.hasNext();
        }

        public void of(RecordCursor managedCursor, SqlExecutionContext executionContext) throws SqlException {
            if (!isOpen) {
                isOpen = true;
                dataMap.reopen();
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            this.managedCursor = managedCursor;
            Function.init(keyFunctions, managedCursor, executionContext);
            isDataMapBuilt = false;
        }

        private void buildDataMap() {
            final Record baseRecord = managedCursor.getRecord();
            while (managedCursor.hasNext()) {
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
            super.of(dataMap.getCursor());
            isDataMapBuilt = true;
        }
    }
}
