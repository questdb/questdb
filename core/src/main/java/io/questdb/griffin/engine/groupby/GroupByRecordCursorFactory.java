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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class GroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
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
            this.mapSink = RecordSinkFactory.getInstance(asm, base.getMetadata(), listColumnFilter, keyFunctions, null, configuration);
            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            this.cursor = new GroupByRecordCursor(configuration, recordFunctions, groupByFunctions, updater, keyTypes, valueTypes);
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
            Function.init(recordFunctions, baseCursor, executionContext, null);
        } catch (Throwable th) {
            baseCursor.close();
            throw th;
        }

        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        final int columnType = getMetadata().getColumnType(columnIndex);
        return columnType == ColumnType.LONG || ColumnType.isTimestamp(columnType);
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

    private class GroupByRecordCursor extends VirtualFunctionSkewedSymbolRecordCursor {
        private final GroupByAllocator allocator;
        private final Map dataMap;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isDataMapBuilt;
        private boolean isOpen;
        private long rowId;

        public GroupByRecordCursor(
                CairoConfiguration configuration,
                ObjList<Function> functions,
                ObjList<GroupByFunction> groupByFunctions,
                GroupByFunctionsUpdater groupByFunctionsUpdater,
                @Transient @NotNull ArrayColumnTypes keyTypes,
                @Transient @NotNull ArrayColumnTypes valueTypes
        ) {
            super(functions);
            try {
                this.isOpen = true;
                this.dataMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
                this.groupByFunctionsUpdater = groupByFunctionsUpdater;
                this.allocator = GroupByAllocatorFactory.createAllocator(configuration);
                GroupByUtils.setAllocator(groupByFunctions, allocator);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            buildMapConditionally();
            baseCursor.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                Misc.free(dataMap);
                Misc.free(allocator);
                Misc.clearObjList(groupByFunctions);
                super.close();
            }
        }

        @Override
        public boolean hasNext() {
            buildMapConditionally();
            return super.hasNext();
        }

        @Override
        public void longTopK(DirectLongLongSortedList list, int columnIndex) {
            buildMapConditionally();
            ((MapRecordCursor) baseCursor).longTopK(list, recordFunctions.getQuick(columnIndex));
        }

        public void of(RecordCursor managedCursor, SqlExecutionContext executionContext) throws SqlException {
            this.managedCursor = managedCursor;
            if (!isOpen) {
                isOpen = true;
                dataMap.reopen();
                allocator.reopen();
            }
            this.circuitBreaker = executionContext.getCircuitBreaker();
            Function.init(keyFunctions, managedCursor, executionContext, null);
            isDataMapBuilt = false;
            rowId = 0;
        }

        @Override
        public long preComputedStateSize() {
            return isDataMapBuilt ? 1 : 0;
        }

        @Override
        public void toTop() {
            super.toTop();
            rowId = 0;
        }

        private void buildDataMap() {
            final Record baseRecord = managedCursor.getRecord();
            while (managedCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                final MapKey key = dataMap.withKey();
                mapSink.copy(baseRecord, key);
                MapValue value = key.createValue();
                if (value.isNew()) {
                    groupByFunctionsUpdater.updateNew(value, baseRecord, rowId++);
                } else {
                    groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId++);
                }
            }
            super.of(dataMap.getCursor());
            isDataMapBuilt = true;
        }

        private void buildMapConditionally() {
            if (!isDataMapBuilt) {
                buildDataMap();
            }
        }
    }
}
